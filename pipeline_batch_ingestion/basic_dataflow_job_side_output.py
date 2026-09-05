# basic_dataflow_job_no_side_input.py
#
# Dataflow batch ingestion demo (NO side input):
# - Read AdidasSalesDataset CSV from GCS
# - Validate + enrich rows (schema/type + operating margin sanity only)
# - Write cleaned rows to BigQuery (schema autodetect via load jobs)
# - Write invalid rows to BigQuery dead-letter table (explicit schema)
# - Emit Beam metrics: total/valid/invalid + per-record processing latency

import argparse
import csv
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Iterable

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition, SCHEMA_AUTODETECT


EXPECTED_HEADERS = [
    "Retailer", "RetailerID", "InvoiceDate", "Region", "State", "City", "Product",
    "PricePerUnit", "UnitsSold", "TotalSales", "OperatingProfit", "OperatingMargin", "SalesMethod"
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_csv_line(line: str, delimiter: str) -> Dict[str, str]:
    row = next(csv.reader([line], delimiter=delimiter))
    if len(row) != len(EXPECTED_HEADERS):
        raise ValueError(f"Expected {len(EXPECTED_HEADERS)} columns, got {len(row)}")
    return dict(zip(EXPECTED_HEADERS, row))


class ParseValidateEnrichNoSideInput(beam.DoFn):
    """
    Parses and validates each record WITHOUT any side input.
    Keeps:
      - required fields
      - type coercion
      - operating margin sanity check
      - basic enrichment (DerivedOperatingMargin, ValidatedAt)
    Routes invalid rows to dead-letter output.
    """

    def __init__(self, delimiter: str):
        self.delimiter = delimiter
        self.total = Metrics.counter("adidas_demo", "rows_total")
        self.valid = Metrics.counter("adidas_demo", "rows_valid")
        self.invalid = Metrics.counter("adidas_demo", "rows_invalid")
        self.proc_ms = Metrics.distribution("adidas_demo", "per_record_processing_ms")

    def process(self, line: str) -> Iterable[Dict[str, Any]]:
        start = time.time()
        self.total.inc()

        try:
            rec = parse_csv_line(line, self.delimiter)

            # Required fields
            required = ["Retailer", "RetailerID", "InvoiceDate", "Region", "State", "Product", "SalesMethod"]
            for k in required:
                if not str(rec.get(k, "")).strip():
                    raise ValueError(f"Missing required field: {k}")

            # Type coercion
            retailer_id = int(rec["RetailerID"])
            price_per_unit = float(rec["PricePerUnit"])
            units_sold = int(rec["UnitsSold"])
            total_sales = float(rec["TotalSales"])
            operating_profit = float(rec["OperatingProfit"])
            operating_margin = float(rec["OperatingMargin"])

            # Operating margin sanity check (reasonable + demo-friendly)
            if total_sales <= 0:
                raise ValueError("TotalSales <= 0, cannot validate OperatingMargin safely")

            derived_margin = (operating_profit / total_sales) * 100.0
            if abs(derived_margin - operating_margin) > 2.0:
                raise ValueError(f"OperatingMargin mismatch: got {operating_margin}, derived {derived_margin:.2f}")

            out = {
                "Retailer": rec["Retailer"].strip(),
                "RetailerID": retailer_id,
                "InvoiceDate": rec["InvoiceDate"].strip(),  # keep as string; schema autodetect can infer if consistent
                "Region": rec["Region"].strip(),
                "State": rec["State"].strip(),
                "City": rec["City"].strip(),
                "Product": rec["Product"].strip(),
                "PricePerUnit": price_per_unit,
                "UnitsSold": units_sold,
                "TotalSales": total_sales,
                "OperatingProfit": operating_profit,
                "OperatingMargin": operating_margin,
                "SalesMethod": rec["SalesMethod"].strip(),
                "DerivedOperatingMargin": round(derived_margin, 2),
                "ValidatedAt": utc_now_iso(),
            }

            self.valid.inc()
            yield out

        except Exception as e:
            self.invalid.inc()
            yield beam.pvalue.TaggedOutput(
                "deadletter",
                {
                    "raw_line": line,
                    "error_message": str(e),
                    "ingested_at": utc_now_iso(),
                }
            )
        finally:
            self.proc_ms.update(int((time.time() - start) * 1000))


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_csv",
        required=True,
        help="GCS path to input CSV/TSV, e.g. gs://loony-data-storage-bucket/AdidasSalesDataset.csv",
    )
    parser.add_argument(
        "--bq_clean_table",
        required=True,
        help="BigQuery table for cleaned output, e.g. your-project:demo_ds.adidas_sales_cleaned",
    )
    parser.add_argument(
        "--bq_deadletter_table",
        required=True,
        help="BigQuery table for dead-letter output, e.g. your-project:demo_ds.adidas_sales_deadletter",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="Delimiter for input file: ',' for CSV or '\\t' for TSV. Default ','",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    deadletter_schema = {
        "fields": [
            {"name": "raw_line", "type": "STRING", "mode": "REQUIRED"},
            {"name": "error_message", "type": "STRING", "mode": "REQUIRED"},
            {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadAdidasCsv" >> ReadFromText(known_args.input_csv, skip_header_lines=1)

        parsed = (
            lines
            | "ParseValidateEnrich_NoSideInput"
            >> beam.ParDo(ParseValidateEnrichNoSideInput(delimiter=known_args.delimiter))
                .with_outputs("deadletter", main="valid")
        )

        valid_rows = parsed.valid
        dead_rows = parsed.deadletter

        _ = (
            valid_rows
            | "WriteCleanToBQ"
            >> WriteToBigQuery(
                table=known_args.bq_clean_table,
                schema=SCHEMA_AUTODETECT,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method=WriteToBigQuery.Method.FILE_LOADS,
            )
        )

        _ = (
            dead_rows
            | "WriteDeadletterToBQ"
            >> WriteToBigQuery(
                table=known_args.bq_deadletter_table,
                schema=deadletter_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method=WriteToBigQuery.Method.FILE_LOADS,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
