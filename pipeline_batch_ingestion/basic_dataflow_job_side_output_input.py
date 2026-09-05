# basic_dataflow_job.py
#
# Dataflow batch ingestion demo:
# - Read AdidasSalesDataset CSV from GCS
# - Read adidas_rules.json from GCS as a SIDE INPUT (robust to multi-line JSON)
# - Validate + enrich rows
# - Write valid rows to BigQuery (schema autodetect via load jobs)
# - Write invalid rows to BigQuery dead-letter table (explicit schema)
# - Emit Beam metrics: total/valid/invalid + per-record processing latency

import argparse
import csv
import json
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
    """
    Parses one CSV/TSV data line into a dict keyed by EXPECTED_HEADERS.
    """
    row = next(csv.reader([line], delimiter=delimiter))
    if len(row) != len(EXPECTED_HEADERS):
        raise ValueError(f"Expected {len(EXPECTED_HEADERS)} columns, got {len(row)}")
    return dict(zip(EXPECTED_HEADERS, row))


def parse_rules_text(rules_text: str) -> Dict[str, Any]:
    """
    Parses the entire rules JSON file content as one JSON blob.
    This supports pretty-printed (multi-line) JSON as well as single-line JSON.

    NOTE: The file must be valid JSON (double quotes, not Python dict syntax).
    """
    try:
        return json.loads(rules_text)
    except json.JSONDecodeError as e:
        snippet = rules_text[:200].replace("\n", "\\n")
        raise ValueError(
            f"Rules JSON is not valid JSON. Ensure keys/strings use double quotes. "
            f"First 200 chars: {snippet}. Original error: {e}"
        )


class ParseValidateEnrich(beam.DoFn):
    """
    Validates + enriches each record using side-input rules.
    Outputs:
      - main output: cleaned/enriched rows (dict) for BigQuery
      - tagged output 'deadletter': invalid rows with error_message
    """

    def __init__(self, delimiter: str):
        self.delimiter = delimiter
        self.total = Metrics.counter("adidas_demo", "rows_total")
        self.valid = Metrics.counter("adidas_demo", "rows_valid")
        self.invalid = Metrics.counter("adidas_demo", "rows_invalid")
        self.proc_ms = Metrics.distribution("adidas_demo", "per_record_processing_ms")

    def process(self, line: str, rules: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
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

            sales_method = rec["SalesMethod"].strip()
            state = rec["State"].strip()
            region = rec["Region"].strip()

            # Side-input validation: allowed SalesMethod
            allowed_methods = set(rules.get("allowed_sales_methods", []))
            if allowed_methods and sales_method not in allowed_methods:
                raise ValueError(f"Invalid SalesMethod '{sales_method}'. Allowed: {sorted(list(allowed_methods))}")

            # Side-input validation: State -> Region consistency
            region_by_state = rules.get("region_by_state", {})
            expected_region = region_by_state.get(state)
            if expected_region and region != expected_region:
                raise ValueError(
                    f"Region mismatch for state '{state}': got '{region}', expected '{expected_region}'"
                )

            # Operating margin sanity check (reasonable + demo-friendly)
            if total_sales <= 0:
                raise ValueError("TotalSales <= 0, cannot validate OperatingMargin safely")

            derived_margin = (operating_profit / total_sales) * 100.0
            if abs(derived_margin - operating_margin) > 2.0:
                raise ValueError(f"OperatingMargin mismatch: got {operating_margin}, derived {derived_margin:.2f}")

            # Enrichment using side input: channel fee + net sales
            fee_rate = float(rules.get("channel_fee_rate", {}).get(sales_method, 0.0))
            net_sales = total_sales * (1.0 - fee_rate)

            out = {
                "Retailer": rec["Retailer"].strip(),
                "RetailerID": retailer_id,
                "InvoiceDate": rec["InvoiceDate"].strip(),  # keep as string; schema autodetect can infer if consistent
                "Region": region,
                "State": state,
                "City": rec["City"].strip(),
                "Product": rec["Product"].strip(),
                "PricePerUnit": price_per_unit,
                "UnitsSold": units_sold,
                "TotalSales": total_sales,
                "OperatingProfit": operating_profit,
                "OperatingMargin": operating_margin,
                "SalesMethod": sales_method,
                "DerivedOperatingMargin": round(derived_margin, 2),
                "ChannelFeeRate": fee_rate,
                "NetSales": round(net_sales, 2),
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


def build_rules_side_input(p: beam.Pipeline, rules_json_path: str) -> beam.pvalue.AsSingleton:
    """
    Reads the rules JSON file and returns an AsSingleton side input value (a dict).

    This approach is robust to multi-line JSON:
      - ReadFromText yields each line
      - ToList collects all lines
      - join lines back into one string
      - json.loads on the full string
    """
    rules_pc = (
        p
        | "ReadRulesJsonLines" >> ReadFromText(rules_json_path)
        | "RulesCollectLines" >> beam.combiners.ToList()
        | "RulesJoinLines" >> beam.Map(lambda lines: "\n".join(lines))
        | "RulesParseJson" >> beam.Map(parse_rules_text)
    )
    return beam.pvalue.AsSingleton(rules_pc)


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_csv",
        required=True,
        help="GCS path to input CSV/TSV, e.g. gs://loony-data-storage-bucket/AdidasSalesDataset.csv",
    )
    parser.add_argument(
        "--rules_json",
        required=True,
        help="GCS path to rules JSON, e.g. gs://loony-data-storage-bucket/ref/adidas_rules.json",
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
        rules_side = build_rules_side_input(p, known_args.rules_json)

        lines = p | "ReadAdidasCsv" >> ReadFromText(known_args.input_csv, skip_header_lines=1)

        parsed = (
            lines
            | "ParseValidateEnrich"
            >> beam.ParDo(
                ParseValidateEnrich(delimiter=known_args.delimiter),
                rules=rules_side,
            ).with_outputs("deadletter", main="valid")
        )

        valid_rows = parsed.valid
        dead_rows = parsed.deadletter

        # Cleaned output: schema autodetect (load jobs)
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

        # Dead-letter output: explicit schema
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
