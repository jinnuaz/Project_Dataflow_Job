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


class ParseAndCleanMinimal(beam.DoFn):

    def __init__(self, delimiter: str):
        self.delimiter = delimiter
        self.total = Metrics.counter("adidas_demo_min", "rows_total")
        self.written = Metrics.counter("adidas_demo_min", "rows_written")
        self.proc_ms = Metrics.distribution("adidas_demo_min", "per_record_processing_ms")

    def process(self, line: str) -> Iterable[Dict[str, Any]]:
        start = time.time()
        self.total.inc()

        rec = parse_csv_line(line, self.delimiter)

        required = ["Retailer", "RetailerID", "InvoiceDate", "Region", "State", "Product", "SalesMethod"]
        for k in required:
            if not str(rec.get(k, "")).strip():
                raise ValueError(f"Missing required field: {k}")

        out = {
            "Retailer": rec["Retailer"].strip(),
            "RetailerID": int(rec["RetailerID"]),
            "InvoiceDate": rec["InvoiceDate"].strip(), 
            "Region": rec["Region"].strip(),
            "State": rec["State"].strip(),
            "City": rec["City"].strip(),
            "Product": rec["Product"].strip(),
            "PricePerUnit": float(rec["PricePerUnit"]),
            "UnitsSold": int(rec["UnitsSold"]),
            "TotalSales": float(rec["TotalSales"]),
            "OperatingProfit": float(rec["OperatingProfit"]),
            "OperatingMargin": float(rec["OperatingMargin"]),
            "SalesMethod": rec["SalesMethod"].strip(),
            "IngestedAt": utc_now_iso(),
        }

        self.written.inc()
        self.proc_ms.update(int((time.time() - start) * 1000))
        yield out


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_csv",
        required=True,
        help="GCS path to input CSV/TSV, e.g. gs://loony-data-storage-bucket/AdidasSalesDataset.csv",
    )
    parser.add_argument(
        "--bq_table",
        required=True,
        help="BigQuery output table, e.g. your-project:demo_ds.adidas_sales_minimal",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="Delimiter for input file: ',' for CSV or '\\t' for TSV. Default ','",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        rows = (
            p
            | "ReadAdidasCsv" >> ReadFromText(known_args.input_csv, skip_header_lines=1)
            | "ParseAndCleanMinimal" >> beam.ParDo(ParseAndCleanMinimal(delimiter=known_args.delimiter))
        )

        _ = (
            rows
            | "WriteToBQ"
            >> WriteToBigQuery(
                table=known_args.bq_table,
                schema=SCHEMA_AUTODETECT,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method=WriteToBigQuery.Method.FILE_LOADS,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
