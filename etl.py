from __future__ import absolute_import

import argparse
import logging
import re

from datetime import datetime

from past.builtins import unicode

import apache_beam as beam
from apache_beam import window

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery

from apache_beam.options.pipeline_options import PipelineOptions

def split_csv(row):
    print("First")
    print(row)
    patt = re.compile("[^\t]+")
    return patt.findall(row)

def format_datetime(row):
    try:
        from datetime import datetime
        print(type(row[1]))
        print(row[1])
        row[1] = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
        row[2] = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
        return row
    except Exception as e:
        print(e)
        print("Rule not satisfied: 'tpep_pickup_datetime' or 'tpep_dropoff_datetime' has no format '%Y-%m-%d %H:%M:%S'")

def convert_in_json_lines(row):
    if(row):
        record = {
            "vendor_id": row[0],
            "tpep_pickup_datetime": row[1],
            "tpep_dropoff_datetime": row[2],
            "passenger_count": row[3],
            "trip_distance": row[4],
            "pickup_longitude": row[5],
            "pickup_latitude": row[6],
            "rate_code_id": row[7],
            "store_and_fwd_flag": row[8],
            "dropoff_longitude": row[9],
            "dropoff_latitude": row[10],
            "payment_type": row[11],
            "fare_amount": row[12],
            "extra": row[12],
            "mta_tax": row[13],
            "tip_amount": row[14],
            "tolls_amount": row[15],
            "improvement_surcharge": row[16],
            "total_amount": row[17]
        }
    else:
        from datetime import datetime
        record = {
            "vendor_id": None,
            "tpep_pickup_datetime": None,
            "tpep_dropoff_datetime": None,
            "passenger_count": None,
            "trip_distance": None,
            "pickup_longitude": None,
            "pickup_latitude": None,
            "rate_code_id": None,
            "store_and_fwd_flag": None,
            "dropoff_longitude": None,
            "dropoff_latitude": None,
            "payment_type": None,
            "fare_amount": None,
            "extra": None,
            "mta_tax": None,
            "tip_amount": None,
            "tolls_amount": None,
            "improvement_surcharge": None,
            "total_amount": None
        }

    return record

class EtlCartoOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument('--input',
                      dest='input',
                      help='Input file to process.') 


def run(argv=None):

  pipeline_options = PipelineOptions()
  carto_options = pipeline_options.view_as(EtlCartoOptions)

  table_spec = "cartointerview:taxi_data.trips"
  table_schema = """vendor_id:NUMERIC,tpep_pickup_datetime:TIMESTAMP,
				  tpep_dropoff_datetime:TIMESTAMP,passenger_count:NUMERIC,
				  trip_distance:NUMERIC,pickup_longitude:FLOAT64,pickup_latitude:FLOAT64,
				  rate_code_id:NUMERIC,store_and_fwd_flag:BOOLEAN,dropoff_longitude:FLOAT64,dropoff_latitude:FLOAT64,
				  payment_type:NUMERIC,fare_amount:NUMERIC,extra:NUMERIC,mta_tax:NUMERIC,
			      tip_amount:NUMERIC,tolls_amount:NUMERIC,improvement_surcharge:NUMERIC,total_amount:NUMERIC"""

  with beam.Pipeline(options=pipeline_options) as p:

      # Read the text file[pattern] into a PCollection.
      lines = p | 'read' >> ReadFromText(carto_options.input, skip_header_lines=1)
                 
      output = ( lines
                 | 'split_row' >> beam.Map(split_csv)
                 | 'format_datetime' >> beam.Map(format_datetime)
                 | 'convert_jsonl' >> beam.Map(convert_in_json_lines)
               )

      #output | 'writeb' >> WriteToText("success.txt")
      

      #output | 'write' >> WriteToText(known_args.output)

      output | 'write' >> beam.io.WriteToBigQuery(
                            table_spec,
                            schema=table_schema,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                            )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
