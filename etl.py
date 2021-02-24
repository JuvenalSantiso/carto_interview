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

from apache_beam.options.pipeline_options import PipelineOptions

class EtlCartoOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument('--input',
                      dest='input',
                      help='Input file to process.') 


def run(argv=None):

  pipeline_options = PipelineOptions()
  carto_options = pipeline_options.view_as(EtlCartoOptions)

  with beam.Pipeline(options=pipeline_options) as p:

      # Read the text file[pattern] into a PCollection.
      lines = p | 'read' >> ReadFromText(carto_options.input, skip_header_lines=1)
       
      lines | 'writeb' >> WriteToText("success.txt")

      #output | 'write' >> WriteToText(known_args.output)

      #output | 'write' >> beam.io.WriteToBigQuery(
      #                      table_spec,
      #                      schema=table_schema,
      #                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
      #                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
      #                      )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
