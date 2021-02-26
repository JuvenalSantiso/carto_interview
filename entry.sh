#!/bin/bash

#filename=$1
for filename in "$@"; do
  echo "$filename"
  unzip ${filename} -d data/data_eng_test
  python etl.py --input "${filename%.*}" --temp_location gs://carto_interview_dataflow/temp --staging_location gs://carto_interview_dataflow/staging 
done