
from __future__ import absolute_import

import os
import csv
import logging
import argparse
import time
import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery

import requests

# To fix bug with the path of the GOOGLE_APPLICATION_CREDENTIALS
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/path/to/your/credentials/file.json"

from google.cloud import bigquery

TABLE_SCHEMA = ('date:TIMESTAMP, btc:NUMERIC, usd:NUMERIC, eur:NUMERIC')

#####################################
def run(argv=None):

    # Parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    
    parser.add_argument('--output',
                        required=False,
                        help=
                        ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
                        'or DATASET.TABLE.'))
    
    parser.add_argument('--key',
                        required=True,
                        help=(
                            'credentials for the GCP is required. --key path/to/your/credentials/file.json'
                        ))

    known_args, pipeline_args = parser.parse_known_args(argv)


    # For GCP

    # the key to GCP
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=known_args.key

    project_id = # replace with your project ID
    dataset_id =  # replace with your dataset ID
    table_id =  # replace with your table ID
    
    # Read from BigQuery
    # client = bigquery.Client()
    # table_ref = client.dataset(dataset_id).table(table_id)
    # table = client.get_table(table_ref)  # API request
    # print(table)
    # QUERY = ('SELECT * FROM ' + dataset_id + '.' + table_id + ' LIMIT 6')
    # query_job = client.query(QUERY)  # API request
    # rows = query_job.result()  # Waits for query to finish
    # for row in rows:
    #       print(row)
    
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    # p = beam.Pipeline(options=PipelineOptions())

    # Some functions to use
    def printIt(row):
        print row
    
    def parse_btc(btc_item):
        btc, usd, eur = btc_item.split(',')
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return [{"date":st,"BTC":btc,"USD":usd,"EUR":eur}]
    
    # Load data from source file
    csvData = (p
            | 'Read My File' >> ReadFromText(known_args.input))

    # Getting One Day data
    transformed = (csvData
            | 'parse btc' >> beam.ParDo(parse_btc))

    # Persist to BigQuery
    transformed | 'Write' >> beam.io.WriteToBigQuery(
                    table=table_id, # known_args.table_name,
                    dataset=dataset_id, #known_args.dataset_name,
                    project=project_id, #known_args.project_id,
                    schema=TABLE_SCHEMA, #known_args.table_schema,
                    # create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=int(100)
                    )      

    result = p.run()

    result.wait_until_finish()

if __name__ == '__main__':
    # Showing the logs
    # logging.getLogger().setLevel(logging.INFO)
    # Run the app
    run()