
from __future__ import absolute_import

import re
import csv
import apache_beam as beam
import logging
import argparse

from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


import requests

class MyOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input',
                        help='Input for the pipeline',
                        default='gs://my-bucket/input')
    parser.add_argument('--output',
                        help='Output for the pipeline',
                        default='gs://my-bucket/output')

#####################################
class Order(object):
    def __init__(self, date, time, transaction, item):
        self.date = date
        self.transaction = transaction
        self.item = item

# class OrderCoder(beam.coders.Coder):
#   def encode(self, order):
#     return '%s:%s:%s:%s' % (order.date, order.time, order.transaction, order.item)

#   def decode(self, s):
#     return Order(*s.split(':'))

#   def is_deterministic(self):
#     return True
#####################################

class AddKeyToDict(beam.DoFn):
    def process(self, element):
        logging.info('LOG: {}'.format(element))
        # Input: Dictionary
        # Output: key/value tuple (key, elemnt)
        return [(element['clientid'], element)]

# TO-USE: records | beam.ParDo(AddKeyToDict())

#####################################
def run(argv=None):
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    # p = beam.Pipeline(options=PipelineOptions())

    def printIt(row):
        print row
    
    def parse_item(csv):
        date, time, transaction, item = csv.split(',')
        return [Order(date, time, transaction, item)]

    def get_maximum_number(one_item):
        # Getting the Maximum transactions for an item
        return [(str(one_item[0]),max(map(int, one_item[1])))]

    def get_minimum_number(one_item):
        # Getting the Minimum transactions for an item
        return [(str(one_item[0]),min(map(int, one_item[1])))]
    
    def get_total_number(one_item):
        # get the total transactions for one item
        return [(str(one_item[0]),sum(one_item[1]))]
    
    def get_unique_items(one_day):
        # get the total transactions for one item
        return [(str(one_day[0]),one_day[1])]

    def get_api_data(data):
        # print data
        data_every_sec = requests.get("https://min-api.cryptocompare.com/data/price?fsym=ETH&tsyms=BTC,USD,EUR").json()
        print data_every_sec
        return [True]

    
    ###############################
    # Read data from PubSub, decode JSON
    # records = (p 
    #             | beam.io.ReadStringsFromPubSub(..)
    #             | beam.Map(lambda e: json.loads(e)))
    ###############################

    # Load data from text file
    csvData = (p
            | 'Read My File' >> ReadFromText(known_args.input)
            | 'Parse CSV to Dict' >> beam.ParDo(parse_item))

    def get_daily_data(one_day):
        # get the total transactions for one item
        return [(str(one_day[0]),one_day[1])]

    # Getting One Day data
    dayData = (csvData
            | 'pair date with data' >> beam.Map(lambda record: (record.date, [record.item,record.transaction]))
            | 'Group Repeate By Date' >> beam.GroupByKey()
            | 'get data' >> beam.ParDo(get_api_data)
            # | 'Save day to file' >> WriteToText(known_args.output)
            | 'JustPrintIt - day' >> beam.ParDo(printIt))

    result = p.run()

    result.wait_until_finish()

if __name__ == '__main__':
    # Showing the logs
    # logging.getLogger().setLevel(logging.INFO)
    # Run the app
    run()