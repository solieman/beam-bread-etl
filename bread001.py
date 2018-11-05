# coding: utf-8
# Python

import re
import csv
import apache_beam as beam
import logging

from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


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
        self.time = time
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

    p = beam.Pipeline(options=PipelineOptions())

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

    
    ###############################
    # Read data from PubSub, decode JSON
    # records = (p 
    #             | beam.io.ReadStringsFromPubSub(..)
    #             | beam.Map(lambda e: json.loads(e)))
    #
    #

    ###############################


    # Load data from text file
    csvData = (p
            # We use a csv file with no header
            | 'Read My File' >> ReadFromText('BreadBasket_DMS.csv')
            | 'Parse CSV to Dict' >> beam.ParDo(parse_item))

    # Pair each item with the number of transactions 
    pairTheItemAndTransactions = (csvData
            # Map the item and the number of transaction in every record
            | 'pair with transaction' >> beam.Map(lambda record: (record.item, record.transaction))
            # Group all the transaction of specific item
            | 'Group Transactions By Key' >> beam.GroupByKey())

    # Getting the maximum number of transactions for each item
    maximumTransactions = (pairTheItemAndTransactions
            | 'Return the Maximum transactions' >> beam.ParDo(get_maximum_number))
            # | 'Save maximum to file' >> WriteToText('output/transactions-maximum','.txt')
            # | 'JustPrintIt 1' >> beam.ParDo(printIt))
    
    # Getting the minimum number of transactions for each item
    minimumTransactions = (pairTheItemAndTransactions
            | 'Return the Minimum transactions' >> beam.ParDo(get_minimum_number))
            # | 'Save minimum to file' >> WriteToText('output/transactions-maximum','.txt')
            # | 'JustPrintIt 1' >> beam.ParDo(printIt))
    
    # Getting the total number of transactions for each item
    transactionsCount = (csvData
            | 'pair with one' >> beam.Map(lambda record: (record.item, 1))
            | 'Group Repeate By Key' >> beam.GroupByKey()
            | 'Return the total of the transactions' >> beam.ParDo(get_total_number))
            # | 'Save total to file' >> WriteToText('output/transactions-count','.txt')
            # | 'JustPrintIt' >> beam.ParDo(printIt))

    # Compine the two previouse operations
    compineCountAndMaximum = ({'count': transactionsCount, 'max': maximumTransactions, 'min': minimumTransactions}
        | beam.CoGroupByKey()
        | 'Save merged to file' >> WriteToText('output/transactions-count-maximum','.txt')
        | beam.ParDo(printIt))

    # Flatten then Group the two previouse operations
    # flattenCountAndMaximum = ((transactionsCount, maximumTransactions, minimumTransactions)
    #     | beam.Flatten()
    #     | beam.GroupByKey()
    #     | 'Print Flattened data' >> beam.ParDo(printIt))
    
    def get_daily_data(one_day):
        # get the total transactions for one item
        return [(str(one_day[0]),one_day[1])]

    # Getting One Day data
    dayData = (csvData
            | 'pair date with data' >> beam.Map(lambda record: (record.date, record.item))
            | 'Group Repeate By Date' >> beam.GroupByKey()
            | 'get total' >> beam.ParDo(get_unique_items)
            | 'Save day to file' >> WriteToText('output/day-list','.txt')
            | 'JustPrintIt - day' >> beam.ParDo(printIt))

    def printer(day_data):
        print day_data
        # WriteToText(day_data[0],'.txt')
    
    # Every day data
    # everyDayData = (csvData
    #         | 'pair every day with its data' >> beam.Map(lambda record: (record.date, [record.item, record.transaction]))
    #         | 'Group By Date' >> beam.GroupByKey())

    # everydayPrinter = (everyDayData
            # | '10' >> beam.Map(lambda record: (record[1]))
            # | '11' >> beam.Map(lambda record: (record[0],record[1]))
            # | '12' >> beam.ParDo(printer))
            # | '2' >> beam.Map(lambda record: (record.item, record.transaction))
            # | '3' >> beam.GroupByKey())
            # | 'print day by day' >> beam.ParDo(printer)
    #         | 'Save day to file' >> WriteToText('output/day-list','.txt')
            # | 'JustPrintIt - every day' >> beam.ParDo(printIt))


    # Specific Day Data
    specificDayData = (csvData
            | 'FilterOneDayData' >> beam.Filter(lambda elem: elem.date == '2016-10-30')
            | 'pair every day with its data 12' >> beam.Map(lambda record: (record.date, [record.item, record.transaction]))
            | 'Group By Date 12' >> beam.GroupByKey())

    specificDayMaxMin = (specificDayData
            | 'day data' >> beam.Map(lambda data: data[1])
            # | 'day data 1' >> beam.Map(lambda dataItem: (dataItem[0], dataItem[1]))
            # | 'group day data' >> beam.GroupByKey()
            | 'just print' >> beam.ParDo(printIt))

    # everydayPrinter = (specificDayData
    #         | '12' >> beam.ParDo(printer))


    result = p.run()

    result.wait_until_finish()


if __name__ == '__main__':
    # Showing the logs
    # logging.getLogger().setLevel(logging.INFO)
    # Run the app
    run()