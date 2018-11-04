# coding: utf-8
# Python

import re
import csv
import apache_beam as beam

from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

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
    
    def get_total_number(one_item):
        # get the total transactions for one item
        return [(str(one_item[0]),sum(one_item[1]))]
    
    def get_unique_items(one_day):
        # get the total transactions for one item
        return [(str(one_day[0]),set(one_day[1]))]



    csvData = (p
            | 'Read My File' >> ReadFromText('BreadBasket_DMS.csv')
            | 'Parse CSV to Dict' >> beam.ParDo(parse_item))

    # Getting the maximum number of transactions for each item
    maximumTransactions = (csvData
            | 'pair with transaction' >> beam.Map(lambda record: (record.item, record.transaction))
            | 'Group Transactions By Key' >> beam.GroupByKey()
            | 'Return the Maximum transactions 1' >> beam.ParDo(get_maximum_number)
            | 'Save maximum to file' >> WriteToText('transactions-maximum','.txt')
            | 'JustPrintIt 1' >> beam.ParDo(printIt))
    
    # Getting the total number of transactions for each item
    transactionsCount = (csvData
            | 'pair with one' >> beam.Map(lambda record: (record.item, 1))
            | 'Group Repeate By Key' >> beam.GroupByKey()
            | 'Return the total of the transactions' >> beam.ParDo(get_total_number)
            | 'Save total to file' >> WriteToText('transactions-count','.txt')
            | 'JustPrintIt' >> beam.ParDo(printIt))

    def get_daily_data(one_day):
        # get the total transactions for one item
        return [(str(one_day[0]),set(one_day[1]))]

    # Getting One Day data
    dayData = (csvData
            | 'pair date with data' >> beam.Map(lambda record: (record.date, record.item))
            | 'Group Repeate By Date' >> beam.GroupByKey()
            | 'get total' >> beam.ParDo(get_unique_items)
            | 'Save day to file' >> WriteToText('day-list','.txt')
            | 'JustPrintIt - day' >> beam.ParDo(printIt))

    result = p.run()

    result.wait_until_finish()


if __name__ == '__main__':
    run()