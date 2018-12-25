# coding: utf-8
# Python 2.7

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


class TransactionsCountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument(
          '--input',
          default='gs://play_with_data/BreadBasket_DMS.csv',
          help='Path of the file to read from')
      parser.add_argument(
          '--output',
          required=True,
          help='Output file to write results to.')


pipeline_options = PipelineOptions(['--output', 'gs://play_with_data/output/day-list'])
p = beam.Pipeline(options=pipeline_options)

# p = beam.Pipeline(options=PipelineOptions())


class Order(object):
    def __init__(self, date, time, transaction, item):
        self.date = date
        self.time = time
        self.transaction = transaction
        self.item = item


class parse_item(beam.DoFn):
    def process(self, element):
        if element:
            date, time, transaction, item = element.split(',')
            return [Order(date, time, transaction, item)]

class GetTotal(beam.DoFn):
    def process(self, element):
        # get the total transactions for one item
        return [(str(element[0]), sum(element[1]))]


data_from_source = (p
                    | 'ReadMyFile' >> ReadFromText('gs://play_with_data/BreadBasket_DMS.csv')
                    | 'Splitter using beam.Map' >> beam.Map(lambda record: (record.split(','))[0])
                    | 'Map record to 1' >> beam.Map(lambda record: (record, 1))
                    | 'GroupBy the data' >> beam.GroupByKey()
                    | 'Get the total in each day' >> beam.ParDo(GetTotal())
                    | 'Export results to new file' >> WriteToText('gs://play_with_data/output/day-list', '.txt')
                    )

result = p.run()