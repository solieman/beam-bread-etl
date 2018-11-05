from __future__ import absolute_import

import collections
import logging
import re
import tempfile
import unittest

from apache_beam.examples import wordcount
from apache_beam.testing.util import open_shards

class WordCountTest(unittest.TestCase):

    SAMPLE_TEXT = (u'a b c a b a\nacento gráfico\nJuly 30, 2018\n\n aa bb cc aa bb aa')

    def create_temp_file(self, contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents.encode('utf-8'))
            return f.name

    def test_basics(self):
        temp_path = self.create_temp_file(self.SAMPLE_TEXT)
        expected_words = collections.defaultdict(int)
        for word in re.findall(r'[\w\']+', self.SAMPLE_TEXT, re.UNICODE):
            expected_words[word.encode('utf-8')] += 1
        wordcount.run([
            '--input=%s*' % temp_path,
            '--output=%s.result' % temp_path
        ])
        # Parse result file and compare
        results = []
        with open_shards(temp_path + '.result-*-of-*') as result_file:
            for line in result_file:
                match = re.search(r'(\S+):([0-9]+)', line, re.UNICODE)
                if match is not None:
                    results.append((match.group(1), int(match.group(2))))
        self.assertEqual(sorted(results), sorted(expected_words.items()))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()