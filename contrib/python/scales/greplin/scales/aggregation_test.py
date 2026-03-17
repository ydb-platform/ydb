# Copyright 2011 The scales Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Stat aggregation tests."""

import re

from greplin.scales import aggregation

import unittest



class AggregationTest(unittest.TestCase):
  """Test cases for stat aggregation classes."""

  def testNoData(self):
    "This used to infinite loop."
    agg = aggregation.Aggregation({
      'a': {
        '*': [aggregation.Sum()]
      }
    })
    agg.addSource('source1', {'a': {}})
    agg.result()


  def testRegex(self):
    "Test regexes in aggregation keys"
    agg = aggregation.Aggregation({
        'a' : {
            ('success', re.compile("[1-3][0-9][0-9]")):  [aggregation.Sum(dataFormat = aggregation.DataFormats.DIRECT)],
            ('error', re.compile("[4-5][0-9][0-9]")):  [aggregation.Sum(dataFormat = aggregation.DataFormats.DIRECT)]
        }})
    agg.addSource('source1', {'a': {'200': 10, '302': 10, '404': 1, '500': 3}})
    result = agg.result()
    self.assertEqual(result['a']['success']['sum'], 20)
    self.assertEqual(result['a']['error']['sum'], 4)



if __name__ == '__main__':
  unittest.main()
