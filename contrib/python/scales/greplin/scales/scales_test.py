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

"""Tests for the stats module."""

from greplin import scales

import unittest



class Root1(object):
  """Root level test class."""

  stateStat = scales.Stat('state')
  errorsStat = scales.IntDictStat('errors')
  activeUrlsStat = scales.IntDictStat('activeUrls', autoDelete=True)


  def __init__(self):
    scales.init(self, 'path/to/A')


  def getChild(self, cls, *args):
    """Creates a child."""
    return cls(*args)



class Root2(object):
  """Root level test class."""

  def __init__(self):
    scales.init(self, 'B')


  def getChild(self, cls):
    """Creates a child."""
    return cls()



class AggregatingRoot(object):
  """Root level test class with aggregation."""

  countStat = scales.SumAggregationStat('count')
  stateStat = scales.HistogramAggregationStat('state')
  errorsStat = scales.IntDictSumAggregationStat('errors')


  def __init__(self):
    scales.init(self, 'Root')


  def getChild(self, cls, *args):
    """Creates a child."""
    return cls(*args)



class AggregatingRootSubclass(AggregatingRoot):
  """Subclass of a class with aggregates."""



class TypedChild(object):
  """Child level test class."""

  countStat = scales.IntStat('count')


  def __init__(self):
    scales.initChildOfType(self, 'C')



class Child(object):
  """Child level test class."""

  countStat = scales.IntStat('count')
  stateStat = scales.Stat('state')
  errorsStat = scales.IntDictStat('errors')


  def __init__(self, name='C'):
    scales.initChild(self, name)



class DynamicRoot(object):
  """Root class with a dynamic stat."""

  value = 100
  dynamicStat = scales.Stat('dynamic')


  def __init__(self):
    scales.init(self)
    self.dynamicStat = lambda: DynamicRoot.value



class StatsTest(unittest.TestCase):
  """Test cases for stats classes."""


  def setUp(self):
    """Reset global state."""
    scales.reset()


  def testChildTypeStats(self):
    """Tests for child stats with typed children (auto-numbered)."""
    a = Root1()
    a.stateStat = 'abc'
    c = a.getChild(TypedChild)
    c.countStat += 1
    b = Root2()
    c = b.getChild(TypedChild)
    c.countStat += 2

    self.assertEqual({
      'path': {
        'to': {
          'A': {
            'state': 'abc',
            'C': {
              '1': {'count': 1}
            }
          }
        }
      },
      'B': {
        'C': {
          '2': {'count': 2}
        },
      }
    }, scales.getStats())


  def testChildStats(self):
    """Tests for child scales."""
    a = Root1()
    a.stateStat = 'abc'
    c = a.getChild(Child)
    c.countStat += 1
    b = Root2()
    c = b.getChild(Child)
    c.countStat += 2

    self.assertEqual({
      'path': {
        'to': {
          'A': {
            'state': 'abc',
            'C': {
              'count': 1
            }
          }
        }
      },
      'B': {
        'C': {
          'count': 2
        },
      }
    }, scales.getStats())


  def testMultilevelChild(self):
    """Tests for multi-level child stats."""
    a = Root1()
    c = a.getChild(Child, 'sub/path')
    c.countStat += 1

    self.assertEqual({
      'path': {
        'to': {
          'A': {
            'sub': {
              'path': {
                'count': 1
              }
            }
          }
        }
      }
    }, scales.getStats())


  def testStatSum(self):
    """Tests for summed stats."""
    self.helpTestStatSum(AggregatingRoot())


  def testStatSumWithSubclassRoot(self):
    """Tests for summed stats."""
    self.helpTestStatSum(AggregatingRootSubclass())


  def helpTestStatSum(self, a):
    """Helps test summed stats."""
    c = a.getChild(Child)

    self.assertEqual({
      'Root': {
        'C': {},
      }
    }, scales.getStats())

    c.countStat += 2

    self.assertEqual({
      'Root': {
        'count': 2,
        'C': {
          'count': 2
        },
      }
    }, scales.getStats())

    d = a.getChild(Child, 'D')
    self.assertEqual({
      'Root': {
        'count': 2,
        'C': {
          'count': 2
        },
        'D': {}
      }
    }, scales.getStats())

    c.countStat -= 1
    d.countStat += 5
    self.assertEqual({
      'Root': {
        'count': 6,
        'C': {
          'count': 1
        },
        'D': {
          'count': 5
        }
      }
    }, scales.getStats())


  def testStatHistogram(self):
    """Tests for stats aggregated in to a histogram."""
    a = AggregatingRoot()
    c = a.getChild(Child)
    d = a.getChild(Child, 'D')

    # Do it twice to make sure its idempotent.
    for _ in range(2):
      c.stateStat = 'good'
      d.stateStat = 'bad'
      self.assertEqual({
        'Root': {
          'state': {
            'good': 1,
            'bad': 1
          },
          'C': {
            'state': 'good'
          },
          'D': {
            'state': 'bad'
          }
        }
      }, scales.getStats())

    c.stateStat = 'great'
    d.stateStat = 'great'
    self.assertEqual({
      'Root': {
        'state': {
          'great': 2,
          'good': 0,
          'bad': 0
        },
        'C': {
          'state': 'great'
        },
        'D': {
          'state': 'great'
        }
      }
    }, scales.getStats())



  def testIntDictStats(self):
    """Tests for int dict stats."""
    a = Root1()
    a.errorsStat['400'] += 1
    a.errorsStat['400'] += 2
    a.errorsStat['404'] += 100
    a.errorsStat['400'] -= 3

    a.activeUrlsStat['http://www.greplin.com'] += 1
    a.activeUrlsStat['http://www.google.com'] += 2
    a.activeUrlsStat['http://www.greplin.com'] -= 1

    self.assertEqual({
      'path': {
        'to': {
          'A': {
            'errors': {
              '400': 0,
              '404': 100
            },
            'activeUrls': {
              'http://www.google.com': 2
            }
          }
        }
      }
    }, scales.getStats())


  def testIntDictStatsAggregation(self):
    """Tests for int dict stats."""
    root = AggregatingRoot()

    errorHolder = root.getChild(Child)

    errorHolder.errorsStat['400'] += 1
    errorHolder.errorsStat['400'] += 2
    errorHolder.errorsStat['404'] += 100
    errorHolder.errorsStat['400'] += 1

    self.assertEqual({
      'Root': {
        'errors': {
          '400': 4,
          '404': 100
        },
        'C': {
          'errors': {
            '400': 4,
            '404': 100
          }
        }
      }
    }, scales.getStats())


  def testDynamic(self):
    """Tests for dynamic stats."""
    DynamicRoot()
    self.assertEqual(100, scales.getStats()['dynamic']())

    DynamicRoot.value = 200
    self.assertEqual(200, scales.getStats()['dynamic']())


  def testCollection(self):
    """Tests for a stat collection."""
    collection = scales.collection('/thePath', scales.IntStat('count'), scales.IntDictStat('histo'))
    collection.count += 100
    collection.histo['cheese'] += 12300
    collection.histo['cheese'] += 45

    self.assertEqual({
      'thePath': {
        'count': 100,
        'histo': {
          'cheese': 12345
        }
      }
    }, scales.getStats())
