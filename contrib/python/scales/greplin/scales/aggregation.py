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

"""Utilities for multi-server stat aggregation."""

from collections import defaultdict
import datetime
import json
import os
import re

import six

class DefaultFormat(object):
  """The default format"""

  def getCount(self, data):
    """Get the count"""
    return data['count']


  def getValue(self, data):
    """Get the value"""
    return data['average']



class DirectFormat(object):
  """The direct format (pointed straight at the field we want)"""

  def getCount(self, _):
    "The count"
    return 1


  def getValue(self, data):
    "The value"
    return data



class TimerFormat(object):
  """A Yammer Metrics Timer datum"""

  def getCount(self, data):
    """Get the count"""
    assert data['type'] == "timer"
    return data['rate']['count']


  def getValue(self, data):
    """Get the value"""
    assert data['type'] == "timer"
    return data['duration']['median']



class TimerMeanFormat(object):
  """A Yammer Metrics Timer datum"""

  def getCount(self, data):
    """Get the count"""
    assert data['type'] == "timer"
    return data['rate']['count']


  def getValue(self, data):
    """Get the value"""
    assert data['type'] == "timer"
    return data['duration']['mean']



class CounterFormat(object):
  """A Yammer Metrics Counter datum"""

  def getCount(self, data):
    """Get the count"""
    assert data['type'] == "counter"
    return data['count']


  def getValue(self, data):
    """Get the value of a count (just the count)"""
    assert data['type'] == "counter"
    return data['count']



class MeterFormat(object):
  """A Yammer Metrics Meter datum"""

  def getCount(self, data):
    """Get the count"""
    assert data['type'] == "meter"
    return data['count']


  def getValue(self, data):
    """Get the value"""
    assert data['type'] == "meter"
    return data['mean']



class GaugeFormat(object):
  """A Yammer Metrics Gauge datum"""

  def getValue(self, data):
    """Get the value"""
    assert data['type'] == 'gauge'
    return data['value']



class DataFormats(object):
  """Different ways data can be formatted"""

  DEFAULT = DefaultFormat()
  DIRECT = DirectFormat()
  TIMER = TimerFormat()
  TIMER_MEAN = TimerMeanFormat()
  COUNTER = CounterFormat()
  METER = MeterFormat()
  GAUGE = GaugeFormat()



class Aggregator(object):
  """Base class for stat aggregators."""

  def __init__(self, name = None, dataFormat = DataFormats.DEFAULT):
    self.name = name or self.DEFAULT_NAME
    self._dataFormat = dataFormat


  def clone(self):
    """Creates a clone of this aggregator."""
    return type(self)(name = self.name, dataFormat = self._dataFormat)



class Average(Aggregator):
  """Aggregate average values of a stat."""

  DEFAULT_NAME = "average"
  _count = 0
  _total = 0


  def addValue(self, _, value):
    """Adds a value from the given source."""
    if value is not None:
      try:
        self._count += self._dataFormat.getCount(value)
        self._total += self._dataFormat.getValue(value) * self._dataFormat.getCount(value)
      except TypeError:
        self._count += 1
        self._total += value


  def result(self):
    """Formats the result."""
    return {
      "count": self._count,
      "total": self._total,
      "average": float(self._total) / self._count if self._count else 0
    }



class Sum(Aggregator):
  """Aggregate sum of a stat."""

  DEFAULT_NAME = "sum"

  total = 0


  def addValue(self, _, value):
    """Adds a value from the given source."""
    self.total += self._dataFormat.getValue(value)


  def result(self):
    """Formats the result."""
    return self.total


def _humanSortKey(s):
  """Sort strings with numbers in a way that makes sense to humans (e.g., 5 < 20)"""
  if isinstance(s, str):
    return [w.isdigit() and int(w) or w for w in re.split(r'(\d+)', s)]
  else:
    return s




class InverseMap(Aggregator):
  """Aggregate sum of a stat."""

  DEFAULT_NAME = "inverse"


  def __init__(self, *args, **kw):
    Aggregator.__init__(self, *args, **kw)
    self.__result = defaultdict(list)


  def addValue(self, source, data):
    """Adds a value from the given source."""
    self.__result[self._dataFormat.getValue(data)].append(source)


  def result(self):
    """Formats the result."""
    for value in six.itervalues(self.__result):
      value.sort(key = _humanSortKey)
    return self.__result



class Sorted(Aggregator):
  """Aggregate sorted version of a stat."""

  DEFAULT_NAME = "sorted"


  # pylint: disable=W0622
  def __init__(self, cmp=None, key=None, reverse=False, *args, **kw):
    Aggregator.__init__(self, *args, **kw)
    self.__result = []
    self.__cmp = cmp
    self.__key = key
    self.__reverse = reverse


  def addValue(self, source, data):
    """Adds a value from the given source."""
    self.__result.append((source, self._dataFormat.getValue(data)))


  def result(self):
    """Formats the result."""
    self.__result.sort(cmp = self.__cmp, key = self.__key, reverse = self.__reverse)
    return self.__result


  def clone(self):
    """Creates a clone of this aggregator."""
    return type(self)(self.__cmp, self.__key, self.__reverse, name = self.name, dataFormat = self._dataFormat)



class Highlight(Aggregator):
  """Picks a single value across all sources and highlights it."""

  value = None
  source = None


  def __init__(self, name, fn, dataFormat = DataFormats.DEFAULT):
    """Creates a highlight aggregator - this will pick one of the values to highlight.

    Args:
      name: The name of this aggregator.
      fn: Callable that takes (a, b) and returns True if b should be selected as the highlight, where as is the
          previous chosen highlight.
    """
    Aggregator.__init__(self, name)
    self.fn = fn


  def addValue(self, source, value):
    """Adds a value from the given source."""
    if self.source is None or self.fn(self.value, value):
      self.value = value
      self.source = source


  def result(self):
    """Formats the result."""
    return {
      "source": self.source,
      "value": self.value
    }


  def clone(self):
    """Creates a clone of this aggregator."""
    return Highlight(self.name, self.fn)



class Aggregation(object):
  """Aggregates stat dictionaries."""

  def __init__(self, aggregators):
    """Creates a stat aggregation object from a hierarchical dict representation:

      agg = aggregation.Aggregation({
        'http_hits' : {
          '200': [aggregation.Sum(dataFormat=aggregation.DataFormats.DIRECT)],
          '404': [aggregation.Sum(dataFormat=aggregation.DataFormats.DIRECT)]
      }})

    Also supports regular expression in aggregations keys:

      agg = aggregation.Aggregation({
        'http_hits' : {
          ('ok', re.compile("[1-3][0-9][0-9]")): [aggregation.Sum(dataFormat=aggregation.DataFormats.DIRECT)],
          ('err', re.compile("[4-5][0-9][0-9]")):  [aggregation.Sum(dataFormat=aggregation.DataFormats.DIRECT)]
      }})

    """
    self._aggregators = aggregators
    self._result = {}


  def addSource(self, source, data):
    """Adds the given source's stats."""
    self._aggregate(source, self._aggregators, data, self._result)


  def addJsonDirectory(self, directory, test=None):
    """Adds data from json files in the given directory."""

    for filename in os.listdir(directory):
      try:
        fullPath = os.path.join(directory, filename)
        if not test or test(filename, fullPath):
          with open(fullPath) as f:
            jsonData = json.load(f)
            name, _ = os.path.splitext(filename)
            self.addSource(name, jsonData)

      except ValueError:
        continue


  def _clone(self, aggregators):
    """Clones a list of aggregators."""
    return [x.clone() for x in aggregators]


  def _aggregate(self, source, aggregators, data, result):
    """Performs aggregation at a specific node in the data/aggregator tree."""
    if data is None:
      return

    if hasattr(aggregators, 'items'):
      # Keep walking the tree.
      for key, value in six.iteritems(aggregators):
        if isinstance(key, tuple):
          key, regex = key
          for dataKey, dataValue in six.iteritems(data):
            if regex.match(dataKey):
              result.setdefault(key, {})
              self._aggregate(source, value, dataValue, result[key])
        else:
          if key == '*':
            for dataKey, dataValue in six.iteritems(data):
              result.setdefault(dataKey, {})
              self._aggregate(source, value, dataValue, result[dataKey])
          elif key in data:
            result.setdefault(key, {})
            self._aggregate(source, value, data[key], result[key])

    else:
      # We found a leaf.
      for aggregator in aggregators:
        if aggregator.name not in result:
          result[aggregator.name] = aggregator.clone()
        result[aggregator.name].addValue(source, data)


  def result(self, root = None):
    """Formats the result."""
    root = root or self._result
    if isinstance(root, Aggregator):
      return root.result()
    else:
      result = {}
      for key, value in six.iteritems(root):
        if value:
          result[key] = self.result(value)
      return result



class FileInclusionTest(object):
  """Object to help create good file inclusion tests."""

  def __init__(self, ignoreByName = None, maxAge = None):
    self.ignoreByName = ignoreByName
    self.maxAge = maxAge


  def __call__(self, _, fullPath):
    """Tests if a file should be included in the aggregation."""
    try:
      # Ignore incoming files
      if self.ignoreByName and self.ignoreByName(fullPath):
        return False

      # Ignore old, dead files.
      if self.maxAge:
        stat = os.stat(fullPath)
        age = datetime.datetime.now() - datetime.datetime.fromtimestamp(stat.st_mtime)
        if age > self.maxAge:
          return False

      return True

    except: # pylint: disable=W0702
      return False
