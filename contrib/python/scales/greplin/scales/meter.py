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

"""Classes for metering values"""

try:
  from UserDict import UserDict
except ImportError:
  from collections import UserDict
from greplin.scales import Stat
from greplin.scales.timer import RepeatTimer
from greplin.scales.util import EWMA

TICKERS = []
TICKER_THREAD = RepeatTimer(5, lambda: [t() for t in TICKERS])



class MeterStatDict(UserDict):
  """Stores the meters for MeterStat. Expects to be ticked every 5 seconds."""

  def __init__(self):
    UserDict.__init__(self)
    self._m1 = EWMA.oneMinute()
    self._m5 = EWMA.fiveMinute()
    self._m15 = EWMA.fifteenMinute()
    self._meters = (self._m1, self._m5, self._m15)
    TICKERS.append(self.tick)

    self['unit'] = 'per second'
    self['count'] = 0


  def __getitem__(self, item):
    if item in self:
      return UserDict.__getitem__(self, item)
    else:
      return 0.0


  def tick(self):
    """Updates meters"""
    for m in self._meters:
      m.tick()
    self['m1'] = self._m1.rate
    self['m5'] = self._m5.rate
    self['m15'] = self._m15.rate


  def mark(self, value=1):
    """Updates the dictionary."""

    self['count'] += value
    for m in self._meters:
      m.update(value)



class MeterStat(Stat):
  """A stat that stores m1, m5, m15. Updated every 5 seconds via TICKER_THREAD."""

  def __init__(self, name, _=None):
    Stat.__init__(self, name, None)


  def _getDefault(self, _):
    """Returns a default MeterStatDict"""
    return MeterStatDict()


  def __set__(self, instance, value):
    self.__get__(instance, None).mark(value)



class MeterDict(UserDict):
  """Dictionary of meters."""

  def __init__(self, parent, instance):
    UserDict.__init__(self)
    self.parent = parent
    self.instance = instance


  def __getitem__(self, item):
    if item in self:
      return UserDict.__getitem__(self, item)
    else:
      meter = MeterStatDict()
      self[item] = meter
      return meter



class MeterDictStat(Stat):
  """Dictionary stat value class."""

  def _getDefault(self, instance):
    return MeterDict(self, instance)
