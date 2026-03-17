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

"""Classes for tracking system statistics."""

import collections
import inspect
import itertools
import gc
import six
import unittest
import json
import time
from contextlib import contextmanager

try:
  from UserDict import UserDict
except ImportError:
  from collections import UserDict
from greplin.scales.samplestats import ExponentiallyDecayingReservoir

ID_KEY = '__STATS__id'


NEXT_ID = itertools.count()


def statsId(obj):
  """Gets a unique ID for each object."""
  if hasattr(obj, ID_KEY):
    return getattr(obj, ID_KEY)
  newId = next(NEXT_ID)
  setattr(obj, ID_KEY, newId)
  return newId


def init(obj, context=None):
  """Initializes stats collection in the given object at the given context path.  Defaults to root-level stats."""
  return _Stats.init(obj, context)


def initChild(obj, name):
  """Initializes stats collection in the given object as a child of the object that created it."""
  return _Stats.initChild(obj, name, '')


def initChildOfType(obj, name, subContext=None):
  """Initializes stats collection in the given object as a child of the object that created it."""
  return _Stats.initChild(obj, name, subContext)


def reset():
  """Resets stats state - should only be called from tests."""
  _Stats.reset()


def getStats():
  """Gets the stats dict."""
  return _Stats.stats


def setCollapsed(path):
  """Sets a stat as collapsed."""
  return _Stats.setCollapsed(path)



class StatContainer(UserDict):
  """Container of stats.  Also contains configuration of how the container should be displayed."""

  def __init__(self):
    UserDict.__init__(self)
    self.__collapsed = False


  def setCollapsed(self, isCollapsed):
    """Sets whether the stat container displays as collapsed by default."""
    self.__collapsed = isCollapsed


  def isCollapsed(self):
    """Returns whether the stat container displays as collapsed by default."""
    return self.__collapsed



class _Stats(object):
  """Static class for stats aggregation."""

  stats = StatContainer()

  parentMap = {}

  containerMap = {}

  subId = 0


  @classmethod
  def reset(cls):
    """Resets the static state.  Should only be called by tests."""
    cls.stats = StatContainer()
    cls.parentMap = {}
    cls.containerMap = {}
    cls.subId = 0
    for stat in gc.get_objects():
      if isinstance(stat, Stat):
        stat._aggregators = {}


  @classmethod
  def init(cls, obj, context):
    """Implementation of init."""
    addr = statsId(obj)
    if addr not in cls.containerMap:
      cls.containerMap[addr] = cls.__getStatContainer(context)
    return cls.containerMap[addr]


  @classmethod
  def __getSelf(cls, frame):
    """Extracts the self object out of a stack frame."""
    return inspect.getargvalues(frame).locals.get('self', None)


  @classmethod
  def initChild(cls, obj, name, subContext, parent = None):
    """Implementation of initChild."""
    addr = statsId(obj)
    if addr not in cls.containerMap:
      if not parent:
        # Find out the parent of the calling object by going back through the call stack until a self != this.
        f = inspect.currentframe()
        while not cls.__getSelf(f):
          f = f.f_back
        this = cls.__getSelf(f)
        f = f.f_back
        while cls.__getSelf(f) == this or not cls.__getSelf(f):
          f = f.f_back
        parent = cls.__getSelf(f)

      # Default subcontext to an autoincrementing ID.
      if subContext is None:
        cls.subId += 1
        subContext = cls.subId

      if subContext is not '':
        path = '%s/%s' % (name, subContext)
      else:
        path = name

      # Now that we have the name, create an entry for this object.
      cls.parentMap[addr] = parent
      container = cls.getContainerForObject(statsId(parent))
      if not container and isinstance(parent, unittest.TestCase):
        cls.init(parent, '/test-case')
      cls.containerMap[addr] = cls.__getStatContainer(path, cls.getContainerForObject(statsId(parent)))
    return cls.containerMap[addr]


  @classmethod
  def __getStatContainer(cls, context, parent=None):
    """Get the stat container for the given context under the given parent."""
    container = parent
    if container is None:
      container = cls.stats
    if context is not None:
      context = str(context).lstrip('/')
      for key in context.split('/'):
        container.setdefault(key, StatContainer())
        container = container[key]
    return container


  @classmethod
  def getContainerForObject(cls, instanceId):
    """Get the stat container for the given object."""
    return cls.containerMap.get(instanceId, None)


  @classmethod
  def getStat(cls, obj, name):
    """Gets the stat for the given object with the given name, or None if no such stat exists."""
    objClass = type(obj)
    for theClass in objClass.__mro__:
      if theClass == object:
        break
      for value in theClass.__dict__.values():
        if isinstance(value, Stat) and value.getName() == name:
          return value


  @classmethod
  def getAggregator(cls, instanceId, name):
    """Gets the aggregate stat for the given stat."""
    parent = cls.parentMap.get(instanceId)
    while parent:
      stat = cls.getStat(parent, name)
      if stat:
        return stat, parent
      parent = cls.parentMap.get(statsId(parent))


  @classmethod
  def setCollapsed(cls, path):
    """Collapses a stat."""
    cls.__getStatContainer(path).setCollapsed(True)



class Stat(object):
  """Basic stat value class."""

  def __init__(self, name, value='', logger = None):
    self.__name = name
    self.__default = value
    self._logger = logger
    self._aggregators = {}


  def getName(self):
    """Gets the name of the stat."""
    return self.__name


  def __get__(self, instance, _):
    container = _Stats.getContainerForObject(statsId(instance))
    if self.__name not in container:
      container[self.__name] = self._getDefault(instance)
    return container[self.__name]


  def _getInit(self):
    """Internal method to return the initial value for a stat that is never set."""
    return self.__default


  def _getDefault(self, _):
    """Internal method to return the default for a stat that hasn't stored a value yet."""
    return self.__default


  def _aggregate(self, instanceId, container, value, subKey = None):
    """Performs stat aggregation."""

    # Get the aggregator.
    if instanceId not in self._aggregators:
      self._aggregators[instanceId] = _Stats.getAggregator(instanceId, self.__name)
    aggregator = self._aggregators[instanceId]

    # If we are aggregating, get the old value.
    if aggregator:
      oldValue = container.get(self.__name)
      if subKey:
        oldValue = oldValue[subKey]
        aggregator[0].update(aggregator[1], oldValue, value, subKey)
      else:
        aggregator[0].update(aggregator[1], oldValue, value)


  def __set__(self, instance, value):
    instanceId = statsId(instance)

    container = _Stats.getContainerForObject(instanceId)
    self._aggregate(instanceId, container, value)
    container[self.__name] = value

    if self._logger:
      self._logger('Updated stat "%s" with value: %s' % (self.__name, value))


  def updateItem(self, instance, subKey, value):
    """Updates a child value.  Must be called before the update has actually occurred."""
    instanceId = statsId(instance)

    container = _Stats.getContainerForObject(instanceId)
    self._aggregate(instanceId, container, value, subKey)


  def logger(self, logger):
    """Log textual updates about this value to the given function."""
    self._logger = logger
    return self



class IntStat(Stat):
  """Integer stat value class."""

  def __init__(self, name, value=0):
    Stat.__init__(self, name, value)



class DoubleStat(Stat):
  """Double stat value class."""

  def __init__(self, name, value=0.0):
    Stat.__init__(self, name, value)



class IntDict(UserDict):
  """Dictionary of integers."""

  def __init__(self, parent, instance, autoDelete=False):
    UserDict.__init__(self)
    self.parent = parent
    self.instance = instance
    self.autoDelete = autoDelete


  def __getitem__(self, item):
    if item in self:
      return UserDict.__getitem__(self, item)
    else:
      return 0


  def __setitem__(self, key, value):
    self.parent.updateItem(self.instance, key, value)
    if value or not self.autoDelete:
      UserDict.__setitem__(self, key, value)
    elif UserDict.__contains__(self, key):
      UserDict.__delitem__(self, key)



class IntDictStat(Stat):
  """Dictionary stat value class."""

  def __init__(self, name, autoDelete = False):
    Stat.__init__(self, name)
    self.autoDelete = autoDelete


  def _getDefault(self, instance):
    return IntDict(self, instance, self.autoDelete)



class StringDict(UserDict):
  """Dictionary of strings."""

  def __init__(self, parent, instance):
    UserDict.__init__(self)
    self.parent = parent
    self.instance = instance


  def __getitem__(self, item):
    if item in self:
      return UserDict.__getitem__(self, item)
    else:
      return ''


  def __setitem__(self, key, value):
    self.parent.updateItem(self.instance, key, value)
    UserDict.__setitem__(self, key, value)



class StringDictStat(Stat):
  """Dictionary stat value class."""

  def _getDefault(self, instance):
    return StringDict(self, instance)



class AggregationStat(Stat):
  """A stat that aggregates child stats."""

  def __init__(self, name, value):
    Stat.__init__(self, name, value)


  def update(self, instance, oldValue, newValue):
    """Updates the aggregate based on a change in the child value."""
    raise NotImplementedError



class ChildAggregationStat(Stat):
  """A stat that aggregates values within child stats."""

  def __init__(self, name, value):
    Stat.__init__(self, name, value)


  def update(self, instance, oldValue, newValue, subKey):
    """Updates the aggregate based on a change in the child sub-value."""



class SumAggregationStat(AggregationStat):
  """A stat that aggregates child stats in to a sum."""

  def __init__(self, name):
    AggregationStat.__init__(self, name, 0)


  def update(self, instance, oldValue, newValue):
    """Updates the aggregate based on a change in the child value."""
    self.__set__(instance,
                 self.__get__(instance, None) + newValue - (oldValue or 0))



class HistogramAggregationStat(AggregationStat):
  """A stat that aggregates child stats in to a histogram counting each unique child value."""

  def __init__(self, name, autoDelete = False):
    AggregationStat.__init__(self, name, None)
    self.autoDelete = autoDelete


  def _getDefault(self, _):
    return collections.defaultdict(int)


  def update(self, instance, oldValue, newValue):
    """Updates the aggregate based on a change in the child value."""
    histogram = self.__get__(instance, None)
    if oldValue:
      histogram[oldValue] -= 1
      if self.autoDelete and histogram[oldValue] == 0:
        del histogram[oldValue]
    if newValue:
      histogram[newValue] += 1



class IntDictSumAggregationStat(ChildAggregationStat):
  """A stat that aggregates child int dict stats in to a int dict summing child values."""

  def __init__(self, name):
    ChildAggregationStat.__init__(self, name, None)


  def _getDefault(self, _):
    return collections.defaultdict(int)


  def update(self, instance, oldValue, newValue, subKey):
    """Updates the aggregate based on a change in the child value."""
    histogram = self.__get__(instance, None)
    histogram[subKey] += newValue - oldValue



class PmfStatDict(UserDict):
  """Ugly hack defaultdict-like thing."""

  class TimeManager(object):
    """Context manager for timing."""

    def __init__(self, container):
      self.container = container
      self.msg99 = None
      self.start = None
      self.__discard = False


    def __enter__(self):
      self.start = time.time()
      return self


    def __exit__(self, *_):
      if not self.__discard:
        latency = time.time() - self.start
        self.container.addValue(latency)

        if self.container.percentile99 is not None and latency >= self.container.percentile99:
          if self.msg99 is not None:
            logger, msg, args = self.msg99
            logger.warn(msg, *args)


    def warn99(self, logger, msg, *args):
      """If this time through the timed section of code takes longer
      than the 99th percentile time, then this will call
      logger.warn(msg, *args) at the end of the section."""
      self.msg99 = (logger, msg, args)


    def discard(self):
      """Discard this sample."""
      self.__discard = True


  def __init__(self, sample = None):
    UserDict.__init__(self)
    if sample:
        self.__sample = sample
    else:
        self.__sample = ExponentiallyDecayingReservoir()
    self.__timestamp = 0
    self.percentile99 = None
    self['count'] = 0


  def __getitem__(self, item):
    if item in self:
      return UserDict.__getitem__(self, item)
    else:
      return 0.0


  def addValue(self, value):
    """Updates the dictionary."""
    self['count'] += 1
    self.__sample.update(value)
    if time.time() > self.__timestamp + 20 and len(self.__sample) > 1:
      self.__timestamp = time.time()
      self['min'] = self.__sample.min
      self['max'] = self.__sample.max
      self['mean'] = self.__sample.mean
      self['stddev'] = self.__sample.stddev

      percentiles = self.__sample.percentiles([0.5, 0.75, 0.95, 0.98, 0.99, 0.999])
      self['median'] = percentiles[0]
      self['75percentile'] = percentiles[1]
      self['95percentile'] = percentiles[2]
      self['98percentile'] = percentiles[3]
      self['99percentile'] = percentiles[4]
      self.percentile99 = percentiles[4]
      self['999percentile'] = percentiles[5]


  def time(self):
    """Measure the time this section of code takes. For use in with statements."""
    return self.TimeManager(self)



class PmfStat(Stat):
  """A stat that stores min, max, mean, standard deviation, and some
  percentiles for arbitrary floating-point data. This is potentially a
  bit expensive, so its child values are only updated once every
  twenty seconds."""

  def __init__(self, name, _=None):
    Stat.__init__(self, name, None)


  def _getDefault(self, _):
    return PmfStatDict()


  def __set__(self, instance, value):
    self.__get__(instance, None).addValue(value)



class NamedPmfDict(UserDict):
  """Dictionary of strings."""

  def __init__(self):
    UserDict.__init__(self)


  def __getitem__(self, item):
    if item not in self:
      value = PmfStatDict()
      UserDict.__setitem__(self, item, value)
    return UserDict.__getitem__(self, item)


  def __setitem__(self, key, value):
    self[key].addValue(value)



class NamedPmfDictStat(Stat):
  """Dictionary stat value class.  Not compatible with aggregation at this time."""

  def _getDefault(self, _):
    return NamedPmfDict()



class StateTimeStatDict(UserDict):
  """Special dict that tracks time spent in current state."""

  def __init__(self, parent, instance):
    UserDict.__init__(self)
    self.parent = parent
    self.instance = instance


  def __getitem__(self, item):
    if item in self:
      value = UserDict.__getitem__(self, item)
    else:
      value = 0.0

    if item is not None and item == self.parent.state:
      return value + (time.time() - self.parent.time)
    else:
      return value


  def incr(self, item, value):
    """Increment a key by the given amount."""
    if item in self:
      old = UserDict.__getitem__(self, item)
    else:
      old = 0.0
    self[item] = old + value


  @contextmanager
  def acquire(self):
    """Assuming that the current state is an integer (it defaults to
    zero), increment it for the duration of the body of the with
    statement."""
    self.parent.__set__(self.instance, self.parent.state + 1)
    try:
      yield
    finally:
      self.parent.__set__(self.instance, self.parent.state - 1)



class StateTimeStat(Stat):
  """A stat that stores the amount of time spent in each of a finite
  number of discrete states. This can be used to track things like
  number of concurrent users, connection pool usage, how much time a
  finite state machine spends in each state, or anything like that. To
  use it, just set the stat to the new state every time the state
  changes."""

  def __init__(self, name, _=None):
    Stat.__init__(self, name, None)
    self.state = 0
    self.time = None


  def _getDefault(self, instance):
    return StateTimeStatDict(self, instance)


  def __set__(self, instance, value):
    if value == self.state:
      return
    histogram = self.__get__(instance, None)
    now = time.time()
    if self.time is not None:
      histogram.incr(self.state, now - self.time)
    self.state = value
    self.time = now



def filterCollapsedItems(data):
  """Return a filtered iteration over a list of items."""
  return ((key, value)\
          for key, value in six.iteritems(data) \
          if not (isinstance(value, StatContainer) and value.isCollapsed()))



class StatContainerEncoder(json.JSONEncoder):
  """JSON encoding that takes in to account collapsed stat containers and stat functions."""

  # pylint: disable=E0202
  def default(self, obj):
    if isinstance(obj, UserDict):
      return dict(filterCollapsedItems(obj.data))

    elif hasattr(obj, '__call__'):
      return obj()

    else:
      return json.JSONEncoder.default(self, obj)


def dumpStatsTo(filename):
  """Writes the stats dict to filanem"""
  with open(filename, 'w') as f:
    latest = getStats()
    latest['last-updated'] = time.time()
    json.dump(getStats(), f, cls=StatContainerEncoder)



def collection(path, *stats):
  """Creates a named stats collection object."""

  def initMethod(self):
    """Init method for the underlying stat object's class."""
    init(self, path)

  attributes = {'__init__': initMethod}
  for stat in stats:
    attributes[stat.getName()] = stat
  newClass = type('Stats:%s' % path, (object,), attributes)
  instance = newClass()
  for stat in stats:
    default = stat._getInit() # Consider this method package-protected. # pylint: disable=W0212
    if default:
      setattr(instance, stat.getName(), default)
  return instance
