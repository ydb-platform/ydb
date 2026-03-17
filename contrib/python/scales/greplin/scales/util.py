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

"""Useful utility functions and objects."""

from six.moves.queue import Queue
from six import binary_type
from math import exp

import logging
import random
import socket
import threading
import time

log = logging.getLogger(__name__)


def lookup(source, keys, fallback = None):
  """Traverses the source, looking up each key.  Returns None if can't find anything instead of raising an exception."""
  try:
    for key in keys:
      source = source[key]
    return source
  except (KeyError, AttributeError, TypeError):
    return fallback



class GraphiteReporter(threading.Thread):
  """A graphite reporter thread."""

  def __init__(self, host, port, maxQueueSize=10000):
    """Connect to a Graphite server on host:port."""
    threading.Thread.__init__(self)

    self.host, self.port = host, port
    self.sock = None
    self.queue = Queue()
    self.maxQueueSize = maxQueueSize
    self.daemon = True


  def run(self):
    """Run the thread."""
    while True:
      try:
        try:
          name, value, valueType, stamp = self.queue.get()
        except TypeError:
          break
        self.log(name, value, valueType, stamp)
      finally:
        self.queue.task_done()


  def connect(self):
    """Connects to the Graphite server if not already connected."""
    if self.sock is not None:
      return
    backoff = 0.01
    while True:
      try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((self.host, self.port))
        self.sock = sock
        return
      except socket.error:
        time.sleep(random.uniform(0, 2.0*backoff))
        backoff = min(backoff*2.0, 5.0)


  def disconnect(self):
    """Disconnect from the Graphite server if connected."""
    if self.sock is not None:
      try:
        self.sock.close()
      except socket.error:
        pass
      finally:
        self.sock = None


  def _sendMsg(self, msg):
    """Send a line to graphite. Retry with exponential backoff."""
    if not self.sock:
      self.connect()
    if not isinstance(msg, binary_type):
      msg = msg.encode("UTF-8")

    backoff = 0.001
    while True:
      try:
        self.sock.sendall(msg)
        break
      except socket.error:
        log.warning('Graphite connection error', exc_info = True)
        self.disconnect()
        time.sleep(random.uniform(0, 2.0*backoff))
        backoff = min(backoff*2.0, 5.0)
        self.connect()


  def _sanitizeName(self, name):
    """Sanitize a metric name."""
    return name.replace(' ', '-')


  def log(self, name, value, valueType=None, stamp=None):
    """Log a named numeric value. The value type may be 'value',
    'count', or None."""
    if type(value) == float:
      form = "%s%s %2.2f %d\n"
    else:
      form = "%s%s %s %d\n"

    if valueType is not None and len(valueType) > 0 and valueType[0] != '.':
      valueType = '.' + valueType

    if not stamp:
      stamp = time.time()

    self._sendMsg(form % (self._sanitizeName(name), valueType or '', value, stamp))


  def enqueue(self, name, value, valueType=None, stamp=None):
    """Enqueue a call to log."""
    # If queue is too large, refuse to log.
    if self.maxQueueSize and self.queue.qsize() > self.maxQueueSize:
      return
    # Stick arguments into the queue
    self.queue.put((name, value, valueType, stamp))


  def flush(self):
    """Block until all stats have been sent to Graphite."""
    self.queue.join()


  def shutdown(self):
    """Shut down the background thread."""
    self.queue.put(None)
    self.flush()



class AtomicValue(object):
  """Stores a value, atomically."""

  def __init__(self, val):
    self.lock = threading.RLock()
    self.value = val


  def update(self, function):
    """Atomically apply function to the value, and return the old and new values."""
    with self.lock:
      oldValue = self.value
      self.value = function(oldValue)
      return oldValue, self.value


  def getAndSet(self, newVal):
    """Sets a new value while returning the old value"""
    return self.update(lambda _: newVal)[0]


  def addAndGet(self, val):
    """Adds val to the value and returns the result"""
    return self.update(lambda x: x + val)[1]



class EWMA(object):
  """
  An exponentially-weighted moving average.

  Ported from Yammer metrics.
  """

  M1_ALPHA = 1 - exp(-5 / 60.0)
  M5_ALPHA = 1 - exp(-5 / 60.0 / 5)
  M15_ALPHA = 1 - exp(-5 / 60.0 / 15)

  TICK_RATE = 5 # Every 5 seconds


  @classmethod
  def oneMinute(cls):
    """Creates an EWMA configured for a 1 min decay with a 5s tick"""
    return EWMA(cls.M1_ALPHA, 5)


  @classmethod
  def fiveMinute(cls):
    """Creates an EWMA configured for a 5 min decay with a 5s tick"""
    return EWMA(cls.M5_ALPHA, 5)


  @classmethod
  def fifteenMinute(cls):
    """Creates an EWMA configured for a 15 min decay with a 5s tick"""
    return EWMA(cls.M15_ALPHA, 5)


  def __init__(self, alpha, interval):
    self.alpha = alpha
    self.interval = interval
    self.rate = 0
    self._uncounted = AtomicValue(0)
    self._initialized = False


  def update(self, val):
    """Adds this value to the count to be averaged"""
    self._uncounted.addAndGet(val)


  def tick(self):
    """Updates rates and decays"""
    count = self._uncounted.getAndSet(0)
    instantRate = float(count) / self.interval

    if self._initialized:
      self.rate += (self.alpha * (instantRate - self.rate))
    else:
      self.rate = instantRate
      self._initialized = True
