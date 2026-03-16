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

"""Tools for pushing stat values to graphite."""

from greplin import scales
from greplin.scales import util

import os
import threading
import logging
import time
from fnmatch import fnmatch
from socket import gethostname

import six

log = logging.getLogger(__name__)


class GraphitePusher(object):
  """A class that pushes all stat values to Graphite on-demand."""

  def __init__(self, host, port, prefix=None):
    """If prefix is given, it will be prepended to all Graphite
    stats. If it is not given, then a prefix will be derived from the
    hostname."""
    self.rules = []
    self.pruneRules = []

    self.prefix = prefix or gethostname().lower()

    if self.prefix and self.prefix[-1] != '.':
      self.prefix += '.'

    self.graphite = util.GraphiteReporter(host, port)


  def _sanitize(self, name):
    """Sanitize a name for graphite."""
    return name.strip().replace(' ', '-').replace('.', '-').replace('/', '_')


  def _forbidden(self, path, value):
    """Is a stat forbidden? Goes through the rules to find one that
    applies. Chronologically newer rules are higher-precedence than
    older ones. If no rule applies, the stat is forbidden by default."""
    if path[0] == '/':
      path = path[1:]
    for rule in reversed(self.rules):
      if isinstance(rule[1], six.string_types):
        if fnmatch(path, rule[1]):
          return not rule[0]
      elif rule[1](path, value):
        return not rule[0]
    return True # do not log by default


  def _pruned(self, path):
    """Is a stat tree node pruned?  Goes through the list of prune rules
    to find one that applies.  Chronologically newer rules are
    higher-precedence than older ones. If no rule applies, the stat is
    not pruned by default."""
    if path[0] == '/':
      path = path[1:]
    for rule in reversed(self.pruneRules):
      if isinstance(rule, six.string_types):
        if fnmatch(path, rule):
          return True
      elif rule(path):
        return True
    return False # Do not prune by default


  def push(self, statsDict=None, prefix=None, path=None):
    """Push stat values out to Graphite."""
    if statsDict is None:
      statsDict = scales.getStats()
    prefix = prefix or self.prefix
    path = path or '/'

    for name, value in list(statsDict.items()):
      name = str(name)
      subpath = os.path.join(path, name)

      if self._pruned(subpath):
        continue

      if hasattr(value, '__call__'):
        try:
          value = value()
        except:                       # pylint: disable=W0702
          value = None
          log.exception('Error when calling stat function for graphite push')

      if hasattr(value, 'items'):
        self.push(value, '%s%s.' % (prefix, self._sanitize(name)), subpath)
      elif self._forbidden(subpath, value):
        continue

      if six.PY3:
        type_values = (int, float)
      else:
        type_values = (int, long, float)

      if type(value) in type_values and len(name) < 500:
        self.graphite.log(prefix + self._sanitize(name), value)


  def _addRule(self, isWhitelist, rule):
    """Add an (isWhitelist, rule) pair to the rule list."""
    if isinstance(rule, six.string_types) or hasattr(rule, '__call__'):
      self.rules.append((isWhitelist, rule))
    else:
      raise TypeError('Graphite logging rules must be glob pattern or callable. Invalid: %r' % rule)


  def allow(self, rule):
    """Append a whitelisting rule to the chain. The rule is either a function (called
    with the stat name and its value, returns True if it matches), or a Bash-style
    wildcard pattern, such as 'foo.*.bar'."""
    self._addRule(True, rule)


  def forbid(self, rule):
    """Append a blacklisting rule to the chain. The rule is either a function (called
    with the stat name and its value, returns True if it matches), or a Bash-style
    wildcard pattern, such as 'foo.*.bar'."""
    self._addRule(False, rule)


  def prune(self, rule):
    """Append a rule that stops traversal at a branch node."""
    self.pruneRules.append(rule)



class GraphitePeriodicPusher(threading.Thread, GraphitePusher):
  """A thread that periodically pushes all stat values to Graphite."""

  def __init__(self, host, port, prefix=None, period=60):
    """If prefix is given, it will be prepended to all Graphite
    stats. If it is not given, then a prefix will be derived from the
    hostname."""
    GraphitePusher.__init__(self, host, port, prefix)
    threading.Thread.__init__(self)
    self.daemon = True

    self.period = period


  def run(self):
    """Loop forever, pushing out stats."""
    self.graphite.start()
    while True:
      log.debug('Graphite pusher is sleeping for %d seconds', self.period)
      time.sleep(self.period)
      log.debug('Pushing stats to Graphite')
      try:
        self.push()
        log.debug('Done pushing stats to Graphite')
      except:
        log.exception('Exception while pushing stats to Graphite')
        raise
