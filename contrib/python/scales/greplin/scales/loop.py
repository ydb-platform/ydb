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

"""Twisted specific stats utilities."""

from greplin import scales

from twisted.internet import reactor


def installStatsLoop(statsFile, statsDelay):
  """Installs an interval loop that dumps stats to a file."""

  def dumpStats():
    """Actual stats dump function."""
    scales.dumpStatsTo(statsFile)
    reactor.callLater(statsDelay, dumpStats)

  def startStats():
    """Starts the stats dump in "statsDelay" seconds."""
    reactor.callLater(statsDelay, dumpStats)

  reactor.callWhenRunning(startStats)
