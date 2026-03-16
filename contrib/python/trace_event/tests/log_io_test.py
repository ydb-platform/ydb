# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import tempfile
import unittest

from trace_event_impl.log import *
from trace_event_impl.parsed_trace_events import *


class LogIOTest(unittest.TestCase):
  def test_enable_with_file(self):
    file = tempfile.NamedTemporaryFile()
    trace_enable(open(file.name, 'w+b'))
    trace_disable()
    e = ParsedTraceEvents(trace_filename = file.name)
    file.close()
    self.assertTrue(len(e) > 0)

  def test_enable_with_filename(self):
    file = tempfile.NamedTemporaryFile()
    trace_enable(file.name)
    trace_disable()
    e = ParsedTraceEvents(trace_filename = file.name)
    file.close()
    self.assertTrue(len(e) > 0)

  def test_enable_with_implicit_filename(self):
    expected_filename = "{0!s}.json".format(sys.argv[0])
    def do_work():
      file = tempfile.NamedTemporaryFile()
      trace_enable()
      trace_disable()
      e = ParsedTraceEvents(trace_filename = expected_filename)
      file.close()
      self.assertTrue(len(e) > 0)
    try:
      do_work()
    finally:
      if os.path.exists(expected_filename):
        os.unlink(expected_filename)
