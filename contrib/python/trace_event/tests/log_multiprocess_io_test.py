# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import multiprocessing

from .trace_test import *


class LogMultipleProcessIOTest(TraceTest):
  def setUp(self):
    self.proc = None

  def tearDown(self):
    if self.proc is not None:
      self.proc.kill()

  # Test that starting a subprocess to record into an existing tracefile works.
  def test_one_subprocess(self):
    def test():
      trace_begin("parent")
      self.proc = multiprocessing.Process(target=self._test_one_subprocess_child)
      self.proc.start()
      self.proc.join()
      self.proc = None
      trace_end("parent")
    res = self.go(test)
    parent_events = res.findByName('parent')
    child_events = res.findByName('child')
    self.assertEqual(2, len(parent_events))
    self.assertEqual(2, len(child_events))

  def _test_one_subprocess_child(self):
    trace_begin("child")
    time.sleep(0.2)
    trace_end("child")

if __name__ == "__main__":
  if len(sys.argv) != 3:
    raise Exception("Expected: method name")
  trace_enable(sys.argv[1])
  t = LogMultipleProcessIOTest(sys.argv[2])
  t.run()
  trace_disable()
