# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import math

from .trace_test import *


class SingleThreadTest(TraceTest):
  def test_one_func(self):
    actual_diff = []
    def func1():
      trace_begin("func1")
      start = time.time()
      time.sleep(0.25)
      end = time.time()
      actual_diff.append(end-start) # Pass via array because of Python scoping
      trace_end("func1")

    res = self.go(func1)
    tids = res.findThreadIds()
    self.assertEqual(1, len(tids))
    events = res.findEventsOnThread(tids[0])
    self.assertEqual(2, len(events))
    self.assertEqual('B', events[0]["ph"])
    self.assertEqual('E', events[1]["ph"])
    measured_diff = events[1]["ts"] - events[0]["ts"]
    actual_diff = 1000000 * actual_diff[0]
    self.assertTrue(math.fabs(actual_diff - measured_diff) < 1000)

  def test_redundant_flush(self):
    def func1():
      trace_begin("func1")
      time.sleep(0.25)
      trace_flush()
      trace_flush()
      trace_end("func1")

    res = self.go(func1)
    events = res.findEventsOnThread(res.findThreadIds()[0])
    self.assertEqual(2, len(events))
    self.assertEqual('B', events[0]["ph"])
    self.assertEqual('E', events[1]["ph"])

  def test_nested_func(self):
    def func1():
      trace_begin("func1")
      time.sleep(0.25)
      func2()
      trace_end("func1")

    def func2():
      trace_begin("func2")
      time.sleep(0.05)
      trace_end("func2")

    res = self.go(func1)
    self.assertEqual(1, len(res.findThreadIds()))

    tids = res.findThreadIds()
    self.assertEqual(1, len(tids))
    events = res.findEventsOnThread(tids[0])
    efmt = ["{0!s} {1!s}".format(e["ph"], e["name"]) for e in events]
    self.assertEqual(
      ["B func1",
       "B func2",
       "E func2",
       "E func1"],
      efmt);

