# Copyright 2012 The scales Authors.
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

"""Tests for the util module."""

from greplin.scales import util

import unittest



class AtomicValueTest(unittest.TestCase):
  """Tests for atomic values."""

  def testUpdate(self):
    """Test update functions."""
    v = util.AtomicValue('hello, world')
    self.assertEqual(v.update(len), ('hello, world', len('hello, world')))
    self.assertEqual(v.value, len('hello, world'))


  def testGetAndSet(self):
    """Test get-and-set."""
    v = util.AtomicValue(42)
    self.assertEqual(v.getAndSet(666), 42)
    self.assertEqual(v.value, 666)


  def testAddAndGet(self):
    """Test add-and-get."""
    v = util.AtomicValue(42)
    self.assertEqual(v.addAndGet(8), 50)
    self.assertEqual(v.value, 50)
