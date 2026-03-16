#! /usr/bin/env python

"""Basic tests to ensure pyjavaproperties behaves like java.util.Properties.

Created - Pepper Lebeck-Jobe (eljobe@gmail.com)
"""

import os
import unittest

from pyjavaproperties import Properties


class PyJavaPropertiesTest(unittest.TestCase):
  """Tests pyjavaproperties complies to java.util.Properties contract."""

  def setUp(self):
    import yatest.common as yc
    test_dir = os.path.join(yc.source_path(os.path.dirname(__file__)), 'tests', 'testdata')
    self.properties_file = os.path.join(test_dir, 'complex.properties')

  def testParsePropertiesInput(self):
    properties = Properties()
    properties.load(open(self.properties_file))
    self.assertEqual(25, len(properties.items()))
    self.assertEqual('Value00', properties['Key00'])
    self.assertEqual('Value01', properties['Key01'])
    self.assertEqual('Value02', properties['Key02'])
    self.assertEqual('Value03', properties['Key03'])
    self.assertEqual('Value04', properties['Key04'])
    self.assertEqual('Value05a, Value05b, Value05c', properties['Key05'])
    self.assertEqual('Value06a, Value06b, Value06c', properties['Key06'])
    self.assertEqual('Value07b', properties['Key07'])
    self.assertEqual(
        'Value08a, Value08b, Value08c, Value08d, Value08e, Value08f',
        properties['Key08'])
    self.assertEqual(
        'Value09a, Value09b, Value09c, Value09d, Value09e, Value09f',
        properties['Key09'])
    self.assertEqual('Value10', properties['Key10'])
    self.assertEqual('', properties['Key11'])
    self.assertEqual('Value12a, Value12b, Value12c', properties['Key12'])
    self.assertEqual('Value13 With Spaces', properties['Key13'])
    self.assertEqual('Value14 With Spaces', properties['Key14'])
    self.assertEqual('Value15 With Spaces', properties['Key15'])
    self.assertEqual('Value16', properties['Key16 With Spaces'])
    self.assertEqual('Value17', properties['Key17 With Spaces'])
    self.assertEqual('Value18 # Not a comment.', properties['Key18'])
    self.assertEqual('Value19 ! Not a comment.', properties['Key19'])
    # Single referenced property    
    self.assertEqual('Value19 ! Not a comment.Value20', properties['Key20'])    
    self.assertEqual('Value21', properties['Key21=WithEquals'])
    self.assertEqual('Value22', properties['Key22:WithColon'])
    self.assertEqual('Value23', properties['Key23'])
    # Multiple referenced properties with separator
    self.assertEqual('Value18 # Not a comment.-separator-Value19 ! Not a comment.', properties['Key24'])
    properties.store(open('saved.properties','w'))
    
if __name__ == '__main__':
  unittest.main()
