#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2009 Søren Roug, European Environment Agency
#
# This is free software.  You may redistribute it under the terms
# of the Apache license and the GNU General Public License Version
# 2 or at your option any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Contributor(s):
#

import unittest
from odf import grammar, attrconverters
from odf.namespaces import OFFICENS

def findconv(attribute, element):
    converter = attrconverters.attrconverters.get((attribute,element), None)
    if converter is not None:
        return attribute
    else:
        converter = attrconverters.attrconverters.get((attribute, None), None)
        if converter is not None:
            return attribute
    return ""


class TestConverters(unittest.TestCase):
    allattrs = {}
    allqattrs = {}

    def testConverters(self):
        """ Check that there are converters for all attributes and vice versa"""
        for element,attrs in grammar.allowed_attributes.items():
            if attrs:
                for attr in attrs:
                    self.allattrs[attr] = 1
                    self.allqattrs[(attr, element)] = 1
                    self.assertEqual(attr, findconv(attr, element))
        for (attr,elem) in attrconverters.attrconverters.keys():
            if attr == (OFFICENS,u'process-content'):  # Special attribute
                continue
            if elem is None:
                self.assertEqual(self.allattrs[attr], 1)
            else:
                self.assertEqual(self.allqattrs[(attr, elem)], 1)

    def testBooleanConverter(self):
        """ Check that the boolean converter understands the values """
        self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", 'false', None), 'false')
        self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", 'true', None), 'true')
        self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", True, None), 'true')
        self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", False, None), 'false')
        self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", 1, None), 'true')
        self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", 0, None), 'false')
        self.assertRaises(ValueError, attrconverters.cnv_boolean, "usesoftpagebreak", '', None)
        self.assertRaises(ValueError, attrconverters.cnv_boolean, "usesoftpagebreak", 'on', None)
        self.assertRaises(ValueError, attrconverters.cnv_boolean, "usesoftpagebreak", None, None)
#       self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", '', None), 'false')
#       self.assertEqual(attrconverters.cnv_boolean("usesoftpagebreak", 'on', None), 'true')


if __name__ == '__main__':
    unittest.main()
