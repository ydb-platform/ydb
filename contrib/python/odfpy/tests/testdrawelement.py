#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2007 Søren Roug, European Environment Agency
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
from odf import draw, svg

from odf.element import IllegalChild

class TestDrawElements(unittest.TestCase):

    def testNoName(self):
        """ Check that 'name' is required """
        self.assertRaises(AttributeError, draw.FillImage, href="y")

    def testFloatingPoints(self):
        """ Points that are floating point are not allowed """
        self.assertRaises(ValueError, draw.Polygon, points='0.0000,0.0000 -30.9017,95.1057', viewbox="-1000 -1000 1000 1000")
        self.assertRaises(ValueError, draw.Polygon, points='0.0000,0.0000 -30,95.7', viewbox="-1000 -1000 1000 1000")

    def testViewbox(self):
        """ ViewBox can only have four integer values """
        self.assertRaises(ValueError, draw.Polygon, points='0,0 -30,957', viewbox="-1000.0 -1000.0 1000 1000")
        self.assertRaises(ValueError, draw.Polygon, points='0,0 -30,957', viewbox="-1000 -1000 1000 1000.1")
        self.assertRaises(ValueError, draw.Polygon, points='0,0 -30,957', viewbox="-1000 -1000 1000")

    def testIntPoints(self):
        """ Points can only be integers """
        draw.Polygon(points='0,0 -31,95', viewbox="-1000 -1000 1000 1000")

    def testCalls(self):
        """ Simple calls """
        self.assertRaises(TypeError, draw.FillImage, "x", href="y")
        draw.FillImage(name="x", href="y", type="simple")
        self.assertRaises(TypeError, draw.Gradient, "x", style="y")
        self.assertRaises(TypeError, draw.Hatch, "x", style="y")
        self.assertRaises(TypeError, draw.Marker, "x",d="y",viewbox="0 0 100 100")
        self.assertRaises(TypeError, draw.Opacity, "x", style="y")
        self.assertRaises(TypeError, draw.StrokeDash, "x")
        self.assertRaises(TypeError, svg.Lineargradient, "x")
        self.assertRaises(TypeError, svg.Radialgradient, "x")

if __name__ == '__main__':
    unittest.main()
