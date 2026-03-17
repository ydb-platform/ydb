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
from odf import dr3d, draw, office, style

from odf.element import IllegalChild


class TestStyleRefs(unittest.TestCase):

    def testTxtStylename(self):
        """ Check that 'name' is required """
        self.assertRaises(AttributeError, dr3d.Cube, stylename="Bold")

    def testBadFamily(self):
        """ Family must be graphic or presentation """
        boldstyle = style.Style(name='Bold', family="paragraph")
        self.assertRaises(ValueError, dr3d.Cube,stylename=boldstyle)
        self.assertRaises(ValueError, dr3d.Cube, stylename=boldstyle)
        self.assertRaises(ValueError, dr3d.Extrude, stylename=boldstyle, d="x", viewbox="0 0 1000 1000")
        self.assertRaises(ValueError, dr3d.Rotate, stylename=boldstyle, d="x", viewbox="0 0 1000 1000")
        self.assertRaises(ValueError, dr3d.Scene, stylename=boldstyle)
        self.assertRaises(ValueError, dr3d.Sphere, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Caption, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Circle, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Connector, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Control, stylename=boldstyle, control="x")
        self.assertRaises(ValueError, draw.CustomShape, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Ellipse, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Frame, stylename=boldstyle)
        self.assertRaises(ValueError, draw.G, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Line, stylename=boldstyle, x1="0", y1="0", x2="1", y2="1")
        self.assertRaises(ValueError, draw.Measure, stylename=boldstyle, x1="0", y1="0", x2="1", y2="1")
        self.assertRaises(ValueError, draw.PageThumbnail, stylename=boldstyle)
        self.assertRaises(ValueError, draw.Path, stylename=boldstyle, d="x", viewbox="0 0 1000 1000")
        self.assertRaises(ValueError, draw.Polygon, stylename=boldstyle, points=((0,0),(1,1)), viewbox="0 0 1000 1000")
        self.assertRaises(ValueError, draw.Polygon, stylename=boldstyle, points="0,0 1,1", viewbox="0 0 1000 1000")
        self.assertRaises(ValueError, draw.Polyline, stylename=boldstyle, points="0,0 1,1", viewbox="0 0 1000 1000")
        self.assertRaises(ValueError, draw.Rect, stylename=boldstyle)
        self.assertRaises(ValueError, draw.RegularPolygon, stylename=boldstyle, corners="x")
        self.assertRaises(ValueError, office.Annotation, stylename=boldstyle)

    def testCalls(self):
        """ Simple calls """
        for family in ("graphic","presentation"):
            boldstyle = style.Style(name='Bold', family=family)
            dr3d.Cube(stylename=boldstyle)
            dr3d.Extrude(stylename=boldstyle, d="x", viewbox="0 0 1000 1000")
            dr3d.Rotate(stylename=boldstyle, d="x", viewbox="0 0 1000 1000")
            dr3d.Scene(stylename=boldstyle)
            dr3d.Sphere(stylename=boldstyle)
            draw.Caption(stylename=boldstyle)
            draw.Circle(stylename=boldstyle)
            draw.Connector(stylename=boldstyle, viewbox="0 0 1000 1000")
            draw.Control(stylename=boldstyle, control="x")
            draw.CustomShape(stylename=boldstyle)
            draw.Ellipse(stylename=boldstyle)
            draw.Frame(stylename=boldstyle)
            draw.G(stylename=boldstyle)
            draw.Line(stylename=boldstyle, x1="0%", y1="0%", x2="100%", y2="100%")
            draw.Measure(stylename=boldstyle, x1="0cm", y1="0cm", x2="100%", y2="100%")
            draw.PageThumbnail(stylename=boldstyle)
            draw.Path(stylename=boldstyle, d="x", viewbox="0 0 1000 1000")
            draw.Polygon(stylename=boldstyle, points=((0,0),(1,1)), viewbox="0 0 1000 1000")
            draw.Polygon(stylename=boldstyle, points="0,0 1,1", viewbox="0 0 1000 1000")
            draw.Polyline(stylename=boldstyle, points="0,0 1,1", viewbox="0 0 1000 1000")
            draw.Rect(stylename=boldstyle)
            draw.RegularPolygon(stylename=boldstyle, corners="x")
            office.Annotation(stylename=boldstyle)

if __name__ == '__main__':
    unittest.main()
