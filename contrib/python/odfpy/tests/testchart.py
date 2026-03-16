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
from odf import chart

from odf.element import IllegalChild

class TestChartElements(unittest.TestCase):

    def testChart(self):
        """ Check chart doesn't allow 'Major' as class """
        chart.Chart( width="8cm", height="7cm", attributes={'class':'chart:circle'})
        self.assertRaises(ValueError, chart.Chart, attributes={'class':'Major'})

    def testLegend(self):
        chart.Legend()
        self.assertRaises(AttributeError, chart.Legend, attributes={'class':'Major'})
        chart.Legend(legendposition="end")
        self.assertRaises(ValueError, chart.Legend, legendposition="nowhere")

    def testNoClass(self):
        """ Check that 'name' is required """
        self.assertRaises(AttributeError, chart.Chart)

    def testGrid(self):
        self.assertRaises(ValueError, chart.Grid, attributes={'class':'chart:circle'})
        self.assertRaises(ValueError, chart.Grid, attributes={'class':'Major'})
        chart.Grid(attributes={'class':'major'})
        chart.Grid(attributes={'class':'minor'})

if __name__ == '__main__':
    unittest.main()
