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

import unittest, os, os.path
from odf.opendocument import OpenDocumentSpreadsheet, OpenDocumentChart, load
from odf.style import Style, ParagraphProperties, TextProperties, GraphicProperties, \
   ChartProperties
from odf.number import Text,PercentageStyle, Number
from odf.table import Table,TableRow,TableCell
from odf import text, chart


class TestDatastyles(unittest.TestCase):

    saved = False

    def tearDown(self):
        if self.saved:
            os.unlink("TEST.ods")

    def test_percentage(self):
        """ Test that an automatic style can refer to a PercentageStyle as a datastylename """
        doc = OpenDocumentSpreadsheet()
        nonze = PercentageStyle(name='N11')
        nonze.addElement(Number(decimalplaces='2', minintegerdigits='1'))
        nonze.addElement(Text(text='%'))
        doc.automaticstyles.addElement(nonze)
        pourcent = Style(name='pourcent', family='table-cell', datastylename='N11')
        pourcent.addElement(ParagraphProperties(textalign='center'))
        pourcent.addElement(TextProperties(attributes={'fontsize':"10pt",'fontweight':"bold", 'color':"#000000" }))
        doc.automaticstyles.addElement(pourcent)

        table = Table(name='sheet1')
        tr = TableRow()
        tc = TableCell(formula='=AVERAGE(C4:CB62)/2',stylename='pourcent', valuetype='percentage')
        tr.addElement(tc)
        table.addElement(tr)
        doc.spreadsheet.addElement(table)
        doc.save(u"TEST.ods")
        self.saved = True
        d = load(u"TEST.ods")
        result = d.contentxml() # contentxml is supposed to yeld a bytes
        self.assertNotEqual(-1, result.find(b'''<number:percentage-style'''))
        self.assertNotEqual(-1, result.find(b'''style:data-style-name="N11"'''))
        self.assertNotEqual(-1, result.find(b'''style:name="pourcent"'''))

    def test_chart_style(self):
        """ Test that chart:style-name reference is seen in content.xml """
        doc = OpenDocumentChart()
        chartstyle  = Style(name="chartstyle", family="chart")
        chartstyle.addElement( GraphicProperties(stroke="none", fillcolor="#ffffff"))
        doc.automaticstyles.addElement(chartstyle)

        mychart = chart.Chart( width="576pt", height="504pt", stylename=chartstyle, attributes={'class':'chart:bar'})
        doc.chart.addElement(mychart)

        # Add title
        titlestyle = Style(name="titlestyle", family="chart")
        titlestyle.addElement( GraphicProperties(stroke="none", fill="none"))
        titlestyle.addElement( TextProperties(fontfamily="'Nimbus Sans L'",
                fontfamilygeneric="swiss", fontpitch="variable", fontsize="13pt"))
        doc.automaticstyles.addElement(titlestyle)

        mytitle = chart.Title(x="385pt", y="27pt", stylename=titlestyle)
        mytitle.addElement( text.P(text="Title"))
        mychart.addElement(mytitle)

        # Add subtitle
        subtitlestyle = Style(name="subtitlestyle", family="chart")
        subtitlestyle.addElement( GraphicProperties(stroke="none", fill="none"))
        subtitlestyle.addElement( TextProperties(fontfamily="'Nimbus Sans L'",
                fontfamilygeneric="swiss", fontpitch="variable", fontsize="10pt"))
        doc.automaticstyles.addElement(subtitlestyle)

        subtitle = chart.Subtitle(x="0pt", y="123pt", stylename=subtitlestyle)
        subtitle.addElement( text.P(text="my subtitle"))
        mychart.addElement(subtitle)

        # Legend
        legendstyle = Style(name="legendstyle", family="chart")
        legendstyle.addElement( GraphicProperties(fill="none"))
        legendstyle.addElement( TextProperties(fontfamily="'Nimbus Sans L'",
                fontfamilygeneric="swiss", fontpitch="variable", fontsize="6pt"))
        doc.automaticstyles.addElement(legendstyle)

        mylegend = chart.Legend(legendposition="end", legendalign="center", stylename=legendstyle)
        mychart.addElement(mylegend)

        # Plot area
        plotstyle = Style(name="plotstyle", family="chart")
        plotstyle.addElement( ChartProperties(seriessource="columns",
                percentage="false", stacked="false",
                threedimensional="true"))
        doc.automaticstyles.addElement(plotstyle)

        plotarea = chart.PlotArea(datasourcehaslabels="both", stylename=plotstyle)
        mychart.addElement(plotarea)

        # Style for the X,Y axes
        axisstyle = Style(name="axisstyle", family="chart")
        axisstyle.addElement( ChartProperties(displaylabel="true"))
        doc.automaticstyles.addElement(axisstyle)

        # Title for the X axis
        xaxis = chart.Axis(dimension="x", name="primary-x", stylename=axisstyle)
        plotarea.addElement(xaxis)
        xt = chart.Title()
        xaxis.addElement(xt)
        xt.addElement(text.P(text="x_axis"))

        # Title for the Y axis
        yaxis = chart.Axis(dimension="y", name="primary-y", stylename=axisstyle)
        plotarea.addElement(yaxis)
        yt = chart.Title()
        yaxis.addElement(yt)
        yt.addElement(text.P(text="y_axis"))

        result = doc.contentxml() # contentxml() is supposed to yeld a bytes
        self.assertNotEqual(-1, result.find(b'''style:family="chart"'''))

if __name__ == '__main__':
    unittest.main()

