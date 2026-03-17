#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2007-2010 Søren Roug, European Environment Agency
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
from odf.opendocument import OpenDocumentPresentation, OpenDocumentText
from odf import draw, style, table, text
from odf.element import IllegalChild
from odf.namespaces import TEXTNS
from .elementparser import ElementParser

class TestPresentations(unittest.TestCase):

    def assertContains(self, stack, needle):
        self.assertNotEqual(-1, stack.find(needle))

    def assertNotContains(self, stack, needle):
        self.assertEqual(-1, stack.find(needle))

    def testTable(self):
        """ Create a presentation with a page layout called MyLayout
        """
        presdoc = OpenDocumentPresentation()
        # We must describe the dimensions of the page
        pagelayout = style.PageLayout(name="MyLayout")
        presdoc.automaticstyles.addElement(pagelayout)
        pagelayout.addElement(style.PageLayoutProperties(margin="0cm", pagewidth="28cm", pageheight="21cm", printorientation="landscape"))

        # Every drawing page must have a master page assigned to it.
        masterpage = style.MasterPage(name="MyMaster", pagelayoutname=pagelayout)
        presdoc.masterstyles.addElement(masterpage)

        # Style for the title frame of the page
        # We set a centered 34pt font with yellowish background
        titlestyle = style.Style(name="MyMaster-title", family="presentation")
        titlestyle.addElement(style.ParagraphProperties(textalign="center"))
        titlestyle.addElement(style.TextProperties(fontsize="34pt"))
        titlestyle.addElement(style.GraphicProperties(fillcolor="#ffff99"))
        presdoc.styles.addElement(titlestyle)

        # Style for the photo frame
        mainstyle = style.Style(name="MyMaster-main", family="presentation")
        presdoc.styles.addElement(mainstyle)

        # Create style for drawing page
        dpstyle = style.Style(name="dp1", family="drawing-page")
        presdoc.automaticstyles.addElement(dpstyle)

        page = draw.Page(stylename=dpstyle, masterpagename=masterpage)
        presdoc.presentation.addElement(page)

        titleframe = draw.Frame(stylename=titlestyle, width="720pt", height="56pt", x="40pt", y="10pt")
        page.addElement(titleframe)
        textbox = draw.TextBox()
        titleframe.addElement(textbox)
        textbox.addElement(text.P(text="Presentation"))

        mainframe = draw.Frame(stylename=mainstyle, width="720pt", height="500pt", x="0pt", y="56pt")
        page.addElement(mainframe)
        mainframe.addElement(table.Table())


if __name__ == '__main__':
    unittest.main()
