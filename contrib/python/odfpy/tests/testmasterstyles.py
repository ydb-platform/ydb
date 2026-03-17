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
from odf import style, text
from odf.table import Table, TableColumn, TableRow, TableCell
from odf.element import IllegalChild
from odf.namespaces import TEXTNS
from .elementparser import ElementParser

class TestMasterStyles(unittest.TestCase):

    def assertContains(self, stack, needle):
        self.assertNotEqual(-1, stack.find(needle))

    def assertNotContains(self, stack, needle):
        self.assertEqual(-1, stack.find(needle))

    def testStyle(self):
        """ Create a presentation with a page layout called MyLayout
            Add a presentation style for the title
            Check that MyLayout is listed in styles.xml
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

        s = presdoc.stylesxml()
        self.assertContains(s, '<style:page-layout style:name="MyLayout"><style:page-layout-properties ')
        e = ElementParser(s,'style:page-layout-properties')
        self.assertEqual(e.element,'style:page-layout-properties')
        self.assertTrue(e.has_value("fo:margin","0cm"))
        self.assertTrue(e.has_value("fo:page-width","28cm"))
        self.assertTrue(e.has_value("fo:page-height","21cm"))
        self.assertTrue(e.has_value("style:print-orientation","landscape"))

        e = ElementParser(s,'style:style')
        self.assertTrue(e.has_value("style:name","MyMaster-title"))
        self.assertTrue(e.has_value("style:display-name","MyMaster-title"))
        self.assertTrue(e.has_value("style:family","presentation"))

        self.assertContains(s, '<style:paragraph-properties fo:text-align="center"/><style:text-properties fo:font-size="34pt"/><style:graphic-properties draw:fill-color="#ffff99"/></style:style></office:styles>')
        e = ElementParser(s,'style:master-page')
        self.assertTrue(e.has_value("style:name","MyMaster"))
        self.assertTrue(e.has_value("style:display-name","MyMaster"))
        self.assertTrue(e.has_value("style:page-layout-name","MyLayout"))

    def testMasterWithHeader(self):
        """ Create a text document with a page layout called "pagelayout"
            Add a master page
            Check that pagelayout is listed in styles.xml
        """
        textdoc = OpenDocumentText()
        pl = style.PageLayout(name="pagelayout")
        textdoc.automaticstyles.addElement(pl)
        mp = style.MasterPage(name="Standard", pagelayoutname=pl)
        textdoc.masterstyles.addElement(mp)
        h = style.Header()
        hp = text.P(text="header try")
        h.addElement(hp)
        mp.addElement(h)
        s = textdoc.stylesxml()
        self.assertContains(s, u'<office:automatic-styles><style:page-layout style:name="pagelayout"/></office:automatic-styles>')

    def testAutomaticStyles(self):
        """ Create a text document with a page layout called "pagelayout"
            Add a master page
            Add an automatic style for the heading
            Check that pagelayout is listed in styles.xml under automatic-styles
            Check that the heading style is NOT listed in styles.xml
            Check that the pagelayout is NOT listed in content.xml
        """
        textdoc = OpenDocumentText()

        parastyle = style.Style(name="Para", family="paragraph")
        parastyle.addElement(style.ParagraphProperties(numberlines="false", linenumber="0"))
        parastyle.addElement(style.TextProperties(fontsize="24pt", fontweight="bold"))
        textdoc.automaticstyles.addElement(parastyle)

        hpstyle = style.Style(name="HeaderPara", family="paragraph")
        hpstyle.addElement(style.ParagraphProperties(linenumber="0"))
        hpstyle.addElement(style.TextProperties(fontsize="18pt", fontstyle="italic"))
        textdoc.automaticstyles.addElement(hpstyle)

        pl = style.PageLayout(name="pagelayout")
        textdoc.automaticstyles.addElement(pl)

        mp = style.MasterPage(name="Standard", pagelayoutname=pl)
        textdoc.masterstyles.addElement(mp)
        h = style.Header()
        hp = text.P(text="header content", stylename=hpstyle)
        h.addElement(hp)
        mp.addElement(h)

        textdoc.text.addElement(text.P(text="Paragraph 1", stylename=parastyle))

        # Check styles.xml
        s = textdoc.stylesxml()
        self.assertContains(s, u'<style:page-layout style:name="pagelayout"/>')
        self.assertContains(s, u'style:name="HeaderPara"')
        self.assertNotContains(s, u'style:name="Para" ')
        # Check content.xml
        s = textdoc.contentxml() # contentxml is supposed to yed a byts
        self.assertNotContains(s, b'<style:page-layout style:name="pagelayout"/>')
        self.assertContains(s, b'style:name="Para"')


if __name__ == '__main__':
    unittest.main()
