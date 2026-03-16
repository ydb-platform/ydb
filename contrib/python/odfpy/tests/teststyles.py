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

import unittest, sys
from odf.opendocument import OpenDocumentText
from odf import style, text
from odf.table import Table, TableColumn, TableRow, TableCell
from odf.element import IllegalChild
from odf.namespaces import TEXTNS
from .elementparser import ElementParser

class TestStyles(unittest.TestCase):

    def test_style(self):
        """ Get a common style with getStyleByName """
        textdoc = OpenDocumentText()
        tablecontents = style.Style(name=u"Table Contents", family=u"paragraph")
        tablecontents.addElement(style.ParagraphProperties(numberlines=u"false", linenumber=u"0"))
        textdoc.styles.addElement(tablecontents)
        s = textdoc.getStyleByName(u'Table Contents')
        self.assertEqual((u'urn:oasis:names:tc:opendocument:xmlns:style:1.0', 'style'), s.qname)


    def test_style(self):
        """ Get an automatic style with getStyleByName """
        textdoc = OpenDocumentText()
        boldstyle = style.Style(name=u'Bold', family=u"text")
        boldstyle.addElement(style.TextProperties(fontweight=u"bold"))
        textdoc.automaticstyles.addElement(boldstyle)
        s = textdoc.getStyleByName(u'Bold')
        self.assertEqual((u'urn:oasis:names:tc:opendocument:xmlns:style:1.0', 'style'), s.qname)

    def testStyleFail(self):
        """ Verify that 'xname' attribute is not legal """
        self.assertRaises(AttributeError, style.Style, xname=u'Table Contents')

    def testBadChild(self):
        """ Test that you can't add an illegal child """
        tablecontents = style.Style(name=u"Table Contents", family=u"paragraph")
        p = text.P(text=u"x")
        with self.assertRaises(Exception) as cm:
            tablecontents.addElement(p)
        # FIXME: This doesn't work on Python 3.
        if sys.version_info[0]==2:
            self.assertTrue(isinstance(cm.exception, IllegalChild))

    def testTextStyleName(self):
        """ Test that you can use the name of the style in references """
        boldstyle = style.Style(name=u"Bold",family=u"text")
        boldstyle.addElement(style.TextProperties(attributes={u'fontweight':u"bold"}))
        text.Span(stylename=u"Bold",text=u"This part is bold. ")

    def testBadFamily(self):
        """ Test that odfpy verifies 'family' argument """
        self.assertRaises(ValueError, style.Style, name=u"Bold",family=u"incorrect")

class TestQattributes(unittest.TestCase):

    def testAttribute(self):
        """ Test that you can add a normal attributes using 'qattributes' """
        standard = style.Style(name=u"Standard", family=u"paragraph")
        p = style.ParagraphProperties(qattributes={(TEXTNS,u'enable-numbering'):'true'})
        standard.addElement(p)

    def testAttributeForeign(self):
        """ Test that you can add foreign attributes """
        textdoc = OpenDocumentText()
        standard = style.Style(name=u"Standard", family=u"paragraph")
        p = style.ParagraphProperties(qattributes={(u'http://foreignuri.com','enable-numbering'):u'true'})
        standard.addElement(p)
        textdoc.styles.addElement(standard)
        s = textdoc.stylesxml()
        s.index(u"""<?xml version='1.0' encoding='UTF-8'?>\n""")

        # XXX: Python 2 and 3 have these entities inserted with slightly different numbers, try a broader substring
        # XXX: Original test code: https://github.com/eea/odfpy/blob/master/tests/teststyles.py#L91-L92
        s.index(u'"http://foreignuri.com"')
        s.index(u'enable-numbering="true"')

        e = ElementParser(s,u'style:style')
#        e = ElementParser('<style:style style:name="Standard" style:display-name="Standard" style:family="paragraph">')
        self.assertEqual(e.element,u'style:style')
        self.assertTrue(e.has_value(u"style:display-name","Standard"))
        self.assertTrue(e.has_value(u"style:name","Standard"))
        self.assertTrue(e.has_value(u"style:family","paragraph"))



if __name__ == '__main__':
    unittest.main()
