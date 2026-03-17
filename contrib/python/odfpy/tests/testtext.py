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
import os, sys
from odf.opendocument import OpenDocumentText, load
from odf import text
from odf.namespaces import TEXTNS

import yatest.common as yc

if sys.version_info[0]==3:
    unicode=str

class TestText(unittest.TestCase):

    def test_softpagebreak(self):
        """ Create a soft page break """
        textdoc = OpenDocumentText()
        spb = text.SoftPageBreak()
        textdoc.text.addElement(spb)
        self.assertEqual(1, 1)

    def test_1stpara(self):
        """ Grab 1st paragraph and convert to string value """
        poem_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"serious_poem.odt")
        d = load(poem_odt)
        shouldbe = u"The boy stood on the burning deck,Whence allbuthim had fled.The flames that litthe battle'swreck, Shone o'er him, round the dead. "
        #self.assertEqual(shouldbe, d.body)
        self.assertEqual(shouldbe, unicode(d.body))

    def test_link(self):
        """ Create a link """
        textdoc = OpenDocumentText()
        para = text.P()
        anchor = text.A(href="http://www.com/", text="A link label")
        para.addElement(anchor)
        textdoc.text.addElement(para)
        self.assertEqual(1, 1)

    def test_simple_link(self):
        """ Create a link """
        textdoc = OpenDocumentText()
        para = text.P()
        anchor = text.A(href="http://www.com/", type="simple", text="A link label")
        para.addElement(anchor)
        textdoc.text.addElement(para)

    def test_extended_link(self):
        """ Create a link """
        textdoc = OpenDocumentText()
        para = text.P()
        self.assertRaises(ValueError, text.A, href="http://www.com/", type="extended", text="A link label")

if __name__ == '__main__':
    unittest.main()
