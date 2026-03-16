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

import unittest, os, sys
from odf.opendocument import OpenDocumentText
from odf import style, text
from odf.text import P, H
from odf.element import IllegalChild

class TestUnicode(unittest.TestCase):

    def setUp(self):
        self.textdoc = OpenDocumentText()
        self.saved = False

    def tearDown(self):
        if self.saved:
            os.unlink("TEST.odt")

    def assertContains(self, stack, needle):
        self.assertNotEqual(-1, stack.find(needle))

    def assertNotContains(self, stack, needle):
        self.assertEqual(-1, stack.find(needle))

    @unittest.skipIf(sys.version_info[0] != 2,
                     "For Python3, unicode strings are type 'str'.")
    def test_xstyle(self):
        self.assertRaises(UnicodeDecodeError, style.Style, name="X✗", family="paragraph")
        xstyle = style.Style(name=u"X✗", family=u"paragraph")
        pp = style.ParagraphProperties(padding=u"0.2cm")
        pp.setAttribute(u"backgroundcolor", u"rød")
        xstyle.addElement(pp)
        self.textdoc.styles.addElement(xstyle)
        self.textdoc.save(u"TEST.odt")
        self.saved = True

    def test_text(self):
        p = P(text=u"Æblegrød")
        p.addText(u' Blåbærgrød')
        self.textdoc.text.addElement(p)
        self.textdoc.save(u"TEST.odt")
        self.saved = True

    def test_contenttext(self):
        p = H(outlinelevel=1,text=u"Æblegrød")
        p.addText(u' Blåbærgrød')
        self.textdoc.text.addElement(p)
        c = self.textdoc.contentxml() # contentxml is supposed to yield a bytes
        self.assertContains(c, b'<office:body><office:text><text:h text:outline-level="1">\xc3\x86blegr\xc3\xb8d Bl\xc3\xa5b\xc3\xa6rgr\xc3\xb8d</text:h></office:text></office:body>')
        self.assertContains(c, b'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"')
        self.assertContains(c, b'<office:automatic-styles/>')

    def test_illegaltext(self):
        p = H(outlinelevel=1,text=u"Spot \u001e the")
        p.addText(u' d\u00a3libe\u0000rate \ud801 mistakes\U0002fffe')
        self.textdoc.text.addElement(p)
        c = self.textdoc.contentxml() # contentxml is supposed to yield a bytes
        # unicode replacement char \UFFFD === \xef\xbf\xbd in UTF-8
        self.assertContains(c, b'<office:body><office:text><text:h text:outline-level="1">Spot \xef\xbf\xbd the d\xc2\xa3libe\xef\xbf\xbdrate \xef\xbf\xbd mistakes\xef\xbf\xbd</text:h></office:text></office:body>')
        self.assertContains(c, b'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"')
        self.assertContains(c, b'<office:automatic-styles/>')

if __name__ == '__main__':
    if sys.version_info[0]==2:
        unittest.main()
    else:
        sys.stderr.write("\n----------------------------------------------------------------------\nRan no test\n\n")
        sys.stderr.write("For Python3, unicode strings are type 'str'.\n")
        sys.stderr.write("OK\n")
