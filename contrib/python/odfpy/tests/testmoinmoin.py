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

import unittest, os, os.path
from odf.opendocument import OpenDocumentText
from odf import style, text
from odf.text import P, H, LineBreak
from odf import odf2moinmoin

import yatest.common as yc

class TestSimple(unittest.TestCase):

    def setUp(self):
        textdoc = OpenDocumentText()
        p = P(text=u"Hello World!")
        textdoc.text.addElement(p)
        textdoc.save(u"TEST.odt")
        self.saved = True

    def tearDown(self):
        if self.saved:
            os.unlink(u"TEST.odt")

    def test_simple(self):
        result = odf2moinmoin.ODF2MoinMoin(u"TEST.odt")
        self.assertEqual(u'Hello World!\n', result.toString())


class TestHeadings(unittest.TestCase):

    saved = False

    def tearDown(self):
        if self.saved:
            os.unlink(u"TEST.odt")

    def test_headings(self):
        textdoc = OpenDocumentText()
        textdoc.text.addElement(H(outlinelevel=1, text=u"Heading 1"))
        textdoc.text.addElement(P(text=u"Hello World!"))
        textdoc.text.addElement(H(outlinelevel=2, text=u"Heading 2"))
        textdoc.save(u"TEST.odt")
        self.saved = True
        result = odf2moinmoin.ODF2MoinMoin(u"TEST.odt")
        self.assertEqual(u'= Heading 1 =\n\nHello World!\n== Heading 2 ==\n\n', result.toString())

    def test_linebreak(self):
        textdoc = OpenDocumentText()
        p = P(text=u"Hello World!")
        textdoc.text.addElement(p)
        p.addElement(LineBreak())
        p.addText(u"Line 2")
        textdoc.save(u"TEST.odt")
        self.saved = True
        result = odf2moinmoin.ODF2MoinMoin(u"TEST.odt")
        self.assertEqual(u'Hello World![[BR]]Line 2\n', result.toString())

class TestExampleDocs(unittest.TestCase):


    def test_twolevellist(self):
        twolevellist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"twolevellist.odt")
        result = odf2moinmoin.ODF2MoinMoin(twolevellist_odt)
        #FIXME: Item c must only have one newline before
        self.assertEqual(u"Line 1\n * Item A\n * Item B\n    * Subitem B.1\n    * '''Subitem B.2 (bold)'''\n\n * Item C\n\nLine 4\n", result.toString())

    def test_simplestyles(self):
        """ The simplestyles.odt has BOLD set in the paragraph style which is
            then turned OFF in the text styles. That is difficult to implement
            in MoinMoin, and currently ignored.
        """
        simplestyles_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"simplestyles.odt")
        result = odf2moinmoin.ODF2MoinMoin(simplestyles_odt)
        # The correct expected:
        #expected = "\nPlain text\n\n'''Bold'''\n\n''Italic''\n\n'''''Bold italic'''''\n\n__Underline__\n\n''__Underline italic__''\n\n'''''__Underline bold italic__'''''\n\nKm^2^ - superscript\n\nH,,2,,O - subscript\n\n~~Strike-through~~\n"
        # The simple-minded expected
        expected = u"Plain text\n\n'''Bold'''\n\n'''''Italic'''''\n\n'''''Bold italic'''''\n\n'''''__Underline__'''''\n\n'''''__Underline italic__'''''\n\n'''''__Underline bold italic__'''''\n\nKm^2^ - superscript\n\nH,,2,,O - subscript\n\n\n"
        self.assertEqual(expected, result.toString())



    def test_parastyles(self):
        parastyles_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"parastyles.odt")
        result = odf2moinmoin.ODF2MoinMoin(parastyles_odt)
        expected = u"Plain text\n\n'''Bold'''\n\n''Italic''\n\n'''''Bold italic'''''\n\n__Underline__\n\n''__Underline italic__''\n\n'''''__Underline bold italic__'''''\n\nKm^2^ - superscript\n\nH,,2,,O - subscript\n\n~~Strike-through~~\n"
        self.assertEqual(expected, result.toString())



    def test_simplelist(self):
        simplelist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"simplelist.odt")
        result = odf2moinmoin.ODF2MoinMoin(simplelist_odt)
        self.assertEqual(u"Line 1\n * Item A\n * Item B\n\nLine 4\n", result.toString())



    def test_simpletable(self):
        simpletable_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"simpletable.odt")
        result = odf2moinmoin.ODF2MoinMoin(simpletable_odt)
        self.assertEqual(u"\n||Cell 1||Cell 2||\n||'''Cell 3 (bold)'''||''Cell 4 (italic)''||\n", result.toString())

if __name__ == '__main__':
    unittest.main()
