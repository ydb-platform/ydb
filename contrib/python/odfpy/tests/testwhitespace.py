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

import unittest, os
import io
import zipfile
from odf import teletype
from odf.opendocument import OpenDocumentText, load
from odf import style, text
from odf.text import P

import yatest.common as yc

class TestWhite(unittest.TestCase):


    def test_create(self):
        """ Test that tabs and newlines are converted to elements """
        para = P()
        teletype.addTextToElement(para,
                                u"The boy stood   on the burning deck,\n" +
                                u"\tHis feet\twere\t\tfull of blisters.\n" +
                                u"The captain  stood in\tthe public house\n" +
                                u"         With beer running down his whiskers.   " );
        outfp = io.StringIO()
        para.toXml(1,outfp)
        self.assertEqual(u'''<text:p>The boy stood <text:s text:c="2"/>on the burning deck,<text:line-break/>''' +
          u'''<text:tab/>His feet<text:tab/>were<text:tab/><text:tab/>full of blisters.<text:line-break/>''' +
          u'''The captain <text:s text:c="1"/>stood in<text:tab/>the public house<text:line-break/>''' +
          u''' <text:s text:c="8"/>With beer running down his whiskers. <text:s text:c="2"/></text:p>''', outfp.getvalue())


    def test_extract(self):
        """ Convert a paragraph to plain text """
        poem_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"serious_poem.odt")
        d = load(poem_odt)
        allparas = d.getElementsByType(P)
        content = u"""<text:p text:style-name="Standard">The boy stood <text:s text:c="3"/>on the burning deck,<text:line-break/><text:tab/>Whence all<text:tab/>but<text:tab/><text:tab/>him had fled.<text:line-break/>The flames <text:s text:c="2"/>that lit<text:tab/>the battle's<text:tab/>wreck,<text:line-break/> <text:s text:c="11"/>Shone o'er him, round the dead. <text:s text:c="2"/></text:p>"""

        self.assertEqual(u"The boy stood    on the burning deck,\n\tWhence all\tbut\t\thim had fled.\nThe flames   that lit\tthe battle's\twreck,\n            Shone o'er him, round the dead.   ", teletype.extractText(allparas[0]))

    def test_extract_with_span(self):
        """ Extract a text with a bold/italic span """
        poem_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"simplestyles.odt")
        d = load(poem_odt)
        teletype.extractText(d.body)
        self.assertEqual(u'Plain textBoldItalicBold italicUnderlineUnderline italicUnderline bold italicKm2 - superscriptH2O - subscript', teletype.extractText(d.body))


if __name__ == '__main__':
    unittest.main()
