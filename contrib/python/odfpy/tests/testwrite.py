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
from odf.opendocument import OpenDocumentText
from odf import style, text
from odf.text import P

class TestWrite(unittest.TestCase):

    def test_write(self):
        """ document's write method """
        outfp = io.BytesIO()
        textdoc = OpenDocumentText()
        p = P(text=u"Æblegrød")
        p.addText(u' Blåbærgrød')
        textdoc.text.addElement(p)
        textdoc.write(outfp)
        outfp.seek(0)
        # outfp now contains the document.
        z = zipfile.ZipFile(outfp,"r")
        self.assertEqual(None, z.testzip())

        outfp.close()

    def test_topnode(self):
        """ Check that topnode is correct """
        textdoc = OpenDocumentText()
        self.assertEqual(8, len(textdoc.topnode.childNodes))
        self.assertEqual(textdoc, textdoc.topnode.ownerDocument)
        self.assertEqual(textdoc, textdoc.styles.ownerDocument)
        self.assertEqual(textdoc, textdoc.settings.ownerDocument)

if __name__ == '__main__':
    unittest.main()
