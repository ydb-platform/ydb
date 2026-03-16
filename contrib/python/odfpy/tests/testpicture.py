#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Søren Roug, European Environment Agency
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
from odf import thumbnail

from odf.element import IllegalChild
from odf.opendocument import OpenDocumentText, load
from odf import text
from odf.namespaces import TEXTNS

class TestPicture(unittest.TestCase):

    def testAddPicture(self):
        """ Check that AddPicture works"""
        THUMBNAILNAME = "thumbnail.png"
        icon = thumbnail.thumbnail()
        f = open(THUMBNAILNAME, "wb")
        f.write(icon)
        f.close()
        textdoc = OpenDocumentText()
        textdoc.addPicture(THUMBNAILNAME)
        os.unlink(THUMBNAILNAME)

if __name__ == '__main__':
    unittest.main()
