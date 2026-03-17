#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017 Martijn Berntsen
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

class TestConceptualspacing(unittest.TestCase):

    def test_contextualspacing(self):
        def printnodes(w, i):
            for p in w.childNodes:
                #if p.attributes <> None:
                for a in p.attributes:
                    key, attr = a
                    value = p.getAttribute(attr.replace('-', ''))
                printnodes(p, i + 1)

        """ Grab 1st paragraph and convert to string value """
        contextualspacing_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"contextualspacing.odt")
        d = load(contextualspacing_odt)
        #try:
        printnodes(d.styles, 0)
        #  self.assertEqual(True, True)
        #except:
        #  self.assertEqual(True, False)
        #self.assertEqual(shouldbe, unicode(d.body))

if __name__ == '__main__':
    unittest.main()
