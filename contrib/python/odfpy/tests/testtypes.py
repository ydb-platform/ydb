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
from odf.opendocument import load
from odf import office, text

import yatest.common as yc

class TestTypes(unittest.TestCase):


    def test_paras(self):
        """ Grab all paragraphs and check they are paragraphs """
        poem_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"serious_poem.odt")
        d = load(poem_odt)
        allparas = d.getElementsByType(text.P)

        for p in allparas:
            self.assertTrue(p.isInstanceOf(text.P))

    def test_body(self):
        """ Check that the document's body is <office:body> """
        poem_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"serious_poem.odt")
        d = load(poem_odt)
        self.assertTrue(d.body.isInstanceOf(office.Body))
        self.assertFalse(d.body.isInstanceOf(text.P))
        self.assertTrue(d.body.parentNode.isInstanceOf(office.Document))
        self.assertTrue(d.topnode.isInstanceOf(office.Document))


if __name__ == '__main__':
    unittest.main()
