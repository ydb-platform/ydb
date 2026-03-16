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

import unittest, os, zipfile, re
from odf.opendocument import OpenDocumentText
from odf import draw, text
from odf.element import IllegalChild
from .elementparser import ElementParser

def _getxmlpart(odffile, xmlfile):
    """ Get the content out of the ODT file"""
    z = zipfile.ZipFile(odffile)
    content = z.read(xmlfile)
    z.close()
    return content

def element_has_attributes(s, tag, attribs):
    """
    checks that at least one of elements in an XML string with a given tag
    has the requested attributes, independently of their order
    @param s: an XML string
    @param tag a tag
    @param attributes an attribute string; attributes are separated by spaces
    @return True if attributes are there, independently of their order
    """
    pattern=re.compile(r'<'+tag+' [^>]*/?>')
    found=pattern.findall(s)
    if not found:
        return False
    # removes parts of the string which are not attributes
    found1=map(lambda s: s.replace('<'+tag+' ', '').replace('/>','').replace('>',''), found)
    attribPattern=re.compile(r'[-a-z0-9:]*="[^"]*"')
    foundAttribMap = map(lambda s: attribPattern.findall(s), found1)
    attribs=attribPattern.findall(attribs)
    for foundAttribs in foundAttribMap:
        if set(foundAttribs) == set(attribs):
            return True
    return False


class TestUnicode(unittest.TestCase):

    def setUp(self):
        self.textdoc = OpenDocumentText()
        self.saved = False

    def tearDown(self):
        if self.saved:
            os.unlink("TEST.odt")

    def test_subobject(self):
        df = draw.Frame(width="476pt", height="404pt", anchortype="paragraph")
        self.textdoc.text.addElement(df)

        subdoc = OpenDocumentText()
        # Here we add the subdocument to the main document. We get back a reference
        # to use in the href.
        subloc = self.textdoc.addObject(subdoc)
        self.assertEqual(subloc,'./Object 1')
        do = draw.Object(href=subloc)
        df.addElement(do)

        subsubdoc = OpenDocumentText()
        subsubloc = subdoc.addObject(subsubdoc)
        self.assertEqual(subsubloc,'./Object 1/Object 1')

        c = self.textdoc.contentxml() # contentxml() is supposed to yeld a bytes
        c.index(b'<office:body><office:text><draw:frame ')
        e = ElementParser(c.decode("utf-8"), u'draw:frame')
#       e = ElementParser('<draw:frame svg:width="476pt" text:anchor-type="paragraph" svg:height="404pt">')
        self.assertTrue(e.has_value(u'svg:width',"476pt"))
        self.assertTrue(e.has_value(u'svg:height',"404pt"))
        self.assertTrue(e.has_value(u'text:anchor-type',"paragraph"))
        self.assertFalse(e.has_value(u'svg:height',"476pt"))
        c.index(b'<draw:object xlink:href="./Object 1"/></draw:frame></office:text></office:body>')
        c.index(b'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"')
        self.textdoc.save(u"TEST.odt")
        self.saved = True
        m = _getxmlpart(u"TEST.odt", u"META-INF/manifest.xml").decode('utf-8')
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="application/vnd.oasis.opendocument.text" manifest:full-path="/"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="application/vnd.oasis.opendocument.text" manifest:full-path="/"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="text/xml" manifest:full-path="content.xml"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="text/xml" manifest:full-path="meta.xml"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="application/vnd.oasis.opendocument.text" manifest:full-path="Object 1/"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="text/xml" manifest:full-path="Object 1/styles.xml"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="text/xml" manifest:full-path="Object 1/content.xml"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="application/vnd.oasis.opendocument.text" manifest:full-path="Object 1/Object 1/"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="text/xml" manifest:full-path="Object 1/Object 1/styles.xml"'))
        assert(element_has_attributes(m, u'manifest:file-entry', u'manifest:media-type="text/xml" manifest:full-path="Object 1/Object 1/content.xml"'))

if __name__ == '__main__':
    unittest.main()
