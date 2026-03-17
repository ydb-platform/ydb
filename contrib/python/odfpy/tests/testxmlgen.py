#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2009 Søren Roug, European Environment Agency
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

import xml.sax, xml.sax.saxutils
import io
import tempfile
import unittest
import sys

class MyGen(xml.sax.saxutils.XMLGenerator):

    def _qname(self, name):
        """Builds a qualified name from a (ns_url, localname) pair"""
        if name[0]:
            if name[0] == u'http://www.w3.org/XML/1998/namespace':
                return u'xml' + ":" + name[1]
            # The name is in a non-empty namespace
            prefix = self._current_context[name[0]]
            if prefix:
                # If it is not the default namespace, prepend the prefix
                return prefix + ":" + name[1]
        # Return the unqualified name
        return name[1]

class TestXMLGenerator(unittest.TestCase):

    def test_xmlgenerator(self):
        """ Test that the xml namespace is understood by XMLGenerator """
        outfp = tempfile.TemporaryFile()
        c = xml.sax.saxutils.XMLGenerator(outfp,'utf-8')
        parser = xml.sax.make_parser()
        parser.setFeature(xml.sax.handler.feature_namespaces, 1)
        parser.setContentHandler(c)
        testcontent="""<?xml version="1.0"?>
<a:greetings xmlns:a="http://example.com/ns" xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
        parser.feed(testcontent)
        parser.close()
        expectedresult = """<?xml version="1.0" encoding="utf-8"?>
<a:greetings xmlns:a="http://example.com/ns" xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
        outfp.seek(0)
        self.assertEqual( outfp.read().decode('utf-8'), expectedresult)
        outfp.close()


    def test_xmlgenerator_wo_ns(self):
        """ Test that the missing xml namespace is understood by XMLGenerator """
        outfp = tempfile.TemporaryFile()
        c = xml.sax.saxutils.XMLGenerator(outfp,'utf-8')
        parser = xml.sax.make_parser()
        parser.setFeature(xml.sax.handler.feature_namespaces, 1)
        parser.setContentHandler(c)
        testcontent="""<?xml version="1.0"?>
<a:greetings xmlns:a="http://example.com/ns">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
        # There is a bug in older versions of saxutils
        if sys.version_info[0] == 2 and sys.version_info[1] == 6:
            self.assertRaises(KeyError, parser.feed, testcontent)
        else:
            parser.feed(testcontent)
            parser.close()
            expectedresult="""<?xml version="1.0" encoding="utf-8"?>
<a:greetings xmlns:a="http://example.com/ns">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
            outfp.seek(0)
            self.assertEqual( outfp.read().decode('utf-8'), expectedresult)
            outfp.close()

    def test_myxml(self):
        """ Test that my patch works """
        outfp = tempfile.TemporaryFile()
        c = MyGen(outfp,'utf-8')
        parser = xml.sax.make_parser()
        parser.setFeature(xml.sax.handler.feature_namespaces, 1)
        parser.setContentHandler(c)
        testcontent="""<?xml version="1.0"?>
<a:greetings xmlns:a="http://example.com/ns" xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
        parser.feed(testcontent)
        parser.close()
        outfp.seek(0)
        expectedresult = """<?xml version="1.0" encoding="utf-8"?>
<a:greetings xmlns:a="http://example.com/ns" xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
        self.assertEqual( outfp.read().decode('utf-8'), expectedresult)
        outfp.close()

    def test_myxml_wo_xml(self):
        """ Test that my patch understands the missing xml namespace """
        outfp = tempfile.TemporaryFile()
        c = MyGen(outfp,'utf-8')
        parser = xml.sax.make_parser()
        parser.setFeature(xml.sax.handler.feature_namespaces, 1)
        parser.setContentHandler(c)
        testcontent="""<?xml version="1.0"?>
<a:greetings xmlns:a="http://example.com/ns" xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
        parser.feed(testcontent)
        parser.close()
        outfp.seek(0)
        expectedresult = """<?xml version="1.0" encoding="utf-8"?>
<a:greetings xmlns:a="http://example.com/ns" xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <a:greet xml:lang="en">Hello world</a:greet>
</a:greetings>"""
        self.assertEqual( outfp.read().decode('utf-8'), expectedresult)
        outfp.close()

if __name__ == '__main__':
    unittest.main()
