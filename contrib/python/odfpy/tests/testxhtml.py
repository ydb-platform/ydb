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
import sys, os, os.path, re
import io
from odf.odf2xhtml import ODF2XHTML

from odf.opendocument import OpenDocumentText
from odf import style
from odf.text import H, P, Span

import yatest.common as yc

def has_rules(html, selector, rules):
    """ Returns false if the selector or rule is not found in html
    """
    selstart = html.find(selector)
    if selstart == -1:
        return False
    selend = html[selstart:].find('}')
    if selend == -1:
        return False
    rulelist = rules.split(";")
    for rule in rulelist:
        if html[selstart:selstart+selend].find(rule.strip()) == -1:
            return False
    return True

def divWithClass_has_styles(s,classname, styleString):
    """
    Checks whether a div with some class attribute bears a set of other
    attributes.
    @param s string to be searched
    @param classname the value of the class attribute
    @param styleString a sequence of style stances separated by semicolons
    @return True when the div opening tag bears all the requested
    styles, independently of their order
    """
    pattern=re.compile(r'<div [^>]*class="'+classname+r'"[^>]*>', re.MULTILINE)
    found=pattern.findall(s)
    if not found:
        return False
    found=found[0]
    foundStyle=re.match(r'''.*style ?= ?["']([^"']*)["'].*''', found)
    if not foundStyle:
        return False
    foundStyle=foundStyle.group(1)
    foundStyle  = map(lambda x: x.replace(' ',''), foundStyle.split(';'))
    styleString = map(lambda x: x.replace(' ',''), styleString.split(';'))
    return set(foundStyle)==set(styleString)



htmlout = """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta content="text/html;charset=UTF-8" http-equiv="Content-Type"/>
<meta name="generator" content="ODFPY/0.7"/>
<style type="text/css">
/*<![CDATA[*/

img { width: 100%; height: 100%; }
* { padding: 0; margin: 0; }
body { margin: 0 1em; }
ol, ul { padding-left: 2em; }
h1 {
	font-size: 24pt;
	font-weight: bold;
}
.S-Bold {
	font-weight: bold;
}
/*]]>*/
</style>
</head>
<body>
<h1>Simple test document<a id="anchor001"></a></h1>
<p>The earth's climate has not changed many times in the course of its long history. <span class="S-Bold">This part is bold. </span>This is after bold.</p>
</body>
</html>
"""

class TestXHTML(unittest.TestCase):

    def setUp(self):
        d = OpenDocumentText()

        # Styles
        h1style = style.Style(name=u"Heading 1",family=u"paragraph")
        h1style.addElement(style.TextProperties(attributes={'fontsize':u"24pt", 'fontweight':u"bold"}))
        d.styles.addElement(h1style)

        boldstyle = style.Style(name=u"Bold",family=u"text")
        boldstyle.addElement(style.TextProperties(attributes={'fontweight':u"bold"}))
        d.automaticstyles.addElement(boldstyle)

        # Text
        h = H(outlinelevel=1, stylename=h1style, text=u"Simple test document")
        d.text.addElement(h)
        p = P(text=u"The earth's climate has not changed many times in the course of its long history. ")
        d.text.addElement(p)
        boldpart = Span(stylename=boldstyle, text=u"This part is bold. ")
        p.addElement(boldpart)
        p.addText(u"This is after bold.")

        d.save(u"TEST.odt")

    def tearDown(self):
        os.unlink(u"TEST.odt")

    def testParsing(self):
        """ Parse the test file """
        odhandler = ODF2XHTML()
        outf = io.BytesIO()

        result = odhandler.odf2xhtml(u"TEST.odt")
        outf.write(result.encode('utf-8'))
        strresult = outf.getvalue().decode('utf-8')
        #self.assertEqual(strresult, htmlout)
        self.assertNotEqual(-1, strresult.find(u"""<p>The earth's climate has \
not changed many times in the course of its long history. \
<span class="S-Bold">This part is bold. </span>This is after bold.</p>"""))
        self.assertNotEqual(-1, strresult.find(u"""<h1>Simple test document<a id="anchor001"></a></h1>"""))
        self.assertNotEqual(-1, strresult.find(u""".S-Bold {"""))
        self.assertEqual(-1, strresult.find(u"<ol "))

class TestExampleDocs(unittest.TestCase):

    def test_twolevellist(self):
        """ Check CSS has list styles for two level lists"""
        twolevellist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"twolevellist.odt")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(twolevellist_odt)
        assert has_rules(result,u".L1_2",u"list-style-type: circle; font-family: StarSymbol, sans-serif;")

    def test_simplestyles(self):
        """ Check CSS has text and paragraph styles """
        simplestyles_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"simplestyles.odt")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(simplestyles_odt)
        assert has_rules(result,u".S-T1",u"font-weight: normal;")
        assert has_rules(result,u".P-P2",u"font-weight: bold; font-style: italic;")
        assert has_rules(result,u".S-T11",u"text-decoration: underline;")
        self.assertNotEqual(-1, result.find(u"""<p class="P-P2"><span class="S-T1">Italic</span></p>\n"""))
        self.assertNotEqual(-1, result.find(u"""\n\ttext-decoration: underline;\n"""))
        self.assertNotEqual(-1, result.find(u"""<p class="P-P3"><span class="S-T1">Underline italic</span></p>\n"""))

    def test_simplelist(self):
        """ Check CSS has list styles """
        simplelist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"simplelist.odt")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(simplelist_odt)
        assert has_rules(result,u".L1_1",u"list-style-type: disc;")
        assert has_rules(result,u".L1_2",u"list-style-type: circle;")
        assert has_rules(result,u".L1_3",u"list-style-type: square;")
        self.assertNotEqual(-1, result.find(u"""<p class="P-Standard">Line 1</p>\n<ul class="L1_1"><li><p class="P-P1">Item A</p>"""))

    def test_mixedlist(self):
        """ Check CSS has list styles """
        simplelist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"ol.odp")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(simplelist_odt)
        assert has_rules(result,u".L2_1",u"list-style-type: decimal;")
        assert has_rules(result,u".L2_2",u"list-style-type: circle;")
        assert has_rules(result,u".L2_3",u"list-style-type: square;")
        assert has_rules(result,u".L3_1",u"list-style-type: disc;")
        assert has_rules(result,u".L3_2",u"list-style-type: decimal;")
        assert has_rules(result,u".L3_3",u"list-style-type: square;")
        assert has_rules(result,u".MP-Default",u"height: 19.05cm; width: 25.4cm; position: relative;")
        self.assertNotEqual(-1, result.find(u"""<ol class="L2_1">"""))
        self.assertNotEqual(-1, result.find(u"""<ol class="L3_2">"""))
        self.assertNotEqual(-1, result.find(u"""position:absolute;width:22.86cm;height:3.176cm;left:1.27cm;top:0.762cm;"""))

    def test_simpletable(self):
        """ Check CSS has table styles """
        simpletable_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"simpletable.odt")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(simpletable_odt)
        assert result.find(u"""<td class="TD-Tabel1_A1"><p class="P-Table_20_Contents">Cell 1</p>""") != -1
        assert result.find(u"""<tr><td class="TD-Tabel1_A2"><p class="P-P1">Cell 3 (bold)</p>""") != -1
        assert result.find(u"""<td class="TD-Tabel1_B2"><p class="P-P2">Cell 4 (italic)</p>""") != -1

    def test_images(self):
        """ Check CSS has frame styles for images """
        odt = os.path.join(os.path.dirname(yc.source_path(__file__)), u"examples", u"images.odt")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(odt)
        assert has_rules(result,u".G-fr1",u"margin-left: 0cm; margin-right: auto;")
        assert has_rules(result,u".G-fr2",u"margin-left: auto; margin-right: 0cm;")
        assert has_rules(result,u".G-fr3",u"float: left")
        assert has_rules(result,u".G-fr4",u"margin-right: auto;margin-left: auto;")
        assert has_rules(result,u".G-fr5",u"float: right")

    def test_imageslabels(self):
        """ Check CSS has frame styles for images with captions"""
        odt = os.path.join(os.path.dirname(yc.source_path(__file__)), u"examples", u"imageslabels.odt")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(odt)
        assert has_rules(result,u".G-fr1",u"margin-left: 0cm; margin-right: auto;")
        assert has_rules(result,u".G-fr2",u"margin-left: auto; margin-right: 0cm;")
        assert has_rules(result,u".G-fr3",u"float: left")
        assert has_rules(result,u".G-fr4",u"float: right")
        assert has_rules(result,u".G-fr5",u"margin-right: auto;margin-left: auto;")
        assert has_rules(result,u".G-fr7",u"margin-right: auto;margin-left: auto;")
        assert has_rules(result,u".P-Illustration",u"font-size: 10pt;")

    def test_css(self):
        """ Test css() method """
        odt = os.path.join(os.path.dirname(yc.source_path(__file__)), u"examples", u"imageslabels.odt")
        odhandler = ODF2XHTML()
        odhandler.load(odt)
        result = odhandler.css()
        assert has_rules(result,u".G-fr1",u"margin-left: 0cm;margin-right: auto")
        assert has_rules(result,u".G-fr2",u"margin-left: auto;margin-right: 0cm")
        assert has_rules(result,u".G-fr3",u"float: left")
        assert has_rules(result,u".G-fr4",u"float: right")
        assert has_rules(result,u".G-fr5",u"margin-right: auto;margin-left: auto")
        assert has_rules(result,u".G-fr7",u"margin-right: auto;margin-left: auto")
        assert has_rules(result,u".P-Illustration",u"font-size: 10pt;")

    def test_positioned_shapes(self):
        """ Test positioned custom-shapes """
        odt = os.path.join(os.path.dirname(yc.source_path(__file__)), u"examples", u"cols.odp")
        odhandler = ODF2XHTML()
        result = odhandler.odf2xhtml(odt)
        # Python3 can ouput style stances in a non-predictable order when
        # parsing an XML document; so the following test may fail
        # unexpectedly with Python3. It is replaced by a more robust test.
        ## self.assertNotEqual(-1, result.find(u'''<div style="position: absolute;width:5.503cm;height:1.905cm;left:2.117cm;top:3.175cm;" class="G-gr1">'''))
        assert(divWithClass_has_styles(result, u"G-gr1", u"position: absolute;width:5.503cm;height:1.905cm;left:2.117cm;top:3.175cm;"))
        assert has_rules(result,u".MP-Default",u"height: 19.05cm; width: 25.4cm; position: relative;")


if __name__ == '__main__':
    unittest.main()
