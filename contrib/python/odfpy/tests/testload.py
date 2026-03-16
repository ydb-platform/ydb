#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2008-2010 Søren Roug, European Environment Agency
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

import unittest, os, os.path, sys

from io import BytesIO
from zipfile import ZipFile

from odf.opendocument import OpenDocumentText, load
from odf import style, text
from odf.text import P, H, LineBreak
from .elementparser import ElementParser

from defusedxml import EntitiesForbidden

import yatest.common as yc


if sys.version_info[0]==3:
    unicode=str

class TestSimple(unittest.TestCase):

    def setUp(self):
        textdoc = OpenDocumentText()
        p = P(text="Hello World!")
        textdoc.text.addElement(p)
        textdoc.save(u"TEST.odt")
        self.saved = True

    def tearDown(self):
        if self.saved:
            os.unlink(u"TEST.odt")

    def test_simple(self):
        """ Check that a simple load works """
        d = load(u"TEST.odt")
        result = d.contentxml() # contentxml() is supposed to yeld a bytes
        self.assertNotEqual(-1, result.find(b"""Hello World!"""))


class TestHeadings(unittest.TestCase):

    saved = False

    def tearDown(self):
        if self.saved:
            os.unlink("TEST.odt")

    def test_headings(self):
        """ Create a document, save it and load it """
        textdoc = OpenDocumentText()
        textdoc.text.addElement(H(outlinelevel=1, text=u"Heading 1"))
        textdoc.text.addElement(P(text=u"Hello World!"))
        textdoc.text.addElement(H(outlinelevel=2, text=u"Heading 2"))
        textdoc.save(u"TEST.odt")
        self.saved = True
        d = load(u"TEST.odt")
        result = d.contentxml() # contentxml() is supposed to yeld a bytes
        self.assertNotEqual(-1, result.find(b"""<text:h text:outline-level="1">Heading 1</text:h><text:p>Hello World!</text:p><text:h text:outline-level="2">Heading 2</text:h>"""))

    def test_linebreak(self):
        """ Test that a line break (empty) element show correctly """
        textdoc = OpenDocumentText()
        p = P(text=u"Hello World!")
        textdoc.text.addElement(p)
        p.addElement(LineBreak())
        p.addText(u"Line 2")
        textdoc.save(u"TEST.odt")
        self.saved = True
        d = load(u"TEST.odt")
        result = d.contentxml() # contentxml() is supposed to yeld a bytes
        self.assertNotEqual(-1, result.find(b"""<text:p>Hello World!<text:line-break/>Line 2</text:p>"""))


class BaseXMLVulnerabilities(unittest.TestCase):
    def _modify_zip_file(self, zip_path, filename, replacement_content=None):
        """
        Replace the contents of a file inside a ZIP-container. The original file
        will not be overwritten. A BytesIO() file will be returned containing
        the new ZIP-container with the file replaced.

        :param zip_path The path to the ZIP-File to be modified
        :param filename Filename inside the container to be modified
        :param replacement_content The new container which will be stored in `filename`
        :return BytesIO contain the modified zip file.
        """
        vuln_zipfile = BytesIO()
        zw = ZipFile(vuln_zipfile, 'w')
        zr = ZipFile(zip_path)

        names_to_copy = set(zr.namelist()) - {filename, }
        for name in names_to_copy:
            content = zr.read(name)
            zw.writestr(name, content)

        if replacement_content:
            zw.writestr(filename, replacement_content)

        zw.close()
        vuln_zipfile.seek(0)

        x = ZipFile(vuln_zipfile, 'r')
        self.assertEqual(x.testzip(), None)

        vuln_zipfile.seek(0)

        return vuln_zipfile

    def modify_zip_file(self, zip_path, filename, replacements):
        """
        Performan a list of replacments on `filename` the original file in
        `zip_path` will remain unaffected and a BytesIO object will be returned
        containing the new zip file.

        :param zip_path Path to the zip-file.
        :param filename Filename where the replacements will be performed on
        :param replacements An iterable containing replacements using two-tuples.
                            e.g. [(orig_string, new_string), ]
        :return BytesIO contain the modified zip file.
        """
        zr = ZipFile(zip_path)
        original_content = zr.read(filename).decode('utf-8')
        new_content = original_content

        for old, new in replacements:
            new_content = new_content.replace(old, new)
        new_content = new_content.encode('utf-8')

        return self._modify_zip_file(zip_path, filename, replacement_content=new_content)


class TestQuadraticBlowup(BaseXMLVulnerabilities):
    attack_entity = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<!DOCTYPE bomb ['
        '<!ENTITY a "{loads_of_bs}">'
        ']>'
    ).format(loads_of_bs="B" * 100000)
    simple_ods = os.path.join(os.path.dirname(yc.source_path(__file__)), "examples", "empty.ods")
    loads_of_as = '&a;' * 1000

    def test_manifest_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'META-INF/manifest.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('manifest:full-path="/" manifest:version="1.2"',
             'manifest:full-path="/" manifest:version="{loads_of_as}"'.format(loads_of_as=self.loads_of_as)),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_settings_xml(self):
        attack_file = 'settings.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('"VerticalSplitMode" config:type="short">0</config:config-item>',
             '"VerticalSplitMode" config:type="short">{loads_of_as}</config:config-item>'.format(
                 loads_of_as=self.loads_of_as
             )),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.settingsxml())

    def test_meta_xml(self):
        attack_file = 'meta.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('<meta:creation-date>', '<meta:creation-date>{loads_of_as}'.format(
                loads_of_as=self.loads_of_as)),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.metaxml())

    def test_content_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'content.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('style:name="Liberation Sans"', 'style:name="Liberation Sans {loads_of_as}"'.format(
                loads_of_as=self.loads_of_as)),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.metaxml())

    def test_styles_xml(self):
        attack_file = 'styles.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('<text:time>00:00:00</text:time>', '<text:time>{loads_of_as}; 00:00:00</text:time>'.format(
                loads_of_as=self.loads_of_as)),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.stylesxml())


class TestXMLBomb(BaseXMLVulnerabilities):
    """
    Tests various XML files inside the ODS container
    for the Billion laughs attack.
    """
    attack_entity = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<!DOCTYPE xmlbomb ['
        '<!ENTITY a "1234567890" >'
        '<!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;">'
        '<!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;">'
        '<!ENTITY d "&c;&c;&c;&c;&c;&c;&c;&c;">'
        '<!ENTITY e "&d;&d;&d;&d;&d;&d;&d;&d;">'
        '<!ENTITY f "&e;&e;&e;&e;&e;&e;&e;&e;">'
        '<!ENTITY g "&f;&f;&f;&f;&f;&f;&f;&f;">'
        ']>'
    )
    simple_ods = os.path.join(os.path.dirname(yc.source_path(__file__)), "examples", "empty.ods")

    def test_manifest_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'META-INF/manifest.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('manifest:full-path="/" manifest:version="1.2"',
             'manifest:full-path="/" manifest:version="&g;"'),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_settings_xml(self):
        attack_file = 'settings.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('<config:config-item config:name="VerticalSplitMode" config:type="short">0</config:config-item>',
             '<config:config-item config:name="VerticalSplitMode" config:type="short">&g;</config:config-item>'),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.settingsxml())

    def test_meta_xml(self):
        attack_file = 'meta.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('<meta:creation-date>', '<meta:creation-date>xx&g;'),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.metaxml())

    def test_content_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'content.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('style:name="Liberation Sans"', 'style:name="Liberation Sans &g;"'),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_styles_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'styles.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('<text:time>00:00:00</text:time>', '<text:time>&g; 00:00:00</text:time>'),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.stylesxml())


class TestXXEFile(BaseXMLVulnerabilities):
    attack_entity = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<!DOCTYPE external ['
        '<!ENTITY ee SYSTEM "/etc/passwd">'
        ']>'
    )
    simple_ods = os.path.join(os.path.dirname(yc.source_path(__file__)), "examples", "empty.ods")

    def test_manifest_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'META-INF/manifest.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('manifest:full-path="/" manifest:version="1.2"',
             'manifest:full-path="/" manifest:version="&ee;"')
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_settings_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'settings.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('config:name="ActiveTable" config:type="string">',
             'config:name="ActiveTable" config:type="string">&xxe;')
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.settingsxml())

    def test_meta_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'meta.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('<meta:creation-date>', '<meta:creation-date>&xxe;')
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.metaxml())

    def test_content_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'content.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('style:name="Liberation Sans"', 'style:name="Liberation Sans &g;"'),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.metaxml())

    def test_styles_xml(self):
        """
        N.B. Haven't actually triggered this attack.
        """
        attack_file = 'styles.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
            ('<text:time>00:00:00</text:time>', '<text:time>&ee; 00:00:00</text:time>'),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

        # To trigger:
        # d = load(new_zipfile)
        # print(d.stylesxml())


class TestXXE(BaseXMLVulnerabilities):
    attack_entity = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<!DOCTYPE test [ '
        '<!ENTITY % one SYSTEM "http://127.0.0.1:8100/x.xml" >'
        '%one;'
        ']>'
    )
    simple_ods = os.path.join(os.path.dirname(yc.source_path(__file__)), "examples", "empty.ods")

    def test_manifest_xml(self):
        attack_file = 'META-INF/manifest.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)

        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_settings_xml(self):
        """
        Was caught by
            parser.setFeature(handler.feature_external_ges, 0)
        """
        attack_file = 'settings.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_meta_xml(self):
        """
        Was caught by
            parser.setFeature(handler.feature_external_ges, 0)
        """
        attack_file = 'meta.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_content_xml(self):
        """
        Was caught by
            parser.setFeature(handler.feature_external_ges, 0)
        """
        attack_file = 'content.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)

        self.assertRaises(EntitiesForbidden, load, new_zipfile)

    def test_styles_xml(self):
        """
        Was caught by
            parser.setFeature(handler.feature_external_ges, 0)
        """
        attack_file = 'styles.xml'
        replacements = (
            ('<?xml version="1.0" encoding="UTF-8"?>', self.attack_entity),
        )
        new_zipfile = self.modify_zip_file(self.simple_ods, attack_file, replacements)
        self.assertRaises(EntitiesForbidden, load, new_zipfile)


class TestExampleDocs(unittest.TestCase):

    def test_metagenerator(self):
        """ Check that meta:generator is the original one """
        parastyles_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), "examples", "parastyles.odt")
        d = load(parastyles_odt)
        meta = d.metaxml()
        self.assertEqual(-1, meta.find(u"""<meta:generator>OpenOffice.org/2.3$Linux OpenOffice.org_project/680m6$Build-9226"""),"Must use the original generator string")

    def test_metagenerator_odp(self):
        """ Check that meta:generator is the original one """
        parastyles_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"emb_spreadsheet.odp")
        d = load(parastyles_odt)
        meta = d.metaxml()
        self.assertNotEqual(-1, meta.find(u"""<meta:generator>ODFPY"""), "Must not use the original generator string")


    def test_simplelist(self):
        """ Check that lists are loaded correctly """
        simplelist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), "examples", "simplelist.odt")
        d = load(simplelist_odt)
        result = unicode(d.contentxml(),'utf-8')
        self.assertNotEqual(-1, result.find(u"""<text:list text:style-name="L1"><text:list-item><text:p text:style-name="P1">Item A</text:p></text:list-item><text:list-item>"""))


    def test_simpletable(self):
        """ Load a document containing tables """
        simpletable_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), "examples", "simpletable.odt")
        d = load(simpletable_odt)
        result = unicode(d.contentxml(),'utf-8')
        e = ElementParser(result,'text:sequence-decl')
        self.assertTrue(e.has_value("text:name","Drawing")) # Last sequence
        self.assertTrue(e.has_value("text:display-outline-level","0"))

        e = ElementParser(result,'table:table-column')
        self.assertTrue(e.has_value("table:number-columns-repeated","2"))
        self.assertTrue(e.has_value("table:style-name","Tabel1.A"))

    def test_headerfooter(self):
        """ Test that styles referenced from master pages are renamed in OOo 2.x documents """
        simplelist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), "examples", "headerfooter.odt")
        d = load(simplelist_odt)
        result = d.stylesxml()
        self.assertNotEqual(-1, result.find(u'''style:name="MP1"'''))
        self.assertNotEqual(-1, result.find(u'''style:name="MP2"'''))
        self.assertNotEqual(-1, result.find(u"""<style:header><text:p text:style-name="MP1">Header<text:tab/>"""))
        self.assertNotEqual(-1, result.find(u"""<style:footer><text:p text:style-name="MP2">Footer<text:tab/>"""))

    def test_formulas_ooo(self):
        """ Check that formula prefixes are preserved """
        pythagoras_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), "examples", "pythagoras.ods")
        d = load(pythagoras_odt)
        result = unicode(d.contentxml(),'utf-8')
        self.assertNotEqual(-1, result.find(u'''xmlns:of="urn:oasis:names:tc:opendocument:xmlns:of:1.2"'''))
        self.assertNotEqual(-1, result.find(u'''table:formula="of:=SQRT([.A1]*[.A1]+[.A2]*[.A2])"'''))
        self.assertNotEqual(-1, result.find(u'''table:formula="of:=SUM([.A1:.A2])"'''))

    def test_formulas_ooo(self):
        """ Check that formulas are understood when there are no prefixes"""
        pythagoras_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), "examples", "pythagoras-kspread.ods")
        d = load(pythagoras_odt)
        result = unicode(d.contentxml(),'utf-8')
        self.assertNotEqual(-1, result.find(u'''table:formula="=SQRT([.A1]*[.A1]+[.A2]*[.A2])"'''))
        self.assertNotEqual(-1, result.find(u'''table:formula="=SUM([.A1]:[.A2])"'''))

    def test_externalent(self):
        """ Check that external entities are not loaded """
        simplelist_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), "examples", "nasty.odt")
        self.assertRaises(EntitiesForbidden, load, simplelist_odt)

    def test_spreadsheet(self):
        """ Load a document containing subobjects """
        spreadsheet_odt = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"emb_spreadsheet.odp")
        d = load(spreadsheet_odt)
        self.assertEqual(1, len(d.childobjects))
#       for s in d.childobjects:
#           print (s.folder)
#       mani = unicode(d.manifestxml(),'utf-8')
#       self.assertNotEqual(-1, mani.find(u''' manifest:full-path="Object 1/"'''), "Must contain the subobject")
#       self.assertNotEqual(-1, mani.find(u''' manifest:full-path="Object 1/settings.xml"'''), "Must contain the subobject settings.xml")

#        d.save("subobject.odp")

    def test_chinese(self):
        """ Load a document containing Chinese content"""
        chinese_spreadsheet = os.path.join(
            os.path.dirname(yc.source_path(__file__)), u"examples", u"chinese_spreadsheet.ods")
        d = load(chinese_spreadsheet)
        result = unicode(d.contentxml(),'utf-8')
        self.assertNotEqual(-1, result.find(u'''工作表1'''))

if __name__ == '__main__':
    unittest.main()
