# -*- coding: utf-8 -*-
"""
Created : 2021-07-30

@author: Eric Lapouyade
"""

from docx import Document
from docx.oxml import CT_SectPr
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from docxcompose.properties import CustomProperties
from docxcompose.utils import xpath
from docxcompose.composer import Composer
from docxcompose.utils import NS
from lxml import etree
import re


class SubdocComposer(Composer):
    def attach_parts(self, doc, remove_property_fields=True):
        """Attach docx parts instead of appending the whole document
        thus subdoc insertion can be delegated to jinja2"""
        self.reset_reference_mapping()

        # Remove custom property fields but keep the values
        if remove_property_fields:
            cprops = CustomProperties(doc)
            for name in cprops.keys():
                cprops.dissolve_fields(name)

        self._create_style_id_mapping(doc)

        for element in doc.element.body:
            if isinstance(element, CT_SectPr):
                continue
            self.add_referenced_parts(doc.part, self.doc.part, element)
            self.add_styles(doc, element)
            self.add_numberings(doc, element)
            self.restart_first_numbering(doc, element)
            self.add_images(doc, element)
            self.add_diagrams(doc, element)
            self.add_shapes(doc, element)
            self.add_footnotes(doc, element)
            self.remove_header_and_footer_references(doc, element)

        self.add_styles_from_other_parts(doc)
        self.renumber_bookmarks()
        self.renumber_docpr_ids()
        self.renumber_nvpicpr_ids()
        self.fix_section_types(doc)

    def add_diagrams(self, doc, element):
        # While waiting docxcompose 1.3.3
        dgm_rels = xpath(element, ".//dgm:relIds[@r:dm]")
        for dgm_rel in dgm_rels:
            for item, rt_type in (
                ("dm", RT.DIAGRAM_DATA),
                ("lo", RT.DIAGRAM_LAYOUT),
                ("qs", RT.DIAGRAM_QUICK_STYLE),
                ("cs", RT.DIAGRAM_COLORS),
            ):
                dm_rid = dgm_rel.get("{%s}%s" % (NS["r"], item))
                dm_part = doc.part.rels[dm_rid].target_part
                new_rid = self.doc.part.relate_to(dm_part, rt_type)
                dgm_rel.set("{%s}%s" % (NS["r"], item), new_rid)


class Subdoc(object):
    """Class for subdocument to insert into master document"""

    def __init__(self, tpl, docpath=None):
        self.tpl = tpl
        self.docx = tpl.get_docx()
        self.subdocx = Document(docpath)
        if docpath:
            compose = SubdocComposer(self.docx)
            compose.attach_parts(self.subdocx)
        else:
            self.subdocx._part = self.docx._part

    def __getattr__(self, name):
        return getattr(self.subdocx, name)

    def _get_xml(self):
        if self.subdocx.element.body.sectPr is not None:
            self.subdocx.element.body.remove(self.subdocx.element.body.sectPr)
        xml = re.sub(
            r"</?w:body[^>]*>",
            "",
            etree.tostring(
                self.subdocx.element.body, encoding="unicode", pretty_print=False
            ),
        )
        return xml

    def __unicode__(self):
        return self._get_xml()

    def __str__(self):
        return self._get_xml()

    def __html__(self):
        return self._get_xml()
