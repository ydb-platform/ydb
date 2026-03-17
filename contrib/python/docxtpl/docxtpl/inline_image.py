# -*- coding: utf-8 -*-
"""
Created : 2021-07-30

@author: Eric Lapouyade
"""
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import qn


class InlineImage(object):
    """Class to generate an inline image

    This is much faster than using Subdoc class.
    """

    tpl = None
    image_descriptor = None
    width = None
    height = None
    anchor = None

    def __init__(self, tpl, image_descriptor, width=None, height=None, anchor=None):
        self.tpl, self.image_descriptor = tpl, image_descriptor
        self.width, self.height = width, height
        self.anchor = anchor

    def _add_hyperlink(self, run, url, part):
        # Create a relationship for the hyperlink
        r_id = part.relate_to(
            url,
            "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink",
            is_external=True,
        )

        # Find the <wp:docPr> and <pic:cNvPr> element
        docPr = run.xpath(".//wp:docPr")[0]
        cNvPr = run.xpath(".//pic:cNvPr")[0]

        # Create the <a:hlinkClick> element
        hlinkClick1 = OxmlElement("a:hlinkClick")
        hlinkClick1.set(qn("r:id"), r_id)
        hlinkClick2 = OxmlElement("a:hlinkClick")
        hlinkClick2.set(qn("r:id"), r_id)

        # Insert the <a:hlinkClick> element right after the <wp:docPr> element
        docPr.append(hlinkClick1)
        cNvPr.append(hlinkClick2)

        return run

    def _insert_image(self):
        pic = self.tpl.current_rendering_part.new_pic_inline(
            self.image_descriptor,
            self.width,
            self.height,
        ).xml
        if self.anchor:
            run = parse_xml(pic)
            if run.xpath(".//a:blip"):
                hyperlink = self._add_hyperlink(
                    run, self.anchor, self.tpl.current_rendering_part
                )
                pic = hyperlink.xml

        return (
            "</w:t></w:r><w:r><w:drawing>%s</w:drawing></w:r><w:r>"
            '<w:t xml:space="preserve">' % pic
        )

    def __unicode__(self):
        return self._insert_image()

    def __str__(self):
        return self._insert_image()

    def __html__(self):
        return self._insert_image()
