import os.path
import random
import re
from collections import OrderedDict
from copy import deepcopy

from docx.opc.constants import CONTENT_TYPE as CT
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from docx.opc.oxml import serialize_part_xml
from docx.opc.packuri import PackURI
from docx.opc.part import Part
from docx.oxml import parse_xml
from docx.oxml.section import CT_SectPr
from docx.parts.numbering import NumberingPart

from docxcompose.image import ImageWrapper
from docxcompose.properties import CustomProperties
from docxcompose.utils import NS
from docxcompose.utils import xpath


FILENAME_IDX_RE = re.compile("([a-zA-Z/_-]+)([1-9][0-9]*)?")
RID_IDX_RE = re.compile("rId([0-9]*)")

REFERENCED_PARTS_IGNORED_RELTYPES = set(
    [
        RT.IMAGE,
        RT.HEADER,
        RT.FOOTER,
    ]
)

PART_RELTYPES_WITH_STYLES = [
    RT.FOOTNOTES,
]


class Composer(object):

    def __init__(self, doc):
        self.doc = doc
        self.pkg = doc.part.package

        self.restart_numbering = True

        self.reset_reference_mapping()

        self.first_section_properties_added = False

    def reset_reference_mapping(self):
        self.num_id_mapping = {}
        self.anum_id_mapping = {}
        self._numbering_restarted = set()

    def append(self, doc, remove_property_fields=True):
        """Append the given document."""
        index = self.append_index()
        self.insert(index, doc, remove_property_fields=remove_property_fields)

    def insert(self, index, doc, remove_property_fields=True):
        """Insert the given document at the given index."""
        self.reset_reference_mapping()

        # Remove custom property fields but keep the values
        if remove_property_fields:
            cprops = CustomProperties(doc)
            for name in cprops.keys():
                cprops.dissolve_fields(name)

        self._create_style_id_mapping(doc)

        for element in doc.element.body:
            if isinstance(element, CT_SectPr):
                """This will lead to unexpected behaviors, for example if one
                of the added documents with landscape set for the last section
                the page orientation will get lost here. Still this is mostly
                ok, and otherwise we would need to create a section for each
                document added, i.e. move the properties into the last
                paragraph and also decide which properties we allow to overwrite
                and which should inherit from the main template."""
                continue
            element = deepcopy(element)
            self.doc.element.body.insert(index, element)
            self.add_referenced_parts(doc.part, self.doc.part, element)
            self.add_styles(doc, element)
            self.add_numberings(doc, element)
            self.restart_first_numbering(doc, element)
            self.add_images(doc, element)
            self.add_diagrams(doc, element)
            self.add_shapes(doc, element)
            self.add_footnotes(doc, element)
            self.remove_header_and_footer_references(doc, element)
            index += 1

        self.add_styles_from_other_parts(doc)
        self.renumber_bookmarks()
        self.renumber_docpr_ids()
        self.renumber_nvpicpr_ids()
        # The two methods below attempt to fix a general issue we have with
        # sections and their properties which is not correctly solved yet.
        # Right now the situation is really messy. When there is only one
        # section per document being assembled, then we remove the properties
        # of all added documents and only use the properties of the main template.
        # When a document has more than one section, then we keep the properties
        # for all the sections of that document except the last one. Also
        # note that for the first such document added, the properties of its
        # first section will get applied to the everything that came before.
        # This is because of how sections and section properties are
        # defined, i.e. sections are defined by the secPr tags inside the last
        # paragraph of a section, except for the last section which has its
        # secPr tag in the body...
        self.fix_section_types(doc)
        self.fix_header_and_footers(doc)

    def save(self, filename):
        self.doc.save(filename)

    def append_index(self):
        section_props = self.doc.element.body.xpath("w:sectPr")
        if section_props:
            return self.doc.element.body.index(section_props[0])
        return len(self.doc.element.body)

    def add_referenced_parts(self, src_part, dst_part, element):
        rid_elements = xpath(element, ".//*[@r:id]")
        for rid_element in rid_elements:
            rid = rid_element.get("{%s}id" % NS["r"])
            rel = src_part.rels[rid]
            if rel.reltype in REFERENCED_PARTS_IGNORED_RELTYPES:
                continue
            new_rel = self.add_relationship(src_part, dst_part, rel)
            rid_element.set("{%s}id" % NS["r"], new_rel.rId)

    def add_relationship(self, src_part, dst_part, relationship):
        """Add relationship and it's target part"""
        if relationship.is_external:
            new_rid = dst_part.rels.get_or_add_ext_rel(
                relationship.reltype, relationship.target_ref
            )
            return dst_part.rels[new_rid]

        part = relationship.target_part

        # Determine next partname
        name = FILENAME_IDX_RE.match(part.partname).group(1)
        used_part_numbers = [
            FILENAME_IDX_RE.match(p.partname).group(2)
            for p in dst_part.package.iter_parts()
            if p.partname.startswith(name)
        ]
        used_part_numbers = [int(idx) for idx in used_part_numbers if idx is not None]

        for n in range(1, len(used_part_numbers) + 2):
            if n not in used_part_numbers:
                next_part_number = n
                break
        next_partname = PackURI("%s%d.%s" % (name, next_part_number, part.partname.ext))

        new_part = Part(next_partname, part.content_type, part.blob, dst_part.package)
        new_rel = dst_part.rels.get_or_add(relationship.reltype, new_part)

        # Sort relationships by rId to get the same rId when adding them to the
        # new part. This avoids fixing references.
        def sort_key(r):
            match = RID_IDX_RE.match(r.rId)
            return int(match.group(1))

        for rel in sorted(part.rels.values(), key=sort_key):
            self.add_relationship(part, new_part, rel)

        return new_rel

    def add_diagrams(self, doc, element):
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

    def add_images(self, doc, element):
        """Add images from the given document used in the given element."""
        blips = xpath(element, "(.//a:blip|.//asvg:svgBlip)[@r:embed]")
        for blip in blips:
            rid = blip.get("{%s}embed" % NS["r"])
            img_part = doc.part.rels[rid].target_part

            new_img_part = self.pkg.image_parts._get_by_sha1(img_part.sha1)
            if new_img_part is None:
                image = ImageWrapper(img_part)
                new_img_part = self.pkg.image_parts._add_image_part(image)

            new_rid = self.doc.part.relate_to(new_img_part, RT.IMAGE)
            blip.set("{%s}embed" % NS["r"], new_rid)

            # handle external reference as images can be embedded and have an
            # external reference
            rid = blip.get("{%s}link" % NS["r"])
            if rid:
                rel = doc.part.rels[rid]
                new_rel = self.add_relationship(None, self.doc.part, rel)
                blip.set("{%s}link" % NS["r"], new_rel.rId)

    def add_shapes(self, doc, element):
        shapes = xpath(element, ".//v:shape/v:imagedata")
        for shape in shapes:
            rid = shape.get("{%s}id" % NS["r"])
            img_part = doc.part.rels[rid].target_part

            new_img_part = self.pkg.image_parts._get_by_sha1(img_part.sha1)
            if new_img_part is None:
                image = ImageWrapper(img_part)
                new_img_part = self.pkg.image_parts._add_image_part(image)

            new_rid = self.doc.part.relate_to(new_img_part, RT.IMAGE)
            shape.set("{%s}id" % NS["r"], new_rid)

    def add_footnotes(self, doc, element):
        """Add footnotes from the given document used in the given element."""
        footnotes_refs = element.findall(".//w:footnoteReference", NS)

        if not footnotes_refs:
            return

        footnote_part = doc.part.rels.part_with_reltype(RT.FOOTNOTES)

        my_footnote_part = self.footnote_part()

        footnotes = parse_xml(my_footnote_part.blob)
        next_id = len(footnotes) + 1

        for ref in footnotes_refs:
            id_ = ref.get("{%s}id" % NS["w"])
            element = parse_xml(footnote_part.blob)
            footnote = deepcopy(element.find('.//w:footnote[@w:id="%s"]' % id_, NS))
            footnotes.append(footnote)
            footnote.set("{%s}id" % NS["w"], str(next_id))
            ref.set("{%s}id" % NS["w"], str(next_id))
            next_id += 1

        self.add_referenced_parts(footnote_part, my_footnote_part, element)

        my_footnote_part._blob = serialize_part_xml(footnotes)

    def footnote_part(self):
        """The footnote part of the document."""
        try:
            footnote_part = self.doc.part.rels.part_with_reltype(RT.FOOTNOTES)
        except KeyError:
            # Create a new empty footnotes part
            partname = PackURI("/word/footnotes.xml")
            content_type = CT.WML_FOOTNOTES
            xml_path = os.path.join(
                os.path.dirname(__file__), "templates", "footnotes.xml"
            )
            with open(xml_path, "rb") as f:
                xml_bytes = f.read()
            footnote_part = Part(
                partname, content_type, xml_bytes, self.doc.part.package
            )
            self.doc.part.relate_to(footnote_part, RT.FOOTNOTES)
        return footnote_part

    def mapped_style_id(self, style_id):
        if style_id not in self._style_id2name:
            return style_id
        return self._style_name2id.get(self._style_id2name[style_id], style_id)

    def _create_style_id_mapping(self, doc):
        # Style ids are language-specific, but names not (always), WTF?
        # The inserted document may have another language than the composed one.
        # Thus we map the style id using the style name.
        self._style_id2name = {s.style_id: s.name for s in doc.styles}
        self._style_name2id = {s.name: s.style_id for s in self.doc.styles}

    def add_styles_from_other_parts(self, doc):
        for reltype in PART_RELTYPES_WITH_STYLES:
            try:
                el = parse_xml(doc.part.rels.part_with_reltype(reltype).blob)
            except (KeyError, ValueError):
                pass
            else:
                self.add_styles(doc, el)

    def add_styles(self, doc, element):
        """Add styles from the given document used in the given element."""
        our_style_ids = [s.style_id for s in self.doc.styles]
        # de-duplicate ids and keep order to make sure tests are not flaky
        used_style_ids = list(
            OrderedDict.fromkeys(
                [e.val for e in xpath(element, ".//w:tblStyle|.//w:pStyle|.//w:rStyle")]
            )
        )

        for style_id in used_style_ids:
            our_style_id = self.mapped_style_id(style_id)
            if our_style_id not in our_style_ids:
                style_element = deepcopy(doc.styles.element.get_by_id(style_id))
                if style_element is not None:
                    self.doc.styles.element.append(style_element)
                    self.add_numberings(doc, style_element)
                    # Also add linked styles
                    linked_style_ids = xpath(style_element, ".//w:link/@w:val")
                    if linked_style_ids:
                        linked_style_id = linked_style_ids[0]
                        our_linked_style_id = self.mapped_style_id(linked_style_id)
                        if our_linked_style_id not in our_style_ids:
                            our_linked_style = doc.styles.element.get_by_id(
                                linked_style_id
                            )
                            if our_linked_style is not None:
                                self.doc.styles.element.append(
                                    deepcopy(our_linked_style)
                                )
            else:
                # Create a mapping for abstractNumIds used in existing styles
                # This is used when adding numberings to avoid having multiple
                # <w:abstractNum> elements for the same style.
                style_element = doc.styles.element.get_by_id(style_id)
                if style_element is not None:
                    num_ids = xpath(style_element, ".//w:numId/@w:val")
                    if num_ids:
                        anum_ids = xpath(
                            doc.part.numbering_part.element,
                            './/w:num[@w:numId="%s"]/w:abstractNumId/@w:val'
                            % num_ids[0],
                        )
                        if anum_ids:
                            our_style_element = self.doc.styles.element.get_by_id(
                                our_style_id
                            )
                            our_num_ids = xpath(our_style_element, ".//w:numId/@w:val")
                            if our_num_ids:
                                numbering_part = self.numbering_part()
                                our_anum_ids = xpath(
                                    numbering_part.element,
                                    './/w:num[@w:numId="%s"]/w:abstractNumId/@w:val'
                                    % our_num_ids[0],
                                )
                                if our_anum_ids:
                                    self.anum_id_mapping[int(anum_ids[0])] = int(
                                        our_anum_ids[0]
                                    )

            # Replace language-specific style id with our style id
            if our_style_id != style_id and our_style_id is not None:
                style_elements = xpath(
                    element,
                    './/w:tblStyle[@w:val="%(styleid)s"]|'
                    './/w:pStyle[@w:val="%(styleid)s"]|'
                    './/w:rStyle[@w:val="%(styleid)s"]' % dict(styleid=style_id),
                )
                for el in style_elements:
                    el.val = our_style_id
            # Update our style ids
            our_style_ids = [s.style_id for s in self.doc.styles]

    def add_numberings(self, doc, element):
        """Add numberings from the given document used in the given element."""
        # Search for numbering references
        num_ids = set([n.val for n in xpath(element, ".//w:numId")])
        if not num_ids:
            return

        next_num_id, next_anum_id = self._next_numbering_ids()

        src_numbering_part = doc.part.numbering_part

        for num_id in num_ids:
            if num_id in self.num_id_mapping:
                continue

            # Find the referenced <w:num> element
            res = src_numbering_part.element.xpath('.//w:num[@w:numId="%s"]' % num_id)
            if not res:
                continue
            num_element = deepcopy(res[0])
            num_element.numId = next_num_id

            self.num_id_mapping[num_id] = next_num_id

            anum_id = num_element.xpath("//w:abstractNumId")[0]
            if anum_id.val not in self.anum_id_mapping:
                # Find the referenced <w:abstractNum> element
                res = src_numbering_part.element.xpath(
                    './/w:abstractNum[@w:abstractNumId="%s"]' % anum_id.val
                )
                if not res:
                    continue
                anum_element = deepcopy(res[0])
                self.anum_id_mapping[anum_id.val] = next_anum_id
                anum_id.val = next_anum_id
                # anum_element.abstractNumId = next_anum_id
                anum_element.set("{%s}abstractNumId" % NS["w"], str(next_anum_id))

                # Make sure we have a unique nsid so numberings restart properly
                nsid = anum_element.find(".//w:nsid", NS)
                if nsid is not None:
                    nsid.set(
                        "{%s}val" % NS["w"],
                        "{0:08X}".format(int(10**8 * random.random())),
                    )

                self._insert_abstract_num(anum_element)
            else:
                anum_id.val = self.anum_id_mapping[anum_id.val]

            self._insert_num(num_element)

        # Fix references
        for num_id_ref in xpath(element, ".//w:numId"):
            num_id_ref.val = self.num_id_mapping.get(num_id_ref.val, num_id_ref.val)

    def _next_numbering_ids(self):
        numbering_part = self.numbering_part()

        # Determine next unused numId (numbering starts with 1)
        current_num_ids = [n.numId for n in xpath(numbering_part.element, ".//w:num")]
        if current_num_ids:
            next_num_id = max(current_num_ids) + 1
        else:
            next_num_id = 1

        # Determine next unused abstractNumId (numbering starts with 0)
        current_anum_ids = [
            int(n)
            for n in xpath(numbering_part.element, ".//w:abstractNum/@w:abstractNumId")
        ]
        if current_anum_ids:
            next_anum_id = max(current_anum_ids) + 1
        else:
            next_anum_id = 0

        return next_num_id, next_anum_id

    def _insert_num(self, element):
        # Find position of last <w:num> element and insert after that
        numbering_part = self.numbering_part()
        nums = numbering_part.element.xpath(".//w:num")
        if nums:
            num_index = numbering_part.element.index(nums[-1])
            numbering_part.element.insert(num_index, element)
        else:
            numbering_part.element.append(element)

    def _insert_abstract_num(self, element):
        # Find position of first <w:num> element
        # We'll insert <w:abstractNum> before that
        numbering_part = self.numbering_part()
        nums = numbering_part.element.xpath(".//w:num")
        if nums:
            anum_index = numbering_part.element.index(nums[0])
        else:
            anum_index = 0
        numbering_part.element.insert(anum_index, element)

    def _replace_mapped_num_id(self, old_id, new_id):
        """Replace a mapped numId with a new one."""

        for key, value in self.num_id_mapping.items():
            if value == old_id:
                self.num_id_mapping[key] = new_id
                return

    def numbering_part(self):
        """The numbering part of the document."""
        try:
            numbering_part = self.doc.part.rels.part_with_reltype(RT.NUMBERING)
        except KeyError:
            # Create a new empty numbering part
            partname = PackURI("/word/numbering.xml")
            content_type = CT.WML_NUMBERING
            xml_path = os.path.join(
                os.path.dirname(__file__), "templates", "numbering.xml"
            )
            with open(xml_path, "rb") as f:
                xml_bytes = f.read()
            element = parse_xml(xml_bytes)
            numbering_part = NumberingPart(
                partname, content_type, element, self.doc.part.package
            )
            self.doc.part.relate_to(numbering_part, RT.NUMBERING)
        return numbering_part

    def restart_first_numbering(self, doc, element):
        if not self.restart_numbering:
            return
        style_id = xpath(element, ".//w:pStyle/@w:val")
        if not style_id:
            return
        style_id = style_id[0]
        if style_id in self._numbering_restarted:
            return
        style_element = self.doc.styles.element.get_by_id(style_id)
        if style_element is None:
            return
        outline_lvl = xpath(style_element, ".//w:outlineLvl")
        if outline_lvl:
            # Styles with an outline level are probably headings.
            # Do not restart numbering of headings
            return

        # if there is a numId referenced from the paragraph, that numId is
        # relevant, otherwise fall back to the style's numId
        local_num_id = xpath(element, ".//w:numPr/w:numId/@w:val")
        if local_num_id:
            num_id = local_num_id[0]
        else:
            style_num_id = xpath(style_element, ".//w:numId/@w:val")
            if not style_num_id:
                return
            num_id = style_num_id[0]

        numbering_part = self.numbering_part()
        num_element = xpath(numbering_part.element, './/w:num[@w:numId="%s"]' % num_id)

        if not num_element:
            # Styles with no numbering element should not be processed
            return

        anum_id = xpath(num_element[0], ".//w:abstractNumId/@w:val")[0]
        anum_element = xpath(
            numbering_part.element, './/w:abstractNum[@w:abstractNumId="%s"]' % anum_id
        )
        num_fmt = xpath(anum_element[0], './/w:lvl[@w:ilvl="0"]/w:numFmt/@w:val')
        # Do not restart numbering of bullets
        if num_fmt and num_fmt[0] == "bullet":
            return

        new_num_element = deepcopy(num_element[0])
        lvl_override = parse_xml(
            '<w:lvlOverride xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
            ' w:ilvl="0"><w:startOverride w:val="1"/></w:lvlOverride>'
        )
        new_num_element.append(lvl_override)
        next_num_id, next_anum_id = self._next_numbering_ids()
        new_num_element.numId = next_num_id
        self._insert_num(new_num_element)

        paragraph_props = xpath(
            element, './/w:pPr/w:pStyle[@w:val="%s"]/parent::w:pPr' % style_id
        )
        num_pr = xpath(paragraph_props[0], ".//w:numPr")
        if num_pr:
            num_pr = num_pr[0]
            previous_num_id = num_pr.numId.val
            self._replace_mapped_num_id(previous_num_id, next_num_id)
            num_pr.numId.val = next_num_id
        else:
            num_pr = parse_xml(
                '<w:numPr xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                '<w:ilvl w:val="0"/><w:numId w:val="%s"/></w:numPr>' % next_num_id
            )
            paragraph_props[0].append(num_pr)
        self._numbering_restarted.add(style_id)

    def header_part(self, content=None):
        """The header part of the document."""
        header_rels = [
            rel for rel in self.doc.part.rels.values() if rel.reltype == RT.HEADER
        ]
        next_id = len(header_rels) + 1
        # Create a new header part
        partname = PackURI("/word/header%s.xml" % next_id)
        content_type = CT.WML_HEADER
        if not content:
            xml_path = os.path.join(
                os.path.dirname(__file__), "templates", "header.xml"
            )
            with open(xml_path, "rb") as f:
                content = f.read()
        header_part = Part(partname, content_type, content, self.doc.part.package)
        self.doc.part.relate_to(header_part, RT.HEADER)
        return header_part

    def footer_part(self, content=None):
        """The footer part of the document."""
        footer_rels = [
            rel for rel in self.doc.part.rels.values() if rel.reltype == RT.FOOTER
        ]
        next_id = len(footer_rels) + 1
        # Create a new header part
        partname = PackURI("/word/footer%s.xml" % next_id)
        content_type = CT.WML_FOOTER
        if not content:
            xml_path = os.path.join(
                os.path.dirname(__file__), "templates", "footer.xml"
            )
            with open(xml_path, "rb") as f:
                content = f.read()
        footer_part = Part(partname, content_type, content, self.doc.part.package)
        self.doc.part.relate_to(footer_part, RT.FOOTER)
        return footer_part

    def remove_header_and_footer_references(self, doc, element):
        refs = xpath(element, ".//w:headerReference|.//w:footerReference")
        for ref in refs:
            ref.getparent().remove(ref)

    def renumber_bookmarks(self):
        bookmarks_start = xpath(self.doc.element.body, ".//w:bookmarkStart")
        bookmark_id = 0
        for bookmark in bookmarks_start:
            bookmark.set("{%s}id" % NS["w"], str(bookmark_id))
            bookmark_id += 1
        bookmarks_end = xpath(self.doc.element.body, ".//w:bookmarkEnd")
        bookmark_id = 0
        for bookmark in bookmarks_end:
            bookmark.set("{%s}id" % NS["w"], str(bookmark_id))
            bookmark_id += 1

    def renumber_docpr_ids(self):
        # Ensure that non-visual drawing properties have a unique id
        doc_prs = xpath(self.doc.element.body, ".//wp:docPr")
        doc_pr_id = 1
        for doc_pr in doc_prs:
            doc_pr.id = doc_pr_id
            doc_pr_id += 1

        parts = [
            rel.target_part
            for rel in self.doc.part.rels.values()
            if rel.reltype
            in [
                RT.HEADER,
                RT.FOOTER,
            ]
        ]
        for part in parts:
            doc_prs = xpath(part.element, ".//wp:docPr")
            for doc_pr in doc_prs:
                doc_pr.id = doc_pr_id
                doc_pr_id += 1

    def renumber_nvpicpr_ids(self):
        # Ensure that non-visual image properties have a unique id
        c_nv_prs = xpath(self.doc.element.body, ".//pic:cNvPr")
        c_nv_pr_id = 1
        for c_nv_pr in c_nv_prs:
            c_nv_pr.id = c_nv_pr_id
            c_nv_pr_id += 1

        parts = [
            rel.target_part
            for rel in self.doc.part.rels.values()
            if rel.reltype
            in [
                RT.HEADER,
                RT.FOOTER,
            ]
        ]
        for part in parts:
            c_nv_prs = xpath(part.element, ".//pic:cNvPr")
            for c_nv_pr in c_nv_prs:
                c_nv_pr.id = c_nv_pr_id
                c_nv_pr_id += 1

    def fix_section_types(self, doc):
        # The section type determines how the contents of the section will be
        # placed relative to the *previous* section.
        # The last section always stays at the end. Therefore we need to adjust
        # the type of first new section.
        # We also need to change the type of the last section of the composed
        # document to the one from the appended document.
        # TODO: Support when inserting document at an arbitrary position

        if len(self.doc.sections) == 1 or len(doc.sections) == 1:
            return

        first_new_section_idx = len(self.doc.sections) - len(doc.sections)
        self.doc.sections[first_new_section_idx].start_type = self.doc.sections[
            -1
        ].start_type
        self.doc.sections[-1].start_type = doc.sections[-1].start_type

    def fix_header_and_footers(self, doc):
        """
        The master document usually only has one section, hence its section
        properties are defined directly in the body of the document and apply
        to the last section of the document. For all other sections but the
        last one, section properties are defined in the last paragraph of
        the section.
        Headers and footers are inherited from the previous section properties
        if they are not defined in a given section. If not defined in the first
        section, then blank headers and footers will be used., so we need to
        make sure to add the definition from the main template in the first
        section of the document if there are more than one sections.
        """

        if self.first_section_properties_added:
            return

        if len(self.doc.sections) == 1 or len(doc.sections) == 1:
            return

        first_new_section_idx = len(self.doc.sections) - len(doc.sections)

        last_section = self.doc.sections[-1]
        first_section = self.doc.sections[first_new_section_idx]
        for footer_name in ("footer", "even_page_footer", "first_page_footer"):
            footer_main = getattr(last_section, footer_name)
            if not footer_main._has_definition:
                continue
            footer_sec = getattr(first_section, footer_name)
            rid = footer_main._sectPr.get_footerReference(footer_main._hdrftr_index).rId
            footer_sec._sectPr.add_footerReference(footer_main._hdrftr_index, rid)

        for header_name in ("header", "even_page_header", "first_page_header"):
            header_main = getattr(last_section, header_name)
            if not header_main._has_definition:
                continue
            header_sec = getattr(first_section, header_name)
            rid = header_main._sectPr.get_headerReference(header_main._hdrftr_index).rId
            header_sec._sectPr.add_headerReference(header_main._hdrftr_index, rid)

        # We also need to move the page number type tag to that section
        # properties and remove it from the section properties from the body.
        last_sect_pr = last_section._sectPr
        first_sect_pr = first_section._sectPr

        pg_num_types = last_sect_pr.xpath("w:pgNumType")
        for pg_num_type in pg_num_types:
            last_sect_pr.remove(pg_num_type)
            first_sect_pr.append(pg_num_type)

        self.first_section_properties_added = True
