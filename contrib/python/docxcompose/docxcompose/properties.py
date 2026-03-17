import importlib.resources as importlib_resources
import re
from copy import deepcopy
from datetime import datetime

from babel.dates import format_datetime
from docx.opc.constants import CONTENT_TYPE as CT
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from docx.opc.oxml import serialize_part_xml
from docx.opc.packuri import PackURI
from docx.opc.part import Part
from docx.oxml import parse_xml
from docx.oxml.coreprops import CT_CoreProperties
from lxml.etree import FunctionNamespace
from lxml.etree import QName
from six import binary_type
from six import text_type

from docxcompose.utils import NS
from docxcompose.utils import word_to_python_date_format
from docxcompose.utils import xpath


CUSTOM_PROPERTY_FMTID = "{D5CDD505-2E9C-101B-9397-08002B2CF9AE}"
CUSTOM_PROPERTY_TYPES = {
    "text": '<vt:lpwstr xmlns:vt="{}"/>'.format(NS["vt"]),
    "int": '<vt:i4 xmlns:vt="{}"/>'.format(NS["vt"]),
    "bool": '<vt:bool xmlns:vt="{}"/>'.format(NS["vt"]),
    "datetime": '<vt:filetime xmlns:vt="{}"/>'.format(NS["vt"]),
    "float": '<vt:r8 xmlns:vt="{}"/>'.format(NS["vt"]),
}
MIN_PID = 2  # Property IDs have to start with 2


def value2vt(value):
    if isinstance(value, bool):
        el = parse_xml(CUSTOM_PROPERTY_TYPES["bool"])
        el.text = "true" if value else "false"
    elif isinstance(value, int):
        el = parse_xml(CUSTOM_PROPERTY_TYPES["int"])
        el.text = text_type(value)
    elif isinstance(value, float):
        el = parse_xml(CUSTOM_PROPERTY_TYPES["float"])
        el.text = text_type(value)
    elif isinstance(value, datetime):
        el = parse_xml(CUSTOM_PROPERTY_TYPES["datetime"])
        el.text = value.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif isinstance(value, text_type):
        el = parse_xml(CUSTOM_PROPERTY_TYPES["text"])
        el.text = value
    elif isinstance(value, binary_type):
        value = value.decode("utf-8")
        el = parse_xml(CUSTOM_PROPERTY_TYPES["text"])
        el.text = value
    else:
        raise TypeError("Unsupported type {}".format(type(value)))
    return el


def vt2value(element):
    tag = QName(element).localname
    if tag == "bool":
        if element.text.lower() == "true":
            return True
        else:
            return False
    elif tag in ["i1", "i2", "i4", "int", "ui1", "ui2", "ui4", "uint"]:
        return int(element.text)
    elif tag in ["r4", "r8"]:
        return float(element.text)
    elif tag == "filetime":
        return CT_CoreProperties._parse_W3CDTF_to_datetime(element.text)
    elif tag == "lpwstr":
        return element.text if element.text else ""
    else:
        return element.text


def is_text_property(property):
    tag = QName(property).localname
    return tag in ["bstr", "lpstr", "lpwstr"]


ns = FunctionNamespace(None)


# lxml doesn't support XPath 2.0 functions
# Thus we implement lower-case() as an extension function
@ns("lower-case")
def lower_case(context, a):
    return [el.lower() for el in a]


class CustomProperties(object):
    """Custom doc properties stored in ``/docProps/custom.xml``.
    Allows updating of doc properties in a document.
    """

    def __init__(self, doc):
        self.doc = doc
        self.part = None
        self._element = None
        self.language = self.get_doc_language()

        try:
            part = doc.part.package.part_related_by(RT.CUSTOM_PROPERTIES)
        except KeyError:
            self._element = parse_xml(self._part_template())
        else:
            self.part = part
            self._element = parse_xml(part.blob)

    def _part_template(self):
        return (
            importlib_resources.files("docxcompose")
            .joinpath("templates/custom.xml")
            .read_bytes()
        )

    def _update_part(self):
        if self.part is None:
            # Create a new part for custom properties
            partname = PackURI("/docProps/custom.xml")
            self.part = Part(
                partname,
                CT.OFC_CUSTOM_PROPERTIES,
                serialize_part_xml(self._element),
                self.doc.part.package,
            )
            self.doc.part.package.relate_to(self.part, RT.CUSTOM_PROPERTIES)
            self._element = parse_xml(self.part.blob)
        else:
            self.part._blob = serialize_part_xml(self._element)

    def __getitem__(self, key):
        """Get the value of a property."""
        props = xpath(
            self._element, './/cp:property[lower-case(@name)="{}"]'.format(key.lower())
        )

        if not props:
            raise KeyError(key)

        return vt2value(props[0][0])

    def __setitem__(self, key, value):
        """Set the value of a property."""
        props = xpath(
            self._element, './/cp:property[lower-case(@name)="{}"]'.format(key.lower())
        )
        if not props:
            self.add(key, value)
            return

        value_el = props[0][0]
        new_value_el = value2vt(value)
        value_el.getparent().replace(value_el, new_value_el)

        self._update_part()

    def __delitem__(self, key):
        """Delete a property."""
        props = xpath(
            self._element, './/cp:property[lower-case(@name)="{}"]'.format(key.lower())
        )

        if not props:
            raise KeyError(key)

        props[0].getparent().remove(props[0])
        # Renumber pids
        pid = MIN_PID
        for prop in self._element:
            prop.set("pid", text_type(pid))
            pid += 1

        self._update_part()

    def get_doc_language(self):
        """We actually should determine the correct language for each field.
        Instead we simply determine the language from the first w:lang tag in
        the document, and if None are found, from the w:lang tag in the default
        style.
        """
        lang_tags = xpath(self.doc.element, ".//w:lang")
        lang_tags.extend(xpath(self.doc.styles.element, ".//w:lang"))
        # keep the first tag containing a setting for Latin languages
        latin_lang_key = "{{{}}}val".format(NS["w"])
        lang_tags = [tag for tag in lang_tags if latin_lang_key in tag.keys()]
        if lang_tags:
            language = lang_tags[0].attrib[latin_lang_key]
            # babel does not support dashes in combined language codes
            return language.replace("-", "_")
        return None

    def nullify(self, key):
        """Delete key for non text-properties, set key to empty string for
        text.
        """

        props = xpath(
            self._element, './/cp:property[lower-case(@name)="{}"]'.format(key.lower())
        )

        if not props:
            raise KeyError(key)

        if is_text_property(props[0][0]):
            self[key] = ""
        else:
            del self[key]

    def __contains__(self, item):
        props = xpath(
            self._element, './/cp:property[lower-case(@name)="{}"]'.format(item.lower())
        )
        if props:
            return True
        else:
            return False

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def add(self, name, value):
        """Add a property."""
        pids = [int(pid) for pid in xpath(self._element, ".//cp:property/@pid")]
        if pids:
            pid = max(pids) + 1
        else:
            pid = MIN_PID
        prop = parse_xml('<cp:property xmlns:cp="{}"/>'.format(NS["cp"]))
        prop.set("fmtid", CUSTOM_PROPERTY_FMTID)
        prop.set("name", name)
        prop.set("pid", text_type(pid))
        value_el = value2vt(value)
        prop.append(value_el)
        self._element.append(prop)

        self._update_part()

    def keys(self):
        if self._element is None:
            return []

        props = xpath(self._element, ".//cp:property")
        return [prop.get("name") for prop in props]

    def values(self):
        if self._element is None:
            return []

        props = xpath(self._element, ".//cp:property")
        return [vt2value(prop[0]) for prop in props]

    def items(self):
        if self._element is None:
            return []

        props = xpath(self._element, ".//cp:property")
        return [(prop.get("name"), vt2value(prop[0])) for prop in props]

    def set_properties(self, properties):
        for name, value in properties.items():
            self.set(name, value)

    def find_docprops_in_document(self, name=None):
        """This method searches for all doc-properties in the document and
        in section headers and footers.
        """
        docprops = []
        for section in self.doc.sections:
            all_header_footers = [
                section.first_page_header,
                section.header,
                section.even_page_header,
                section.first_page_footer,
                section.footer,
                section.even_page_footer,
            ]
            # word seems to keep "hidden" header and footer definitions, so
            # even though some may have been deactivated via the
            # "different first page" or "different odd & even pages" checkboxes
            # the definitions will be accessible and also reactivated when the
            # checkboxes are re-enabled.
            # we deliberately bypass the `different_first_page_header_footer`
            # accessor method and check via the underlying `_has_definition`
            # method if the header/footer has a definition in xml.
            for container in all_header_footers:
                if container._has_definition and not container.is_linked_to_previous:
                    docprops.extend(
                        self._find_docprops_in(container.part.element, name=name)
                    )

        docprops.extend(self._find_docprops_in(self.doc.element.body, name=name))

        return docprops

    def _find_docprops_in(self, element, name=None):
        # First we search for the simple fields:
        sfield_nodes = xpath(
            element, ".//w:fldSimple[contains(@w:instr, 'DOCPROPERTY ')]"
        )

        docprops = [SimpleField(sfield_node) for sfield_node in sfield_nodes]

        # Now for the complex fields
        cfield_nodes = xpath(element, ".//w:instrText[contains(.,'DOCPROPERTY ')]")

        docprops.extend([ComplexField(cfield_node) for cfield_node in cfield_nodes])

        if name is not None:
            docprops = filter(lambda prop: prop.name == name, docprops)
        return docprops

    def update_all(self):
        """Update all the document's doc-properties."""
        docprops = self.find_docprops_in_document()
        available_docprops = dict(self.items())

        for docprop in docprops:
            value = available_docprops.get(docprop.name)
            if value is None:
                continue
            docprop.update(value, language=self.language)

    def update(self, name, value):
        """Update all instances of a given doc-property in the document."""
        docprops = self.find_docprops_in_document(name)
        for docprop in docprops:
            docprop.update(value, language=self.language)

    def dissolve_fields(self, name):
        """Remove the property fields but keep their value."""
        docprops = self.find_docprops_in_document(name)

        for docprop in docprops:
            docprop.replace_field_with_value()


class FieldBase(object):
    """Class used to represent a docproperty field in the document.xml."""

    fieldname_and_format_search_expr = re.compile(
        r'DOCPROPERTY +"{0,1}([^\\]*?)"{0,1} +(?:\\\@ +"{0,1}([^\\]*?)"{0,1} +){0,1}\\\* MERGEFORMAT',
        flags=re.UNICODE,
    )

    def __init__(self, field_node):
        self.node = field_node
        self.name, self.date_format = self._parse_fieldname_and_format()
        if self.date_format:
            self.date_format = word_to_python_date_format(self.date_format)
        else:
            self.date_format = "short"

    def _format_value(self, value, language=None):
        if isinstance(value, bool):
            return "Y" if value else "N"
        elif isinstance(value, datetime):
            if language is not None:
                return format_datetime(value, self.date_format, locale=language)
            return format_datetime(value, self.date_format)
        else:
            return text_type(value)

    def update(self, value, language=None):
        """Sets the value of the docproperty in the document"""
        raise NotImplementedError()

    def replace_field_with_value(self):
        """Removes the field from the document, replacing it with
        its value.
        """
        raise NotImplementedError()

    def _get_fieldname_string(self):
        raise NotImplementedError()

    def _parse_fieldname_and_format(self):
        match = self.fieldname_and_format_search_expr.search(
            self._get_fieldname_string()
        )
        if match is None:
            return None, None
        return match.groups()


class SimpleField(FieldBase):
    """Represents a simple field, i.e. <w:fldSimple> node in the
    document.xml, its body containing the value of the field.
    self.node here is the <w:fldSimple> node.
    """

    attr_name = "{{{}}}instr".format(NS["w"])

    def _get_fieldname_string(self):
        return self.node.attrib[self.attr_name]

    def update(self, value, language=None):
        text = xpath(self.node, ".//w:t")
        if text:
            text[0].text = self._format_value(value, language=language)

    def replace_field_with_value(self):
        parent = self.node.getparent()
        index = list(parent).index(self.node)
        w_r = deepcopy(self.node[0])
        parent.remove(self.node)
        parent.insert(index, w_r)


class InvalidComplexField(Exception):
    """This exception is raised when a complex field cannot
    be handled correctly."""


class ComplexField(FieldBase):
    """Represents a complex field, i.e. a several <w:r> nodes delimited by runs
    containing <w:fldChar w:fldCharType="begin"/> and <w:fldChar w:fldCharType="end"/>.
    In these fields, the actual value is stored in <w:r> nodes that come after a
    <w:r><w:fldChar w:fldCharType="separate"/></w:r> node.
    """

    XPATH_PRECEDING_BEGINS = (
        './preceding-sibling::w:r/w:fldChar[@w:fldCharType="begin"]/..'
    )
    XPATH_FOLLOWING_ENDS = './following-sibling::w:r/w:fldChar[@w:fldCharType="end"]/..'
    XPATH_FOLLOWING_SEPARATES = (
        './following-sibling::w:r/w:fldChar[@w:fldCharType="separate"]/..'
    )
    XPATH_TEXTS = "w:instrText"

    def __init__(self, field_node):
        # run and paragraph containing the field
        self.w_r = field_node.getparent()
        self.w_p = self.w_r.getparent()
        super(ComplexField, self).__init__(field_node)

    def _get_fieldname_string(self):
        """The field name can be split up in several instrText runs
        so we look for all the instrText nodes between the begin and either
        separate or end runs
        """
        separate_run = self.get_separate_run()
        last = (
            self.w_p.index(separate_run)
            if separate_run is not None
            else self.w_p.index(self.end_run)
        )
        runs = [run for run in self._runs if self.w_p.index(run) < last]
        texts = []
        for run in runs:
            texts.extend(xpath(run, self.XPATH_TEXTS))
        return "".join([each.text for each in texts])

    @property
    def begin_run(self):
        begins = xpath(self.w_r, self.XPATH_PRECEDING_BEGINS)
        if not begins:
            msg = "Complex field without begin node is not supported"
            raise InvalidComplexField(msg)
        return begins[-1]

    @property
    def end_run(self):
        if not hasattr(self, "_end_run"):
            ends = xpath(self.w_r, self.XPATH_FOLLOWING_ENDS)
            if not ends:
                msg = "Complex field without end node is not supported"
                raise InvalidComplexField(msg)
            self._end_run = ends[0]
        return self._end_run

    def get_separate_run(self):
        """The ooxml format standard says that the separate node is optional,
        so we check whether we find one in our complex field, otherwise
        we return None."""
        separates = xpath(self.w_r, self.XPATH_FOLLOWING_SEPARATES)
        if not separates:
            return None

        separate = separates[0]
        if not self.w_p.index(separate) < self.w_p.index(self.end_run):
            return None

        return separate

    @property
    def _runs(self):
        return xpath(self.begin_run, "./following-sibling::w:r")

    def get_runs_for_update(self):
        """
        Get run fields after <w:r><w:fldChar w:fldCharType="separate"/></w:r>
        """
        end_index = self.w_p.index(self.end_run)
        separate_run = self.get_separate_run()

        # if there is no separate, we have no value to update
        if separate_run is None:
            return []

        separate_index = self.w_p.index(separate_run)
        return [
            run
            for run in self._runs
            if self.w_p.index(run) > separate_index and self.w_p.index(run) < end_index
        ]

    def get_runs_to_replace_field_with_value(self):
        """
        Get all <w:r> nodes between <w:fldChar w:fldCharType="begin"/>
        and <w:fldChar w:fldCharType="separate"/> including boundaries,
        plus the <w:fldChar w:fldCharType="end"/> node
        """
        separate_run = self.get_separate_run()

        # If there is no separate, then the field has no value
        # meaning we can remove the whole field.
        if separate_run is None:
            end_index = self.w_p.index(self.end_run)
            runs = [run for run in self._runs if self.w_p.index(run) < end_index]
        else:
            separate_index = self.w_p.index(separate_run)
            runs = [run for run in self._runs if self.w_p.index(run) <= separate_index]
        runs.insert(0, self.begin_run)
        runs.append(self.end_run)
        return runs

    def update(self, value, language=None):
        runs_after_separate = self.get_runs_for_update()

        if runs_after_separate:
            first_w_r = runs_after_separate[0]
            text = xpath(first_w_r, ".//w:t")
            if text:
                text[0].text = self._format_value(value, language=language)
            # remove any additional text-nodes inside the first run. we
            # update the first text-node only with the full cached
            # docproperty value. if for some reason the initial cached
            # value is split into multiple text nodes we remove any
            # additional node after updating the first node.
            for unnecessary_w_t in text[1:]:
                first_w_r.remove(unnecessary_w_t)

            # if there are multiple runs between "separate" and "end" they
            # all may contain a piece of the cached docproperty value. we
            # can't reliably handle this situation and only update the
            # first node in the first run with the full cached value. it
            # appears any additional runs with text nodes should then be
            # removed to avoid duplicating parts of the cached docproperty
            # value.
            for run in runs_after_separate[1:]:
                text = xpath(run, ".//w:t")
                if text:
                    self.w_p.remove(run)
        else:
            # create a <w:fldChar w:fldCharType="separate"/> run using
            # the <w:fldChar w:fldCharType="begin"/> run as a template.
            # the node can contain all kind of formatting information, the
            # easiest way to preserve it seems to base new nodes on an existing
            # node.
            # we just swap out the fldCharType from begin to separate.
            separate_run = deepcopy(self.begin_run)
            w_fld_char = xpath(separate_run, "w:fldChar")[0]
            w_fld_char.set("{{{}}}fldCharType".format(NS["w"]), "separate")

            # create new run containing the actual docproperty value using
            # the <w:fldChar w:fldCharType="begin"/> run as a template.
            # the node can contain all kind of formatting information, the
            # easiest way to preserve it seems to base new nodes on an existing
            # node.
            # we drop the fldChar node and insert a text node instead.
            value_run = deepcopy(self.begin_run)
            value_run.remove(xpath(value_run, "w:fldChar")[0])
            text = parse_xml('<w:t xmlns:w="{}"></w:t>'.format(NS["w"]))
            text.text = self._format_value(value, language=language)
            value_run.append(text)

            # insert newly created nodes after the node containing the
            # docproperty field code in <w:instrText>.
            docprop_index = self.w_p.index(self.w_r)
            self.w_p.insert(docprop_index + 1, separate_run)
            self.w_p.insert(docprop_index + 2, value_run)

    def replace_field_with_value(self):
        # Get list of <w:r> nodes for removal
        runs_to_remove = self.get_runs_to_replace_field_with_value()
        for run in runs_to_remove:
            self.w_p.remove(run)
