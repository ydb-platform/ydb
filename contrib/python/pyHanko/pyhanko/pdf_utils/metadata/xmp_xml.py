import re
from datetime import datetime
from io import BytesIO
from typing import (
    BinaryIO,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)
from xml.etree import ElementTree

import tzlocal
from defusedxml.ElementTree import XMLParser as DefusedXMLParser
from defusedxml.ElementTree import parse as defused_parse

from pyhanko.pdf_utils import generic, misc

from ..crypt.api import SecurityHandler
from ..misc import get_and_apply, isoparse
from . import model


def _tag(name: model.ExpandedName) -> str:
    return "{%s}%s" % (name.ns, name.local_name)


TAG_RE = re.compile(r'\{(.*)}(.*)')


def _untag(tag: str) -> Optional[model.ExpandedName]:
    m = TAG_RE.match(tag)
    if m is not None:
        return model.ExpandedName(ns=m.group(1), local_name=m.group(2))
    return None


def _name(elem: ElementTree.Element) -> Optional[model.ExpandedName]:
    return _untag(elem.tag)


def iter_attrs(
    elem: ElementTree.Element,
) -> Iterator[Tuple[model.ExpandedName, str]]:
    for attr_name, value in elem.attrib.items():
        name = _untag(attr_name)
        if name:
            yield name, value


def _xmp_struct_to_xml(
    description: ElementTree.Element, value: model.XmpStructure
):
    for k, v in value:
        if isinstance(v.value, str) and not v.qualifiers:
            # simple unqualified non-URI fields can be serialised
            # as attributes
            description.set(_tag(k), v.value)
        else:
            add_xmp_value(ElementTree.SubElement(description, _tag(k)), v)


def _add_inner_value(
    container: ElementTree.Element,
    value: Union[model.XmpStructure, model.XmpArray, model.XmpUri, str],
):
    if isinstance(value, str):
        container.text = value
        return
    if isinstance(value, model.XmpUri):
        container.set(_tag(model.RDF_RESOURCE), str(value))
        return
    elif isinstance(value, model.XmpStructure):
        description = ElementTree.SubElement(
            container,
            _tag(model.RDF_DESCRIPTION),
        )
        _xmp_struct_to_xml(description, value)
        return
    elif isinstance(value, model.XmpArray):
        arr = ElementTree.SubElement(
            container,
            _tag(value.array_type.as_rdf()),
        )
        for v in value.entries:
            add_xmp_value(ElementTree.SubElement(arr, _tag(model.RDF_LI)), v)
        return
    raise NotImplementedError(str(type(value)))


def add_xmp_value(container: ElementTree.Element, value: model.XmpValue):
    quals = value.qualifiers
    if quals.has_non_lang_quals:
        # non-lang qualifiers -> nest
        description = ElementTree.SubElement(
            container,
            _tag(model.RDF_DESCRIPTION),
        )
        for k, v in quals.iter_quals(with_lang=False):
            add_xmp_value(ElementTree.SubElement(description, _tag(k)), v)
        _add_inner_value(
            ElementTree.SubElement(description, _tag(model.RDF_VALUE)),
            value.value,
        )
    else:
        _add_inner_value(container, value.value)

    if quals.lang is not None:
        container.set(_tag(model.XML_LANG), quals.lang)


def _xmp_root_as_xml_tree(root: model.XmpStructure) -> ElementTree.ElementTree:
    description = ElementTree.Element(_tag(model.RDF_DESCRIPTION))
    _xmp_struct_to_xml(description, root)
    # manually set rdf:about="" on each of the roots
    description.set(_tag(model.RDF_ABOUT), "")
    return ElementTree.ElementTree(description)


def serialise_xmp(roots: List[model.XmpStructure], out: BinaryIO):
    out.write(
        '<?xpacket begin="\ufeff" id="W5M0MpCehiHzreSzNTczkc9d"?>\n'.encode(
            'utf-8'
        )
    )

    # some processors don't like it if all namespace declarations are dumped
    # on the xmpmeta tag, and want them lower in the tree instead
    # Hence, we write x:xmpmeta manually.
    out.write(
        f'<x:xmpmeta xmlns:x="{model.NS["x"]}" x:xmptk="{model.VENDOR}">\n'.encode(
            'utf-8'
        )
    )
    # same story for rdf:RDF
    out.write(f'<rdf:RDF xmlns:rdf="{model.NS["rdf"]}">\n'.encode('utf-8'))
    for root in roots:
        xmp_data = _xmp_root_as_xml_tree(root)
        xmp_data.write(out, xml_declaration=False, encoding='utf-8')
    out.write(b'\n</rdf:RDF>')
    out.write(b'\n</x:xmpmeta>')
    # do not allow "dumb" processors to touch the XMP, so we don't have
    # to bother with padding
    out.write(b'\n<?xpacket end="r"?>')


class MetadataStream(generic.StreamObject):
    def __init__(
        self,
        dict_data: Optional[dict] = None,
        stream_data: Optional[bytes] = None,
        encoded_data: Optional[bytes] = None,
        handler: Optional[SecurityHandler] = None,
    ):
        self._xmp: Optional[List[model.XmpStructure]] = None
        super().__init__(
            dict_data=dict_data,
            stream_data=stream_data,
            encoded_data=encoded_data,
            handler=handler,
        )
        self['/Type'] = generic.pdf_name('/Metadata')
        self['/Subtype'] = generic.pdf_name('/XML')

    @classmethod
    def from_xmp(cls, xmp: List[model.XmpStructure]) -> 'MetadataStream':
        stm = cls()
        stm._xmp = xmp
        stm._reserialise()
        return stm

    @property
    def xmp(self) -> List[model.XmpStructure]:
        if self._xmp is None:
            self._xmp = parse_xmp(BytesIO(self.data))
        return self._xmp

    def update_xmp_with_meta(self, meta: model.DocumentMetadata):
        self._xmp = update_xmp_with_meta(meta, roots=self.xmp)
        self._reserialise()
        self._encoded_data = None

    def _reserialise(self) -> bytes:
        stm = BytesIO()
        assert self._xmp is not None
        serialise_xmp(self._xmp, stm)
        self._data = data = stm.getvalue()
        return data


LANG_X_DEFAULT = model.Qualifiers.of(
    (model.XML_LANG, model.XmpValue("x-default")),
)


def _meta_string_as_value(
    meta_str: model.MetaString, lang_xdefault=False
) -> Optional[model.XmpValue]:
    if isinstance(meta_str, misc.StringWithLanguage):
        if meta_str.lang_code == "DEFAULT":
            quals = LANG_X_DEFAULT if lang_xdefault else model.Qualifiers.of()
        else:
            cc = ("-" + meta_str.country_code) if meta_str.country_code else ""
            quals = model.Qualifiers.of(
                (model.XML_LANG, model.XmpValue(f"{meta_str.lang_code}{cc}")),
            )
        return model.XmpValue(meta_str.value, quals)
    elif isinstance(meta_str, str):
        quals = LANG_X_DEFAULT if lang_xdefault else model.Qualifiers.of()
        return model.XmpValue(meta_str, quals)
    return None


def _write_meta_string(
    fields: Dict[model.ExpandedName, model.XmpValue],
    key: model.ExpandedName,
    meta_str: model.MetaString,
):
    val = _meta_string_as_value(meta_str, lang_xdefault=False)
    if val is not None:
        fields[key] = val


def _write_lang_alternative(
    fields: Dict[model.ExpandedName, model.XmpValue],
    key: model.ExpandedName,
    meta_str: model.MetaString,
):
    val = _meta_string_as_value(meta_str, lang_xdefault=True)
    if val is not None:
        fields[key] = model.XmpValue(model.XmpArray.alternative([val]))


def _write_meta_date(
    fields: Dict[model.ExpandedName, model.XmpValue],
    key: model.ExpandedName,
    meta_date: Union[datetime, str, None],
) -> bool:
    if isinstance(meta_date, datetime):
        value = meta_date
    elif meta_date == 'now':
        value = datetime.now(tz=tzlocal.get_localzone())
    else:
        return False

    fields[key] = model.XmpValue(value.replace(microsecond=0).isoformat())
    return True


def update_xmp_with_meta(
    meta: model.DocumentMetadata, roots: Iterable[model.XmpStructure] = ()
):
    # group everything first, then populate
    fields: Dict[model.ExpandedName, model.XmpValue] = {
        k: v for root in roots for k, v in root
    }
    return _populate_xmp_with_meta(meta, fields)


def _populate_xmp_with_meta(
    meta: model.DocumentMetadata,
    fields: Dict[model.ExpandedName, model.XmpValue],
) -> List[model.XmpStructure]:
    _write_meta_date(fields, model.XMP_MODDATE, meta.last_modified)
    _write_meta_string(fields, model.PDF_PRODUCER, model.VENDOR)
    if meta.xmp_unmanaged:
        return [model.XmpStructure(fields), *meta.xmp_extra]

    _write_meta_date(fields, model.XMP_CREATEDATE, meta.created)

    _write_lang_alternative(fields, model.DC_TITLE, meta.title)
    author = _meta_string_as_value(meta.author, lang_xdefault=False)
    if author is not None:
        fields[model.DC_CREATOR] = model.XmpValue(
            model.XmpArray.ordered([author])
        )
    _write_lang_alternative(fields, model.DC_DESCRIPTION, meta.subject)
    _write_meta_string(fields, model.XMP_CREATORTOOL, meta.creator)
    if meta.keywords:
        _write_meta_string(fields, model.PDF_KEYWORDS, ','.join(meta.keywords))

    return [model.XmpStructure(fields), *meta.xmp_extra]


def _parse_dt(xmp_val: model.XmpValue):
    if isinstance(xmp_val.value, str):
        dt_str = xmp_val.value
    else:
        raise XmpXmlProcessingError("Wrong type for XMP date")
    try:
        dt = isoparse(dt_str)
    except ValueError:
        raise XmpXmlProcessingError(f"Failed to parse {dt_str!r} as a date")
    return dt


def _simplify_meta_str(val: model.XmpValue) -> model.MetaString:
    result: model.MetaString = None
    focus = val
    if isinstance(val.value, model.XmpArray) and len(val.value.entries) > 0:
        # we expect this to be the case
        # (we're not going to check the array type, though)
        # we also tolerate simple values
        focus = val.value.entries[0]

    if isinstance(focus.value, str):
        val_str = focus.value
        quals = focus.qualifiers
        lang = quals.lang
        if not lang:
            result = val_str
        elif lang == "x-default":
            result = model.StringWithLanguage(val_str, lang_code="DEFAULT")
        else:
            components = lang.split('-', 1)
            result = model.StringWithLanguage(
                val_str,
                lang_code=components[0],
                country_code=(components[1] if len(components) > 1 else None),
            )
    return result


def meta_from_xmp(roots: List[model.XmpStructure]):
    all_fields: Dict[model.ExpandedName, model.XmpValue] = {
        k: v for root in roots for k, v in root
    }

    kwargs = {}
    mod_date = all_fields.get(model.XMP_MODDATE, None)
    if mod_date is not None:
        kwargs['last_modified'] = _parse_dt(mod_date)

    create_date = all_fields.get(model.XMP_CREATEDATE, None)
    if create_date is not None:
        kwargs['created'] = _parse_dt(create_date)

    title = get_and_apply(all_fields, model.DC_TITLE, _simplify_meta_str)
    if title is not None:
        kwargs['title'] = title
    author = get_and_apply(all_fields, model.DC_CREATOR, _simplify_meta_str)
    if author is not None:
        kwargs['author'] = author
    subject = get_and_apply(
        all_fields, model.DC_DESCRIPTION, _simplify_meta_str
    )
    if subject is not None:
        kwargs['subject'] = subject

    keywords = all_fields.get(model.PDF_KEYWORDS, None)
    if keywords is not None and isinstance(keywords.value, str):
        kwargs['keywords'] = keywords.value.split(',')

    creator = all_fields.get(model.XMP_CREATORTOOL, None)
    if creator is not None and isinstance(creator.value, str):
        kwargs['creator'] = creator.value

    return model.DocumentMetadata(**kwargs)


XMP_HEADER_PATTERN = re.compile(
    b'\\s*<\\?\\s?xpacket begin="(...?)" id="W5M0MpCehiHzreSzNTczkc9d"\\s?\\?>',
)

BOM_REGISTRY = {
    "\ufeff".encode(enc): enc
    for enc in ('utf-8', 'utf-16be', 'utf-16le', 'utf32')
}


class XmpXmlProcessingError(ValueError):
    pass


def _check_lang(elem: ElementTree.Element) -> Optional[str]:
    return elem.get(_tag(model.XML_LANG), None)


def _proc_xmp_struct(
    elem: ElementTree.Element, lang: Optional[str]
) -> model.XmpStructure:
    fields: Dict[model.ExpandedName, model.XmpValue] = {}
    # 'lang' can't occur on rdf:Description, so don't bother to check
    for child in elem:
        name = _name(child)
        if name is not None:
            if name in fields:
                raise XmpXmlProcessingError(
                    f"Duplicate field {name} in XMP structure value"
                )
            fields[name] = _proc_xmp_value(child, lang=lang)

    # extract attributes as unqualified simple values
    value: Union[model.XmpUri, str]
    for name, attr_value in iter_attrs(elem):
        if name != model.XML_LANG:
            if HTTP_URI_RE.match(attr_value):
                # hack to get around some popular XMP processors
                # putting URIs in places where they shouldn't go
                # (in particular: "Structure element with field attributes"
                # pattern
                value = model.XmpUri(attr_value)
            else:
                value = attr_value
            fields[name] = model.XmpValue(value)

    return model.XmpStructure(fields)


def _proc_xmp_arr(
    elem: ElementTree.Element, lang: Optional[str]
) -> model.XmpArray:
    name = _name(elem)
    if name is None:
        raise ValueError

    arr_type = {
        'Seq': model.XmpArrayType.ORDERED,
        'Bag': model.XmpArrayType.UNORDERED,
        'Alt': model.XmpArrayType.ALTERNATIVE,
    }[name.local_name]

    def _entries():
        for li in elem:
            if _name(li) == model.RDF_LI:
                yield _proc_xmp_value(li, lang=lang)

    return model.XmpArray(arr_type, list(_entries()))


def _extract_qualifiers(
    elem: ElementTree.Element, lang: Optional[str]
) -> model.Qualifiers:
    # extract the qualifiers from a Description element wrapping
    # a value
    def _quals():
        if lang:
            yield model.XML_LANG, model.XmpValue(lang)
        for q_xml in elem:
            q_name = _name(q_xml)
            if q_name != model.RDF_VALUE:
                yield q_name, _proc_xmp_value(q_xml, lang)

    return model.Qualifiers.of(*_quals())


def _unwrap_resource(elem: ElementTree.Element, lang: Optional[str]):
    # check if we're dealing with a wrapped element
    try:
        rdf_value = next(c for c in elem if _name(c) == model.RDF_VALUE)
    except StopIteration:
        rdf_value = None

    if rdf_value is not None:
        # this is the actual value, the other things are qualifiers
        inner_value = _proc_xmp_value(rdf_value, lang).value
        quals = _extract_qualifiers(elem, lang)
    else:
        # no rdf:value? -> regular structure element
        inner_value = _proc_xmp_struct(elem, lang)
        quals = model.Qualifiers.lang_as_qual(lang)
    return inner_value, quals


HTTP_URI_RE = re.compile("^https?://")


def _proc_xmp_value(
    elem: ElementTree.Element, lang: Optional[str]
) -> model.XmpValue:
    lang = _check_lang(elem) or lang
    # Step 1: check for parseType=Resource
    parse_type = elem.get(_tag(model.RDF_PARSE_TYPE), None)
    if parse_type == "Resource":
        inner_value, quals = _unwrap_resource(elem, lang=lang)
        return model.XmpValue(inner_value, quals)
    elif parse_type is not None:
        raise XmpXmlProcessingError(
            f"Parse type {parse_type!r} is not supported"
        )

    # Step 2: check if the element has children
    child_count = len(elem)
    if child_count == 0:
        # simple value
        uri_str = elem.get(_tag(model.RDF_RESOURCE), None)
        if uri_str is not None:
            return model.XmpValue(model.XmpUri(uri_str))
        elif elem.text:
            return model.XmpValue(
                elem.text, model.Qualifiers.lang_as_qual(lang)
            )
        elif elem.attrib:
            return model.XmpValue(_proc_xmp_struct(elem, lang))
        else:
            return model.XmpValue("", model.Qualifiers.lang_as_qual(lang))
    elif child_count == 1:
        # Child should be rdf:Description or one of the array types
        child = elem[0]
        name = _name(child)
        if name in (model.RDF_SEQ, model.RDF_ALT, model.RDF_BAG):
            inner_value = _proc_xmp_arr(child, lang)
            quals = model.Qualifiers.lang_as_qual(lang)
        elif name == model.RDF_DESCRIPTION:
            inner_value, quals = _unwrap_resource(child, lang)
        else:
            raise XmpXmlProcessingError(
                f"Cannot process tag with name {name} as an XMP value form"
            )
        return model.XmpValue(inner_value, quals)
    else:
        raise XmpXmlProcessingError(
            f"Tag with name {_check_lang(elem)} has more than one child."
        )


def parse_xmp(inp: BinaryIO) -> List[model.XmpStructure]:
    # parse the XMP packet header to figure out what encoding to use
    header = inp.read(128)
    header_match = XMP_HEADER_PATTERN.match(header)
    if not header_match:
        # assume the payload is UTF-8 and start decoding immediately
        # at the start
        encoding = 'utf-8'
        start_offset = 0
    else:
        bom = header_match.group(1)
        encoding = BOM_REGISTRY.get(bom, 'utf-8')
        start_offset = len(header_match.group(0))
    inp.seek(start_offset)

    # TODO this would be a lot cleaner with code gen, but that feels like
    #  overkill for a minor feature. Reevaluate later
    tree: ElementTree.ElementTree = defused_parse(
        inp, DefusedXMLParser(encoding=encoding)
    )

    root: ElementTree.Element = tree.getroot()
    root_name = _name(root)

    if root_name == model.RDF_RDF:
        rdf_root = root
    elif root_name == model.X_XMPMETA:
        try:
            rdf_root = next(c for c in root if _name(c) == model.RDF_RDF)
        except StopIteration:
            raise XmpXmlProcessingError("No rdf:RDF node in x:xmpmeta")
    else:
        raise XmpXmlProcessingError("XMP root must be rdf:RDF or x:xmpmeta")

    return [
        _proc_xmp_struct(node, lang=None)
        for node in rdf_root
        if _name(node) == model.RDF_DESCRIPTION
    ]


def register_namespaces():
    for prefix, uri in model.NS.items():
        ElementTree.register_namespace(prefix, uri)


register_namespaces()
