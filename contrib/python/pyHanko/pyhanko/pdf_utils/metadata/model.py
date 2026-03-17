"""
.. versionadded:: 0.14.0

This module contains the XMP data model classes and namespace registry,
in addition to a simplified document metadata model used for automated
metadata management.
"""

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Union

from pyhanko import __version__
from pyhanko.pdf_utils.misc import StringWithLanguage

__all__ = [
    'DocumentMetadata',
    'VENDOR',
    'MetaString',
    'ExpandedName',
    'Qualifiers',
    'XmpValue',
    'XmpStructure',
    'XmpArrayType',
    'XmpArray',
    'NS',
    'XML_LANG',
    'RDF_RDF',
    'RDF_SEQ',
    'RDF_BAG',
    'RDF_ALT',
    'RDF_LI',
    'RDF_VALUE',
    'RDF_RESOURCE',
    'RDF_PARSE_TYPE',
    'RDF_ABOUT',
    'RDF_DESCRIPTION',
    'DC_TITLE',
    'DC_CREATOR',
    'DC_DESCRIPTION',
    'PDF_PRODUCER',
    'PDF_KEYWORDS',
    'X_XMPMETA',
    'X_XMPTK',
    'XMP_CREATORTOOL',
    'XMP_CREATEDATE',
    'XMP_MODDATE',
]

VENDOR = 'pyHanko ' + __version__
"""
pyHanko version identifier in textual form
"""

MetaString = Union[StringWithLanguage, str, None]
"""
A regular string, a string with a language code, or nothing at all.
"""


@dataclass
class DocumentMetadata:
    """
    Simple representation of document metadata. All entries are optional.
    """

    title: MetaString = None
    """
    The document's title.
    """

    author: MetaString = None
    """
    The document's author.
    """

    subject: MetaString = None
    """
    The document's subject.
    """

    keywords: List[str] = field(default_factory=list)
    """
    Keywords associated with the document.
    """

    creator: MetaString = None
    """
    The software that was used to author the document.

    .. note::
        This is distinct from the producer, which is typically used to indicate
        which PDF processor(s) interacted with the file.
    """

    created: Union[str, datetime, None] = None
    """
    The time when the document was created. To set it to the current time,
    specify ``now``.
    """

    last_modified: Union[str, datetime, None] = "now"
    """
    The time when the document was last modified. Defaults to the current time
    upon serialisation if not specified.
    """

    xmp_extra: List['XmpStructure'] = field(default_factory=list)
    """
    Extra XMP metadata.
    """

    xmp_unmanaged: bool = False
    """
    Flag metadata as XMP-only. This means that the info dictionary will be
    cleared out as much as possible, and that all attributes other than
    :attr:`xmp_extra` will be ignored when updating XMP metadata.

    .. note::
        The last-modified date and producer entries
        in the info dictionary will still be updated.

    .. note::
        :class:`DocumentMetadata` represents a data model that is much more
        simple than what XMP is actually capable of. You can use this flag
        if you need more fine-grained control.
    """

    def view_over(self, base: 'DocumentMetadata'):
        return DocumentMetadata(
            title=self.title or base.title,
            author=self.author or base.author,
            subject=self.subject or base.subject,
            keywords=list(self.keywords or base.keywords),
            creator=self.creator or base.creator,
            created=self.created or base.created,
            last_modified=self.last_modified,
        )


@dataclass(frozen=True)
class ExpandedName:
    """
    An expanded XML name.
    """

    ns: str
    """
    The URI of the namespace in which the name resides.
    """

    local_name: str
    """
    The local part of the name.
    """

    def __str__(self):
        ns = self.ns
        sep = '' if ns.endswith('/') or ns.endswith('#') else '/'
        return f"{ns}{sep}{self.local_name}"

    def __repr__(self):
        return str(self)


NS = {
    'xml': 'http://www.w3.org/XML/1998/namespace',
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'xmp': 'http://ns.adobe.com/xap/1.0/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'pdf': 'http://ns.adobe.com/pdf/1.3/',
    'pdfaid': 'http://www.aiim.org/pdfa/ns/id/',
    'pdfuaid': 'http://www.aiim.org/pdfua/ns/id/',
    'pdfaSchema': 'http://www.aiim.org/pdfa/ns/schema#',
    'pdfaExtension': 'http://www.aiim.org/pdfa/ns/extension/',
    'pdfaProperty': 'http://www.aiim.org/pdfa/ns/property#',
    'x': 'adobe:ns:meta/',
}
"""
Known namespaces and their customary prefixes.
"""


XML_LANG = ExpandedName(ns=NS['xml'], local_name='lang')
"""
``lang`` in the ``xml`` namespace.
"""

RDF_RDF = ExpandedName(ns=NS['rdf'], local_name='RDF')
"""
``RDF`` in the ``rdf`` namespace.
"""

RDF_SEQ = ExpandedName(ns=NS['rdf'], local_name='Seq')
"""
``Seq`` in the ``rdf`` namespace.
"""

RDF_BAG = ExpandedName(ns=NS['rdf'], local_name='Bag')
"""
``Bag`` in the ``rdf`` namespace.
"""

RDF_ALT = ExpandedName(ns=NS['rdf'], local_name='Alt')
"""
``Alt`` in the ``rdf`` namespace.
"""

RDF_LI = ExpandedName(ns=NS['rdf'], local_name='li')
"""
``li`` in the ``rdf`` namespace.
"""

RDF_VALUE = ExpandedName(ns=NS['rdf'], local_name='value')
"""
``value`` in the ``rdf`` namespace.
"""

RDF_RESOURCE = ExpandedName(ns=NS['rdf'], local_name='resource')
"""
``resource`` in the ``rdf`` namespace.
"""

RDF_ABOUT = ExpandedName(ns=NS['rdf'], local_name='about')
"""
``about`` in the ``rdf`` namespace.
"""

RDF_PARSE_TYPE = ExpandedName(ns=NS['rdf'], local_name='parseType')
"""
``parseType`` in the ``rdf`` namespace.
"""

RDF_DESCRIPTION = ExpandedName(ns=NS['rdf'], local_name='Description')
"""
``Description`` in the ``rdf`` namespace.
"""

X_XMPMETA = ExpandedName(ns=NS['x'], local_name='xmpmeta')
"""
``xmpmeta`` in the ``x`` namespace.
"""

X_XMPTK = ExpandedName(ns=NS['x'], local_name='xmptk')
"""
``xmptk`` in the ``x`` namespace.
"""


DC_TITLE = ExpandedName(ns=NS['dc'], local_name='title')
"""
``title`` in the ``dc`` namespace.
"""

DC_CREATOR = ExpandedName(ns=NS['dc'], local_name='creator')
"""
``creator`` in the ``dc`` namespace.
"""

DC_DESCRIPTION = ExpandedName(ns=NS['dc'], local_name='description')
"""
``description`` in the ``dc`` namespace.
"""

PDF_KEYWORDS = ExpandedName(ns=NS['pdf'], local_name='keywords')
"""
``keywords`` in the ``pdf`` namespace.
"""

PDF_PRODUCER = ExpandedName(ns=NS['pdf'], local_name='Producer')
"""
``Producer`` in the ``pdf`` namespace.
"""

XMP_CREATORTOOL = ExpandedName(ns=NS['xmp'], local_name='CreatorTool')
"""
``CreatorTool`` in the ``xmp`` namespace.
"""

XMP_CREATEDATE = ExpandedName(ns=NS['xmp'], local_name='CreateDate')
"""
``CreateDate`` in the ``xmp`` namespace.
"""

XMP_MODDATE = ExpandedName(ns=NS['xmp'], local_name='ModifyDate')
"""
``ModifyDate`` in the ``xmp`` namespace.
"""


class Qualifiers:
    """
    XMP value qualifiers wrapper. Implements ``__getitem__``.
    Note that ``xml:lang`` gets special treatment.

    :param quals:
        The qualifiers to model.
    """

    _quals: Dict[ExpandedName, 'XmpValue']
    _lang: Optional[str]

    def __init__(self, quals: Dict[ExpandedName, 'XmpValue']):
        self._quals = quals
        try:
            lang = quals[XML_LANG]
            del quals[XML_LANG]
            if not isinstance(lang.value, str):
                raise TypeError  # pragma: nocover
            self._lang = lang.value
        except KeyError:
            self._lang = None

    @classmethod
    def of(cls, *lst: Tuple[ExpandedName, 'XmpValue']) -> 'Qualifiers':
        """
        Construct a :class:`.Qualifiers` object from a list of name-value pairs.

        :param lst:
            A list of name-value pairs.
        :return:
            A :class:`.Qualifiers` object.
        """
        return Qualifiers({k: v for k, v in lst})

    @classmethod
    def lang_as_qual(cls, lang: Optional[str]) -> 'Qualifiers':
        """
        Construct a :class:`.Qualifiers` object that only wraps a language
        qualifier.

        :param lang:
            A language code.
        :return:
            A :class:`.Qualifiers` object.
        """
        quals = Qualifiers({})
        if lang:
            quals._lang = lang
        return quals

    def __getitem__(self, item):
        return self._quals[item]

    def iter_quals(
        self, with_lang: bool = True
    ) -> Iterable[Tuple[ExpandedName, 'XmpValue']]:
        """
        Iterate over all qualifiers.

        :param with_lang:
            Include the language qualifier.
        :return:
        """
        yield from self._quals.items()
        if with_lang and self._lang is not None:
            yield XML_LANG, XmpValue(self._lang)

    @property
    def lang(self) -> Optional[str]:
        """
        Retrieve the language qualifier, if any.
        """
        return self._lang

    @property
    def has_non_lang_quals(self) -> bool:
        """
        Check if there are any non-language qualifiers.
        """
        return bool(self._quals)

    def __bool__(self):
        return bool(self._quals or self._lang)

    def __repr__(self):
        q = dict(self._quals)
        if self._lang:
            q['lang'] = self._lang
        return f"Qualifiers({q!r})"

    def __eq__(self, other):
        return (
            isinstance(other, Qualifiers)
            and self._lang == other._lang
            and self._quals == other._quals
        )


@dataclass(frozen=True)
class XmpUri:
    """
    An XMP URI value.
    """

    value: str

    def __str__(self):
        return self.value


@dataclass
class XmpValue:
    """
    A general XMP value, potentially with qualifiers.
    """

    value: Union['XmpStructure', 'XmpArray', XmpUri, str]
    """
    The value.
    """

    qualifiers: Qualifiers = field(default_factory=Qualifiers.of)
    """
    Qualifiers that apply to the value.
    """


class XmpStructure:
    """
    A generic XMP structure value. Implements ``__getitem__`` for field access.

    :param fields:
        The structure's fields.
    """

    # isomorphic to Qualifiers, but we keep them separate to stay
    # closer to the spec (and this one doesn't special-case anything)

    def __init__(self, fields: Dict[ExpandedName, 'XmpValue']):
        self._fields: Dict[ExpandedName, XmpValue] = fields

    @classmethod
    def of(cls, *lst: Tuple[ExpandedName, 'XmpValue']) -> 'XmpStructure':
        """
        Construct an :class:`.XmpStructure` from a list of name-value pairs.

        :param lst:
            A list of name-value pairs.
        :return:
            An an :class:`.XmpStructure`.
        """
        return cls({k: v for k, v in lst})

    def __getitem__(self, item):
        return self._fields[item]

    def __iter__(self) -> Iterator[Tuple[ExpandedName, 'XmpValue']]:
        yield from self._fields.items()

    def __repr__(self):
        return f"XmpStructure({self._fields!r})"

    def __eq__(self, other):
        return isinstance(other, XmpStructure) and self._fields == other._fields


@enum.unique
class XmpArrayType(enum.Enum):
    """
    XMP array types.
    """

    ORDERED = 'Seq'
    """
    Ordered array.
    """

    UNORDERED = 'Bag'
    """
    Unordered array.
    """

    ALTERNATIVE = 'Alt'
    """
    Alternative array.
    """

    def as_rdf(self) -> ExpandedName:
        """
        Render the type as an XML name.
        """
        return ExpandedName(ns=NS['rdf'], local_name=str(self.value))


@dataclass
class XmpArray:
    """
    An XMP array.
    """

    array_type: XmpArrayType
    """
    The type of the array.
    """

    entries: List[XmpValue]
    """
    The entries in the array.
    """

    @classmethod
    def ordered(cls, lst: Iterable[XmpValue]) -> 'XmpArray':
        """
        Convert a list to an ordered XMP array.

        :param lst:
            An iterable of XMP values.
        :return:
            An ordered :class:`.XmpArray`.
        """
        return cls(XmpArrayType.ORDERED, list(lst))

    @classmethod
    def unordered(cls, lst: Iterable[XmpValue]) -> 'XmpArray':
        """
        Convert a list to an unordered XMP array.

        :param lst:
            An iterable of XMP values.
        :return:
            An unordered :class:`.XmpArray`.
        """
        return cls(XmpArrayType.UNORDERED, list(lst))

    @classmethod
    def alternative(cls, lst: Iterable[XmpValue]) -> 'XmpArray':
        """
        Convert a list to an alternative XMP array.

        :param lst:
            An iterable of XMP values.
        :return:
            An alternative :class:`.XmpArray`.
        """
        return cls(XmpArrayType.ALTERNATIVE, list(lst))

    def __eq__(self, other):
        if (
            not isinstance(other, XmpArray)
            or self.array_type != other.array_type
        ):
            return False
        if self.array_type == XmpArrayType.UNORDERED:
            return all(e in self.entries for e in other.entries) and all(
                e in other.entries for e in self.entries
            )
        else:
            return self.entries == other.entries
