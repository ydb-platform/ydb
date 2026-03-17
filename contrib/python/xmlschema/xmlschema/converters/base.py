#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections import namedtuple
from collections.abc import Callable, Iterator, Iterable, MutableMapping, MutableSequence
from itertools import chain
from typing import TYPE_CHECKING, Any, Optional, TypeVar, Union
from xml.etree.ElementTree import Element

from xmlschema.aliases import NsmapType, BaseXsdType, XmlnsType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError
from xmlschema.namespaces import NamespaceMapper
from xmlschema.arguments import ConverterArguments
from xmlschema.resources import XMLResource
from xmlschema.utils.misc import iter_class_slots, deprecated
from xmlschema.utils.qnames import get_namespace

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement  # noqa: F401


ElementData = namedtuple('ElementData',
                         ['tag', 'text', 'content', 'attributes', 'xmlns'],
                         defaults=(None, None, None, None))
"""
Namedtuple for Element data interchange between decoders and converters.
The field *tag* is a string containing the Element's tag, *text* can be `None`
or a string representing the Element's text, *content* can be `None`, a list
containing the Element's children or a dictionary containing element name to
list of element contents for the Element's children (used for unordered input
data), *attributes* can be `None` or a dictionary containing the Element's
attributes, *xmlns* can be `None` or a list of couples containing namespace
declarations.
"""

T = TypeVar('T')


def stackable(method: Callable[..., T]) -> Callable[..., T]:
    """Mark if a converter object method supports 'stacked' xmlns processing mode."""
    method.stackable = True  # type: ignore[attr-defined]
    return method


# Default placeholder for deprecation of argument 'indent' of XMLSchemaConverter
_indent = type('int', (int,), {})(4)


class XMLSchemaConverter(NamespaceMapper):
    """
    Generic XML Schema based converter class. A converter is used to compose
    decoded XML data for an Element into a data structure and to build an Element
    from encoded data structure. There are two methods for interfacing the
    converter with the decoding/encoding process. The method *element_decode*
    accepts an ElementData tuple, containing the element parts, and returns
    a data structure. The method *element_encode* accepts a data structure and
    returns an ElementData tuple. For default character data parts are ignored.
    Prefixes and text key can be changed also using alphanumeric values but
    ambiguities with schema elements could affect XML data re-encoding.

    :param namespaces: map from namespace prefixes to URI.
    :param dict_class: dictionary class to use for decoded data. Default is `dict`.
    :param list_class: list class to use for decoded data. Default is `list`.
    :param etree_element_class: the class to use for creating new XML elements, \
    if not provided uses the ElementTree's Element class.
    :param text_key: The dictionary key of the item containing the text of the element, \
    if present and if expected by the converter.
    :param attr_prefix: controls the mapping of XML attributes, to the same name or \
    with a prefix. If `None` the converter ignores attributes.
    :param cdata_prefix: is used for including and prefixing the character data parts \
    of a mixed content, that are labeled with an integer instead of a string. \
    Character data parts are ignored if this argument is `None`.
    :param indent: number of spaces for XML indentation (default is 4).
    :param process_namespaces: whether to use namespace information in name mapping \
    methods. If set to `False` then the name mapping methods simply return the \
    provided name.
    :param strip_namespaces: if set to `True` removes namespace declarations from data and \
    namespace information from names, during decoding or encoding. Defaults to `False`.
    :param xmlns_processing: defines the processing mode of XML namespace declarations. \
    Can be 'stacked', 'collapsed', 'root-only' or 'none', with the meaning defined for \
    the `NamespaceMapper` base class. For default the xmlns processing mode is chosen \
    between 'stacked', 'collapsed' and 'none', depending on the provided XML source \
    and the capabilities and the settings of the converter instance.
    :param source: the origin of XML data. Con be an `XMLResource` instance or `None`.
    :param preserve_root: if set to `True` the root element is preserved, wrapped into a \
    single-item dictionary. Applicable only to default converter, to \
    :class:`UnorderedConverter` and to :class:`ParkerConverter`.
    :param force_dict: if set to `True` complex elements with simple content are decoded \
    with a dictionary also if there are no decoded attributes. Applicable only to default \
    converter and to :class:`UnorderedConverter`. Defaults to `False`.
    :param force_list: if set to `True` child elements are decoded within a list in any case. \
    Applicable only to default converter and to :class:`UnorderedConverter`. Defaults to `False`.

    :ivar dict_class: dictionary class to use for decoded data.
    :ivar list_class: list class to use for decoded data.
    :ivar text_key: key for decoded Element text
    :ivar attr_prefix: prefix for attribute names
    :ivar cdata_prefix: prefix for character data parts
    :ivar indent: indentation to use for rebuilding XML trees
    :ivar preserve_root: preserve the root element on decoding
    :ivar force_dict: force dictionary for complex elements with simple content
    :ivar force_list: force list for child elements
    """
    _arguments = ConverterArguments
    ns_prefix: str

    __slots__ = ('dict_class', 'list_class', 'text_key', 'ns_prefix', 'attr_prefix',
                 'cdata_prefix', 'preserve_root', 'force_dict', 'force_list')

    def __init__(self, namespaces: Optional[NsmapType] = None,
                 dict_class: Optional[type[dict[str, Any]]] = None,
                 list_class: Optional[type[list[Any]]] = None,
                 etree_element_class: Optional[type[Element]] = None,
                 text_key: Optional[str] = '$',
                 attr_prefix: Optional[str] = '@',
                 cdata_prefix: Optional[str] = None,
                 indent: int = _indent,
                 process_namespaces: bool = True,
                 strip_namespaces: bool = False,
                 xmlns_processing: Optional[str] = None,
                 source: Optional[XMLResource] = None,
                 level: int = 0,
                 preserve_root: bool = False,
                 force_dict: bool = False,
                 force_list: bool = False,
                 **kwargs: Any) -> None:

        self.dict_class: type[dict[str, Any]]
        self.list_class: type[list[Any]]

        if dict_class is not None:
            self.dict_class = dict_class
        else:
            self.dict_class = dict

        if list_class is not None:
            self.list_class = list_class
        else:
            self.list_class = list

        if etree_element_class is not None:
            self.etree_element_class = etree_element_class
        else:
            self.etree_element_class = Element

        self.dict = self.dict_class
        self.list = self.list_class
        # Deprecated attributes: will be removed in v5.0

        self.text_key = text_key
        self.attr_prefix = attr_prefix
        self.cdata_prefix = cdata_prefix
        self.ns_prefix = 'xmlns' if attr_prefix is None else f'{attr_prefix}xmlns'
        self.indent = indent
        self.preserve_root = preserve_root
        self.force_dict = force_dict
        self.force_list = force_list

        super().__init__(
            namespaces, process_namespaces, strip_namespaces, xmlns_processing, source
        )

    @property
    def xmlns_processing_default(self) -> str:
        """
        Returns the default of the xmlns processing mode, used if `None` is provided.
        """
        if isinstance(self.source, XMLResource):
            if getattr(self.element_decode, 'stackable', False):
                return 'stacked'
            else:
                return 'collapsed'
        elif getattr(self.element_encode, 'stackable', False):
            return 'stacked'
        else:
            return 'collapsed'

    @property
    def lossy(self) -> bool:
        """The converter ignores some kind of XML data during decoding/encoding."""
        return self.cdata_prefix is None or self.text_key is None or self.attr_prefix is None

    @property
    def losslessly(self) -> bool:
        """
        The XML data is decoded without loss of quality, neither on data nor on data model
        shape. Only losslessly converters can be always used to encode to an XML data that
        is strictly conformant to the schema.
        """
        return False

    @property
    def loss_xmlns(self) -> bool:
        """The converter ignores XML namespace information during decoding/encoding."""
        return not self._use_namespaces

    def replace(self, /, **kwargs: Any) -> 'XMLSchemaConverter':
        """
        Creates a new converter instance from the existing, replacing options provided
        with keyword arguments.
        """
        if 'source' in kwargs:
            kwargs['xmlns_processing'] = None

        for attr in chain(iter_class_slots(self), self.__dict__):
            if attr[0] == '_':
                continue
            elif attr not in kwargs:
                kwargs[attr] = getattr(self, attr)
            elif attr == 'indent':
                if self.indent != 4:
                    kwargs['indent'] = self.indent
            elif attr == 'etree_element_class':
                if self.etree_element_class is not Element:
                    kwargs['etree_element_class'] = self.etree_element_class

        return type(self)(**kwargs)

    @deprecated('5.0')
    def copy(self, keep_namespaces: bool = True, **kwargs: Any) -> 'XMLSchemaConverter':
        if keep_namespaces:
            kwargs.pop('namespaces', None)
        else:
            kwargs['namespaces'] = None
        return self.replace(**kwargs)

    def map_attributes(self, attributes: Iterable[tuple[str, Any]]) \
            -> Iterator[tuple[str, Any]]:
        """
        Creates an iterator for converting decoded attributes to a data structure with
        appropriate prefixes.

        :param attributes: A sequence or an iterator of couples with the name of \
        the attribute and the decoded value. Default is `None` (for `simpleType` \
        elements, that don't have attributes).
        """
        if self.attr_prefix is not None and attributes:
            for name, value in attributes:
                yield self.attr_prefix + self.map_qname(name), value

    def map_content(self, content: Iterable[tuple[str, Any, Any]]) \
            -> Iterator[tuple[str, Any, Any]]:
        """
        A generator function for converting the decoded content to a data structure.

        :param content: A sequence or an iterator of tuples with the name of the \
        element, the decoded value and the `XsdElement` instance associated.
        """
        if content:
            for name, value, xsd_child in content:
                if isinstance(name, int):
                    if self.cdata_prefix is not None:
                        yield f'{self.cdata_prefix}{name}', value, xsd_child
                elif name[0] == '{':
                    yield self.map_qname(name), value, xsd_child
                else:
                    yield name, value, xsd_child

    @deprecated('5.0')
    def etree_element(self, tag: str,
                      text: Optional[str] = None,
                      children: Optional[list[Element]] = None,
                      attrib: Optional[Union[dict[str, str], Iterable[tuple[str, str]]]] = None,
                      level: int = 0) -> Element:
        """
        Builds an ElementTree's Element using arguments and the element class and
        the indent spacing stored in the converter instance.

        :param tag: the Element tag string.
        :param text: the Element text.
        :param children: the list of Element children/subelements.
        :param attrib: a dictionary with Element attributes.
        :param level: the level related to the encoding process (0 means the root).
        :return: an instance of the Element class is set for the converter instance.
        """
        if type(self.etree_element_class) is type(Element):
            elem = self.etree_element_class(tag)
        else:
            nsmap = {prefix if prefix else None: uri
                     for prefix, uri in self.namespaces.items() if uri}
            elem = self.etree_element_class(tag, nsmap=nsmap)  # type: ignore[arg-type]

        if attrib is not None:
            elem.attrib.update(attrib)

        if children:
            elem.extend(children)
            elem.text = text or '\n' + ' ' * self.indent * (level + 1)
            elem.tail = '\n' + ' ' * self.indent * level
        else:
            elem.text = text
            elem.tail = '\n' + ' ' * self.indent * level

        return elem

    def is_xmlns(self, name: str) -> bool:
        """Returns `True` if the name is a xmlns declaration."""
        return name.startswith(self.ns_prefix) and \
            (name == self.ns_prefix or name.startswith(f'{self.ns_prefix}:'))

    def get_effective_xmlns(self, xmlns: XmlnsType, level: int,
                            xsd_element: Optional['XsdElement'] = None) -> XmlnsType:
        """
        Returns the effective xmlns for element decoding/encoding, considering the
        level and the matching XSD element. At level 0, that is the root of the
        single decoding/encoding process, all the defined namespaces are returned
        only if the XSD element is global, otherwise no namespace is returned.
        """
        if level:
            return xmlns
        elif xsd_element is None or not xsd_element.is_global():
            return None
        else:
            return [x for x in self.namespaces.items()]

    def get_xmlns_from_data(self, obj: Any) -> Optional[list[tuple[str, str]]]:
        """Returns the XML declarations from decoded element data."""
        if not self._use_namespaces or not isinstance(obj, MutableMapping):
            return None

        xmlns = []
        for name, value in obj.items():
            if name == self.ns_prefix:
                xmlns.append(('', value))
            elif name.startswith(f'{self.ns_prefix}:'):
                xmlns.append((name[len(self.ns_prefix) + 1:], value))

        return xmlns

    @stackable
    def element_decode(self, data: ElementData, xsd_element: 'XsdElement',
                       xsd_type: Optional[BaseXsdType] = None, level: int = 0) -> Any:
        """
        Converts a decoded element data to a data structure.

        :param data: ElementData instance decoded from an Element node.
        :param xsd_element: the `XsdElement` associated to decode the data.
        :param xsd_type: optional XSD type for supporting dynamic type through \
        *xsi:type* or xs:alternative.
        :param level: the level related to the decoding process (0 means the root).
        :return: a data structure containing the decoded data.
        """
        _xsd_type = xsd_type or xsd_element.type
        result_dict = self.dict_class()
        xmlns = self.get_effective_xmlns(data.xmlns, level, xsd_element)

        def keep_result_dict() -> bool:
            """
            Decide when to keep a result dict in case of an element with simple content.
            """
            if data.attributes or self.force_dict and _xsd_type.is_complex():
                return True
            elif not xmlns or not self._use_namespaces:
                return False

            namespace = get_namespace(data.tag)
            if any(x[1] == namespace for x in xmlns):
                return True

            if _xsd_type.is_qname() and isinstance(data.text, str):
                try:
                    prefix = data.text.split(':')[0]
                except IndexError:
                    prefix = ''

                if any(x[0] == prefix for x in xmlns):
                    return True

            return False

        if self._use_namespaces and xmlns:
            result_dict.update(
                (f'{self.ns_prefix}:{k}' if k else self.ns_prefix, v) for k, v in xmlns
            )

        if data.attributes:
            result_dict.update(self.map_attributes(data.attributes))

        xsd_group = _xsd_type.model_group
        if xsd_group is None or not data.content:
            if keep_result_dict():
                result_dict.update(self.map_attributes(data.attributes))
                if data.text is not None and self.text_key is not None:
                    result_dict[self.text_key] = data.text
            elif not level and self.preserve_root:
                return self.dict_class(((self.map_qname(data.tag), data.text),))
            else:
                return data.text
        else:
            if data.attributes:
                result_dict.update(self.map_attributes(data.attributes))

            has_single_group = xsd_group.is_single()
            for name, value, xsd_child in self.map_content(data.content):
                try:
                    result = result_dict[name]
                except KeyError:
                    if xsd_child is None or has_single_group and xsd_child.is_single():
                        result_dict[name] = self.list_class((value,)) if self.force_list else value
                    else:
                        result_dict[name] = self.list_class((value,))
                else:
                    if not isinstance(result, MutableSequence) or not result:
                        result_dict[name] = self.list_class((result, value))
                    elif isinstance(result[0], MutableSequence) or \
                            not isinstance(value, MutableSequence):
                        result.append(value)
                    else:
                        result_dict[name] = self.list_class((result, value))

        if not level and self.preserve_root:
            return self.dict_class(((self.map_qname(data.tag), result_dict or None),))
        return result_dict or None

    @stackable
    def element_encode(self, obj: Any, xsd_element: 'XsdElement', level: int = 0) -> ElementData:
        """
        Extracts XML decoded data from a data structure for encoding into an ElementTree.

        :param obj: the decoded object.
        :param xsd_element: the `XsdElement` associated to the decoded data structure.
        :param level: the level related to the encoding process (0 means the root).
        :return: an ElementData instance.
        """
        if level or not self.preserve_root:
            element_name = None
        elif not isinstance(obj, MutableMapping):
            raise XMLSchemaTypeError(f"A dictionary expected, got {type(obj)} instead.")
        elif len(obj) != 1:
            raise XMLSchemaValueError("The dictionary must have exactly one element.")
        else:
            element_name, obj = next(iter(obj.items()))

        if not isinstance(obj, MutableMapping):
            if xsd_element.type.simple_type is not None:
                return ElementData(xsd_element.name, obj, None, {}, None)
            elif xsd_element.type.mixed and isinstance(obj, (str, bytes)):
                return ElementData(xsd_element.name, None, [(1, obj)], {}, None)
            else:
                return ElementData(xsd_element.name, None, obj, {}, None)

        text = None
        content: list[tuple[Union[int, str], Any]] = []
        attributes = {}

        xmlns = self.set_xmlns_context(obj, level)

        if element_name is None:
            tag = xsd_element.name
        else:
            tag = self.unmap_qname(element_name)
            if not xsd_element.is_matching(tag, self.default_namespace):
                raise XMLSchemaValueError("data tag does not match XSD element name")

        for name, value in obj.items():
            if name == self.text_key:
                text = value
            elif self.cdata_prefix is not None and \
                    name.startswith(self.cdata_prefix) and \
                    (index := name[len(self.cdata_prefix):]).isdigit():
                content.append((int(index), value))
            elif self.is_xmlns(name):
                continue
            elif self.attr_prefix and \
                    name.startswith(self.attr_prefix) and \
                    (attr_name := name[len(self.attr_prefix):]):
                ns_name = self.unmap_qname(attr_name, xsd_element.attributes)
                attributes[ns_name] = value
            elif not isinstance(value, MutableSequence) or not value:
                ns_name = self.unmap_qname(name, xmlns=self.get_xmlns_from_data(value))
                content.append((ns_name, value))
            elif isinstance(value[0], (MutableMapping, MutableSequence)):
                ns_name = self.unmap_qname(name, xmlns=self.get_xmlns_from_data(value[0]))
                content.extend((ns_name, item) for item in value)
            else:
                ns_name = self.unmap_qname(name)
                xsd_child = xsd_element.match_child(ns_name)
                if xsd_child is not None:
                    if xsd_child.type and xsd_child.type.is_list():
                        content.append((ns_name, value))
                    else:
                        content.extend((ns_name, item) for item in value)
                elif self.attr_prefix == '' and ns_name in xsd_element.attributes:
                    attributes[ns_name] = value
                else:
                    content.extend((ns_name, item) for item in value)

        return ElementData(tag, text, content, attributes, xmlns)
