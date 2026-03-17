#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import MutableMapping, MutableSequence
from typing import TYPE_CHECKING, Any

from xmlschema.aliases import NsmapType, BaseXsdType, XmlnsType
from xmlschema.exceptions import XMLSchemaTypeError
from xmlschema.names import XSD_ANY_TYPE
from xmlschema.utils.qnames import local_name

from .base import ElementData, stackable, XMLSchemaConverter

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement


class BadgerFishConverter(XMLSchemaConverter):
    """
    XML Schema based converter class for Badgerfish convention.

    ref: http://www.sklar.com/badgerfish/
    ref: https://badgerfish.ning.com/

    :param namespaces: Map from namespace prefixes to URI.
    :param dict_class: dictionary class to use for decoded data. Default is `dict`.
    :param list_class: list class to use for decoded data. Default is `list`.
    """
    __slots__ = ()

    def __init__(self, namespaces: NsmapType | None = None,
                 dict_class: type[dict[str, Any]] | None = None,
                 list_class: type[list[Any]] | None = None,
                 **kwargs: Any) -> None:
        kwargs.update(attr_prefix='@', text_key='$', cdata_prefix='$')
        super().__init__(namespaces, dict_class, list_class, **kwargs)

    @property
    def lossy(self) -> bool:
        return False

    def get_xmlns_from_data(self, obj: Any) -> XmlnsType:
        if not self._use_namespaces or not isinstance(obj, MutableMapping) or '@xmlns' not in obj:
            return None
        return [(k if k != '$' else '', v) for k, v in obj['@xmlns'].items()]

    @stackable
    def element_decode(self, data: ElementData, xsd_element: 'XsdElement',
                       xsd_type: BaseXsdType | None = None, level: int = 0) -> Any:
        xsd_type = xsd_type or xsd_element.type

        tag = self.map_qname(data.tag)
        result_dict = self.dict_class(t for t in self.map_attributes(data.attributes))

        xmlns = self.get_effective_xmlns(data.xmlns, level, xsd_element)
        if self._use_namespaces and xmlns:
            result_dict['@xmlns'] = self.dict_class((k or '$', v) for k, v in xmlns)

        xsd_group = xsd_type.model_group
        if xsd_group is None or not data.content:
            if data.text is not None:
                result_dict['$'] = data.text
        else:
            has_single_group = xsd_group.is_single()
            for name, item, xsd_child in self.map_content(data.content):
                if name.startswith('$') and name[1:].isdigit():
                    result_dict[name] = item
                    continue

                assert isinstance(item, MutableMapping) and xsd_child is not None

                item = item[name]
                if name in result_dict:
                    other = result_dict[name]
                    if not isinstance(other, MutableSequence) or not other:
                        result_dict[name] = self.list_class((other, item))
                    elif isinstance(other[0], MutableSequence) or \
                            not isinstance(item, MutableSequence):
                        other.append(item)
                    else:
                        result_dict[name] = self.list_class((other, item))
                else:
                    if xsd_type.name == XSD_ANY_TYPE or \
                            has_single_group and xsd_child.is_single():
                        result_dict[name] = item
                    else:
                        result_dict[name] = self.list_class((item,))

        if self.dict_class is dict:
            return {tag: result_dict}
        return self.dict_class(((tag, result_dict),))

    @stackable
    def element_encode(self, obj: Any, xsd_element: 'XsdElement', level: int = 0) -> ElementData:
        if not isinstance(obj, MutableMapping):
            raise XMLSchemaTypeError(f"A dictionary expected, got {type(obj)} instead.")
        elif len(obj) != 1 or all(k.startswith(('$', '@')) for k in obj):
            tag = xsd_element.name
        else:
            key, value = next(iter(obj.items()))
            tag = self.unmap_qname(key, xmlns=self.get_xmlns_from_data(value))
            if xsd_element.is_matching(tag):
                obj = value
            elif not self.namespaces and local_name(tag) == xsd_element.local_name:
                obj = value
            else:
                tag = xsd_element.name

        text = None
        content: list[tuple[str | int, Any]] = []
        attributes = {}

        xmlns = self.set_xmlns_context(obj, level)

        for name, value in obj.items():
            if name == '@xmlns':
                continue
            elif name == '$':
                text = value
            elif name[0] == '$' and name[1:].isdigit():
                content.append((int(name[1:]), value))
            elif name[0] == '@':
                attr_name = name[1:]
                ns_name = self.unmap_qname(attr_name, xsd_element.attributes)
                attributes[ns_name] = value
            elif not isinstance(value, MutableSequence) or not value:
                ns_name = self.unmap_qname(name, xmlns=self.get_xmlns_from_data(value))
                content.append((ns_name, value))
            elif isinstance(value[0], (MutableMapping, MutableSequence)):
                ns_name = self.unmap_qname(name, xmlns=self.get_xmlns_from_data(value[0]))
                for item in value:
                    content.append((ns_name, item))
            else:
                ns_name = self.unmap_qname(name)
                xsd_child = xsd_element.match_child(ns_name)
                if xsd_child is not None:
                    if xsd_child.type and xsd_child.type.is_list():
                        content.append((ns_name, value))
                    else:
                        content.extend((ns_name, item) for item in value)
                else:
                    content.extend((ns_name, item) for item in value)

        return ElementData(tag, text, content, attributes, xmlns)
