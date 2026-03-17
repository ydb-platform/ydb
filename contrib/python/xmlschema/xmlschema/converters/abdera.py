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

from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.aliases import NsmapType, BaseXsdType
from xmlschema.utils.qnames import local_name
from xmlschema.resources import XMLResource

from .base import ElementData, XMLSchemaConverter

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement


class AbderaConverter(XMLSchemaConverter):
    """
    XML Schema based converter class for Abdera convention.

    ref: https://wiki.open311.org/JSON_and_XML_Conversion/#the-abdera-convention
    ref: https://cwiki.apache.org/confluence/display/ABDERA/JSON+Serialization

    :param namespaces: Map from namespace prefixes to URI.
    :param dict_class: dictionary class to use for decoded data. Default is `dict`.
    :param list_class: list class to use for decoded data. Default is `list`.
    """
    __slots__ = ()

    def __init__(self, namespaces: NsmapType | None = None,
                 dict_class: type[dict[str, Any]] | None = None,
                 list_class: type[list[Any]] | None = None,
                 **kwargs: Any) -> None:
        kwargs.update(attr_prefix='', text_key='', cdata_prefix=None)
        super().__init__(namespaces, dict_class, list_class, **kwargs)

    @property
    def xmlns_processing_default(self) -> str:
        return 'stacked' if isinstance(self.source, XMLResource) else 'none'

    @property
    def lossy(self) -> bool:
        return True  # Loss cdata parts

    @property
    def loss_xmlns(self) -> bool:
        return True

    def element_decode(self, data: ElementData, xsd_element: 'XsdElement',
                       xsd_type: BaseXsdType | None = None, level: int = 0) -> Any:
        xsd_type = xsd_type or xsd_element.type
        if xsd_type.simple_type is not None:
            children = data.text
        else:
            children = self.dict_class()
            for name, value, xsd_child in self.map_content(data.content):
                if value is None:
                    value = self.list_class()

                try:
                    children[name].append(value)
                except KeyError:
                    if isinstance(value, MutableSequence) and value:
                        children[name] = self.list_class((value,))
                    else:
                        children[name] = value
                except AttributeError:
                    children[name] = self.list_class((children[name], value))
            if not children:
                children = data.text

        result: list[Any] | dict[str, Any]
        if data.attributes:
            result = self.dict_class([
                ('attributes',
                 self.dict_class((k, v) for k, v in self.map_attributes(data.attributes)))
            ])
            if children is not None and children != []:
                result['children'] = self.list_class((children,))

        elif children is not None:
            result = children
        else:
            result = self.list_class()

        if level:
            return result
        elif self.dict_class is dict:
            return {self.map_qname(data.tag): result}
        return self.dict_class(((self.map_qname(data.tag), result),))

    def element_encode(self, obj: Any, xsd_element: 'XsdElement', level: int = 0) -> ElementData:
        if not isinstance(obj, MutableMapping):
            if not obj and isinstance(obj, MutableSequence):
                obj = None
            return ElementData(xsd_element.name, obj, None, {}, None)
        elif len(obj) != 1:
            tag = xsd_element.name
        else:
            key, value = next(iter(obj.items()))
            tag = self.unmap_qname(key)
            if xsd_element.is_matching(tag):
                obj = value
            elif not self.namespaces and local_name(tag) == xsd_element.local_name:
                obj = value
            else:
                tag = xsd_element.name

        attributes: dict[str, Any] = {}
        children: list[Any] | MutableMapping[str, Any]

        try:
            attributes.update((self.unmap_qname(k, xsd_element.attributes), v)
                              for k, v in obj['attributes'].items())
        except KeyError:
            children = obj
        else:
            children = obj.get('children', [])

        if isinstance(children, MutableMapping):
            children = [children]
        elif children and not isinstance(children[0], MutableMapping):
            if len(children) > 1:
                raise XMLSchemaValueError("Element %r should have only one child" % tag)
            else:
                return ElementData(tag, children[0], None, attributes, None)

        content = []
        for child in children:
            for name, value in child.items():
                if not isinstance(value, MutableSequence) or not value:
                    content.append((self.unmap_qname(name), value))
                elif isinstance(value[0], (MutableMapping, MutableSequence)):
                    ns_name = self.unmap_qname(name)
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

        return ElementData(tag, None, content, attributes, None)
