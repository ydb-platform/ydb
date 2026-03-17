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

from xmlschema.aliases import NsmapType, BaseXsdType
from xmlschema.resources import XMLResource

from .base import ElementData, XMLSchemaConverter

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement


class ParkerConverter(XMLSchemaConverter):
    """
    XML Schema based converter class for Parker convention.

    ref: http://wiki.open311.org/JSON_and_XML_Conversion/#the-parker-convention
    ref: https://developer.mozilla.org/en-US/docs/Archive/JXON#The_Parker_Convention

    :param namespaces: Map from namespace prefixes to URI.
    :param dict_class: dictionary class to use for decoded data. Default is `dict`.
    :param list_class: list class to use for decoded data. Default is `list`.
    :param preserve_root: If `True` the root element will be preserved. For default \
    the Parker convention remove the document root element, returning only the value.
    """
    __slots__ = ()

    def __init__(self, namespaces: NsmapType | None = None,
                 dict_class: type[dict[str, Any]] | None = None,
                 list_class: type[list[Any]] | None = None,
                 preserve_root: bool = False, **kwargs: Any) -> None:
        kwargs.update(attr_prefix=None, text_key='', cdata_prefix=None)
        super().__init__(
            namespaces, dict_class, list_class, preserve_root=preserve_root, **kwargs
        )

    @property
    def xmlns_processing_default(self) -> str:
        return 'stacked' if isinstance(self.source, XMLResource) else 'none'

    @property
    def lossy(self) -> bool:
        return True

    @property
    def loss_xmlns(self) -> bool:
        return True

    def element_decode(self, data: ElementData, xsd_element: 'XsdElement',
                       xsd_type: BaseXsdType | None = None, level: int = 0) -> Any:
        xsd_type = xsd_type or xsd_element.type
        preserve_root = self.preserve_root

        if xsd_type.model_group is None or not data.content:
            if preserve_root:
                return self.dict_class(((self.map_qname(data.tag), data.text),))
            else:
                return data.text
        else:
            result_dict = self.dict_class()
            for name, value, xsd_child in self.map_content(data.content):
                if preserve_root:
                    try:
                        if len(value) == 1:
                            value = value[name]
                    except (TypeError, KeyError):
                        pass

                try:
                    result_dict[name].append(value)
                except KeyError:
                    if isinstance(value, MutableSequence):
                        result_dict[name] = self.list_class((value,))
                    else:
                        result_dict[name] = value
                except AttributeError:
                    result_dict[name] = self.list_class((result_dict[name], value))

            for k, v in result_dict.items():
                if isinstance(v, MutableSequence) and len(v) == 1:
                    value = v.pop()
                    v.extend(value)

            if preserve_root:
                return self.dict_class(((self.map_qname(data.tag), result_dict),))
            else:
                return result_dict if result_dict else None

    def element_encode(self, obj: Any, xsd_element: 'XsdElement', level: int = 0) -> ElementData:
        tag: str = xsd_element.name
        if not isinstance(obj, MutableMapping):
            if obj == '':
                obj = None
            if xsd_element.type.simple_type is not None:
                return ElementData(tag, obj, None, {}, None)
            else:
                return ElementData(tag, None, obj, {}, None)
        else:
            if not obj:
                return ElementData(tag, None, None, {}, None)
            elif self.preserve_root:
                try:
                    items = obj[self.map_qname(tag)]
                except KeyError:
                    return ElementData(tag, None, None, {}, None)
            else:
                items = obj

            try:
                content = []
                for name, value in obj.items():
                    ns_name = self.unmap_qname(name)
                    if not isinstance(value, MutableSequence) or not value:
                        content.append((ns_name, value))
                    elif any(isinstance(v, MutableSequence) for v in value):
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

            except AttributeError:
                return ElementData(tag, items, None, {}, None)
            else:
                return ElementData(tag, None, content, {}, None)
