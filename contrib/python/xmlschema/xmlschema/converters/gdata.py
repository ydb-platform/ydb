#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Mikhail Razgovorov <1338833@gmail.com>
#
from collections.abc import Container, MutableMapping, MutableSequence
from typing import TYPE_CHECKING, Any, Optional

from xmlschema.exceptions import XMLSchemaTypeError
from xmlschema.aliases import NsmapType, BaseXsdType
from xmlschema.names import XSD_ANY_TYPE
from xmlschema.utils.qnames import local_name

from .base import ElementData, stackable, XMLSchemaConverter

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement


class GDataConverter(XMLSchemaConverter):
    """
    XML Schema based converter class for GData protocol convention.

    ref: https://developers.google.com/gdata/docs/json

    :param namespaces: Map from namespace prefixes to URI.
    :param dict_class: dictionary class to use for decoded data. Default is `dict`.
    :param list_class: list class to use for decoded data. Default is `list`.
    :param kwargs: Additional keyword arguments to pass to base converter and \
    namespace mapper classes.
    """
    __slots__ = ()

    def __init__(self, namespaces: NsmapType | None = None,
                 dict_class: type[dict[str, Any]] | None = None,
                 list_class: type[list[Any]] | None = None,
                 **kwargs: Any) -> None:
        kwargs.update(attr_prefix='', text_key='$t', cdata_prefix='$')
        super().__init__(namespaces, dict_class, list_class, **kwargs)

    @property
    def lossy(self) -> bool:
        return True  # a child element can override an attribute in the same namespace

    def map_qname(self, qname: str) -> str:
        name = super().map_qname(qname)
        if name.startswith('{') or ':' not in name:
            return name
        else:
            return name.replace(':', '$')

    def unmap_qname(self, qname: str,
                    name_table: Container[str | None] | None = None,
                    xmlns: list[tuple[str, str]] | None = None) -> str:
        if '$' in qname and not qname.startswith('$'):
            qname = qname.replace('$', ':')
        return super().unmap_qname(qname, name_table, xmlns)

    def get_xmlns_from_data(self, obj: Any) -> Optional[list[tuple[str, str]]]:
        if not self._use_namespaces or not isinstance(obj, MutableMapping):
            return None

        xmlns = []
        for k, v in obj.items():
            if k == 'xmlns':
                xmlns.append(('', v))
            elif k.startswith('xmlns$'):
                xmlns.append((k[6:], v))
        return xmlns

    @stackable
    def element_decode(self, data: ElementData, xsd_element: 'XsdElement',
                       xsd_type: Optional[BaseXsdType] = None, level: int = 0) -> Any:
        xsd_type = xsd_type or xsd_element.type

        tag = self.map_qname(data.tag)
        result_dict = self.dict_class(t for t in self.map_attributes(data.attributes))

        xmlns = self.get_effective_xmlns(data.xmlns, level, xsd_element)
        if self._use_namespaces and xmlns:
            result_dict.update((f'xmlns${k}' if k else 'xmlns', v) for k, v in xmlns)

        xsd_group = xsd_type.model_group
        if xsd_group is None or not data.content:
            if data.text is not None:
                result_dict['$t'] = data.text
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

        return self.dict_class(((tag, result_dict),))

    @stackable
    def element_encode(self, obj: Any, xsd_element: 'XsdElement', level: int = 0) -> ElementData:
        if not isinstance(obj, MutableMapping):
            raise XMLSchemaTypeError(f"A dictionary expected, got {type(obj)} instead.")
        elif len(obj) != 1 or '$t' in obj:
            tag = xsd_element.name
        else:
            key, value = next(iter(obj.items()))
            if not isinstance(value, MutableMapping):
                tag = xsd_element.name
            else:
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
            if name == '$t':
                text = value
            elif name[0] == '$' and name[1:].isdigit():
                content.append((int(name[1:]), value))
            elif not isinstance(value, (MutableMapping, MutableSequence)):
                if name == 'xmlns' or name.startswith('xmlns$'):
                    continue  # an xmlns declaration
                ns_name = self.unmap_qname(name, xsd_element.attributes)
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
                    if isinstance(value, MutableSequence):
                        # Fallback tentative to an attribute if no element match
                        attr_name = self.unmap_qname(name, xsd_element.attributes)
                        if attr_name in xsd_element.attributes:
                            attributes[attr_name] = value
                            continue

                    content.append((ns_name, value))

        return ElementData(tag, text, content, attributes, xmlns)
