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

from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError
from xmlschema.aliases import NsmapType, BaseXsdType
from xmlschema.resources import XMLResource

from .base import ElementData, XMLSchemaConverter

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement


class ColumnarConverter(XMLSchemaConverter):
    """
    XML Schema based converter class for columnar formats.

    :param namespaces: map from namespace prefixes to URI.
    :param dict_class: dictionary class to use for decoded data. Default is `dict`.
    :param list_class: list class to use for decoded data. Default is `list`.
    :param attr_prefix: used as separator string for renaming the decoded attributes. \
    Can be the empty string (the default) or a single/double underscore.
    """
    __slots__ = ()

    def __init__(self, namespaces: NsmapType | None = None,
                 dict_class: type[dict[str, Any]] | None = None,
                 list_class: type[list[Any]] | None = None,
                 attr_prefix: str | None = '',
                 **kwargs: Any) -> None:
        kwargs.update(text_key=None, cdata_prefix=None)
        super().__init__(namespaces, dict_class, list_class,
                         attr_prefix=attr_prefix, **kwargs)

    @property
    def xmlns_processing_default(self) -> str:
        return 'stacked' if isinstance(self.source, XMLResource) else 'none'

    @property
    def lossy(self) -> bool:
        return True  # Loss cdata parts

    @property
    def loss_xmlns(self) -> bool:
        return True

    def __setattr__(self, name: str, value: Any) -> None:
        if name != 'attr_prefix':
            super().__setattr__(name, value)
        elif not isinstance(value, str):
            msg = "%(name)r must be a <class 'str'> instance, not %(type)r"
            raise XMLSchemaTypeError(msg % {'name': name, 'type': type(value)})
        elif value not in ('', '_', '__'):
            msg = '%r can be the empty string or a single/double underscore'
            raise XMLSchemaValueError(msg % name)
        else:
            super(XMLSchemaConverter, self).__setattr__(name, value)

    def element_decode(self, data: ElementData, xsd_element: 'XsdElement',
                       xsd_type: BaseXsdType | None = None, level: int = 0) -> Any:
        result_dict: Any

        xsd_type = xsd_type or xsd_element.type
        if data.attributes:
            if self.attr_prefix:
                pfx = xsd_element.local_name + self.attr_prefix
            else:
                pfx = xsd_element.local_name
            result_dict = self.dict_class((pfx + self.map_qname(k), v) for k, v in data.attributes)
        else:
            result_dict = self.dict_class()

        if xsd_type.simple_type is not None:
            result_dict[xsd_element.local_name] = data.text

        if data.content:
            for name, value, xsd_child in self.map_content(data.content):
                if not value:
                    continue
                elif xsd_child.local_name:
                    name = xsd_child.local_name
                else:
                    name = name[2 + len(xsd_child.namespace):]

                if xsd_child.is_single():
                    if xsd_child.type is not None and xsd_child.type.simple_type is not None:
                        for k in value:
                            result_dict[k] = value[k]
                    else:
                        result_dict[name] = value
                else:
                    if xsd_child.type is not None and xsd_child.type.simple_type is not None \
                            and not xsd_child.attributes:
                        if len(xsd_element.findall('*')) == 1:
                            try:
                                result_dict.append(list(value.values())[0])
                            except AttributeError:
                                result_dict = self.list_class(value.values())
                        else:
                            try:
                                result_dict[name].append(list(value.values())[0])
                            except KeyError:
                                result_dict[name] = self.list_class(value.values())
                            except AttributeError:
                                result_dict[name] = self.list_class(value.values())
                    else:
                        try:
                            result_dict[name].append(value)
                        except KeyError:
                            result_dict[name] = self.list_class([value])
                        except AttributeError:
                            result_dict[name] = self.list_class([value])

        if level == 0:
            return self.dict_class([(xsd_element.local_name, result_dict)])
        else:
            return result_dict

    def element_encode(self, obj: Any, xsd_element: 'XsdElement', level: int = 0) -> ElementData:
        if level != 0:
            tag = xsd_element.local_name
        else:
            tag = xsd_element.local_name
            try:
                obj = obj[tag]
            except (KeyError, AttributeError, TypeError):
                pass

        if not isinstance(obj, MutableMapping):
            if xsd_element.type.simple_type is not None:
                return ElementData(xsd_element.name, obj, None, {}, None)
            elif xsd_element.type.mixed and not isinstance(obj, MutableSequence):
                return ElementData(xsd_element.name, obj, None, {}, None)
            else:
                return ElementData(xsd_element.name, None, obj, {}, None)

        text = None
        content: list[tuple[str | None, MutableSequence[Any]]] = []
        attributes = {}
        pfx = tag + self.attr_prefix if self.attr_prefix else tag

        for name, value in obj.items():
            if name == tag:
                text = value
            elif name.startswith(pfx) and len(name) > len(pfx):
                attr_name = name[len(pfx):]
                ns_name = self.unmap_qname(attr_name, xsd_element.attributes)
                attributes[ns_name] = value
            elif not isinstance(value, MutableSequence) or not value:
                content.append((self.unmap_qname(name), value))
            elif isinstance(value[0], (MutableMapping, MutableSequence)):
                ns_name = self.unmap_qname(name)
                content.extend((ns_name, item) for item in value)
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

        return ElementData(xsd_element.name, text, content, attributes, None)
