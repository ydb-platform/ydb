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
from typing import TYPE_CHECKING, Any, Union

from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError

from .base import ElementData, stackable, XMLSchemaConverter

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement


class UnorderedConverter(XMLSchemaConverter):
    """
    Same as :class:`XMLSchemaConverter` but :meth:`XMLSchemaConverter.element_encode`
    returns a dictionary for the content of the element, that can be used directly
    for unordered encoding mode. In this mode the order of the elements in
    the encoded output is based on the model visitor pattern rather than
    the order in which the elements were added to the input dictionary.
    As the order of the input dictionary is not preserved, character data
    between sibling elements are interleaved between tags.
    """
    __slots__ = ()

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
        attributes = {}

        # The unordered encoding mode assumes that the values of this dict will
        # all be lists where each item is the content of a single element. When
        # building content_lu, content which is not a list or lists to be placed
        # into a single element (element has a list content type) must be wrapped
        # in a list to retain that structure. Character data are not wrapped into
        # lists because they are divided from the rest of the content into the
        # unordered mode generator function of the ModelVisitor class.
        content_lu: dict[Union[int, str], Any] = {}

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
                content_lu[int(index)] = value
            elif self.is_xmlns(name):
                continue
            elif self.attr_prefix and \
                    name.startswith(self.attr_prefix) and \
                    (attr_name := name[len(self.attr_prefix):]):
                ns_name = self.unmap_qname(attr_name, xsd_element.attributes)
                attributes[ns_name] = value
            elif not isinstance(value, MutableSequence) or not value:
                ns_name = self.unmap_qname(name, xmlns=self.get_xmlns_from_data(value))
                content_lu[ns_name] = [value]
            elif isinstance(value[0], (MutableMapping, MutableSequence)):
                ns_name = self.unmap_qname(name, xmlns=self.get_xmlns_from_data(value[0]))
                content_lu[ns_name] = value
            else:
                # `value` is a list but not a list of lists or list of dicts.
                ns_name = self.unmap_qname(name)
                xsd_child = xsd_element.match_child(ns_name)
                if xsd_child is not None:
                    if xsd_child.type and xsd_child.type.is_list():
                        content_lu[ns_name] = [value]
                    else:
                        content_lu[ns_name] = value
                elif self.attr_prefix == '' and ns_name in xsd_element.attributes:
                    attributes[ns_name] = value
                else:
                    content_lu[ns_name] = value

        return ElementData(tag, text, content_lu, attributes, xmlns)
