import copy
import logging
from collections import OrderedDict, deque
from itertools import chain

from cached_property import threaded_cached_property

from zeep.exceptions import UnexpectedElementError, XMLParseError
from zeep.xsd.const import NotSet, SkipValue, Nil, xsi_ns
from zeep.xsd.elements import (
    Any, AnyAttribute, AttributeGroup, Choice, Element, Group, Sequence)
from zeep.xsd.elements.indicators import OrderIndicator
from zeep.xsd.types.any import AnyType
from zeep.xsd.types.simple import AnySimpleType
from zeep.xsd.utils import NamePrefixGenerator
from zeep.xsd.valueobjects import ArrayValue, CompoundValue

logger = logging.getLogger(__name__)

__all__ = ["ComplexType"]


class ComplexType(AnyType):
    _xsd_name = None

    def __init__(
        self,
        element=None,
        attributes=None,
        restriction=None,
        extension=None,
        qname=None,
        is_global=False,
    ):
        if element and type(element) == list:
            element = Sequence(element)

        self.name = self.__class__.__name__ if qname else None
        self._element = element
        self._attributes = attributes or []
        self._restriction = restriction
        self._extension = extension
        self._extension_types = tuple()
        super(ComplexType, self).__init__(qname=qname, is_global=is_global)

    def __call__(self, *args, **kwargs):
        if self._array_type:
            return self._array_class(*args, **kwargs)
        return self._value_class(*args, **kwargs)

    @property
    def accepted_types(self):
        return (self._value_class,) + self._extension_types

    @threaded_cached_property
    def _array_class(self):
        assert self._array_type
        return type(
            self.__class__.__name__,
            (ArrayValue,),
            {"_xsd_type": self, "__module__": "zeep.objects"},
        )

    @threaded_cached_property
    def _value_class(self):
        return type(
            self.__class__.__name__,
            (CompoundValue,),
            {"_xsd_type": self, "__module__": "zeep.objects"},
        )

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self.signature())

    @threaded_cached_property
    def attributes(self):
        generator = NamePrefixGenerator(prefix="_attr_")
        result = []
        elm_names = {name for name, elm in self.elements if name is not None}
        for attr in self._attributes_unwrapped:
            if attr.name is None:
                name = generator.get_name()
            elif attr.name in elm_names:
                name = "attr__%s" % attr.name
            else:
                name = attr.name
            result.append((name, attr))
        return result

    @threaded_cached_property
    def _attributes_unwrapped(self):
        attributes = []
        for attr in self._attributes:
            if isinstance(attr, AttributeGroup):
                attributes.extend(attr.attributes)
            else:
                attributes.append(attr)
        return attributes

    @threaded_cached_property
    def elements(self):
        """List of tuples containing the element name and the element"""
        result = []
        for name, element in self.elements_nested:
            if isinstance(element, Element):
                result.append((element.attr_name, element))
            else:
                result.extend(element.elements)
        return result

    @threaded_cached_property
    def elements_nested(self):
        """List of tuples containing the element name and the element"""
        result = []
        generator = NamePrefixGenerator()

        # Handle wsdl:arrayType objects
        if self._array_type:
            name = generator.get_name()
            if isinstance(self._element, Group):
                result = [
                    (
                        name,
                        Sequence(
                            [
                                Any(
                                    max_occurs="unbounded",
                                    restrict=self._array_type.array_type,
                                )
                            ]
                        ),
                    )
                ]
            else:
                result = [(name, self._element)]
        else:
            # _element is one of All, Choice, Group, Sequence
            if self._element:
                result.append((generator.get_name(), self._element))
        return result

    @property
    def _array_type(self):
        attrs = {attr.qname.text: attr for attr in self._attributes if attr.qname}
        array_type = attrs.get("{http://schemas.xmlsoap.org/soap/encoding/}arrayType")
        return array_type

    def parse_xmlelement(
        self, xmlelement, schema=None, allow_none=True, context=None, schema_type=None
    ):
        """Consume matching xmlelements and call parse() on each

        :param xmlelement: XML element objects
        :type xmlelement: lxml.etree._Element
        :param schema: The parent XML schema
        :type schema: zeep.xsd.Schema
        :param allow_none: Allow none
        :type allow_none: bool
        :param context: Optional parsing context (for inline schemas)
        :type context: zeep.xsd.context.XmlParserContext
        :param schema_type: The original type (not overriden via xsi:type)
        :type schema_type: zeep.xsd.types.base.Type
        :rtype: dict or None

        """
        # If this is an empty complexType (<xsd:complexType name="x"/>)
        if not self.attributes and not self.elements:
            return None

        attributes = xmlelement.attrib
        init_kwargs = OrderedDict()

        # If this complexType extends a simpleType then we have no nested
        # elements. Parse it directly via the type object. This is the case
        # for xsd:simpleContent
        if isinstance(self._element, Element) and isinstance(
            self._element.type, AnySimpleType
        ):
            name, element = self.elements_nested[0]
            init_kwargs[name] = element.type.parse_xmlelement(
                xmlelement, schema, name, context=context
            )
        else:
            elements = deque(xmlelement.iterchildren())
            if allow_none and len(elements) == 0 and len(attributes) == 0:
                return

            # Parse elements. These are always indicator elements (all, choice,
            # group, sequence)
            assert len(self.elements_nested) < 2
            for name, element in self.elements_nested:
                try:
                    result = element.parse_xmlelements(
                        elements, schema, name, context=context
                    )
                    if result:
                        init_kwargs.update(result)
                except UnexpectedElementError as exc:
                    raise XMLParseError(exc.message)

            # Check if all children are consumed (parsed)
            if elements:
                if schema.settings.strict:
                    raise XMLParseError("Unexpected element %r" % elements[0].tag)
                else:
                    init_kwargs["_raw_elements"] = elements

        # Parse attributes
        if attributes:
            attributes = copy.copy(attributes)
            for name, attribute in self.attributes:
                if attribute.name:
                    if attribute.qname.text in attributes:
                        value = attributes.pop(attribute.qname.text)
                        init_kwargs[name] = attribute.parse(value)
                else:
                    init_kwargs[name] = attribute.parse(attributes)

        value = self._value_class(**init_kwargs)
        schema_type = schema_type or self
        if schema_type and getattr(schema_type, "_array_type", None):
            value = schema_type._array_class.from_value_object(value)
        return value

    def render(self, parent, value, xsd_type=None, render_path=None):
        """Serialize the given value lxml.Element subelements on the parent
        element.

        :type parent: lxml.etree._Element
        :type value: Union[list, dict, zeep.xsd.valueobjects.CompoundValue]
        :type xsd_type: zeep.xsd.types.base.Type
        :param render_path: list

        """
        if not render_path:
            render_path = [self.name]

        if not self.elements_nested and not self.attributes:
            return

        # TODO: Implement test case for this
        if value is None:
            value = {}

        if isinstance(value, ArrayValue):
            value = value.as_value_object()

        # Render attributes
        for name, attribute in self.attributes:
            attr_value = value[name] if name in value else NotSet
            child_path = render_path + [name]
            attribute.render(parent, attr_value, child_path)

        if (
            len(self.elements_nested) == 1
            and isinstance(value, self.accepted_types)
            and not isinstance(value, (list, dict, CompoundValue))
        ):
            element = self.elements_nested[0][1]
            element.type.render(parent, value, None, child_path)
            return

        # Render sub elements
        for name, element in self.elements_nested:
            if isinstance(element, Element) or element.accepts_multiple:
                element_value = value[name] if name in value else NotSet
                child_path = render_path + [name]
            else:
                element_value = value
                child_path = list(render_path)

            # We want to explicitly skip this sub-element
            if element_value is SkipValue:
                continue

            if isinstance(element, Element):
                element.type.render(parent, element_value, None, child_path)
            else:
                element.render(parent, element_value, child_path)

        if xsd_type:
            if xsd_type._xsd_name:
                parent.set(xsi_ns("type"), xsd_type._xsd_name)
            if xsd_type.qname:
                parent.set(xsi_ns("type"), xsd_type.qname)

    def parse_kwargs(self, kwargs, name, available_kwargs):
        """Parse the kwargs for this type and return the accepted data as
        a dict.

        :param kwargs: The kwargs
        :type kwargs: dict
        :param name: The name as which this type is registered in the parent
        :type name: str
        :param available_kwargs: The kwargs keys which are still available,
         modified in place
        :type available_kwargs: set
        :rtype: dict

        """
        value = None
        name = name or self.name

        if name in available_kwargs:
            value = kwargs[name]
            available_kwargs.remove(name)

            if value is not Nil:
                value = self._create_object(value, name)

            return {name: value}
        return {}

    def _create_object(self, value, name):
        """Return the value as a CompoundValue object

        :type value: str
        :type value: list, dict, CompoundValue

        """
        if value is None:
            return None

        if isinstance(value, list) and not self._array_type:
            return [self._create_object(val, name) for val in value]

        if isinstance(value, CompoundValue) or value is SkipValue:
            return value

        if isinstance(value, dict):
            return self(**value)

        # Try to automatically create an object. This might fail if there
        # are multiple required arguments.
        return self(value)

    def resolve(self):
        """Resolve all sub elements and types"""
        if self._resolved:
            return self._resolved
        self._resolved = self

        resolved = []
        for attribute in self._attributes:
            value = attribute.resolve()
            assert value is not None
            if isinstance(value, list):
                resolved.extend(value)
            else:
                resolved.append(value)
        self._attributes = resolved

        if self._extension:
            self._extension = self._extension.resolve()
            self._resolved = self.extend(self._extension)
        elif self._restriction:
            self._restriction = self._restriction.resolve()
            self._resolved = self.restrict(self._restriction)

        if self._element:
            self._element = self._element.resolve()

        return self._resolved

    def extend(self, base):
        """Create a new ComplexType instance which is the current type
        extending the given base type.

        Used for handling xsd:extension tags

        TODO: Needs a rewrite where the child containers are responsible for
        the extend functionality.

        :type base: zeep.xsd.types.base.Type
        :rtype base: zeep.xsd.types.base.Type

        """
        if isinstance(base, ComplexType):
            base_attributes = base._attributes_unwrapped
            base_element = base._element
        else:
            base_attributes = []
            base_element = None
        attributes = base_attributes + self._attributes_unwrapped

        # Make sure we don't have duplicate (child is leading)
        if base_attributes and self._attributes_unwrapped:
            new_attributes = OrderedDict()
            for attr in attributes:
                if isinstance(attr, AnyAttribute):
                    new_attributes["##any"] = attr
                else:
                    new_attributes[attr.qname.text] = attr
            attributes = new_attributes.values()

        # If the base and the current type both have an element defined then
        # these need to be merged. The base_element might be empty (or just
        # container a placeholder element).
        element = []
        if self._element and base_element:
            self._element = self._element.resolve()
            base_element = base_element.resolve()

            element = self._element.clone(self._element.name)
            if isinstance(base_element, OrderIndicator):
                if isinstance(base_element, Choice):
                    element.insert(0, base_element)
                elif isinstance(self._element, Choice):
                    element = base_element.clone(self._element.name)
                    element.append(self._element)
                elif isinstance(element, OrderIndicator):
                    for item in reversed(base_element):
                        element.insert(0, item)
                elif isinstance(element, Group):
                    for item in reversed(base_element):
                        element.child.insert(0, item)

            elif isinstance(self._element, Group):
                raise NotImplementedError("TODO")
            else:
                pass  # Element (ignore for now)

        elif self._element or base_element:
            element = self._element or base_element
        else:
            element = Element("_value_1", base)

        new = self.__class__(
            element=element,
            attributes=attributes,
            qname=self.qname,
            is_global=self.is_global,
        )

        new._extension_types = base.accepted_types
        return new

    def restrict(self, base):
        """Create a new complextype instance which is the current type
        restricted by the base type.

        Used for handling xsd:restriction

        :type base: zeep.xsd.types.base.Type
        :rtype base: zeep.xsd.types.base.Type


        """
        attributes = list(chain(base._attributes_unwrapped, self._attributes_unwrapped))

        # Make sure we don't have duplicate (self is leading)
        if base._attributes_unwrapped and self._attributes_unwrapped:
            new_attributes = OrderedDict()
            for attr in attributes:
                if isinstance(attr, AnyAttribute):
                    new_attributes["##any"] = attr
                else:
                    new_attributes[attr.qname.text] = attr
            attributes = list(new_attributes.values())

        if base._element:
            base._element.resolve()

        new = self.__class__(
            element=self._element or base._element,
            attributes=attributes,
            qname=self.qname,
            is_global=self.is_global,
        )
        return new.resolve()

    def signature(self, schema=None, standalone=True):
        parts = []
        for name, element in self.elements_nested:
            part = element.signature(schema, standalone=False)
            parts.append(part)

        for name, attribute in self.attributes:
            part = "%s: %s" % (name, attribute.signature(schema, standalone=False))
            parts.append(part)

        value = ", ".join(parts)
        if standalone:
            return "%s(%s)" % (self.get_prefixed_name(schema), value)
        else:
            return value
