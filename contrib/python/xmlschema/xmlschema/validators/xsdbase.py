#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
This module contains base functions and classes XML Schema components.
"""
import logging
from collections.abc import Iterator, MutableMapping
from functools import cached_property
from typing import TYPE_CHECKING, cast, Any, Optional, Union

from elementpath import select
from elementpath.etree import etree_tostring

import xmlschema.names as nm
from xmlschema.exceptions import XMLSchemaTypeError
from xmlschema.aliases import ElementType, NsmapType, SchemaType, BaseXsdType, \
    ComponentClassType, DecodedValueType, ModelParticleType
from xmlschema.translation import gettext as _
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.utils.qnames import get_qname, local_name, get_prefixed_qname
from xmlschema.utils.etree import is_etree_element
from xmlschema.utils.logger import format_xmlschema_stack, dump_data
from xmlschema.arguments import check_validation_mode
from xmlschema.resources import XMLResource
from xmlschema.caching import schema_cache

from .validation import ValidationContext
from .exceptions import XMLSchemaParseError, XMLSchemaNotBuiltError
from .helpers import get_xsd_annotation, parse_target_namespace

if TYPE_CHECKING:
    from xmlschema.validators import XsdSimpleType, XsdElement, XsdGroup, XsdGlobals  # noqa: F401

logger = logging.getLogger('xmlschema')

XSD_TYPE_DERIVATIONS = ('extension', 'restriction')
XSD_ELEMENT_DERIVATIONS = ('extension', 'restriction', 'substitution')


class XsdValidator:
    """
    Common base class for XML Schema validator, that represents a PSVI (Post Schema Validation
    Infoset) information item. A concrete XSD validator have to report its validity collecting
    building errors and implementing the properties.

    :param validation: defines the XSD validation mode to use for build the validator, \
    its value can be 'strict', 'lax' or 'skip'. Strict mode is the default.
    :type validation: str

    :ivar validation: XSD validation mode.
    :vartype validation: str
    :ivar errors: XSD validator building errors.
    :vartype errors: list
    """
    def __init__(self, validation: str = 'strict') -> None:
        self.validation = validation
        self.errors: list[XMLSchemaParseError] = []

    @classmethod
    def _mro_slots(cls) -> Iterator[str]:
        for c in cls.__mro__:
            if hasattr(c, '__slots__'):
                yield from c.__slots__

    @classmethod
    def _cached_properties(cls) -> Iterator[str]:
        for k in dir(cls):
            if isinstance(getattr(cls, k), cached_property):
                yield k

    def __getstate__(self) -> dict[str, Any]:
        state = {attr: getattr(self, attr) for attr in self._mro_slots()}
        state.update(self.__dict__)
        for k in self._cached_properties():
            state.pop(k, None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        for attr, value in state.items():
            object.__setattr__(self, attr, value)

    @property
    def built(self) -> bool:
        """Defines whether the XSD validator has been fully parsed and built"""
        raise NotImplementedError()

    def build(self) -> None:
        """
        Build the validator and its components. Do nothing if the validator is already built.
        """
        raise NotImplementedError()

    @property
    def validation_attempted(self) -> str:
        """
        Property that returns the *validation status* of the XSD validator.
        It can be 'full', 'partial' or 'none'.

        | https://www.w3.org/TR/xmlschema-1/#e-validation_attempted
        | https://www.w3.org/TR/2012/REC-xmlschema11-1-20120405/#e-validation_attempted
        """
        raise NotImplementedError()

    @property
    def validity(self) -> str:
        """
        Property that returns the XSD validator's validity.
        It can be ‘valid’, ‘invalid’ or ‘notKnown’.

        | https://www.w3.org/TR/xmlschema-1/#e-validity
        | https://www.w3.org/TR/2012/REC-xmlschema11-1-20120405/#e-validity
        """
        if self.validation == 'skip':
            return 'notKnown'
        elif self.errors:
            return 'invalid'
        elif self.validation_attempted == 'full':
            return 'valid'
        else:
            return 'notKnown'

    def check_validator(self, validation: Optional[str] = None) -> None:
        """Checks the status of a schema validator against a validation mode."""
        if validation is None:
            # Validator self-check
            validation = self.validation
            if self.validation_attempted == 'none' and self.validity == 'notKnown':
                return
        else:
            # Check called before validation
            check_validation_mode(validation)

        if self.validation_attempted == 'none' and validation != 'skip':
            msg = _("%r is not built") % self
            raise XMLSchemaNotBuiltError(self, msg)

        if validation == 'strict':
            if self.validation_attempted != 'full':
                msg = _("validation mode is 'strict' and %r is not built") % self
                raise XMLSchemaNotBuiltError(self, msg)
            if self.validity != 'valid':
                msg = _("validation mode is 'strict' and %r is not valid") % self
                raise XMLSchemaNotBuiltError(self, msg)

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[Union['XsdComponent', SchemaType, 'XsdGlobals']]:
        """
        Creates an iterator for traversing all XSD components of the validator.

        :param xsd_classes: returns only a specific class/classes of components, \
        otherwise returns all components.
        """
        raise NotImplementedError()

    @property
    def all_errors(self) -> list[XMLSchemaParseError]:
        """
        A list with all the building errors of the XSD validator and its components.
        """
        errors = []
        for comp in self.iter_components():
            if comp.errors:
                errors.extend(comp.errors)
        return errors

    @property
    def total_errors(self) -> int:
        return sum(len(comp.errors) for comp in self.iter_components())

    def __copy__(self) -> 'XsdValidator':
        validator: 'XsdValidator' = object.__new__(self.__class__)
        validator.validation = self.validation
        validator.errors = self.errors.copy()
        return validator

    def parse_error(self, error: Union[str, Exception],
                    elem: Optional[ElementType] = None,
                    namespaces: Optional[NsmapType] = None) -> None:
        """
        Helper method for registering parse errors. Does nothing if validation mode is 'skip'.
        Il validation mode is 'lax' collects the error, otherwise raise the error.

        :param error: can be a parse error or an error message.
        :param elem: the Element instance related to the error, for default uses the 'elem' \
        attribute of the validator, if it's present.
        :param namespaces: overrides the namespaces of the validator, or provides a mapping \
        if the validator hasn't a namespaces attribute.
        """
        if self.validation == 'skip':
            return
        elif elem is None:
            elem = getattr(self, 'elem', None)
        elif not is_etree_element(elem):
            msg = "the argument 'elem' must be an Element instance, not {!r}."
            raise XMLSchemaTypeError(msg.format(elem))

        if namespaces is None:
            namespaces = getattr(self, 'namespaces', None)

        if isinstance(error, XMLSchemaParseError):
            if error.namespaces is None:
                error.namespaces = namespaces
            if error.elem is None:
                error.elem = elem
            if error.source is None:
                error.source = getattr(self, 'source', None)
        elif isinstance(error, Exception):
            message = str(error).strip()
            if message[0] in '\'"' and message[0] == message[-1]:
                message = message.strip('\'"')
            error = XMLSchemaParseError(self, message, elem, namespaces=namespaces)
        elif isinstance(error, str):
            error = XMLSchemaParseError(self, error, elem, namespaces=namespaces)
        else:
            msg = "'error' argument must be an exception or a string, not {!r}."
            raise XMLSchemaTypeError(msg.format(error))

        if self.validation == 'lax':
            if error.stack_trace is None and logger.level == logging.DEBUG:
                error.stack_trace = format_xmlschema_stack('xmlschema/validators')
                logger.debug("Collect %r with traceback:\n%s", error, error.stack_trace)

            self.errors.append(error)
        else:
            raise error


class XsdComponent(XsdValidator):
    """
    Class for XSD components. See: https://www.w3.org/TR/xmlschema-ref/

    :param elem: ElementTree's node containing the definition.
    :param schema: the XMLSchema object that owns the definition.
    :param parent: the XSD parent, `None` means that is a global component that \
    has the schema as parent.
    :param name: name of the component, maybe overwritten by the parse of the `elem` argument.

    """
    @classmethod
    def meta_tag(cls) -> str:
        """The reference tag for the component type."""
        try:
            return cls._ADMITTED_TAGS[0]
        except IndexError:
            raise NotImplementedError(f"not available for {cls!r}")

    _ADMITTED_TAGS: Union[tuple[str, ...], tuple[()]] = ()
    maps: 'XsdGlobals'
    elem: ElementType
    target_namespace: str

    qualified: bool = True
    """For name matching, unqualified matching may be admitted only for elements and attributes"""

    ref: Optional['XsdComponent']
    """Defined if the component is a reference to a global component."""

    redefine: Optional['XsdComponent']
    """Defined if the component is a redefinition/override of another component."""

    _built: bool | None
    """
    A three-state flag for determining if a component is built or not. A `None`
    value means that a component is under construction, to avoid to rebuild a
    component more times for a circularity between elements and groups. The
    lock for threads is on global map.
    """

    __slots__ = ('name', 'parent', 'schema', 'xsd_version', 'target_namespace', 'maps',
                 'builders', 'elem', 'validation', 'errors', 'ref', 'redefine', '_built')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: Optional['XsdComponent'] = None,
                 name: Optional[str] = None) -> None:

        super().__init__(schema.validation)
        self._built = False
        self.ref = self.redefine = None
        self.name = name
        self.parent = parent
        self.schema = schema
        self.xsd_version = schema.XSD_VERSION
        self.target_namespace = schema.target_namespace
        self.maps = schema.maps
        self.builders = schema.builders
        self.parse(elem)

    def __repr__(self) -> str:
        if self.ref is not None:
            return '%s(ref=%r)' % (self.__class__.__name__, self.prefixed_name)
        else:
            return '%s(name=%r)' % (self.__class__.__name__, self.prefixed_name)

    def __copy__(self) -> 'XsdComponent':
        component: 'XsdComponent' = object.__new__(self.__class__)
        component.__dict__.update(self.__dict__)

        for cls in self.__class__.__mro__:
            for attr in getattr(cls, '__slots__', ()):
                object.__setattr__(component, attr, getattr(self, attr))

        component.errors = self.errors.copy()
        return component

    @property
    def built(self) -> bool:
        return self._built is True

    def build(self) -> None:
        self._built = True

    @property
    def validation_attempted(self) -> str:
        return 'full' if self._built else 'partial'

    def is_global(self) -> bool:
        """Returns `True` if the instance is a global component, `False` if it's local."""
        return self.parent is None

    @schema_cache
    def is_override(self) -> bool:
        """Returns `True` if the instance is an override of a global component."""
        if self.parent is not None:
            return False
        return any(self.elem in x for x in self.schema.root if x.tag == nm.XSD_OVERRIDE)

    @property
    def schema_elem(self) -> ElementType:
        """The reference element of the schema for the component instance."""
        return self.elem

    @property
    def source(self) -> XMLResource:
        """Property that references to schema source."""
        return self.schema.source

    @property
    def default_namespace(self) -> str:
        """Property that references to schema's default namespaces."""
        return self.schema.namespaces['']

    @property
    def namespaces(self) -> NsmapType:
        """Property that references to schema's namespace mapping."""
        return self.schema.namespaces

    @cached_property
    def annotation(self) -> Optional['XsdAnnotation']:
        """
        The primary annotation of the XSD component, if any. This is the annotation
        defined in the first child of the element where the component is defined.
        """
        return get_xsd_annotation(self.elem, self.schema, self)

    @cached_property
    def annotations(self) -> Union[tuple[()], list['XsdAnnotation']]:
        """A list containing all the annotations of the XSD component."""
        annotations = []
        components = self.schema.components
        parent_map = self.schema.source.parent_map

        for elem in self.elem.iter():
            if elem is self.elem:
                if self.annotation is not None:
                    annotations.append(self.annotation)
            elif elem in components:
                break
            elif elem.tag == nm.XSD_ANNOTATION:
                parent_elem = parent_map[elem]
                if parent_elem is not self.elem:
                    annotations.append(XsdAnnotation(elem, self.schema, self, parent_elem))

        return annotations

    def parse(self, elem: ElementType) -> None:
        """Set and parse the component Element."""
        if elem.tag not in self._ADMITTED_TAGS:
            msg = "wrong XSD element {!r} for {!r}, must be one of {!r}"
            raise XMLSchemaValueError(
                msg.format(elem.tag, self.__class__, self._ADMITTED_TAGS)
            )

        if hasattr(self, 'elem'):
            # Redefinition of a global component
            if self.parent is not None:
                raise XMLSchemaValueError(f'{self!r} is not a global component')
            self.__dict__.clear()

        self.elem = elem
        if self.errors:
            self.errors.clear()
        self._parse()
        if not self._built:
            self._built = self.__class__.build is XsdComponent.build

    def _parse(self) -> None:
        return

    def _parse_reference(self) -> Optional[bool]:
        """
        Helper method for referable components. Returns `True` if a valid reference QName
        is found without any error, otherwise returns `None`. Sets an id-related name for
        the component ('nameless_<id of the instance>') if both the attributes 'ref' and
        'name' are missing.
        """
        ref = self.elem.get('ref')
        if ref is None:
            if 'name' in self.elem.attrib:
                return None
            elif self.parent is None:
                msg = _("missing attribute 'name' in a global %r")
                self.parse_error(msg % type(self))
            else:
                msg = _("missing both attributes 'name' and 'ref' in local %r")
                self.parse_error(msg % type(self))
        elif 'name' in self.elem.attrib:
            msg = _("attributes 'name' and 'ref' are mutually exclusive")
            self.parse_error(msg)
        elif self.parent is None:
            msg = _("attribute 'ref' not allowed in a global %r")
            self.parse_error(msg % type(self))
        else:
            try:
                self.name = self.schema.resolve_qname(ref)
            except (KeyError, ValueError, RuntimeError) as err:
                self.parse_error(err)
            else:
                if self._parse_child_component(self.elem, strict=False) is not None:
                    msg = _("a reference component cannot have child definitions/declarations")
                    self.parse_error(msg)
                return True

        return None

    def _parse_child_component(self, elem: ElementType, strict: bool = True) \
            -> Optional[ElementType]:
        child = None
        for e in elem:
            if e.tag == nm.XSD_ANNOTATION or callable(e.tag):
                continue
            elif not strict:
                return e
            elif child is not None:
                msg = _("too many XSD components, unexpected {0!r} found at position {1}")
                self.parse_error(msg.format(child, elem[:].index(e)), elem)
                break
            else:
                child = e
        return child

    def _parse_target_namespace(self) -> None:
        """
        XSD 1.1 targetNamespace attribute in elements and attributes declarations.
        """
        target_namespace = parse_target_namespace(self)
        if 'name' not in self.elem.attrib:
            msg = _("attribute 'name' must be present when "
                    "'targetNamespace' attribute is provided")
            self.parse_error(msg)
        if 'form' in self.elem.attrib:
            msg = _("attribute 'form' must be absent when "
                    "'targetNamespace' attribute is provided")
            self.parse_error(msg)
        if target_namespace != self.target_namespace:
            if self.parent is None:
                msg = _("a global %s must have the same namespace as its parent schema")
                self.parse_error(msg % self.__class__.__name__)

            xsd_type = self.get_parent_type()
            if xsd_type is None or xsd_type.parent is not None:
                pass
            elif xsd_type.derivation != 'restriction' or \
                    getattr(xsd_type.base_type, 'name', None) == nm.XSD_ANY_TYPE:
                msg = _("a declaration contained in a global complexType "
                        "must have the same namespace as its parent schema")
                self.parse_error(msg)

        self.target_namespace = target_namespace
        if self.name is None:
            pass  # pragma: no cover
        elif not target_namespace:
            self.name = local_name(self.name)
        else:
            self.name = f'{{{target_namespace}}}{local_name(self.name)}'

    @cached_property
    def local_name(self) -> Optional[str]:
        """The local part of the name of the component, or `None` if the name is `None`."""
        return None if self.name is None else local_name(self.name)

    @cached_property
    def qualified_name(self) -> Optional[str]:
        """The name of the component in extended format, or `None` if the name is `None`."""
        return None if self.name is None else get_qname(self.target_namespace, self.name)

    @cached_property
    def prefixed_name(self) -> Optional[str]:
        """The name of the component in prefixed format, or `None` if the name is `None`."""
        return None if self.name is None else get_prefixed_qname(self.name, self.namespaces)

    @cached_property
    def display_name(self) -> Optional[str]:
        """
        The name of the component to display when you have to refer to it with a
        simple unambiguous format.
        """
        prefixed_name = self.prefixed_name
        if prefixed_name is None:
            return None
        return self.name if ':' not in prefixed_name else prefixed_name

    @property
    def id(self) -> Optional[str]:
        """The ``'id'`` attribute of the component tag, ``None`` if missing."""
        return self.elem.get('id')

    def is_matching(self, name: Optional[str], default_namespace: Optional[str] = None,
                    **kwargs: Any) -> bool:
        """
        Returns `True` if the component name is matching the name provided as argument,
        `False` otherwise. For XSD elements the matching is extended to substitutes.

        :param name: a local or fully-qualified name.
        :param default_namespace: used by the XPath processor for completing \
        the name argument in case it's a local name.
        :param kwargs: additional options that can be used by certain components.
        """
        return bool(self.name == name or default_namespace and name and
                    name[0] != '{' and self.name == f'{{{default_namespace}}}{name}')

    def match(self, name: Optional[str], default_namespace: Optional[str] = None,
              **kwargs: Any) -> Optional['XsdComponent']:
        """
        Returns the component if its name is matching the name provided as argument,
        `None` otherwise.
        """
        return self if self.is_matching(name, default_namespace, **kwargs) else None

    def get_matching_item(self, mapping: MutableMapping[str, Any],
                          ns_prefix: str = 'xmlns',
                          match_local_name: bool = False) -> Optional[Any]:
        """
        If a key is matching component name, returns its value, otherwise returns `None`.
        """
        if self.name is None:
            return None
        elif not self.target_namespace:
            return mapping.get(self.name)
        elif self.qualified_name in mapping:
            return mapping[cast(str, self.qualified_name)]
        elif self.prefixed_name in mapping:
            return mapping[cast(str, self.prefixed_name)]

        # Try a match with other prefixes
        target_namespace = self.target_namespace
        suffix = f':{self.local_name}'

        for k in filter(lambda x: x.endswith(suffix), mapping):
            prefix = k.split(':')[0]
            if self.schema.namespaces.get(prefix) == target_namespace:
                return mapping[k]

            # Match namespace declaration within value
            ns_declaration = f'{ns_prefix}:{prefix}'
            try:
                if mapping[k][ns_declaration] == target_namespace:
                    return mapping[k]
            except (KeyError, TypeError):
                pass
        else:
            if match_local_name:
                return mapping.get(self.local_name)  # type: ignore[arg-type]
            return None

    @schema_cache
    def get_global(self) -> 'XsdComponent':
        """Returns the global XSD component that contains the component instance."""
        if self.parent is None:
            return self
        component = self.parent
        while component is not self:
            if component.parent is None:
                return component
            component = component.parent
        else:  # pragma: no cover
            msg = _("parent circularity from {}")
            raise XMLSchemaValueError(msg.format(self))

    @schema_cache
    def get_parent_type(self) -> Optional['XsdType']:
        """
        Returns the nearest XSD type that contains the component instance,
        or `None` if the component doesn't have an XSD type parent.
        """
        component = self.parent
        while component is not self and component is not None:
            if isinstance(component, XsdType):
                return component
            component = component.parent
        return None

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator['XsdComponent']:
        """
        Creates an iterator for XSD subcomponents.

        :param xsd_classes: provide a class or a tuple of classes to iterate \
        over only a specific classes of components.
        """
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self

    def iter_ancestors(self, xsd_classes: ComponentClassType = None)\
            -> Iterator['XsdComponent']:
        """
        Creates an iterator for XSD ancestor components, schema excluded.
        Stops when the component is global or if the ancestor is not an
        instance of the specified class/classes.

        :param xsd_classes: provide a class or a tuple of classes to iterate \
        over only a specific classes of components.
        """
        ancestor = self
        while True:
            if ancestor.parent is None:
                break
            ancestor = ancestor.parent
            if xsd_classes is not None and not isinstance(ancestor, xsd_classes):
                break
            yield ancestor

    def tostring(self, indent: str = '', max_lines: Optional[int] = None,
                 spaces_for_tab: int = 4) -> Union[str, bytes]:
        """Serializes the XML elements that declare or define the component to a string."""
        return etree_tostring(self.schema_elem, self.namespaces, indent, max_lines, spaces_for_tab)

    def dump_status(self, *args: Any) -> None:
        """Dump component status to logger for debugging purposes."""
        dump_data(self.schema.source, *args)


class XsdAnnotation(XsdComponent):
    """
    Class for XSD *annotation* definitions.

    :ivar appinfo: a list containing the xs:appinfo children.
    :ivar documentation: a list containing the xs:documentation children.

    ..  <annotation
          id = ID
          {any attributes with non-schema namespace . . .}>
          Content: (appinfo | documentation)*
        </annotation>

    ..  <appinfo
          source = anyURI
          {any attributes with non-schema namespace . . .}>
          Content: ({any})*
        </appinfo>

    ..  <documentation
          source = anyURI
          xml:lang = language
          {any attributes with non-schema namespace . . .}>
          Content: ({any})*
        </documentation>
    """
    _ADMITTED_TAGS = nm.XSD_ANNOTATION,

    annotation = None
    annotations = ()

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: Optional[XsdComponent] = None,
                 parent_elem: Optional[ElementType] = None) -> None:

        super().__init__(elem, schema, parent)
        if parent_elem is not None:
            self.parent_elem = parent_elem
        elif parent is not None:
            self.parent_elem = parent.elem
        else:
            self.parent_elem = schema.source.root

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, str(self)[:40])

    def __str__(self) -> str:
        return '\n'.join(select(self.elem, '*/fn:string()'))

    def _parse(self) -> None:
        self.appinfo = []
        self.documentation = []
        for child in self.elem:
            if child.tag == nm.XSD_APPINFO:
                self.appinfo.append(child)
            elif child.tag == nm.XSD_DOCUMENTATION:
                self.documentation.append(child)


class XsdType(XsdComponent):
    """Common base class for XSD types."""

    __slots__ = ()

    base_type: Optional[BaseXsdType] = None
    derivation: Optional[str] = None
    _final: Optional[str] = None
    ref: Optional[BaseXsdType]

    @property
    def final(self) -> str:
        return self.schema.final_default if self._final is None else self._final

    @property
    def content_type_label(self) -> str:
        """
        The content type classification. Can be 'simple', 'mixed', 'element-only' or 'empty'.
        """
        raise NotImplementedError()

    @property
    def sequence_type(self) -> str:
        """The XPath sequence type associated with the content."""
        raise NotImplementedError()

    @property
    def root_type(self) -> BaseXsdType:
        """
        The root type of the type definition hierarchy. For an atomic type
        is the primitive type. For a list is the primitive type of the item.
        For a union is the base union type. For a complex type is xs:anyType.
        """
        raise NotImplementedError()

    @property
    def simple_type(self) -> Optional['XsdSimpleType']:
        """
        Property that is the instance itself for a simpleType. For a
        complexType is the instance's *content* if this is a simpleType
        or `None` if the instance's *content* is a model group.
        """
        raise NotImplementedError()

    @property
    def model_group(self) -> Optional['XsdGroup']:
        """
        Property that is `None` for a simpleType. For a complexType is
        the instance's *content* if this is a model group or `None` if
        the instance's *content* is a simpleType.
        """
        return None

    @staticmethod
    def is_simple() -> bool:
        """Returns `True` if the instance is a simpleType, `False` otherwise."""
        raise NotImplementedError()

    @staticmethod
    def is_complex() -> bool:
        """Returns `True` if the instance is a complexType, `False` otherwise."""
        raise NotImplementedError()

    def is_atomic(self) -> bool:
        """Returns `True` if the instance is an atomic simpleType, `False` otherwise."""
        return False

    def is_primitive(self) -> bool:
        """Returns `True` if the type is an XSD primitive builtin type, `False` otherwise."""
        return False

    def is_list(self) -> bool:
        """Returns `True` if the instance is a list simpleType, `False` otherwise."""
        return False

    def is_union(self) -> bool:
        """Returns `True` if the instance is a union simpleType, `False` otherwise."""
        return False

    def is_datetime(self) -> bool:
        """
        Returns `True` if the instance is a datetime/duration XSD builtin-type, `False` otherwise.
        """
        return False

    def is_empty(self) -> bool:
        """Returns `True` if the instance has an empty content, `False` otherwise."""
        raise NotImplementedError()

    def is_emptiable(self) -> bool:
        """Returns `True` if the instance has an emptiable value or content, `False` otherwise."""
        raise NotImplementedError()

    def has_simple_content(self) -> bool:
        """
        Returns `True` if the instance has a simple content, `False` otherwise.
        """
        raise NotImplementedError()

    def has_complex_content(self) -> bool:
        """
        Returns `True` if the instance is a complexType with mixed or element-only
        content, `False` otherwise.
        """
        raise NotImplementedError()

    def has_mixed_content(self) -> bool:
        """
        Returns `True` if the instance is a complexType with mixed content, `False` otherwise.
        """
        raise NotImplementedError()

    def is_element_only(self) -> bool:
        """
        Returns `True` if the instance is a complexType with element-only content,
        `False` otherwise.
        """
        raise NotImplementedError()

    def is_derived(self, other: BaseXsdType, derivation: Optional[str] = None) -> bool:
        """
        Returns `True` if the instance is derived from *other*, `False` otherwise.
        The optional argument derivation can be a string containing the words
        'extension' or 'restriction' or both.
        """
        raise NotImplementedError()

    def is_extension(self) -> bool:
        return self.derivation == 'extension'

    def is_restriction(self) -> bool:
        return self.derivation == 'restriction'

    @schema_cache
    def is_blocked(self, xsd_element: 'XsdElement') -> bool:
        """
        Returns `True` if the base type derivation is blocked, `False` otherwise.
        """
        xsd_type = xsd_element.type
        if self is xsd_type:
            return False

        block = f'{xsd_element.block} {xsd_type.block}'.strip()
        if not block:
            return False

        _block = {x for x in block.split() if x in ('extension', 'restriction')}
        return any(self.is_derived(xsd_type, derivation) for derivation in _block)

    def is_dynamic_consistent(self, other: Any) -> bool:
        raise NotImplementedError()

    def is_key(self) -> bool:
        return self.is_derived(self.maps.types[nm.XSD_ID])

    def is_qname(self) -> bool:
        return self.is_derived(self.maps.types[nm.XSD_QNAME])

    def is_notation(self) -> bool:
        return self.is_derived(self.maps.types[nm.XSD_NOTATION_TYPE])

    def is_decimal(self) -> bool:
        return self.is_derived(self.maps.types[nm.XSD_DECIMAL])

    def is_boolean(self) -> bool:
        return self.is_derived(self.maps.types[nm.XSD_BOOLEAN])

    def text_decode(self, text: str, validation: str = 'skip',
                    context: Optional[ValidationContext] = None) -> DecodedValueType:
        raise NotImplementedError()

    def text_is_valid(self, text: str, context: Optional[ValidationContext] = None) -> bool:
        raise NotImplementedError()

    @schema_cache
    def overall_min_occurs(self, particle: ModelParticleType) -> int:
        """Returns the overall minimum for occurrences of a content model particle."""
        content = self.model_group
        if content is None or self.is_empty():
            raise XMLSchemaTypeError(_("content type must be 'element-only' or 'mixed'"))
        return content.overall_min_occurs(particle)

    @schema_cache
    def overall_max_occurs(self, particle: ModelParticleType) -> Optional[int]:
        """Returns the overall maximum for occurrences of a content model particle."""
        content = self.model_group
        if content is None or self.is_empty():
            raise XMLSchemaTypeError(_("content must type be 'element-only' or 'mixed'"))
        return content.overall_max_occurs(particle)
