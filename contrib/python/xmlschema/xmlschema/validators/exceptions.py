#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import textwrap
from collections.abc import Callable, Iterable
from pprint import PrettyPrinter
from typing import TYPE_CHECKING, Any, cast, Optional, Union

from elementpath.etree import etree_tostring

from xmlschema.exceptions import XMLSchemaException, XMLSchemaWarning, \
    XMLSchemaValueError, XMLSchemaAttributeError, XMLSchemaTypeError
from xmlschema.aliases import ElementType, NsmapType, SchemaType, SchemaElementType, \
    ModelParticleType
from xmlschema.translation import gettext as _
from xmlschema.resources import XMLResource
from xmlschema.utils.etree import etree_getpath, is_etree_element
from xmlschema.utils.qnames import get_prefixed_qname, local_name

if TYPE_CHECKING:
    from .xsdbase import XsdValidator  # noqa: F401
    from .wildcards import XsdAnyElement  # noqa: F401
    from .groups import XsdGroup  # noqa: F401

ValidatorType = Union['XsdValidator', Callable[[Any], None]]


class XMLSchemaValidatorError(XMLSchemaException):
    """
    Base class for XSD validator errors.

    :param validator: the XSD validator.
    :param message: the error message.
    :param elem: the element that contains the error.
    :param source: the XML resource or the decoded data that contains the error.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    """
    _root: Optional[ElementType] = None
    _path: Optional[str] = None
    _sourceline: Optional[int] = None

    # Optional dump of the execution stack that can be set in collected
    # validator errors for debugging purposes.
    stack_trace: Optional[str] = None

    def __init__(self, validator: ValidatorType,
                 message: str,
                 elem: Optional[ElementType] = None,
                 source: Optional[Any] = None,
                 namespaces: Optional[NsmapType] = None) -> None:
        self.validator = validator
        self._message = message
        self.namespaces = namespaces
        self.source = source
        self.elem = elem

    @property
    def message(self) -> str:
        return self._message

    def __str__(self) -> str:
        chunks: list[str] = ['%s:\n' % self.message.rstrip('.:')]
        if self.elem is not None:
            elem_as_string = etree_tostring(self.elem, self.namespaces, '  ', 20)
            if isinstance(elem_as_string, bytes):
                elem_as_string = elem_as_string.decode('utf-8')
            chunks.append("Schema component:\n\n%s\n" % elem_as_string)

        path = self.path
        if path is not None:
            chunks.append("Path: %s\n" % path)

        if self.schema_url is not None:
            chunks.append("Schema URL: %s\n" % self.schema_url)
            if self.origin_url not in (None, self.schema_url):
                chunks.append("Origin URL: %s\n" % self.origin_url)

        return '\n'.join(chunks) if len(chunks) > 1 else chunks[0][:-2]

    @property
    def msg(self) -> str:
        return self.__str__()

    @msg.setter
    def msg(self, msg: str) -> None:
        return

    def __setattr__(self, name: str, value: Any) -> None:
        if name == 'elem':
            if value is None:
                self._sourceline = None
                self._path = None
                super().__setattr__(name, value)
                return

            if not is_etree_element(value):
                msg = _("{!r} attribute requires an Element, not {!r}.")
                raise XMLSchemaValueError(msg.format(name, value))

            self._sourceline = getattr(value, 'sourceline', self._sourceline)
            self._path = None
            if isinstance(self.source, XMLResource) and self.source.is_lazy():
                # Don't save the element of a lazy resource but set the path
                self._path = etree_getpath(
                    elem=value,
                    root=self.source.root,
                    namespaces=self.namespaces,
                    relative=False,
                    add_position=True
                )
                value = None

        elif name == 'source' and isinstance(value, XMLResource):
            self._path = None

        super().__setattr__(name, value)

    @property
    def sourceline(self) -> Optional[int]:
        """XML element *sourceline* if available (lxml Element)."""
        return getattr(self.elem, 'sourceline', self._sourceline)

    @property
    def root(self) -> Optional[ElementType]:
        """The XML resource root element if *source* is set."""
        if isinstance(self.source, XMLResource):
            return self.source.root
        else:
            return self._root

    @root.setter
    def root(self, value: ElementType) -> None:
        if isinstance(self.source, XMLResource):
            msg = _("can't set the 'root' attribute if an XMLResource source is set")
            raise XMLSchemaAttributeError(msg)
        elif not is_etree_element(value):
            msg = _("{!r} attribute requires an Element, not {!r}.")
            raise XMLSchemaTypeError(msg.format('root', type(value)))
        elif self._root is not value:
            self._root = value
            self._path = None

    @property
    def schema_url(self) -> Optional[str]:
        """The schema URL, if available and the *validator* is an XSD component."""
        url: Optional[str]
        try:
            url = self.validator.schema.source.url  # type: ignore[union-attr]
        except AttributeError:
            return getattr(self.validator, 'url', None)  # it's the schema
        else:
            return url

    @property
    def origin_url(self) -> Optional[str]:
        """The origin schema URL, if available and the *validator* is an XSD component."""
        url: Optional[str]
        try:
            url = self.validator.maps.validator.source.url  # type: ignore[union-attr]
        except AttributeError:
            return None
        else:
            return url

    @property
    def path(self) -> Optional[str]:
        """The XPath of the element, if it's not `None` and the XML resource is set."""
        if self._path is None and self.elem is not None and self.root is not None:
            self._path = etree_getpath(
                elem=self.elem,
                root=self.root,
                namespaces=self.namespaces,
                relative=False,
                add_position=True
            )
        return self._path

    def get_elem_as_string(self, indent: str = '', max_lines: Optional[int] = None) -> str:
        """Returns a string representation of elem attribute."""
        kwargs = {
            'elem': self.elem,
            'namespaces': self.namespaces,
            'indent': indent,
            'max_lines': max_lines
        }
        try:
            return cast(str, etree_tostring(**kwargs))  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return indent + repr(self.elem)


class XMLSchemaCircularityError(XMLSchemaValidatorError):
    """
    Raised when a circularity is found building a global component.
    """
    def __init__(self, name: str, elem: ElementType, schema: SchemaType) -> None:
        msg = _("Circular definition detected for xs:{} {!r}.")
        super().__init__(
            validator=schema,
            message=msg.format(local_name(elem.tag), name),
            elem=elem,
            source=schema.source,
            namespaces=schema.namespaces
        )


class XMLSchemaNotBuiltError(XMLSchemaValidatorError, RuntimeError):
    """
    Raised when there is an improper usage attempt of a not built XSD validator.

    :param validator: the XSD validator.
    :param message: the error message.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    """
    def __init__(self, validator: 'XsdValidator',
                 message: str,
                 namespaces: Optional[NsmapType] = None) -> None:
        if namespaces is None:
            namespaces = getattr(validator, 'namespaces', None)
        super().__init__(
            validator=validator,
            message=message,
            elem=getattr(validator, 'elem', None),
            source=getattr(validator, 'source', None),
            namespaces=namespaces
        )


class XMLSchemaParseError(XMLSchemaValidatorError, SyntaxError):
    """
    Raised when an error is found during the building of an XSD validator.

    :param validator: the XSD validator.
    :param message: the error message.
    :param elem: the element that contains the error.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    """
    def __init__(self, validator: 'XsdValidator', message: str,
                 elem: Optional[ElementType] = None,
                 namespaces: Optional[NsmapType] = None) -> None:
        if namespaces is None:
            namespaces = getattr(validator, 'namespaces', None)

        super().__init__(
            validator=validator,
            message=message,
            elem=elem if elem is not None else getattr(validator, 'elem', None),
            source=getattr(validator, 'source', None),
            namespaces=namespaces
        )


class XMLSchemaModelError(XMLSchemaValidatorError, ValueError):
    """
    Raised when a model error is found during the checking of a model group.

    :param group: the XSD model group.
    :param message: the error message.
    """
    def __init__(self, group: 'XsdGroup', message: str) -> None:
        super().__init__(
            validator=group,
            message=message,
            elem=getattr(group, 'elem', None),
            source=getattr(group, 'source', None),
            namespaces=getattr(group, 'namespaces', None)
        )


class XMLSchemaModelDepthError(XMLSchemaModelError):
    """Raised when recursion depth is exceeded while iterating a model group."""

    @property
    def message(self) -> str:
        return f"{self._message} {self.validator!r}."

    def __init__(self, group: 'XsdGroup') -> None:
        msg = "maximum model recursion depth exceeded while iterating"
        super().__init__(group, message=msg)


class XMLSchemaValidationError(XMLSchemaValidatorError, ValueError):
    """
    Raised when the XML data is not validated with the XSD component or schema.
    It's used by decoding and encoding methods. Encoding validation errors do
    not include XML data element and source, so the error is limited to a message
    containing object representation and a reason.

    :param validator: the XSD validator.
    :param obj: the not validated XML data.
    :param reason: the detailed reason of failed validation.
    :param source: the XML resource that contains the error.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    """
    _message = 'failed validating {} with'

    @property
    def message(self) -> str:
        return f'{self._message} {self.validator!r}.'

    # For compatibility with XMLSchemaChildrenValidationError
    invalid_tag: Optional[str] = None

    @property
    def expected_tags(self) -> list[str]: return []

    @property
    def invalid_child(self) -> Optional[ElementType]: return None

    def __init__(self,
                 validator: ValidatorType,
                 obj: Any,
                 reason: Optional[str] = None,
                 source: Optional[Any] = None,
                 namespaces: Optional[NsmapType] = None) -> None:

        if isinstance(obj, str):
            obj_repr = repr(obj.encode('ascii', 'xmlcharrefreplace').decode('utf-8'))
        else:
            obj_repr = repr(obj)

        if len(obj_repr) > 200:
            obj_repr = f"{type(obj)} instance"

        super().__init__(
            validator=validator,
            message=_(self._message).format(obj_repr),
            elem=obj if is_etree_element(obj) else None,
            source=source,
            namespaces=namespaces,
        )
        self.obj = obj
        self.reason = reason

    def __repr__(self) -> str:
        return '%s(reason=%r)' % (self.__class__.__name__, self.reason)

    def __str__(self) -> str:
        chunks: list[str] = ['%s:\n' % self.message.rstrip('.:')]

        if self.reason is not None:
            chunks.append('Reason: %s\n' % self.reason)

        if hasattr(self.validator, 'tostring'):
            component_as_string = self.validator.tostring('  ', 20)
            chunks.append("Schema component:\n\n%s\n" % component_as_string)

        if is_etree_element(self.elem):
            chunks.append(f"Instance type: {type(self.elem)}\n")
            instance_as_string = self.get_elem_as_string(indent='  ', max_lines=20)
        else:
            chunks.append(f"Instance type: {type(self.obj)}\n")
            instance_as_string = self.get_obj_as_string(indent='  ', max_lines=20)

        if hasattr(self.elem, 'sourceline'):
            line = getattr(self.elem, 'sourceline')
            chunks.append(f"Instance (line {line!r}):\n\n{instance_as_string}\n")
        else:
            chunks.append(f"Instance:\n\n{instance_as_string}\n")

        if self.path is not None:
            chunks.append("Path: %s\n" % self.path)

        return '\n'.join(chunks) if len(chunks) > 1 else chunks[0][:-2]

    def get_obj_as_string(self, indent: str = '', max_lines: Optional[int] = None) -> str:
        """
        Return a string representation of obj attribute, with optional indentation
        and an optional limit on lines.
        """
        if is_etree_element(self.obj):
            return self.get_elem_as_string(indent, max_lines)

        pp = PrettyPrinter(indent=2, depth=6)
        obj_as_string = pp.pformat(self.obj)
        if indent:
            obj_as_string = textwrap.indent(obj_as_string, prefix=indent)

        if max_lines and len(obj_as_string.splitlines()) > max_lines:
            obj_as_string = '\n'.join(obj_as_string.splitlines()[:max_lines - 3])
            obj_as_string += f'\n\n{indent}...\n{indent}...'

        return obj_as_string


class XMLSchemaDecodeError(XMLSchemaValidationError):
    """
    Raised when an XML data string is not decodable to a Python object.

    :param validator: the XSD validator.
    :param obj: the not validated XML data.
    :param decoder: the XML data decoder.
    :param reason: the detailed reason of failed validation.
    :param source: the XML resource that contains the error.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    """
    _message = 'failed decoding {} with'

    def __init__(self, validator: Union['XsdValidator', Callable[[Any], None]],
                 obj: Any,
                 decoder: Any,
                 reason: Optional[str] = None,
                 source: Optional[Any] = None,
                 namespaces: Optional[NsmapType] = None) -> None:
        super().__init__(validator, obj, reason, source, namespaces)
        self.decoder = decoder


class XMLSchemaEncodeError(XMLSchemaValidationError):
    """
    Raised when an object is not encodable to an XML data string.

    :param validator: the XSD validator.
    :param obj: the not validated XML data.
    :param encoder: the XML encoder.
    :param reason: the detailed reason of failed validation.
    :param source: the XML resource that contains the error.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    """
    _message = 'failed encoding {} with'

    def __init__(self, validator: Union['XsdValidator', Callable[[Any], None]],
                 obj: Any,
                 encoder: Any,
                 reason: Optional[str] = None,
                 source: Optional[Any] = None,
                 namespaces: Optional[NsmapType] = None) -> None:
        super().__init__(validator, obj, reason, source, namespaces)
        self.encoder = encoder


class XMLSchemaChildrenValidationError(XMLSchemaValidationError):
    """
    Raised when a child element is not validated.

    :param validator: the XSD validator.
    :param elem: the not validated XML element.
    :param index: the child index.
    :param particle: the model particle that generated the error. Maybe the validator itself.
    :param occurs: the particle occurrences.
    :param expected: the expected element tags/object names.
    :param source: the XML resource that contains the error.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    """
    invalid_tag: Optional[str]
    """The tag of the invalid child element, `None` in case of an incomplete content."""

    def __init__(self, validator: 'XsdValidator',
                 elem: ElementType,
                 index: int,
                 particle: ModelParticleType,
                 occurs: int = 0,
                 expected: Optional[Iterable[SchemaElementType]] = None,
                 source: Optional[Any] = None,
                 namespaces: Optional[NsmapType] = None) -> None:

        self.index = index
        self.particle = particle
        self.occurs = occurs
        self.expected = expected

        if namespaces is None:
            namespaces = getattr(validator, 'namespaces', {})

        if index >= len(elem):
            self.invalid_tag = None
            tag = get_prefixed_qname(elem.tag, namespaces, use_empty=False)
            reason = _("The content of element %r is not complete.") % tag
        else:
            self.invalid_tag = elem[index].tag
            tag = get_prefixed_qname(self.invalid_tag, namespaces, use_empty=False)
            reason = _("Unexpected child with tag %r at position %d.") % (tag, index + 1)

        if occurs and particle.min_occurs > occurs:
            reason += " The particle %r occurs %d times but the minimum is %d." % (
                particle, occurs, particle.min_occurs
            )
        elif particle.max_occurs is not None and particle.max_occurs < occurs:
            reason += " The particle %r occurs %r times but the maximum is %r." % (
                particle, occurs, particle.max_occurs
            )

        expected_tags = self.expected_tags

        if not expected_tags:
            pass
        elif len(expected_tags) > 1:
            reason += _(" Tag (%s) expected.") % ' | '.join(repr(tag) for tag in expected_tags)
        elif expected_tags[0].startswith('from '):
            reason += _(" Tag %s expected.") % expected_tags[0]
        else:
            reason += _(" Tag %r expected.") % expected_tags[0]

        super().__init__(validator, elem, reason, source, namespaces)

    @property
    def expected_tags(self) -> list[str]:
        expected_tags: list[str] = []
        if not self.expected:
            return expected_tags

        for xsd_element in self.expected:
            name = xsd_element.display_name
            if name is not None:
                expected_tags.append(name)
            elif hasattr(xsd_element, 'process_contents'):
                wildcard = cast('XsdAnyElement', xsd_element)
                if wildcard.process_contents == 'strict':
                    tmpl = 'from {!r} namespace/s'
                    items = tuple(wildcard.namespace)
                    if len(wildcard.namespace) == 1:
                        expected_tags.append(tmpl.format(items[0]))
                    else:
                        expected_tags.append(tmpl.format(items))

        return expected_tags

    @property
    def invalid_child(self) -> Optional[ElementType]:
        """
        The invalid child element, if any, `None` otherwise. It's `None` in case of
        incomplete content or if the parent has been cleared during lazy validation.
        """
        try:
            return self.elem[self.index] if self.elem is not None else None
        except IndexError:
            return None  # in case of incomplete content or lazy trees


class XMLSchemaStopValidation(XMLSchemaException):
    """Stops the validation process."""


class XMLSchemaIncludeWarning(XMLSchemaWarning):
    """A schema include fails."""


class XMLSchemaImportWarning(XMLSchemaWarning):
    """A schema namespace import fails."""


class XMLSchemaTypeTableWarning(XMLSchemaWarning):
    """Not equivalent type table found in model."""


class XMLSchemaAssertPathWarning(XMLSchemaWarning):
    """An improper XPath expression found in XSD 1.1 assertion."""
