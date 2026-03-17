#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import copy
import decimal
import logging
from abc import abstractmethod, ABCMeta
from collections import Counter
from collections.abc import Iterable, Iterator, MutableSequence
from functools import partial
from typing import Any, Generic, Optional, TYPE_CHECKING, TypeVar, Union
from xml.etree.ElementTree import Element

from elementpath.datatypes import AbstractDateTime, Duration, AbstractBinary

from xmlschema.aliases import DecodeType, DepthFillerType, ElementType, \
    ElementHookType, EncodeType, ExtraValidatorType, FillerType, IterDecodeType, \
    IterEncodeType, ModelParticleType, NsmapType, SchemaElementType, \
    SchemaType, ValidationHookType, ValueHookType, ErrorsType, DecodedValueType, \
    GlobalMapsType
from xmlschema.exceptions import XMLSchemaTypeError
from xmlschema.translation import gettext as _
from xmlschema.utils.decoding import EmptyType, raw_encode_value
from xmlschema.utils.etree import is_etree_element, is_etree_document
from xmlschema.utils.logger import format_xmlschema_stack
from xmlschema.utils.misc import iter_class_slots
from xmlschema.utils.qnames import get_prefixed_qname
from xmlschema.namespaces import NamespaceMapper
from xmlschema.converters import XMLSchemaConverter
from xmlschema.resources import XMLResource
from xmlschema.arguments import Arguments, BooleanOption, NonNegIntOption, \
    validate_type, Argument, MaxDepthOption, ExtraValidatorOption, \
    ValidationHookOption, FillerOption, ElementHookOption, DepthFillerOption, \
    ValueHookOption, DecimalTypeOption, ElementTypeOption

from .exceptions import XMLSchemaValidationError, \
    XMLSchemaChildrenValidationError, XMLSchemaDecodeError, XMLSchemaEncodeError

if TYPE_CHECKING:
    from .xsdbase import XsdValidator  # noqa: F401
    from .facets import XsdPatternFacets  # noqa: F401
    from .identities import XsdIdentity, IdentityCounter  # noqa: F401

logger = logging.getLogger('xmlschema')


###
# Arguments for validation contexts

class ValidationSourceArgument(Argument[Any]):
    _validators = partial(validate_type, types=XMLResource),


class EncodeSourceArgument(Argument[Any]):
    def validated_value(self, value: Any) -> Any:
        if isinstance(value, XMLResource) or is_etree_document(value) or \
                (is_etree_element(value) and not isinstance(value, MutableSequence)):
            msg = _("invalid type {!r} for {}")
            raise XMLSchemaTypeError(msg.format(type(value), self))
        return value


class NamespaceMapperArgument(Argument[NamespaceMapper]):
    _validators = partial(validate_type, types=NamespaceMapper),


class ConverterArgument(Argument[XMLSchemaConverter]):
    _validators = partial(validate_type, types=XMLSchemaConverter),


class ErrorsArgument(Argument[list['XMLSchemaValidationError']]):
    _validators = partial(validate_type, types=list),


class ValidationArguments(Arguments):
    source = ValidationSourceArgument()
    converter = NamespaceMapperArgument()
    errors = ErrorsArgument()
    level = NonNegIntOption(default=0)
    max_depth = MaxDepthOption(default=None)
    extra_validator = ExtraValidatorOption(default=None)
    validation_hook = ValidationHookOption(default=None)

    use_defaults = BooleanOption(default=True)
    check_identities = preserve_mixed = process_skipped = \
        use_location_hints = BooleanOption(default=False)


class DecodeArguments(ValidationArguments):
    converter = ConverterArgument()
    decimal_type = DecimalTypeOption(default=None)
    filler = FillerOption(default=None)
    depth_filler = DepthFillerOption(default=None)
    value_hook = ValueHookOption(default=None)
    element_hook = ElementHookOption(default=None)

    datetime_types = binary_types = fill_missing = \
        keep_empty = keep_unknown = BooleanOption(default=False)


class EncodeArguments(ValidationArguments):
    source = EncodeSourceArgument()
    converter = ConverterArgument()
    indent = NonNegIntOption(default=4)
    etree_element_class = ElementTypeOption(default=None)

    unordered = untyped_data = BooleanOption(default=False)


class ValidationContext:
    """
    A context class for handling validated decoding process. It stores together
    status-related fields, that are updated or set during the validation process,
    and parameters, as specific values or functions. Parameters can be provided
    as keyword-only arguments.
    """
    errors: ErrorsType
    _arguments = ValidationArguments

    __slots__ = ('source', 'converter', 'namespaces', 'errors', 'level',
                 'validation_only', 'check_identities', 'use_defaults',
                 'preserve_mixed', 'process_skipped', 'max_depth',
                 'extra_validator', 'validation_hook', 'use_location_hints',
                 'inherited', 'id_map', 'identities', 'id_list', 'elem',
                 'attribute', 'patterns')

    def __init__(self,
                 source: Union[XMLResource, Any],
                 converter: Optional[NamespaceMapper] = None,
                 errors: Optional[ErrorsType] = None,
                 level: int = 0,
                 check_identities: bool = False,
                 use_defaults: bool = True,
                 preserve_mixed: bool = False,
                 process_skipped: bool = False,
                 max_depth: Optional[int] = None,
                 extra_validator: Optional[ExtraValidatorType] = None,
                 validation_hook: Optional[ValidationHookType] = None,
                 use_location_hints: bool = False,
                 **kwargs: Any) -> None:

        self.source = source
        if converter is None:
            converter = NamespaceMapper(kwargs.get('namespaces'), source=source)

        self.converter = converter
        self.namespaces = converter.namespaces

        self.errors = errors if errors is not None else []
        self.level = level
        self.check_identities = check_identities
        self.use_defaults = use_defaults
        self.preserve_mixed = preserve_mixed
        self.process_skipped = process_skipped
        self.max_depth = max_depth
        self.extra_validator = extra_validator
        self.validation_hook = validation_hook
        self.use_location_hints = use_location_hints

        self.id_map: Counter[str] = Counter()
        self.identities: dict['XsdIdentity', 'IdentityCounter'] = {}
        self.inherited: dict[str, str] = {}

        # Local validation status
        self.id_list: Optional[list[Any]] = None
        self.elem: Optional[ElementType] = None
        self.attribute: Optional[str] = None
        self.patterns: Optional['XsdPatternFacets'] = None

        self.validation_only = self.__class__ is ValidationContext
        self._arguments.validate(self)

    def __copy__(self) -> 'ValidationContext':
        context = object.__new__(self.__class__)
        for attr in iter_class_slots(self):
            setattr(context, attr, getattr(self, attr))

        context.errors = self.errors.copy()
        context.id_map = self.id_map.copy()
        context.identities = self.identities.copy()
        context.inherited = self.inherited.copy()
        context.id_list = self.id_list if self.id_list is None else self.id_list.copy()

        if self.converter.xmlns_processing == 'none':
            context.converter = self.converter
            context.namespaces = self.namespaces
        else:
            context.converter = copy.copy(self.converter)
            context.namespaces = context.converter.namespaces
        return context

    def clear(self) -> None:
        self.errors.clear()
        self.id_map.clear()
        self.identities.clear()
        self.inherited.clear()
        self.level = 0
        self.elem = None
        self.attribute = None
        self.id_list = None
        self.patterns = None

    @property
    def root_namespace(self) -> Optional[str]:
        if not isinstance(self.source, XMLResource):
            return None
        else:
            return self.source.namespace

    def raise_or_collect(self, validation: str, error: XMLSchemaValidationError) \
            -> XMLSchemaValidationError:
        if error.elem is None and self.elem is not None:
            error.elem = self.elem

        if self.attribute is not None and error.reason is not None \
                and not error.reason.startswith('attribute '):
            name = get_prefixed_qname(self.attribute, self.namespaces)
            value = raw_encode_value(error.obj)
            error.reason = _('attribute {0}={1!r}: {2}').format(name, value, error.reason)

        if validation == 'strict':
            raise error

        if error.stack_trace is None and logger.level == logging.DEBUG:
            error.stack_trace = format_xmlschema_stack('xmlschema/validators')
            logger.debug("Collect %r with traceback:\n%s", error, error.stack_trace)

        if validation == 'lax':
            self.errors.append(error)
        return error

    def validation_error(self,
                         validation: str,
                         validator: 'XsdValidator',
                         error: Union[str, Exception],
                         obj: Any = None) -> XMLSchemaValidationError:
        """
        Helper method for collecting or raising validation errors.

        :param validation:
        :param validator: the XSD validator related with the error.
        :param error: an error instance or the detailed reason of failed validation.
        :param obj: the instance related to the error.
        """
        if not isinstance(error, XMLSchemaValidationError):
            error = XMLSchemaValidationError(
                validator, obj, str(error), self.source, self.namespaces
            )
        else:
            if error.obj is None and obj is not None:
                error.obj = obj

            error.source = self.source
            error.namespaces = self.namespaces

        return self.raise_or_collect(validation, error)

    def children_validation_error(
            self, validation: str, validator: 'XsdValidator', elem: ElementType,
            index: int, particle: ModelParticleType, occurs: int = 0,
            expected: Optional[Iterable[SchemaElementType]] = None) \
            -> XMLSchemaValidationError:

        error = XMLSchemaChildrenValidationError(
            validator=validator,
            elem=elem,
            index=index,
            particle=particle,
            occurs=occurs,
            expected=expected,
            source=self.source,
            namespaces=self.namespaces,
        )
        return self.raise_or_collect(validation, error)

    def missing_element_error(self, validation: str,
                              validator: 'XsdValidator',
                              elem: ElementType,
                              path: Optional[str] = None,
                              schema_path: Optional[str] = None) -> XMLSchemaValidationError:
        if not path:
            reason = _("{!r} is not an element of the schema").format(elem.tag)
        elif schema_path != path:
            reason = _(
                "schema_path={!r} doesn't select any {!r} element of the schema"
            ).format(schema_path, elem.tag)
        else:
            reason = _(
                "path={!r} doesn't select any {!r} element of the schema, "
                "maybe you have to provide a different path using the "
                "schema_path argument"
            ).format(path, elem.tag)

        error = XMLSchemaValidationError(validator, elem, reason, self.source, self.namespaces)
        return self.raise_or_collect(validation, error)

    def decode_error(self,
                     validation: str,
                     validator: 'XsdValidator',
                     obj: Any,
                     decoder: Any,
                     error: Union[str, Exception]) -> XMLSchemaValidationError:

        error = XMLSchemaDecodeError(
            validator=validator,
            obj=obj,
            decoder=decoder,
            reason=str(error),
            source=self.source,
            namespaces=self.namespaces,
        )
        return self.raise_or_collect(validation, error)


class DecodeContext(ValidationContext):
    """A context for handling validated decoding processes."""
    source: XMLResource
    converter: XMLSchemaConverter

    _arguments = DecodeArguments

    __slots__ = ('decimal_type', 'datetime_types', 'binary_types', 'filler',
                 'fill_missing', 'keep_empty', 'keep_unknown', 'depth_filler',
                 'value_hook', 'element_hook', 'keep_datatypes')

    def __init__(self,
                 source: XMLResource,
                 converter: Optional[XMLSchemaConverter] = None,
                 decimal_type: type[str] | type[float] | None = None,
                 datetime_types: bool = False,
                 binary_types: bool = False,
                 filler: Optional[FillerType] = None,
                 fill_missing: bool = False,
                 keep_empty: bool = False,
                 keep_unknown: bool = False,
                 depth_filler: Optional[DepthFillerType] = None,
                 value_hook: Optional[ValueHookType] = None,
                 element_hook: Optional[ElementHookType] = None,
                 **kwargs: Any) -> None:

        if not isinstance(converter, XMLSchemaConverter):
            converter = XMLSchemaConverter(**kwargs)

        self.decimal_type = decimal_type
        self.datetime_types = datetime_types
        self.binary_types = binary_types
        self.filler = filler
        self.fill_missing = fill_missing
        self.keep_empty = keep_empty
        self.keep_unknown = keep_unknown
        self.depth_filler = depth_filler
        self.value_hook = value_hook
        self.element_hook = element_hook

        keep_datatypes: list[type[DecodedValueType]] = [int, float, list]
        if decimal_type is None:
            keep_datatypes.append(decimal.Decimal)
        if datetime_types:
            keep_datatypes.append(AbstractDateTime)
            keep_datatypes.append(Duration)
        if binary_types:
            keep_datatypes.append(AbstractBinary)
        self.keep_datatypes = tuple(keep_datatypes)

        super().__init__(source, converter, **kwargs)


class EncodeContext(ValidationContext):
    """A context for handling validated encoding processes."""
    source: Any
    converter: XMLSchemaConverter

    _arguments = EncodeArguments

    __slots__ = ('unordered', 'untyped_data', 'indent', 'etree_element_class')

    def __init__(self,
                 source: Any,
                 converter: Optional[XMLSchemaConverter] = None,
                 unordered: bool = False,
                 untyped_data: bool = False,
                 **kwargs: Any) -> None:

        if not isinstance(converter, XMLSchemaConverter):
            converter = XMLSchemaConverter(**kwargs)

        self.unordered = unordered
        self.untyped_data = untyped_data
        self.indent = converter.indent
        self.etree_element_class = converter.etree_element_class

        super().__init__(source, converter, **kwargs)

    def encode_error(self,
                     validation: str,
                     validator: 'XsdValidator',
                     obj: Any,
                     encoder: Any,
                     error: Union[str, Exception]) -> XMLSchemaValidationError:

        error = XMLSchemaEncodeError(
            validator=validator,
            obj=obj,
            encoder=encoder,
            reason=str(error),
            source=self.source,
            namespaces=self.namespaces,
        )
        return self.raise_or_collect(validation, error)

    def create_element(self, tag: str) -> ElementType:
        """
        Create an ElementTree's Element using converter setting.

        :param tag: the Element tag string.
        """
        if self.etree_element_class is Element:
            return Element(tag)
        else:
            nsmap = {prefix if prefix else None: uri
                     for prefix, uri in self.namespaces.items() if uri}
            try:
                return self.etree_element_class(
                    tag, None, nsmap  # type: ignore[call-arg, arg-type]
                )
            except TypeError:
                return self.etree_element_class(tag)

    def set_element_content(self, elem: ElementType,
                            text: Optional[str] = None,
                            children: Optional[list[Element]] = None,
                            level: int = 0,
                            mixed: bool = False) -> None:
        """
        Set the content of an Element.

        :param elem: the target Element.
        :param text: the Element text.
        :param children: the list of Element children/subelements.
        :param level: the level related to the encoding process (0 means the root).
        :param mixed: whether to add custom indentation to children. For default \
        the element content is considered to be element-only.
        """
        elem.text = text
        if children:
            elem[:] = children
            if not mixed:
                padding = '\n' + ' ' * self.indent * level
                elem.text = padding if not elem.text else padding + elem.text  # FIXME? + padding

                for child in elem:
                    if not child.tail:
                        child.tail = padding
                    else:
                        child.tail = padding + child.tail + padding

                if self.indent:
                    assert children[-1].tail is not None
                    children[-1].tail = children[-1].tail[:-self.indent]


ST = TypeVar('ST')
DT = TypeVar('DT')


class ValidationMixin(Generic[ST, DT], metaclass=ABCMeta):
    """
    Mixin for implementing XML data validators/decoders on XSD components.
    A derived class must implement the methods `raw_decode` and `raw_encode`.
    """
    schema: SchemaType
    maps: GlobalMapsType

    def validate(self, obj: ST,
                 use_defaults: bool = True,
                 namespaces: Optional[NsmapType] = None,
                 max_depth: Optional[int] = None,
                 extra_validator: Optional[ExtraValidatorType] = None,
                 validation_hook: Optional[ValidationHookType] = None) -> None:
        """
        Validates XML data against the XSD schema/component instance.

        :param obj: the XML data. Can be a string for an attribute or a simple type \
        validators, or an ElementTree's Element otherwise.
        :param use_defaults: indicates whether to use default values for filling missing data.
        :param namespaces: is an optional mapping from namespace prefix to URI.
        :param max_depth: maximum level of validation, for default there is no limit.
        :param extra_validator: an optional function for performing non-standard \
        validations on XML data. The provided function is called for each traversed \
        element, with the XML element as 1st argument and the corresponding XSD \
        element as 2nd argument. It can be also a generator function and has to \
        raise/yield :exc:`xmlschema.XMLSchemaValidationError` exceptions.
        :param validation_hook: an optional function for stopping or changing \
        validation at element level. The provided function must accept two arguments, \
        the XML element and the matching XSD element. If the value returned by this \
        function is evaluated to false then the validation process continues without \
        changes, otherwise the validation process is stopped or changed. If the value \
        returned is a validation mode the validation process continues changing the \
        current validation mode to the returned value, otherwise the element and its \
        content are not processed. The function can also stop validation suddenly \
        raising a `XmlSchemaStopValidation` exception.
        :raises: :exc:`xmlschema.XMLSchemaValidationError` if the XML data instance is invalid.
        """
        for error in self.iter_errors(obj, use_defaults, namespaces,
                                      max_depth, extra_validator, validation_hook):
            raise error

    def is_valid(self, obj: ST,
                 use_defaults: bool = True,
                 namespaces: Optional[NsmapType] = None,
                 max_depth: Optional[int] = None,
                 extra_validator: Optional[ExtraValidatorType] = None,
                 validation_hook: Optional[ValidationHookType] = None) -> bool:
        """
        Like :meth:`validate` except that does not raise an exception but returns
        ``True`` if the XML data instance is valid, ``False`` if it is invalid.
        """
        error = next(self.iter_errors(obj, use_defaults, namespaces, max_depth,
                                      extra_validator, validation_hook), None)
        return error is None

    def iter_errors(self, obj: ST,
                    use_defaults: bool = True,
                    namespaces: Optional[NsmapType] = None,
                    max_depth: Optional[int] = None,
                    extra_validator: Optional[ExtraValidatorType] = None,
                    validation_hook: Optional[ValidationHookType] = None) \
            -> Iterator[XMLSchemaValidationError]:
        """
        Creates an iterator for the errors generated by the validation of an XML data against
        the XSD schema/component instance. Accepts the same arguments of :meth:`validate`.
        """
        tag = getattr(self, 'tag', None)
        source = self.maps.settings.get_resource_from_data(obj, tag)
        converter = NamespaceMapper(namespaces, source=source)
        context = ValidationContext(
            source=source,
            converter=converter,
            use_defaults=use_defaults,
            max_depth=max_depth,
            extra_validator=extra_validator,
            validation_hook=validation_hook,
        )
        self.raw_decode(obj, 'lax', context)
        yield from context.errors

    def decode(self, obj: ST, validation: str = 'strict', **kwargs: Any) -> DecodeType[DT]:
        """
        Decodes XML data.

        :param obj: the XML data. Can be a string for an attribute or for simple type \
        components or a dictionary for an attribute group or an ElementTree's \
        Element for other components.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip.
        :param kwargs: optional keyword arguments for the method :func:`iter_decode`.
        :return: a dictionary like object if the XSD component is an element, a \
        group or a complex type; a list if the XSD component is an attribute group; \
        a simple data type object otherwise. If *validation* argument is 'lax' a 2-items \
        tuple is returned, where the first item is the decoded object and the second item \
        is a list containing the errors.
        :raises: :exc:`xmlschema.XMLSchemaValidationError` if the object is not decodable by \
        the XSD component, or also if it's invalid when ``validation='strict'`` is provided.
        """
        tag = getattr(self, 'tag', None)
        kwargs['source'] = self.maps.settings.get_resource_from_data(obj, tag)
        kwargs['converter'] = self.maps.settings.get_converter(**kwargs)
        context = DecodeContext(**kwargs)

        result = self.raw_decode(obj, validation, context)
        if isinstance(result, EmptyType):
            return (None, context.errors) if validation == 'lax' else None
        return (result, context.errors) if validation == 'lax' else result

    def encode(self, obj: Any, validation: str = 'strict', **kwargs: Any) -> EncodeType[Any]:
        """
        Encodes data to XML.

        :param obj: the data to be encoded to XML.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip.
        :param kwargs: optional keyword arguments for the method :func:`iter_encode`.
        :return: An element tree's Element if the original data is a structured data or \
        a string if it's simple type datum. If *validation* argument is 'lax' a 2-items \
        tuple is returned, where the first item is the encoded object and the second item \
        is a list containing the errors.
        :raises: :exc:`xmlschema.XMLSchemaValidationError` if the object is not encodable by \
        the XSD component, or also if it's invalid when ``validation='strict'`` is provided.
        """
        kwargs['source'] = obj
        kwargs['converter'] = self.maps.settings.get_converter(**kwargs)
        context = EncodeContext(**kwargs)

        result = self.raw_encode(obj, validation, context)
        if isinstance(result, EmptyType):
            return (None, context.errors) if validation == 'lax' else None
        return (result, context.errors) if validation == 'lax' else result

    def iter_decode(self, obj: ST, validation: str = 'lax', **kwargs: Any) \
            -> IterDecodeType[DT]:
        """
        Creates an iterator for decoding an XML source to a Python object.

        :param obj: the XML data.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip'.
        :param kwargs: keyword arguments for the decoder API.
        :return: Yields a decoded object, eventually preceded by a sequence of \
        validation or decoding errors.
        """
        tag = getattr(self, 'tag', None)
        kwargs['source'] = self.maps.settings.get_resource_from_data(obj, tag)
        kwargs['converter'] = self.maps.settings.get_converter(**kwargs)
        context = DecodeContext(**kwargs)

        result = self.raw_decode(obj, validation, context)
        yield from context.errors
        context.errors.clear()
        if not isinstance(result, EmptyType):
            yield result

    def iter_encode(self, obj: Any, validation: str = 'lax', **kwargs: Any) \
            -> IterEncodeType[Any]:
        """
        Creates an iterator for encoding data to an Element tree.

        :param obj: The data that has to be encoded.
        :param validation: The validation mode. Can be 'lax', 'strict' or 'skip'.
        :param kwargs: keyword arguments for the encoder API.
        :return: Yields an Element, eventually preceded by a sequence of validation \
        or encoding errors.
        """
        kwargs['source'] = obj
        kwargs['converter'] = self.maps.settings.get_converter(**kwargs)
        context = EncodeContext(**kwargs)

        result = self.raw_encode(obj, validation, context)
        if is_etree_element(result):
            for err in context.errors:
                err.root = result
                yield err
        else:
            yield from context.errors

        context.errors.clear()
        if not isinstance(result, EmptyType):
            yield result

    @abstractmethod
    def raw_decode(self, obj: ST, validation: str, context: ValidationContext) \
            -> Union[DT, EmptyType]:
        """
        Internal decode method. Takes the same arguments as *decode*, but keyword arguments
        are replaced with a decode context. Returns a decoded data structure, usually a
        nested dict and/or list.
        """
        raise NotImplementedError()

    @abstractmethod
    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) -> Any:
        """
        Internal encode method. Takes the same arguments as *encode*, but keyword arguments
        are replaced with a decode context. Returns a tree of Elements or a fragment of it
        (e.g. an attribute value).
        """
        raise NotImplementedError()
