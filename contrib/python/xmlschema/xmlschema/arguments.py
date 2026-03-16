#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import io
import logging
import os
from collections.abc import Callable, Iterable, MutableMapping, MutableSequence
from functools import partial
from pathlib import Path
from typing import Any, cast, Generic, Optional, overload, Union
from urllib.request import OpenerDirector
from xml.etree.ElementTree import Element
from importlib.resources.abc import Traversable

from xmlschema.aliases import T, XMLSourceType, UriMapperType, IterParseType, BlockType, \
    FillerType, DepthFillerType, ExtraValidatorType, ValidationHookType, ValueHookType, \
    ElementHookType, ElementType, LogLevelType, LocationsType, NsmapType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError, \
    XMLSchemaAttributeError
from xmlschema.translation import gettext as _
from xmlschema.locations import NamespaceResourcesMap
from xmlschema.utils.etree import is_etree_element, is_etree_document
from xmlschema.utils.misc import is_subclass
from xmlschema.utils.streams import is_file_object
from xmlschema.utils.urls import is_url
from xmlschema.xpath import ElementSelector

# Sets of values for arguments with choices
DEFUSE_MODES = frozenset(('never', 'remote', 'nonlocal', 'always'))
SECURITY_MODES = frozenset(('all', 'remote', 'local', 'sandbox', 'none'))
BLOCK_TYPES = frozenset(('text', 'file', 'io', 'url', 'tree'))
LOG_LEVELS = frozenset(('DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL', 10, 20, 30, 40, 50))
VALIDATION_MODES = frozenset(('strict', 'lax', 'skip'))
XMLNS_PROCESSING_MODES = frozenset(('stacked', 'collapsed', 'root-only', 'none'))

XSD_VALIDATION_MODES = frozenset(('strict', 'lax', 'skip'))
"""
XML Schema validation modes
Ref.: https://www.w3.org/TR/xmlschema11-1/#key-va
"""

LOCATIONS_TYPES = (tuple, dict, list, NamespaceResourcesMap)


class Argument(Generic[T]):
    """
    A descriptor for positional and optional arguments. An argument can't be changed nor deleted.
    Arguments are validated with a sequence of validation functions tha are called by the base
    *validated_value* method.
    """
    __slots__ = ('_name', '_check_only', '_default')

    _default: T
    _validators: tuple[Callable[['Argument[T]', T], None], ...] = ()

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = f'_{name}'
        self._check_only = issubclass(owner, Arguments)

    def __str__(self) -> str:
        if hasattr(self, '_default'):
            return _('optional argument {!r}').format(self._name[1:])
        return _('non-default argument {!r}').format(self._name[1:])

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> 'Argument[T]': ...

    @overload
    def __get__(self, instance: Any, owner: type[Any]) -> T: ...

    def __get__(self, instance: Optional[Any], owner: type[Any]) -> Union['Argument[T]', T]:
        if instance is None and self._check_only:
            return self

        try:
            return cast(T, getattr(instance, self._name))
        except AttributeError:
            try:
                return self._default
            except AttributeError:
                if instance is None:
                    msg = _("{} can't be accessed from {!r}").format(self, owner)
                else:
                    msg = _("{} of {!r} object has not been set").format(self, instance)
                raise XMLSchemaAttributeError(msg) from None

    def __set__(self, instance: Any, value: Any) -> None:
        if hasattr(instance, self._name):
            raise XMLSchemaAttributeError(_("can't change {}").format(self))
        setattr(instance, self._name, self.validated_value(value))

    def __delete__(self, instance: Any) -> None:
        raise XMLSchemaAttributeError(_("can't delete {}").format(self))

    def validated_value(self, value: Any) -> T:
        for validator in self._validators:
            validator(self, value)
        return cast(T, value)


class Option(Argument[T]):
    """
    A descriptor for handling optional arguments.

    :param default: The default value for the optional argument.
    """
    def __init__(self, *, default: T) -> None:
        self._default = default


###
# Validation helpers for arguments and options

def validate_type(attr: Argument[T], value: T,
                  types: Union[None, type[T], tuple[type[T], ...]] = None,
                  none: bool = False,
                  call: bool = False) -> None:
    """
    Base function for validating an argument type.

    :param attr: the argument to validate.
    :param value: the argument value to validate.
    :param types: the optional types to validate against.
    :param none: if `True` a None value is accepted.
    :param call: if `True` a callable value is accepted.
    """
    if none and value is None \
            or types is not None and isinstance(value, types) \
            or call and callable(value):
        return None

    if types is None:
        if none and call:
            msg = _("invalid type {!r} for {}, must be None o a callable")
        elif call:
            msg = _("invalid type {!r} for {}, must be a callable")
        elif none:
            msg = _("invalid type {!r} for {}, must be None")
        else:
            return None

        raise XMLSchemaTypeError(msg.format(type(value), attr))

    elif none and call:
        msg = _("invalid type {!r} for {}, must be None, a {!r} instance or a callable")
    elif call:
        msg = _("invalid type {!r} for {}, must be a {!r} instance or a callable")
    elif none:
        msg = _("invalid type {!r} for {}, must be None or a {!r} instance")
    else:
        msg = _("invalid type {!r} for {}, must be a {!r} instance")

    raise XMLSchemaTypeError(msg.format(type(value), attr, types))


def validate_subclass(attr: Argument[T], value: type[Any],
                      cls: type[Any], none: bool = False) -> None:
    if value is None and none:
        return None
    elif cls is dict:
        cls = MutableMapping
    elif cls is list:
        cls = MutableSequence
    if is_subclass(value, cls):
        return None

    if none:
        msg = _("invalid {!r} for {}, must be None or a subclass of {!r}")
    else:
        msg = _("invalid {!r} for {}, must be a subclass {!r}")

    raise XMLSchemaTypeError(msg.format(value, attr, cls))


def validate_choice(attr: Argument[T], value: T, choices: Iterable[T]) -> None:
    if value is not None and value not in choices:
        msg = _("invalid value {!r} for {}: must be one of {}")
        raise XMLSchemaValueError(msg.format(value, attr, tuple(choices)))


def validate_minimum(attr: Argument[int], value: int, min_value: int) -> None:
    if value is not None and value < min_value:
        msg = _("the value of {} must be greater or equal than {}")
        raise XMLSchemaValueError(msg.format(attr, min_value))


def validate_instance(attr: Argument[T], value: T) -> None:
    if isinstance(value, type):
        msg = _("invalid value {!r} for {}, must be an object instance, not a type")
        raise XMLSchemaTypeError(msg.format(value, attr))


bool_validator = partial(validate_type, types=bool)
bool_int_validator = partial(validate_type, types=int)
str_validator = partial(validate_type, types=str)
none_str_validator = partial(validate_type, types=str, none=True)
none_int_validator = partial(validate_type, types=int, none=True)
pos_int_validator = partial(validate_minimum, min_value=1)
non_neg_int_validator = partial(validate_minimum, min_value=0)
callable_validator = partial(validate_type, call=True)
opt_callable_validator = partial(validate_type, none=True, call=True)


class BooleanOption(Option[bool]):
    _validators = bool_validator,


class StringOption(Option[str]):
    _validators = str_validator,


class NillableStringOption(Option[Optional[str]]):
    _validators = none_str_validator,


class PositiveIntOption(Option[int]):
    _validators = pos_int_validator,


class NonNegIntOption(Option[int]):
    _validators = non_neg_int_validator,


###
# XMLResource arguments/settings

class SourceArgument(Argument[XMLSourceType]):
    def validated_value(self, value: Any) -> XMLSourceType:
        if isinstance(value, (str, bytes, Path, io.StringIO, io.BytesIO)):
            return cast(XMLSourceType, value)
        elif is_file_object(value) or is_etree_element(value):
            return cast(XMLSourceType, value)
        elif is_etree_document(value):
            if value.getroot() is None:
                raise XMLSchemaValueError(_("source XML document is empty"))
            return cast(XMLSourceType, value)
        else:
            msg = _("invalid type {!r} for {}, must be a string containing the "
                    "XML document or file path or a URL or a file like object or "
                    "an ElementTree or an Element")
            raise XMLSchemaTypeError(msg.format(type(value), self))


class BaseUrlOption(Option[Optional[str]]):
    """Base URL option test."""
    @overload
    def __get__(self, instance: None, owner: type[Any]) -> 'Argument[Optional[str]]': ...

    @overload
    def __get__(self, instance: Any, owner: type[Any]) -> Optional[str]: ...

    def __get__(self, instance: Any, owner: type[Any]) -> \
            Union['Argument[Optional[str]]', Optional[str]]:
        if instance is None:
            return self._default
        if isinstance(url := getattr(instance, 'url', None), str):
            return os.path.dirname(url)
        return cast(Optional[str], getattr(instance, self._name, self._default))

    def validated_value(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        elif isinstance(value, Traversable):
            return value
        elif not isinstance(value, (str, bytes, Path)):
            msg = _("invalid type {!r} for {}, must be of type {!r}")
            raise XMLSchemaTypeError(msg.format(type(value), self, (str, bytes, Path)))
        elif not is_url(value):
            msg = _("invalid value {!r} for {}")
            raise XMLSchemaValueError(msg.format(value, self))
        elif isinstance(value, str):
            return value
        elif isinstance(value, bytes):
            return value.decode()
        else:
            return str(value)


class AllowOption(Option[str]):
    _validators = str_validator, partial(validate_choice, choices=SECURITY_MODES)


class DefuseOption(Option[str]):
    _validators = str_validator, partial(validate_choice, choices=DEFUSE_MODES)


class LazyOption(Option[Union[bool, int]]):
    _validators = bool_int_validator, non_neg_int_validator


class BlockOption(Option[Optional[BlockType]]):
    def validated_value(self, value: Any) -> Optional[BlockType]:
        if value is None:
            return value
        elif isinstance(value, str):
            value = value.split()

        if isinstance(value, (list, tuple)) and value:
            for v in value:
                if not isinstance(v, str):
                    break
                validate_choice(self, v, BLOCK_TYPES)
            else:
                return tuple(value)

        msg = _("invalid type {!r} for {}, must be None or a tuple/list of strings")
        raise XMLSchemaTypeError(msg.format(type(value), self, (str, tuple)))


class UriMapperOption(Option[Optional[UriMapperType]]):
    _validators = partial(validate_type, types=MutableMapping, none=True, call=True),


class OpenerOption(Option[Optional[OpenerDirector]]):
    _validators = partial(validate_type, types=OpenerDirector, none=True),


class IterParseOption(Option[Optional[IterParseType]]):
    def validated_value(self, value: Any) -> Optional[IterParseType]:
        if value is None:
            return self._default
        validate_type(self, value, none=True, call=True)
        return cast(IterParseType, value)


class SelectorOption(Option[Optional[type[ElementSelector]]]):
    def validated_value(self, value: Any) -> Optional[type[ElementSelector]]:
        if value is None:
            return self._default
        elif not is_subclass(value, ElementSelector):
            msg = _("invalid type {!r} for {}, must be subclass of ElementSelector or None")
            raise XMLSchemaTypeError(msg.format(value, self))
        return cast(Optional[type[ElementSelector]], value)


###
# Other options for schema settings, NamespaceMapper, decoding/encoding context

def check_validation_mode(validation: str) -> None:
    try:
        if validation in XSD_VALIDATION_MODES:
            return
    except TypeError:
        pass

    if not isinstance(validation, str):
        raise XMLSchemaTypeError(_("validation mode must be a string"))
    else:
        raise XMLSchemaValueError(_("validation mode can be 'strict', "
                                    "'lax' or 'skip': %r") % validation)


class ValidationOption(Option[str]):
    _validators = str_validator, partial(validate_choice, choices=XSD_VALIDATION_MODES),


class NamespacesOption(Option[Optional[NsmapType]]):
    _validators = partial(validate_type, types=MutableMapping, none=True),


class LogLevelOption(Option[LogLevelType]):
    _validators = (partial(validate_type, types=(str, int), none=True),
                   partial(validate_choice, choices=LOG_LEVELS))

    def validated_value(self, value: Any) -> Optional[int]:
        super().validated_value(value)
        if isinstance(value, str):
            return cast(int, getattr(logging, value.upper()))
        return cast(Optional[int], value)


class LocationsOption(Option[Optional[LocationsType]]):
    _validators = partial(validate_type, types=LOCATIONS_TYPES, none=True),


class ElementTypeOption(Option[Optional[ElementType]]):
    def validated_value(self, value: Any) -> Optional[ElementType]:
        if value is None or is_subclass(value, Element) or \
                callable(value) and value.__name__ == 'Element' or \
                is_subclass(value, object) and hasattr(value, '__iter__') \
                and hasattr(value, '__len__') and hasattr(value, 'makeelement'):
            return cast(ElementType, value)

        msg = _("invalid type {!r} for {}, must be an Element class or None")
        raise XMLSchemaTypeError(msg.format(value, self, MutableSequence))


class XmlNsProcessingOption(Option[str]):
    _validators = str_validator, partial(validate_choice, choices=XMLNS_PROCESSING_MODES)


class SourceOption(Option[Any]):
    _validators = validate_instance,


class DictClassOption(Option[Optional[type[dict[str, Any]]]]):
    _validators = partial(validate_subclass, cls=dict, none=True),


class ListClassOption(Option[Optional[type[list[Any]]]]):
    _validators = partial(validate_subclass, cls=list, none=True),


class DecimalTypeOption(Option[Union[None, type[str], type[float]]]):
    def validated_value(self, value: Any) -> Union[None, type[str], type[float]]:
        if value is None or is_subclass(value, object):
            return cast(Union[None, type[str], type[float]], value)

        msg = _("invalid type {!r} for {}, must be a type or None")
        raise XMLSchemaTypeError(msg.format(value, self, str, float))


class ElementClassOption(Option[Optional[type[ElementType]]]):
    def validated_value(self, value: Any) -> Optional[type[ElementType]]:
        if value is None or is_subclass(value, object) or callable(value):
            return cast(Optional[type[ElementType]], value)

        msg = _("invalid type {!r} for {}, must be a type, a callable or None")
        raise XMLSchemaTypeError(msg.format(value, self, str, float))


class MaxDepthOption(Option[Optional[int]]):
    _validators = none_int_validator, non_neg_int_validator


class FillerOption(Option[Optional[FillerType]]):
    _validators = opt_callable_validator,


class DepthFillerOption(Option[Optional[DepthFillerType]]):
    _validators = opt_callable_validator,


class ExtraValidatorOption(Option[Optional[ExtraValidatorType]]):
    _validators = opt_callable_validator,


class ValidationHookOption(Option[Optional[ValidationHookType]]):
    _validators = opt_callable_validator,


class ValueHookOption(Option[Optional[ValueHookType]]):
    _validators = opt_callable_validator,


class ElementHookOption(Option[Optional[ElementHookType]]):
    _validators = opt_callable_validator,


###
# Validation-only classes for arguments check

class Arguments:
    """Base class for arguments validation-only classes."""
    @classmethod
    def validate(cls, instance: Any) -> None:
        for attr in dir(cls):
            if attr[0] != '_' and isinstance(value := getattr(cls, attr), Argument):
                value.validated_value(getattr(instance, attr))


class NsMapperArguments(Arguments):
    namespaces = NamespacesOption(default=None)
    process_namespaces = BooleanOption(default=True)
    strip_namespaces = BooleanOption(default=False)
    xmlns_processing = XmlNsProcessingOption(default='stacked')
    source = SourceOption(default=None)


class ConverterArguments(NsMapperArguments):
    dict_class = DictClassOption(default=None)
    list_class = ListClassOption(default=None)
    etree_element_class = ElementTypeOption(default=None)
    text_key = NillableStringOption(default='$')
    attr_prefix = NillableStringOption(default='@')
    cdata_prefix = NillableStringOption(default=None)
    preserve_root = BooleanOption(default=False)
    force_dict = BooleanOption(default=False)
    force_list = BooleanOption(default=False)
