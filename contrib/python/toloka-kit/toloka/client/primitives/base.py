__all__ = [
    'VariantRegistry',
    'autocast_to_enum',
    'fix_attrs_converters',
    'BaseTolokaObjectMetaclass',
    'BaseTolokaObject',
    'BaseParameters',
]

import inspect
import logging
import typing
from copy import copy, deepcopy
from enum import Enum
from functools import update_wrapper, partial
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar, Union, Tuple

import attr
import simplejson as json


from ...util._extendable_enum import ExtendableStrEnumMetaclass
from .._converter import converter
from ..exceptions import SpecClassIdentificationError
from ...util._codegen import (
    attribute, expand, fix_attrs_converters, REQUIRED_KEY, ORIGIN_KEY, AUTOCAST_KEY,
    universal_decorator,
)
from ...util._typing import generate_type_var_mapping

E = TypeVar('E', bound=Enum)

logger = logging.getLogger(__file__)


class VariantRegistry:

    def __init__(self, field: str, enum: Type[E], extendable: Optional[bool] = None):
        if extendable is None:
            extendable = isinstance(enum, ExtendableStrEnumMetaclass)
        if extendable and not isinstance(enum, ExtendableStrEnumMetaclass):
            raise ValueError('VariantRegistry could be extendable only if spec_enum is extendable.')
        self.field: str = field
        self.enum: Type[E] = enum
        self.registered_classes: Dict[E, type] = {}
        self.extendable = extendable

    def register(self, type_: type, value: E) -> type:

        if not isinstance(value, self.enum):
            raise TypeError(f'spec_value must be an instance of {self.enum} not {value}')

        if value in self.registered_classes:
            raise TypeError(f'Specification for {value} is already registered')

        setattr(type_, self.field, value)
        self.registered_classes[value] = type_

        return type_

    def generate_subtype(self, type_: type, value: E) -> type:
        if not self.extendable:
            raise NotImplementedError('Only extendable VariantRegistry can generate subtype')

        generated_type_name = '_Generated' + value.value.title() + type_.__name__
        BaseTolokaObjectMetaclass(generated_type_name, (type_,), {}, spec_value=value)
        logger.info(f'{generated_type_name} class was generated. '
                    f'Probably it is a new functionality on the platform.\n'
                    f'If you want it to be supported by toloka-kit faster you can make feature request here:'
                    f'https://github.com/Toloka/toloka-kit/issues/new/choose.')
        return self.registered_classes[value]

    def __getitem__(self, value: E) -> type:
        return self.registered_classes[value]


def get_autocast_converter(type_):
    structure_type = convert_type_recursively(type_, partial(replace_with_any_if_not_whitelisted, types_whitelist=(Enum,)))

    def autocast_converter(value: convert_type_recursively(type_, enum_type_to_union)):
        return converter.structure(value, structure_type)

    return autocast_converter


def convert_type_recursively(cur_type, type_converter):
    if cur_type.__module__ == 'typing':
        if not hasattr(cur_type, '__args__') or not hasattr(cur_type, '__origin__'):
            return type_converter(cur_type)

        origin = cur_type.__origin__
        # starting from python 3.7 origin of generic types returns true type instead of typing generic (i.e.
        # typing.List[int].__origin__ == list) but using types as type hints directly supported only in python >= 3.9
        if origin.__module__ != 'typing':
            origin = getattr(typing, origin.__name__.title())

        # Callables are unsupported
        if origin is typing.Callable:
            return type_converter(cur_type)

        new_args = tuple(convert_type_recursively(arg, type_converter=type_converter) for arg in cur_type.__args__)
        return origin[new_args]

    return type_converter(cur_type)


def enum_type_to_union(cur_type):
    if inspect.isclass(cur_type) and issubclass(cur_type, Enum):
        possible_types = set(type(item.value) for item in cur_type)
        return typing.Union[(cur_type, *possible_types)] if possible_types else cur_type
    return cur_type


def replace_with_any_if_not_whitelisted(cur_type, types_whitelist: Tuple[Type]):
    if inspect.isclass(cur_type) and issubclass(cur_type, types_whitelist):
        return cur_type
    return Any


class BaseTolokaObjectMetaclass(type):

    def __new__(mcs, name, bases, namespace, auto_attribs=True, kw_only=True, frozen=False, order=True, eq=True,
                **kwargs):
        cls = attr.attrs(
            auto_attribs=auto_attribs,
            kw_only=kw_only,
            field_transformer=mcs.transformer,
            frozen=frozen,
            order=order,
            eq=eq,
            str=True,
            collect_by_mro=True,
        )(super().__new__(mcs, name, bases, namespace, **kwargs))

        cls = fix_attrs_converters(cls)

        # Transformer's change in field type does not affect created
        # class's annotations. So we synchronize them manually
        annotations = getattr(cls.__dict__, '__annotations__', {})
        for field in attr.fields(cls):
            if field.type is not None:
                annotations[field.name] = field.type
        cls.__annotations__ = annotations

        return cls

    @staticmethod
    def transformer(type_: type, fields: List[attr.Attribute]) -> List[attr.Attribute]:
        transformed_fields = []

        for field in fields:
            # Make all attributes optional unless explicitly configured otherwise
            if not field.metadata.get(REQUIRED_KEY):
                field = field.evolve(
                    type=Optional[field.type] if field.type else field.type,
                    default=None if field.default is attr.NOTHING else field.default,
                )

            if field.metadata.get(AUTOCAST_KEY):
                def on_setattr(self, attrib, value, f=get_autocast_converter(field.type)):
                    return f(value)

                field = field.evolve(
                    converter=get_autocast_converter(field.type),
                    on_setattr=on_setattr,
                )

            transformed_fields.append(field)

        return transformed_fields


class BaseTolokaObject(metaclass=BaseTolokaObjectMetaclass):
    """
    A base class for classes representing Toloka objects.



    Subclasses of BaseTolokaObject will:
    * Automatically convert annotated attributes attributes via attrs making them optional
      if not explicitly configured otherwise
    * Skip missing optional fields during unstructuring with client's cattr converter
    """

    _variant_registry: ClassVar[Optional[VariantRegistry]] = None
    _unexpected: Dict[str, Any] = attribute(factory=dict, init=False)

    def __new__(cls, *args, **kwargs):
        """Overriding new for our check to be executed before auto-generated __init__"""
        if cls.is_variant_incomplete():
            message = 'Cannot instantiate an incomplete variant type on field {}'
            raise TypeError(message.format(cls._variant_registry.field))

        return super().__new__(cls)

    @classmethod
    def __init_subclass__(
            cls,
            spec_enum: Optional[Union[str, Type[E]]] = None,
            spec_field: Optional[str] = None,
            spec_value=None,
            extend_spec: Optional[bool] = None,
    ):
        super().__init_subclass__()
        # Completing a variant type
        if spec_value is not None:
            cls._variant_registry.register(cls, spec_value)

        if extend_spec and (spec_enum is None or spec_field is None):
            raise ValueError('extend_spec could be True only with spec_enum and spec_field provided')

        # Making into a variant type
        if spec_enum is not None or spec_field is not None:

            if spec_enum is None or spec_field is None:
                raise ValueError('Both spec_enum and spec_field must be provided')

            if cls.is_variant_incomplete():
                message = 'Incomplete variant type on field {} cannot be a variant type itself'
                raise TypeError(message.format(cls._variant_registry.field))

            # TODO: Possibly make it immutable
            enum = getattr(cls, spec_enum) if isinstance(spec_enum, str) else spec_enum
            cls._variant_registry = VariantRegistry(spec_field, enum, extend_spec)

    # Unexpected fields access

    def __getattr__(self, item):
        try:
            # get _unexpected pickle-friendly
            _unexpected = super().__getattribute__('_unexpected')
            return _unexpected[item]
        except KeyError as exc:
            raise AttributeError(str(item)) from exc

    # Variant type related checks

    @classmethod
    def is_variant_base(cls) -> bool:
        return '_variant_registry' in cls.__dict__

    @classmethod
    def is_variant_incomplete(cls) -> bool:
        return cls._variant_registry and cls._variant_registry.field not in cls.__dict__  # type: ignore

    @classmethod
    def is_variant_spec(cls) -> bool:
        return cls._variant_registry and cls._variant_registry.field in cls.__dict__  # type: ignore

    @classmethod
    def get_variant_specs(cls) -> dict:
        variant_specs = {}
        for base in cls.__mro__:
            registry = base.__dict__.get('_variant_registry')
            if registry:
                variant_specs[registry.field] = getattr(cls, registry.field)

        return variant_specs

    @classmethod
    def get_spec_subclass_for_value(cls, spec_value: Union[str, E] = None) -> type:
        try:
            spec_value = cls._variant_registry.enum(spec_value)
        except ValueError:
            return None
        return cls._variant_registry[spec_value]

    # Conversions related functions

    def unstructure(self) -> Optional[dict]:
        data = deepcopy(self._unexpected)
        obj_class = type(self)

        for field in attr.fields(obj_class):
            if field.name == '_unexpected':
                continue

            value = converter.unstructure(getattr(self, field.name))
            if field.metadata.get(REQUIRED_KEY) or value is not None:
                key = field.metadata.get(ORIGIN_KEY, field.name)
                data[key] = value

        data.update(converter.unstructure(self.get_variant_specs()))
        assert '_unexpected' not in data
        return data or None

    @classmethod
    def structure(cls, data: Any):

        cls, type_var_mapping = generate_type_var_mapping(cls)

        # If a class is an incomplete variant type we structure it into
        # one of its subclasses
        if cls.is_variant_incomplete():
            # TODO: Optimize copying
            data = dict(data)  # Do not modify input data
            spec_field = cls._variant_registry.field
            data_field = data.pop(spec_field)
            try:
                spec_value = cls._variant_registry.enum(data_field)

                if spec_value in cls._variant_registry.registered_classes:
                    spec_class = cls._variant_registry[spec_value]
                else:
                    spec_class = cls._variant_registry.generate_subtype(cls, spec_value)
            except Exception:
                raise SpecClassIdentificationError(spec_field=spec_field,
                                                   spec_enum=cls._variant_registry.enum.__name__)
            return spec_class.structure(data)

        data = copy(data)
        kwargs = {}

        for field in attr.fields(cls):
            key = field.metadata.get(ORIGIN_KEY, field.name)
            if key not in data:
                continue

            value = data.pop(key)
            if field.type is not None:
                target_type = _get_mapped_type(field.type, type_var_mapping)
                value = converter.structure(value, target_type)

            kwargs[field.name] = value
        obj = cls(**kwargs)
        obj._unexpected = data
        return obj

    def to_json(self, pretty: bool = False) -> str:
        basic_config = {
            'use_decimal': True,
            'ensure_ascii': False
        }
        if pretty:
            return json.dumps(self.unstructure(), sort_keys=True, indent=4, **basic_config)
        else:
            return json.dumps(self.unstructure(), separators=(',', ':'), **basic_config)

    @classmethod
    def from_json(cls, json_str: str):
        return cls.structure(json.loads(json_str, use_decimal=True))


def _get_mapped_type(t, mapping):
    if isinstance(t, typing.TypeVar):
        return mapping.get(t.__name__, t)
    try:
        origin_type = t.__dict__['__origin__']
        type_args = t.__dict__.get('__args__', [])
    except AttributeError:
        return t
    except KeyError:
        return t
    mapped_args = tuple(_get_mapped_type(t_arg, mapping) for t_arg in type_args)

    # TODO: support all generic types
    origin_type_mapped = {
        tuple: typing.Tuple,
        list: typing.List,
        dict: typing.Dict,
    }
    origin_type = origin_type_mapped.get(origin_type, origin_type)

    if origin_type in [typing.Union, typing.List, typing.Dict, typing.Tuple] and not mapped_args:
        return origin_type
    if origin_type in [typing.Union, typing.List, typing.Tuple]:
        return origin_type[mapped_args]
    if origin_type in [typing.Dict]:
        return typing.Dict[mapped_args[0], mapped_args[1]]

    return t


@universal_decorator(has_parameters=False)
def autocast_to_enum(func: typing.Callable) -> typing.Callable:
    """Function decorator that performs str -> Enum conversion when decorated function is called

    This decorator modifies function so that every argument annotated with any subclass of Enum type (including Enum
    itself) can be passed a value of str (or any )
    """
    signature = inspect.signature(func)
    new_params = []
    # cattr supports structuring of generic types (i.e. List, Dict) instances. We want to perform only str -> Enum
    # structuring conversion. It is possible to achieve this behavior by replacing any other than Enum types with
    # Any type hint (cattr structures data to Any type simply by passing through data without any conversion). Special
    # case is Union[Enum, Any] which is supported in _converter.py.
    casting_types = []
    casting_converter = partial(replace_with_any_if_not_whitelisted, types_whitelist=(Enum,))
    for param in signature.parameters.values():
        new_params.append(param.replace(annotation=convert_type_recursively(param.annotation, enum_type_to_union)))
        casting_types.append(convert_type_recursively(param.annotation, casting_converter))

    signature = signature.replace(parameters=new_params)

    def wrapper(*args, **kwargs):
        bound_arguments = signature.bind(*args, **kwargs)
        new_args = {}
        for (argument_name, argument_value), casting_type, parameter in zip(
                bound_arguments.arguments.items(),
                casting_types,
                signature.parameters.values()
        ):
            new_args[argument_name] = converter.structure(argument_value, casting_type)
        return func(**new_args)

    update_wrapper(wrapper, func)
    wrapper.__signature__ = signature
    wrapper.__casting_types = casting_types

    return wrapper


class ExpandParametersMetaclass(BaseTolokaObjectMetaclass):

    def __new__(mcs, name, bases, namespace, **kwargs):
        if 'Parameters' in namespace:
            namespace.setdefault('__annotations__', {})['parameters'] = namespace['Parameters']
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        cls.__init__ = expand('parameters')(cls.__init__)

        return cls


class BaseParameters(BaseTolokaObject, metaclass=ExpandParametersMetaclass):
    class Parameters(BaseTolokaObject):
        pass
