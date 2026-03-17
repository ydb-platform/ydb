from typing import Type, Any, TypeVar, Callable, Generic, List, Optional, Dict, Iterable, Set, Union, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from inspect import signature
from copy import deepcopy

from typing_inspect import get_origin, get_args, is_union_type, is_forward_ref, get_forward_arg


@dataclass
class MappingExceptionInfo:
    a: Type[Any]
    b: Type[Any]
    a_fields_chain: List[str] = field(default_factory=list)
    b_fields_chain: List[str] = field(default_factory=list)

    def has_fields_chain(self):
        return 0 < len(self.a_fields_chain) or 0 < len(self.b_fields_chain)


class MappingException(Exception):
    def __init__(self, error_description: str, exc_info: Optional[MappingExceptionInfo] = None):
        if exc_info is None:
            message = error_description
        else:
            a_name = self._get_type_name(exc_info.a)
            b_name = self._get_type_name(exc_info.b)
            if exc_info.has_fields_chain():
                a_field_name = self._get_filed_name(exc_info.a_fields_chain)
                b_field_name = self._get_filed_name(exc_info.b_fields_chain)

                message = (
                    f"Cannot map field '{a_field_name}' of type '{a_name}' "
                    f"to field '{b_field_name}' of type '{b_name}': {error_description}"
                )
            else:
                message = f"Cannot map type '{a_name}' to type '{b_name}': {error_description}"

        super(MappingException, self).__init__(message)

    @staticmethod
    def _get_type_name(t: Type) -> str:
        if hasattr(t, "__name__"):
            return t.__name__
        else:
            origin = get_origin(t)
            if origin:
                name = MappingException._get_type_name(origin)
            elif is_forward_ref(t):
                name = get_forward_arg(t)
            else:
                name = t._name
            args = list(map(lambda x: MappingException._get_type_name(x), get_args(t)))
            if len(args) != 0:
                return f"{name}[{', '.join(args)}]"
            else:
                return name

    @staticmethod
    def _get_filed_name(fields_chain: List[str]):
        return ".".join(fields_chain)


class DuplicateMappingException(MappingException):
    def __init__(self, exc_info: MappingExceptionInfo):
        super(DuplicateMappingException, self).__init__("Mapping already defined.", exc_info)


class MissingMappingException(MappingException):
    def __init__(self, exc_info: MappingExceptionInfo, a: Type, b: Type):
        a_name = MappingException._get_type_name(a)
        b_name = MappingException._get_type_name(b)
        super(MissingMappingException, self).__init__(
            f"Mapping from type '{a_name}' to type '{b_name}' is not defined.", exc_info
        )


class ImproperlyConfiguredException(MappingException):
    def __init__(self, exc_info: MappingExceptionInfo, error: str):
        super(ImproperlyConfiguredException, self).__init__(f"Mapping is improperly configured: {error}.")


class UnsupportedFieldException(MappingException):
    def __init__(self, t: Type, field_name: str):
        super(UnsupportedFieldException, self).__init__(f"Unsupported field '{field_name}' for type '{t,__name__}'")


class FieldMappingException(MappingException):
    def __init__(self, exc_info: MappingExceptionInfo, error: str):
        super(MappingException, self).__init__(exc_info, error)


T = TypeVar("T")
F = TypeVar("F")


@dataclass
class FieldDescriptor(Generic[T, F]):
    name: str
    type: Type[F]
    getter: Callable[[T], F]
    setter: Optional[Callable[[T, F], None]]
    is_constructor_arg: bool
    is_required_constructor_arg: bool


T1 = TypeVar("T1")
T2 = TypeVar("T2")
F1 = TypeVar("F1")
F2 = TypeVar("F2")


@dataclass
class FieldMapRule(Generic[T1, F1, T2, F2]):
    from_field: FieldDescriptor[T1, F1]
    to_field: FieldDescriptor[T2, F2]
    converter: Optional[Callable[[F1], F2]]


class MappingDescriptor(ABC, Generic[T]):
    def __init__(self, t: Type[T]):
        self.type = t

    @classmethod
    @abstractmethod
    def supports_type(cls, t: Type[Any]) -> bool:
        """
        Return true if descriptor can describe type
        """
        pass  # pragma: no cover

    @classmethod
    def resolve_type_name(cls, t: Type[Any]) -> Optional[str]:
        return None  # pragma: no cover

    def get_field_descriptor(self, field_name: str) -> Optional[FieldDescriptor[T, F]]:
        if self.is_field_supported(field_name):
            return FieldDescriptor(
                name=field_name,
                type=self.get_preferred_field_type(field_name),
                getter=self.get_getter(field_name),
                setter=self.get_setter(field_name),
                is_constructor_arg=self.is_constructor_arg(field_name),
                is_required_constructor_arg=self.is_required_constructor_arg(field_name),
            )
        else:
            return None

    @abstractmethod
    def get_getter(self, field_name: str) -> Callable[[T], Any]:
        """
        Return setter for field name
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_setter(self, field_name: str) -> Callable[[Dict, Any], None]:
        """
        Return getter for filed name
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_constructor_args(self) -> Set[str]:
        """
        Return constructor args
        """
        pass  # pragma: no cover

    def is_constructor_arg(self, field_name: str) -> bool:
        """
        Checks if filed_name is constructor arg
        """
        return field_name in self.get_constructor_args()

    @abstractmethod
    def get_required_constructor_args(self) -> Set[str]:
        """
        Return required constructor args
        """
        pass  # pragma: no cover

    def is_required_constructor_arg(self, field_name: str) -> bool:
        """
        Checks if field_name is required constructor arg
        """
        return field_name in self.get_required_constructor_args()

    @abstractmethod
    def get_declared_fields(self) -> Set[str]:
        """
        Return set of declared fields. Note that if filed is not declared it still can be supported.
        Used in map_matching.
        """
        pass  # pragma: no cover

    @abstractmethod
    def is_field_supported(self, field_name: str) -> bool:
        """
        Checks if field can be set
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_preferred_field_type(self, field_name: str) -> Type[Any]:
        """
        Returns filed type if available else Any
        """
        pass  # pragma: no cover

    @abstractmethod
    def is_container_type(self) -> bool:
        """
        Marks container types designed to store arbitrary fields
        """
        pass  # pragma: no cover

    @staticmethod
    def uncase(field_name: str) -> str:
        return field_name.replace("_", "").lower()

    @staticmethod
    def to_uncase_dict(fields: Iterable[str]) -> Dict[str, str]:
        return {MappingDescriptor.uncase(field): field for field in fields}


class CommonTypeMappingDescriptor(MappingDescriptor):
    def __init__(self, t: Type[T]):
        super(CommonTypeMappingDescriptor, self).__init__(t)
        self.constructor_parameters = signature(t.__init__).parameters
        self.uncased_dict = self.to_uncase_dict(self.constructor_parameters.keys())

    @classmethod
    def supports_type(cls, t: Type[Any]) -> bool:
        return True

    def get_getter(self, field_name: str) -> Callable[[T], Any]:
        def getter(obj: T):
            if hasattr(obj, field_name):
                return getattr(obj, field_name)

        return getter

    def get_setter(self, field_name: str) -> Callable[[T, Any], None]:
        def setter(obj, value):
            setattr(obj, field_name, value)

        return setter

    def get_preferred_field_type(self, field_name: str) -> Type[Any]:
        param = self.constructor_parameters.get(field_name)
        if param is not None:
            return param.annotation
        else:
            return Any

    def get_constructor_args(self) -> Set[str]:
        return set(self.constructor_parameters.keys()).difference({"self"})

    def get_required_constructor_args(self) -> Set[str]:
        return {name for name, props in self.constructor_parameters.items() if props.default == props.empty}.difference(
            {"self"}
        )

    def get_declared_fields(self) -> Set[str]:
        return self.get_constructor_args()

    def is_field_supported(self, field_name: str) -> bool:
        return True

    def is_container_type(self) -> bool:
        return False


class DictMappingDescriptor(MappingDescriptor):
    def __init__(self, d: Type[Dict]):
        super(DictMappingDescriptor, self).__init__(d)

    @classmethod
    def supports_type(cls, t: Type[Any]) -> bool:
        return t is dict

    def get_getter(self, field_name: str) -> Callable[[Dict], Any]:
        def getter(d: Dict):
            return d.get(field_name)

        return getter

    def get_setter(self, field_name: str) -> Callable[[Dict, Any], None]:
        def setter(d: Dict, value: Any):
            d[field_name] = value

        return setter

    def get_constructor_args(self) -> Set[str]:
        return set()

    def get_required_constructor_args(self) -> Set[str]:
        return set()

    def get_declared_fields(self) -> Set[str]:
        return set()

    def is_field_supported(self, field_name: str) -> bool:
        return True

    def get_preferred_field_type(self, field_name: str) -> Type[Any]:
        return Any

    def is_container_type(self) -> bool:
        return True


L = TypeVar("L")
R = TypeVar("R")


class MappingConfigFlow(Generic[L, R]):
    def __init__(self, mapper: "Mapper", left_descriptor: MappingDescriptor, right_descriptor: MappingDescriptor):
        self.mapper = mapper
        self.left = left_descriptor.type
        self.left_descriptor = left_descriptor
        self.right = right_descriptor.type
        self.right_descriptor = right_descriptor

        self.l_to_r_touched = False
        self.l_to_r_map_list: List[FieldMapRule] = []

        self.r_to_l_touched = False
        self.r_to_l_map_list: List[FieldMapRule] = []

        self.l_to_r_converter_callable: Optional[Callable[[L, Dict[str, Any]], R]] = None
        self.r_to_l_converter_callable: Optional[Callable[[R, Dict[str, Any]], L]] = None

    def l_to_r(
        self, left_field_name: str, right_field_name: str, converter: Callable[[Any], Any] = None
    ) -> "MappingConfigFlow":
        left_field = self.left_descriptor.get_field_descriptor(left_field_name)
        if left_field is None:
            raise UnsupportedFieldException(self.left_descriptor.type, left_field_name)

        right_field = self.right_descriptor.get_field_descriptor(right_field_name)
        if right_field is None:
            raise UnsupportedFieldException(self.right_descriptor.type, right_field_name)

        self.l_to_r_map_list.append(FieldMapRule(from_field=left_field, to_field=right_field, converter=converter))

        self.l_to_r_touched = True
        self._l_to_r_check()
        return self

    def r_to_l(
        self, left_field_name: str, right_field_name: str, converter: Callable[[Any], Any] = None
    ) -> "MappingConfigFlow":
        left_field = self.left_descriptor.get_field_descriptor(left_field_name)
        if left_field is None:
            raise UnsupportedFieldException(self.left_descriptor.type, left_field_name)

        right_field = self.right_descriptor.get_field_descriptor(right_field_name)
        if right_field is None:
            raise UnsupportedFieldException(self.right_descriptor.type, right_field_name)

        self.r_to_l_map_list.append(FieldMapRule(from_field=right_field, to_field=left_field, converter=converter))

        self.r_to_l_touched = True
        self._r_to_l_check()
        return self

    def bidirectional(self, l_field_name: str, r_field_name: str) -> "MappingConfigFlow":
        self.l_to_r(l_field_name, r_field_name)
        self.r_to_l(l_field_name, r_field_name)
        return self

    def l_to_r_empty(self):
        if self.l_to_r_touched:
            raise ImproperlyConfiguredException(
                MappingExceptionInfo(self.left, self.right), "empty mapping after another configuration"
            )
        self.l_to_r_touched = True

        self._l_to_r_check()
        return self

    def r_to_l_empty(self):
        if self.r_to_l_touched:
            raise ImproperlyConfiguredException(
                MappingExceptionInfo(self.left, self.right), "empty mapping after another configuration"
            )

        self.r_to_l_touched = True
        self._r_to_l_check()
        return self

    def bidirectional_empty(self):
        self.r_to_l_empty()
        self.l_to_r_empty()
        return self

    def map_matching(self, ignore_case: bool = False) -> "MappingConfigFlow":
        if self.left_descriptor.is_container_type() and self.right_descriptor.is_container_type():
            raise ImproperlyConfiguredException(
                MappingExceptionInfo(self.left, self.right), "map matching for two container types doesn't make sense"
            )
        if ignore_case and (self.left_descriptor.is_container_type() or self.right_descriptor.is_container_type()):
            raise ImproperlyConfiguredException(
                MappingExceptionInfo(self.left, self.right),
                "map matching for container types with ignored case does not supported yet",
            )

        if ignore_case:
            l_fields = MappingDescriptor.to_uncase_dict(self.left_descriptor.get_declared_fields())
            r_fields = MappingDescriptor.to_uncase_dict(self.right_descriptor.get_declared_fields())
        else:
            l_fields = {f: f for f in self.left_descriptor.get_declared_fields()}
            r_fields = {f: f for f in self.right_descriptor.get_declared_fields()}

        if self.left_descriptor.is_container_type():
            common_fields = set(r_fields.keys())
            l_fields = r_fields
        elif self.right_descriptor.is_container_type():
            common_fields = set(l_fields.keys())
            r_fields = l_fields
        else:
            common_fields = set(l_fields.keys()).intersection(r_fields.keys())
        for f in common_fields:
            lf_name = l_fields[f]
            rf_name = r_fields[f]
            self.bidirectional(lf_name, rf_name)
        return self

    def l_to_r_converter(
        self, converter: Union[Callable[[L], R], Callable[[L, Dict[str, Any]], R]]
    ) -> "MappingConfigFlow":
        self.l_to_r_converter_callable = self._wrap_converter_if_need_to(converter)

        self._l_to_r_check()
        return self

    def r_to_l_converter(
        self, converter: Union[Callable[[R], L], Callable[[R, Dict[str, Any]], L]]
    ) -> "MappingConfigFlow":
        self.r_to_l_converter_callable = self._wrap_converter_if_need_to(converter)

        self._r_to_l_check()
        return self

    def register(self) -> None:
        if self.l_to_r_touched:
            self.mapper._add_map_rules(self.left, self.right, self.l_to_r_map_list)
        if self.r_to_l_touched:
            self.mapper._add_map_rules(self.right, self.left, self.r_to_l_map_list)
        if self.l_to_r_converter_callable is not None:
            self.mapper._add_converter(self.left, self.right, self.l_to_r_converter_callable)
        if self.r_to_l_converter_callable is not None:
            self.mapper._add_converter(self.right, self.left, self.r_to_l_converter_callable)

    def _l_to_r_check(self):
        if self.l_to_r_touched and self.l_to_r_converter_callable is not None:
            raise ImproperlyConfiguredException(
                MappingExceptionInfo(self.left, self.right), "Map rules and converter defined at the same time"
            )

    def _r_to_l_check(self):
        if self.r_to_l_touched and self.r_to_l_converter_callable is not None:
            raise ImproperlyConfiguredException(
                MappingExceptionInfo(self.right, self.left), "Map rules and converter defined at the same time"
            )

    @staticmethod
    def _wrap_converter_if_need_to(converter: Union[Callable[[T1], T2], Callable[[T1, Dict[str, Any]], T2]]):
        if len(signature(converter).parameters) == 1:

            def wrapped_converter(left: L, ignored_context: Dict[str, Any]):
                return converter(left)

            return wrapped_converter
        else:
            return converter


class Mapper:
    DEFAULT_DESCRIPTORS: List[Type[MappingDescriptor]] = [
        DictMappingDescriptor,
        CommonTypeMappingDescriptor,
    ]

    PRIMITIVE_CONVERTERS: Dict[Tuple[Type[Any], Type[Any]], Callable[[Any], Any]] = {
        (int, str): str,
        (int, float): float,
        (float, str): str,
        (str, int): int,
        (str, float): float,
        (str, bytes): lambda s: s.encode("utf-8"),
    }

    def __init__(self, custom_descriptors: Optional[List[Type[MappingDescriptor]]] = None):
        self.custom_descriptors = custom_descriptors if custom_descriptors else []

        self.forward_ref_dict: Dict[str, Type[Any]] = {}

        self.map_rules: Dict[Type, Dict[Type, List[FieldMapRule]]] = {}
        self.converters: Dict[Type[Any], Dict[Type[Any], Callable[[Any, Dict[str, Any]], Any]]] = {}

    def mapping(self, a: Union[Type, MappingDescriptor], b: Union[Type, MappingDescriptor]) -> MappingConfigFlow:
        if not isinstance(a, MappingDescriptor):
            a = self._wrap_type_to_descriptor(a)
        if not isinstance(b, MappingDescriptor):
            b = self._wrap_type_to_descriptor(b)
        return MappingConfigFlow(self, a, b)

    def _wrap_type_to_descriptor(self, t: Type[Any]):
        for d in self.custom_descriptors + self.DEFAULT_DESCRIPTORS:
            if d.supports_type(t):
                return d(t)
        else:
            raise Exception(f"Cannot found descriptor for type '{t}'")

    def _add_map_rules(self, a: Type, b: Type, rules: List[FieldMapRule]):
        a_type_mappings = self.map_rules.setdefault(a, {})
        a_type_converters = self.converters.setdefault(a, {})
        if b in a_type_mappings or b in a_type_converters:
            raise DuplicateMappingException(MappingExceptionInfo(a, b))

        a_type_mappings[b] = rules
        self._add_class_to_forward_ref_dict(a)
        self._add_class_to_forward_ref_dict(b)

    def _add_converter(self, a: Type[L], b: Type[R], converter: Callable[[L, Dict[str, Any]], R]):
        a_type_mappings = self.map_rules.setdefault(a, {})
        a_type_converters = self.converters.setdefault(a, {})
        if b in a_type_mappings or b in a_type_converters:
            raise DuplicateMappingException(MappingExceptionInfo(a, b))

        a_type_converters[b] = converter
        self._add_class_to_forward_ref_dict(a)
        self._add_class_to_forward_ref_dict(b)

    def _add_class_to_forward_ref_dict(self, t: Type):
        if hasattr(t, "__name__"):
            name = t.__name__
        else:
            for d in self.custom_descriptors:
                name = d.resolve_type_name(t)
                if name is not None:
                    break
            else:
                raise Exception(f"Cannot define name of class {t}")
        if name in self.forward_ref_dict and t != self.forward_ref_dict[name]:
            raise Exception(
                f"Conflicting forward references '{name}'. Rearrange your class definitions or use type aliases."
            )
        else:
            self.forward_ref_dict[name] = t

    def _resolve_forward_ref(self, t: Type[Any]) -> Type[Any]:
        if isinstance(t, str) or is_forward_ref(t):
            if isinstance(t, str):
                name = t
            else:
                name = get_forward_arg(t)
            if name in self.forward_ref_dict:
                return self.forward_ref_dict[name]
            else:
                raise Exception(f"Unknown forward reference '{name}'")
        else:
            return t

    def map(
        self, a_obj: Any, b: Type[T], context: Dict[str, Any] = None, *, exc_info: Optional[MappingExceptionInfo] = None
    ) -> T:
        a = a_obj.__class__
        if context is None:
            context = {}
        if exc_info is None:
            exc_info = MappingExceptionInfo(a, b)

        if self._has_converter(a, b):
            return self._convert_with_converter(a_obj, b, context, exc_info)
        elif self._has_mapping_rules(a, b):
            return self._map_with_map_rules(a_obj, b, context, exc_info)
        elif self._is_iterable_mapping_possible(a, b):
            return self._map_iterables(a_obj, b, context, exc_info)
        elif self._has_primitive_mapping(a, b):
            return self._map_primitives(a_obj, b, exc_info)
        elif self._is_direct_assignment_possible(a, b):
            return deepcopy(a_obj)
        else:
            raise MissingMappingException(exc_info, a, b)

    def _has_converter(self, a: Type[Any], b: Type[Any]) -> bool:
        if a not in self.converters:
            return False

        b = self._resolve_forward_ref(b)
        if is_union_type(b):
            for actual_class in get_args(b):
                if self._resolve_forward_ref(actual_class) in self.converters[a]:
                    return True
            else:
                return False
        else:
            return b in self.converters[a]

    def _convert_with_converter(
        self, a_obj: Any, b: Type[Any], context: Dict[str, Any], exc_info: MappingExceptionInfo
    ):
        a = a_obj.__class__

        if is_union_type(b):
            for to_class in get_args(b):
                to_class = self._resolve_forward_ref(to_class)
                if to_class in self.converters[a]:
                    converter = self.converters[a][to_class]
                    break
            else:
                raise FieldMappingException(exc_info, f"Not found matching class in union {b}")
        else:
            converter = self.converters[a][b]

        try:
            return converter(a_obj, context)
        except Exception as e:
            raise FieldMappingException(exc_info, "Error on converting") from e

    def _has_mapping_rules(self, a: Type[Any], b: Type[Any]) -> bool:
        if a not in self.map_rules:
            return False

        b = self._resolve_forward_ref(b)
        if is_union_type(b):
            for actual_class in get_args(b):
                if self._resolve_forward_ref(actual_class) in self.map_rules[a]:
                    return True
            else:
                return False
        else:
            return b in self.map_rules[a]

    def _map_with_map_rules(self, a_obj: Any, b: Type[Any], context: Dict[str, Any], exc_info: MappingExceptionInfo):
        a = a_obj.__class__

        if is_union_type(b):
            for to_class in get_args(b):
                if to_class in self.map_rules[a]:
                    to_class = self._resolve_forward_ref(to_class)
                    map_rules = self.map_rules[a][to_class]
                    break
            else:
                raise FieldMappingException(exc_info, f"Not found matching class in union {b}")
        else:
            map_rules = self.map_rules[a][b]

        constructor_args = {}
        fields = []

        for rule in map_rules:
            from_field_type = self._resolve_forward_ref(rule.from_field.type)
            to_field_type = self._resolve_forward_ref(rule.to_field.type)
            fields_exc_info = MappingExceptionInfo(
                from_field_type,
                to_field_type,
                exc_info.a_fields_chain + [rule.from_field.name],
                exc_info.b_fields_chain + [rule.to_field.name],
            )
            field_value = rule.from_field.getter(a_obj)
            if field_value is None and not rule.to_field.is_required_constructor_arg:
                continue

            if rule.converter is not None:
                try:
                    value = rule.converter(field_value)
                except Exception as e:
                    raise FieldMappingException(fields_exc_info, "Error on value conversion") from e
            else:
                value = self.map(field_value, to_field_type, context, exc_info=fields_exc_info)

            if rule.to_field.is_constructor_arg:
                constructor_args[rule.to_field.name] = value
            else:
                fields.append((rule.to_field.setter, value))

        b_obj = b(**constructor_args)
        for op in fields:
            setter, value = op
            setter(b_obj, value)

        return b_obj

    def _has_primitive_mapping(self, a: Type[Any], b: Type[Any]) -> bool:
        return (a, b) in self.PRIMITIVE_CONVERTERS

    def _map_primitives(self, a_obj: Any, b: Type[Any], exc_info: MappingExceptionInfo):
        a = a_obj.__class__
        primitive_converter = self.PRIMITIVE_CONVERTERS[(a, b)]
        try:
            return primitive_converter(a_obj)
        except Exception as e:
            raise FieldMappingException(exc_info, "Exception on mapping primitive values") from e

    def _is_iterable_mapping_possible(self, a: Type[Any], b: Type[Any]) -> bool:
        return self._is_iterable(a) and self._is_iterable(b)

    def _map_iterables(self, a_obj: Any, b: Type[Any], context: Dict[str, Any], exc_info: MappingExceptionInfo):
        b = self._resolve_forward_ref(b)
        args = get_args(b)
        if len(args) == 0:
            # Iterable without type
            to_type = get_origin(b)

            try:
                return to_type(*a_obj)
            except Exception as e:
                raise FieldMappingException(exc_info, "Error on mapping iterable") from e

        elif len(args) == 1:
            # Iterable with type
            to_type = get_origin(b)
            to_type_item = self._resolve_forward_ref(args[0])

            mapped_list = []
            for index, item in enumerate(a_obj):
                current_exc_info = MappingExceptionInfo(
                    exc_info.a,
                    exc_info.b,
                    exc_info.a_fields_chain + [f"[{index}]"],
                    exc_info.b_fields_chain + [f"[{index}]"],
                )
                try:
                    mapped_list.append(self.map(item, to_type_item, context, exc_info=current_exc_info))
                except Exception as e:
                    raise FieldMappingException(exc_info, f"Error on mapping iterable at index {index}") from e
            return to_type(mapped_list)

        else:
            # Tuple
            to_type = get_origin(b)

            mapped_list = []
            for index, item in enumerate(a_obj):
                to_type_item = self._resolve_forward_ref(args[index])
                current_exc_info = MappingExceptionInfo(
                    exc_info.a,
                    exc_info.b,
                    exc_info.a_fields_chain + [f"[{index}]"],
                    exc_info.b_fields_chain + [f"[{index}]"],
                )
                try:
                    mapped_list.append(self.map(item, to_type_item, context, exc_info=current_exc_info))
                except Exception as e:
                    raise FieldMappingException(exc_info, f"Error on mapping iterable at index {index}") from e

            return to_type(mapped_list)

    def _is_direct_assignment_possible(self, a: Type[Any], b: Type[Any]) -> bool:
        b = self._resolve_forward_ref(b)
        if b is Any:
            return True
        elif is_union_type(b):
            return any([self._is_direct_assignment_possible(a, t) for t in get_args(b)])
        elif issubclass(a, b):
            return True
        return False

    @staticmethod
    def _is_iterable(t: Type[Any]):
        origin = get_origin(t)
        return origin in [list, set, tuple] or t in [list, set, tuple]
