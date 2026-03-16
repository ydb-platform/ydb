# ruff: noqa: RET503, UP006
import dataclasses
import sys
import types
import typing
from abc import ABC, abstractmethod
from collections import abc as c_abc, defaultdict
from collections.abc import Hashable, Iterable, Sequence
from copy import copy
from dataclasses import InitVar, dataclass
from enum import Enum, EnumMeta
from functools import lru_cache, partial
from typing import (
    Annotated,
    Any,
    Callable,
    ClassVar,
    Final,
    ForwardRef,
    Literal,
    NewType,
    NoReturn,
    Optional,
    TypeVar,
    Union,
    overload,
)

from ..common import TypeHint, VarTuple
from ..feature_requirement import (
    HAS_PARAM_SPEC,
    HAS_PY_310,
    HAS_PY_311,
    HAS_PY_313,
    HAS_SELF_TYPE,
    HAS_TV_DEFAULT,
    HAS_TV_SYNTAX,
    HAS_TV_TUPLE,
    HAS_TYPE_ALIAS,
    HAS_TYPE_GUARD,
    HAS_TYPE_UNION_OP,
    HAS_TYPED_DICT_REQUIRED,
    HAS_UNPACK,
)
from .basic_utils import create_union, eval_forward_ref, is_new_type, is_subclass_soft, strip_alias
from .fundamentals import get_generic_args
from .implicit_params import ImplicitParamsGetter


class BaseNormType(Hashable, ABC):
    @property
    @abstractmethod
    def origin(self) -> Any:
        ...

    @property
    @abstractmethod
    def args(self) -> VarTuple[Any]:
        ...

    @property
    @abstractmethod
    def source(self) -> TypeHint:
        ...


T = TypeVar("T")


class _BasicNormType(BaseNormType, ABC):
    __slots__ = ("_args", "_source")

    def __init__(self, args: VarTuple[Any], *, source: TypeHint):
        self._source = source
        self._args = args

    @property
    def args(self) -> VarTuple[Any]:
        return self._args

    @property
    def source(self) -> TypeHint:
        return self._source

    def __hash__(self):
        return hash((self.origin, self._args))

    def __eq__(self, other):
        if isinstance(other, _BasicNormType):
            return (
                self.origin == other.origin
                and
                self._args == other._args
            )
        if isinstance(other, BaseNormType):
            return False
        return NotImplemented

    def __repr__(self):
        args_str = f" {list(self.args)}," if self.args else ""
        return f"<{type(self).__name__}({self.origin},{args_str} source={self._source})>"


class _NormType(_BasicNormType):
    __slots__ = (*_BasicNormType.__slots__, "_source")

    def __init__(self, origin: TypeHint, args: VarTuple[Any], *, source: TypeHint):
        self._origin = origin
        super().__init__(args, source=source)

    @property
    def origin(self) -> Any:
        return self._origin


class _UnionNormType(_BasicNormType):
    def __init__(self, args: VarTuple[Any], *, source: TypeHint):
        super().__init__(self._order_args(args), source=source)

    @property
    def origin(self) -> Any:
        return Union

    # ensure stable order of args during one interpreter session
    def _make_orderable(self, obj: object) -> str:
        if isinstance(obj, BaseNormType):
            return f"{obj.origin} {[self._make_orderable(arg) for arg in obj.args]}"
        return str(obj)

    def _order_args(self, args: VarTuple[BaseNormType]) -> VarTuple[BaseNormType]:
        args_list = list(args)
        args_list.sort(key=self._make_orderable)
        return tuple(args_list)


def _type_and_value_iter(args):
    return [(type(arg), arg) for arg in args]


LiteralArg = Union[str, int, bytes, Enum]


class _LiteralNormType(_BasicNormType):
    def __init__(self, args: VarTuple[Any], *, source: TypeHint):
        super().__init__(self._order_args(args), source=source)

    @property
    def origin(self) -> Any:
        return Literal

    # ensure stable order of args during one interpreter session
    def _make_orderable(self, obj: LiteralArg) -> str:
        return f"{type(obj)}{obj.name}" if isinstance(obj, Enum) else repr(obj)

    def _order_args(self, args: VarTuple[LiteralArg]) -> VarTuple[LiteralArg]:
        args_list = list(args)
        args_list.sort(key=self._make_orderable)
        return tuple(args_list)

    def __eq__(self, other):
        if isinstance(other, _LiteralNormType):
            return _type_and_value_iter(self._args) == _type_and_value_iter(other._args)
        if isinstance(other, BaseNormType):
            return False
        return NotImplemented

    __hash__ = _BasicNormType.__hash__


class _AnnotatedNormType(_BasicNormType):
    @property
    def origin(self) -> Any:
        return Annotated

    __slots__ = (*_BasicNormType.__slots__, "_hash")

    def __init__(self, args: VarTuple[Hashable], *, source: TypeHint):
        super().__init__(args, source=source)
        self._hash = self._calc_hash()

    # calculate hash even if one of Annotated metadata is not hashable
    def _calc_hash(self) -> int:
        lst = [self.origin]
        for arg in self._args:
            try:
                arg_hash = hash(arg)
            except TypeError:
                pass
            else:
                lst.append(arg_hash)
        return hash(tuple(lst))

    def __hash__(self):
        return self._hash


class Variance(Enum):
    INVARIANT = 0
    COVARIANT = 1
    CONTRAVARIANT = 2
    INFERRED = 3

    def __repr__(self):
        return f"{type(self).__name__}.{self.name}"


@dataclass
class Bound:
    value: BaseNormType


@dataclass
class Constraints:
    value: VarTuple[BaseNormType]


TypeVarLimit = Union[Bound, Constraints]


class _BaseNormTypeVarLike(BaseNormType):
    __slots__ = ("_source", "_var")

    def __init__(self, var: Any, *, source: TypeHint):
        self._var = var
        self._source = source

    @property
    def origin(self) -> Any:
        return self._var

    @property
    def args(self) -> tuple[()]:
        return ()

    @property
    def source(self) -> TypeHint:
        return self._source

    @property
    def name(self) -> str:
        return self._var.__name__

    def __repr__(self):
        return f"<{type(self).__name__}({self._var})>"

    def __hash__(self):
        return hash(self._var)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self._var == other._var
        if isinstance(other, BaseNormType):
            return False
        return NotImplemented


class NormTV(_BaseNormTypeVarLike):
    __slots__ = (*_BaseNormTypeVarLike.__slots__, "_limit", "_variance", "_default")

    def __init__(self, var: Any, limit: TypeVarLimit, *, source: TypeHint, default: Optional[BaseNormType]):
        super().__init__(var, source=source)
        self._limit = limit

        if var.__covariant__:
            self._variance = Variance.COVARIANT
        if var.__contravariant__:
            self._variance = Variance.CONTRAVARIANT
        if getattr(var, "__infer_variance__", False):
            self._variance = Variance.INFERRED
        self._variance = Variance.INVARIANT
        self._default = default

    @property
    def variance(self) -> Variance:
        return self._variance

    @property
    def limit(self) -> TypeVarLimit:
        return self._limit

    @property
    def default(self) -> Optional[BaseNormType]:
        return self._default


class NormTVTuple(_BaseNormTypeVarLike):
    __slots__ = (*_BaseNormTypeVarLike.__slots__, "_default")

    def __init__(self, var: Any, *, source: TypeHint, default: Optional[tuple[BaseNormType, ...]]):
        super().__init__(var, source=source)
        self._default = default

    @property
    def default(self) -> Optional[tuple[BaseNormType, ...]]:
        return self._default


class NormParamSpec(_BaseNormTypeVarLike):
    __slots__ = (*_BaseNormTypeVarLike.__slots__, "_limit", "_default")

    def __init__(self, var: Any, limit: TypeVarLimit, *, source: TypeHint, default: Optional[tuple[BaseNormType, ...]]):
        super().__init__(var, source=source)
        self._default = default
        self._limit = limit

    @property
    def limit(self) -> TypeVarLimit:
        return self._limit

    @property
    def default(self) -> Optional[tuple[BaseNormType, ...]]:
        return self._default


class NormParamSpecMarker(BaseNormType, ABC):
    __slots__ = ("_param_spec", "_source")

    def __init__(self, param_spec: Any, *, source: TypeHint):
        self._param_spec = param_spec
        self._source = source

    @property
    def param_spec(self) -> NormParamSpec:
        return self._param_spec

    @property
    def args(self) -> tuple[()]:
        return ()

    @property
    def source(self) -> TypeHint:
        return self._source

    def __hash__(self):
        return hash((self.origin, self.param_spec))

    def __eq__(self, other):
        if isinstance(other, NormParamSpecMarker):
            return self.origin == other.origin and self._param_spec == other._param_spec
        if isinstance(other, BaseNormType):
            return False
        return NotImplemented


class _NormParamSpecArgs(NormParamSpecMarker):
    @property
    def origin(self) -> Any:
        return typing.ParamSpecArgs


class _NormParamSpecKwargs(NormParamSpecMarker):
    @property
    def origin(self) -> Any:
        return typing.ParamSpecKwargs


AnyNormTypeVarLike = Union[NormTV, NormTVTuple, NormParamSpec]


class NormTypeAlias(BaseNormType):
    __slots__ = ("_args", "_norm_type_vars", "_source", "_type_alias")

    def __init__(self, type_alias, args: VarTuple[BaseNormType], type_vars: VarTuple[AnyNormTypeVarLike], source):
        self._type_alias = type_alias
        self._args = args
        self._type_vars = type_vars
        self._source = source

    @property
    def origin(self) -> Any:
        return self._type_alias

    @property
    def args(self) -> VarTuple[BaseNormType]:
        return self._args

    @property
    def source(self) -> TypeHint:
        return self._source

    @property
    def value(self):
        return self._type_alias.__value__

    @property
    def module(self):
        return self._type_alias.__module__

    @property
    def type_params(self) -> VarTuple[AnyNormTypeVarLike]:
        return self._type_vars

    def __eq__(self, other):
        if isinstance(other, NormTypeAlias):
            return self._type_alias == other._type_alias and self._args == other._args
        if isinstance(other, BaseNormType):
            return False
        return NotImplemented

    def __hash__(self):
        return hash(self._type_alias)


_SPECIAL_CONSTRUCTOR_TYPE = (
    TypeVar,
    *((typing.ParamSpecArgs, typing.ParamSpecKwargs, typing.ParamSpec) if HAS_PARAM_SPEC else ()),
    *((typing.TypeVarTuple,) if HAS_TV_TUPLE else ()),
)


def make_norm_type(
    origin: TypeHint,
    args: VarTuple[Hashable],
    *,
    source: TypeHint,
) -> BaseNormType:
    if origin == Union:
        if not all(isinstance(arg, BaseNormType) for arg in args):
            raise TypeError
        return _UnionNormType(args, source=source)
    if origin == Literal:
        if not all(type(arg) in [int, bool, str, bytes] or isinstance(type(arg), EnumMeta) for arg in args):
            raise TypeError
        return _LiteralNormType(args, source=source)
    if origin == Annotated:
        return _AnnotatedNormType(args, source=source)
    if isinstance(origin, _SPECIAL_CONSTRUCTOR_TYPE) or isinstance(source, _SPECIAL_CONSTRUCTOR_TYPE):
        raise TypeError
    return _NormType(origin, args, source=source)


NoneType = type(None)
ANY_NT = _NormType(Any, (), source=Any)


def _create_norm_union(args: VarTuple[BaseNormType]) -> BaseNormType:
    return _UnionNormType(args, source=create_union(tuple(a.source for a in args)))


def _dedup(inp: Iterable[T]) -> Iterable[T]:
    in_set = set()
    result = []
    for item in inp:
        if item not in in_set:
            result.append(item)
            in_set.add(item)
    return result


def _create_norm_literal(args: Iterable):
    dedup_args = tuple(_dedup(args))
    return _LiteralNormType(
        dedup_args,
        source=Literal[dedup_args],
    )


def _replace_source(norm: BaseNormType, *, source: TypeHint) -> BaseNormType:
    norm_copy = copy(norm)
    norm_copy._source = source  # type: ignore[attr-defined]
    return norm_copy


def _replace_source_with_union(norm: BaseNormType, sources: list) -> BaseNormType:
    return _replace_source(
        norm=norm,
        source=create_union(tuple(sources)),
    )


NormAspect = Callable[["TypeNormalizer", Any, Any, tuple], Optional[BaseNormType]]


class AspectStorage(list[str]):
    @overload
    def add(self, *, condition: object = True) -> Callable[[NormAspect], NormAspect]:
        ...

    @overload
    def add(self, func: NormAspect) -> NormAspect:
        ...

    def add(self, func: Optional[NormAspect] = None, *, condition: object = True) -> Any:
        if func is None:
            return partial(self.add, condition=condition)

        if condition:
            self.append(func.__name__)
        return func

    def copy(self) -> "AspectStorage":
        return type(self)(super().copy())


class NotSubscribedError(ValueError):
    pass


N = TypeVar("N", bound=BaseNormType)
TN = TypeVar("TN", bound="TypeNormalizer")


class TypeNormalizer:
    def __init__(self, implicit_params_getter: ImplicitParamsGetter):
        self.implicit_params_getter = implicit_params_getter
        self._namespace: Optional[dict[str, Any]] = None

    def _with_namespace(self: TN, namespace: dict[str, Any]) -> TN:
        self_copy = copy(self)
        self_copy._namespace = namespace
        return self_copy

    def _with_module_namespace(self: TN, module_name: str) -> TN:
        try:
            module = sys.modules[module_name]
        except KeyError:
            return self
        return self._with_namespace(vars(module))

    @overload
    def normalize(self, tp: TypeVar) -> NormTV:
        ...

    @overload
    def normalize(self, tp: TypeHint) -> BaseNormType:
        ...

    def normalize(self, tp: TypeHint) -> BaseNormType:
        origin = strip_alias(tp)
        args = get_generic_args(tp)

        result = self._norm_forward_ref(tp)
        if result is not None:
            return result

        for attr_name in self._aspect_storage:
            result = getattr(self, attr_name)(tp, origin, args)
            if result is not None:
                return result

        raise RuntimeError

    def _norm_forward_ref(self, tp):
        if isinstance(tp, str):
            fwd_ref = ForwardRef(tp)
        elif isinstance(tp, ForwardRef):
            fwd_ref = tp
        else:
            return None

        if fwd_ref.__forward_module__ is not None:
            ns = fwd_ref.__forward_module__.__dict__
        elif self._namespace is not None:
            ns = self._namespace
        else:
            raise ValueError(f"Cannot normalize value {tp!r}, there are no namespace to evaluate types")

        return _replace_source(
            self.normalize(eval_forward_ref(ns, fwd_ref)),
            source=tp,
        )

    _aspect_storage = AspectStorage()

    def _norm_iter(self, tps: Iterable[Any]) -> VarTuple[BaseNormType]:
        return tuple(self.normalize(tp) for tp in tps)

    MUST_SUBSCRIBED_ORIGINS = [
        ClassVar, Final, Literal,
        Union, Optional, InitVar,
        Annotated,
    ]
    if HAS_TYPE_GUARD:
        MUST_SUBSCRIBED_ORIGINS.append(typing.TypeGuard)
    if HAS_TYPED_DICT_REQUIRED:
        MUST_SUBSCRIBED_ORIGINS.extend([typing.Required, typing.NotRequired])
    if HAS_PY_313:
        MUST_SUBSCRIBED_ORIGINS.extend([typing.ReadOnly, typing.TypeIs])  # type: ignore[attr-defined]

    @_aspect_storage.add
    def _check_bad_input(self, tp, origin, args):
        if tp in self.MUST_SUBSCRIBED_ORIGINS:
            raise NotSubscribedError(f"{tp} must be subscribed")

        if tp in (NewType, TypeVar):
            raise ValueError(f"{origin} must be instantiated")

    @_aspect_storage.add
    def _norm_none(self, tp, origin, args):
        if origin is None or origin is NoneType:
            return _NormType(None, (), source=tp)

    @_aspect_storage.add
    def _norm_annotated(self, tp, origin, args):
        if origin == Annotated:
            return _AnnotatedNormType(
                (self.normalize(args[0]), *args[1:]),
                source=tp,
            )

    def _get_bound(self, type_var) -> Bound:
        return (
            Bound(ANY_NT)
            if (type_var.__bound__ is None or type_var.__bound__ is NoneType) else
            Bound(self.normalize(type_var.__bound__))
        )

    @_aspect_storage.add
    def _norm_type_var(self, tp, origin, args):
        if isinstance(origin, TypeVar):
            namespaced = self._with_module_namespace(origin.__module__)
            limit = (
                Constraints(
                    tuple(
                        namespaced._dedup_union_args(
                            namespaced._norm_iter(origin.__constraints__),
                        ),
                    ),
                )
                if origin.__constraints__ else
                namespaced._get_bound(origin)
            )
            default = namespaced.normalize(origin.__default__) if HAS_TV_DEFAULT and origin.has_default() else None
            return NormTV(var=origin, limit=limit, source=tp, default=default)

    @_aspect_storage.add(condition=HAS_TV_TUPLE)
    def _norm_type_var_tuple(self, tp, origin, args):
        if isinstance(origin, typing.TypeVarTuple):
            namespaced = self._with_module_namespace(origin.__module__)
            default = namespaced._norm_iter(origin.__default__) if HAS_TV_DEFAULT and origin.has_default() else None
            return NormTVTuple(var=origin, source=tp, default=default)

    @_aspect_storage.add(condition=HAS_PARAM_SPEC)
    def _norm_param_spec(self, tp, origin, args):
        if isinstance(tp, typing.ParamSpecArgs):
            return _NormParamSpecArgs(param_spec=self.normalize(origin), source=tp)

        if isinstance(tp, typing.ParamSpecKwargs):
            return _NormParamSpecKwargs(param_spec=self.normalize(origin), source=tp)

        if isinstance(origin, typing.ParamSpec):
            namespaced = self._with_module_namespace(origin.__module__)
            default = namespaced._norm_iter(origin.__default__) if HAS_TV_DEFAULT and origin.has_default() else None
            return NormParamSpec(
                var=origin,
                limit=namespaced._get_bound(origin),
                source=tp,
                default=default,
            )

    @_aspect_storage.add(condition=HAS_TV_SYNTAX)
    def _norm_type_alias_type(self, tp, origin, args):
        if isinstance(origin, typing.TypeAliasType):
            return NormTypeAlias(
                type_alias=origin,
                args=self._norm_iter(args),
                type_vars=self._norm_iter(tp.__type_params__),
                source=tp,
            )

    @_aspect_storage.add
    def _norm_init_var(self, tp, origin, args):
        if isinstance(origin, InitVar):
            # this origin is InitVar[T]
            return _NormType(
                InitVar,
                (self.normalize(origin.type),),
                source=tp,
            )

    @_aspect_storage.add
    def _norm_new_type(self, tp, origin, args):
        if is_new_type(tp):
            return _NormType(tp, (), source=tp)

    @_aspect_storage.add
    def _norm_tuple(self, tp, origin, args):
        if origin is tuple:
            if tp in (tuple, typing.Tuple):  # not subscribed values
                return _NormType(
                    tuple,
                    (ANY_NT, ...),
                    source=tp,
                )

            # >>> Tuple[()].__args__ == ((),)
            # >>> tuple[()].__args__ == ()
            if not args or args == ((),):
                return _NormType(tuple, (), source=tp)

            if args[-1] is Ellipsis:
                return _NormType(
                    tuple, (*self._norm_iter(args[:-1]), ...),
                    source=tp,
                )

            norm_args = self._norm_iter(args)
            if HAS_UNPACK:
                norm_args = self._unpack_tuple_elements(norm_args)
            return _NormType(tuple, norm_args, source=tp)

    def _unpack_tuple_elements(self, args: VarTuple[BaseNormType]) -> VarTuple[BaseNormType]:
        # it is necessary to unpack the variable-length tuple as well
        if len(args) == 1 and args[0].origin == typing.Unpack:
            inner_tp = args[0].args[0]
            if inner_tp.origin is tuple:
                return inner_tp.args

        return self._unpack_generic_elements(args)

    def _unpack_generic_elements(self, args: VarTuple[Any]) -> VarTuple[BaseNormType]:
        result = []
        for arg in args:
            if isinstance(arg, BaseNormType) and arg.origin == typing.Unpack and self._is_fixed_size_tuple(arg.args[0]):
                result.extend(arg.args[0].args)
            else:
                result.append(arg)
        return tuple(result)

    def _is_fixed_size_tuple(self, tp: BaseNormType) -> bool:
        return tp.origin is tuple and (not tp.args or tp.args[-1] is not Ellipsis)

    @_aspect_storage.add
    def _norm_callable(self, tp, origin, args):
        if origin == c_abc.Callable:
            if not args:
                return _NormType(
                    c_abc.Callable, (..., ANY_NT), source=tp,
                )

            if args[0] is Ellipsis:
                call_args = ...
            elif isinstance(args[0], list):
                call_args = self._norm_iter(args[0])
                if HAS_TV_TUPLE:
                    call_args = self._unpack_generic_elements(call_args)
            else:
                call_args = normalize_type(args[0])
            return _NormType(
                c_abc.Callable, (call_args, self.normalize(args[-1])), source=tp,
            )

    @_aspect_storage.add
    def _norm_literal(self, tp, origin, args):
        if origin == Literal:
            if args == (None,):  # Literal[None] converted to None
                return _NormType(None, (), source=tp)

            if None in args:
                args_without_none = list(args)
                args_without_none.remove(None)

                return _UnionNormType(
                    (
                        _NormType(None, (), source=Literal[None]),
                        _create_norm_literal(args_without_none),
                    ),
                    source=tp,
                )

            return _LiteralNormType(args, source=tp)

    def _unfold_union_args(self, norm_args: Iterable[N]) -> Iterable[N]:
        result: list[N] = []
        for norm in norm_args:
            if norm.origin == Union:
                result.extend(norm.args)
            else:
                result.append(norm)
        return result

    def _dedup_union_args(self, args: Iterable[BaseNormType]) -> Iterable[BaseNormType]:
        args_to_sources: defaultdict[BaseNormType, list[Any]] = defaultdict(list)

        for arg in args:
            args_to_sources[arg].append(arg.source)

        return [
            _replace_source_with_union(arg, sources)
            if len(sources) != 1 and isinstance(arg, _NormType)
            else arg
            for arg, sources in args_to_sources.items()
        ]

    def _merge_literals(self, args: Iterable[N]) -> Sequence[N]:
        result = []
        lit_args: list[N] = []
        for norm in args:
            if norm.origin == Literal:
                lit_args.extend(norm.args)
            else:
                result.append(norm)

        if lit_args:
            result.append(_create_norm_literal(lit_args))
        return result

    _UNION_ORIGINS: list[Any] = [Union]
    if HAS_TYPE_UNION_OP:
        _UNION_ORIGINS.append(types.UnionType)

    @_aspect_storage.add
    def _norm_union(self, tp, origin, args):
        if origin in self._UNION_ORIGINS:
            norm_args = self._norm_iter(args)
            unfolded_n_args = self._unfold_union_args(norm_args)
            unique_n_args = self._dedup_union_args(unfolded_n_args)
            merged_n_args = self._merge_literals(unique_n_args)

            if len(merged_n_args) == 1:
                arg = merged_n_args[0]
                return make_norm_type(origin=arg.origin, args=arg.args, source=tp)
            return _UnionNormType(tuple(merged_n_args), source=tp)

    @_aspect_storage.add
    def _norm_type(self, tp, origin, args):
        if is_subclass_soft(origin, type) and args:
            norm = self.normalize(args[0])

            if norm.origin == Union:
                return _UnionNormType(
                    tuple(
                        _NormType(type, (arg,), source=type[arg.source])
                        for arg in norm.args
                    ),
                    source=tp,
                )

    ALLOWED_ZERO_PARAMS_ORIGINS: set[Any] = {Any, NoReturn}
    if HAS_TYPE_ALIAS:
        ALLOWED_ZERO_PARAMS_ORIGINS.add(typing.TypeAlias)
    if HAS_PY_310:
        ALLOWED_ZERO_PARAMS_ORIGINS.add(dataclasses.KW_ONLY)
    if HAS_PY_311:
        ALLOWED_ZERO_PARAMS_ORIGINS.add(typing.Never)
    if HAS_SELF_TYPE:
        ALLOWED_ZERO_PARAMS_ORIGINS.add(typing.Self)
    if HAS_PY_311:
        ALLOWED_ZERO_PARAMS_ORIGINS.add(typing.LiteralString)

    def _norm_generic_arg(self, arg):
        if arg is Ellipsis:
            return Ellipsis
        if isinstance(arg, tuple):
            return self._norm_iter(arg)
        return self.normalize(arg)

    def _norm_implicit_param(self, param):
        if param is Ellipsis:
            return Ellipsis
        return self.normalize(param)

    @_aspect_storage.add
    def _norm_other(self, tp, origin, args):
        if args:
            norm_args = tuple(self._norm_generic_arg(el) for el in args)
            if HAS_TV_TUPLE:
                norm_args = self._unpack_generic_elements(norm_args)
            return _NormType(origin, norm_args, source=tp)

        params = self.implicit_params_getter.get_implicit_params(origin)
        if not (
            params
            or isinstance(origin, type)
            or origin in self.ALLOWED_ZERO_PARAMS_ORIGINS
        ):
            raise ValueError(f"Cannot normalize value {tp!r}")

        return _NormType(
            origin,
            tuple(self._norm_implicit_param(param) for param in params),
            source=tp,
        )


_STD_NORMALIZER = TypeNormalizer(ImplicitParamsGetter())
_cached_normalize = lru_cache(maxsize=128)(_STD_NORMALIZER.normalize)


def normalize_type(tp: TypeHint) -> BaseNormType:
    try:
        hash(tp)
    except TypeError:
        return _STD_NORMALIZER.normalize(tp)

    return _cached_normalize(tp)
