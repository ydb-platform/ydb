import typing
from collections.abc import Collection, Hashable, Mapping
from dataclasses import dataclass, replace
from itertools import chain
from typing import Callable, Generic, TypeVar

from ..common import TypeHint
from ..feature_requirement import HAS_TV_TUPLE, HAS_UNPACK
from .basic_utils import get_type_vars, get_type_vars_of_parametrized, is_generic, is_parametrized, strip_alias
from .fundamentals import get_generic_args
from .implicit_params import fill_implicit_params
from .normalize_type import normalize_type

M = TypeVar("M")
K = TypeVar("K", bound=Hashable)


@dataclass
class MembersStorage(Generic[K, M]):
    members: Mapping[K, TypeHint]
    overriden: Collection[K]
    meta: M


class GenericResolver(Generic[K, M]):
    def __init__(self, members_getter: Callable[[TypeHint], MembersStorage[K, M]]):
        self._raw_members_getter = members_getter

    def get_resolved_members(self, tp: TypeHint) -> MembersStorage[K, M]:
        if is_parametrized(tp):
            return self._get_members_of_parametrized_generic(tp)
        if is_generic(tp):
            return self._get_members_of_parametrized_generic(fill_implicit_params(tp))
        return self._get_members_by_parents(tp)

    def _get_members_of_parametrized_generic(self, parametrized_generic) -> MembersStorage[K, M]:
        origin = strip_alias(parametrized_generic)
        members_storage = self._get_members_by_parents(origin)
        type_var_to_actual = self._get_type_var_to_actual(
            get_type_vars(origin),
            self._unpack_args(get_generic_args(parametrized_generic)),
        )
        return replace(
            members_storage,
            members={
                key: self._parametrize_by_dict(type_var_to_actual, tp)
                for key, tp in members_storage.members.items()
            },
        )

    def _unpack_args(self, args):
        if HAS_UNPACK and any(strip_alias(arg) == typing.Unpack for arg in args):
            return tuple(arg.source for arg in normalize_type(tuple[args]).args)
        return args

    def _get_type_var_to_actual(self, type_vars, args):
        result = {}
        idx = 0
        for tv in type_vars:
            if HAS_TV_TUPLE and isinstance(tv, typing.TypeVarTuple):
                tuple_len = len(args) - len(type_vars) + 1
                result[tv] = args[idx:idx + tuple_len]
                idx += tuple_len
            else:
                result[tv] = (args[idx], )
                idx += 1

        return result

    def _parametrize_by_dict(self, type_var_to_actual, tp: TypeHint) -> TypeHint:
        if tp in type_var_to_actual:
            return type_var_to_actual[tp][0]

        params = get_type_vars_of_parametrized(tp)
        if not params:
            return tp
        return tp[tuple(chain.from_iterable(type_var_to_actual[type_var] for type_var in params))]

    def _get_members_by_parents(self, tp) -> MembersStorage[K, M]:
        members_storage = self._raw_members_getter(tp)
        if not any(
            get_type_vars_of_parametrized(tp) or isinstance(tp, TypeVar)
            for tp in members_storage.members.values()
        ):
            return members_storage
        if not hasattr(tp, "__orig_bases__"):
            return members_storage

        bases_members: dict[K, TypeHint] = {}
        for base in reversed(tp.__orig_bases__):
            bases_members.update(self.get_resolved_members(base).members)

        return replace(
            members_storage,
            members={
                key: (
                    bases_members[key]
                    if (
                        key in bases_members
                        and key not in members_storage.overriden
                        and (is_generic(value) or isinstance(value, TypeVar))
                    )
                    else value
                )
                for key, value in members_storage.members.items()
            },
        )
