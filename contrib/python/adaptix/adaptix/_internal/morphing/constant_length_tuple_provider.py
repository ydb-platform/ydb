# ruff: noqa: SIM113
import collections.abc
import typing
from collections.abc import Collection, Mapping

from ..common import Dumper, Loader
from ..compat import CompatExceptionGroup
from ..definitions import DebugTrail
from ..feature_requirement import HAS_UNPACK
from ..provider.essential import CannotProvide, Mediator
from ..provider.located_request import for_predicate
from ..provider.location import GenericParamLoc
from ..struct_trail import append_trail, render_trail_as_note
from .load_error import (
    AggregateLoadError,
    ExcludedTypeLoadError,
    ExtraItemsLoadError,
    LoadError,
    NoRequiredItemsLoadError,
    TypeLoadError,
)
from .provider_template import DumperProvider, LoaderProvider
from .request_cls import DebugTrailRequest, DumperRequest, LoaderRequest, StrictCoercionRequest
from .utils import try_normalize_type

CollectionsMapping = collections.abc.Mapping


@for_predicate(tuple)
class ConstantLengthTupleProvider(LoaderProvider, DumperProvider):
    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        norm = try_normalize_type(request.last_loc.type)
        if len(norm.args) > 1 and norm.args[1] == Ellipsis:
            raise CannotProvide
        if HAS_UNPACK and any(arg.origin == typing.Unpack for arg in norm.args if arg != Ellipsis):
            raise CannotProvide(
                "Unpacking tuples of dynamic length isn't supported yet",
                is_terminal=True,
                is_demonstrative=True,
            )

        loaders = mediator.mandatory_provide_by_iterable(
            [
                request.append_loc(GenericParamLoc(type=tp.source, generic_pos=i))
                for i, tp in enumerate(norm.args)
            ],
            lambda: "Cannot create loader for tuple. Loaders for some elements cannot be created",
        )
        strict_coercion = mediator.mandatory_provide(StrictCoercionRequest(loc_stack=request.loc_stack))
        debug_trail = mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack))
        return mediator.cached_call(
            self._make_loader,
            loaders=tuple(loaders),
            strict_coercion=strict_coercion,
            debug_trail=debug_trail,
        )

    def _make_loader(self, loaders: Collection[Loader], *, strict_coercion: bool, debug_trail: DebugTrail):
        if debug_trail == DebugTrail.DISABLE:
            if strict_coercion:
                return self._get_dt_disable_sc_loader(loaders)
            return self._get_dt_disable_non_sc_loader(loaders)
        if debug_trail == DebugTrail.FIRST:
            tuple_mapper = self._create_dt_first_loader(loaders)
        elif debug_trail == DebugTrail.ALL:
            tuple_mapper = self._create_dt_all_loader(loaders)
        else:
            raise ValueError
        if strict_coercion:
            return self._get_dt_sc_loader(tuple_mapper)
        return self._get_dt_non_sc_loader(tuple_mapper)

    def _get_dt_non_sc_loader(self, tuple_mapper):
        def dt_non_sc_loader(data):
            try:
                value_tuple = tuple(data)
            except TypeError:
                raise TypeLoadError(tuple, data)
            return tuple(tuple_mapper(value_tuple))

        return dt_non_sc_loader

    def _get_dt_sc_loader(self, tuple_mapper):
        def dt_sc_loader(data):
            if isinstance(data, CollectionsMapping):
                raise ExcludedTypeLoadError(tuple, Mapping, data)
            if type(data) is str:
                raise ExcludedTypeLoadError(tuple, str, data)

            try:
                value_tuple = tuple(data)
            except TypeError:
                raise TypeLoadError(tuple, data)
            return tuple(tuple_mapper(value_tuple))

        return dt_sc_loader

    def _create_dt_all_loader(self, loaders: Collection[Loader]):  # noqa: C901
        loaders_len = len(loaders)

        def dt_all_loader(data):
            try:
                data_len = len(data)
            except TypeError:
                raise TypeLoadError(tuple, data)

            if data_len != loaders_len:
                if data_len > loaders_len:
                    raise ExtraItemsLoadError(loaders_len, data)
                if loaders_len > data_len:
                    raise NoRequiredItemsLoadError(loaders_len, data)

            idx = 0
            errors = []
            has_unexpected_error = False
            for loader, field in zip(loaders, data):
                try:
                    yield loader(field)
                except LoadError as e:
                    errors.append(append_trail(e, idx))
                except Exception as e:
                    errors.append(append_trail(e, idx))
                    has_unexpected_error = True

                idx += 1

            if errors:
                if has_unexpected_error:
                    raise CompatExceptionGroup(
                        "while loading tuple",
                        [render_trail_as_note(e) for e in errors],
                    )
                raise AggregateLoadError(
                    "while loading tuple",
                    [render_trail_as_note(e) for e in errors],
                )

        return dt_all_loader

    def _create_dt_first_loader(self, loaders: Collection[Loader]):
        loaders_len = len(loaders)

        def dt_first_loader(data):
            try:
                data_len = len(data)
            except TypeError:
                raise TypeLoadError(tuple, data)

            if data_len != loaders_len:
                if data_len > loaders_len:
                    raise ExtraItemsLoadError(loaders_len, data)
                if loaders_len > data_len:
                    raise NoRequiredItemsLoadError(loaders_len, data)

            idx = 0
            for loader, field in zip(loaders, data):
                try:
                    yield loader(field)
                except Exception as e:
                    append_trail(e, idx)
                    raise
                idx += 1

        return dt_first_loader

    def _get_dt_disable_non_sc_loader(self, loaders: Collection[Loader]):
        loaders_len = len(loaders)

        def dt_disable_non_sc_loader(data):
            try:
                data_len = len(data)
            except TypeError:
                raise TypeLoadError(tuple, data)

            if data_len != loaders_len:
                if data_len > loaders_len:
                    raise ExtraItemsLoadError(loaders_len, data)
                if loaders_len > data_len:
                    raise NoRequiredItemsLoadError(loaders_len, data)

            return tuple(
                loader(field)
                for loader, field in zip(loaders, data)
            )

        return dt_disable_non_sc_loader

    def _get_dt_disable_sc_loader(self, loaders: Collection[Loader]):
        loaders_len = len(loaders)

        def dt_disable_sc_loader(data):
            if isinstance(data, CollectionsMapping):
                raise ExcludedTypeLoadError(tuple, Mapping, data)
            if type(data) is str:
                raise ExcludedTypeLoadError(tuple, str, data)

            try:
                data_len = len(data)
            except TypeError:
                raise TypeLoadError(tuple, data)

            if data_len != loaders_len:
                if data_len > loaders_len:
                    raise ExtraItemsLoadError(loaders_len, data)
                if loaders_len > data_len:
                    raise NoRequiredItemsLoadError(loaders_len, data)

            return tuple(
                loader(field)
                for loader, field in zip(loaders, data)
            )

        return dt_disable_sc_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        norm = try_normalize_type(request.last_loc.type)
        if len(norm.args) > 1 and norm.args[1] == Ellipsis:
            raise CannotProvide
        if HAS_UNPACK and any(arg.origin == typing.Unpack for arg in norm.args if arg != Ellipsis):
            raise CannotProvide(
                "tuples of dynamic length isn't supported yet",
                is_terminal=True,
                is_demonstrative=True,
            )
        dumpers = mediator.mandatory_provide_by_iterable(
            [
                request.append_loc(GenericParamLoc(type=tp.source, generic_pos=i))
                for i, tp in enumerate(norm.args)
            ],
            lambda: "Cannot create dumper for tuple. Dumpers for some elements cannot be created",
        )
        debug_trail = mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack))
        return mediator.cached_call(
            self._make_dumper,
            dumpers=tuple(dumpers),
            debug_trail=debug_trail,
        )

    def _make_dumper(self, dumpers: Collection[Dumper], debug_trail: DebugTrail):
        if debug_trail == DebugTrail.DISABLE:
            return self._get_dt_disable_dumper(dumpers)
        if debug_trail == DebugTrail.FIRST:
            return self._get_dt_dumper(self._create_dt_first_dumper(dumpers))
        if debug_trail == DebugTrail.ALL:
            return self._get_dt_dumper(self._create_dt_all_dumper(dumpers))
        raise ValueError

    def _create_dt_all_dumper(self, dumpers: Collection[Dumper]):
        dumpers_len = len(dumpers)

        def dt_all_dumper(data):
            try:
                data_len = len(data)
            except TypeError:
                raise TypeLoadError(tuple, data)

            if data_len != dumpers_len:
                if data_len > dumpers_len:
                    raise ExtraItemsLoadError(dumpers_len, data)
                if dumpers_len > data_len:
                    raise NoRequiredItemsLoadError(dumpers_len, data)

            idx = 0
            errors = []
            for dumper, field in zip(dumpers, data):
                try:
                    yield dumper(field)
                except Exception as e:
                    errors.append(append_trail(e, idx))

                idx += 1

            if errors:
                raise CompatExceptionGroup(
                    "while dumping tuple",
                    [render_trail_as_note(e) for e in errors],
                )

        return dt_all_dumper

    def _create_dt_first_dumper(self, dumpers: Collection[Dumper]):
        dumpers_len = len(dumpers)

        def dt_first_dumper(data):
            try:
                data_len = len(data)
            except TypeError:
                raise TypeLoadError(tuple, data)

            if data_len != dumpers_len:
                if data_len > dumpers_len:
                    raise ExtraItemsLoadError(dumpers_len, data)
                if dumpers_len > data_len:
                    raise NoRequiredItemsLoadError(dumpers_len, data)

            idx = 0
            for dumper, field in zip(dumpers, data):
                try:
                    yield dumper(field)
                except Exception as e:
                    append_trail(e, idx)
                    raise
                idx += 1

        return dt_first_dumper

    def _get_dt_dumper(self, tuple_dumper):
        def dt_dumper(data):
            return tuple(tuple_dumper(data))

        return dt_dumper

    def _get_dt_disable_dumper(self, dumpers: Collection[Dumper]):
        dumpers_len = len(dumpers)

        def tuple_dumper(data):
            try:
                data_len = len(data)
            except TypeError:
                raise TypeLoadError(tuple, data)

            if data_len != dumpers_len:
                if data_len > dumpers_len:
                    raise ExtraItemsLoadError(dumpers_len, data)
                if dumpers_len > data_len:
                    raise NoRequiredItemsLoadError(dumpers_len, data)

            return tuple(
                dumper(field)
                for dumper, field in zip(dumpers, data)
            )

        return tuple_dumper
