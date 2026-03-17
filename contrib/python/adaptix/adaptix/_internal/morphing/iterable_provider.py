# ruff: noqa: SIM113
import collections.abc
from collections.abc import Iterable, Mapping
from typing import Callable, TypeVar

from ..common import Dumper, Loader, TypeHint
from ..compat import CompatExceptionGroup
from ..definitions import DebugTrail
from ..morphing.provider_template import MorphingProvider
from ..provider.essential import Mediator
from ..provider.location import GenericParamLoc
from ..struct_trail import append_trail, render_trail_as_note
from .json_schema.definitions import JSONSchema
from .json_schema.request_cls import JSONSchemaRequest
from .json_schema.schema_model import JSONSchemaType
from .load_error import AggregateLoadError, ExcludedTypeLoadError, LoadError, TypeLoadError
from .request_cls import DebugTrailRequest, DumperRequest, LoaderRequest, StrictCoercionRequest
from .utils import try_normalize_type

CollectionsMapping = collections.abc.Mapping

T = TypeVar("T")


class IterableProvider(MorphingProvider):
    def __init__(self, *, dump_as: Callable[[Iterable[T]], Iterable[T]], json_schema_unique_items: bool = False):
        self._dump_as = dump_as
        self._json_schema_unique_items = json_schema_unique_items

    def _parse_origin_and_arg(self, tp: TypeHint) -> tuple[TypeHint, TypeHint]:
        norm = try_normalize_type(tp)
        return norm.origin, norm.args[0].source

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        origin, arg = self._parse_origin_and_arg(request.last_loc.type)
        arg_loader = mediator.mandatory_provide(
            request.append_loc(GenericParamLoc(type=arg, generic_pos=0)),
            lambda x: "Cannot create loader for iterable. Loader for element cannot be created",
        )
        strict_coercion = mediator.mandatory_provide(StrictCoercionRequest(loc_stack=request.loc_stack))
        debug_trail = mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack))
        return mediator.cached_call(
            self._make_loader,
            origin=origin,
            iter_factory=origin,
            arg_loader=arg_loader,
            strict_coercion=strict_coercion,
            debug_trail=debug_trail,
        )

    def _create_dt_first_iter_loader(self, origin, loader):
        def iter_loader_dt_first(iterable):
            idx = 0

            for el in iterable:
                try:
                    yield loader(el)
                except Exception as e:
                    append_trail(e, idx)
                    raise

                idx += 1

        return iter_loader_dt_first

    def _create_dt_all_iter_loader(self, origin, loader):
        def iter_loader_dt_all(iterable):
            idx = 0
            errors = []
            has_unexpected_error = False

            for el in iterable:
                try:
                    yield loader(el)
                except LoadError as e:
                    errors.append(append_trail(e, idx))
                except Exception as e:
                    errors.append(append_trail(e, idx))
                    has_unexpected_error = True

                idx += 1

            if errors:
                if has_unexpected_error:
                    raise CompatExceptionGroup(
                        f"while loading iterable {origin}",
                        [render_trail_as_note(e) for e in errors],
                    )
                raise AggregateLoadError(
                    f"while loading iterable {origin}",
                    [render_trail_as_note(e) for e in errors],
                )

        return iter_loader_dt_all

    def _make_loader(self, *, origin, iter_factory, arg_loader, strict_coercion: bool, debug_trail: DebugTrail):
        if debug_trail == DebugTrail.DISABLE:
            if strict_coercion:
                return self._get_dt_disable_sc_loader(iter_factory, arg_loader)
            return self._get_dt_disable_non_sc_loader(iter_factory, arg_loader)

        if debug_trail == DebugTrail.FIRST:
            iter_mapper = self._create_dt_first_iter_loader(origin, arg_loader)
        elif debug_trail == DebugTrail.ALL:
            iter_mapper = self._create_dt_all_iter_loader(origin, arg_loader)
        else:
            raise ValueError

        if strict_coercion:
            return self._get_dt_sc_loader(iter_factory, iter_mapper)
        return self._get_dt_non_sc_loader(iter_factory, iter_mapper)

    def _get_dt_non_sc_loader(self, iter_factory, iter_mapper):
        def iter_loader_dt(data):
            try:
                value_iter = iter(data)
            except TypeError:
                raise TypeLoadError(Iterable, data)

            return iter_factory(iter_mapper(value_iter))

        return iter_loader_dt

    def _get_dt_sc_loader(self, iter_factory, iter_mapper):
        def iter_loader_dt_sc(data):
            if isinstance(data, CollectionsMapping):
                raise ExcludedTypeLoadError(Iterable, Mapping, data)
            if type(data) is str:
                raise ExcludedTypeLoadError(Iterable, str, data)

            try:
                value_iter = iter(data)
            except TypeError:
                raise TypeLoadError(Iterable, data)

            return iter_factory(iter_mapper(value_iter))

        return iter_loader_dt_sc

    def _get_dt_disable_sc_loader(self, iter_factory, arg_loader):
        def iter_loader_sc(data):
            if isinstance(data, CollectionsMapping):
                raise ExcludedTypeLoadError(Iterable, Mapping, data)
            if type(data) is str:
                raise ExcludedTypeLoadError(Iterable, str, data)

            try:
                map_iter = map(arg_loader, data)
            except TypeError:
                raise TypeLoadError(Iterable, data)

            return iter_factory(map_iter)

        return iter_loader_sc

    def _get_dt_disable_non_sc_loader(self, iter_factory, arg_loader):
        def iter_loader(data):
            try:
                map_iter = map(arg_loader, data)
            except TypeError:
                raise TypeLoadError(Iterable, data)

            return iter_factory(map_iter)

        return iter_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        origin, arg = self._parse_origin_and_arg(request.last_loc.type)
        arg_dumper = mediator.mandatory_provide(
            request.append_loc(GenericParamLoc(type=arg, generic_pos=0)),
            lambda x: "Cannot create dumper for iterable. Dumper for element cannot be created",
        )
        debug_trail = mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack))
        return mediator.cached_call(
            self._make_dumper,
            origin=origin,
            iter_factory=self._dump_as,
            arg_dumper=arg_dumper,
            debug_trail=debug_trail,
        )

    def _make_dumper(self, *, origin, iter_factory, arg_dumper, debug_trail: DebugTrail):
        if debug_trail == DebugTrail.DISABLE:
            return self._get_dt_disable_dumper(iter_factory, arg_dumper)
        if debug_trail == DebugTrail.FIRST:
            return self._get_dt_dumper(iter_factory, self._create_dt_first_iter_dumper(origin, arg_dumper))
        if debug_trail == DebugTrail.ALL:
            return self._get_dt_dumper(iter_factory, self._create_dt_all_iter_dumper(origin, arg_dumper))
        raise ValueError

    def _create_dt_first_iter_dumper(self, origin, dumper):
        def iter_dumper_dt_first(iterable):
            idx = 0

            for el in iterable:
                try:
                    yield dumper(el)
                except Exception as e:
                    append_trail(e, idx)
                    raise

                idx += 1

        return iter_dumper_dt_first

    def _create_dt_all_iter_dumper(self, origin, dumper):
        def iter_dumper_dt_all(iterable):
            idx = 0
            errors = []

            for el in iterable:
                try:
                    yield dumper(el)
                except Exception as e:
                    errors.append(append_trail(e, idx))

                idx += 1

            if errors:
                raise CompatExceptionGroup(
                    f"while dumping iterable {origin}",
                    [render_trail_as_note(e) for e in errors],
                )

        return iter_dumper_dt_all

    def _get_dt_dumper(self, iter_factory, iter_dumper):
        def iter_dt_dumper(data):
            return iter_factory(iter_dumper(data))

        return iter_dt_dumper

    def _get_dt_disable_dumper(self, iter_factory, arg_dumper: Dumper):
        def iter_dumper(data):
            return iter_factory(map(arg_dumper, data))

        return iter_dumper

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        origin, arg = self._parse_origin_and_arg(request.last_loc.type)
        item_schema = mediator.mandatory_provide(
            request.append_loc(
                GenericParamLoc(
                    type=arg,
                    generic_pos=0,
                ),
            ),
            lambda x: "Cannot create JSONSchema for iterable. JSONSchema for element cannot be created",
        )
        if self._json_schema_unique_items:
            return JSONSchema(type=JSONSchemaType.ARRAY, items=item_schema, unique_items=True)
        return JSONSchema(type=JSONSchemaType.ARRAY, items=item_schema)
