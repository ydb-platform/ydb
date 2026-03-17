from collections.abc import Iterable, Mapping
from typing import Any, Optional, TypeVar, overload

from ...common import TypeHint
from ...definitions import Direction
from ..json_schema.definitions import ResolvedJSONSchema
from ..json_schema.mangling import CompoundRefMangler, IndexRefMangler, QualnameRefMangler
from ..json_schema.ref_generator import BuiltinRefGenerator
from ..json_schema.request_cls import JSONSchemaContext
from ..json_schema.resolver import BuiltinJSONSchemaResolver, JSONSchemaResolver
from ..json_schema.schema_model import JSONSchemaDialect
from .retort import AdornedRetort, Retort

_global_retort = Retort()
T = TypeVar("T")


@overload
def load(data: Any, tp: type[T], /) -> T:
    ...


@overload
def load(data: Any, tp: TypeHint, /) -> Any:
    ...


def load(data: Any, tp: TypeHint, /):
    return _global_retort.load(data, tp)


@overload
def dump(data: T, tp: type[T], /) -> Any:
    ...


@overload
def dump(data: Any, tp: Optional[TypeHint] = None, /) -> Any:
    ...


def dump(data: Any, tp: Optional[TypeHint] = None, /) -> Any:
    return _global_retort.dump(data, tp)


_global_resolver = BuiltinJSONSchemaResolver(
    ref_generator=BuiltinRefGenerator(),
    ref_mangler=CompoundRefMangler(QualnameRefMangler(), IndexRefMangler()),
)


DumpedJSONSchema = Mapping[str, Any]


def generate_json_schemas(
    retort: AdornedRetort,
    tps: Iterable[TypeHint],
    *,
    direction: Direction,
    resolver: JSONSchemaResolver = _global_resolver,
    dialect: str = JSONSchemaDialect.DRAFT_2020_12,
) -> tuple[DumpedJSONSchema, Iterable[DumpedJSONSchema]]:
    ctx = JSONSchemaContext(dialect=dialect, direction=direction)
    defs, schemas = resolver.resolve((), [retort.make_json_schema(tp, ctx) for tp in tps])
    dumped_defs = _global_retort.dump(defs, dict[str, ResolvedJSONSchema])
    dumped_schemas = _global_retort.dump(schemas, Iterable[ResolvedJSONSchema])
    return dumped_defs, dumped_schemas


def generate_json_schema(
    retort: AdornedRetort,
    tp: TypeHint,
    *,
    direction: Direction,
    resolver: JSONSchemaResolver = _global_resolver,
    dialect: str = JSONSchemaDialect.DRAFT_2020_12,
) -> Mapping[str, Any]:
    defs, [schema] = generate_json_schemas(
        retort,
        [tp],
        direction=direction,
        resolver=resolver,
        dialect=dialect,
    )
    return {**schema, "$defs": defs}
