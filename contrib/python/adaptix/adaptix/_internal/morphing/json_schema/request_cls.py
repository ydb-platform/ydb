from dataclasses import dataclass

from ...definitions import Direction
from ...provider.located_request import LocatedRequest
from .definitions import JSONSchema, RefSource


@dataclass(frozen=True)
class JSONSchemaContext:
    dialect: str
    direction: Direction


@dataclass(frozen=True)
class WithJSONSchemaContext:
    ctx: JSONSchemaContext


@dataclass(frozen=True)
class JSONSchemaRequest(LocatedRequest[JSONSchema], WithJSONSchemaContext):
    pass


@dataclass(frozen=True)
class RefSourceRequest(LocatedRequest[RefSource], WithJSONSchemaContext):
    json_schema: JSONSchema


@dataclass(frozen=True)
class InlineJSONSchemaRequest(LocatedRequest[bool], WithJSONSchemaContext):
    pass
