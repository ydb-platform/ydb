from dataclasses import dataclass, field
from typing import Generic, Optional, TypeVar, Union

from ...provider.loc_stack_filtering import LocStack
from ...type_tools.fwd_ref import FwdRef
from .schema_model import BaseJSONSchema

T = TypeVar("T")

JSONSchemaT = TypeVar("JSONSchemaT")


@dataclass(frozen=True)
class RefSource(Generic[JSONSchemaT]):
    value: Optional[str]
    json_schema: JSONSchemaT = field(hash=False)
    loc_stack: LocStack = field(repr=False)


Boolable = Union[T, bool]


@dataclass(repr=False)
class JSONSchema(BaseJSONSchema[RefSource[FwdRef["JSONSchema"]], Boolable[FwdRef["JSONSchema"]]]):
    pass


@dataclass(repr=False)
class ResolvedJSONSchema(BaseJSONSchema[str, Boolable[FwdRef["ResolvedJSONSchema"]]]):
    pass

