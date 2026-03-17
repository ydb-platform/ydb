from typing import TypeAlias, List, Mapping, TypedDict, Callable, NotRequired

JsonType: TypeAlias = (
    List["JsonValueType"] | Mapping[str, "JsonValueType"] | "JsonValueType"
)
JsonValueType: TypeAlias = str | int | float | None | JsonType

JsonPath = List[str | int]

# TODO: improve this type definition to be a list with a string followed by zero or multiple JsonType's
JsonQueryType: TypeAlias = list[str | JsonType] | JsonType
JsonQueryFunctionType: TypeAlias = list[str | JsonType]
JsonQueryObjectType: TypeAlias = Mapping[str, JsonQueryType]


OperatorGroup: TypeAlias = Mapping[str, str]


class CustomOperatorAt(TypedDict):
    name: str
    op: str
    at: str
    vararg: NotRequired[bool]
    left_associative: NotRequired[bool]


class CustomOperatorBefore(TypedDict):
    name: str
    op: str
    before: str
    vararg: NotRequired[bool]
    left_associative: NotRequired[bool]


class CustomOperatorAfter(TypedDict):
    name: str
    op: str
    after: str
    vararg: NotRequired[bool]
    left_associative: NotRequired[bool]


CustomOperator: TypeAlias = (
    CustomOperatorAt | CustomOperatorBefore | CustomOperatorAfter
)


class JsonQueryOptions(TypedDict):
    functions: NotRequired[Mapping[str, Callable]]
    operators: NotRequired[list[CustomOperator]]


class JsonQueryStringifyOptions(TypedDict):
    operators: NotRequired[list[CustomOperator]]
    max_line_length: NotRequired[int]
    indentation: NotRequired[str]


class JsonQueryParseOptions(TypedDict):
    operators: NotRequired[list[CustomOperator]]
