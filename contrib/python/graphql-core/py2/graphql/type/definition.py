import collections

try:
    from collections.abc import Hashable, Mapping
except ImportError:  # Python < 3.3
    from collections import Hashable, Mapping
import copy

from typing import Union

from ..language import ast
from ..pyutils.cached_property import cached_property
from ..pyutils.ordereddict import OrderedDict
from ..pyutils.compat import Enum as PyEnum
from ..utils.assert_valid_name import assert_valid_name
from ..utils.undefined import Undefined

# Necessary for static type checking
if False:  # flake8: noqa
    from typing import List, Dict, Any, Callable, Optional, Type


def is_type(type_):
    # type: (Any) -> bool
    return isinstance(
        type_,
        (
            GraphQLScalarType,
            GraphQLObjectType,
            GraphQLInterfaceType,
            GraphQLUnionType,
            GraphQLEnumType,
            GraphQLInputObjectType,
            GraphQLList,
            GraphQLNonNull,
        ),
    )


def is_input_type(type_):
    # type: (Any) -> bool
    named_type = get_named_type(type_)
    return isinstance(
        named_type, (GraphQLScalarType, GraphQLEnumType, GraphQLInputObjectType)
    )


def get_nullable_type(type_):
    if isinstance(type_, GraphQLNonNull):
        return type_.of_type
    return type_


def get_named_type(type_):
    # type: (Optional[GraphQLType]) -> Optional[GraphQLType]
    unmodified_type = type_
    while isinstance(unmodified_type, (GraphQLList, GraphQLNonNull)):
        unmodified_type = unmodified_type.of_type

    return unmodified_type


class GraphQLType(object):
    pass


class GraphQLNamedType(GraphQLType):
    __slots__ = ("name",)

    def __init__(self, name):
        # type: (str) -> None
        self.name = name

    def __str__(self):
        # type: () -> str
        return self.name

    def is_same_type(self, other):
        # type: (Any) -> bool
        return self.__class__ is other.__class__ and self.name == other.name


def none_func(x):
    return None


class GraphQLScalarType(GraphQLNamedType):
    """Scalar Type Definition

    The leaf values of any request and input values to arguments are
    Scalars (or Enums) and are defined with a name and a series of coercion
    functions used to ensure validity.

    Example:

        def coerce_odd(value):
            if value % 2 == 1:
                return value
            return None

        OddType = GraphQLScalarType(name='Odd', serialize=coerce_odd)
    """

    __slots__ = "name", "description", "serialize", "parse_value", "parse_literal"

    def __init__(
        self,
        name,  # type: str
        description=None,  # type: Optional[str]
        serialize=None,  # type: Optional[Callable]
        parse_value=None,  # type: Optional[Callable]
        parse_literal=None,  # type: Optional[Callable]
    ):
        # type: (...) -> None
        assert name, "Type must be named."
        assert_valid_name(name)
        super(GraphQLScalarType, self).__init__(name)
        self.name = name
        self.description = description

        assert callable(serialize), (
            '{} must provide "serialize" function. If this custom Scalar is '
            'also used as an input type, ensure "parse_value" and "parse_literal" '
            "functions are also provided."
        ).format(self)

        if parse_value is not None or parse_literal is not None:
            assert callable(parse_value) and callable(
                parse_literal
            ), '{} must provide both "parse_value" and "parse_literal" functions.'.format(
                self
            )

        self.serialize = serialize
        self.parse_value = parse_value or none_func
        self.parse_literal = parse_literal or none_func

    def __str__(self):
        # type: () -> str
        return self.name


class GraphQLObjectType(GraphQLNamedType):
    """Object Type Definition

    Almost all of the GraphQL types you define will be object types.
    Object types have a name, but most importantly describe their fields.

    Example:

        AddressType = GraphQLObjectType('Address', {
            'street': GraphQLField(GraphQLString),
            'number': GraphQLField(GraphQLInt),
            'formatted': GraphQLField(GraphQLString,
                resolver=lambda obj, info, **args: obj.number + ' ' + obj.street),
        })

    When two types need to refer to each other, or a type needs to refer to
    itself in a field, you can use a static method to supply the fields
    lazily.

    Example:

        PersonType = GraphQLObjectType('Person', lambda: {
            'name': GraphQLField(GraphQLString),
            'bestFriend': GraphQLField(PersonType)
        })
    """

    def __init__(
        self,
        name,  # type: str
        fields,  # type: Union[Callable[[], Dict[str, GraphQLField]], Dict[str, GraphQLField]]
        interfaces=None,  # type: Optional[List[GraphQLInterfaceType]]
        is_type_of=None,  # type: Optional[Callable]
        description=None,  # type: Optional[Any]
    ):
        # type: (...) -> None
        assert name, "Type must be named."
        assert_valid_name(name)
        super(GraphQLObjectType, self).__init__(name)
        self.name = name
        self.description = description

        if is_type_of is not None:
            assert callable(
                is_type_of
            ), '{} must provide "is_type_of" as a function.'.format(self)

        self.is_type_of = is_type_of
        self._fields = fields
        self._provided_interfaces = interfaces
        self._interfaces = None

    @cached_property
    def fields(self):
        # type: () -> Dict[str, GraphQLField]
        return define_field_map(self, self._fields)

    @cached_property
    def interfaces(self):
        # type: () -> List[GraphQLInterfaceType]
        return define_interfaces(self, self._provided_interfaces)


def define_field_map(
    type_,  # type: Union[GraphQLInterfaceType, GraphQLObjectType]
    field_map,  # type: Union[Callable, Dict[str, GraphQLField], OrderedDict]
):
    # type: (...) -> OrderedDict
    if callable(field_map):
        field_map = field_map()

    assert isinstance(field_map, Mapping) and len(field_map) > 0, (
        "{} fields must be a mapping (dict / OrderedDict) with field names as keys or a "
        "function which returns such a mapping."
    ).format(type_)

    for field_name, field in field_map.items():
        assert_valid_name(field_name)
        assert isinstance(
            field, GraphQLField
        ), "{}.{} must be an instance of GraphQLField.".format(type_, field_name)
        field_args = getattr(field, "args", None)

        if field_args:
            assert isinstance(
                field_args, Mapping
            ), "{}.{} args must be a mapping (dict / OrderedDict) with argument names as keys.".format(
                type_, field_name
            )

            for arg_name, arg in field_args.items():
                assert_valid_name(arg_name)

    return OrderedDict(field_map)


def define_interfaces(
    type_,  # type: GraphQLObjectType
    interfaces,  # type: Optional[List[GraphQLInterfaceType]]
):
    # type: (...) -> List[GraphQLInterfaceType]
    if callable(interfaces):
        interfaces = interfaces()

    if interfaces is None:
        interfaces = []

    assert isinstance(
        interfaces, (list, tuple)
    ), "{} interfaces must be a list/tuple or a function which returns a list/tuple.".format(
        type_
    )

    for interface in interfaces:
        assert isinstance(
            interface, GraphQLInterfaceType
        ), "{} may only implement Interface types, it cannot implement: {}.".format(
            type_, interface
        )

        if not callable(interface.resolve_type):
            assert callable(type_.is_type_of), (
                'Interface Type {} does not provide a "resolve_type" function '
                'and implementing Type {} does not provide a "is_type_of" '
                "function. There is no way to resolve this implementing type "
                "during execution."
            ).format(interface, type_)

    return interfaces


class GraphQLField(object):
    __slots__ = "type", "args", "resolver", "deprecation_reason", "description"

    def __init__(
        self,
        type_,  # type: Any
        args=None,  # type: Optional[Dict[str, GraphQLArgument]]
        resolver=None,  # type: Optional[Callable]
        deprecation_reason=None,  # type: Optional[Any]
        description=None,  # type: Optional[Any]
    ):
        # type: (...) -> None
        self.type = type_
        self.args = args or OrderedDict()
        self.resolver = resolver
        self.deprecation_reason = deprecation_reason
        self.description = description

    def __eq__(self, other):
        return self is other or (
            isinstance(other, GraphQLField)
            and self.type == other.type
            and self.args == other.args
            and self.resolver == other.resolver
            and self.deprecation_reason == other.deprecation_reason
            and self.description == other.description
        )

    def __hash__(self):
        # type: () -> int
        return id(self)

    @property
    def is_deprecated(self):
        return bool(self.deprecation_reason)


class GraphQLArgument(object):
    __slots__ = "type", "default_value", "description", "out_name"

    def __init__(
        self,
        type_,  # type: Union[GraphQLInputObjectType, GraphQLNonNull, GraphQLList, GraphQLScalarType]
        default_value=None,  # type: Optional[Any]
        description=None,  # type: Optional[Any]
        out_name=None,  # type: Optional[str]
    ):
        # type: (...) -> None
        self.type = type_
        self.default_value = default_value
        self.description = description
        self.out_name = out_name

    def __eq__(self, other):
        return self is other or (
            isinstance(other, GraphQLArgument)
            and self.type == other.type
            and self.default_value == other.default_value
            and self.description == other.description
            and self.out_name == other.out_name
        )

    def __hash__(self):
        return id(self)


class GraphQLInterfaceType(GraphQLNamedType):
    """Interface Type Definition

    When a field can return one of a heterogeneous set of types, a Interface type is used to describe what types
    are possible, what fields are in common across all types, as well as a function to determine which type
    is actually used when the field is resolved.

    Example:

        EntityType = GraphQLInterfaceType(
            name='Entity',
            fields={
                'name': GraphQLField(GraphQLString),
            })
    """

    def __init__(
        self,
        name,  # type: str
        fields=None,  # type: Union[Callable[[], Dict[str, GraphQLField]], Dict[str, GraphQLField]]
        resolve_type=None,  # type: Optional[Callable]
        description=None,  # type: Optional[Any]
    ):
        # type: (...) -> None
        assert name, "Type must be named."
        assert_valid_name(name)
        super(GraphQLInterfaceType, self).__init__(name)
        self.name = name
        self.description = description

        if resolve_type is not None:
            assert callable(
                resolve_type
            ), '{} must provide "resolve_type" as a function.'.format(self)

        self.resolve_type = resolve_type
        self._fields = fields

    @cached_property
    def fields(self):
        # type: () -> Dict[str, GraphQLField]
        assert self._fields is not None, '"fields" cannot be None'
        return define_field_map(self, self._fields)


class GraphQLUnionType(GraphQLNamedType):
    """Union Type Definition

    When a field can return one of a heterogeneous set of types, a Union type is used to describe what types are possible
    as well as providing a function to determine which type is actually used when the field is resolved.

    Example:

        class PetType(GraphQLUnionType):
            name = 'Pet'
            types = [DogType, CatType]

            def resolve_type(self, value):
                if isinstance(value, Dog):
                    return DogType()
                if isinstance(value, Cat):
                    return CatType()
    """

    def __init__(
        self,
        name,  # type: str
        types,  # type: Union[Callable[[], List[GraphQLObjectType]], List[GraphQLObjectType]]
        resolve_type=None,  # type: Optional[Callable]
        description=None,  # type: Optional[Any]
    ):
        # type: (...) -> None
        assert name, "Type must be named."
        assert_valid_name(name)
        super(GraphQLUnionType, self).__init__(name)
        self.name = name
        self.description = description

        if resolve_type is not None:
            assert callable(
                resolve_type
            ), '{} must provide "resolve_type" as a function.'.format(self)

        self.resolve_type = resolve_type
        self._types = types

    @cached_property
    def types(self):
        # type: () -> List[GraphQLObjectType]
        return define_types(self, self._types)


# fmt: off
def define_types(
    union_type,  # type: GraphQLUnionType
    types,  # type: Union[Callable[[], List[GraphQLObjectType]], List[GraphQLObjectType]]
):
    # type: (...) -> List[GraphQLObjectType]
    # fmt: on
    if callable(types):
        types = types()

    assert (
        isinstance(types, (list, tuple)) and len(types) > 0
    ), "Must provide types for Union {}.".format(union_type.name)
    has_resolve_type_fn = callable(union_type.resolve_type)

    for type_ in types:
        assert isinstance(
            type_, GraphQLObjectType
        ), "{} may only contain Object types, it cannot contain: {}.".format(
            union_type, type_
        )

        if not has_resolve_type_fn:
            assert callable(type_.is_type_of), (
                'Union Type {} does not provide a "resolve_type" function '
                'and possible Type {} does not provide a "is_type_of" '
                "function. There is no way to resolve this possible type "
                "during execution."
            ).format(union_type, type_)

    return types


class GraphQLEnumType(GraphQLNamedType):
    """Enum Type Definition

    Some leaf values of requests and input values are Enums. GraphQL serializes Enum values as strings,
    however internally Enums can be represented by any kind of type, often integers.

    Example:

        RGBType = GraphQLEnumType(
            name='RGB',
            values=OrderedDict([
                ('RED', GraphQLEnumValue(0)),
                ('GREEN', GraphQLEnumValue(1)),
                ('BLUE', GraphQLEnumValue(2))
            ])
        )

    Note: If a value is not provided in a definition, the name of the enum value will be used as it's internal value.
    """

    def __init__(self, name, values, description=None):
        assert name, "Type must provide name."
        assert_valid_name(name)
        super(GraphQLEnumType, self).__init__(name)
        self.name = name
        self.description = description

        self.values = define_enum_values(self, values)

    def get_values(self):
        return self.values

    def get_value(self, name):
        return self._name_lookup.get(name)

    def serialize(self, value):
        # type: (Union[str, PyEnum]) -> Optional[str]
        if isinstance(value, PyEnum):
            # We handle PyEnum values
            value = value.value
        if isinstance(value, Hashable):
            enum_value = self._value_lookup.get(value)
            if enum_value:
                return enum_value.name

        return None

    def parse_value(self, value):
        if isinstance(value, Hashable):
            enum_value = self._name_lookup.get(value)

            if enum_value:
                return enum_value.value

        return None

    def parse_literal(self, value_ast):
        if isinstance(value_ast, ast.EnumValue):
            enum_value = self._name_lookup.get(value_ast.value)

            if enum_value:
                return enum_value.value

    @cached_property
    def _value_lookup(self):
        # type: () -> Dict[str, GraphQLEnumValue]
        return {
            value.value.value if isinstance(value.value, PyEnum) else value.value: value
            for value in self.values
        }

    @cached_property
    def _name_lookup(self):
        return {value.name: value for value in self.values}


def define_enum_values(type, value_map):
    assert (
        isinstance(value_map, Mapping) and len(value_map) > 0
    ), "{} values must be a mapping (dict / OrderedDict) with value names as keys.".format(
        type
    )

    values = []
    if not isinstance(value_map, (collections.OrderedDict, OrderedDict)):
        value_map = OrderedDict(sorted(list(value_map.items())))

    for value_name, value in value_map.items():
        assert_valid_name(value_name)
        assert isinstance(
            value, GraphQLEnumValue
        ), "{}.{} must be an instance of GraphQLEnumValue, but got: {}".format(
            type, value_name, value
        )
        value = copy.copy(value)
        value.name = value_name
        if value.value == Undefined:
            value.value = value_name

        values.append(value)

    return values


class GraphQLEnumValue(object):
    __slots__ = "name", "value", "deprecation_reason", "description"

    def __init__(
        self, value=Undefined, deprecation_reason=None, description=None, name=None
    ):
        self.name = name
        self.value = value
        self.deprecation_reason = deprecation_reason
        self.description = description

    @property
    def is_deprecated(self):
        return bool(self.deprecation_reason)

    def __eq__(self, other):
        return self is other or (
            isinstance(other, GraphQLEnumValue)
            and self.name == other.name
            and self.value == other.value
            and self.deprecation_reason == other.deprecation_reason
            and self.description == other.description
        )


class GraphQLInputObjectType(GraphQLNamedType):
    """Input Object Type Definition

    An input object defines a structured collection of fields which may be
    supplied to a field argument.

    Using `NonNull` will ensure that a value must be provided by the query

    Example:

        NonNullFloat = GraphQLNonNull(GraphQLFloat())

        class GeoPoint(GraphQLInputObjectType):
            name = 'GeoPoint'
            fields = {
                'lat': GraphQLInputObjectField(NonNullFloat),
                'lon': GraphQLInputObjectField(NonNullFloat),
                'alt': GraphQLInputObjectField(GraphQLFloat(),
                    default_value=0)
            }
    """

    def __init__(
        self,
        name,  # type: str
        fields,  # type: Union[Callable[[], Dict[str, GraphQLInputObjectField]], Dict[str, GraphQLInputObjectField]]
        description=None,  # type: Optional[str]
        container_type=None,  # type: Type[Dict[str, Any]]
    ):
        # type: (...) -> None
        assert name, "Type must be named."
        self.name = name
        self.description = description
        super(GraphQLInputObjectType, self).__init__(name)
        if container_type is None:
            container_type = OrderedDict  # type: ignore
        assert callable(container_type), "container_type must be callable"
        self.container_type = container_type
        self._fields = fields

    def create_container(self, data):
        # type: (Dict[str, Any]) -> Dict[str, Any]
        return self.container_type(data)

    @cached_property
    def fields(self):
        # type: () -> Dict[str, GraphQLInputObjectField]
        return self._define_field_map()

    def _define_field_map(self):
        # type: () -> OrderedDict
        if callable(self._fields):
            fields = self._fields()
        else:
            fields = self._fields

        assert isinstance(fields, Mapping) and len(fields) > 0, (
            "{} fields must be a mapping (dict / OrderedDict) with field names as keys or a "
            "function which returns such a mapping."
        ).format(self)
        if not isinstance(fields, (collections.OrderedDict, OrderedDict)):
            fields = OrderedDict(sorted(list(fields.items())))

        for field_name, field in fields.items():
            assert_valid_name(field_name)

        return fields


class GraphQLInputObjectField(object):
    __slots__ = "type", "default_value", "description", "out_name"

    def __init__(
        self,
        type_,  # type: Union[GraphQLInputObjectType, GraphQLScalarType]
        default_value=None,  # type: Optional[Any]
        description=None,  # type: Optional[Any]
        out_name=None,  # type: str
    ):
        # type: (...) -> None
        self.type = type_  # type: Union[GraphQLInputObjectType, GraphQLScalarType]
        self.default_value = default_value
        self.description = description
        self.out_name = out_name

    def __eq__(self, other):
        return self is other or (
            isinstance(other, GraphQLInputObjectField)
            and self.type == other.type
            and self.description == other.description
            and self.out_name == other.out_name
        )


class GraphQLList(GraphQLType):
    """List Modifier

    A list is a kind of type marker, a wrapping type which points to another
    type. Lists are often created within the context of defining the fields
    of an object type.

    Example:

        class PersonType(GraphQLObjectType):
            name = 'Person'

            def get_fields(self):
                return {
                    'parents': GraphQLField(GraphQLList(PersonType())),
                    'children': GraphQLField(GraphQLList(PersonType())),
                }
    """

    __slots__ = ("of_type",)

    def __init__(self, type_):
        # type: (Any) -> None
        assert is_type(
            type_
        ), "Can only create List of a GraphQLType but got: {}.".format(type_)
        self.of_type = type_

    def __str__(self):
        # type: () -> str
        return "[" + str(self.of_type) + "]"

    def is_same_type(self, other):
        return isinstance(other, GraphQLList) and self.of_type.is_same_type(
            other.of_type
        )


# These types can all accept null as a value.
graphql_nullable_types = (
    GraphQLScalarType,
    GraphQLObjectType,
    GraphQLInterfaceType,
    GraphQLUnionType,
    GraphQLEnumType,
    GraphQLInputObjectType,
    GraphQLList,
)

GraphQLNullableType = Union[
    GraphQLScalarType,
    GraphQLObjectType,
    GraphQLInterfaceType,
    GraphQLUnionType,
    GraphQLEnumType,
    GraphQLInputObjectType,
    GraphQLList,
]


def is_nullable_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, graphql_nullable_types)


class GraphQLNonNull(GraphQLType):
    """Non-Null Modifier

    A non-null is a kind of type marker, a wrapping type which points to another type. Non-null types enforce
    that their values are never null and can ensure an error is raised if this ever occurs during a request.
    It is useful for fields which you can make a strong guarantee on non-nullability,
    for example usually the id field of a database row will never be null.

    Example:

        class RowType(GraphQLObjectType):
            name = 'Row'
            fields = {
                'id': GraphQLField(type=GraphQLNonNull(GraphQLString()))
            }

    Note: the enforcement of non-nullability occurs within the executor.
    """

    __slots__ = ("of_type",)

    def __init__(
        self,
        type_,  # type: GraphQLNullableType
    ):
        # type: (...) -> None
        assert is_type(type_) and not isinstance(
            type_, GraphQLNonNull
        ), "Can only create NonNull of a Nullable GraphQLType but got: {}.".format(type_)
        self.of_type = type_  # type: GraphQLNullableType

    def __str__(self):
        # type: () -> str
        return str(self.of_type) + "!"

    def is_same_type(self, other):
        return isinstance(other, GraphQLNonNull) and self.of_type.is_same_type(
            other.of_type
        )


# These types may be used as output types as the result of fields.
graphql_output_types = (
    GraphQLScalarType,
    GraphQLObjectType,
    GraphQLInterfaceType,
    GraphQLUnionType,
    GraphQLEnumType,
)

GraphQLOutputType = Union[
    GraphQLScalarType,
    GraphQLObjectType,
    GraphQLInterfaceType,
    GraphQLUnionType,
    GraphQLEnumType,
    GraphQLNullableType
]


def is_output_type(type_):
    # type: (Any) -> bool
    named_type = get_named_type(type_)
    return isinstance(named_type, graphql_output_types)


def is_union_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, GraphQLUnionType)


def is_interface_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, GraphQLInterfaceType)


def is_list_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, GraphQLList)


def is_non_null_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, GraphQLNonNull)


def is_object_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, GraphQLObjectType)


# These types may describe types which may be leaf values.
graphql_leaf_types = (GraphQLScalarType, GraphQLEnumType)

GraphQLLeafType = Union[GraphQLScalarType, GraphQLEnumType]


def is_leaf_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, (GraphQLScalarType, GraphQLEnumType))


# These types may describe the parent context of a selection set.
graphql_composite_types = (GraphQLObjectType, GraphQLInterfaceType, GraphQLUnionType)

GraphQLCompositeType = Union[GraphQLObjectType, GraphQLInterfaceType, GraphQLUnionType]


def is_composite_type(type_):
    # type: (Any) -> bool
    named_type = get_named_type(type_)
    return isinstance(named_type, graphql_composite_types)


# These types may describe abstract types.
graphql_abstract_types = (GraphQLInterfaceType, GraphQLUnionType)

GraphQLAbstractType = Union[GraphQLInterfaceType, GraphQLUnionType]


def is_abstract_type(type_):
    # type: (Any) -> bool
    return isinstance(type_, graphql_abstract_types)
