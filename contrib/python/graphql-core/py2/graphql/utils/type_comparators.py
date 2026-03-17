from ..type.definition import (
    GraphQLAbstractType,
    GraphQLInterfaceType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLType,
    GraphQLUnionType,
    is_abstract_type,
    is_interface_type,
    is_list_type,
    is_non_null_type,
    is_object_type,
)

from typing import cast

# Necessary for static type checking
if False:  # flake8: noqa
    from ..type.schema import GraphQLSchema
    from typing import Union


def is_equal_type(type_a, type_b):
    if type_a is type_b:
        return True

    if isinstance(type_a, GraphQLNonNull) and isinstance(type_b, GraphQLNonNull):
        return is_equal_type(type_a.of_type, type_b.of_type)

    if isinstance(type_a, GraphQLList) and isinstance(type_b, GraphQLList):
        return is_equal_type(type_a.of_type, type_b.of_type)

    return False


def is_type_sub_type_of(
    schema,  # type: GraphQLSchema
    maybe_subtype,  # type: GraphQLType
    super_type,  # type: GraphQLType
):
    # type: (...) -> bool
    """Check whether a type is subtype of another type in a given schema.
    Provided a type and a super type, return true if the first type is either equal or
    a subset of the second super type (covariant).
    """
    # Equivalent type is a valid subtype
    if maybe_subtype is super_type:
        return True

    # If super_type is non-null, maybe_subtype must also be non-null.
    if is_non_null_type(super_type):
        if is_non_null_type(maybe_subtype):
            return is_type_sub_type_of(
                schema,
                cast(GraphQLNonNull, maybe_subtype).of_type,
                cast(GraphQLNonNull, super_type).of_type,
            )
        return False
    elif is_non_null_type(maybe_subtype):
        # If super_type is nullable, maybe_subtype may be non-null or nullable.
        return is_type_sub_type_of(
            schema, cast(GraphQLNonNull, maybe_subtype).of_type, super_type
        )

    # If super_type type is a list, maybeSubType type must also be a list.
    if is_list_type(super_type):
        if is_list_type(maybe_subtype):
            return is_type_sub_type_of(
                schema,
                cast(GraphQLList, maybe_subtype).of_type,
                cast(GraphQLList, super_type).of_type,
            )
        return False
    elif is_list_type(maybe_subtype):
        # If super_type is not a list, maybe_subtype must also be not a list.
        return False

    # If super_type type is abstract, check if it is super type of maybe_subtype.
    # Otherwise, the child type is not a valid subtype of the parent type.
    return (
        is_abstract_type(super_type)
        and (is_interface_type(maybe_subtype) or is_object_type(maybe_subtype))
        and schema.is_possible_type(
            cast(GraphQLAbstractType, super_type),
            cast(GraphQLObjectType, maybe_subtype),
        )
    )


def do_types_overlap(
    schema,  # type: GraphQLSchema
    t1,  # type: Union[GraphQLInterfaceType, GraphQLUnionType]
    t2,  # type: Union[GraphQLInterfaceType, GraphQLUnionType]
):
    # type: (...) -> bool
    # print 'do_types_overlap', t1, t2
    if t1 == t2:
        # print '1'
        return True

    if isinstance(t1, (GraphQLInterfaceType, GraphQLUnionType)):
        if isinstance(t2, (GraphQLInterfaceType, GraphQLUnionType)):
            # If both types are abstract, then determine if there is any intersection
            # between possible concrete types of each.
            s = any(
                [
                    schema.is_possible_type(t2, type)
                    for type in schema.get_possible_types(t1)
                ]
            )
            # print '2',s
            return s
        # Determine if the latter type is a possible concrete type of the former.
        r = schema.is_possible_type(t1, t2)
        # print '3', r
        return r

    if isinstance(t2, (GraphQLInterfaceType, GraphQLUnionType)):
        t = schema.is_possible_type(t2, t1)
        # print '4', t
        return t

    # print '5'
    return False
