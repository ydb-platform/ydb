from collections import OrderedDict, defaultdict
from functools import reduce

from typing import cast

from ..utils.type_comparators import is_equal_type, is_type_sub_type_of
from .definition import (
    GraphQLArgument,
    GraphQLInputObjectField,
    GraphQLInputObjectType,
    GraphQLInterfaceType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLUnionType,
    is_input_type,
    is_output_type,
)

# Necessary for static type checking
if False:  # flake8: noqa
    from ..type.definition import GraphQLNamedType
    from ..type.schema import GraphQLSchema
    from typing import List, Union, Dict, Set, DefaultDict


class GraphQLTypeMap(OrderedDict):
    def __init__(self, types):
        # type: (List[GraphQLNamedType]) -> None
        super(GraphQLTypeMap, self).__init__()
        self.update(reduce(self.reducer, types, OrderedDict()))  # type: ignore
        self._possible_type_map = defaultdict(set)  # type: DefaultDict[str, Set[str]]

        # Keep track of all implementations by interface name.
        self._implementations = defaultdict(
            list
        )  # type: DefaultDict[str, List[GraphQLObjectType]]
        for gql_type in self.values():
            if isinstance(gql_type, GraphQLObjectType):
                for interface in gql_type.interfaces:
                    self._implementations[interface.name].append(gql_type)

        # Enforce correct interface implementations.
        for type_ in self.values():
            if isinstance(type_, GraphQLObjectType):
                for interface in type_.interfaces:
                    self.assert_object_implements_interface(self, type_, interface)

    def get_possible_types(self, abstract_type):
        # type: (Union[GraphQLInterfaceType, GraphQLUnionType]) -> List[GraphQLObjectType]
        if isinstance(abstract_type, GraphQLUnionType):
            return abstract_type.types
        assert isinstance(abstract_type, GraphQLInterfaceType)
        if abstract_type.name not in self._implementations:
            return []
        return self._implementations[abstract_type.name]

    def is_possible_type(
        self,
        abstract_type,  # type: Union[GraphQLInterfaceType, GraphQLUnionType]
        possible_type,  # type: GraphQLObjectType
    ):
        # type: (...) -> bool
        possible_types = self.get_possible_types(abstract_type)
        assert possible_types, (
            "Could not find possible implementing types for {} in "
            + "schema. Check that schema.types is defined and is an array of"
            + "all possible types in the schema."
        ).format(abstract_type)

        if abstract_type.name not in self._possible_type_map:
            self._possible_type_map[abstract_type.name].update(
                [p.name for p in possible_types]
            )

        return possible_type.name in self._possible_type_map[abstract_type.name]

    @classmethod
    def reducer(cls, map_, type_):
        # type: (Dict, Union[GraphQLNamedType, GraphQLList, GraphQLNonNull]) -> Dict
        if not type_:
            return map_

        if isinstance(type_, (GraphQLList, GraphQLNonNull)):
            return cls.reducer(map_, type_.of_type)

        if type_.name in map_:
            assert map_[type_.name] == type_, (
                'Schema must contain unique named types but contains multiple types named "{}".'
            ).format(type_.name)

            return map_

        map_[type_.name] = type_  # type: ignore

        reduced_map = map_

        if isinstance(type_, GraphQLUnionType):
            for t in type_.types:
                reduced_map = cls.reducer(reduced_map, t)

        if isinstance(type_, GraphQLObjectType):
            for t in type_.interfaces:
                reduced_map = cls.reducer(reduced_map, t)

        if isinstance(
            type_, (GraphQLObjectType, GraphQLInterfaceType, GraphQLInputObjectType)
        ):
            field_map = type_.fields
            type_is_input = isinstance(type_, GraphQLInputObjectType)
            for field_name, field in field_map.items():
                if type_is_input:
                    assert isinstance(
                        field, GraphQLInputObjectField
                    ), "{}.{} must be an instance of GraphQLInputObjectField.".format(
                        type_, field_name
                    )
                    assert is_input_type(
                        field.type
                    ), "{}.{} field type must be Input Type but got: {}.".format(
                        type_, field_name, field.type
                    )
                else:
                    assert is_output_type(
                        field.type
                    ), "{}.{} field type must be Output Type but got: {}.".format(
                        type_, field_name, field.type
                    )
                    for arg_name, arg in field.args.items():
                        assert isinstance(
                            arg, (GraphQLArgument, GraphQLArgument)
                        ), "{}.{}({}:) argument must be an instance of GraphQLArgument.".format(
                            type_, field_name, arg_name
                        )
                        assert is_input_type(
                            arg.type
                        ), "{}.{}({}:) argument type must be Input Type but got: {}.".format(
                            type_, field_name, arg_name, arg.type
                        )
                        reduced_map = cls.reducer(reduced_map, arg.type)

                reduced_map = cls.reducer(reduced_map, getattr(field, "type", None))

        return reduced_map

    @classmethod
    def assert_object_implements_interface(
        cls,
        schema,  # type: Union[GraphQLTypeMap, GraphQLSchema]
        object,  # type: GraphQLObjectType
        interface,  # type: GraphQLInterfaceType
    ):
        # type: (...) -> None
        object_field_map = object.fields
        interface_field_map = interface.fields

        for field_name, interface_field in interface_field_map.items():
            object_field = object_field_map.get(field_name)

            assert (
                object_field
            ), '"{}" expects field "{}" but "{}" does not provide it.'.format(
                interface, field_name, object
            )

            assert is_type_sub_type_of(
                cast("GraphQLSchema", schema), object_field.type, interface_field.type
            ), '{}.{} expects type "{}" but {}.{} provides type "{}".'.format(
                interface,
                field_name,
                interface_field.type,
                object,
                field_name,
                object_field.type,
            )

            for arg_name, interface_arg in interface_field.args.items():
                object_arg = object_field.args.get(arg_name)

                assert object_arg, (
                    '{}.{} expects argument "{}" but {}.{} does not provide it.'
                ).format(interface, field_name, arg_name, object, field_name)

                assert is_equal_type(interface_arg.type, object_arg.type), (
                    '{}.{}({}:) expects type "{}" but {}.{}({}:) provides type "{}".'
                ).format(
                    interface,
                    field_name,
                    arg_name,
                    interface_arg.type,
                    object,
                    field_name,
                    arg_name,
                    object_arg.type,
                )

            for arg_name, object_arg in object_field.args.items():
                interface_arg = interface_field.args.get(arg_name)
                if not interface_arg:
                    assert not isinstance(object_arg.type, GraphQLNonNull), (
                        "{}.{}({}:) is of required type "
                        '"{}" but is not also provided by the '
                        "interface {}.{}."
                    ).format(
                        object,
                        field_name,
                        arg_name,
                        object_arg.type,
                        interface,
                        field_name,
                    )
