import sys
from typing import Any, List, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field as PydanticField
from pydantic import ConfigDict as PydanticConfigDict

from .templates import (
    _template_directive,
    _template_field,
    _template_fragment,
    _template_inline_fragment,
    _template_key_argument,
    _template_key_arguments,
    _template_key_objects,
    _template_key_value,
    _template_key_values,
    _template_key_variable,
    _template_operation,
    _template_query,
    _template_variable,
)

if sys.version_info >= (3, 10):
    from typing import TypeGuard
else:
    # Use TypeGuard from typing_extensions for python <= 3.9
    from typing_extensions import TypeGuard

__all__ = [
    "Variable",
    "Argument",
    "Directive",
    "Field",
    "InlineFragment",
    "Fragment",
    "Query",
    "Operation",
]


class _GraphQL2PythonQuery(PydanticBaseModel):
    """An abstract class for GraphQL query type."""

    model_config = PydanticConfigDict(
        arbitrary_types_allowed=True,
    )

    @staticmethod
    def _line_shift(text: str) -> str:
        return "\n  ".join(text.split("\n"))

    def _render_field(self, field: Union[str, 'Field', 'InlineFragment', 'Fragment']) -> str:
        if isinstance(field, str):
            return field

        if isinstance(field, Fragment):
            return f"...{field.name}"

        return self._line_shift(field.render())

    def render(self) -> str:
        raise NotImplementedError


class Variable(_GraphQL2PythonQuery):
    """GraphQL variable type.

    See https://graphql.org/learn/queries/#variables for more details.

    Attributes:
        name: A name of the variable.
        type: A GraphQL type of the variable.
        default: A default value for the variable.

    Example:

        In the query

        >>> query_str = '''
        ... query HeroNameAndFriends($episode: Episode = JEDI) {
        ...   hero(episode: $episode) {
        ...     name
        ...     friends {
        ...       name
        ...     }
        ...   }
        ... }
        ... '''

        we have the following variable:

        >>> var_episode = Variable(name="episode", type="Episode", default="JEDI")

    """

    name: str
    type: str
    default: Optional[str] = PydanticField(default=None)

    def render(self) -> str:
        return _template_variable.render(name=self.name, type=self.type, default=self.default)


class Argument(_GraphQL2PythonQuery):
    """GraphQL Argument type.

    See https://graphql.org/learn/queries/#arguments for more details.

    Attributes:
        name: A name of the argument.
        value: The argument value.

    Example 1:

        In the query

        >>> query_string = '''
        ... {
        ...   human(id: "1000") {
        ...     name
        ...     height(unit: FOOT)
        ...   }
        ... }
        ... '''

        we have two arguments

        >>> arg_id = Argument(name="id", value='"1000"')
        >>> arg_unit = Argument(name="unit", value='FOOT')

    Example 2:

        In the query

        >>> query_string = '''
        ... {
        ...   q(
        ...     filter1: {
        ...       filter2: {
        ...         field1: "value1"
        ...          field2: VALUE2
        ...        }
        ...      }
        ...   ) {
        ...     ...
        ...   }
        ... }
        ... '''

        we have the long argument

        >>> filter1 = Argument(
        ...     name="filter1",
        ...     value=Argument(
        ...         name="filter2",
        ...         value=[
        ...             Argument(name="field1", value='"value1"'),
        ...             Argument(name="field2", value='VALUE2'),
        ...         ]
        ...     )
        ... )

    """

    name: str
    value: Union[
        str,
        int,
        bool,
        float,
        'Argument',
        Variable,
        List[str],
        List[int],
        List[bool],
        List[float],
        List['Argument'],
        List[List['Argument']],
    ]

    @staticmethod
    def _check_is_list_of_str(values: List[Any]) -> TypeGuard[List[str]]:
        return all(isinstance(value, str) for value in values)

    @staticmethod
    def _check_is_list_of_int(values: List[Any]) -> TypeGuard[List[int]]:
        return all(isinstance(value, int) for value in values)

    @staticmethod
    def _check_is_list_of_bool(values: List[Any]) -> TypeGuard[List[bool]]:
        return all(isinstance(value, bool) for value in values)

    @staticmethod
    def _check_is_list_of_float(values: List[Any]) -> TypeGuard[List[float]]:
        return all(isinstance(value, float) for value in values)

    @staticmethod
    def _check_is_list_of_arguments(values: List[Any]) -> TypeGuard[List['Argument']]:
        return all(isinstance(value, Argument) for value in values)

    @staticmethod
    def _check_is_list_of_list(values: List[Any]) -> TypeGuard[List[List[Any]]]:
        return all(isinstance(value, list) for value in values)

    @staticmethod
    def _render_for_str(name: str, value: str) -> str:
        return _template_key_value.render(name=name, value=value)

    @staticmethod
    def _render_for_int(name: str, value: int) -> str:
        return _template_key_value.render(name=name, value=str(value))

    @staticmethod
    def _render_for_float(name: str, value: float) -> str:
        return _template_key_value.render(name=name, value=str(value))

    @staticmethod
    def _render_for_list_str(name: str, value: List[str]) -> str:
        clean_list = []
        for item in value:
            result = item.replace('"', '').split(',')
            if isinstance(result, list):
                trimmed_result = [i.strip() for i in result]
                clean_list.extend(trimmed_result)
            else:
                clean_list.append(result)

        return _template_key_values.render(name=name, values=[f'''\"{v.replace('"', '')}\"''' for v in clean_list])

    @staticmethod
    def _render_for_list_int(name: str, value: List[int]) -> str:
        return _template_key_values.render(name=name, values=[str(v) for v in value])

    @staticmethod
    def _render_for_list_bool(name: str, value: List[bool]) -> str:
        return _template_key_values.render(name=name, values=[str(v).lower() for v in value])

    @staticmethod
    def _render_for_list_float(name: str, value: List[float]) -> str:
        return _template_key_values.render(name=name, values=[str(v).lower() for v in value])

    @staticmethod
    def _render_for_variable(name: str, value: Variable) -> str:
        return _template_key_variable.render(name=name, value=value.name)

    def _render_for_argument(self, name: str, value: 'Argument') -> str:
        return _template_key_argument.render(name=name, argument=self._line_shift(value.render()))

    def _render_for_list_argument(self, name: str, value: List['Argument']) -> str:
        return _template_key_arguments.render(
            name=name, arguments=[self._line_shift(argument.render()) for argument in value]
        )

    def _render_for_list_list_argument(self, name: str, value: List[List['Argument']]) -> str:
        return _template_key_objects.render(
            name=name,
            list_arguments=[
                [self._line_shift(self._line_shift(argument.render())) for argument in arguments] for arguments in value
            ],
        )

    def render(self) -> str:
        if isinstance(self.value, str):
            return self._render_for_str(self.name, self.value)

        if isinstance(self.value, bool):
            return self._render_for_str(self.name, str(self.value).lower())

        if isinstance(self.value, int):
            return self._render_for_int(self.name, self.value)

        if isinstance(self.value, float):
            return self._render_for_float(self.name, self.value)

        if isinstance(self.value, Argument):
            return self._render_for_argument(self.name, self.value)

        if isinstance(self.value, Variable):
            return self._render_for_variable(self.name, self.value)

        if isinstance(self.value, list):
            if self._check_is_list_of_str(self.value):
                return self._render_for_list_str(self.name, self.value)

            if self._check_is_list_of_bool(self.value):
                return self._render_for_list_bool(self.name, self.value)

            if self._check_is_list_of_float(self.value):
                return self._render_for_list_float(self.name, self.value)

            if self._check_is_list_of_int(self.value):
                return self._render_for_list_int(self.name, self.value)

            if self._check_is_list_of_arguments(self.value):
                return self._render_for_list_argument(self.name, self.value)

            if self._check_is_list_of_list(self.value):
                if all(self._check_is_list_of_arguments(v) for v in self.value):
                    return self._render_for_list_list_argument(self.name, self.value)

        raise ValueError("Invalid type for `graphql_query.Argument.value`.")


class Directive(_GraphQL2PythonQuery):
    """GraphQL directive type.

    See https://graphql.org/learn/queries/#directives for more details.

    Attributes:
        name: A directive name.
        arguments: Directive arguments.

    Example:

        In the query

        >>> query_str = '''
        ... query Hero($episode: Episode, $withFriends: Boolean!) {
        ...   hero(episode: $episode) {
        ...     name
        ...     friends @include(if: $withFriends) {
        ...       name
        ...     }
        ...   }
        ... }
        ... '''

        we have the directive

        >>> var_with_friends = Variable(name="withFriends", type="Boolean!")
        >>> directive_if = Directive(
        ...     name="include",
        ...     arguments=[
        ...         Argument(name="if", value=var_with_friends)
        ...     ]
        ... )

    """

    name: str
    arguments: List[Argument] = PydanticField(default_factory=list)

    def render(self) -> str:
        return _template_directive.render(
            name=self.name,
            arguments=[self._line_shift(argument.render()) for argument in self.arguments],
        )


class Field(_GraphQL2PythonQuery):
    """GraphQL Field type.

    See https://graphql.org/learn/queries/#fields for more details.

    Attributes:
        name: The field name.
        alias: The field alias.
        arguments: All arguments for the field.
        fields: All sub-fields for the field.
        directives: All field directives.
        typename: Add meta field `__typename` to sub-fields.

    Example:

        In the query

        .. code-block:: python

        >>> query_string = '''
        ... {
        ...   query {
        ...     field1 {
        ...       __typename
        ...       field2 {
        ...         __typename
        ...         f1
        ...         f2
        ...         f3
        ...       }
        ...     }
        ...   }
        ... }
        ... '''

        we have the following fields

        >>> Field(
        ...     name="field1",
        ...     fields=[
        ...         Field(
        ...             name="field2",
        ...             fields=["f1", Field(name="f2"), "f3"],
        ...             typename=True
        ...         )
        ...     ],
        ...     typename=True
        ... )

    """

    name: str
    alias: Optional[str] = PydanticField(default=None)
    arguments: List[Argument] = PydanticField(default_factory=list)
    fields: List[Union[str, 'Field', 'InlineFragment', 'Fragment']] = PydanticField(default_factory=list)
    directives: List[Directive] = PydanticField(default_factory=list)
    typename: bool = PydanticField(default=False, description="Add meta field `__typename` to sub-fields.")

    def render(self) -> str:
        return _template_field.render(
            name=self.name,
            alias=self.alias,
            arguments=[self._line_shift(argument.render()) for argument in self.arguments],
            fields=[self._render_field(field) for field in self.fields],
            directives=[directive.render() for directive in self.directives],
            typename=self.typename,
        )


class InlineFragment(_GraphQL2PythonQuery):
    """Inline Fragment GraphQL type.

    See https://graphql.org/learn/queries/#inline-fragments for more details.

    Attributes:
        type: A GraphQL type for the inline fragment.
        arguments: All arguments for the inline fragment.
        fields: All sub-fields for the inline fragment.
        typename: Add meta field `__typename` to sub-fields.

    Example:

        In the query

        >>> query_string = '''
        ... query HeroForEpisode($ep: Episode!) {
        ...   hero(episode: $ep) {
        ...     name
        ...     ... on Droid {
        ...       primaryFunction
        ...     }
        ...     ... on Human {
        ...       height
        ...     }
        ...   }
        ... }
        ... '''

        we have two inline fragments

        >>> inline_fragment_droid = InlineFragment(type="Droid", fields=["primaryFunction"])
        >>> inline_fragment_human = InlineFragment(type="Human", fields=["height"])

    """

    type: str
    arguments: List[Argument] = PydanticField(default_factory=list)
    fields: List[Union[str, 'Field', 'InlineFragment', 'Fragment']] = PydanticField(default_factory=list)
    typename: bool = PydanticField(default=False, description="Add meta field `__typename` to sub-fields.")

    def render(self) -> str:
        return _template_inline_fragment.render(
            type=self.type,
            arguments=[self._line_shift(argument.render()) for argument in self.arguments],
            fields=[self._render_field(field) for field in self.fields],
            typename=self.typename,
        )


class Fragment(_GraphQL2PythonQuery):
    """GraphQL fragment type.

    See https://graphql.org/learn/queries/#fragments for more details.

    Attributes:
        name: The fragment name.
        type: A GraphQL type for the fragment.
        fields: All sub-fields for the fragment.
        typename: Add meta field `__typename` to sub-fields.

    Example:

        In the query

        >>> query_string = '''
        ... {
        ...   leftComparison: hero(episode: EMPIRE) {
        ...     ...comparisonFields
        ...   }
        ...   rightComparison: hero(episode: JEDI) {
        ...     ...comparisonFields
        ...   }
        ... }
        ...
        ... fragment comparisonFields on Character {
        ...   name
        ...   appearsIn
        ...   friends {
        ...     name
        ...   }
        ... }
        ... '''

        we have the fragment

        >>> fragment_comparisonFields = Fragment(
        ...     name="comparisonFields",
        ...     type="Character",
        ...     fields=["name", "appearsIn", Field(name="friends", fields=["name"])]
        ... )

    """

    name: str
    type: str
    fields: List[Union[str, 'Field', 'InlineFragment', 'Fragment']] = PydanticField(default_factory=list)
    typename: bool = PydanticField(default=False, description="Add meta field `__typename` to sub-fields")

    def render(self) -> str:
        return _template_fragment.render(
            name=self.name,
            type=self.type,
            fields=[self._render_field(field) for field in self.fields],
            typename=self.typename,
        )


class Query(_GraphQL2PythonQuery):
    """GraphQL query type.

    See https://graphql.org/learn/queries/ for more details.

    Attributes:
        name: The query name.
        alias: An optional query alias.
        arguments: All query arguments.
        typename: Add meta field `__typename` to the query.
        fields: All sub-fields for the query.

    Example:

        For the query

        >>> query_string = '''
        ... {
        ...   human: human1000th(id: "1000") {
        ...     name
        ...     height
        ...   }
        ... }
        ... '''

        we have

        >>> human = Query(
        ...     name="human",
        ...     alias="human1000th",
        ...     arguments=[
        ...         Argument(name="id", value='"1000"')
        ...     ],
        ...     fields=["name", "height"]
        ... )

    """

    name: str
    alias: Optional[str] = PydanticField(default=None)
    arguments: List[Argument] = PydanticField(default_factory=list)
    typename: bool = PydanticField(default=False, description="Add meta field `__typename` to the query.")
    fields: List[Union[str, 'Field', 'InlineFragment', 'Fragment']] = PydanticField(default_factory=list)

    def render(self) -> str:
        return _template_query.render(
            name=self.name,
            alias=self.alias,
            arguments=[self._line_shift(argument.render()) for argument in self.arguments],
            typename=self.typename,
            fields=[self._render_field(field) for field in self.fields],
        )


class Operation(_GraphQL2PythonQuery):
    """GraphQL Operation type.

    See https://graphql.org/learn/queries/ for more details.

    Attributes:
        type: A operation type.
        name: An optional operation name.
        variables: All operation variables.
        queries: All operation queries.
        fragments: All fragments for the operation.

    Example:

        For the query

        >>> query_string = '''
        ... mutation CreateReviewForEpisode($ep: Episode!, $review: ReviewInput!) {
        ...   createReview(episode: $ep, review: $review) {
        ...     stars
        ...     commentary
        ...   }
        ... }
        ... '''

        we have

        >>> var_ep = Variable(name="ep", type="Episode!")
        >>> var_review = Variable(name="review", type="ReviewInput!")
        >>>
        >>> Operation(
        ...     type="mutation",
        ...     name="CreateReviewForEpisode",
        ...     variables=[var_ep, var_review],
        ...     queries=[
        ...         Query(
        ...             name="createReview",
        ...             arguments=[
        ...                 Argument(name="episode", value=var_ep),
        ...                 Argument(name="review", value=var_review),
        ...             ],
        ...             fields=["stars", "commentary"]
        ...         ),
        ...     ],
        ... )

    """

    type: str = PydanticField(default="query", description="https://graphql.org/learn/queries")
    name: Optional[str] = PydanticField(default=None, description="https://graphql.org/learn/queries/#operation-name")
    variables: List[Variable] = PydanticField(
        default_factory=list, description="https://graphql.org/learn/queries/#fragments"
    )
    queries: List[Query] = PydanticField(default_factory=list, description="Queries for this GraphQL operation.")
    fragments: List[Fragment] = PydanticField(
        default_factory=list, description="https://graphql.org/learn/queries/#fragments"
    )

    def render(self) -> str:
        return _template_operation.render(
            type=self.type,
            name=self.name,
            variables=[self._line_shift(variable.render()) for variable in self.variables],
            queries=[self._line_shift(query.render()) for query in self.queries],
            fragments=[fragment.render() for fragment in self.fragments],
        )
