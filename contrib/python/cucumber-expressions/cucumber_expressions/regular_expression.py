import re

from cucumber_expressions.argument import Argument
from cucumber_expressions.parameter_type import ParameterType
from cucumber_expressions.parameter_type_registry import ParameterTypeRegistry
from cucumber_expressions.tree_regexp import TreeRegexp


class RegularExpression:
    """Creates a new instance. Use this when the transform types are not known in advance,
    and should be determined by the regular expression's capture groups. Use this with
    dynamically typed languages."""

    def __init__(
        self,
        expression_regexp,
        parameter_type_registry: ParameterTypeRegistry,
    ):
        """Creates a new instance. Use this when the transform types are not known in advance,
        and should be determined by the regular expression's capture groups. Use this with
        dynamically typed languages.
        :param expression_regexp: the regular expression to use
        :type expression_regexp: Pattern
        :param parameter_type_registry: used to look up parameter types
        :type parameter_type_registry: ParameterTypeRegistry
        """
        self.expression_regexp = re.compile(expression_regexp)
        self.parameter_type_registry = parameter_type_registry
        self.tree_regexp: TreeRegexp = TreeRegexp(self.expression_regexp.pattern)

    def match(self, text) -> list[Argument] | None:
        return Argument.build(
            self.tree_regexp,
            text,
            list(self.generate_parameter_types(text)),
        )

    def generate_parameter_types(self, text):
        for group_builder in self.tree_regexp.group_builder.children:
            parameter_type_regexp = group_builder.source
            possible_regexp = self.parameter_type_registry.lookup_by_regexp(
                parameter_type_regexp,
                self.expression_regexp,
                text,
            )
            yield possible_regexp or ParameterType(
                None,
                parameter_type_regexp,
                str,
                lambda *s: s[0],
                False,
                False,
            )

    @property
    def regexp(self):
        return self.expression_regexp.pattern
