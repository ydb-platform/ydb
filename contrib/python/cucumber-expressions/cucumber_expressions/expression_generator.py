import functools
import re

from cucumber_expressions.combinatorial_generated_expression_factory import (
    CombinatorialGeneratedExpressionFactory,
)
from cucumber_expressions.generated_expression import GeneratedExpression
from cucumber_expressions.parameter_type import ParameterType
from cucumber_expressions.parameter_type_matcher import ParameterTypeMatcher


class CucumberExpressionGenerator:
    def __init__(self, parameter_type_registry):
        self.parameter_type_registry = parameter_type_registry

    def generate_expressions(self, text: str) -> list[GeneratedExpression]:
        parameter_type_combinations = []
        parameter_type_matchers = self.create_parameter_type_matchers(text)
        expression_template: list[str] = []
        pos = 0

        while True:

            def get_matching_parameter_type_matchers():
                for parameter_type_matcher in parameter_type_matchers:
                    advanced_parameter_type_matcher = parameter_type_matcher.advance_to(
                        pos,
                    )
                    if advanced_parameter_type_matcher.find:
                        yield advanced_parameter_type_matcher

            matching_parameter_type_matchers = list(
                get_matching_parameter_type_matchers(),
            )
            if not matching_parameter_type_matchers:
                break

            matching_parameter_type_matchers.sort(
                key=functools.cmp_to_key(ParameterTypeMatcher.compare),
            )
            best_parameter_type_matcher = matching_parameter_type_matchers[0]
            best_parameter_type_matchers = list(
                filter(
                    lambda m: ParameterTypeMatcher.compare(
                        m,
                        best_parameter_type_matcher,
                    )
                    == 0,
                    matching_parameter_type_matchers,
                ),
            )
            # Filter duplicates
            parameter_types = {
                _parameter_type_matcher.parameter_type
                for _parameter_type_matcher in best_parameter_type_matchers
            }
            parameter_type_combinations.append(
                sorted(
                    parameter_types,
                    key=functools.cmp_to_key(ParameterType.compare),
                ),
            )
            expression_template.append(
                self.escape(text[pos : best_parameter_type_matcher.start]),
            )
            expression_template.append("{%s}")
            pos = best_parameter_type_matcher.start + len(
                best_parameter_type_matcher.group,
            )
            if pos >= len(text):
                break
        expression_template.append(self.escape(text[pos:]))
        return CombinatorialGeneratedExpressionFactory(
            "".join(expression_template),
            parameter_type_combinations,
        ).generate_expressions()

    @staticmethod
    def escape(string: str) -> str:
        return (
            string.replace("%", "%%")
            .replace(r"(", "\\(")
            .replace(r"{", "\\{")
            .replace(r"/", "\\/")
        )

    def create_parameter_type_matchers(self, text) -> list[ParameterTypeMatcher]:
        parameter_type_matchers = []
        for parameter_type in self.parameter_type_registry.parameter_types:
            if parameter_type.use_for_snippets:
                parameter_type_matchers.extend(
                    self.create_parameter_type_matchers_with_type(parameter_type, text),
                )
        return parameter_type_matchers

    @staticmethod
    def create_parameter_type_matchers_with_type(
        parameter_type,
        text,
    ) -> list[ParameterTypeMatcher]:
        return [
            ParameterTypeMatcher(parameter_type, re.compile(f"({regexp})"), text, 0)
            for regexp in parameter_type.regexps
        ]
