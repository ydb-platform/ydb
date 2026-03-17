import functools
import re
from decimal import Decimal

from cucumber_expressions.errors import (
    AmbiguousParameterTypeError,
    CucumberExpressionError,
)
from cucumber_expressions.expression_generator import CucumberExpressionGenerator
from cucumber_expressions.parameter_type import ParameterType

INTEGER_REGEXPS = [re.compile(r"-?\d+"), re.compile(r"\d+")]
FLOAT_REGEXP = re.compile(r"(?=.*\d.*)[-+]?\d*(?:\.(?=\d.*))?\d*(?:\d+[E][+-]?\d+)?")
WORD_REGEXP = re.compile(r"[^\s]+")
STRING_REGEXPS = [
    re.compile(r'"([^\"\\]*(\\.[^\"\\]*)*)"'),
    re.compile(r"'([^'\\]*(\\.[^'\\]*)*)'"),
]
ANONYMOUS_REGEXP = re.compile(r".*")


class ParameterTypeRegistry:
    def __init__(self):
        self.parameter_type_by_name = {}
        self.parameter_types_by_regexp = {}
        self.define_parameter_type(
            ParameterType(
                "int",
                INTEGER_REGEXPS,
                int,
                lambda s: s and int(s),
                True,
                True,
            ),
        )
        self.define_parameter_type(
            ParameterType(
                "float",
                FLOAT_REGEXP,
                float,
                lambda s: s and float(s),
                True,
                False,
            ),
        )
        self.define_parameter_type(
            ParameterType("word", WORD_REGEXP, str, lambda s: s, False, False),
        )
        self.define_parameter_type(
            ParameterType(
                "string",
                STRING_REGEXPS,
                str,
                lambda s1, s2: str(s2 if s1 is None else s1)
                .replace('\\"', '"')
                .replace("\\'", "'"),
                True,
                False,
            ),
        )
        self.define_parameter_type(
            ParameterType("", ANONYMOUS_REGEXP, int, lambda s: s, False, True),
        )
        self.define_parameter_type(
            ParameterType(
                "bigdecimal",
                FLOAT_REGEXP,
                Decimal,
                lambda s: Decimal(s),
                False,
                False,
            ),
        )
        self.define_parameter_type(
            ParameterType(
                "biginteger",
                INTEGER_REGEXPS,
                int,
                lambda s: int(s),
                False,
                False,
            ),
        )
        self.define_parameter_type(
            ParameterType("byte", INTEGER_REGEXPS, int, lambda s: int(s), False, False),
        )
        self.define_parameter_type(
            ParameterType(
                "short",
                INTEGER_REGEXPS,
                int,
                lambda s: int(s),
                False,
                False,
            ),
        )
        self.define_parameter_type(
            ParameterType("long", INTEGER_REGEXPS, int, lambda s: int(s), False, False),
        )
        self.define_parameter_type(
            ParameterType(
                "double",
                FLOAT_REGEXP,
                float,
                lambda s: float(s),
                False,
                False,
            ),
        )

    @property
    def parameter_types(self) -> list:
        return list(self.parameter_type_by_name.values())

    def lookup_by_type_name(self, name: str) -> ParameterType | None:
        return self.parameter_type_by_name.get(name)

    def lookup_by_regexp(
        self,
        parameter_type_regexp: str,
        expression_regexp,
        text: str,
    ):
        raw_regex = rf"{parameter_type_regexp}"
        parameter_types = self.parameter_types_by_regexp.get(raw_regex)
        if not parameter_types:
            return None
        if len(parameter_types) > 1 and not parameter_types[0].prefer_for_regexp_match:
            generated_expressions = CucumberExpressionGenerator(
                self,
            ).generate_expressions(text)
            raise AmbiguousParameterTypeError(
                parameter_type_regexp,
                expression_regexp,
                parameter_types,
                generated_expressions,
            )
        return parameter_types[0]

    def define_parameter_type(self, parameter_type: ParameterType) -> None:
        if parameter_type.name is not None:
            if parameter_type.name in self.parameter_type_by_name:
                if not parameter_type.name:
                    raise CucumberExpressionError(
                        "The anonymous parameter type has already been defined",
                    )
                raise CucumberExpressionError(
                    f"There is already a parameter with name {parameter_type.name}",
                )
            self.parameter_type_by_name[parameter_type.name] = parameter_type

        for parameter_type_regexp in parameter_type.regexps:
            if parameter_type_regexp not in self.parameter_types_by_regexp:
                self.parameter_types_by_regexp[parameter_type_regexp] = []
            parameter_types = self.parameter_types_by_regexp[parameter_type_regexp]
            if bool(
                parameter_types
                and parameter_types[0].prefer_for_regexp_match
                and parameter_type.prefer_for_regexp_match,
            ):
                raise CucumberExpressionError(
                    f"""
                    There can only be one preferential parameter type per regexp.
                    The regexp '{parameter_type_regexp}' is used for two preferential parameter types,
                    {parameter_types[0].name} and {parameter_type.name}
                    """,
                )
            parameter_types.append(parameter_type)
            parameter_types.sort(key=functools.cmp_to_key(ParameterType.compare))
            self.parameter_types_by_regexp[parameter_type_regexp] = parameter_types
