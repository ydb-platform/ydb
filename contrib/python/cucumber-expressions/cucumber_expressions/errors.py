from cucumber_expressions.ast import Token


def generate_message(
    index: int,
    expression: str,
    pointer: str,
    problem: str,
    solution: str,
):
    return "\n".join(
        [
            f"This Cucumber Expression has a problem at column {index + 1}:",
            "",
            expression,
            pointer,
            problem + ".",
            solution,
        ],
    )


def point_at(index: int):
    return " " * index + "^"


def point_at_located(node):
    pointer = [point_at(node.start)]
    if node.start + 1 < node.end:
        for _ in range(node.start + 1, node.end - 1):
            pointer.append("-")
        pointer.append("^")
    return "".join(pointer)


class CucumberExpressionError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class AlternativeMayNotExclusivelyContainOptionals(CucumberExpressionError):
    def __init__(self, node, expression: str):
        super().__init__(
            generate_message(
                index=node.start,
                expression=expression,
                pointer=point_at_located(node),
                problem="An alternative may not exclusively contain optionals",
                solution="If you did not mean to use an optional you can use '\\(' to escape the '('",
            ),
        )


class AlternativeMayNotBeEmpty(CucumberExpressionError):
    def __init__(self, node, expression: str):
        super().__init__(
            generate_message(
                index=node.start,
                expression=expression,
                pointer=point_at_located(node),
                problem="Alternative may not be empty",
                solution="If you did not mean to use an alternative you can use '\\/' to escape the '/'",
            ),
        )


class CantEscape(CucumberExpressionError):
    def __init__(self, expression: str, index: int):
        super().__init__(
            generate_message(
                index=index,
                expression=expression,
                pointer=point_at(index),
                problem="Only the characters '{', '}', '(', ')', '\\', '/' and whitespace can be escaped",
                solution="If you did mean to use an '\\' you can use '\\\\' to escape it",
            ),
        )


class OptionalMayNotBeEmpty(CucumberExpressionError):
    def __init__(self, node, expression: str):
        super().__init__(
            generate_message(
                index=node.start,
                expression=expression,
                pointer=point_at_located(node),
                problem="An optional must contain some text",
                solution="If you did not mean to use an optional you can use '\\(' to escape the '('",
            ),
        )


class ParameterIsNotAllowedInOptional(CucumberExpressionError):
    def __init__(self, node, expression: str):
        super().__init__(
            generate_message(
                index=node.start,
                expression=expression,
                pointer=point_at_located(node),
                problem="An optional may not contain a parameter type",
                solution="If you did not mean to use an parameter type you can use '\\{' to escape the '{'",
            ),
        )


class OptionalIsNotAllowedInOptional(CucumberExpressionError):
    def __init__(self, node, expression: str):
        super().__init__(
            generate_message(
                index=node.start,
                expression=expression,
                pointer=point_at_located(node),
                problem="An optional may not contain an other optional",
                solution=(
                    "If you did not mean to use an optional type you can use '\\(' to escape the '('. "
                    "For more complicated expressions consider using a regular expression instead."
                ),
            ),
        )


class MissingEndToken(CucumberExpressionError):
    def __init__(self, expression: str, begin_token, end_token, current):
        begin_symbol = Token.symbol_of(begin_token)
        end_symbol = Token.symbol_of(end_token)
        purpose = Token.purpose_of(begin_token)

        super().__init__(
            generate_message(
                index=current.start,
                expression=expression,
                pointer=point_at_located(current),
                problem=f"The '{begin_symbol}' does not have a matching '{end_symbol}'",
                solution=f"If you did not intend to use {purpose} you can use '\\{begin_symbol}' to escape the {purpose}",
            ),
        )


class AlternationNotAllowedInOptional(CucumberExpressionError):
    def __init__(self, expression: str, current):
        super().__init__(
            generate_message(
                index=current.start,
                expression=expression,
                pointer=point_at_located(current),
                problem="An alternation can not be used inside an optional",
                solution=(
                    "If you did not mean to use an alternation you can use '\\/' to escape the '/'. "
                    "Otherwise rephrase your expression or consider using a regular expression instead."
                ),
            ),
        )


class InvalidParameterTypeName(CucumberExpressionError):
    def __init__(self, type_name: str):
        super().__init__(
            "Illegal character in parameter name {"
            + type_name
            + "}. "
            + "Parameter names may not contain '{', '}', '(', ')', '\\' or '/'",
        )


class InvalidParameterTypeNameInNode(CucumberExpressionError):
    def __init__(self, expression: str, token):
        super().__init__(
            generate_message(
                index=token.start,
                expression=expression,
                pointer=point_at_located(token),
                problem="Parameter names may not contain '{', '}', '(', ')', '\\' or '/'",
                solution="Did you mean to use a regular expression?",
            ),
        )


class UndefinedParameterTypeError(CucumberExpressionError):
    def __init__(self, node, expression: str, undefined_parameter_type_name: str):
        super().__init__(
            generate_message(
                index=node.start,
                expression=expression,
                pointer=point_at_located(node),
                problem=f"Undefined parameter type '{undefined_parameter_type_name}'",
                solution=f"Please register a ParameterType for '{undefined_parameter_type_name}'",
            ),
        )


class AmbiguousParameterTypeError(CucumberExpressionError):
    def __init__(
        self,
        parameter_type_regexp,
        expression_regexp,
        parameter_types,
        generated_expressions,
    ):
        self.message = f"""Your Regular Expression /{expression_regexp}/
matches multiple parameter types with regexp /{parameter_type_regexp}/:
   {self.parameter_type_names(parameter_types)}

I couldn't decide which one to use. You have two options:

1) Use a Cucumber Expression instead of a Regular Expression. Try one of these:
   {self.expressions(generated_expressions)}

2) Make one of the parameter types preferential and continue to use a Regular Expression."""
        super().__init__(self.message)

    @staticmethod
    def parameter_type_names(parameter_types):
        return "\n   ".join(["{" + p.name + "}" for p in parameter_types])

    @staticmethod
    def expressions(generated_expressions):
        return "\n   ".join([ge.source for ge in generated_expressions])


class TheEndOfLineCannotBeEscaped(CucumberExpressionError):
    def __init__(self, expression: str):
        index = len(expression) - 1

        super().__init__(
            generate_message(
                index=index,
                expression=expression,
                pointer=point_at(index),
                problem="The end of line can not be escaped",
                solution="You can use '\\\\' to escape the '\\'",
            ),
        )
