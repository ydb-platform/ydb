import json
from functools import reduce
from typing import Optional, Callable, Pattern, Final

from jsonquerylang.regexps import (
    starts_with_whitespace_regex,
    starts_with_keyword_regex,
    starts_with_int_regex,
    starts_with_number_regex,
    starts_with_unquoted_property_regex,
    starts_with_string_regex,
)
from jsonquerylang.operators import (
    operators,
    extend_operators,
    left_associative_operators,
    vararg_operators,
)
from jsonquerylang.types import JsonQueryParseOptions, JsonQueryType, OperatorGroup
from jsonquerylang.utils import merge


def parse(query: str, options: Optional[JsonQueryParseOptions] = None) -> JsonQueryType:
    """
    Parse a string containing a JSON Query into JSON.

    Example:

        from pprint import pprint
        from jsonquerylang import parse

        text_query = '.friends | filter(.city == "new York") | sort(.age) | pick(.name, .age)'
        json_query = parse(text_query)
        pprint(json_query)
        # ['pipe',
        #  ['get', 'friends'],
        #  ['filter', ['eq', ['get', 'city'], 'New York']],
        #  ['sort', ['get', 'age']],
        #  ['pick', ['get', 'name'], ['get', 'age']]]

    :param query: A query in text format
    :param options: Can an object with custom operators and functions
    :return: Returns the query in JSON format
    """

    custom_operators: Final = options.get("operators", []) if options else []

    all_operators: Final = extend_operators(operators, custom_operators)
    all_operators_map: Final = reduce(merge, all_operators)
    all_vararg_operators: Final = vararg_operators + list(
        map(
            lambda op: op.get("op"),
            filter(lambda op: op.get("vararg"), custom_operators),
        )
    )
    all_left_associative_operators: Final = left_associative_operators + list(
        map(
            lambda op: op.get("op"),
            filter(lambda op: op.get("left_associative"), custom_operators),
        )
    )

    i = 0

    def parse_operator(precedence_level: int = len(all_operators) - 1):
        nonlocal i

        if precedence_level < 0:
            return parse_parenthesis()

        current_operators = all_operators[precedence_level]

        left_parenthesis = get_char() == "("
        left = parse_operator(precedence_level - 1)

        while True:
            skip_whitespace()

            if get_char() == "." and "pipe" in current_operators:
                # an implicitly piped property like "fn().prop"
                right = parse_property()
                left = [*left, right] if left[0] == "pipe" else ["pipe", left, right]
                continue

            start = i
            name = parse_operator_name(current_operators)
            if not name:
                break

            right = parse_operator(precedence_level - 1)

            child_name = left[0] if type(left) is list else None
            chained = name == child_name and not left_parenthesis
            if (
                chained
                and all_operators_map[name] not in all_left_associative_operators
            ):
                i = start
                break

            left = (
                [*left, right]
                if chained and all_operators_map[name] in all_vararg_operators
                else [name, left, right]
            )

        return left

    def parse_operator_name(current_operators: OperatorGroup) -> str | None:
        nonlocal i

        # we sort the operators from longest to shortest, so we first handle "<=" and next "<"
        sorted_operator_names: Final = sorted(
            current_operators.keys(), key=lambda _name: len(_name), reverse=True
        )

        for name in sorted_operator_names:
            op = current_operators[name]
            if query[i : i + len(op)] == op:
                i += len(op)

                skip_whitespace()

                return name

        return None

    def parse_parenthesis():
        nonlocal i

        skip_whitespace()

        if get_char() == "(":
            i += 1
            inner = parse_operator()
            eat_char(")")
            return inner

        return parse_property()

    def parse_property():
        nonlocal i

        if get_char() == ".":
            props = []

            while get_char() == ".":
                i += 1

                prop = parse_key("Property expected")

                props.append(prop)

                skip_whitespace()

            return ["get", *props]

        return parse_function()

    def parse_function():
        nonlocal i

        start: Final = i
        name = parse_unquoted_string()
        skip_whitespace()

        if name is None or get_char() != "(":
            i = start
            return parse_object()

        i += 1

        skip_whitespace()

        args = [parse_operator()] if get_char() != ")" else []
        while i < len(query) and get_char() != ")":
            skip_whitespace()
            eat_char(",")
            args.append(parse_operator())

        eat_char(")")

        return [name, *args]

    def parse_object():
        nonlocal i

        if get_char() == "{":
            i += 1
            skip_whitespace()

            object = {}
            first = True
            while i < len(query) and get_char() != "}":
                if first:
                    first = False
                else:
                    eat_char(",")
                    skip_whitespace()

                key = str(parse_key("Key expected"))

                skip_whitespace()
                eat_char(":")

                object[key] = parse_operator()

            eat_char("}")

            return ["object", object]

        return parse_array()

    def parse_key(error_message: str):
        string = parse_string()
        if string is not None:
            return string

        unquoted_string = parse_unquoted_string()
        if unquoted_string is not None:
            return unquoted_string

        integer = parse_integer()
        if integer is not None:
            return integer

        raise_error(error_message)

    def parse_array():
        nonlocal i

        if get_char() == "[":
            i += 1
            skip_whitespace()

            array = []
            first = True
            while i < len(query) and get_char() != "]":
                if first:
                    first = False
                else:
                    eat_char(",")
                    skip_whitespace()

                array.append(parse_operator())

            eat_char("]")

            return ["array", *array]

        string = parse_string()
        if string is not None:
            return string

        number = parse_number()
        if number is not None:
            return number

        return parse_keyword()

    def parse_string():
        return parse_regex(starts_with_string_regex, json.loads)

    def parse_unquoted_string():
        return parse_regex(starts_with_unquoted_property_regex, lambda text: text)

    def parse_number():
        return parse_regex(starts_with_number_regex, json.loads)

    def parse_integer():
        return parse_regex(starts_with_int_regex, json.loads)

    def parse_keyword():
        start: Final = i
        keyword = parse_regex(starts_with_keyword_regex, json.loads)

        if i == start:
            raise_error("Value expected")

        return keyword

    def parse_end():
        skip_whitespace()

        if i < len(query):
            raise_error(f"Unexpected part '{query[i:]}'")

    def parse_regex(regex: Pattern, callback: Callable):
        nonlocal i
        match = regex.match(query[i:])
        if match:
            i += len(match.group(0))
            return callback(match.group(0))
        return None

    def skip_whitespace():
        parse_regex(starts_with_whitespace_regex, lambda text: text)

    def eat_char(char: str):
        nonlocal i

        if i < len(query) and get_char() == char:
            i += 1
        else:
            raise_error(f"Character '{char}' expected")

    def get_char():
        return query[i] if i < len(query) else None

    def raise_error(message: str, pos: Optional[int] = i):
        raise SyntaxError(f"{message} (pos: {pos if pos else i})")

    output = parse_operator()
    parse_end()

    return output
