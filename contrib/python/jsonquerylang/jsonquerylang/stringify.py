import json
from functools import reduce
from typing import List, Optional, Union, Final

from jsonquerylang.regexps import unquoted_property_regex
from jsonquerylang.operators import (
    operators,
    extend_operators,
    left_associative_operators,
)
from jsonquerylang.types import (
    JsonQueryType,
    JsonQueryStringifyOptions,
    JsonQueryObjectType,
    JsonPath,
    JsonQueryFunctionType,
)
from jsonquerylang.utils import find_index, merge

DEFAULT_MAX_LINE_LENGTH = 40
DEFAULT_INDENTATION = "  "


def stringify(
    query: JsonQueryType, options: Optional[JsonQueryStringifyOptions] = None
) -> str:
    """
    Stringify a JSON Query into a readable, human friendly text syntax.

    Example:

        from jsonquerylang import stringify

        jsonQuery = [
            "pipe",
            ["get", "friends"],
            ["filter", ["eq", ["get", "city"], "New York"]],
            ["sort", ["get", "age"]],
            ["pick", ["get", "name"], ["get", "age"]],
        ]
        textQuery = stringify(jsonQuery)
        print(textQuery)
        # '.friends | filter(.city == "new York") | sort(.age) | pick(.name, .age)'

    :param query: A JSON Query
    :param options: A dict with custom operators, max_line_length, and indentation
    :return: Returns a human friendly string representation of the query
    """

    space: Final = (
        options.get("indentation") if options else None
    ) or DEFAULT_INDENTATION
    max_line_length: Final = (
        options.get("max_line_length") if options else None
    ) or DEFAULT_MAX_LINE_LENGTH
    custom_operators: Final = (options.get("operators") if options else None) or []
    all_operators: Final = extend_operators(operators, custom_operators)
    all_operators_map: Final = reduce(merge, all_operators)
    all_left_associative_operators: Final = left_associative_operators + list(
        map(
            lambda op: op["op"],
            filter(lambda op: op.get("left_associative"), custom_operators),
        )
    )

    def _stringify(_query: JsonQueryType, indent: str, parenthesis=False) -> str:
        if type(_query) is list:
            return stringify_function(_query, indent, parenthesis)
        else:
            return json.dumps(_query)  # value (string, number, boolean, null)

    def stringify_function(
        query_fn: JsonQueryFunctionType, indent: str, parenthesis: bool
    ) -> str:
        name, *args = query_fn

        if name == "get" and len(args) > 0:
            return stringify_path(args)

        if name == "object":
            return stringify_object(args[0], indent)

        if name == "array":
            args_str = stringify_args(args, indent + space)
            return join(
                args_str,
                ["[", ", ", "]"],
                [f"[\n{indent + space}", f",\n{indent + space}", f"\n{indent}]"],
            )

        # operator like ".age >= 18"
        op = all_operators_map.get(name)
        if op:
            start = "(" if parenthesis else ""
            end = ")" if parenthesis else ""

            def stringify_operator_arg(arg: JsonQueryType, index: int):
                child_name = arg[0] if type(arg) is list else None
                precedence = find_index(lambda group: name in group, all_operators)
                child_precedence = find_index(
                    lambda group: child_name in group, all_operators
                )
                child_parenthesis = (
                    precedence < child_precedence
                    or (precedence == child_precedence and index > 0)
                    or (name == child_name and op not in all_left_associative_operators)
                )

                return _stringify(arg, indent + space, child_parenthesis)

            args_str = [
                stringify_operator_arg(arg, index) for index, arg in enumerate(args)
            ]

            return join(
                args_str,
                [start, f" {op} ", end],
                [start, f"\n{indent + space}{op} ", end],
            )

        # regular function like "sort(.age)"
        child_indent = indent if len(args) == 1 else indent + space
        args_str = stringify_args(args, child_indent)
        return join(
            args_str,
            [f"{name}(", ", ", ")"],
            (
                [f"{name}(", f",\n{indent}", ")"]
                if len(args) == 1
                else [
                    f"{name}(\n{child_indent}",
                    f",\n{child_indent}",
                    f"\n{indent})",
                ]
            ),
        )

    def stringify_object(query_obj: JsonQueryObjectType, indent: str) -> str:
        child_indent = indent + space
        entries = [
            f"{stringify_property(key)}: {_stringify(value, child_indent)}"
            for key, value in query_obj.items()
        ]
        return join(
            entries,
            ["{ ", ", ", " }"],
            [f"{{\n{child_indent}", f",\n{child_indent}", f"\n{indent}}}"],
        )

    def stringify_args(args: List, indent: str) -> List[str]:
        return list(map(lambda arg: _stringify(arg, indent), args))

    def stringify_path(path: JsonPath) -> str:
        return "".join([f".{stringify_property(prop)}" for prop in path])

    def stringify_property(prop: Union[str, int]) -> str:
        prop_str = str(prop)
        return prop_str if unquoted_property_regex.match(prop_str) else json.dumps(prop)

    def join(items: List[str], compact: List[str], formatted: List[str]) -> str:
        compact_start, compact_separator, compact_end = compact
        format_start, format_separator, format_end = formatted

        compact_length = (
            len(compact_start)
            + sum(len(item) + len(compact_separator) for item in items)
            - len(compact_separator)
            + len(compact_end)
        )
        if compact_length <= max_line_length:
            return compact_start + compact_separator.join(items) + compact_end
        else:
            return format_start + format_separator.join(items) + format_end

    return _stringify(query, "")
