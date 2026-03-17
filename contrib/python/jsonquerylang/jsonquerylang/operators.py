from functools import reduce
from typing import Mapping
from jsonquerylang.types import OperatorGroup, CustomOperator
from jsonquerylang.utils import find_index

operators: list[OperatorGroup] = [
    {"pow": "^"},
    {"multiply": "*", "divide": "/", "mod": "%"},
    {"add": "+", "subtract": "-"},
    {"gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "in": "in", "not in": "not in"},
    {"eq": "==", "ne": "!="},
    {"and": "and"},
    {"or": "or"},
    {"pipe": "|"},
]

vararg_operators = ["|", "and", "or"]

left_associative_operators = ["|", "and", "or", "*", "/", "%", "+", "-"]


def extend_operators(
    all_operators: list[OperatorGroup], custom_operators: list[CustomOperator]
) -> list[OperatorGroup]:
    # backward compatibility error with v4 where `operators` was an object
    if type(custom_operators) is not list:
        raise RuntimeError("Invalid custom operators")

    return reduce(extend_operator, custom_operators, all_operators)


def extend_operator(
    all_operators: list[OperatorGroup], custom_operator: CustomOperator
) -> list[OperatorGroup]:
    name = custom_operator.get("name")
    op = custom_operator.get("op")
    at = custom_operator.get("at")
    after = custom_operator.get("after")
    before = custom_operator.get("before")

    if at:
        callback = lambda group: {**group, name: op} if at in group.values() else group

        return list(map(callback, all_operators))

    search_op = after or before
    index = find_index(lambda group: search_op in group.values(), all_operators)
    if index != -1:
        updated_operators = all_operators.copy()
        new_group: Mapping[str, str] = {name: op}
        updated_operators.insert(index + (1 if after else 0), new_group)

        return updated_operators

    raise RuntimeError("Invalid custom operator")
