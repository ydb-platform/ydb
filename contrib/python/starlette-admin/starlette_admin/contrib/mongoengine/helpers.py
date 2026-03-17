import functools
from typing import Any, Callable, Dict, List, Optional, Sequence, Type

import mongoengine.fields as me
from mongoengine.base.fields import BaseField as MongoBaseField
from mongoengine.queryset import Q as BaseQ
from mongoengine.queryset import QNode


class Q(BaseQ):
    """
    Override mongoengine.Q class to support expression like this:
    >>> Q('name', 'Jo', 'istartswith') # same as Q(name__istartswith = 'Jo')
    or
    >>> Q('name', 'John') # same as Q(name = 'John')
    """

    def __init__(self, field: str, value: Any, op: Optional[str] = None) -> None:
        field = f'{field.replace(".", "__")}__'
        if op is not None:
            field = f"{field}{op}"
        super().__init__(**{field: value})

    @classmethod
    def empty(cls) -> BaseQ:
        return BaseQ()


OPERATORS: Dict[str, Callable[[str, Any], Q]] = {
    "eq": lambda f, v: Q(f, v),
    "neq": lambda f, v: Q(f, v, "ne"),
    "lt": lambda f, v: Q(f, v, "lt"),
    "gt": lambda f, v: Q(f, v, "gt"),
    "le": lambda f, v: Q(f, v, "lte"),
    "ge": lambda f, v: Q(f, v, "gte"),
    "in": lambda f, v: Q(f, v, "in"),
    "not_in": lambda f, v: Q(f, v, "nin"),
    "startswith": lambda f, v: Q(f, v, "istartswith"),
    "not_startswith": lambda f, v: Q(f, v, "not__istartswith"),
    "endswith": lambda f, v: Q(f, v, "iendswith"),
    "not_endswith": lambda f, v: Q(f, v, "not__iendswith"),
    "contains": lambda f, v: Q(f, v, "icontains"),
    "not_contains": lambda f, v: Q(f, v, "not__icontains"),
    "is_false": lambda f, v: Q(f, False),
    "is_true": lambda f, v: Q(f, True),
    "is_null": lambda f, v: Q(f, None),
    "is_not_null": lambda f, v: Q(f, None, "ne"),
    "between": lambda f, v: Q(f, v[0], "gte") & Q(f, v[1], "lte"),
    "not_between": lambda f, v: Q(f, v[0], "lt") | Q(f, v[1], "gt"),
}


def isvalid_field(document: Type[me.Document], field: str) -> bool:
    """
    Check if field is valid field for document. nested field is separate with '.'
    """
    try:
        document._lookup_field(field.split("."))
    except Exception:  # pragma: no cover
        return False
    return True


def resolve_deep_query(
    where: Dict[str, Any],
    document: Type[me.Document],
    latest_field: Optional[str] = None,
) -> QNode:
    _all_queries = []
    for key, _ in where.items():
        if key in ["or", "and"]:
            _arr = [(resolve_deep_query(q, document, latest_field)) for q in where[key]]
            if len(_arr) > 0:
                funcs = {"or": lambda q1, q2: q1 | q2, "and": lambda q1, q2: q1 & q2}
                _all_queries.append(functools.reduce(funcs[key], _arr))
        elif key in OPERATORS:
            _all_queries.append(OPERATORS[key](latest_field, where[key]))  # type: ignore
        elif isvalid_field(document, key):
            _all_queries.append(resolve_deep_query(where[key], document, key))
    if _all_queries:
        return functools.reduce(lambda q1, q2: q1 & q2, _all_queries)
    return Q.empty()


def build_order_clauses(order_list: List[str]) -> List[str]:
    clauses = []
    for value in order_list:
        key, order = value.strip().split(maxsplit=1)
        clauses.append("{}{}".format("-" if order.lower() == "desc" else "+", key))
    return clauses


def normalize_list(
    arr: Optional[Sequence[Any]], is_default_sort_list: bool = False
) -> Optional[Sequence[str]]:
    if arr is None:
        return None
    _new_list = []
    for v in arr:
        if isinstance(v, MongoBaseField):
            _new_list.append(v.name)
        elif isinstance(v, str):
            _new_list.append(v)
        elif (
            isinstance(v, tuple) and is_default_sort_list
        ):  # Support for fields_default_sort:
            if (
                len(v) == 2
                and isinstance(v[0], (str, MongoBaseField))
                and isinstance(v[1], bool)
            ):
                _new_list.append(
                    (
                        v[0].name if isinstance(v[0], MongoBaseField) else v[0],
                        v[1],
                    )
                )
            else:
                raise ValueError(
                    "Invalid argument, Expected Tuple[str | monogoengine.BaseField, bool]"
                )

        else:
            raise ValueError(
                f"Expected str or monogoengine.BaseField, got {type(v).__name__}"
            )
    return _new_list
