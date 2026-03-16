import datetime
import re
import typing as t

import bson
from odmantic import Model, query
from odmantic.field import (
    FieldProxy,
)
from odmantic.query import QueryExpression
from pydantic import TypeAdapter, ValidationError  # type: ignore[attr-defined]


def normalize_list(
    arr: t.Optional[t.Sequence[t.Any]], is_default_sort_list: bool = False
) -> t.Optional[t.Sequence[str]]:
    if arr is None:
        return None
    _new_list = []
    for v in arr:
        if isinstance(v, FieldProxy):
            _new_list.append(str(+v))
        elif isinstance(v, str):
            _new_list.append(v)
        elif (
            isinstance(v, tuple) and is_default_sort_list
        ):  # Support for fields_default_sort:
            if (
                len(v) == 2
                and isinstance(v[0], (str, FieldProxy))
                and isinstance(v[1], bool)
            ):
                _new_list.append(
                    (
                        +v[0] if isinstance(v[0], FieldProxy) else v[0],  # type: ignore[arg-type]
                        v[1],
                    )
                )
            else:
                raise ValueError(
                    "Invalid argument, Expected Tuple[str | FieldProxy, bool]"
                )
        else:
            raise ValueError(f"Expected str or FieldProxy, got {type(v).__name__}")
    return _new_list


def _rec(value: t.Any, regex: str) -> t.Pattern:
    return re.compile(regex % re.escape(value), re.IGNORECASE)


OPERATORS: t.Dict[str, t.Callable[[FieldProxy, t.Any], QueryExpression]] = {
    "eq": lambda f, v: f == v,
    "neq": lambda f, v: f != v,
    "lt": lambda f, v: f < v,
    "gt": lambda f, v: f > v,
    "le": lambda f, v: f <= v,
    "ge": lambda f, v: f >= v,
    "in": lambda f, v: f.in_(v),
    "not_in": lambda f, v: f.not_in(v),
    "startswith": lambda f, v: f.match(_rec(v, r"^%s")),
    "not_startswith": lambda f, v: query.nor_(f.match(_rec(v, r"^%s"))),
    "endswith": lambda f, v: f.match(_rec(v, r"%s$")),
    "not_endswith": lambda f, v: query.nor_(f.match(_rec(v, r"%s$"))),
    "contains": lambda f, v: f.match(_rec(v, r"%s")),
    "not_contains": lambda f, v: query.nor_(f.match(_rec(v, r"%s"))),
    "is_false": lambda f, v: f.eq(False),
    "is_true": lambda f, v: f.eq(True),
    "is_null": lambda f, v: f.eq(None),
    "is_not_null": lambda f, v: f.ne(None),
    "between": lambda f, v: query.and_(f >= v[0], f <= v[1]),
    "not_between": lambda f, v: query.or_(f < v[0], f > v[1]),
}


def parse_datetime(value: str) -> bool:
    try:
        TypeAdapter(datetime.datetime).validate_python(value)
    except ValidationError:
        return False
    return True


def resolve_proxy(model: t.Type[Model], proxy_name: str) -> t.Optional[FieldProxy]:
    _list = proxy_name.split(".")
    m = model
    for v in _list:
        if m is not None:
            m = getattr(m, v, None)  # type: ignore
    return m  # type: ignore[return-value]


def _check_value(v: t.Any, proxy: t.Optional[FieldProxy]) -> t.Any:
    """
    The purpose of this function is to detect datetime string, or ObjectId
    and convert them into the appropriate python type.
    """
    if isinstance(v, str) and parse_datetime(v):
        return datetime.datetime.fromisoformat(v)
    if proxy is not None and +proxy == "_id" and bson.ObjectId.is_valid(v):
        return bson.ObjectId(v)
    return v


def resolve_deep_query(
    where: t.Dict[str, t.Any],
    model: t.Type[Model],
    field_proxy: t.Optional[FieldProxy] = None,
) -> QueryExpression:
    _all_queries = []
    for key, _ in where.items():
        if key == "or":
            _all_queries.append(
                query.or_(
                    *[(resolve_deep_query(q, model, field_proxy)) for q in where[key]]
                )
            )
        elif key == "and":
            _all_queries.append(
                query.and_(
                    *[resolve_deep_query(q, model, field_proxy) for q in where[key]]
                )
            )
        elif key in OPERATORS:
            v = where[key]
            v = (
                [_check_value(it, field_proxy) for it in v]
                if isinstance(v, list)
                else _check_value(v, field_proxy)
            )
            _all_queries.append(OPERATORS[key](field_proxy, v))  # type: ignore
        else:
            proxy = resolve_proxy(model, key)
            if proxy is not None:
                _all_queries.append(resolve_deep_query(where[key], model, proxy))
    if len(_all_queries) == 1:
        return _all_queries[0]
    return query.and_(*_all_queries) if _all_queries else QueryExpression({})
