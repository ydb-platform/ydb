from typing import Any, Callable, Dict, Optional, Sequence

from sqlalchemy import String, and_, cast, false, not_, or_, true
from sqlalchemy.orm import (
    InstrumentedAttribute,
    RelationshipProperty,
)
from sqlalchemy.orm.attributes import ScalarObjectAttributeImpl
from sqlalchemy.sql import ClauseElement


def __is_null(latest_attr: InstrumentedAttribute) -> Any:
    if isinstance(latest_attr.property, RelationshipProperty):
        if isinstance(latest_attr.impl, ScalarObjectAttributeImpl):
            return ~latest_attr.has()
        return ~latest_attr.any()
    return latest_attr.is_(None)


def __is_not_null(latest_attr: InstrumentedAttribute) -> Any:
    if isinstance(latest_attr.property, RelationshipProperty):
        if isinstance(latest_attr.impl, ScalarObjectAttributeImpl):
            return latest_attr.has()
        return latest_attr.any()
    return latest_attr.is_not(None)


OPERATORS: Dict[str, Callable[[InstrumentedAttribute, Any], ClauseElement]] = {
    "eq": lambda f, v: f == v,
    "neq": lambda f, v: f != v,
    "lt": lambda f, v: f < v,
    "gt": lambda f, v: f > v,
    "le": lambda f, v: f <= v,
    "ge": lambda f, v: f >= v,
    "in": lambda f, v: f.in_(v),
    "not_in": lambda f, v: f.not_in(v),
    "startswith": lambda f, v: cast(f, String).startswith(v),
    "not_startswith": lambda f, v: not_(cast(f, String).startswith(v)),
    "endswith": lambda f, v: cast(f, String).endswith(v),
    "not_endswith": lambda f, v: not_(cast(f, String).endswith(v)),
    "contains": lambda f, v: cast(f, String).contains(v),
    "not_contains": lambda f, v: not_(cast(f, String).contains(v)),
    "is_false": lambda f, v: f == false(),
    "is_true": lambda f, v: f == true(),
    "is_null": lambda f, v: __is_null(f),
    "is_not_null": lambda f, v: __is_not_null(f),
    "between": lambda f, v: f.between(*v),
    "not_between": lambda f, v: not_(f.between(*v)),
}


def build_query(
    where: Dict[str, Any],
    model: Any,
    latest_attr: Optional[InstrumentedAttribute] = None,
) -> Any:
    filters = []
    for key, _ in where.items():
        if key == "or":
            filters.append(
                or_(*[build_query(v, model, latest_attr) for v in where[key]])
            )
        elif key == "and":
            filters.append(
                and_(*[build_query(v, model, latest_attr) for v in where[key]])
            )
        elif key in OPERATORS:
            filters.append(OPERATORS[key](latest_attr, where[key]))  # type: ignore
        else:
            attr: Optional[InstrumentedAttribute] = getattr(model, key, None)
            if attr is not None:
                filters.append(build_query(where[key], model, attr))
    if len(filters) == 1:
        return filters[0]
    if filters:
        return and_(*filters)
    return and_(True)


def normalize_list(
    arr: Optional[Sequence[Any]], is_default_sort_list: bool = False
) -> Optional[Sequence[str]]:
    """This methods will convert all InstrumentedAttribute into str"""
    if arr is None:
        return None
    _new_list = []
    for v in arr:
        if isinstance(v, InstrumentedAttribute):
            _new_list.append(v.key)
        elif isinstance(v, str):
            _new_list.append(v)
        elif (
            isinstance(v, tuple) and is_default_sort_list
        ):  # Support for fields_default_sort:
            if (
                len(v) == 2
                and isinstance(v[0], (str, InstrumentedAttribute))
                and isinstance(v[1], bool)
            ):
                _new_list.append(
                    (
                        v[0].key if isinstance(v[0], InstrumentedAttribute) else v[0],  # type: ignore[arg-type]
                        v[1],
                    )
                )
            else:
                raise ValueError(
                    "Invalid argument, Expected Tuple[str | InstrumentedAttribute, bool]"
                )
        else:
            raise ValueError(
                f"Expected str or InstrumentedAttribute, got {type(v).__name__}"
            )
    return _new_list


def extract_column_python_type(attr: InstrumentedAttribute) -> type:
    try:
        return attr.type.python_type
    except NotImplementedError:
        return str
