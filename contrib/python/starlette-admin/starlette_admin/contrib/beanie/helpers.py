import functools
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

from beanie import Document, Link
from beanie.odm.enums import SortDirection
from beanie.odm.fields import ExpressionField
from beanie.odm.operators.find import BaseFindOperator
from beanie.odm.operators.find.logical import LogicalOperatorForListOfExpressions
from beanie.operators import GT, GTE, LT, LTE, NE, And, Eq, In, Not, NotIn, Or, RegEx


class BeanieLogicalOperator(LogicalOperatorForListOfExpressions):

    @property
    def query(self) -> Dict[str, Any]:
        if not self.expressions:
            return {}
        raise ValueError(
            "BeanieLogicalOperator should not be used directly. Use resolve_deep_query instead."
        )


OPERATORS: Dict[str, Callable[[str, Any], BaseFindOperator]] = {
    "eq": lambda f, v: Eq(f, v),
    "neq": lambda f, v: NE(f, v),
    "lt": lambda f, v: LT(f, v),
    "gt": lambda f, v: GT(f, v),
    "le": lambda f, v: LTE(f, v),
    "ge": lambda f, v: GTE(f, v),
    "in": lambda f, v: In(f, v),
    "not_in": lambda f, v: NotIn(f, v),
    "startswith": lambda f, v: RegEx(f, f"^{v}", "i"),
    "not_startswith": lambda f, v: Not(RegEx(f, f"^{v}", "i")),
    "endswith": lambda f, v: RegEx(f, f"{v}$", "i"),
    "not_endswith": lambda f, v: Not(RegEx(f, f"{v}$", "i")),
    "contains": lambda f, v: RegEx(f, v, "i"),
    "not_contains": lambda f, v: Not(RegEx(f, v, "i")),
    "is_false": lambda f, v: Eq(f, False),
    "is_true": lambda f, v: Eq(f, True),
    "is_null": lambda f, v: Eq(f, None),
    "is_not_null": lambda f, v: NE(f, None),
    "between": lambda f, v: And(GTE(f, v[0]), LTE(f, v[1])),
    "not_between": lambda f, v: Or(LT(f, v[0]), GT(f, v[1])),
}


def isvalid_field(document: Type[Document], field: str) -> bool:
    """
    Check if field is valid field for document. nested field is separate with '.'
    """
    try:
        split_fields = field.split(".", maxsplit=1)
        if len(split_fields) == 1:
            top_field, nested_field = split_fields[0], None
        else:
            top_field, nested_field = split_fields

        subdoc = document.model_fields.get(top_field)
        if not subdoc:
            return False
        if nested_field is None:
            return True

        nested_type = subdoc.annotation
        return isvalid_field(nested_type, nested_field)

    except Exception:  # pragma: no cover
        return False


def resolve_expression_field_name(field: ExpressionField) -> str:
    field_str = str(field)
    return "id" if field_str == "_id" else field_str


def normalize_field_list(field_list: List[Union[str, ExpressionField]]) -> List[str]:
    converted_field_list = []
    for field in field_list:
        if isinstance(field, ExpressionField):
            field_name = resolve_expression_field_name(field)
        elif isinstance(field, str):
            field_name = field
        else:
            raise ValueError(
                f"Expected str or ExpressionField, got {type(field).__name__}"
            )

        converted_field_list.append(field_name)
    return converted_field_list


def is_link_type(field_type: Type) -> bool:
    """Check if the field type is a Link or a list of Links.
    This is used to determine if the field is a relation field.
    If the field type is Optional[Link], return true

    Args:
        field_type (Type): The field type to check.

    Returns:
        bool: True if the field type is a Link or a list of Links, False otherwise.
    """
    if get_origin(field_type) is Link:
        return True
    if get_origin(field_type) is Union:
        field_args = get_args(field_type)
        if any(get_origin(arg) is Link for arg in field_args):
            return True
    return False


def is_list_of_links_type(field_type: Type) -> bool:
    """Check if the field type is a list of Links.

    Args:
        field_type (Type): The field type to check.

    Returns:
        bool: True if the field type is a list of Links, False otherwise.
    """
    if get_origin(field_type) is list:
        field_args = get_args(field_type)
        if len(field_args) == 1 and get_origin(field_args[0]) is Link:
            return True

    # if is Optional[List[Link]], return true
    if get_origin(field_type) is Union:
        field_args = get_args(field_type)
        if any(
            get_origin(arg) is list and get_origin(get_args(arg)[0]) is Link
            for arg in field_args
        ):
            return True
    return False


def resolve_deep_query(
    where: Dict[str, Any],
    document: Type[Document],
    latest_field: Optional[str] = None,
) -> LogicalOperatorForListOfExpressions:
    _all_queries = []
    for key, _ in where.items():
        if key in ["or", "and"]:
            _arr = [(resolve_deep_query(q, document, latest_field)) for q in where[key]]
            if len(_arr) > 0:
                funcs = {
                    "or": lambda q1, q2: Or(q1, q2),
                    "and": lambda q1, q2: And(q1, q2),
                }
                _all_queries.append(functools.reduce(funcs[key], _arr))
        elif key in OPERATORS:
            _all_queries.append(OPERATORS[key](latest_field, where[key]))  # type: ignore
        elif isvalid_field(document, key):
            _all_queries.append(resolve_deep_query(where[key], document, key))
    if _all_queries:
        return functools.reduce(lambda q1, q2: And(q1, q2), _all_queries)
    return BeanieLogicalOperator()


def build_order_clauses(order_list: List[str]) -> List[Tuple[str, SortDirection]]:
    clauses: List[Tuple[str, SortDirection]] = []
    for value in order_list:
        key, order = value.strip().split(maxsplit=1)
        if key == "id":
            key = "_id"  # this is a beanie quirk
        direction = (
            SortDirection.DESCENDING
            if order.lower() == "desc"
            else SortDirection.ASCENDING
        )
        clauses.append((key, direction))
    return clauses
