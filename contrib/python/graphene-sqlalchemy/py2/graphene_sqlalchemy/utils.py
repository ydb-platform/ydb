from sqlalchemy.exc import ArgumentError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import class_mapper, object_mapper
from sqlalchemy.orm.exc import UnmappedClassError, UnmappedInstanceError
from graphene.utils.str_converters import to_camel_case, to_snake_case

from graphene import Argument, Enum, List


def get_session(context):
    return context.get("session")


def get_query(model, context):
    query = getattr(model, "query", None)
    if not query:
        session = get_session(context)
        if not session:
            raise Exception(
                "A query in the model Base or a session in the schema is required for querying.\n"
                "Read more http://docs.graphene-python.org/projects/sqlalchemy/en/latest/tips/#querying"
            )
        query = session.query(model)
    return query


_operator_aliases = {
    "==": lambda x, y: x == y,
    "=": lambda x, y: x == y,
    ">=": lambda x, y: x >= y,
    ">": lambda x, y: x > y,
    "<=": lambda x, y: x <= y,
    "<": lambda x, y: x < y,
    "!=": lambda x, y: x != y
}


def get_operator_function(operator=None):
    if not operator:
        def operator(x, y):
            return x == y

    if not callable(operator):
        operator_attr = operator

        if operator in _operator_aliases:
            operator = _operator_aliases[operator_attr]
        else:
            def operator(x, y):
                return getattr(x, operator_attr)(y)

    return operator


def get_snake_or_camel_attr(model, attr):
    try:
        return getattr(model, to_snake_case(attr))
    except Exception:
        pass
    try:
        return getattr(model, to_camel_case(attr))
    except Exception:
        pass
    return getattr(model, attr)


def is_mapped_class(cls):
    try:
        class_mapper(cls)
    except (ArgumentError, UnmappedClassError):
        return False
    else:
        return True


def is_mapped_instance(cls):
    try:
        object_mapper(cls)
    except (ArgumentError, UnmappedInstanceError):
        return False
    else:
        return True


def _symbol_name(column_name, is_asc):
    return column_name + ("_asc" if is_asc else "_desc")


class EnumValue(str):
    """Subclass of str that stores a string and an arbitrary value in the "value" property"""

    def __new__(cls, str_value, value):
        return super(EnumValue, cls).__new__(cls, str_value)

    def __init__(self, str_value, value):
        super(EnumValue, self).__init__()
        self.value = value


# Cache for the generated enums, to avoid name clash
_ENUM_CACHE = {}


def _sort_enum_for_model(cls, name=None, symbol_name=_symbol_name):
    name = name or cls.__name__ + "SortEnum"
    if name in _ENUM_CACHE:
        return _ENUM_CACHE[name]
    items = []
    default = []
    for column in inspect(cls).columns.values():
        asc_name = symbol_name(column.name, True)
        asc_value = EnumValue(asc_name, column.asc())
        desc_name = symbol_name(column.name, False)
        desc_value = EnumValue(desc_name, column.desc())
        if column.primary_key:
            default.append(asc_value)
        items.extend(((asc_name, asc_value), (desc_name, desc_value)))
    enum = Enum(name, items)
    _ENUM_CACHE[name] = (enum, default)
    return enum, default


def sort_enum_for_model(cls, name=None, symbol_name=_symbol_name):
    """Create Graphene Enum for sorting a SQLAlchemy class query

    Parameters
    - cls : Sqlalchemy model class
        Model used to create the sort enumerator
    - name : str, optional, default None
        Name to use for the enumerator. If not provided it will be set to `cls.__name__ + 'SortEnum'`
    - symbol_name : function, optional, default `_symbol_name`
        Function which takes the column name and a boolean indicating if the sort direction is ascending,
        and returns the symbol name for the current column and sort direction.
        The default function will create, for a column named 'foo', the symbols 'foo_asc' and 'foo_desc'

    Returns
    - Enum
        The Graphene enumerator
    """
    enum, _ = _sort_enum_for_model(cls, name, symbol_name)
    return enum


def sort_argument_for_model(cls, has_default=True):
    """Returns a Graphene argument for the sort field that accepts a list of sorting directions for a model.
    If `has_default` is True (the default) it will sort the result by the primary key(s)
    """
    enum, default = _sort_enum_for_model(cls)
    if not has_default:
        default = None
    return Argument(List(enum), default_value=default)
