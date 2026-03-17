from sqlalchemy.orm import ColumnProperty
from sqlalchemy.types import Enum as SQLAlchemyEnumType

from graphene import Argument, Enum, List

from .utils import EnumValue, to_enum_value_name, to_type_name


def _convert_sa_to_graphene_enum(sa_enum, fallback_name=None):
    """Convert the given SQLAlchemy Enum type to a Graphene Enum type.

    The name of the Graphene Enum will be determined as follows:
    If the SQLAlchemy Enum is based on a Python Enum, use the name
    of the Python Enum.  Otherwise, if the SQLAlchemy Enum is named,
    use the SQL name after conversion to a type name. Otherwise, use
    the given fallback_name or raise an error if it is empty.

    The Enum value names are converted to upper case if necessary.
    """
    if not isinstance(sa_enum, SQLAlchemyEnumType):
        raise TypeError(
            "Expected sqlalchemy.types.Enum, but got: {!r}".format(sa_enum)
        )
    enum_class = sa_enum.enum_class
    if enum_class:
        if all(to_enum_value_name(key) == key for key in enum_class.__members__):
            return Enum.from_enum(enum_class)
        name = enum_class.__name__
        members = [
            (to_enum_value_name(key), value.value)
            for key, value in enum_class.__members__.items()
        ]
    else:
        sql_enum_name = sa_enum.name
        if sql_enum_name:
            name = to_type_name(sql_enum_name)
        elif fallback_name:
            name = fallback_name
        else:
            raise TypeError("No type name specified for {!r}".format(sa_enum))
        members = [(to_enum_value_name(key), key) for key in sa_enum.enums]
    return Enum(name, members)


def enum_for_sa_enum(sa_enum, registry):
    """Return the Graphene Enum type for the specified SQLAlchemy Enum type."""
    if not isinstance(sa_enum, SQLAlchemyEnumType):
        raise TypeError(
            "Expected sqlalchemy.types.Enum, but got: {!r}".format(sa_enum)
        )
    enum = registry.get_graphene_enum_for_sa_enum(sa_enum)
    if not enum:
        enum = _convert_sa_to_graphene_enum(sa_enum)
        registry.register_enum(sa_enum, enum)
    return enum


def enum_for_field(obj_type, field_name):
    """Return the Graphene Enum type for the specified Graphene field."""
    from .types import SQLAlchemyObjectType

    if not isinstance(obj_type, type) or not issubclass(obj_type, SQLAlchemyObjectType):
        raise TypeError(
            "Expected SQLAlchemyObjectType, but got: {!r}".format(obj_type))
    if not field_name or not isinstance(field_name, str):
        raise TypeError(
            "Expected a field name, but got: {!r}".format(field_name))
    registry = obj_type._meta.registry
    orm_field = registry.get_orm_field_for_graphene_field(obj_type, field_name)
    if orm_field is None:
        raise TypeError("Cannot get {}.{}".format(obj_type._meta.name, field_name))
    if not isinstance(orm_field, ColumnProperty):
        raise TypeError(
            "{}.{} does not map to model column".format(obj_type._meta.name, field_name)
        )
    column = orm_field.columns[0]
    sa_enum = column.type
    if not isinstance(sa_enum, SQLAlchemyEnumType):
        raise TypeError(
            "{}.{} does not map to enum column".format(obj_type._meta.name, field_name)
        )
    enum = registry.get_graphene_enum_for_sa_enum(sa_enum)
    if not enum:
        fallback_name = obj_type._meta.name + to_type_name(field_name)
        enum = _convert_sa_to_graphene_enum(sa_enum, fallback_name)
        registry.register_enum(sa_enum, enum)
    return enum


def _default_sort_enum_symbol_name(column_name, sort_asc=True):
    return to_enum_value_name(column_name) + ("_ASC" if sort_asc else "_DESC")


def sort_enum_for_object_type(
    obj_type, name=None, only_fields=None, only_indexed=None, get_symbol_name=None
):
    """Return Graphene Enum for sorting the given SQLAlchemyObjectType.

    Parameters
    - obj_type : SQLAlchemyObjectType
        The object type for which the sort Enum shall be generated.
    - name : str, optional, default None
        Name to use for the sort Enum.
        If not provided, it will be set to the object type name + 'SortEnum'
    - only_fields : sequence, optional, default None
        If this is set, only fields from this sequence will be considered.
    - only_indexed : bool, optional, default False
        If this is set, only indexed columns will be considered.
    - get_symbol_name : function, optional, default None
        Function which takes the column name and a boolean indicating
        if the sort direction is ascending, and returns the symbol name
        for the current column and sort direction. If no such function
        is passed, a default function will be used that creates the symbols
        'foo_asc' and 'foo_desc' for a column with the name 'foo'.

    Returns
    - Enum
        The Graphene Enum type
    """
    name = name or obj_type._meta.name + "SortEnum"
    registry = obj_type._meta.registry
    enum = registry.get_sort_enum_for_object_type(obj_type)
    custom_options = dict(
        only_fields=only_fields,
        only_indexed=only_indexed,
        get_symbol_name=get_symbol_name,
    )
    if enum:
        if name != enum.__name__ or custom_options != enum.custom_options:
            raise ValueError(
                "Sort enum for {} has already been customized".format(obj_type)
            )
    else:
        members = []
        default = []
        fields = obj_type._meta.fields
        get_name = get_symbol_name or _default_sort_enum_symbol_name
        for field_name in fields:
            if only_fields and field_name not in only_fields:
                continue
            orm_field = registry.get_orm_field_for_graphene_field(obj_type, field_name)
            if not isinstance(orm_field, ColumnProperty):
                continue
            column = orm_field.columns[0]
            if only_indexed and not (column.primary_key or column.index):
                continue
            asc_name = get_name(column.key, True)
            asc_value = EnumValue(asc_name, column.asc())
            desc_name = get_name(column.key, False)
            desc_value = EnumValue(desc_name, column.desc())
            if column.primary_key:
                default.append(asc_value)
            members.extend(((asc_name, asc_value), (desc_name, desc_value)))
        enum = Enum(name, members)
        enum.default = default  # store default as attribute
        enum.custom_options = custom_options
        registry.register_sort_enum(obj_type, enum)
    return enum


def sort_argument_for_object_type(
    obj_type,
    enum_name=None,
    only_fields=None,
    only_indexed=None,
    get_symbol_name=None,
    has_default=True,
):
    """"Returns Graphene Argument for sorting the given SQLAlchemyObjectType.

    Parameters
    - obj_type : SQLAlchemyObjectType
        The object type for which the sort Argument shall be generated.
    - enum_name : str, optional, default None
        Name to use for the sort Enum.
        If not provided, it will be set to the object type name + 'SortEnum'
    - only_fields : sequence, optional, default None
        If this is set, only fields from this sequence will be considered.
    - only_indexed : bool, optional, default False
        If this is set, only indexed columns will be considered.
    - get_symbol_name : function, optional, default None
        Function which takes the column name and a boolean indicating
        if the sort direction is ascending, and returns the symbol name
        for the current column and sort direction. If no such function
        is passed, a default function will be used that creates the symbols
        'foo_asc' and 'foo_desc' for a column with the name 'foo'.
    - has_default : bool, optional, default True
        If this is set to False, no sorting will happen when this argument is not
        passed. Otherwise results will be sortied by the primary key(s) of the model.

    Returns
    - Enum
        A Graphene Argument that accepts a list of sorting directions for the model.
    """
    enum = sort_enum_for_object_type(
        obj_type,
        enum_name,
        only_fields=only_fields,
        only_indexed=only_indexed,
        get_symbol_name=get_symbol_name,
    )
    if not has_default:
        enum.default = None

    return Argument(List(enum), default_value=enum.default)
