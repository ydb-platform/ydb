#  Copyright (c) Kuba Szczodrzyński 2024-10-12.

from dataclasses import Field, is_dataclass
from enum import Enum, auto
from types import FunctionType

from datastruct.types import FieldMeta, FieldType

from .fields import field_get_base, field_get_meta
from .fmt import fmt_check
from .types import check_types_match, decode_type


class ValidType(Enum):
    REPEAT = auto()
    UNION = auto()
    ANY = auto()
    ELLIPSIS = auto()
    SIMPLE = auto()


UNION_FIELDS = (FieldType.COND, FieldType.SWITCH)


def _validate_ellipsis_usage(meta: FieldMeta) -> bool:
    # process special fields (seek, padding, etc)
    # also: wrapper fields that wrap special fields - except switch()
    # (wrapper fields inherit the 'public' property; switch() doesn't)
    if not meta.public:
        if meta.types != type(Ellipsis):
            raise TypeError("Use Ellipsis (...) for special fields")
        # wrapped special fields make the wrappers non-public
        # so accept any special fields that aren't wrappers
        if not meta.base:
            return True
        # otherwise only allow wrapping special fields inside cond()
        if meta.ftype != FieldType.COND:
            raise TypeError("Only cond() and switch() can wrap special fields")
    # reject public fields with incorrect type
    # (except switch() fields, as they can wrap special fields)
    elif meta.types == type(Ellipsis) and meta.ftype not in UNION_FIELDS:
        raise TypeError("Cannot use Ellipsis (...) for standard fields")
    return False


def _validate_property_type_usage(meta: FieldMeta) -> ValidType:
    # CHECK FIELD VALIDITY DEPENDING ON THE PROPERTY CLASS(ES)
    is_tuple = isinstance(meta.types, tuple)

    # generic type (cls, True, args...) - only allow repeat()
    if is_tuple and True in meta.types:
        if meta.types[0] != list:
            # var: dict[int, int] = field(...)
            raise TypeError("Unknown generic type; only list[...] is supported")
        if meta.ftype != FieldType.REPEAT:
            # var: list[int] = field(...)
            raise TypeError("Use repeat() for lists")
        return ValidType.REPEAT

    # union type (cls, cls...) - only allow cond() or switch()
    elif is_tuple and len(meta.types):
        # len(meta.types) >= 2
        if meta.ftype not in UNION_FIELDS:
            if len(meta.types) > 2:
                # var: int | float | bytes = field(...)
                raise TypeError("Use switch() for union of 3 or more types")
            elif type(None) in meta.types:
                # var: DataStruct | None = field(...)
                raise TypeError("Use cond() for optional types")
            else:
                # var: DataStruct | OtherStruct = field(...)
                raise TypeError("Use switch()/cond() for union/optional types")
        return ValidType.UNION

    # special type Any (empty tuple) - only allow switch()
    elif is_tuple:
        if meta.ftype not in UNION_FIELDS:
            # var: Any = field(...)
            raise TypeError("The 'Any' type can only be used with switch()/cond()")
        return ValidType.ANY

    # special type Ellipsis - only allow cond(), switch()
    elif meta.types == type(Ellipsis):
        if meta.ftype not in UNION_FIELDS:
            # var: ... = field(...)
            raise TypeError("The Ellipsis (...) can only be used with switch()/cond()")
        return ValidType.ELLIPSIS

    # simple types (primitives, DataStruct, etc.)
    else:
        # FieldType.FIELD is used for field(), subfield(), built()
        if isinstance(None, meta.types):
            # var: None = field(...)
            raise TypeError("Cannot use None as field type")
        if is_dataclass(meta.types):
            if meta.fmt is not None and not meta.adapter:
                # var: DataStruct = field(...)
                # var: DataStruct = built(...)
                raise TypeError("Use subfield() for instances of DataStruct")
        else:
            if meta.fmt is None and not meta.base and meta.ftype != FieldType.SWITCH:
                # var: int = subfield()
                raise TypeError("Use field() for non-DataStruct types")
        return ValidType.SIMPLE


def _validate_field(field: Field, meta: FieldMeta, valid_type: ValidType) -> None:
    # validate format specifiers
    if meta.fmt is not None:
        # var: int = field(...)
        fmt_check(meta.fmt)


def _validate_repeat(field: Field, meta: FieldMeta, valid_type: ValidType) -> None:
    if valid_type != ValidType.REPEAT:
        # var: int = repeat()(...)
        raise TypeError("Can't use repeat() for a non-list field")
    base_field, base_meta = field_get_base(meta)
    if base_meta.builder and not base_meta.always:
        # var: ... = repeat()(built(..., always=False))
        raise TypeError("Built fields inside repeat() are always built")
    if not meta.types[2] and base_meta.ftype == FieldType.FIELD:
        # var: list = repeat()(field(...))
        raise TypeError("Lists of standard fields must be parameterized")
    base_field.name = field.name
    # "unwrap" item types for repeat fields only
    field.type = meta.types[0]
    base_field.type = meta.types[2]
    field_validate(base_field, base_meta)


def _validate_cond(field: Field, meta: FieldMeta, valid_type: ValidType) -> None:
    if_not_type = None
    if type(meta.if_not) != FunctionType:
        # cannot get 'if_not=' type for lambdas
        if meta.if_not in [None, Ellipsis]:
            # specified None as default value
            if_not_type = type(None)
        else:
            if_not_type = type(meta.if_not)
    base_field, base_meta = field_get_base(meta)
    base_field.name = field.name
    if valid_type == ValidType.UNION:
        # var: int | bool = cond(...)(field(...))
        # var: int | None = cond(...)(field(...))
        # -> len(meta.types) >= 2
        # verify that the type is specified for this field
        if if_not_type and if_not_type not in meta.types:
            # var: int | bool = cond(..., if_not=None)(field(...))
            raise TypeError(
                f"Type of 'if_not=' ({if_not_type}) must be part of the union"
            )
        # for wrapped switch() - don't change the base field type, let it handle that
        if base_meta.ftype == FieldType.SWITCH:
            base_field.type = field.type
        # for Union[*, ..., None] - use all non-None types
        elif type(None) in meta.types:
            # var: int | None = cond(...)(field(...))
            types = list(meta.types)
            types.remove(type(None))
            base_field.type = types[0] if len(types) == 1 else tuple(types)
        # for Union[*, *, ...] - try to guess the two types
        else:
            # var: int | bool = cond(...)(field(...))
            if if_not_type:
                # var: int | bool = cond(..., if_not=False)(field(...))
                # 'if_not=' has a known type, simply use the other one
                types = list(meta.types)
                types.remove(if_not_type)
                base_field.type = types[0]
            else:
                # var: int | bool = cond(..., if_not=lambda ctx: ...)(field(...))
                # two types - check if any is a DataStruct
                structs = tuple(is_dataclass(cls) for cls in meta.types)
                if len(structs) == 2 and structs[0] and not structs[1]:
                    # var: DataStruct | int = cond(..., ...)(subfield(...))
                    # var: DataStruct | int = cond(..., ...)(field(...))
                    base_field.type = meta.types[0 if base_meta.fmt is None else 1]
                elif len(structs) == 2 and structs[1] and not structs[0]:
                    # var: int | DataStruct = cond(..., ...)(subfield(...))
                    # var: int | DataStruct = cond(..., ...)(field(...))
                    base_field.type = meta.types[1 if base_meta.fmt is None else 0]
                else:
                    # var: int | bool = cond(..., ...)(field(...))
                    raise TypeError("Couldn't guess the wrapped field's type")
    elif valid_type == ValidType.ANY:
        # var: Any = cond(...)(field(...))
        # pass empty tuple for validation of the base field
        base_field.type = ()
    elif valid_type == ValidType.ELLIPSIS:
        # var: ... = cond(...)(field(...))
        # -> meta.types == type(Ellipsis)
        base_field.type = Ellipsis
    elif valid_type == ValidType.SIMPLE:
        # var: int = cond(..., if_not=0)(field(...))
        # -> type(meta.types) == type
        if if_not_type and if_not_type != meta.types:
            # var: int = cond(..., if_not=None)(field(...))
            raise TypeError(
                f"Type of 'if_not=' ({if_not_type}) different than the field type"
            )
        base_field.type = meta.types
    else:
        raise TypeError("No valid class/type found for cond()")
    field_validate(base_field, base_meta)


def _validate_switch(field: Field, meta: FieldMeta, valid_type: ValidType) -> None:
    # test each case of the switch field
    base_types = []
    for key, (field_type, base_field) in meta.fields.items():
        base_type = decode_type(field_type)
        if not check_types_match(base_type, meta.types):
            # var: int | bool = switch(...)(_1=(bytes, field(...)))
            raise TypeError(
                f"Case field type {base_type} (for case '{key}') "
                f"does not fit the switch() field type {meta.types}"
            )
        base_meta = field_get_meta(base_field)
        base_field.name = field.name
        base_field.type = base_type
        field_validate(base_field, base_meta)
        base_types.append(base_type)
    if meta.types == type(Ellipsis) and type(Ellipsis) not in base_types:
        raise TypeError(
            "Cannot use Ellipsis (...) for switch() fields without special fields"
        )


VALIDATORS = {
    FieldType.FIELD: _validate_field,
    FieldType.REPEAT: _validate_repeat,
    FieldType.COND: _validate_cond,
    FieldType.SWITCH: _validate_switch,
}


def field_validate(field: Field, meta: FieldMeta) -> None:
    if meta.validated:
        return
    # decode field type
    meta.types = decode_type(field.type)

    # Validation checks all standard and wrapper fields, which are based on 4 types:
    # - FIELD - field(), subfield(), built(), adapter()
    # - REPEAT - repeat()
    # - COND - cond()
    # - SWITCH - switch()
    # It also makes sure that special fields (non-public) are used either unwrapped,
    # or with a compatible wrapper field.

    if _validate_ellipsis_usage(meta):
        meta.validated = True
        return
    valid_type = _validate_property_type_usage(meta)
    if meta.ftype in VALIDATORS:
        VALIDATORS[meta.ftype](field, meta, valid_type)
    meta.validated = True
