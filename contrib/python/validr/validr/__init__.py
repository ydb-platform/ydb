"""A simple, fast, extensible python library for data validation."""
from .model import ImmutableInstanceError, asdict, fields, modelclass
from .schema import Builder, Compiler, Schema, T
from .validator import (
    Invalid,
    ModelInvalid,
    SchemaError,
    ValidrError,
    builtin_validators,
    create_enum_validator,
    create_re_validator,
)
from .validator import py_mark_index as mark_index
from .validator import py_mark_key as mark_key
from .validator import validator

__all__ = (
    'ValidrError', 'Invalid', 'ModelInvalid', 'SchemaError',
    'mark_index', 'mark_key',
    'create_re_validator', 'create_enum_validator',
    'builtin_validators', 'validator',
    'Schema', 'Compiler', 'T', 'Builder',
    'modelclass', 'fields', 'asdict', 'ImmutableInstanceError',
)
