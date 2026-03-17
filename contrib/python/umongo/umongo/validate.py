import marshmallow as ma

from .abstract import BaseValidator


__all__ = (
    'URL',
    'Email',
    'Range',
    'Length',
    'Equal',
    'Regexp',
    'Predicate',
    'NoneOf',
    'OneOf',
    'ContainsOnly'
)


class URL(BaseValidator, ma.validate.URL):
    pass


class Email(BaseValidator, ma.validate.Email):
    pass


class Range(BaseValidator, ma.validate.Range):
    pass


class Length(BaseValidator, ma.validate.Length):
    pass


class Equal(BaseValidator, ma.validate.Equal):
    pass


class Regexp(BaseValidator, ma.validate.Regexp):
    pass


class Predicate(BaseValidator, ma.validate.Predicate):
    pass


class NoneOf(BaseValidator, ma.validate.NoneOf):
    pass


class OneOf(BaseValidator, ma.validate.OneOf):
    pass


class ContainsOnly(BaseValidator, ma.validate.ContainsOnly):
    pass
