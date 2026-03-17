from collections import UserList
from collections.abc import Iterable

from django import forms
from django.core.validators import RegexValidator
from django.db.models import CharField, IntegerField
from django.db.models.expressions import Func
from django.db.models.fields import TextField
from django.db.models.lookups import PostgresOperatorLookup, Transform
from django.forms.widgets import TextInput

# PathField implementation borrows significantly from https://github.com/mariocesar/django-ltree/blob/master/django_ltree/fields.py

path_label_validator = RegexValidator(
    r"^(?P<root>[a-zA-Z0-9_-]+)(?:\.[a-zA-Z0-9_-]+)*$",
    "A label is a sequence of alphanumeric characters and underscores separated by dots or slashes.",
    "invalid",
)


class PathValue(UserList):
    def __init__(self, value):
        if isinstance(value, str):
            value = value.strip().split(".") if value else []
        elif isinstance(value, Iterable):
            value = list(value)
        else:
            raise ValueError(f"Invalid value: {value!r} for path")

        super().__init__(initlist=value)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return ".".join(self)


class PathValueProxy:
    def __init__(self, field_name):
        self.field_name = field_name

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = instance.__dict__[self.field_name]

        if value is None:
            return value

        return PathValue(instance.__dict__[self.field_name])

    def __set__(self, instance, value):
        if instance is None:
            return self

        instance.__dict__[self.field_name] = value


class PathFormField(forms.CharField):
    default_validators = [path_label_validator]


class PathField(TextField):
    default_validators = [path_label_validator]

    def db_type(self, connection):
        return "ltree"

    def formfield(self, **kwargs):
        kwargs["form_class"] = PathFormField
        kwargs["widget"] = TextInput(attrs={"class": "vTextField"})
        return super().formfield(**kwargs)

    def contribute_to_class(self, cls, name, private_only=False):
        super().contribute_to_class(cls, name)
        setattr(cls, self.name, PathValueProxy(self.name))

    def from_db_value(self, value, expression, connection, *args):
        if value is None:
            return value
        return PathValue(value)

    def get_prep_value(self, value):
        if value is None:
            return value
        return str(PathValue(value))

    def to_python(self, value):
        if value is None:
            return value

        if isinstance(value, PathValue):
            return value

        return PathValue(value)

    def get_db_prep_value(self, value, connection, prepared=False):
        if value is None:
            return value

        if isinstance(value, PathValue):
            return str(value)

        if isinstance(value, (list, str)):
            return str(PathValue(value))

        raise ValueError(f"Unknown value type {type(value)}")


@PathField.register_lookup
class AncestorLookup(PostgresOperatorLookup):
    lookup_name = "ancestors"
    postgres_operator = "@>"


@PathField.register_lookup
class DescendantLookup(PostgresOperatorLookup):
    lookup_name = "descendants"
    postgres_operator = "<@"


class Subpath(Func):
    function = "subpath"
    output_field = PathField()

    def __init__(self, expression, pos, length=None, **extra):
        """
        expression: the name of a field, or an expression returning a string
        pos: an integer >= 0, or an expression returning an integer
        length: an optional number of labels to return
        """
        if not hasattr(pos, "resolve_expression"):
            if pos < 0:
                raise ValueError("'pos' must be positive")
        expressions = [expression, pos]
        if length is not None:
            expressions.append(length)
        super().__init__(*expressions, **extra)


class Ltree2Text(Transform):
    function = "ltree2text"
    lookup_name = "ltree2text"
    output_field = CharField()


class Text2LTree(Transform):
    function = "text2ltree"
    lookup_name = "text2ltree"
    output_field = PathField()


@PathField.register_lookup
class NLevel(Transform):
    lookup_name = "depth"
    function = "nlevel"
    output_field = IntegerField()
