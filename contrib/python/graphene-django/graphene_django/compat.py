import sys
from collections.abc import Callable
from pathlib import PurePath

# For backwards compatibility, we import JSONField to have it available for import via
# this compat module (https://github.com/graphql-python/graphene-django/issues/1428).
# Django's JSONField is available in Django 3.2+ (the minimum version we support)
from django.db.models import Choices, JSONField


class MissingType:
    def __init__(self, *args, **kwargs):
        pass


try:
    # Postgres fields are only available in Django with psycopg2 installed
    # and we cannot have psycopg2 on PyPy
    from django.contrib.postgres.fields import (
        ArrayField,
        HStoreField,
        IntegerRangeField,
        RangeField,
    )
except ImportError:
    IntegerRangeField, HStoreField, RangeField = (MissingType,) * 3

    # For unit tests we fake ArrayField using JSONFields
    if any(
        PurePath(sys.argv[0]).match(p)
        for p in [
            "**/pytest",
            "**/py.test",
            "**/pytest/__main__.py",
        ]
    ):

        class ArrayField(JSONField):
            def __init__(self, *args, **kwargs):
                if len(args) > 0:
                    self.base_field = args[0]
                super().__init__(**kwargs)

    else:
        ArrayField = MissingType


try:
    from django.utils.choices import normalize_choices
except ImportError:

    def normalize_choices(choices):
        if isinstance(choices, type) and issubclass(choices, Choices):
            choices = choices.choices

        if isinstance(choices, Callable):
            choices = choices()

        # In restframework==3.15.0, choices are not passed
        # as OrderedDict anymore, so it's safer to check
        # for a dict
        if isinstance(choices, dict):
            choices = choices.items()

        return choices
