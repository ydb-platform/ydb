import django
from django.db import models

dj3 = (3,) <= django.VERSION < (4,)
dj4 = (4,) <= django.VERSION < (5,)
dj_ge4 = django.VERSION >= (4,)
dj_ge41 = django.VERSION >= (4, 1)
dj_ge42 = django.VERSION >= (4, 2)
dj_ge5 = django.VERSION >= (5,)
dj_ge51 = django.VERSION >= (5, 1)
dj_ge52 = django.VERSION >= (5, 2)
dj_ge6 = django.VERSION >= (6,)


def db_table_comment(model: models.Model) -> str:
    """return a model's database table comment.

    https://docs.djangoproject.com/en/4.2/releases/4.2/#comments-on-columns-and-tables
    """
    return dj_ge42 and model._meta.db_table_comment or ""


def field_db_comment(field: models.Field) -> str:
    """return a field's database column comment.

    https://docs.djangoproject.com/en/4.2/releases/4.2/#comments-on-columns-and-tables
    """
    return dj_ge42 and field.db_comment or ""


def field_has_db_default(field: models.Field) -> bool:
    """check if a field has database level default value.

    https://docs.djangoproject.com/en/5.0/releases/5.0/#database-computed-default-values
    """
    if dj_ge52:
        return field.has_db_default()
    if dj_ge5:
        return field.db_default is not models.NOT_PROVIDED
    return False
