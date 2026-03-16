import inspect

import django
from django.db import connection, models, transaction
from django.db.models.manager import Manager
from django.utils.encoding import force_str
from django.utils.functional import Promise

from graphene.utils.str_converters import to_camel_case

try:
    import django_filters  # noqa

    DJANGO_FILTER_INSTALLED = True
except ImportError:
    DJANGO_FILTER_INSTALLED = False


def isiterable(value):
    try:
        iter(value)
    except TypeError:
        return False
    return True


def _camelize_django_str(s):
    if isinstance(s, Promise):
        s = force_str(s)
    return to_camel_case(s) if isinstance(s, str) else s


def camelize(data):
    if isinstance(data, dict):
        return {_camelize_django_str(k): camelize(v) for k, v in data.items()}
    if isiterable(data) and not isinstance(data, (str, Promise)):
        return [camelize(d) for d in data]
    return data


def _get_model_ancestry(model):
    model_ancestry = [model]

    for base in model.__bases__:
        if is_valid_django_model(base) and getattr(base, "_meta", False):
            model_ancestry.append(base)
    return model_ancestry


def get_reverse_fields(model, local_field_names):
    """
    Searches through the model's ancestry and gets reverse relationships the models
    Yields a tuple of (field.name, field)
    """
    model_ancestry = _get_model_ancestry(model)

    for _model in model_ancestry:
        for name, attr in _model.__dict__.items():
            # Don't duplicate any local fields
            if name in local_field_names:
                continue

            # "rel" for FK and M2M relations and "related" for O2O Relations
            related = getattr(attr, "rel", None) or getattr(attr, "related", None)
            if isinstance(related, models.ManyToOneRel):
                yield (name, related)
            elif isinstance(related, models.ManyToManyRel) and not related.symmetrical:
                yield (name, related)


def get_local_fields(model):
    """
    Searches through the model's ancestry and gets the fields on the models
    Returns a dict of {field.name: field}
    """
    model_ancestry = _get_model_ancestry(model)

    local_fields_dict = {}
    for _model in model_ancestry:
        for field in sorted(
            list(_model._meta.fields) + list(_model._meta.local_many_to_many)
        ):
            if field.name not in local_fields_dict:
                local_fields_dict[field.name] = field

    return list(local_fields_dict.items())


def maybe_queryset(value):
    if isinstance(value, Manager):
        value = value.get_queryset()
    return value


def get_model_fields(model):
    """
    Gets all the fields and relationships on the Django model and its ancestry.
    Prioritizes local fields and relationships over the reverse relationships of the same name
    Returns a tuple of (field.name, field)
    """
    local_fields = get_local_fields(model)
    local_field_names = {field[0] for field in local_fields}
    reverse_fields = get_reverse_fields(model, local_field_names)
    all_fields = local_fields + list(reverse_fields)

    return all_fields


def is_valid_django_model(model):
    return inspect.isclass(model) and issubclass(model, models.Model)


def import_single_dispatch():
    from functools import singledispatch

    return singledispatch


def set_rollback():
    atomic_requests = connection.settings_dict.get("ATOMIC_REQUESTS", False)
    if atomic_requests and connection.in_atomic_block:
        transaction.set_rollback(True)


def bypass_get_queryset(resolver):
    """
    Adds a bypass_get_queryset attribute to the resolver, which is used to
    bypass any custom get_queryset method of the DjangoObjectType.
    """
    resolver._bypass_get_queryset = True
    return resolver


_DJANGO_VERSION_AT_LEAST_4_2 = django.VERSION[0] > 4 or (
    django.VERSION[0] >= 4 and django.VERSION[1] >= 2
)
