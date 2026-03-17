from __future__ import unicode_literals, absolute_import

from itertools import chain

from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import FieldDoesNotExist


def _get_field_by_name(model_class, field_name):
    field = model_class._meta.get_field(field_name)
    return (
        field,
        field.model,
        not field.auto_created or field.concrete,
        field.many_to_many
    )


def _get_remote_field(field):
    if hasattr(field, 'remote_field'):
        return field.remote_field
    elif hasattr(field, 'related'):
        return field.related
    else:
        return None


def _get_all_field_names(model):
    return list(set(chain.from_iterable(
        (field.name, field.attname) if hasattr(field, 'attname') else (field.name,)
        for field in model._meta.get_fields()
        if not (field.many_to_one and field.related_model is None)
    )))


def get_relation_fields_from_model(model_class):
    relation_fields = []
    all_fields_names = _get_all_field_names(model_class)
    for field_name in all_fields_names:
        field, model, direct, m2m = _get_field_by_name(model_class, field_name)
        if field_name[-3:] == '_id' and field_name[:-3] in all_fields_names:
            continue
        if m2m or not direct or _get_remote_field(field):
            field.field_name_override = field_name
            relation_fields += [field]
    return relation_fields


def get_direct_fields_from_model(model_class):
    direct_fields = []
    all_fields_names = _get_all_field_names(model_class)
    for field_name in all_fields_names:
        field, model, direct, m2m = _get_field_by_name(model_class, field_name)
        if direct and not m2m and not _get_remote_field(field):
            direct_fields += [field]
    return direct_fields


def get_model_from_path_string(root_model, path):
    for path_section in path.split('__'):
        if path_section:
            try:
                field, model, direct, m2m = _get_field_by_name(root_model, path_section)
            except FieldDoesNotExist:
                return root_model
            if direct:
                if _get_remote_field(field):
                    try:
                        root_model = _get_remote_field(field).parent_model()
                    except AttributeError:
                        root_model = _get_remote_field(field).model
            else:
                if hasattr(field, 'related_model'):
                    root_model = field.related_model
                else:
                    root_model = field.model
    return root_model


def get_fields(model_class, field_name='', path=''):
    fields = get_direct_fields_from_model(model_class)
    app_label = model_class._meta.app_label

    if field_name != '':
        field, model, direct, m2m = _get_field_by_name(model_class, field_name)

        path += field_name
        path += '__'
        if direct:
            try:
                new_model = _get_remote_field(field).parent_model
            except AttributeError:
                new_model = _get_remote_field(field).model
        else:
            new_model = field.related_model

        fields = get_direct_fields_from_model(new_model)

        app_label = new_model._meta.app_label

    return {
        'fields': fields,
        'path': path,
        'app_label': app_label,
    }


def get_related_fields(model_class, field_name, path=""):
    if field_name:
        field, model, direct, m2m = _get_field_by_name(model_class, field_name)
        if direct:
            try:
                new_model = _get_remote_field(field).parent_model()
            except AttributeError:
                new_model = _get_remote_field(field).model
        else:
            if hasattr(field, 'related_model'):
                new_model = field.related_model
            else:
                new_model = field.model()

        path += field_name
        path += '__'
    else:
        new_model = model_class

    new_fields = get_relation_fields_from_model(new_model)
    model_ct = ContentType.objects.get_for_model(new_model)

    return (new_fields, model_ct, path)
