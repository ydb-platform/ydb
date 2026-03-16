from __future__ import unicode_literals

import django
from django.contrib.auth.management import _get_all_permissions

from .enums import DjangoVersion


def django_version():
    if django.get_version().startswith('1.8'):
        return DjangoVersion.DJANGO_18
    elif django.get_version().startswith('1.9'):
        return DjangoVersion.DJANGO_19
    elif django.get_version().startswith('1.10'):
        return DjangoVersion.DJANGO_110
    elif django.get_version().startswith('1.11'):
        return DjangoVersion.DJANGO_111
    elif django.get_version().startswith('2.0'):
        return DjangoVersion.DJANGO_20
    elif django.get_version().startswith('2.1'):
        return DjangoVersion.DJANGO_21


def get_model_name(model):
    if django_version() == DjangoVersion.DJANGO_18:
        return '%s.%s' % (model._meta.app_label, model._meta.object_name)

    return model._meta.label


def get_all_permissions(opts, ctype=None):
    if django_version() < DjangoVersion.DJANGO_110:
        return _get_all_permissions(opts, ctype)

    return _get_all_permissions(opts)
