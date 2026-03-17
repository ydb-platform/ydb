from __future__ import unicode_literals

import warnings

from django.apps import AppConfig
from django.apps import apps as global_apps
from django.conf import settings
from django.contrib import admin
from django.db.models.signals import post_migrate

from .admin import AdminViewPermissionAdminSite
from .enums import DjangoVersion
from .utils import django_version, get_model_name


def update_permissions(sender, app_config, verbosity, apps=global_apps,
                       **kwargs):
    settings_models = getattr(settings, 'ADMIN_VIEW_PERMISSION_MODELS', None)

    # TODO: Maybe look at the registry not in all models
    for app in apps.get_app_configs():
        for model in app.get_models():
            view_permission = 'view_%s' % model._meta.model_name
            if settings_models or (settings_models is not None and len(
                    settings_models) == 0):
                model_name = get_model_name(model)
                if model_name in settings_models and view_permission not in \
                        [perm[0] for perm in model._meta.permissions]:
                    model._meta.permissions += (
                        (view_permission,
                         'Can view %s' % model._meta.model_name),)
            else:
                if view_permission not in [perm[0] for perm in
                                           model._meta.permissions]:
                    model._meta.permissions += (
                        ('view_%s' % model._meta.model_name,
                         'Can view %s' % model._meta.model_name),)


class AdminViewPermissionConfig(AppConfig):
    name = 'admin_view_permission'

    def ready(self):
        if django_version() == DjangoVersion.DJANGO_21:
            # Disable silently the package for Django => 2.1. We don't override
            # admin_site neither the default ModelAdmin.
            warnings.warn(
                'The package `admin_view_permission is deprecated in '
                'Django 2.1. Django added this functionality into the core.',
                DeprecationWarning
            )
            return

        if not isinstance(admin.site, AdminViewPermissionAdminSite):
            admin.site = AdminViewPermissionAdminSite('admin')
            admin.sites.site = admin.site

        post_migrate.connect(update_permissions)
