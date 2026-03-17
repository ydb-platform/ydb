from django.apps import AppConfig
from django.conf import settings
from django.db.models.signals import post_migrate

from . import monkey_patch_group, monkey_patch_user


class GuardianConfig(AppConfig):
    name = "guardian"
    default_auto_field = "django.db.models.AutoField"

    def ready(self):
        from .shortcuts import clear_ct_cache

        post_migrate.connect(clear_ct_cache)
        if settings.GUARDIAN_MONKEY_PATCH_GROUP:
            monkey_patch_group()
        if settings.GUARDIAN_MONKEY_PATCH_USER:
            monkey_patch_user()
