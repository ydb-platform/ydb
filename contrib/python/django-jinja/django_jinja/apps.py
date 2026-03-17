from django.apps import AppConfig

from django_jinja import base


class DjangoJinjaAppConfig(AppConfig):
    name = "django_jinja"
    verbose_name = "Django Jinja"

    def ready(self):
        base.patch_django_for_autoescape()
