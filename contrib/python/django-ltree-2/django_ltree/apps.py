from django.apps import AppConfig


class DjangoLtreeConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_ltree"

    def ready(self):
        from . import checks as checks
        from . import lookups as lookups
        from . import functions as functions
