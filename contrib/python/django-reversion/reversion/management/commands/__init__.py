from django.apps import apps
from django.conf import settings
from django.contrib import admin
from django.core.management.base import BaseCommand, CommandError
from reversion.revisions import is_registered


class BaseRevisionCommand(BaseCommand):

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "app_label",
            metavar="app_label",
            nargs="*",
            help="Optional app_label or app_label.model_name list.",
        )
        parser.add_argument(
            "--using",
            default=None,
            help="The database to query for revision data.",
        )
        parser.add_argument(
            "--model-db",
            default=None,
            help="The database to query for model data.",
        )

    def get_models(self, options):
        # Load admin classes.
        if "django.contrib.admin" in settings.INSTALLED_APPS:
            admin.autodiscover()
        # Get options.
        app_labels = options["app_label"]
        # Parse model classes.
        if len(app_labels) == 0:
            selected_models = apps.get_models()
        else:
            selected_models = set()
            for label in app_labels:
                if "." in label:
                    # This is an app.Model specifier.
                    try:
                        model = apps.get_model(label)
                    except LookupError:
                        raise CommandError(f"Unknown model: {label}")
                    selected_models.add(model)
                else:
                    # This is just an app - no model qualifier.
                    app_label = label
                    try:
                        app = apps.get_app_config(app_label)
                    except LookupError:
                        raise CommandError(f"Unknown app: {app_label}")
                    selected_models.update(app.get_models())
        for model in selected_models:
            if is_registered(model):
                yield model
