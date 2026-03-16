import django.apps
import django.db.backends.postgresql.schema as postgresql_schema
from django.conf import settings
from django.core.management.commands import makemigrations, migrate
from django.db.migrations import state
from django.db.models import options
from django.db.models.signals import post_migrate
from django.db.utils import load_backend

from pgtrigger import core, features, installation, migrations

# Allow triggers to be specified in model Meta. Users can turn this
# off via settings if it causes issues. If turned off, migrations
# are also disabled
if features.model_meta():  # pragma: no branch
    if "triggers" not in options.DEFAULT_NAMES:  # pragma: no branch
        options.DEFAULT_NAMES = tuple(options.DEFAULT_NAMES) + ("triggers",)


def patch_migrations():
    """
    Patch the autodetector and model state detection if migrations are turned on
    """
    if features.migrations():  # pragma: no branch
        if "triggers" not in state.DEFAULT_NAMES:  # pragma: no branch
            state.DEFAULT_NAMES = tuple(state.DEFAULT_NAMES) + ("triggers",)

        if not issubclass(  # pragma: no branch
            makemigrations.MigrationAutodetector, migrations.MigrationAutodetectorMixin
        ):
            makemigrations.MigrationAutodetector = type(
                "MigrationAutodetector",
                (migrations.MigrationAutodetectorMixin, makemigrations.MigrationAutodetector),
                {},
            )

        if not issubclass(  # pragma: no branch
            migrate.MigrationAutodetector, migrations.MigrationAutodetectorMixin
        ):
            migrate.MigrationAutodetector = type(
                "MigrationAutodetector",
                (migrations.MigrationAutodetectorMixin, migrate.MigrationAutodetector),
                {},
            )

        if django.VERSION >= (5, 2):
            makemigrations.Command.autodetector = makemigrations.MigrationAutodetector
            migrate.Command.autodetector = makemigrations.MigrationAutodetector


def patch_schema_editor():
    """
    Patch the schema editor to allow for column types to be altered on
    trigger conditions
    """
    if features.schema_editor():  # pragma: no branch
        for config in settings.DATABASES.values():
            backend = load_backend(config["ENGINE"])
            schema_editor_class = backend.DatabaseWrapper.SchemaEditorClass

            if (
                schema_editor_class
                and issubclass(
                    schema_editor_class,
                    postgresql_schema.DatabaseSchemaEditor,
                )
                and not issubclass(schema_editor_class, migrations.DatabaseSchemaEditorMixin)
            ):
                backend.DatabaseWrapper.SchemaEditorClass = type(
                    "DatabaseSchemaEditor",
                    (migrations.DatabaseSchemaEditorMixin, schema_editor_class),
                    {},
                )


def register_triggers_from_meta():
    """
    Populate the trigger registry from model `Meta.triggers`
    """
    if features.model_meta():  # pragma: no branch
        for model in django.apps.apps.get_models():
            triggers = getattr(model._meta, "triggers", [])
            for trigger in triggers:
                if not isinstance(trigger, core.Trigger):  # pragma: no cover
                    raise TypeError(f"Triggers in {model} Meta must be pgtrigger.Trigger classes")

                trigger.register(model)


def install_on_migrate(using, **kwargs):
    if features.install_on_migrate():
        installation.install(database=using)


class PGTriggerConfig(django.apps.AppConfig):
    name = "pgtrigger"

    def ready(self):
        """
        Do all necessary patching, trigger setup, and signal handler configuration
        """
        patch_migrations()
        patch_schema_editor()
        register_triggers_from_meta()

        # Configure triggers to automatically be installed after migrations
        post_migrate.connect(install_on_migrate, sender=self)
