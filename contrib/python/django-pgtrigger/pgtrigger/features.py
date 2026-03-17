from django.conf import settings


def model_meta():
    """
    True if model meta support is enabled
    """
    return getattr(settings, "PGTRIGGER_MODEL_META", True)


def schema_editor():
    """
    True if we are using the patched Postgres schema editor.

    Note that setting this to False means that we cannot easily
    alter columns of models that are associated with trigger
    conditions
    """
    return getattr(settings, "PGTRIGGER_SCHEMA_EDITOR", True)


def migrations():
    """
    True if migrations are enabled
    """
    return model_meta() and getattr(settings, "PGTRIGGER_MIGRATIONS", True)


def install_on_migrate():
    """
    True if triggers should be installed after migrations
    """
    return getattr(settings, "PGTRIGGER_INSTALL_ON_MIGRATE", False)


def schema():
    """
    The default schema where special objects are installed
    """
    return getattr(settings, "PGTRIGGER_SCHEMA", "public")


def prune_on_install():
    """
    True if triggers should be pruned on a full install or uninstall
    """
    return getattr(settings, "PGTRIGGER_PRUNE_ON_INSTALL", True)
