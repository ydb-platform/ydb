from django.apps import AppConfig
from django.core import checks
from typing_extensions import final

from django_test_migrations.checks import autonames, database_configuration


@final
class AutoNames(AppConfig):
    """
    Class to install this check into ``INSTALLED_APPS`` in ``django``.

    If you have migrations that cannot be renamed,
    use ``DTM_IGNORED_MIGRATIONS`` setting in ``django.conf``
    to ignore ones you have to deal with:

    .. code:: python

        # settings.py
        DTM_IGNORED_MIGRATIONS = {
            ('main_app', '0004_auto_20191119_2125'),
            ('dependency_app', '0001_auto_20201110_2100'),
        }

    To run checks use:

    .. code:: bash

        python manage.py check --deploy --fail-level WARNING

    It will return exit code ``1`` if any violations are found.
    This can be easily added into your CI.

    See:
        https://docs.djangoproject.com/en/3.0/ref/applications/
        https://twitter.com/AdamChainz/status/1231895529686208512

    """

    #: Part of Django API.
    name = autonames.CHECK_NAME

    def ready(self) -> None:
        """That's how we register our check when apps are ready."""
        for check in autonames.CHECKS:
            checks.register(check, checks.Tags.compatibility)


@final
class DatabaseConfiguration(AppConfig):
    """Class to install this check into ``INSTALLED_APPS`` in ``django``.

    Database configuration checks are made with aim to help/guide developers
    set the most appropriate values for some database settings according to
    best practices.
    Currently supported database settings:

    * statement timeout (timeout queries that execution take too long):
        * `postgresql` via `statement_timeout` - https://bit.ly/2ZFjaRM
        * `mysql` via `max_execution_time` - https://bit.ly/399TBvk

    See:
        https://github.com/wemake-services/wemake-django-template/issues/1064

    """

    #: Part of Django API.
    name = database_configuration.CHECK_NAME

    def ready(self) -> None:
        """Register database configuration checks."""
        for check in database_configuration.CHECKS:
            checks.register(check, checks.Tags.database)
