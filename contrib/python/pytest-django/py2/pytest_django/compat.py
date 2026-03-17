# This file cannot be imported from until Django sets up


def _get_setup_and_teardown_databases():
    try:
        # Django 3.2+ has added timing capabilities that we don't really support
        # right now. Unfortunately that new time_keeper is required.
        from django.test.utils import NullTimeKeeper
    except ImportError:
        pass
    else:
        from django.test.utils import setup_databases, teardown_databases

        def wrapped_setup_databases(*args, **kwargs):
            return setup_databases(*args, time_keeper=NullTimeKeeper(), **kwargs)

        return wrapped_setup_databases, teardown_databases

    try:
        # Django 1.11+
        from django.test.utils import setup_databases, teardown_databases  # noqa: F401, F811
    except ImportError:
        pass
    else:
        return setup_databases, teardown_databases

    # In Django prior to 1.11, teardown_databases is only available as a method on DiscoverRunner
    from django.test.runner import setup_databases, DiscoverRunner  # noqa: F401, F811

    def teardown_databases(db_cfg, verbosity):
        DiscoverRunner(verbosity=verbosity, interactive=False).teardown_databases(
            db_cfg
        )

    return setup_databases, teardown_databases


setup_databases, teardown_databases = _get_setup_and_teardown_databases()
del _get_setup_and_teardown_databases
