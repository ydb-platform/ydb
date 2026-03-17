from importlib import metadata

__version__ = metadata.version("django-simple-history")


def register(
    model,
    app=None,
    manager_name="history",
    records_class=None,
    table_name=None,
    **records_config,
):
    """
    Create historical model for `model` and attach history manager to `model`.

    Keyword arguments:
    app -- App to install historical model into (defaults to model.__module__)
    manager_name -- class attribute name to use for historical manager
    records_class -- class to use for history relation (defaults to
        HistoricalRecords)
    table_name -- Custom name for history table (defaults to
        'APPNAME_historicalMODELNAME')

    This method should be used as an alternative to attaching an
    `HistoricalManager` instance directly to `model`.
    """
    from . import models

    if records_class is None:
        records_class = models.HistoricalRecords

    records = records_class(**records_config)
    records.manager_name = manager_name
    records.table_name = table_name
    records.module = app and ("%s.models" % app) or model.__module__
    records.cls = model
    records.add_extra_methods(model)
    records.finalize(model)
