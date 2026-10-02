from sqlalchemy import Table

_DICTIONARY_KWARGS = ("source", "layout", "lifetime", "primary_key")


def _pop_dictionary_kwargs(kwargs):
    """Pop Dictionary-specific kwargs before Table validates them."""
    return {k: kwargs.pop(k, None) for k in _DICTIONARY_KWARGS}


def _apply_dictionary_metadata(table, popped):
    """Set dialect-prefixed kwargs on the table after construction."""
    table.kwargs["clickhouse_table_type"] = "dictionary"
    if popped.get("source") is not None:
        table.source = popped["source"]
        table.kwargs["clickhouse_dictionary_source"] = popped["source"]
    if popped.get("layout") is not None:
        table.layout = popped["layout"]
        table.kwargs["clickhouse_dictionary_layout"] = popped["layout"]
    if popped.get("lifetime") is not None:
        table.lifetime = popped["lifetime"]
        table.kwargs["clickhouse_dictionary_lifetime"] = popped["lifetime"]
    if popped.get("primary_key") is not None:
        table.primary_key_def = popped["primary_key"]
        table.kwargs["clickhouse_dictionary_primary_key"] = popped["primary_key"]


class Dictionary(Table):
    """
    Represents a ClickHouse Dictionary.

    Inherits from Table so it can be attached to metadata and have columns.

    Custom kwargs must be intercepted before Table's dialect-kwarg validation
    runs. The interception point differs between SQLAlchemy versions:

      - SQA 1.4: Table.__new__ calls _init() directly, bypassing __init__
      - SQA 2.x: Table.__new__ calls __init__() directly (no _init)

    We override both to handle either path.
    """

    __visit_name__ = "dictionary"

    def __init__(self, name, metadata, *args, **kwargs):
        popped = _pop_dictionary_kwargs(kwargs)
        super().__init__(name, metadata, *args, **kwargs)
        _apply_dictionary_metadata(self, popped)

    def _init(self, name, metadata, *args, **kwargs):
        popped = _pop_dictionary_kwargs(kwargs)
        super()._init(name, metadata, *args, **kwargs)
        _apply_dictionary_metadata(self, popped)
