from typing import TypedDict

class CommenterOptions(TypedDict, total=False):
    """The `commenter_options` parameter for `instrument_sqlalchemy`."""
    db_driver: bool
    db_framework: bool
    opentelemetry_values: bool
