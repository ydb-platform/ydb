from logfire._internal.main import Logfire as Logfire
from logfire._internal.scrubbing import BaseScrubber as BaseScrubber
from logfire._internal.utils import handle_internal_errors as handle_internal_errors
from surrealdb.connections.async_template import AsyncTemplate
from surrealdb.connections.sync_template import SyncTemplate
from typing import Any

def get_all_surrealdb_classes() -> set[type]: ...
def instrument_surrealdb(obj: SyncTemplate | AsyncTemplate | type[SyncTemplate] | type[AsyncTemplate] | None, logfire_instance: Logfire): ...
@handle_internal_errors
def patch_method(obj: Any, method_name: str, logfire_instance: Logfire): ...
