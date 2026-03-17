import ast
from collections.abc import Iterator
from contextlib import AbstractContextManager
from logfire import Logfire as Logfire
from logfire._internal.ast_utils import CallNodeFinder as CallNodeFinder, get_node_source_text as get_node_source_text
from logfire._internal.constants import ATTRIBUTES_MESSAGE_KEY as ATTRIBUTES_MESSAGE_KEY
from logfire._internal.scrubbing import MessageValueCleaner as MessageValueCleaner
from logfire._internal.utils import handle_internal_errors as handle_internal_errors
from typing import Any

FALLBACK_ATTRIBUTE_KEY: str

def instrument_print(logfire_instance: Logfire) -> AbstractContextManager[None]:
    """Instruments the built-in `print` function to send logs to **Logfire**.

    See Logfire.instrument_print for full documentation.
    """

class PrintCallNodeFinder(CallNodeFinder):
    def heuristic_main_nodes(self) -> Iterator[ast.AST]: ...
    def heuristic_call_node_filter(self, node: ast.Call) -> bool: ...
    def warn_inspect_arguments_middle(self): ...
    def get_magic_attributes(self, args: tuple[Any, ...], value_cleaner: MessageValueCleaner) -> tuple[dict[str, Any], list[str]]:
        """Inspects argument expression AST nodes.

        Returns:
            - An attributes dict mapping non-literal argument expressions sources to their runtime values.
            - A list of strings to construct the log message.
        """
