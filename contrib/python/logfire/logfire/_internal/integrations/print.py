from __future__ import annotations

import ast
import builtins
import functools
import inspect
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any

from logfire import Logfire
from logfire._internal.ast_utils import CallNodeFinder, get_node_source_text
from logfire._internal.constants import ATTRIBUTES_MESSAGE_KEY
from logfire._internal.scrubbing import MessageValueCleaner
from logfire._internal.utils import handle_internal_errors

# Attribute name used when we can't determine the actual argument expressions.
# The value will always be a tuple/list of one or more arguments passed to print().
# Except in the specific case where there's more than one `*args` passed to print()
# and at least one non-starred argument outside of them,
# the value will simply be *all* the arguments.
FALLBACK_ATTRIBUTE_KEY = 'logfire.print_args'


def instrument_print(logfire_instance: Logfire) -> AbstractContextManager[None]:
    """Instruments the built-in `print` function to send logs to **Logfire**.

    See Logfire.instrument_print for full documentation.
    """
    original_print = builtins.print
    logfire_instance = logfire_instance.with_settings(custom_scope_suffix='print')
    scrubber = logfire_instance.config.scrubber
    inspect_args = logfire_instance.config.inspect_arguments

    def _instrumented_print(*args: Any, sep: str | None = None, **kwargs: Any) -> None:
        """The wrapper function that will replace builtins.print."""
        original_print(*args, sep=sep, **kwargs)

        if not args:
            # Don't log empty print() calls.
            return

        # None and ' ' are equivalent for the real print, but we want an actual string now.
        if sep is None:
            sep = ' '

        with handle_internal_errors:
            value_cleaner = MessageValueCleaner(scrubber, check_keys=True)
            attributes: dict[str, Any]
            call_node = None
            node_finder = None

            if inspect_args:
                frame = inspect.currentframe()
                assert frame, 'Could not get current frame'
                frame = frame.f_back
                assert frame, 'Could not get caller frame'

                node_finder = PrintCallNodeFinder(frame)
                call_node = node_finder.node

            if call_node is None:
                attributes = {FALLBACK_ATTRIBUTE_KEY: args}
                message_parts = [value_cleaner.clean_value(FALLBACK_ATTRIBUTE_KEY, str(arg)) for arg in args]
            else:
                assert node_finder
                attributes, message_parts = node_finder.get_magic_attributes(args, value_cleaner)

            attributes[ATTRIBUTES_MESSAGE_KEY] = sep.join(message_parts)
            attributes.update(value_cleaner.extra_attrs())

            logfire_instance.log('info', 'print', attributes)

    builtins.print = _instrumented_print

    @contextmanager
    def uninstrument_context():
        # The user isn't required (or even expected) to use this context manager,
        # which is why the instrumenting and patching has already happened before this point.
        # It exists mostly for tests, and just in case users want it.
        try:
            yield
        finally:
            builtins.print = original_print

    return uninstrument_context()


@functools.lru_cache(maxsize=1024)
def _is_literal(node: ast.expr):
    try:
        ast.literal_eval(node)
        return True
    except Exception:
        return False


class PrintCallNodeFinder(CallNodeFinder):
    def heuristic_main_nodes(self) -> Iterator[ast.AST]:
        yield from self.ex.statements

    def heuristic_call_node_filter(self, node: ast.Call) -> bool:
        # The print call must have some positional arguments, otherwise we don't log anything.
        return bool(node.args)

    def warn_inspect_arguments_middle(self):
        return f'Using `{FALLBACK_ATTRIBUTE_KEY}` as the fallback attribute key for all print arguments.'

    def get_magic_attributes(
        self, args: tuple[Any, ...], value_cleaner: MessageValueCleaner
    ) -> tuple[dict[str, Any], list[str]]:
        """Inspects argument expression AST nodes.

        Returns:
            - An attributes dict mapping non-literal argument expressions sources to their runtime values.
            - A list of strings to construct the log message.
        """
        assert self.node
        attributes: dict[str, Any] = {}
        ast_args = list(self.node.args)
        runtime_args = list(args)

        def _process_end():
            """Helper to process non-starred args from the end of the lists."""
            message_parts: list[str] = []
            while ast_args and not isinstance(ast_args[-1], ast.Starred):
                node = ast_args.pop()
                value = runtime_args.pop()
                if _is_literal(node):
                    # Don't scrub this or use it as an attribute.
                    message_parts.append(value_cleaner.truncate(str(value)))
                else:
                    node_source = get_node_source_text(node, self.source)
                    attributes[node_source] = value
                    message_parts.append(value_cleaner.clean_value(node_source, str(value)))
            return message_parts

        # Starred args make it tricky to match up AST nodes to runtime values.

        # Ensure that everything is produced in the correct order.
        # Since _process_end works from the end of the lists backwards, reverse them first.
        # This is like going forward from the beginning.
        ast_args.reverse()
        runtime_args.reverse()
        message_parts_start = _process_end()

        if not runtime_args:
            # There were no starred args, so everything was processed and we can stop here.
            return attributes, message_parts_start

        # Process the non-starred args from the end of the original lists, so unreverse them.
        ast_args.reverse()
        runtime_args.reverse()
        message_parts_end = _process_end()
        # This was produced from the end backwards, so reverse it to go forwards.
        # message_parts_start is already in the correct order,
        # because _process_end was called on the reversed lists.
        message_parts_end.reverse()

        if len(ast_args) == 1:
            # There was only one starred arg, which both _process_end calls ran into.
            # Since there's only one, we can use its expression as the attribute key.
            assert isinstance(ast_args[0], ast.Starred)
            # .value leaves out the * operator.
            middle_key = get_node_source_text(ast_args[0].value, self.source)
        else:
            # There are multiple starred args, so we can't know what's what.
            middle_key = FALLBACK_ATTRIBUTE_KEY

        # runtime_args is whatever was left after removing the non-starred args from both ends.
        attributes[middle_key] = runtime_args
        message_parts_middle = [value_cleaner.clean_value(middle_key, str(arg)) for arg in runtime_args]

        full_message_parts = message_parts_start + message_parts_middle + message_parts_end
        return attributes, full_message_parts
