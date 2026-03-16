from __future__ import annotations

import ast
import types
from collections.abc import Iterator
from functools import lru_cache
from string import Formatter
from types import CodeType
from typing import Any, Literal

import executing
from typing_extensions import NotRequired, TypedDict

import logfire

from .ast_utils import CallNodeFinder, get_node_source_text
from .scrubbing import NOOP_SCRUBBER, BaseScrubber, MessageValueCleaner
from .stack_info import warn_at_user_stacklevel
from .utils import log_internal_error


class LiteralChunk(TypedDict):
    t: Literal['lit']
    v: str


class ArgChunk(TypedDict):
    t: Literal['arg']
    v: str
    spec: NotRequired[str]


class ChunksFormatter(Formatter):
    def chunks(
        self,
        format_string: str,
        kwargs: dict[str, Any],
        *,
        scrubber: BaseScrubber,
        fstring_frame: types.FrameType | None = None,
    ) -> tuple[list[LiteralChunk | ArgChunk], dict[str, Any], str]:
        # Returns
        # 1. A list of chunks
        # 2. A dictionary of extra attributes to add to the span/log.
        #      These can come from evaluating values in f-strings,
        #      or from noting scrubbed values.
        # 3. The final message template, which may differ from `format_string` if it was an f-string.
        if fstring_frame:
            result = self._fstring_chunks(kwargs, scrubber, fstring_frame)
            if result:  # returns None if failed
                return result

        chunks, extra_attrs = self._vformat_chunks(
            format_string,
            kwargs=kwargs,
            scrubber=scrubber,
        )
        # When there's no f-string magic, there's no changes in the template string.
        return chunks, extra_attrs, format_string

    def _fstring_chunks(
        self,
        kwargs: dict[str, Any],
        scrubber: BaseScrubber,
        frame: types.FrameType,
    ) -> tuple[list[LiteralChunk | ArgChunk], dict[str, Any], str] | None:
        # `frame` is the frame of the method that's being called by the user,
        # so that we can tell if `logfire.log` is being called.
        called_code = frame.f_code
        frame = frame.f_back  # type: ignore
        # Now `frame` is the frame where the user called a logfire method.
        assert frame is not None

        node_finder = FormattingCallNodeFinder(frame)
        call_node = node_finder.node
        if call_node is None:
            return None

        if called_code == logfire.Logfire.log.__code__:
            # The `log` method is a bit different from the others:
            # the argument that might be the f-string is the second argument and it can be named.
            if len(call_node.args) >= 2:
                arg_node = call_node.args[1]
            else:
                # Find the arg named 'msg_template'
                for keyword in call_node.keywords:
                    if keyword.arg == 'msg_template':
                        arg_node = keyword.value
                        break
                else:
                    node_finder.warn_inspect_arguments("Couldn't identify the `msg_template` argument in the call.")
                    return None
        elif call_node.args:
            arg_node = call_node.args[0]
        else:
            # Very unlikely.
            node_finder.warn_inspect_arguments("Couldn't identify the `msg_template` argument in the call.")
            return None

        if not isinstance(arg_node, ast.JoinedStr):
            # Not an f-string, not a problem.
            # Just use normal formatting.
            return None

        # We have an f-string AST node.
        # Now prepare the namespaces that we will use to evaluate the components.
        global_vars = frame.f_globals
        local_vars = {**frame.f_locals, **kwargs}

        # Now for the actual formatting!
        result: list[LiteralChunk | ArgChunk] = []

        # We construct the message template (i.e. the span name) from the AST.
        # We don't use the source code of the f-string because that gets messy
        # if there's escaped quotes or implicit joining of adjacent strings.
        new_template = ''

        extra_attrs: dict[str, Any] = {}
        value_cleaner = MessageValueCleaner(scrubber, check_keys=False)
        for node_value in arg_node.values:
            if isinstance(node_value, ast.Constant):
                # These are the parts of the f-string not enclosed by `{}`, e.g. 'foo ' in f'foo {bar}'
                value: str = node_value.value  # type: ignore
                result.append({'v': value, 't': 'lit'})
                new_template += value
            else:
                # These are the parts of the f-string enclosed by `{}`, e.g. 'bar' in f'foo {bar}'
                assert isinstance(node_value, ast.FormattedValue)

                # This is cached.
                source, value_code, formatted_code = compile_formatted_value(node_value, node_finder.source)

                # Note that this doesn't include:
                # - The format spec, e.g. `:0.2f`
                # - The conversion, e.g. `!r`
                # - The '=' sign within the braces, e.g. `{bar=}`.
                #     The AST represents f'{bar = }' as f'bar = {bar}' which is how the template will look.
                new_template += '{' + source + '}'

                # The actual value of the expression.
                value = eval(value_code, global_vars, local_vars)
                extra_attrs[source] = value

                # Format the value according to the format spec, converting to a string.
                formatted = eval(formatted_code, global_vars, {**local_vars, '@fvalue': value})
                formatted = value_cleaner.clean_value(source, formatted)
                result.append({'v': formatted, 't': 'arg'})

        extra_attrs.update(value_cleaner.extra_attrs())
        return result, extra_attrs, new_template

    def _vformat_chunks(
        self,
        format_string: str,
        kwargs: dict[str, Any],
        *,
        scrubber: BaseScrubber,
        recursion_depth: int = 2,
    ) -> tuple[list[LiteralChunk | ArgChunk], dict[str, Any]]:
        """Copied from `string.Formatter._vformat` https://github.com/python/cpython/blob/v3.11.4/Lib/string.py#L198-L247 then altered."""
        if recursion_depth < 0:
            raise KnownFormattingError('Max format spec recursion exceeded')
        result: list[LiteralChunk | ArgChunk] = []
        # We currently don't use positional arguments
        args = ()
        value_cleaner = MessageValueCleaner(scrubber, check_keys=False)
        for literal_text, field_name, format_spec, conversion in self.parse(format_string):
            # output the literal text
            if literal_text:
                result.append({'v': literal_text, 't': 'lit'})

            # if there's a field, output it
            if field_name is not None:
                # this is some markup, find the object and do
                #  the formatting
                if field_name == '':
                    raise KnownFormattingError('Empty curly brackets `{}` are not allowed. A field name is required.')

                # ADDED BY US:
                if field_name.endswith('='):
                    if result and result[-1]['t'] == 'lit':
                        result[-1]['v'] += field_name
                    else:
                        result.append({'v': field_name, 't': 'lit'})
                    field_name = field_name[:-1]

                # given the field_name, find the object it references
                #  and the argument it came from
                try:
                    obj, _arg_used = self.get_field(field_name, args, kwargs)
                except IndexError:
                    raise KnownFormattingError('Numeric field names are not allowed.')
                except KeyError as exc1:
                    if str(exc1) == repr(field_name):
                        raise KnownFormattingError(f'The field {{{field_name}}} is not defined.') from exc1

                    try:
                        # field_name is something like 'a.b' or 'a[b]'
                        # Evaluating that expression failed, so now just try getting the whole thing from kwargs.
                        # In particular, OTEL attributes with dots in their names are normal and handled here.
                        obj = kwargs[field_name]
                    except KeyError as exc2:
                        # e.g. neither 'a' nor 'a.b' is defined
                        raise KnownFormattingError(f'The fields {exc1} and {exc2} are not defined.') from exc2
                except Exception as exc:
                    raise KnownFormattingError(f'Error getting field {{{field_name}}}: {exc}') from exc

                # do any conversion on the resulting object
                if conversion is not None:
                    try:
                        obj = self.convert_field(obj, conversion)
                    except Exception as exc:
                        raise KnownFormattingError(f'Error converting field {{{field_name}}}: {exc}') from exc

                # expand the format spec, if needed
                format_spec_chunks, _ = self._vformat_chunks(
                    format_spec or '', kwargs, scrubber=NOOP_SCRUBBER, recursion_depth=recursion_depth - 1
                )
                format_spec = ''.join(chunk['v'] for chunk in format_spec_chunks)

                try:
                    value = self.format_field(obj, format_spec)
                except Exception as exc:
                    raise KnownFormattingError(f'Error formatting field {{{field_name}}}: {exc}') from exc
                value = value_cleaner.clean_value(field_name, value)
                d: ArgChunk = {'v': value, 't': 'arg'}
                if format_spec:
                    d['spec'] = format_spec
                result.append(d)

        return result, value_cleaner.extra_attrs()


chunks_formatter = ChunksFormatter()


def logfire_format(format_string: str, kwargs: dict[str, Any], scrubber: BaseScrubber) -> str:
    result, _extra_attrs, _new_template = logfire_format_with_magic(
        format_string,
        kwargs,
        scrubber,
    )
    return result


def logfire_format_with_magic(
    format_string: str,
    kwargs: dict[str, Any],
    scrubber: BaseScrubber,
    fstring_frame: types.FrameType | None = None,
) -> tuple[str, dict[str, Any], str]:
    # Returns
    # 1. The formatted message.
    # 2. A dictionary of extra attributes to add to the span/log.
    #      These can come from evaluating values in f-strings.
    # 3. The final message template, which may differ from `format_string` if it was an f-string.
    try:
        chunks, extra_attrs, new_template = chunks_formatter.chunks(
            format_string,
            kwargs,
            scrubber=scrubber,
            fstring_frame=fstring_frame,
        )
        return ''.join(chunk['v'] for chunk in chunks), extra_attrs, new_template
    except KnownFormattingError as e:
        warn_formatting(str(e) or str(e.__cause__))
    except FStringAwaitError as e:
        warn_fstring_await(str(e))
    except Exception:
        # This is an unexpected error that likely indicates a bug in our logic.
        # Handle it here so that the span/log still gets created, just without a nice message.
        log_internal_error()

    # Formatting failed, so just use the original format string as the message.
    return format_string, {}, format_string


@lru_cache
def compile_formatted_value(node: ast.FormattedValue, ex_source: executing.Source) -> tuple[str, CodeType, CodeType]:
    """Returns three things that can be expensive to compute.

    1. Source code corresponding to the node value (excluding the format spec).
    2. A compiled code object which can be evaluated to calculate the value.
    3. Another code object which formats the value.
    """
    source = get_node_source_text(node.value, ex_source)

    # Check if the expression contains await before attempting to compile
    for sub_node in ast.walk(node.value):
        if isinstance(sub_node, ast.Await):
            raise FStringAwaitError(source)

    value_code = compile(source, '<fvalue1>', 'eval')
    expr = ast.Expression(
        ast.JoinedStr(
            values=[
                # Similar to the original FormattedValue node,
                # but replace the actual expression with a simple variable lookup
                # so that it the expression doesn't need to be evaluated again.
                # Use @ in the variable name so that it can't possibly conflict
                # with a normal variable.
                # The value of this variable will be provided in the eval() call
                # and will come from evaluating value_code above.
                ast.FormattedValue(
                    value=ast.Name(id='@fvalue', ctx=ast.Load()),
                    conversion=node.conversion,
                    format_spec=node.format_spec,
                )
            ]
        )
    )
    ast.fix_missing_locations(expr)
    formatted_code = compile(expr, '<fvalue2>', 'eval')
    return source, value_code, formatted_code


class KnownFormattingError(Exception):
    """An error raised when there's something wrong with a format string or the field values.

    In other words this should correspond to errors that would be raised when using `str.format`,
    and generally indicate a user error, most likely that they weren't trying to pass a template string at all.
    """


class FStringAwaitError(Exception):
    """An error raised when an await expression is found in an f-string.

    This is a specific case that can't be handled by f-string introspection and requires
    pre-evaluating the await expression before logging.
    """


class FormattingFailedWarning(UserWarning):
    pass


def warn_formatting(msg: str):
    warn_at_user_stacklevel(
        f'\n'
        f'    Ensure you are either:\n'
        '      (1) passing an f-string directly, with inspect_arguments enabled and working, or\n'
        '      (2) passing a literal `str.format`-style template, not a preformatted string.\n'
        '    See https://logfire.pydantic.dev/docs/guides/onboarding-checklist/add-manual-tracing/#messages-and-span-names.\n'
        f'    The problem was: {msg}',
        category=FormattingFailedWarning,
    )


def warn_fstring_await(msg: str):
    warn_at_user_stacklevel(
        f'\n'
        f'    Cannot evaluate await expression in f-string. Pre-evaluate the expression before logging.\n'
        '    For example, change:\n'
        '      logfire.info(f"{await get_value()}")\n'
        '    To:\n'
        '      value = await get_value()\n'
        '      logfire.info(f"{value}")\n'
        f'    The problematic f-string value was: {msg}',
        category=FormattingFailedWarning,
    )


class FormattingCallNodeFinder(CallNodeFinder):
    """Finds the call node corresponding to a call like `logfire.span` or `logfire.info`."""

    def heuristic_main_nodes(self) -> Iterator[ast.AST]:
        for statement in self.ex.statements:
            if isinstance(statement, ast.With):
                # Only look at the 'header' of a with statement, not its body.
                yield from statement.items
            else:
                yield statement

    def heuristic_call_node_filter(self, node: ast.Call) -> bool:
        # The call must have at least some arguments.
        return bool(node.args or node.keywords)

    def warn_inspect_arguments_middle(self):
        return 'Falling back to normal message formatting which may result in loss of information if using an f-string.'
