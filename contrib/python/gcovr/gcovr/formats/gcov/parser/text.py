# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

"""
Handle parsing of the textual ``.gcov`` file format.

Other modules should only use the following items:
`parse_metadata()`, `parse_coverage()`

The behavior of this parser was informed by the following sources:

* the old GcovParser class
  <https://github.com/gcovr/gcovr/blob/e0b7afef00123b7b6ce4f487a1c4cc9fc60528bc/gcovr/gcov.py#L239>
* the *Invoking Gcov* section in the GCC manual (version 11)
  <https://gcc.gnu.org/onlinedocs/gcc-11.1.0/gcc/Invoking-Gcov.html>
* the ``gcov.c`` source code in GCC
  (especially for understanding the exact number format)
  <https://github.com/gcc-mirror/gcc/blob/releases/gcc-11.1.0/gcc/gcov.c>
"""
# pylint: disable=too-many-lines

import enum
import re
from typing import Any, Iterable, NamedTuple, Pattern

from ....data_model.coverage import FileCoverage, LineCoverage
from ....data_model.merging import FUNCTION_MAX_LINE_MERGE_OPTIONS, MergeOptions
from ....exceptions import SanityCheckError
from ....logging import LOGGER
from ....utils import get_md5_hexdigest

from .common import (
    SUSPICIOUS_COUNTER,
    check_hits,
)

UNKNOWN_FUNCTION_NAME_FORMAT_STRING = "<unknown function {}>"


def _line_pattern(pattern: str) -> Pattern[str]:
    """
    Compile a regex from a line pattern.

    A line pattern is a normal regex, except that the following placeholders
    will be replaced by pattern fragments:

    * ``VALUE`` -> matches gcov's ``format_gcov()`` output (percentage or
      human-readable)
    * ``INT`` -> matches an integer
    * the pattern is anchored at the start/end
    * space is replaced by ``[ ]+``
    """
    pattern = pattern.replace(" ", r" +")
    pattern = pattern.replace("INT", r"[0-9]+")
    pattern = pattern.replace("VALUE", r"(?:NAN %|-?[0-9.]+[%kMGTPEZY]?)")
    return re.compile("^" + pattern + "$")


_RE_FUNCTION_LINE = _line_pattern(
    r"function (.*?) called (INT) returned (VALUE) blocks executed (VALUE)"
)
_RE_BRANCH_LINE = _line_pattern(
    r"branch (INT) (?:taken (VALUE)|never executed)(?: \((\w+)\))?"
)
_RE_CALL_LINE = _line_pattern(r"call (INT) (?:returned (VALUE)|never executed)")
_RE_UNCONDITIONAL_LINE = _line_pattern(
    r"unconditional (INT) (?:taken (VALUE)|never executed)"
)
_RE_SOURCE_LINE = _line_pattern(r"(?: )?(VALUE[*]?|-|[#]{5}|[=]{5}):(?: )?(INT):(.*)")
_RE_BLOCK_LINE = _line_pattern(r"(?: )?(VALUE|[$]{5}|[%]{5}):(?: )?(INT)-block (INT)")


class _ExtraInfo(enum.Flag):
    """Additional info about lines, such as noncode or exception-only status."""

    NONE = 0
    NONCODE = enum.auto()
    EXCEPTION_ONLY = enum.auto()
    PARTIAL = enum.auto()

    def __repr__(self) -> str:
        return str(self).replace("_ExtraInfo.", "")


class _SourceLine(NamedTuple):
    """A gcov line with source code: ``HITS: LINENO:CODE``"""

    hits: int
    lineno: int
    source_code: str
    extra_info: _ExtraInfo


class _MetadataLine(NamedTuple):
    """A gcov line with metadata: ``-: 0:KEY:VALUE``"""

    key: str
    value: str | None


class _BlockLine(NamedTuple):
    """A gcov line with block data: ``HITS: LINENO-block BLOCKNO``"""

    hits: int
    lineno: int
    block_id: int
    extra_info: _ExtraInfo


class _CallLine(NamedTuple):
    """A gcov line with call data: ``call CALLNO returned RETURNED``"""

    callno: int
    returned: int


class _BranchLine(NamedTuple):
    """A gcov line with branch data: ``branch BRANCHNO taken HITS (ANNOTATION)``"""

    branchno: int
    hits: int
    annotation: str | None


class _UnconditionalLine(NamedTuple):
    """
    A gcov line with unconditional branch data: ``unconditional BRANCHNO taken HITS``
    """

    branchno: int
    hits: int


class _FunctionLine(NamedTuple):
    """
    A gcov line with function coverage data for the next line.

    ``function NAME called COUNT returned RETURNED blocks executed BLOCKS``
    """

    name: str
    call_count: int | None
    blocks_covered: float | None


class _FunctionSpecializationNameLine(NamedTuple):
    """A gcov line with the name of a specialization section: ``NAME:``"""

    name: str


class _FunctionSpecializationSeparatorLine(NamedTuple):
    """A gcov line that delimits function specializations (no fields)"""


# NamedTuples can't inherit from a common base,
# so we use a Union type as the _parse_line() return type.
#
# Why NamedTuples? Better type safety than tuples, but very low memory overhead.
_Line = (
    _SourceLine
    | _MetadataLine
    | _BlockLine
    | _CallLine
    | _BranchLine
    | _UnconditionalLine
    | _FunctionLine
    | _FunctionSpecializationNameLine
    | _FunctionSpecializationSeparatorLine
)


class UnknownLineType(Exception):
    """Used by `_parse_line()` to signal that no known line type matched."""

    def __init__(self, line: str) -> None:
        super().__init__(line)
        self.line = line


def parse_metadata(
    filename: str,
    lines: list[str],
    *,
    suspicious_hits_threshold: int = SUSPICIOUS_COUNTER,
    activate_trace_logging: bool = False,
) -> dict[str, str | None]:
    r"""
    Collect the header/metadata lines from a gcov file.

    Example:
    >>> parse_metadata("file", '''
    ...   -: 0:Foo:bar
    ...   -: 0:Key:123
    ... '''.splitlines())
    Traceback (most recent call last):
       ...
    RuntimeError: Missing key 'Source' in metadata. GCOV data was >>
      -: 0:Foo:bar
      -: 0:Key:123<< End of GCOV data
    >>> parse_metadata("file", '-: 0:Source: file \n -: 0:Foo: bar \n -: 0:Key: 123 '.splitlines())
    {'Source': 'file', 'Foo': 'bar', 'Key': '123'}
    >>> parse_metadata("file", '''
    ...   -: 0:Source:file
    ...   -: 0:Foo:bar
    ...   -: 0:Key
    ... '''.splitlines())
    {'Source': 'file', 'Foo': 'bar', 'Key': None}
    """
    collected = {}
    for line in lines:
        # empty lines shouldn't occur in reality, but are common in testing
        if not line:
            continue

        parsed_line = _parse_line(filename, line, suspicious_hits_threshold)

        if isinstance(parsed_line, _MetadataLine):
            key, value = parsed_line
            if activate_trace_logging:
                LOGGER.trace("Metadata: %s=%s", parsed_line.key, parsed_line.value)
            collected[key] = value
        else:
            break  # stop at the first line that is not metadata

    if "Source" not in collected:
        data = "\n".join(lines)
        raise RuntimeError(
            f"Missing key 'Source' in metadata. GCOV data was >>{data}<< End of GCOV data"
        )

    return collected


_LineWithError = tuple[str, Exception]


def parse_coverage(
    data_filename: str | set[tuple[str, ...]],
    lines: list[str],
    *,
    filename: str,
    ignore_parse_errors: set[str] | None,
    suspicious_hits_threshold: int = SUSPICIOUS_COUNTER,
    activate_trace_logging: bool = False,
    use_existing_files: bool = False,
) -> tuple[FileCoverage, list[str]]:
    """
    Extract coverage data from a gcov report.

    Logging:
    Parse problems are reported as warnings.
    Coverage exclusion decisions are reported as verbose messages.

    Arguments:
        lines: the lines of the file to be parsed (excluding newlines)
        filename: for error reports
        ignore_parse_errors: which errors should be converted to warnings

    Returns:
        tuple of the coverage data and the source code lines

    Raises:
        Any exceptions during parsing, unless ignore_parse_errors is set.
    """

    lines_with_errors = list[_LineWithError]()
    tokenized_lines = list[tuple[_Line, str]]()
    persistent_states = dict[str, Any]()
    for raw_line in lines:
        # empty lines shouldn't occur in reality, but are common in testing
        if not raw_line:
            continue

        try:
            parsed_line = _parse_line(
                filename,
                raw_line,
                suspicious_hits_threshold,
                ignore_parse_errors,
                persistent_states,
            )
            tokenized_lines.append((parsed_line, raw_line))
        except Exception as ex:  # pylint: disable=broad-except
            lines_with_errors.append((raw_line, ex))

    missing_function_lines = not any(
        isinstance(line, _FunctionLine) for line, _ in tokenized_lines
    )

    if (
        use_existing_files
        # Only check for missing function lines if we have any non-metadata lines.
        and any(not isinstance(line, _MetadataLine) for line, _ in tokenized_lines)
        and missing_function_lines
    ):
        files_for_message = (
            "\n   ".join(
                " -> ".join(file_tuple) for file_tuple in sorted(data_filename)
            )
            if isinstance(data_filename, set)
            else data_filename
        )
        LOGGER.warning(
            f"No function line found in gcov file:\n   {files_for_message}\n"
            "This may indicate that the file was generated without "
            "the proper gcov options (especially --branch-probabilities)?\n"
            "See <https://gcovr.com/en/stable/faq.html#which-options-are-used-for-calling-gcov>.",
        )

    if (
        "negative_hits.warn_once_per_file" in persistent_states
        and persistent_states["negative_hits.warn_once_per_file"] > 1
    ):
        LOGGER.warning(
            "Ignored %d negative hits overall.",
            persistent_states["negative_hits.warn_once_per_file"],
        )

    if (
        "suspicious_hits.warn_once_per_file" in persistent_states
        and persistent_states["suspicious_hits.warn_once_per_file"] > 1
    ):
        LOGGER.warning(
            "Ignored %d suspicious hits overall.",
            persistent_states["suspicious_hits.warn_once_per_file"],
        )

    filecov = FileCoverage(data_filename, filename=filename)
    state = _ParserState.new(
        function_name=UNKNOWN_FUNCTION_NAME_FORMAT_STRING,
    )
    for line, raw_line in tokenized_lines:
        try:
            if activate_trace_logging:
                LOGGER.trace("Processing line: %s", line)
            state = _gather_coverage_from_line(
                state,
                line,
                filecov=filecov,
                activate_trace_logging=activate_trace_logging,
            )
        except Exception as ex:  # pylint: disable=broad-except
            lines_with_errors.append((raw_line, ex))
            state = _ParserState.new(is_recovering=True)

    # Clean up the final state. This shouldn't happen,
    # but the last line could theoretically contain pending function lines
    for function in state.deferred_functions:
        name, count, blocks = function
        filecov.insert_function_coverage(
            str(data_filename),
            MergeOptions(func_opts=FUNCTION_MAX_LINE_MERGE_OPTIONS),
            mangled_name=name,
            demangled_name=None,
            lineno=(state.last_linecov.lineno + 1) if state.last_linecov else 0,
            count=count,
            blocks=blocks,
        )

    if state.unknown_function is not None:
        filecov.insert_function_coverage(
            str(data_filename),
            MergeOptions(func_opts=FUNCTION_MAX_LINE_MERGE_OPTIONS),
            mangled_name=state.unknown_function[0],
            demangled_name=None,
            lineno=state.unknown_function[1],
            count=None,
            blocks=None,
            excluded=True,
        )

    _report_lines_with_errors(
        lines_with_errors,
        filename=filename,
        ignore_parse_errors=ignore_parse_errors,
    )

    src_lines = _reconstruct_source_code(line for line, _ in tokenized_lines)

    return filecov, src_lines


def _reconstruct_source_code(tokens: Iterable[_Line]) -> list[str]:
    source_token_lines = [line for line in tokens if isinstance(line, _SourceLine)]

    src_lines = [""] * max((line.lineno for line in source_token_lines), default=0)
    for line in source_token_lines:
        src_lines[line.lineno - 1] = line.source_code

    return src_lines


class _ParserState(NamedTuple):
    """State information while parsing gcov lines.

    >>> state = _ParserState.new()
    >>> state.previous_state is None
    True
    >>> state = state._replace(linecov_list=["x"])
    >>> state.linecov_list == ["x"]
    True
    >>> new_state = state.save_state()
    >>> new_state == state
    False
    >>> new_state.previous_state == state
    True
    >>> new_state.linecov_list == ["x"]
    True
    >>> state.linecov_list == ["x"]
    True
    >>> state = new_state._replace(linecov_list=[])
    >>> state.linecov_list == []
    True
    >>> restored_state = state.restore_state()
    >>> restored_state.previous_state is None
    True
    >>> state.linecov_list == []
    True
    >>> restored_state.restore_state()
    Traceback (most recent call last):
    ...
    gcovr.exceptions.SanityCheckError: Previous_state of _ParserState is None.
    """

    deferred_functions: list[_FunctionLine] = []
    function_name: str | None = None
    function_specialization: bool = False
    last_linecov: LineCoverage | None = None
    linecov_list: list[LineCoverage] = []
    block_id: int | None = None
    is_recovering: bool = False
    previous_state: "_ParserState | None" = None
    unknown_function: tuple[str, int] | None = None

    @classmethod
    def new(cls, **kwargs: Any) -> "_ParserState":
        """Return a newly initialized parser state object."""
        # We need to overwrite the list values because the default values are shared
        # and changed.
        return (
            cls()
            ._replace(
                deferred_functions=[],
                linecov_list=[],
            )
            ._replace(**kwargs)
        )

    def save_state(self) -> "_ParserState":
        """Save the current parser state and return a new one with the current line coverage list."""
        return _ParserState.new(
            previous_state=self,
            linecov_list=self.linecov_list,
            unknown_function=self.unknown_function,
        )

    def restore_state(self) -> "_ParserState":
        """Restore the previous parser state with the current line coverage list."""
        if self.previous_state is None:
            raise SanityCheckError(f"Previous_state of {type(self).__name__} is None.")
        return self.previous_state._replace(
            linecov_list=self.linecov_list,
            unknown_function=self.unknown_function,
        )


def _gather_coverage_from_line(
    state: _ParserState,
    line: _Line,
    *,
    filecov: FileCoverage,
    activate_trace_logging: bool,
) -> _ParserState:
    """
    Interpret a Line, updating the FileCoverage, and transitioning ParserState.

    The function handles all possible Line variants, and dies otherwise:
    >>> _gather_coverage_from_line(_ParserState(), "illegal line type", filecov=..., activate_trace_logging=...)
    Traceback (most recent call last):
    AssertionError: Unexpected line type: 'illegal line type'
    """
    # pylint: disable=too-many-return-statements,too-many-branches
    # pylint: disable=no-else-return  # make life easier for type checkers
    if isinstance(line, _SourceLine):
        raw_count, lineno, source_code, extra_info = line

        # handle deferred functions
        if state.deferred_functions:
            for function in state.deferred_functions:
                name, count, blocks = function
                filecov.insert_function_coverage(
                    filecov.data_sources,
                    MergeOptions(func_opts=FUNCTION_MAX_LINE_MERGE_OPTIONS),
                    mangled_name=name,
                    demangled_name=None,
                    lineno=lineno,
                    count=count,
                    blocks=blocks,
                )
            state.deferred_functions.clear()

        # If a specialized function is following a normal one the overall lines were added
        # already to the previous function and we need to remove them again.
        if (
            state.function_specialization
            and state.linecov_list
            and state.linecov_list[0].lineno <= lineno <= state.linecov_list[-1].lineno
        ):
            to_remove = [
                linecov for linecov in state.linecov_list if linecov.lineno >= lineno
            ]

            for linecov in to_remove:
                if activate_trace_logging:
                    LOGGER.trace(
                        "%s: Removing line coverage of function %s.",
                        linecov.location,
                        linecov.report_function_name,
                    )
                filecov.remove_line_coverage(linecov)

            if state.unknown_function == (
                to_remove[0].function_name,
                to_remove[0].lineno,
            ):
                state = state._replace(unknown_function=None)

            state.linecov_list.clear()

        if not extra_info & _ExtraInfo.NONCODE:
            # Add line number to unknown function name and insert the function coverage object.
            if state.function_name == UNKNOWN_FUNCTION_NAME_FORMAT_STRING:
                unknown_function_name = UNKNOWN_FUNCTION_NAME_FORMAT_STRING.format(
                    lineno
                )
                state = state._replace(
                    function_name=unknown_function_name,
                    unknown_function=(unknown_function_name, lineno),
                )

            linecov = filecov.insert_line_coverage(
                filecov.data_sources,
                lineno=lineno,
                count=raw_count,
                function_name=state.function_name,
                md5=get_md5_hexdigest(source_code.encode("UTF-8")),
            )
            state = state._replace(last_linecov=linecov)
            if not state.function_specialization:
                state.linecov_list.append(linecov)

        return state

    elif state.is_recovering:
        return state  # skip until the next _SourceLine

    elif isinstance(line, _FunctionLine):
        # Defer handling of the function tag until the next source line.
        # This is important to get correct line number information.
        if state.deferred_functions and activate_trace_logging:
            LOGGER.trace(
                "Several function definitions for the next coverage lines, function %s will not contain line coverage.",
                state.deferred_functions[-1].name,
            )
        return state._replace(
            deferred_functions=[*state.deferred_functions, line],
            function_name=line.name,
        )

    elif isinstance(line, _FunctionSpecializationNameLine):
        # Now we know that we are in a specialization and not at the end of it.
        return state.save_state()._replace(
            function_specialization=True,
        )

    elif isinstance(line, _FunctionSpecializationSeparatorLine):
        # If there was a specialization active we need to restore the previous state, else we do nothing.
        if state.function_specialization:
            state = state.restore_state()

        return state

    elif isinstance(line, _BranchLine):
        branchno, hits, annotation = line

        if state.last_linecov is not None:
            state.last_linecov.insert_branch_coverage(
                filecov.data_sources,
                branchno=branchno,
                count=hits,
                source_block_id=state.block_id,
                fallthrough=(annotation == "fallthrough"),
                throw=(annotation == "throw"),
            )

        return state

    # ignore unused line types, such as specialization sections
    elif isinstance(line, _CallLine):
        callno, returned = line

        # linecov won't exist if it was considered noncode
        if state.last_linecov is not None:
            state.last_linecov.insert_call_coverage(
                filecov.data_sources,
                callno=callno,
                source_block_id=state.block_id,  # type: ignore [arg-type]
                destination_block_id=None,
                returned=returned,
            )

        return state

    elif isinstance(line, _BlockLine):
        return state._replace(block_id=line.block_id)

    # ignore metadata in this phase
    elif isinstance(line, _MetadataLine):
        return state

    elif isinstance(line, (_UnconditionalLine,)):
        return state

    raise AssertionError(f"Unexpected line type: {line!r}")


def _report_lines_with_errors(
    lines_with_errors: list[_LineWithError],
    *,
    filename: str,
    ignore_parse_errors: set[str] | None,
) -> None:
    """Log warnings and potentially re-throw exceptions"""

    if not lines_with_errors:
        return

    lines = [line for line, _ in lines_with_errors]
    errors = [error for _, error in lines_with_errors]

    lines_output = "\n\t  ".join(lines)
    LOGGER.warning(
        "Unrecognized GCOV output for %s\n\t  %s\n"
        "\tThis is indicative of a gcov output parse error.\n"
        "\tPlease report this to the gcovr developers\n"
        "\tat <https://github.com/gcovr/gcovr/issues>.",
        filename,
        lines_output,
    )

    for ex in errors:
        LOGGER.warning("Exception during parsing:\n\t%s: %s", type(ex).__name__, ex)

    if ignore_parse_errors is not None and "all" in ignore_parse_errors:
        return

    LOGGER.error(
        "Exiting because of parse errors.\n"
        "\tYou can run gcovr with --gcov-ignore-parse-errors=...\n"
        "\tto continue anyway."
    )

    # if we caught an exception, re-raise it for the traceback
    raise errors[0]  # guaranteed to have at least one exception


def _parse_line(
    filename: str,
    line: str,
    suspicious_hits_threshold: int = SUSPICIOUS_COUNTER,
    ignore_parse_errors: set[str] | None = None,
    persistent_states: dict[str, Any] | None = None,
) -> _Line:
    """
    Categorize/parse individual lines without further processing.

    Example: can parse code line:
    >>> _parse_line("file", '     -: 13:struct Foo{};')
    _SourceLine(hits=0, lineno=13, source_code='struct Foo{};', extra_info=NONCODE)
    >>> _parse_line("file", '    12: 13:foo += 1;  ')
    _SourceLine(hits=12, lineno=13, source_code='foo += 1;  ', extra_info=NONE)
    >>> _parse_line("file", ' #####: 13:foo += 1;')
    _SourceLine(hits=0, lineno=13, source_code='foo += 1;', extra_info=NONE)
    >>> _parse_line("file", ' #####:10000:foo += 1;')  # see https://github.com/gcovr/gcovr/issues/882
    _SourceLine(hits=0, lineno=10000, source_code='foo += 1;', extra_info=NONE)
    >>> _parse_line("file", ' =====: 13:foo += 1;')
    _SourceLine(hits=0, lineno=13, source_code='foo += 1;', extra_info=EXCEPTION_ONLY)
    >>> _parse_line("file", '   12*: 13:cond ? f() : g();')
    _SourceLine(hits=12, lineno=13, source_code='cond ? f() : g();', extra_info=PARTIAL)
    >>> _parse_line("file", ' 1.7k*: 13:foo();')
    _SourceLine(hits=1700, lineno=13, source_code='foo();', extra_info=PARTIAL)

    Example: can parse metadata line:
    >>> _parse_line("file", '  -: 0:Foo:bar baz')
    _MetadataLine(key='Foo', value='bar baz')
    >>> _parse_line("file", '  -: 0:Some key:2')  # coerce numbers
    _MetadataLine(key='Some key', value='2')

    Example: can parse branch tags:
    >>> _parse_line("file", 'branch 3 taken 15%')
    _BranchLine(branchno=3, hits=1, annotation=None)
    >>> _parse_line("file", 'branch 3 taken 0%')
    _BranchLine(branchno=3, hits=0, annotation=None)
    >>> _parse_line("file", 'branch 3 taken 123')
    _BranchLine(branchno=3, hits=123, annotation=None)
    >>> _parse_line("file", 'branch 3 taken -1', ignore_parse_errors=("negative_hits.warn",))
    _BranchLine(branchno=3, hits=0, annotation=None)
    >>> _parse_line("file", 'branch 3 taken 4294967296', ignore_parse_errors=("suspicious_hits.warn",))
    _BranchLine(branchno=3, hits=0, annotation=None)
    >>> _parse_line("file", 'branch 7 taken 3% (fallthrough)')
    _BranchLine(branchno=7, hits=1, annotation='fallthrough')
    >>> _parse_line("file", 'branch 17 taken 99% (throw)')
    _BranchLine(branchno=17, hits=1, annotation='throw')
    >>> _parse_line("file", 'branch  0 never executed')
    _BranchLine(branchno=0, hits=0, annotation=None)
    >>> _parse_line("file", 'branch  0 never executed (fallthrough)')
    _BranchLine(branchno=0, hits=0, annotation='fallthrough')
    >>> _parse_line("file", 'branch 2 with some unknown format')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType: branch 2 with some unknown format

    Example: can parse call tags:
    >>> _parse_line("file", 'call  0 never executed')
    _CallLine(callno=0, returned=0)
    >>> _parse_line("file", 'call  17 returned 50%')
    _CallLine(callno=17, returned=1)
    >>> _parse_line("file", 'call  17 returned 9')
    _CallLine(callno=17, returned=9)
    >>> _parse_line("file", 'call 2 with some unknown format')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType: call 2 with some unknown format

    Example: can parse unconditional branches
    >>> _parse_line("file", 'unconditional 1 taken 17')
    _UnconditionalLine(branchno=1, hits=17)
    >>> _parse_line("file", 'unconditional 2 taken -1', ignore_parse_errors=set(['negative_hits.warn']))
    _UnconditionalLine(branchno=2, hits=0)
    >>> _parse_line("file", 'unconditional 2 taken 4294967296', ignore_parse_errors=set(['suspicious_hits.warn']))
    _UnconditionalLine(branchno=2, hits=0)
    >>> _parse_line("file", 'unconditional 3 never executed')
    _UnconditionalLine(branchno=3, hits=0)
    >>> _parse_line("file", 'unconditional with some unknown format')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType: unconditional with some unknown format

    Example: can parse function tags:
    >>> _parse_line("file", 'function foo called 2 returned 1 blocks executed 85%')
    _FunctionLine(name='foo', call_count=2, blocks_covered=85.0)
    >>> _parse_line("file", 'function foo called 2 returned 50% blocks executed 85%')
    _FunctionLine(name='foo', call_count=2, blocks_covered=85.0)
    >>> _parse_line("file", 'function foo called 2 returned 100% blocks executed 85%')
    _FunctionLine(name='foo', call_count=2, blocks_covered=85.0)
    >>> _parse_line("file", 'function foo with some unknown format')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType: function foo with some unknown format

    Example: can parse template specialization markers:
    >>> _parse_line("file", '------------------')
    _FunctionSpecializationSeparatorLine()

    Example: can parse template specialization names:
    >>> _parse_line("file", 'Foo<bar>::baz():')
    _FunctionSpecializationNameLine(name='Foo<bar>::baz()')
    >>> _parse_line("file", ' foo:')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType:  foo:
    >>> _parse_line("file", ':')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType: :

    Example: can parse block line:
    >>> _parse_line("file", '     1: 32-block  0')
    _BlockLine(hits=1, lineno=32, block_id=0, extra_info=NONE)
    >>> _parse_line("file", ' %%%%%: 33-block  1')
    _BlockLine(hits=0, lineno=33, block_id=1, extra_info=NONE)
    >>> _parse_line("file", ' $$$$$: 33-block  1')
    _BlockLine(hits=0, lineno=33, block_id=1, extra_info=EXCEPTION_ONLY)
    >>> _parse_line("file", ' %%%%%:10000-block  0')  # see https://github.com/gcovr/gcovr/issues/882
    _BlockLine(hits=0, lineno=10000, block_id=0, extra_info=NONE)
    >>> _parse_line("file", '     -1: 32-block  0', ignore_parse_errors=set(['negative_hits.warn']))
    _BlockLine(hits=0, lineno=32, block_id=0, extra_info=NONE)
    >>> _parse_line("file", '     4294967296: 32-block  0', ignore_parse_errors=set(['suspicious_hits.warn']))
    _BlockLine(hits=0, lineno=32, block_id=0, extra_info=NONE)
    >>> _parse_line("file", '     1: 9-block with some unknown format')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType:      1: 9-block with some unknown format

    Example: will reject garbage:
    >>> _parse_line("file", 'nonexistent_tag foo bar')
    Traceback (most recent call last):
    gcovr.formats.gcov.parser.text.UnknownLineType: nonexistent_tag foo bar
    """
    # pylint: disable=too-many-branches
    if ignore_parse_errors is None:
        ignore_parse_errors = set()
    if persistent_states is None:
        persistent_states = {"location": (filename, 0)}

    tag = _parse_tag_line(
        line,
        suspicious_hits_threshold,
        ignore_parse_errors,
        persistent_states,
    )
    if tag is not None:
        return tag

    # Handle lines that are like source lines.
    # But this could also include metadata lines and block-coverage lines.

    # CODE
    #
    # Structure: "COUNT: LINENO:CODE"
    #
    # Examples:
    #     -: 13:struct Foo{};
    #    12: 13:foo += 1;
    # #####: 13:foo += 1;
    # =====: 13:foo += 1;
    #   12*: 13:cond ? bar() : baz();
    match = _RE_SOURCE_LINE.fullmatch(line)
    if match is not None:
        hits_str, lineno, source_code = match.groups()
        persistent_states.update(location=(filename, int(lineno)))

        # METADATA (key, value)
        if hits_str == "-" and lineno == "0":
            if ":" in source_code:
                key, value = source_code.split(":", 1)
                return _MetadataLine(key, value.strip())

            # Add a synthetic metadata with no value
            return _MetadataLine(source_code, None)

        if hits_str == "-":
            hits = 0
            extra_info = _ExtraInfo.NONCODE
        elif hits_str == "#####":
            hits = 0
            extra_info = _ExtraInfo.NONE
        elif hits_str == "=====":
            hits = 0
            extra_info = _ExtraInfo.EXCEPTION_ONLY
        elif hits_str.endswith("*"):
            hits = _int_from_gcov_unit(hits_str[:-1])
            extra_info = _ExtraInfo.PARTIAL
        else:
            hits = _int_from_gcov_unit(hits_str)
            extra_info = _ExtraInfo.NONE

        hits = check_hits(
            hits,
            line,
            ignore_parse_errors,
            suspicious_hits_threshold,
            persistent_states,
        )

        return _SourceLine(hits, int(lineno), source_code, extra_info)

    # BLOCK
    #
    # Structure: "COUNT: LINENO-block BLOCKNO"
    if "-block " in line:
        match = _RE_BLOCK_LINE.match(line)
        if match is not None:
            hits_str, lineno, block_id = match.groups()
            persistent_states.update(location=(filename, int(lineno)))

            if hits_str == "%%%%%":
                hits = 0
                extra_info = _ExtraInfo.NONE
            elif hits_str == "$$$$$":
                hits = 0
                extra_info = _ExtraInfo.EXCEPTION_ONLY
            else:
                hits = _int_from_gcov_unit(hits_str)
                extra_info = _ExtraInfo.NONE

            hits = check_hits(
                hits,
                line,
                ignore_parse_errors,
                suspicious_hits_threshold,
                persistent_states,
            )

            return _BlockLine(hits, int(lineno), int(block_id), extra_info)

    # SPECIALIZATION NAME
    #
    # Structure: a name starting in the first column, ending with a ":". It is
    # not safe to make further assumptions about the layout of the (demangled)
    # identifier. For example, Rust might produce "<X as Y>::foo::h12345".
    #
    # This line type is therefore checked LAST! The old parser might have been
    # more robust because it would only consider specialization names on the
    # line following a specialization marker.
    if len(line) > 2 and not line[0].isspace() and line.endswith(":"):
        return _FunctionSpecializationNameLine(line[:-1])

    raise UnknownLineType(line)


def _parse_tag_line(  # pylint: disable=too-many-return-statements
    line: str,
    suspicious_hits_threshold: int,
    ignore_parse_errors: set[str],
    persistent_states: dict[str, Any],
) -> _Line | None:
    """A tag line is any gcov line that starts in the first column."""

    # Tag lines never start with whitespace.
    #
    # In principle, specialization names are also like tag lines.
    # But they don't have a marker, so their detection is done last.
    if line.startswith(" "):
        return None

    # BRANCH
    #
    # Structure:
    # branch BRANCHNO never executed
    # branch BRANCHNO taken VALUE
    # branch BRANCHNO taken VALUE (ANNOTATION)
    if line.startswith("branch "):
        match = _RE_BRANCH_LINE.match(line)
        if match is not None:
            branch_id, taken_str, annotation = match.groups()
            hits = 0 if taken_str is None else _int_from_gcov_unit(taken_str)

            hits = check_hits(
                hits,
                line,
                ignore_parse_errors,
                suspicious_hits_threshold,
                persistent_states,
            )

            return _BranchLine(int(branch_id), hits, annotation)

    # CALL
    #
    # Structure (note whitespace after tag):
    # call  0 never executed
    # call  1 returned VALUE
    if line.startswith("call "):
        match = _RE_CALL_LINE.match(line)
        if match is not None:
            call_id, returned_str = match.groups()
            returned = 0 if returned_str is None else _int_from_gcov_unit(returned_str)
            return _CallLine(int(call_id), returned)

    # UNCONDITIONAL
    #
    # Structure:
    # unconditional NUM taken VALUE
    # unconditional NUM never executed
    if line.startswith("unconditional "):
        match = _RE_UNCONDITIONAL_LINE.match(line)
        if match is not None:
            branch_id, taken_str = match.groups()
            hits = 0 if taken_str is None else _int_from_gcov_unit(taken_str)

            hits = check_hits(
                hits,
                line,
                ignore_parse_errors,
                suspicious_hits_threshold,
                persistent_states,
            )

            return _UnconditionalLine(int(branch_id), hits)

    # FUNCTION
    #
    # Structure:
    # function NAME called VALUE returned VALUE blocks executed VALUE
    if line.startswith("function "):
        match = _RE_FUNCTION_LINE.match(line)
        if match is not None:
            name, count, _, blocks = match.groups()
            return _FunctionLine(
                name, _int_from_gcov_unit(count), _float_from_gcov_percent(blocks)
            )

    # Function separator line,
    # see https://github.com/gcc-mirror/gcc/blob/50bc9185c2821350f0b785d6e23a6e9dcde58466/gcc/gcov.c#L3100C23-L3100C41
    #
    # Structure: literally just lots of hyphens
    if line == "------------------":
        return _FunctionSpecializationSeparatorLine()

    return None


def _int_from_gcov_unit(formatted: str) -> int:
    """
    Try to reverse gcov's number formatting.

    Gcov's number formatting works like this:

    * if ``decimal_places >= 0``, format a percentage
      * the percentage is fudged so that 0% and 100% are only shown
        when that's the true value
    * otherwise, format a count
      * if human readable numbers are enabled,
        use SI units like ``1.7k`` instead of ``1693``

    Relevant gcov command line flags:

    * ``-c`` enables counts instead of percentages
    * ``-H`` enables human-readable numbers (SI units)

    Note that percentages destroy information: the original value can't be recovered,
    so we must map to zero/one.
    Of course, counts are not that useful either because we don't know the max value.

    Examples:
    >>> _int_from_gcov_unit('123')
    123
    >>> _int_from_gcov_unit('-1.2k')
    -1200
    >>> [_int_from_gcov_unit(value) for value in ('NAN %', '17.2%', '0%')]
    [0, 1, 0]
    >>> [_int_from_gcov_unit(value) for value in ('1.7k', '0.5G')]
    [1700, 500000000]
    """
    if formatted.endswith("%"):
        return 1 if float(formatted[:-1]) > 0 else 0

    units = "kMGTPEZY"
    for exponent, unit in enumerate(units, 1):
        if formatted.endswith(unit):
            return int(float(formatted[:-1]) * 1000**exponent)

    return int(formatted)


def _float_from_gcov_percent(formatted: str) -> float:
    """
    Transform percentage to float value

    Examples:
    >>> [_float_from_gcov_percent(value) for value in ('NAN %', '17.2%', '0%')]
    [nan, 17.2, 0.0]
    """

    if not formatted.endswith("%"):
        raise AssertionError(f"Number must end with %, got {formatted}")

    return float(formatted[:-1])
