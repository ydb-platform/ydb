from __future__ import annotations

import inspect
import linecache
import re
import sys
import tokenize
from collections import namedtuple
from contextlib import suppress
from pathlib import Path
from secrets import token_urlsafe
from urllib.parse import quote

from . import trace_cpy
from .inspector import extract_variables
from .logging import logger
from .syntaxerror import clean_syntax_error_message, extract_enhanced_positions

# Position range: lines are 1-based inclusive, columns are 0-based exclusive
Range = namedtuple("Range", ["lfirst", "lfinal", "cbeg", "cend"])


def compute_cursor_position(
    mark_range: Range | None,
    em_ranges: Range | list[Range] | None,
    linenostart: int,
    common_indent: str = "",
) -> tuple[int, int]:
    """Compute the preferred cursor position from mark and emphasis ranges.

    Prefers the end of emphasis (em) ranges if available, as these mark the
    error position more precisely. Falls back to end of mark range, then
    to line 1, column 0.

    Args:
        mark_range: The marked region Range, or None
        em_ranges: The emphasis Range, list of Ranges, or None
        linenostart: The starting line number of the displayed code (for conversion)
        common_indent: The common indent string that was stripped (to restore original columns)

    Returns:
        Tuple of (line, column) where line is 1-based absolute line number
        and column is 0-based.
    """
    indent_len = len(common_indent)

    # Try emphasis ranges first (more precise error position)
    if em_ranges:
        if isinstance(em_ranges, list) and em_ranges:
            # Use the last em range's end position
            last_em = em_ranges[-1]
            # Convert from context-relative to absolute line number
            # lfinal is 1-based relative to displayed code, linenostart is absolute
            line = linenostart + last_em.lfinal - 1
            # cend is 0-based exclusive in dedented code, add indent for original
            col = last_em.cend + indent_len
            return (line, col)
        elif isinstance(em_ranges, Range):
            line = linenostart + em_ranges.lfinal - 1
            col = em_ranges.cend + indent_len
            return (line, col)

    # Fall back to mark range
    if mark_range:
        line = linenostart + mark_range.lfinal - 1
        col = mark_range.cend + indent_len
        return (line, col)

    # No range information available
    return (linenostart, 0)


# Will be set to an instance if loaded as an IPython extension by %load_ext
ipython = None

# Locations considered to be bug-free (library code, not user code), capture pretty suffix
libdir = re.compile(
    r".*(?:site-packages|dist-packages)/(.+)"
    r"|.*/lib/python\d+\.\d+/(.+)"
    r"|.*/bin/([^/]+)(?<!\.py)"  # CLI scripts
    r"|.*/\.cache/(.+)"
)

# Messages for exception chaining (oldest-first order)
# Suffix added to exception type when chained from a previous exception
chainmsg = {
    "cause": " from previous",
    "context": " in except",
    "none": "",
}

# Symbol descriptions for display in HTML and TTY outputs
symdesc = {
    "call": "Call",
    "warning": "Call from your code",
    "except": "Call from except",
    "error": "",
    "stop": "",
}

# Symbols for each frame relevance type
symbols = {"call": "➤", "warning": "⚠️", "error": "💣", "stop": "🛑", "except": "⚠️"}


def build_chain_header(chain: list[dict]) -> str:
    """Build a header message describing the exception chain."""
    if not chain:
        return ""

    # Chain is oldest-first: chain[0] is first exception, chain[-1] is last (uncaught)
    last_exc = chain[-1]

    # For ExceptionGroups, show the final exception types from subexceptions
    subexceptions = last_exc.get("subexceptions")
    if subexceptions:
        leaf_types = _collect_leaf_exception_types(subexceptions)
        if leaf_types:
            exc_type = " | ".join(leaf_types)
            # Don't say "Uncaught" for ExceptionGroups, just show the leaf types
            if len(chain) == 1:
                return f"⚠️  {exc_type}"
        else:
            exc_type = last_exc.get("type", "Exception")
    else:
        exc_type = last_exc.get("type", "Exception")

    if len(chain) == 1:
        return f"⚠️  Uncaught {exc_type}"

    # Build from last to first
    parts = [f"⚠️  {exc_type}"]

    # Add each previous exception with appropriate joiner
    for i in range(len(chain) - 2, -1, -1):
        exc = chain[i]
        next_exc = chain[i + 1]
        from_type = next_exc.get("from", "none")
        joiner = "from" if from_type == "cause" else "while handling"
        parts.append(f"{joiner} {exc.get('type', 'Exception')}")

    return " ".join(parts)


def _collect_leaf_exception_types(subexceptions: list[list[dict]]) -> list[str]:
    """Collect the final exception types from all subexception chains.

    For nested ExceptionGroups, recursively collects leaf exception types.
    Returns a flat list of exception type names.
    """
    leaf_types = []
    for sub_chain in subexceptions:
        if not sub_chain:
            continue
        # Get the last exception in this chain (the one that was raised)
        last_exc = sub_chain[-1]
        # Check if this is itself an ExceptionGroup with subexceptions
        nested_subs = last_exc.get("subexceptions")
        if nested_subs:
            # Recursively collect from nested ExceptionGroup
            leaf_types.extend(_collect_leaf_exception_types(nested_subs))
        else:
            # This is a leaf exception
            leaf_types.append(last_exc.get("type", "Exception"))
    return leaf_types


def extract_chain(exc=None, **kwargs) -> list:
    """Extract information on current exception.

    Returns a list of exception info dicts, ordered from oldest to newest
    (i.e., the original exception first, then any exceptions that occurred
    while handling it or were raised from it).
    """
    chain = []
    exc = exc or sys.exc_info()[1]
    while exc:
        chain.append(exc)
        exc = exc.__cause__ or None if exc.__suppress_context__ else exc.__context__
    # Reverse to get oldest first (chain is built newest-first)
    chain = list(reversed(chain))
    result = [extract_exception(e, **(kwargs if e is chain[-1] else {})) for e in chain]
    # Deduplicate variable inspectors: only keep variables for the last occurrence
    # of each (filename, function) pair across the entire chain
    _deduplicate_variables(result)
    return result


def _deduplicate_variables(chain: list) -> None:
    """Remove duplicate variables from inspectors, showing each only once.

    Variables are only shown if they appear in the frame's highlighted code
    (the lines indicated by the error range, expanded to include full
    comprehensions). If a variable appears in multiple frames' highlighted
    code (same filename/function), it's only shown in the last frame where
    it appears.
    """

    def _get_highlighted_lines(frame: dict) -> str:
        """Extract the highlighted lines from a frame based on its range.

        Expands to include full comprehension if error is inside one.
        """
        lines = frame.get("lines", "")
        range_obj = frame.get("range")
        if not range_obj or not lines:
            return lines  # Fall back to all lines if no range

        start = frame.get("linenostart", 1)
        lfirst, lfinal = range_obj.lfirst, range_obj.lfinal

        # Check if error is inside a comprehension - if so, return full comprehension
        comp_range = _find_comprehension_range(lines, lfirst, start)
        if comp_range is not None:
            # Error is inside a comprehension - return full lines (already trimmed to comprehension)
            return lines

        # No comprehension, return just the highlighted lines
        lines_list = lines.splitlines()

        # Convert to 0-based indices relative to displayed lines
        first_idx = lfirst - start
        final_idx = lfinal - start + 1

        if first_idx < 0 or first_idx >= len(lines_list):
            return lines  # Fall back if range is invalid

        return "\n".join(lines_list[first_idx:final_idx])

    def _variable_in_code(name: str, lines: str) -> bool:
        """Check if a variable name appears in the code as a word."""
        return bool(re.search(rf"\b{re.escape(name)}\b", lines))

    # First pass: collect frames by (filename, function) key
    # Maps key -> list of (exception_idx, frame_idx)
    frame_groups: dict[tuple, list[tuple[int, int]]] = {}
    for ei, exc in enumerate(chain):
        for fi, frame in enumerate(exc.get("frames", [])):
            if frame.get("relevance") == "call":
                continue
            key = (frame.get("filename"), frame.get("function"))
            if key not in frame_groups:
                frame_groups[key] = []
            frame_groups[key].append((ei, fi))

    # Second pass: for each group, determine which variables to show in each frame
    for _key, occurrences in frame_groups.items():
        # For each variable, find the LAST frame where it appears in highlighted code
        # variable_name -> (exception_idx, frame_idx) of last appearance in highlighted code
        last_appearance: dict[str, tuple[int, int]] = {}

        for ei, fi in occurrences:
            frame = chain[ei]["frames"][fi]
            highlighted = _get_highlighted_lines(frame)
            for v in frame.get("variables", []):  # pragma: no cover
                if v.name and _variable_in_code(v.name, highlighted):
                    # Update to this frame (later frames overwrite earlier)
                    last_appearance[v.name] = (ei, fi)

        # Now filter each frame's variables: keep only if this is the last appearance
        for ei, fi in occurrences:
            frame = chain[ei]["frames"][fi]
            frame["variables"] = [
                v
                for v in frame.get("variables", [])
                if v.name and last_appearance.get(v.name) == (ei, fi)
            ]


def _create_summary(message):
    """Extract the first line of the exception message as summary."""
    return message.split("\n", 1)[0]


def _set_relevances(frames: list, e: BaseException) -> None:
    """Set relevance for frames after extraction.

    - The last frame gets "error" (regular Exception) or "stop" (BaseException like KeyboardInterrupt)
    - ExceptionGroups also get "stop" since the interesting parts are in subexceptions
    - If the last frame is in library code, the last user code frame gets "warning"
    - All other frames remain "call"
    """
    if not frames:
        return

    # Last frame is where the exception occurred
    # ExceptionGroups get "stop" like BaseExceptions - the real errors are in subexceptions
    is_regular_exception = isinstance(e, Exception) and not _is_exception_group(e)
    frames[-1]["relevance"] = "error" if is_regular_exception else "stop"

    # Check if the last frame (error frame) is in user code
    last_filename = (
        frames[-1].get("original_filename") or frames[-1].get("filename") or ""
    )
    if _libdir_match(Path(last_filename).as_posix()) is None:
        return
    # Error is in library code - find the last user code frame to mark as warning
    for frame in reversed(frames[:-1]):  # Exclude the last frame  # pragma: no cover
        filename = frame.get("original_filename") or frame.get("filename") or ""
        if _libdir_match(Path(filename).as_posix()) is None:
            # This is user code - mark as warning (bug origin)
            frame["relevance"] = "warning"
            break


def extract_exception(e, *, skip_outmost=0, skip_until=None) -> dict:
    raw_tb = e.__traceback__
    try:
        tb = inspect.getinnerframes(raw_tb)
    except IndexError:  # Bug in inspect internals, find_source()
        logger.exception("Bug in inspect?")
        tb = []
        raw_tb = None

    # For SyntaxError, check if the error is in user code (notebook cell or matching skip_until)
    syntax_frame = None
    if isinstance(e, SyntaxError):
        syntax_frame = _extract_syntax_error_frame(e)
        if syntax_frame:
            # Check if this is a notebook cell (using IPython's filename map) or matches skip_until
            is_user_code = _is_notebook_cell(e.filename) or (
                skip_until and skip_until in (e.filename or "")
            )
            if is_user_code:
                skip_outmost = len(tb)  # Skip all frames

    if skip_until and skip_outmost == 0:
        for i, frame in enumerate(tb):
            if skip_until in frame.filename:
                skip_outmost = i
                break
    tb = tb[skip_outmost:]

    # Also skip the same number of frames from raw_tb
    if raw_tb and skip_outmost > 0:
        for _ in range(skip_outmost):
            if raw_tb:
                raw_tb = raw_tb.tb_next

    # Header and exception message
    message = getattr(e, "message", "") or str(e)
    # For SyntaxError, trim redundant location info from message
    if isinstance(e, SyntaxError):
        message = clean_syntax_error_message(message)
    summary = _create_summary(message)
    # Check if context is suppressed (raise X from None) - affects source trimming
    f = (
        "cause"
        if e.__cause__
        else "context"
        if e.__context__ and not e.__suppress_context__
        else "none"
    )
    try:
        frames = extract_frames(tb, raw_tb, except_block=(f != "none"), exc=e)
        # For SyntaxError, add the synthetic frame showing the problematic code
        if syntax_frame:
            # Demote the previous frame (compile, exec, etc.) to call only
            if frames and frames[-1]["relevance"] == "error":
                frames[-1]["relevance"] = "call"
            frames.append(syntax_frame)
    except Exception:
        logger.exception("Error extracting traceback")
        frames = None

    # Determine if this is a "stop" type exception (BaseException or ExceptionGroup)
    # These suppress inner library frames, showing only up to the last user code frame.
    # ExceptionGroups suppress because the interesting parts are in subexceptions.
    is_stop_type = not isinstance(e, Exception) or _is_exception_group(e)

    result = {
        "type": type(e).__name__,
        "message": message,
        "summary": summary,
        "from": f,
        "repr": repr(e),
        "frames": frames or [],
        "suppress_inner": is_stop_type,
    }

    # Extract subexceptions for ExceptionGroups (Python 3.11+)
    # These form parallel timelines within the group's traceback
    subexceptions = _extract_subexceptions(
        e, skip_outmost=skip_outmost, skip_until=skip_until
    )
    if subexceptions:
        result["subexceptions"] = subexceptions

    return result


def _extract_subexceptions(
    e, *, skip_outmost=0, skip_until=None
) -> list[list[dict]] | None:
    """Extract subexceptions from an ExceptionGroup.

    ExceptionGroups (Python 3.11+) contain multiple exceptions that occurred
    in parallel (e.g., in concurrent tasks). Each subexception forms its own
    traceback chain that ran in parallel with others.

    Args:
        e: The exception to check for subexceptions
        skip_outmost: Number of outermost frames to skip
        skip_until: Skip frames until this string is found in filename

    Returns:
        List of exception chains (each chain is a list of exception info dicts),
        or None if not an ExceptionGroup or has no subexceptions.
        Each chain represents a parallel timeline of exceptions.
    """
    # Check if this is an ExceptionGroup (Python 3.11+)
    # BaseExceptionGroup is the base class for both ExceptionGroup and BaseExceptionGroup
    if not hasattr(e, "exceptions") or not isinstance(
        getattr(e, "exceptions", None), (tuple, list)
    ):
        return None

    subexceptions = e.exceptions
    if not subexceptions:
        return None

    # Extract each subexception as its own chain
    # Each subexception may itself be an ExceptionGroup with nested subexceptions
    parallel_chains = []
    for sub_exc in subexceptions:
        # Recursively extract the chain for this subexception
        # This handles nested ExceptionGroups and exception chaining within each sub
        sub_chain = _extract_subexception_chain(
            sub_exc, skip_outmost=skip_outmost, skip_until=skip_until
        )
        if sub_chain:  # pragma: no cover
            parallel_chains.append(sub_chain)

    return parallel_chains if parallel_chains else None


def _extract_subexception_chain(exc, *, skip_outmost=0, skip_until=None) -> list[dict]:
    """Extract the full exception chain for a single subexception.

    Similar to extract_chain but for a subexception that may have its own
    __cause__ or __context__ chain.

    Args:
        exc: The subexception to extract
        skip_outmost: Number of outermost frames to skip
        skip_until: Skip frames until this string is found in filename

    Returns:
        List of exception info dicts, ordered from oldest to newest
    """
    chain = []
    current = exc
    while current:
        chain.append(current)
        current = (
            current.__cause__ or None
            if current.__suppress_context__
            else current.__context__
        )
    # Reverse to get oldest first
    chain = list(reversed(chain))

    # Extract info for each exception in the chain
    # Pass skip args only to the last one (the actual subexception)
    kwargs = {"skip_outmost": skip_outmost, "skip_until": skip_until}
    result = [extract_exception(e, **(kwargs if e is chain[-1] else {})) for e in chain]
    return result


def _is_notebook_cell(filename):
    """Check if the filename corresponds to a Jupyter notebook cell."""
    try:
        return filename in ipython.compile._filename_map  # type: ignore[attr-defined]
    except (AttributeError, KeyError, TypeError):
        return False


def _is_exception_group(e: BaseException) -> bool:
    """Check if exception is an ExceptionGroup (Python 3.11+)."""
    # Check for BaseExceptionGroup which is the base class for both
    # ExceptionGroup and BaseExceptionGroup
    return hasattr(e, "exceptions") and isinstance(
        getattr(e, "exceptions", None), (tuple, list)
    )


def _find_except_start_for_line(frame, lineno: int) -> int | None:
    """If lineno is inside an except handler, return the except line number.

    Uses AST analysis to find if the given line is within an except block.
    Returns the line number of the 'except' keyword for the innermost matching
    except handler, or None if not in an except block.
    """
    from .chain_analysis import (
        find_try_block_for_except_line,
        parse_source_for_try_except,
    )

    try:
        filename = frame.f_code.co_filename
        blocks = parse_source_for_try_except(filename)
        # Find the innermost except block containing this line
        block = find_try_block_for_except_line(blocks, lineno)
        if block:
            return block.except_start
    except Exception:  # pragma: no cover
        pass
    return None


def _get_source_lines_from_code(code, lineno: int, end_lineno: int | None = None):
    """Get source lines from a code object using Python 3.11+ linecache API.

    This provides a fallback for getting source code for interactive code
    (REPL, -c command, exec'd strings) where inspect.getsourcelines() fails.

    Args:
        code: The code object from a frame (frame.f_code)
        lineno: The line number where the error occurred (1-based)
        end_lineno: Optional end line number for multi-line errors

    Returns:
        (lines, start) tuple where lines is a list of source lines with
        newlines, or (None, None) if source cannot be retrieved.
    """
    # Python 3.13+ has linecache._getline_from_code for interactive code
    if not hasattr(linecache, "_getline_from_code"):
        return None, None  # pragma: no cover

    # First, check if we can get the error line at all
    error_line = linecache._getline_from_code(code, lineno)
    if not error_line:
        return None, None

    first_lineno = code.co_firstlineno
    is_module = code.co_name in (
        "<module>",
        "<listcomp>",
        "<dictcomp>",
        "<setcomp>",
        "<genexpr>",
    )

    # For module level, just get context around the error line
    if is_module:
        start = max(1, lineno - 10)
        final = (end_lineno or lineno) + 3
        lines = []
        actual_start = None
        for ln in range(start, final + 1):
            line = linecache._getline_from_code(code, ln)
            if line:
                if actual_start is None:
                    actual_start = ln
                lines.append(line)
            elif lines and ln > (end_lineno or lineno):  # pragma: no cover
                break  # Stop at empty lines after error (e.g., end of source)
        # Defensive: error_line check above guarantees we have lines
        if not lines or actual_start is None:  # pragma: no cover
            return None, None
        return lines, actual_start

    # For functions/methods, collect all lines starting from definition
    # then use inspect.getblock to find the function boundaries
    all_lines = []
    ln = first_lineno
    while True:
        line = linecache._getline_from_code(code, ln)
        if not line:
            break
        all_lines.append(line)
        ln += 1

    # Defensive: error_line check above guarantees we have lines
    if not all_lines:  # pragma: no cover
        return None, None

    # Use inspect.getblock to find the function's extent (same as inspect.getsourcelines)
    try:
        block_lines = inspect.getblock(all_lines)
    except (IndentationError, SyntaxError, tokenize.TokenError):  # pragma: no cover
        # Fallback: just use lines up to a reasonable extent
        block_lines = all_lines[: (end_lineno or lineno) - first_lineno + 3]

    return block_lines, first_lineno


def extract_source_lines(
    frame, lineno, end_lineno=None, *, notebook_cell=False, except_block=False
):
    try:
        lines, start = inspect.getsourcelines(frame)
        if start == 0:
            start = 1

        # Check if lineno is inside an except handler BEFORE trimming
        # This ensures we include the except line even for notebook cells
        # Skip this detection if context was suppressed (raise X from None)
        except_start = (
            _find_except_start_for_line(frame, lineno) if except_block else None
        )

        # For notebook cells, show only the error lines (no context)
        # For regular files, show 10 lines before and 2 lines after
        # Exception: if we're in an except block, ensure except line is included
        if notebook_cell:
            if except_start is not None and except_start >= start:
                # In except block: include from except line to lineno
                lines_before = lineno - except_start  # pragma: no cover
            else:
                lines_before = 0
            lines_after = (end_lineno - lineno) if end_lineno else 0
        else:
            lines_before = 10
            lines_after = (end_lineno - lineno + 2) if end_lineno else 2
        # Calculate slice bounds
        slice_start = max(0, lineno - start - lines_before)
        slice_end = max(0, lineno - start + lines_after + 1)

        # Skip forward if the slice would start inside a string or unclosed parens
        # Analyze all lines before slice_start to determine context state
        skip_to = _find_clean_start_line(lines, slice_start)
        if skip_to > slice_start:
            slice_start = skip_to

        lines = lines[slice_start:slice_end]
        start += slice_start

        # If lineno is inside an except handler, trim to start from the except line
        # (For non-notebook cells, this may still trim if lines_before > distance to except)
        if except_start is not None and except_start > start:
            skip = except_start - start
            if skip < len(lines):  # pragma: no branch
                lines = lines[skip:]
                start = except_start

        # Calculate error line position
        error_idx = lineno - start
        end_idx = (end_lineno - start) if end_lineno else error_idx

        # Safety check: ensure error_idx is valid
        if not lines or error_idx < 0 or error_idx >= len(lines):
            return "", lineno, ""

        # Get the indentation of the first marked line (error line) before any dedenting
        error_indent = 0
        error_line = lines[error_idx]
        error_indent = len(error_line) - len(error_line.lstrip(" \t"))

        # Trim leading lines that have more indentation than error line
        while lines and error_idx > 0:
            first_line = lines[0]
            if first_line.strip():
                first_indent = len(first_line) - len(first_line.lstrip(" \t"))
                if first_indent <= error_indent:
                    break  # This line has same or less indent, keep it
            start += 1
            lines.pop(0)
            error_idx -= 1
            end_idx -= 1

        # Trim trailing lines with less indentation than the error line
        # (hides external structures like else/except that aren't relevant)
        # But don't trim if we're inside unclosed brackets (e.g., list comprehension)
        trim_after = end_idx + 1
        bracket_depth = _count_bracket_depth("".join(lines[: end_idx + 1]))
        while trim_after < len(lines):
            line = lines[trim_after]
            # Keep lines if brackets are still open
            if bracket_depth > 0:
                bracket_depth += _count_bracket_depth(line)
                trim_after += 1
                continue
            # Keep empty lines, but check non-empty lines for indentation
            if line.strip():
                line_indent = len(line) - len(line.lstrip(" \t"))
                if line_indent < error_indent:
                    break  # Found a line with less indent, trim from here
            trim_after += 1
        lines = lines[:trim_after]

        # Calculate common indentation and dedent AFTER pruning
        common_indent = _calculate_common_indent(lines)
        lines = [ln.removeprefix(common_indent) for ln in lines]

        return "".join(lines), start, common_indent
    except OSError:
        # Fallback: try to get source from code object (Python 3.13+ interactive code)
        # This is tested via subprocess tests in test_tty.py::TestInteractiveSourceRetrieval
        code = frame.f_code if hasattr(frame, "f_code") else frame  # pragma: no cover
        fallback_lines, fallback_start = (
            _get_source_lines_from_code(  # pragma: no cover
                code, lineno, end_lineno
            )
        )
        if fallback_lines:  # pragma: no cover
            common_indent = _calculate_common_indent(fallback_lines)
            lines = [ln.removeprefix(common_indent) for ln in fallback_lines]
            return "".join(lines), fallback_start, common_indent
        return "", lineno, ""  # Source not available (non-Python module)


def _count_bracket_depth(text: str) -> int:
    """Count net bracket depth change in text, ignoring brackets in strings/comments.

    Returns positive for more opens than closes, negative for more closes.
    """
    depth = 0
    in_string = False
    string_char = None
    escape_next = False
    i = 0

    while i < len(text):
        char = text[i]

        if escape_next:
            escape_next = False
            i += 1
            continue

        if char == "\\":
            escape_next = True
            i += 1
            continue

        # Handle comments (outside strings)
        if not in_string and char == "#":
            break  # Rest of line is comment

        # Handle string boundaries
        if not in_string:
            # Check for triple-quoted strings
            if char in ('"', "'") and text[i : i + 3] in ('"""', "'''"):
                in_string = True
                string_char = text[i : i + 3]
                i += 3
                continue
            elif char in ('"', "'"):
                in_string = True
                string_char = char
        else:
            # Check for end of string
            if string_char in ('"""', "'''") and text[i : i + 3] == string_char:
                in_string = False
                string_char = None
                i += 3
                continue
            elif len(string_char) == 1 and char == string_char:
                in_string = False
                string_char = None

        # Count brackets only outside strings
        if not in_string:
            if char in "([{":
                depth += 1
            elif char in ")]}":
                depth -= 1

        i += 1

    return depth


def _find_clean_start_line(lines: list[str], target_idx: int) -> int:
    """Find the first line at or after target_idx that isn't inside an unclosed context.

    Analyzes lines[0:target_idx] to determine if target_idx would start inside:
    - A multi-line string (triple-quoted docstring, etc.)
    - An unclosed parenthesis/bracket/brace expression

    If so, scans forward from target_idx to find where that context closes,
    returning the index of the first "clean" line.

    Args:
        lines: List of source lines (with newlines)
        target_idx: The 0-based index we want to start displaying from

    Returns:
        Index >= target_idx of the first line not inside an unclosed context
    """
    if target_idx <= 0 or target_idx >= len(lines):
        return target_idx

    # Parse all lines before target to determine state at target_idx
    in_string = False
    string_char = None  # The quote char(s) that opened the string
    bracket_depth = 0

    for line in lines[:target_idx]:
        i = 0
        text = line
        escape_next = False

        while i < len(text):
            char = text[i]

            if escape_next:
                escape_next = False
                i += 1
                continue

            if char == "\\" and in_string:
                escape_next = True
                i += 1
                continue

            # Handle comments (outside strings)
            if not in_string and char == "#":
                break  # Rest of line is comment

            # Handle string boundaries
            if not in_string:
                # Check for triple-quoted strings first
                if char in ('"', "'") and text[i : i + 3] in ('"""', "'''"):
                    in_string = True
                    string_char = text[i : i + 3]
                    i += 3
                    continue
                elif char in ('"', "'"):
                    in_string = True
                    string_char = char
            else:
                # Check for end of string
                if string_char in ('"""', "'''") and text[i : i + 3] == string_char:
                    in_string = False
                    string_char = None
                    i += 3
                    continue
                elif len(string_char) == 1 and char == string_char:
                    in_string = False
                    string_char = None

            # Count brackets only outside strings
            if not in_string:
                if char in "([{":
                    bracket_depth += 1
                elif char in ")]}":
                    bracket_depth -= 1

            i += 1

    # If we're not in a bad context, target_idx is fine
    if not in_string and bracket_depth <= 0:
        return target_idx

    # Scan forward from target_idx until context closes
    # This is defensive code for rare edge cases (multiline strings/brackets at slice boundary)
    for idx in range(target_idx, len(lines)):  # pragma: no cover
        text = lines[idx]
        i = 0
        escape_next = False

        while i < len(text):
            char = text[i]

            if escape_next:
                escape_next = False
                i += 1
                continue

            if char == "\\" and in_string:
                escape_next = True
                i += 1
                continue

            # Handle comments (outside strings)
            if not in_string and char == "#":
                break

            # Handle string boundaries
            if not in_string:
                if char in ('"', "'") and text[i : i + 3] in ('"""', "'''"):
                    in_string = True
                    string_char = text[i : i + 3]
                    i += 3
                    continue
                elif char in ('"', "'"):
                    in_string = True
                    string_char = char
            else:
                if string_char in ('"""', "'''") and text[i : i + 3] == string_char:
                    in_string = False
                    string_char = None
                    i += 3
                    continue
                elif string_char and len(string_char) == 1 and char == string_char:
                    in_string = False
                    string_char = None

            if not in_string:
                if char in "([{":
                    bracket_depth += 1
                elif char in ")]}":
                    bracket_depth -= 1

            i += 1

        # After processing this line, check if we've exited the bad context
        if not in_string and bracket_depth <= 0:
            return idx + 1  # Start from the line AFTER the context closes

    # Couldn't find clean exit, fall back to target
    return target_idx  # pragma: no cover


def _get_full_source(frame, lineno=None):
    """Get the full source code for a frame using inspect.

    Returns (source, start_line) tuple. This works with any source Python
    knows about, including notebook cells and exec'd strings.

    Args:
        frame: The frame object or code object
        lineno: Optional line number hint for fallback source retrieval
    """
    try:
        lines, start = inspect.getsourcelines(frame)
        if start == 0:
            start = 1
        return "".join(lines), start
    except OSError:
        # Fallback: try to get source from code object (Python 3.13+ interactive code)
        # This is tested via subprocess tests in test_tty.py::TestInteractiveSourceRetrieval
        code = frame.f_code if hasattr(frame, "f_code") else frame  # pragma: no cover
        if lineno is None:  # pragma: no cover
            lineno = getattr(frame, "f_lineno", code.co_firstlineno)
        fallback_lines, fallback_start = _get_source_lines_from_code(
            code, lineno
        )  # pragma: no cover
        if fallback_lines:  # pragma: no cover
            return "".join(fallback_lines), fallback_start
        return None, None


def _libdir_match(path):
    """Check if path is in a library directory and return the short suffix if so."""
    m = libdir.fullmatch(path)
    if m:
        return next((g for g in m.groups() if g), "")
    return None


def format_location(filename, lineno, col=1):
    """Format location information for a frame.

    Args:
        filename: The source file path
        lineno: Line number (1-based)
        col: Column number (1-based, default 1)

    Returns:
        Tuple of (filename, location, urls) where:
        - filename: Possibly shortened file path
        - location: Display string for the location
        - urls: Dict of URL schemes to URLs (e.g., VS Code, Jupyter)
    """
    urls = {}
    location = None
    try:
        ipython_in = ipython.compile._filename_map[filename]  # type: ignore[attr-defined]
        location = f"In [{ipython_in}]"
        filename = None
    except (AttributeError, KeyError):
        pass
    if filename and Path(filename).is_file():
        fn = Path(filename).resolve()
        # vscode:// URLs use format vscode://file/path:line:col
        urls["VS Code"] = f"vscode://file{quote(fn.as_posix())}:{lineno}:{col}"
        cwd = Path.cwd()
        if cwd in fn.parents:
            fn = fn.relative_to(cwd)
            if ipython is not None:
                urls["Jupyter"] = f"/edit/{quote(fn.as_posix())}"
        filename = fn.as_posix()
    if not location and filename:
        # Use library short path if available, otherwise truncate long paths
        location = _libdir_match(filename)
        if location is None:
            split = (
                filename.rfind("/", 10, len(filename) - 20) + 1
                if len(filename) > 40
                else 0
            )
            location = filename[split:]
    # Ensure location is never None (fallback for edge cases)
    if not location:
        location = "<unknown>"
    return filename, location, urls


def _get_qualified_function_name(frame, function):
    """Get qualified function name with class prefix if available."""
    if function == "<module>":
        return None
    try:
        cls = next(
            v.__class__ if n == "self" else v
            for n, v in frame.f_locals.items()
            if n in ("self", "cls") and v is not None
        )
        function = f"{cls.__name__}.{function}"
    except StopIteration:
        pass
    return ".".join(function.split(".")[-2:])


def _extract_text_from_range(lines: str, mark_range) -> str | None:
    """Extract the text covered by a Range from source lines.

    Args:
        lines: The source code (may contain multiple lines)
        mark_range: Range object with lfirst, lfinal (1-based inclusive lines),
                   cbeg, cend (0-based exclusive columns), or None

    Returns:
        The extracted text, or None if mark_range is None.
    """
    if mark_range is None:
        return None

    lines_list = lines.splitlines(keepends=True)

    # Convert to 0-based line indices
    start_line_idx = mark_range.lfirst - 1
    end_line_idx = mark_range.lfinal - 1

    # Bounds check
    if start_line_idx < 0 or end_line_idx >= len(lines_list):
        return None

    extracted_parts = []
    for line_idx in range(start_line_idx, end_line_idx + 1):
        line = lines_list[line_idx].rstrip("\r\n")

        if line_idx == start_line_idx == end_line_idx:
            # Single line case
            extracted_parts.append(line[mark_range.cbeg : mark_range.cend])
        elif line_idx == start_line_idx:
            # First line of multi-line
            extracted_parts.append(line[mark_range.cbeg :])
        elif line_idx == end_line_idx:
            # Last line of multi-line
            extracted_parts.append(line[: mark_range.cend])
        else:
            # Middle lines of multi-line
            extracted_parts.append(line)

    return " ".join(extracted_parts)


def _expand_source_for_comprehension(
    lines: str, lineno: int, start: int
) -> str:  # pragma: no cover
    """Expand source to include full comprehension/generator expression if error is inside one.

    This helps show relevant variables like the iterator source (e.g., `data` in `for item in data`).
    Note: Currently unused but kept for future use.

    Args:
        lines: The source code snippet
        lineno: The 1-based line number where the error occurred
        start: The 1-based starting line number of the snippet

    Returns:
        Source code that includes the full comprehension, or original lines if not in one.
    """
    result = _find_comprehension_range(lines, lineno, start)
    if result:
        lines_list = lines.splitlines(keepends=True)
        comp_start, comp_end = result
        return "".join(lines_list[comp_start:comp_end])
    return lines


def _find_comprehension_range(lines: str, lineno: int, start: int):
    """Find the line range of a comprehension containing the error line.

    Args:
        lines: The source code snippet
        lineno: The 1-based line number where the error occurred
        start: The 1-based starting line number of the snippet

    Returns:
        Tuple of (start_idx, end_idx) as 0-based indices into lines_list,
        or None if error is not inside a comprehension.
    """
    import ast

    # Try to parse the source and find comprehensions containing the error line
    try:
        tree = ast.parse(lines)
    except SyntaxError:
        return None

    error_line_in_source = lineno - start + 1

    # Find comprehension nodes that contain the error line
    comprehension_types = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)
    for node in ast.walk(tree):
        if isinstance(
            node, comprehension_types
        ) and node.lineno <= error_line_in_source <= (node.end_lineno or node.lineno):
            comp_start = node.lineno - 1  # 0-based
            comp_end = node.end_lineno or node.lineno  # 1-based, inclusive
            return (comp_start, comp_end)

    return None


def _trim_source_to_comprehension(lines: str, lineno: int, start: int):
    """Trim source context to just the comprehension if error is inside one.

    Args:
        lines: The source code snippet
        lineno: The 1-based line number where the error occurred
        start: The 1-based starting line number of the snippet

    Returns:
        Tuple of (trimmed_lines, new_start) where new_start is adjusted line number,
        or (lines, start) if not inside a comprehension.
    """
    result = _find_comprehension_range(lines, lineno, start)
    if result:
        lines_list = lines.splitlines(keepends=True)
        comp_start_idx, comp_end_idx = result
        trimmed = "".join(lines_list[comp_start_idx:comp_end_idx])
        new_start = start + comp_start_idx
        return trimmed, new_start
    return lines, start


def _get_variable_source_for_comprehension(
    lines: str, lineno: int, start: int, mark_range
) -> str:
    """Get the source code to use for variable extraction, handling comprehensions.

    For comprehensions, includes the entire comprehension plus the marked region.
    This ensures external variables used anywhere in the comprehension are visible,
    even when the error occurs in a specific part (e.g., the filter clause).

    Comprehension loop variables (like 'x' in 'for x in data') won't be accessible
    in frame.f_locals anyway, so including them doesn't hurt - they'll just be
    filtered out during variable extraction.

    Args:
        lines: The source code snippet
        lineno: The 1-based line number where the error occurred
        start: The 1-based starting line number of the snippet
        mark_range: Range object with the marked region, or None

    Returns:
        Source code string for variable extraction.
    """
    # Check if we're inside a comprehension
    comp_range = _find_comprehension_range(lines, lineno, start)

    if comp_range is not None:
        # Inside a comprehension: use full comprehension text
        lines_list = lines.splitlines(keepends=True)
        comp_start_idx, comp_end_idx = comp_range
        return "".join(lines_list[comp_start_idx:comp_end_idx])

    # Not in a comprehension: use marked text or fall back to full lines
    marked_text = _extract_text_from_range(lines, mark_range)
    return marked_text or lines


def _extract_emphasis_columns(
    lines, error_line_in_context, end_line, start_col, end_col, start
):
    """Extract emphasis columns using caret anchors from the code segment.

    Returns Range with 1-based inclusive line numbers and 0-based exclusive columns,
    or None if no anchors found.
    """
    if not (end_line and start_col is not None and end_col is not None):
        return None

    all_lines = lines.splitlines(keepends=True)
    segment_start = error_line_in_context - 1  # Convert to 0-based for indexing
    segment_end = end_line if end_line else error_line_in_context

    if not (0 <= segment_start < len(all_lines) and segment_end <= len(all_lines)):
        return None

    # Extract the segment using CPython's approach
    relevant_lines = all_lines[segment_start:segment_end]
    if not relevant_lines:
        # This can happen when re-raising an existing exception where CPython's
        # position info refers to the original raise site but end_line < error_line
        return None

    segment = "".join(relevant_lines)

    # Trim segment using start_col and end_col
    segment = segment[start_col : len(segment) - (len(relevant_lines[-1]) - end_col)]
    # Attempt to parse for anchors
    anchors = None
    with suppress(Exception):
        anchors = trace_cpy._extract_caret_anchors_from_line_segment(segment)
    if not anchors:
        return None

    l0, l1, c0, c1 = (
        anchors.left_end_lineno,
        anchors.right_start_lineno,
        anchors.left_end_offset,
        anchors.right_start_offset,
    )
    # We get 0-based line numbers and offsets within the segment,
    # so we need to adjust them to match the original code.
    if l0 == 0:
        c0 += start_col
    if l1 == 0:
        c1 += start_col

    # Convert to 1-based inclusive line numbers for consistency
    lfirst = l0 + segment_start + 1
    lfinal = l1 + segment_start + 1

    return Range(lfirst, lfinal, c0, c1)


def _build_position_map(raw_tb):
    """Build mapping from frame objects to position tuples."""
    position_map = {}
    if not raw_tb:
        return position_map
    try:
        for frame_obj, positions in trace_cpy._walk_tb_with_full_positions(raw_tb):
            position_map[frame_obj] = positions
    except Exception:
        logger.exception("Error extracting position information")
    return position_map


def _extract_syntax_error_frame(e):
    """Create a synthetic frame dict for a SyntaxError showing the problematic code."""
    if not isinstance(e, SyntaxError):
        return None

    filename = e.filename
    lineno = e.lineno
    if not filename or not lineno:
        return None

    # SyntaxError attributes: filename, lineno, offset, text, end_lineno, end_offset
    end_lineno = getattr(e, "end_lineno", None) or lineno
    # offset is 1-based in SyntaxError, convert to 0-based for our Range
    start_col = (e.offset - 1) if e.offset else 0
    end_col = getattr(e, "end_offset", None)

    if end_col:
        end_col = end_col - 1  # Convert to 0-based
        # Ensure we have at least one character highlighted
        if end_col <= start_col and end_lineno == lineno:
            end_col = start_col + 1
    else:
        end_col = start_col + 1  # Default to single character

    assert start_col is not None and end_col is not None

    # Get source lines
    notebook_cell = _is_notebook_cell(filename)
    lines = None
    all_lines = None
    start = 1  # For SyntaxErrors, we want full source to show bracket matches etc.

    # Try to get source from the file or notebook
    try:
        import linecache

        # For notebook cells, try to get from IPython's cache
        if notebook_cell and ipython:
            try:
                cell_source = ipython.compile._filename_map.get(filename)
                if cell_source is not None:
                    # Get the cell content from the history
                    all_lines = linecache.getlines(filename)
                    if all_lines:
                        # For SyntaxErrors, get full source to enable bracket matching
                        lines = "".join(all_lines)
            except Exception:
                pass

        # Fallback: try linecache directly
        if not lines:
            all_lines = linecache.getlines(filename)
            if all_lines:
                # For SyntaxErrors, get full source to enable bracket matching
                lines = "".join(all_lines)

        # Last resort: use the text attribute from SyntaxError itself
        if not lines and e.text:
            lines = e.text if e.text.endswith("\n") else e.text + "\n"
            start = lineno
    except Exception:
        if e.text:
            lines = e.text if e.text.endswith("\n") else e.text + "\n"
            start = lineno

    if not lines:
        return None

    # Calculate error position within the displayed lines
    error_line_in_context = lineno - start + 1
    end_line = end_lineno - start + 1 if end_lineno else None

    # Calculate common indentation
    lines_list = lines.splitlines(keepends=True)
    common_indent = _calculate_common_indent(lines_list)

    # Try enhanced SyntaxError position extraction for better highlighting
    enhanced_mark, enhanced_em = extract_enhanced_positions(e, lines_list)

    if enhanced_mark:
        # Override lineno/end_lineno with the enhanced range (e.g., from opening bracket)
        lineno = enhanced_mark.lfirst
        end_lineno = enhanced_mark.lfinal

        # Trim source to start from the mark's first line
        lines_list = lines_list[lineno - 1 :]
        lines = "".join(lines_list)
        start = lineno
        common_indent = _calculate_common_indent(lines_list)

        error_line_in_context = 1  # Now lineno is the first line
        end_line = end_lineno - start + 1

        # Adjust enhanced ranges from absolute line numbers to context-relative
        mark_range = Range(
            1,
            enhanced_mark.lfinal - start + 1,
            max(0, enhanced_mark.cbeg - len(common_indent)),
            max(0, enhanced_mark.cend - len(common_indent)),
        )
        # Convert list of em ranges to context-relative
        em_ranges = (
            [
                Range(
                    em.lfirst - start + 1,
                    em.lfinal - start + 1,
                    max(0, em.cbeg - len(common_indent)),
                    max(0, em.cend - len(common_indent)),
                )
                for em in enhanced_em
            ]
            if enhanced_em
            else None
        )
    else:
        # Fallback to Python's positions
        # Adjust columns for dedenting
        adjusted_start_col = max(0, start_col - len(common_indent))
        adjusted_end_col = max(0, end_col - len(common_indent))

        # Create mark range
        mark_range = None
        mark_lfinal = end_line or error_line_in_context
        mark_range = Range(
            error_line_in_context, mark_lfinal, adjusted_start_col, adjusted_end_col
        )

        # Build emphasis range
        em_ranges = _extract_emphasis_columns(
            lines,
            error_line_in_context,
            end_line,
            adjusted_start_col,
            adjusted_end_col,
            start,
        )

    fragments = _parse_lines_to_fragments(lines, mark_range, em_ranges)

    # Compute cursor position (prefer em end, fall back to mark end)
    cursor_line, cursor_col = compute_cursor_position(
        mark_range, em_ranges, start, common_indent
    )

    # Format location info (after enhanced positions may have updated lineno)
    fmt_filename, location, urls = format_location(filename, cursor_line, cursor_col)

    # Get the code line for display
    codeline = lines_list[error_line_in_context - 1].strip() if lines_list else None

    return {
        "id": f"tb-{token_urlsafe(12)}",
        "relevance": "error",
        "filename": fmt_filename,
        "location": location,
        "notebook_cell": notebook_cell,
        "codeline": codeline,
        "range": Range(lineno, end_lineno or lineno, start_col, end_col)
        if start_col is not None
        else None,
        "cursor_line": cursor_line,
        "cursor_col": cursor_col,
        "linenostart": start,
        "lines": lines,
        "fragments": fragments,
        "function": None,
        "function_suffix": "",
        "urls": urls,
        "variables": [],
    }


def extract_frames(tb, raw_tb=None, *, except_block=False, exc=None) -> list:
    if not tb:
        return []

    position_map = _build_position_map(raw_tb)

    frames = []
    for frame, filename, lineno, function, codeline, _ in tb:
        hide = frame.f_globals.get("__tracebackhide__") or frame.f_locals.get(
            "__tracebackhide__"
        )
        if hide:
            if hide == "until":
                # Hide this frame and all previous frames
                frames = []
                continue
            # Mark frame as hidden but keep it for chain analysis
            # (will be filtered out after chronological ordering is built)
            hidden = True
        else:
            hidden = False

        # Relevance is set later in extract_exception via _set_frame_relevance
        relevance = "call"

        # Extract position information first so we can use it for source extraction
        pos = position_map.get(frame, [None] * 4)
        pos_end_lineno, start_col, end_col = pos[1], pos[2], pos[3]

        # Check if this is a notebook cell (to reduce context)
        notebook_cell = _is_notebook_cell(filename)

        lines, start, original_common_indent = extract_source_lines(
            frame,
            lineno,
            pos_end_lineno,
            notebook_cell=notebook_cell,
            except_block=except_block,
        )
        is_last_frame = frame is tb[-1][0]
        if not lines and not is_last_frame:
            if hidden:
                # Still include hidden frames with minimal info for chain analysis
                full_source, full_source_start = _get_full_source(frame)
                frames.append(
                    {
                        "id": f"tb-{token_urlsafe(12)}",
                        "relevance": relevance,
                        "hidden": True,
                        "lineno": lineno,
                        "full_source": full_source,
                        "full_source_start": full_source_start,
                    }
                )
            continue

        # Get full source for chain analysis (AST parsing for try-except matching)
        # This uses inspect which works with any source Python knows about
        full_source, full_source_start = _get_full_source(frame)

        # For comprehensions/generators, trim context to just the expression
        lines, start = _trim_source_to_comprehension(lines, lineno, start)
        # Recalculate common indent after trimming and dedent again if needed
        lines_list = lines.splitlines(keepends=True)
        extra_indent = _calculate_common_indent(lines_list)
        lines = "".join(ln.removeprefix(extra_indent) for ln in lines_list)
        # Total indent removed is original + any extra from trimming
        total_indent = len(original_common_indent) + len(extra_indent)

        # Preserve original filename for chain analysis (needed for AST parsing)
        original_filename = filename
        function = _get_qualified_function_name(frame, function)

        error_line_in_context = lineno - start + 1
        end_line = pos_end_lineno - start + 1 if pos_end_lineno else None

        # Adjust column positions to account for dedenting
        # Python's column numbers are based on the original indented code,
        # but we display dedented code, so we need to subtract total indentation removed
        adjusted_start_col = start_col - total_indent if start_col is not None else None
        adjusted_end_col = end_col - total_indent if end_col is not None else None

        # Create mark range (1-based inclusive lines, 0-based exclusive columns)
        mark_range = None
        if adjusted_start_col is not None and adjusted_end_col is not None:
            # Ensure columns are not negative after dedenting adjustment
            adjusted_start_col = max(0, adjusted_start_col)
            adjusted_end_col = max(0, adjusted_end_col)
            mark_lfinal = end_line or error_line_in_context
            mark_range = Range(
                error_line_in_context, mark_lfinal, adjusted_start_col, adjusted_end_col
            )

        # Build emphasis range and fragments
        em_range = _extract_emphasis_columns(
            lines,
            error_line_in_context,
            end_line,
            adjusted_start_col,
            adjusted_end_col,
            start,
        )
        fragments = _parse_lines_to_fragments(lines, mark_range, em_range)

        # Compute cursor position (prefer em end, fall back to mark end)
        # original_common_indent + extra_indent = total common indent removed
        cursor_line, cursor_col = compute_cursor_position(
            mark_range, em_range, start, original_common_indent + extra_indent
        )

        # Format location with cursor position for precise navigation
        filename, location, urls = format_location(
            original_filename, cursor_line, cursor_col
        )

        # Extract variable source: use marked region + comprehension expansion if inside one
        variable_source = _get_variable_source_for_comprehension(
            lines, lineno, start, mark_range
        )

        frames.append(
            {
                "id": f"tb-{token_urlsafe(12)}",
                "relevance": relevance,
                "hidden": hidden,  # For chain analysis; filtered out after ordering
                "filename": filename,
                "original_filename": original_filename,  # For chain analysis AST parsing
                "location": location,
                "notebook_cell": notebook_cell,
                "codeline": codeline[0].strip() if codeline else None,
                "range": Range(lineno, pos_end_lineno or lineno, start_col, end_col)
                if start_col is not None
                else None,
                "lineno": lineno,  # Actual error line from traceback (always available)
                "cursor_line": cursor_line,
                "cursor_col": cursor_col,
                "linenostart": start,
                "lines": lines,
                "fragments": fragments,
                "function": function,
                "function_suffix": "",
                "urls": urls,
                "variables": extract_variables(frame.f_locals, variable_source)
                if not hidden
                else [],
                # Full source for chain analysis (try-except matching via AST)
                "full_source": full_source,
                "full_source_start": full_source_start,
            }
        )

    if exc is not None:
        _set_relevances(frames, exc)
    return frames


def _calculate_common_indent(lines):
    """Calculate common indentation across all non-empty lines."""
    non_empty_lines = [line.rstrip("\r\n") for line in lines if line.strip()]
    if not non_empty_lines:
        return ""
    indent_len = min(len(ln) - len(ln.lstrip(" \t")) for ln in non_empty_lines)
    return non_empty_lines[0][:indent_len]


def _convert_range_to_positions(range_obj, lines):
    """Convert Range (1-based inclusive lines, 0-based exclusive columns) to absolute character positions."""
    positions = set()

    if not range_obj:
        return positions

    # Convert to 0-based line indices for processing
    start_line_idx = range_obj.lfirst - 1
    end_line_idx = range_obj.lfinal - 1

    # Calculate absolute positions
    char_pos = 0
    for line_idx, line in enumerate(lines):
        if start_line_idx <= line_idx <= end_line_idx:
            line_content = line.rstrip("\r\n")

            if line_idx == start_line_idx == end_line_idx:
                # Single line case
                for col in range(
                    max(0, range_obj.cbeg), min(len(line_content), range_obj.cend)
                ):
                    positions.add(char_pos + col)
            elif line_idx == start_line_idx:
                # First line of multi-line
                for col in range(max(0, range_obj.cbeg), len(line_content)):
                    positions.add(char_pos + col)
            elif line_idx == end_line_idx:
                # Last line of multi-line
                for col in range(0, min(len(line_content), range_obj.cend)):
                    positions.add(char_pos + col)
            else:
                # Middle lines of multi-line
                for col in range(len(line_content)):
                    positions.add(char_pos + col)

        char_pos += len(line)

    return positions


def _create_unified_fragments(lines_text, common_indent, mark_positions, em_positions):
    """Create fragments with unified mark/em highlighting."""
    lines = lines_text.splitlines(keepends=True)
    result = []

    for line_idx, line in enumerate(lines):
        line_num = line_idx + 1
        fragments = _parse_line_to_fragments_unified(
            line,
            common_indent,
            mark_positions,
            em_positions,
            sum(len(lines[i]) for i in range(line_idx)),  # char offset for this line
        )
        result.append({"line": line_num, "fragments": fragments})

    return result


def _parse_line_to_fragments_unified(
    line, common_indent, mark_positions, em_positions, line_char_offset
):
    """Parse a single line into fragments using unified highlighting."""
    line_content, line_ending = _split_line_content(line)
    if not line_content and not line_ending:
        return []

    # Process indentation
    fragments, remaining, pos = _process_indentation(line_content, common_indent)

    # Find comment split
    comment_start = _find_comment_start(remaining)

    if comment_start is not None:
        # Handle line with comment
        code_part = remaining[:comment_start]
        comment_part = remaining[comment_start:]

        # Process code part (with trimming)
        code_trimmed = code_part.rstrip()
        code_whitespace = code_part[len(code_trimmed) :]

        if code_trimmed:
            fragments.extend(
                _create_highlighted_fragments_unified(
                    code_trimmed, line_char_offset + pos, mark_positions, em_positions
                )
            )

        # Process comment part
        comment_trimmed = comment_part.rstrip()
        comment_trailing = comment_part[len(comment_trimmed) :]

        comment_with_leading_space = code_whitespace + comment_trimmed
        fragments.append({"code": comment_with_leading_space, "comment": "solo"})

        # Add trailing content
        trailing_content = comment_trailing + line_ending
        if trailing_content:
            fragments.append({"code": trailing_content, "trailing": "solo"})
    else:
        # Handle line without comment
        code_trimmed = remaining.rstrip()
        trailing_whitespace = remaining[len(code_trimmed) :]

        if code_trimmed:
            fragments.extend(
                _create_highlighted_fragments_unified(
                    code_trimmed, line_char_offset + pos, mark_positions, em_positions
                )
            )

        trailing_content = trailing_whitespace + line_ending
        if trailing_content:
            fragments.append({"code": trailing_content, "trailing": "solo"})

    return fragments


def _create_highlighted_fragments_unified(
    text, start_pos, mark_positions, em_positions
):
    """Create fragments with mark/em highlighting using unified position sets."""
    if not text:
        return []

    # Convert absolute positions to text-relative positions
    text_mark_positions = set()
    text_em_positions = set()

    for i in range(len(text)):
        abs_pos = start_pos + i
        if abs_pos in mark_positions:
            text_mark_positions.add(i)
        if abs_pos in em_positions:
            text_em_positions.add(i)

    # Create fragments using existing logic
    return _create_fragments_with_highlighting(
        text, text_mark_positions, text_em_positions
    )


def _parse_lines_to_fragments(lines_text, mark_range=None, em_ranges=None):
    """
    Parse lines of code into fragments with mark/em highlighting information.

    Args:
        lines_text: The multi-line string containing code
        mark_range: Range object for mark highlighting (or None)
        em_ranges: Range object or list of Range objects for em highlighting (or None)

    Returns:
        List of line dictionaries with fragment information
    """
    lines = lines_text.splitlines(keepends=True)
    if not lines:
        return []

    common_indent = _calculate_common_indent(lines)

    # Convert both mark and em to position sets using unified logic
    mark_positions = _convert_range_to_positions(mark_range, lines)

    # Handle em_ranges as either a single Range or a list of Ranges
    em_positions = set()
    if em_ranges:
        if isinstance(em_ranges, list):
            for em_range in em_ranges:
                em_positions |= _convert_range_to_positions(em_range, lines)
        else:
            em_positions = _convert_range_to_positions(em_ranges, lines)

    # Create fragments using unified highlighting
    return _create_unified_fragments(
        lines_text, common_indent, mark_positions, em_positions
    )


def _split_line_content(line):
    """Split line into content and line ending."""
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    elif line.endswith("\n"):
        return line[:-1], "\n"
    elif line.endswith("\r"):
        return line[:-1], "\r"
    else:
        return line, ""


def _process_indentation(line_content, common_indent):
    """Process dedent and additional indentation, return fragments and remaining content."""
    fragments = []
    pos = 0

    # Handle dedent (common indentation)
    if common_indent and len(line_content) > len(common_indent):
        dedent_text = line_content[: len(common_indent)]
        fragments.append({"code": dedent_text, "dedent": "solo"})
        pos = len(common_indent)

    # Handle additional indentation
    remaining = line_content[pos:]
    indent_match = re.match(r"^(\s+)", remaining)
    if indent_match:
        indent_text = indent_match.group(1)
        fragments.append({"code": indent_text, "indent": "solo"})
        pos += len(indent_text)
        remaining = remaining[len(indent_text) :]

    return fragments, remaining, pos


def _find_comment_start(text):
    """Find the start of a comment, ignoring # inside strings."""
    in_string = False
    string_char = None
    escape_next = False

    for i, char in enumerate(text):
        if escape_next:
            escape_next = False
            continue
        if char == "\\":
            escape_next = True
            continue
        if not in_string and char == "#":
            return i
        if not in_string and char in ('"', "'"):
            in_string = True
            string_char = char
        elif in_string and char == string_char:
            in_string = False
            string_char = None

    return None


def _positions_to_consecutive_ranges(positions):
    """Convert a set/list of positions to consecutive (start, end) ranges."""
    if not positions:
        return []

    sorted_positions = sorted(set(positions))
    ranges = []
    start = sorted_positions[0]
    end = start + 1

    for pos in sorted_positions[1:]:
        if pos == end:
            # Consecutive position, extend current range
            end = pos + 1
        else:
            # Gap found, close current range and start new one
            ranges.append((start, end))
            start = pos
            end = pos + 1

    # Close the last range
    ranges.append((start, end))
    return ranges


def _get_highlight_boundaries(text, mark_positions, em_positions):
    """Get all boundaries for highlighting (start/end of mark and em regions)."""
    boundaries = {0, len(text)}

    # Add mark boundaries
    for start, end in _positions_to_consecutive_ranges(mark_positions):
        boundaries.add(start)
        boundaries.add(end)

    # Add em boundaries
    for start, end in _positions_to_consecutive_ranges(em_positions):
        boundaries.add(start)
        boundaries.add(end)

    return sorted(boundaries)


def _create_fragments_with_highlighting(text, mark_positions, em_positions):
    """Create fragments with mark/em highlighting using beg/mid/fin/solo logic."""
    if not text:
        return []

    # Get all boundaries and create fragments
    boundaries = _get_highlight_boundaries(text, mark_positions, em_positions)
    mark_ranges = _positions_to_consecutive_ranges(mark_positions)
    em_ranges = _positions_to_consecutive_ranges(em_positions)

    fragments = []
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1]

        if start >= len(text):
            break

        fragment_text = text[start:end]
        fragment = {"code": fragment_text}

        # Determine mark status
        mark_status = _get_highlight_status(start, end, mark_ranges)
        if mark_status:
            fragment["mark"] = mark_status

        # Determine em status
        em_status = _get_highlight_status(start, end, em_ranges)
        if em_status:
            fragment["em"] = em_status

        fragments.append(fragment)

    return fragments


def _get_highlight_status(frag_start, frag_end, ranges):
    """Determine beg/mid/fin/solo status for a fragment within ranges."""
    # Find overlapping ranges
    overlapping = []
    for range_start, range_end in ranges:
        if frag_start < range_end and frag_end > range_start:
            overlapping.append((range_start, range_end))

    if not overlapping:
        return None

    # Use the first overlapping range (they should align with fragment boundaries)
    range_start, range_end = overlapping[0]

    is_start = frag_start <= range_start
    is_end = frag_end >= range_end

    if is_start and is_end:
        return "solo"
    elif is_start:
        return "beg"
    elif is_end:
        return "fin"
    else:
        return "mid"
