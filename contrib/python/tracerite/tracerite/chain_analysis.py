"""AST-based analysis for exception chain try-except block matching.

This module provides utilities to build a chronological chain of events
during a multi-exception chain by analyzing the AST to identify which
try block contains the code that raised the inner exception, relative
to the except block where the outer exception was raised.

Key insight about Python exception frames:
- Inner exception frames start from the try block (not app entry point)
  So the first frame is always in the try body where the exception occurred.
- Outer exception frames start from app entry point and traverse through
  the call stack. We search these to find a frame in an except handler
  that corresponds to the inner's try block.

ExceptionGroups (Python 3.11+) introduce parallel timelines:
- An ExceptionGroup contains multiple subexceptions that occurred concurrently
- Each subexception has its own traceback chain
- These parallel timelines are represented as branches in the chronological view
- The ExceptionGroup's own traceback provides the surrounding context
"""

from __future__ import annotations

import ast

__all__ = [
    "TryExceptBlock",
    "ChainLink",
    "parse_source_for_try_except",
    "parse_source_string_for_try_except",
    "find_try_block_for_except_line",
    "find_matching_try_for_inner_exception",
    "analyze_exception_chain_links",
    "enrich_chain_with_links",
    "build_chronological_frames",
]
import linecache
from dataclasses import dataclass

from .logging import logger


@dataclass
class TryExceptBlock:
    """Represents a try-except block with its line ranges."""

    try_start: int  # First line of try keyword
    try_end: int  # Last line of try body (before except/else/finally)
    except_start: int | None  # First line of except handlers
    except_end: int | None  # Last line of except handlers
    finally_start: int | None = None
    finally_end: int | None = None

    def contains_in_try(self, lineno: int) -> bool:
        """Check if a line number is within the try body."""
        return self.try_start <= lineno <= self.try_end

    def contains_in_except(self, lineno: int) -> bool:
        """Check if a line number is within an except handler."""
        if self.except_start is None or self.except_end is None:
            return False
        return self.except_start <= lineno <= self.except_end


@dataclass
class ChainLink:
    """Represents a link between two exceptions in a chain.

    Attributes:
        outer_frame_idx: Index of the frame in the outer exception that's in the except block
        try_block: The TryExceptBlock that links the inner and outer exceptions
        matched: Whether we successfully matched the try-except relationship
    """

    outer_frame_idx: int
    try_block: TryExceptBlock | None
    matched: bool


class TryExceptVisitor(ast.NodeVisitor):
    """AST visitor that collects all try-except blocks with their line ranges."""

    def __init__(self):
        self.try_except_blocks: list[TryExceptBlock] = []

    def visit_Try(self, node: ast.Try):
        """Visit a Try node and record its structure."""
        # Find the end of the try body
        try_body_end = self._get_last_line(node.body)

        # Process except handlers
        except_start = None
        except_end = None
        if node.handlers:
            except_start = node.handlers[0].lineno
            except_end = self._get_last_line(list(node.handlers))

        # Process finally block
        finally_start = None
        finally_end = None
        if node.finalbody:
            finally_start = node.finalbody[0].lineno
            finally_end = self._get_last_line(node.finalbody)

        if except_start is not None:
            block = TryExceptBlock(
                try_start=node.lineno,
                try_end=try_body_end,
                except_start=except_start,
                except_end=except_end,
                finally_start=finally_start,
                finally_end=finally_end,
            )
            self.try_except_blocks.append(block)

        # Continue visiting nested structures
        self.generic_visit(node)

    def _get_last_line(self, nodes) -> int:
        """Get the last line number from a list of AST nodes."""
        last_line = 0
        for node in nodes:
            last_line = max(last_line, getattr(node, "end_lineno", node.lineno))
        return last_line


def parse_source_for_try_except(
    filename: str, function_name: str | None = None
) -> list[TryExceptBlock]:
    """Parse source file and extract try-except blocks.

    Args:
        filename: Path to the source file
        function_name: Optional function name to limit scope

    Returns:
        List of TryExceptBlock objects found in the source
    """
    try:
        lines = linecache.getlines(filename)
        if not lines:
            return []

        source = "".join(lines)
        tree = ast.parse(source, filename=filename)

        visitor = TryExceptVisitor()
        visitor.visit(tree)

        return visitor.try_except_blocks
    except (SyntaxError, OSError, ValueError) as e:
        logger.debug(f"Failed to parse {filename} for try-except analysis: {e}")
        return []


def parse_source_string_for_try_except(
    source: str, start_line: int = 1
) -> list[TryExceptBlock]:
    """Parse source string and extract try-except blocks.

    Args:
        source: The source code as a string
        start_line: The line number where this source starts (for offset adjustment)

    Returns:
        List of TryExceptBlock objects found in the source
    """
    try:
        if not source:
            return []

        tree = ast.parse(source)

        visitor = TryExceptVisitor()
        visitor.visit(tree)

        # Adjust line numbers if source doesn't start at line 1
        if start_line != 1:
            offset = start_line - 1
            adjusted_blocks = []
            for block in visitor.try_except_blocks:
                adjusted_blocks.append(
                    TryExceptBlock(
                        try_start=block.try_start + offset,
                        try_end=block.try_end + offset,
                        except_start=block.except_start + offset
                        if block.except_start
                        else None,
                        except_end=block.except_end + offset
                        if block.except_end
                        else None,
                        finally_start=block.finally_start + offset
                        if block.finally_start
                        else None,
                        finally_end=block.finally_end + offset
                        if block.finally_end
                        else None,
                    )
                )
            return adjusted_blocks

        return visitor.try_except_blocks
    except (SyntaxError, ValueError) as e:
        logger.debug(f"Failed to parse source string for try-except analysis: {e}")
        return []


def find_try_block_for_except_line(
    blocks: list[TryExceptBlock], except_lineno: int
) -> TryExceptBlock | None:
    """Find the try-except block that contains the given line in its except handler.

    Args:
        blocks: List of TryExceptBlock objects
        except_lineno: Line number that should be within an except handler

    Returns:
        The TryExceptBlock if found, None otherwise
    """
    # Sort by specificity - prefer innermost blocks (those with higher try_start)
    matching_blocks = [b for b in blocks if b.contains_in_except(except_lineno)]

    if not matching_blocks:
        return None

    # Return the most specific (innermost) block
    return max(matching_blocks, key=lambda b: b.try_start)


def find_matching_try_for_inner_exception(
    blocks: list[TryExceptBlock], inner_first_lineno: int, outer_except_lineno: int
) -> TryExceptBlock | None:
    """Find the try block that contains the inner exception's first frame
    and whose except block contains the outer exception's frame.

    This is the key function for building the chronological chain:
    - inner_first_lineno: The line in the inner exception's first frame (in the try body)
    - outer_except_lineno: A line from the outer exception that's in an except block

    Args:
        blocks: List of TryExceptBlock objects
        inner_first_lineno: First line of the inner exception's traceback
        outer_except_lineno: Line from outer exception that should be in except block

    Returns:
        The TryExceptBlock that links both exceptions, or None if no match
    """
    for block in blocks:
        # The outer exception line must be in the except handler
        if not block.contains_in_except(outer_except_lineno):
            continue
        # The inner exception's first line must be in the try body
        if block.contains_in_try(inner_first_lineno):
            return block

    return None


def analyze_exception_chain_links(chain: list[dict]) -> list[ChainLink | None]:
    """Analyze an exception chain to find try-except relationships.

    For each pair of consecutive exceptions in the chain (inner, outer),
    find the try-except block that links them.

    Args:
        chain: List of exception info dicts from extract_chain (oldest first)

    Returns:
        List of ChainLink objects (one per exception, first is always None
        since the first exception has no prior exception to link to)
    """
    if len(chain) <= 1:
        return [None] * len(chain)

    links: list[ChainLink | None] = [None]  # First exception has no link

    for i in range(1, len(chain)):
        inner_exc = chain[i - 1]
        outer_exc = chain[i]

        link = _find_chain_link(inner_exc, outer_exc)
        links.append(link)

    return links


def _get_frame_lineno(frame: dict) -> int | None:
    """Extract the line number from a frame dict.

    Tries in order:
    1. range[0] (lfirst) - most precise, from Python 3.11+ co_positions
    2. lineno - actual traceback line number (always available)
    3. linenostart - first line of displayed source (fallback)
    """
    frame_range = frame.get("range")
    if frame_range:
        return frame_range[0]  # lfirst from Range namedtuple
    # Fallback to lineno (actual error line from traceback)
    if frame.get("lineno"):
        return frame.get("lineno")
    # Final fallback to linenostart (first line of displayed source)
    return frame.get("linenostart")


def _find_chain_link(inner_exc: dict, outer_exc: dict) -> ChainLink | None:
    """Find the try-except link between two consecutive exceptions.

    The inner exception's frames start from the try block (not app entry point),
    so its first frame is always the one in the try block we're looking for.

    The outer exception's frames start from app entry point and traverse through
    the call stack. We need to search these frames to find one that's within
    an except handler that corresponds to the inner's try block.

    Args:
        inner_exc: The earlier/inner exception info dict
        outer_exc: The later/outer exception info dict

    Returns:
        ChainLink if a relationship is found, None otherwise
    """
    inner_frames = inner_exc.get("frames", [])
    outer_frames = outer_exc.get("frames", [])

    if not inner_frames or not outer_frames:
        return None

    # Inner exception: first frame is always in the try block
    # (Python captures frames starting from the try block, not app entry)
    inner_first_frame = inner_frames[0]
    inner_first_lineno = _get_frame_lineno(inner_first_frame)

    if inner_first_lineno is None:
        return None

    # Get try-except blocks from the frame's full source (works with any source)
    # This avoids reading from files, using the source Python already has
    inner_full_source = inner_first_frame.get("full_source")
    inner_source_start = inner_first_frame.get("full_source_start", 1)

    if inner_full_source:
        try_except_blocks = parse_source_string_for_try_except(
            inner_full_source, inner_source_start
        )
    else:
        # Fallback to file-based parsing if full_source not available
        inner_filename = inner_first_frame.get(
            "original_filename"
        ) or inner_first_frame.get("filename")
        if not inner_filename:
            return None
        try_except_blocks = parse_source_for_try_except(inner_filename)

    if not try_except_blocks:
        return None

    # Outer exception: search through ALL frames to find one in an except block
    # The frame list starts from app entry point and may visit the same function
    # multiple times, or have additional frames after the except block
    for frame_idx, frame in enumerate(outer_frames):
        frame_lineno = _get_frame_lineno(frame)
        if frame_lineno is None:
            continue

        # Try to find a try-except block where:
        # - inner_first_lineno is in the try body
        # - frame_lineno is in the except handler
        matching_block = find_matching_try_for_inner_exception(
            try_except_blocks, inner_first_lineno, frame_lineno
        )

        if matching_block:
            return ChainLink(
                outer_frame_idx=frame_idx,
                try_block=matching_block,
                matched=True,
            )

    return None


def enrich_chain_with_links(chain: list[dict]) -> list[dict]:
    """Enrich exception chain with try-except link information.

    Adds a 'chain_link' key to each exception dict with information
    about how it relates to the previous exception in the chain.

    Args:
        chain: List of exception info dicts from extract_chain (oldest first)

    Returns:
        The same chain list, with 'chain_link' added to each dict
    """
    links = analyze_exception_chain_links(chain)

    for _i, (exc, link) in enumerate(zip(chain, links)):
        if link and link.matched:
            exc["chain_link"] = {
                "outer_frame_idx": link.outer_frame_idx,
                "try_start": link.try_block.try_start,
                "try_end": link.try_block.try_end,
                "except_start": link.try_block.except_start,
                "except_end": link.try_block.except_end,
            }
        else:
            exc["chain_link"] = None

    return chain


def build_chronological_frames(chain: list[dict]) -> list[dict]:
    """Build a chronological list of frames showing the actual sequence of events.

    This creates a flat list of frames in the order they were executed, with
    exception information embedded in the frames. The result shows:
    1. Frames from entry point leading to the try block
    2. The inner exception's frames (from try block to error)
    3. The except handler frame (promoted to relevance="except")
    4. Any frames after except leading to the next exception
    5. ...and so on for nested chains

    For ExceptionGroups:
    - Subexceptions form parallel timelines that occurred concurrently
    - These are represented as a special "parallel" frame containing multiple branches
    - Each branch is itself a list of chronological frames
    - The parallel block is inserted before the ExceptionGroup's error frame

    The key insight is that the LAST exception in the chain has the full call
    stack from entry point. Inner exceptions only have partial stacks starting
    from where they were raised. We use the outer exception's frames as the
    "backbone" and insert inner exception frames at the appropriate positions.

    Hidden frames (marked with hidden=True, e.g., from __tracebackhide__) are
    kept during chain analysis to enable proper try-except matching, then
    filtered out at the end.

    Args:
        chain: List of exception info dicts from extract_chain (oldest first),
               should already be enriched with chain_link info

    Returns:
        List of frame dicts in chronological order. Each frame may have:
        - "exception": dict with exception info (type, message, from) if this
          frame is where an exception was raised
        - "relevance": may be promoted to "except" for except handler frames
        - "parallel": list of parallel branches (for ExceptionGroup subexceptions)
    """
    if not chain:
        return []

    # First, ensure chain has link info
    links = analyze_exception_chain_links(chain)

    # Build chronological frames by working backwards from the last exception
    # The last exception has the full call stack; inner exceptions have partial stacks
    chronological: list[dict] = []

    # Process from the last (outermost) exception backwards
    # This lets us use the outer exception's frames as the backbone
    for exc_idx in range(len(chain) - 1, -1, -1):
        exc = chain[exc_idx]
        frames = exc.get("frames", [])

        if not frames:
            continue

        # Check if this exception has a link to an inner exception
        # (i.e., is there a next exception in the chain that links to this one?)
        links[exc_idx + 1] if exc_idx + 1 < len(chain) else None

        if exc_idx == len(chain) - 1:
            # This is the outermost exception - use its frames as the backbone
            # But we need to insert inner exception frames at the right positions
            _build_backbone_frames(chronological, exc, exc_idx, frames, links, chain)
        # Inner exceptions are handled by _build_backbone_frames inserting them

    # Filter out hidden frames (kept for chain analysis, not for display)
    chronological = _filter_hidden_frames(chronological)

    # Apply suppression for BaseExceptions (KeyboardInterrupt, SystemExit, etc.)
    # These should only show frames up to the last user code frame ("bug frame"),
    # suppressing library internals that came after
    chronological = _apply_base_exception_suppression(chronological, chain)

    return chronological


def _filter_hidden_frames(chronological: list[dict]) -> list[dict]:
    """Filter out hidden frames, handling parallel branches recursively."""
    result = []
    for frame in chronological:
        if frame.get("hidden"):
            continue
        # Recursively filter parallel branches
        if "parallel" in frame:
            filtered_branches = []
            for branch in frame["parallel"]:
                filtered_branch = _filter_hidden_frames(branch)
                if filtered_branch:
                    filtered_branches.append(filtered_branch)
            if filtered_branches:
                frame = {**frame, "parallel": filtered_branches}
                result.append(frame)
        else:
            result.append(frame)
    return result


def _apply_base_exception_suppression(
    chronological: list[dict], chain: list[dict]
) -> list[dict]:
    """Suppress library frames after the last user code frame for BaseExceptions.

    For BaseExceptions (KeyboardInterrupt, SystemExit, etc.), we don't want to
    show library internals - only show frames up to the last user code frame
    (the "bug frame"). The exception info is transferred to the last shown frame.

    Args:
        chronological: The full list of chronological frames
        chain: The exception chain (to check suppress_inner flag)

    Returns:
        Filtered list of frames with appropriate adjustments
    """
    if not chronological or not chain:
        return chronological

    # Check if any exception in the chain has suppress_inner=True
    # (This is set for BaseExceptions like KeyboardInterrupt, SystemExit)
    has_suppress = any(exc.get("suppress_inner") for exc in chain)
    if not has_suppress:
        return chronological

    # Find the last "bug frame" (last user code frame before library code)
    # This is marked by relevance="warning" during extraction
    last_bug_frame_idx = None
    for idx, frame in enumerate(chronological):
        if frame.get("relevance") == "warning":
            last_bug_frame_idx = idx

    if last_bug_frame_idx is None:
        # No bug frame found, return as-is
        return chronological

    # Suppress all frames after the bug frame
    result = chronological[: last_bug_frame_idx + 1]

    # Transfer exception info and parallel branches from suppressed frames to the last shown frame
    if result:  # pragma: no cover
        # Check if any suppressed frame had an exception or parallel branches attached
        suppressed_exception = None
        suppressed_parallel = None
        for suppressed in chronological[last_bug_frame_idx + 1 :]:
            if suppressed.get("exception") and not suppressed_exception:
                suppressed_exception = suppressed["exception"]
            if suppressed.get("parallel") and not suppressed_parallel:
                suppressed_parallel = suppressed["parallel"]

        # If we're suppressing frames with an exception, transfer it to the last shown frame
        if suppressed_exception and not result[-1].get("exception"):
            result[-1] = {**result[-1], "exception": suppressed_exception}

        # Transfer parallel branches (subexceptions) to the last shown frame
        if suppressed_parallel and not result[-1].get("parallel"):
            result[-1] = {**result[-1], "parallel": suppressed_parallel}

        # Change relevance to "stop" to indicate suppression point
        # (unless it already has an exception, which sets its own relevance)
        if result[-1].get("relevance") in ("call", "warning"):  # pragma: no cover
            result[-1] = {**result[-1], "relevance": "stop"}

    return result


def _build_backbone_frames(
    chronological: list[dict],
    exc: dict,
    exc_idx: int,
    frames: list[dict],
    links: list,
    chain: list[dict],
) -> None:
    """Build chronological frames using this exception's frames as backbone.

    Recursively inserts inner exception frames at the appropriate positions.
    Also handles ExceptionGroup subexceptions as parallel branches.
    """
    # Find if there's an inner exception that links to one of our frames
    inner_exc_idx = exc_idx - 1
    inner_link = links[exc_idx] if exc_idx > 0 else None

    # If we have an inner exception with a matched link, we need to:
    # 1. Output frames from start up to (but not including) the except handler
    # 2. Insert the inner exception's frames
    # 3. Output the except handler frame and any frames after it

    if inner_link and inner_link.matched and inner_exc_idx >= 0:
        except_frame_idx = inner_link.outer_frame_idx
        inner_exc = chain[inner_exc_idx]
        inner_frames = inner_exc.get("frames", [])

        # Output frames before the except handler (these are the call stack leading to try)
        for frame_idx in range(except_frame_idx):
            frame = frames[frame_idx]
            chrono_frame = _copy_frame(frame)
            chronological.append(chrono_frame)

        # Recursively handle the inner exception
        # Always recurse to handle even more inner exceptions (best effort)
        _build_backbone_frames(
            chronological, inner_exc, inner_exc_idx, inner_frames, links, chain
        )

        # Now output the except handler frame and frames after it
        for frame_idx in range(except_frame_idx, len(frames)):
            frame = frames[frame_idx]
            chrono_frame = _copy_frame(frame)

            is_except_entry = frame_idx == except_frame_idx
            is_last = frame_idx == len(frames) - 1

            if is_except_entry:
                # Promote relevance to "except" if it was just a "call" or "warning" frame
                if chrono_frame.get("relevance") in ("call", "warning"):
                    chrono_frame["relevance"] = "except"
                chrono_frame["function_suffix"] = "⚡except"

            if is_last:
                chrono_frame["exception"] = {
                    "type": exc.get("type"),
                    "message": exc.get("message"),
                    "summary": exc.get("summary"),
                    "from": exc.get("from"),
                    "exc_idx": exc_idx,
                }
                # Handle subexceptions for this ExceptionGroup
                _add_subexceptions_to_frame(chrono_frame, exc)

            chronological.append(chrono_frame)
    else:
        # No matched inner exception link - but still include inner exceptions (best effort)
        # First, recursively process any inner exceptions
        if exc_idx > 0:
            inner_exc = chain[exc_idx - 1]
            inner_frames = inner_exc.get("frames", [])
            if inner_frames:
                _build_backbone_frames(
                    chronological,
                    inner_exc,
                    exc_idx - 1,
                    inner_frames,
                    links,
                    chain,
                )

        # Then output all frames for this exception
        for frame_idx, frame in enumerate(frames):
            chrono_frame = _copy_frame(frame)
            is_last = frame_idx == len(frames) - 1

            if is_last:
                chrono_frame["exception"] = {
                    "type": exc.get("type"),
                    "message": exc.get("message"),
                    "summary": exc.get("summary"),
                    "from": exc.get("from"),
                    "exc_idx": exc_idx,
                }
                # Handle subexceptions for this ExceptionGroup
                _add_subexceptions_to_frame(chrono_frame, exc)

            chronological.append(chrono_frame)


def _add_subexceptions_to_frame(frame: dict, exc: dict) -> None:
    """Add subexceptions from an ExceptionGroup as parallel branches.

    If the exception has subexceptions (from an ExceptionGroup), each one
    is processed into its own chronological frame list and added as a
    parallel branch to the frame.

    Args:
        frame: The frame dict to add parallel branches to
        exc: The exception dict that may contain subexceptions
    """
    subexceptions = exc.get("subexceptions")
    if not subexceptions:
        return

    parallel_branches = []
    for sub_chain in subexceptions:
        # Build chronological frames for this subexception chain
        sub_chrono = build_chronological_frames(sub_chain)
        if sub_chrono:
            parallel_branches.append(sub_chrono)

    if parallel_branches:
        frame["parallel"] = parallel_branches


def _copy_frame(frame: dict) -> dict:
    """Create a shallow copy of a frame dict."""
    return {**frame}
