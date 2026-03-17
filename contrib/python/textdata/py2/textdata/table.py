# -*- coding: utf-8 -*-

from __future__ import print_function, division, unicode_literals
import re
import textwrap
import sys

from intspan import intspan

from .eval import evaluation
from .util import CSTRIP, ensure_text, _PY2, partition
from .core import words
from .attrs import Dict


if not _PY2:
    basestring = str


def all_indices(s, substr):
    """
    Find all indices in ``s`` where ``substr`` begins.
    Returns results in list.
    """
    indices = []
    i = s.find(substr)
    while i >= 0:
        indices.append(i)
        i = s.find(substr, i+1)
    return indices


def is_separator(line):
    """
    Does the given line consist solely of separator characters?
    -=+: and Unicode line-drawing equivalents
    """
    return bool(re.match(r"^[-=+:|\.`'\s\u2500-\u257F]*$", line))


def vertical_sep_in_line(line):
    """
    Find the indices in a line which are potentially or probably
    vertical separators (characters commonly used to indicate
    the end of columns).
    """
    VERTICAL_SEP = r"[+|\.`'╔╦╗╠╬╣╚╩╝┌┬┐╞╪╡├┼┤└┴┘]"
    return [m.start() for m in re.finditer(VERTICAL_SEP, line)]


def col_break_indices(lines, combine='update'):
    """
    Given a set of horizontal separator lines, return a guess as to
    which indices have column breaks, based on common indicator characters.
    """
    all_lines_indices = [vertical_sep_in_line(line) for line in lines]
    combined = intspan(all_lines_indices[0])

    update_func = getattr(combined, combine)
    for line_indices in all_lines_indices[1:]:
        update_func(line_indices)
    return combined


def find_columns(lines):
    """
    Given a list of text lines, assume they represent a fixed-width
    "ASCII table" and guess the column indices therein. Depends on
    finding typographical "rivers" of spaces running vertically
    through text indicating column breaks.

    This is a high-probability heuristic (based on the many tests performed on
    it). There are some cases where all rows happen to include aligned spaces
    that do *not* signify a column break. In this case, recommend you modify
    the table with a separator line (e.g. using --- characters) showing where
    the columns should be. Since separators are stripped out, adding an
    explicit set of separators will not alter result data.
    """
    # Partition lines into seps (separators and blank lines) and nonseps (content)
    nonseps, seps = partition(is_separator, lines)

    # Find max length of content lines. This defines the "universe" of
    # available content columns. Use only non-separator lines because they
    # are the content we care most about.
    maxlen = max(len(l) for l in nonseps)
    universe = intspan.from_range(0, maxlen - 1)

    # If there are separators lines, try to find definitive vertical separation
    # markers in them to define column boundaries.
    if seps:
        # If separators, try to find the column breaks in them
        indices = col_break_indices(seps)
        iranges = (universe - indices).ranges()
    else:
        indices = None


    if not seps or not indices:
        # If horizontal separators not present, or if present but lack the vertical
        # separation indicators needed to determine column locations, look for
        # vertical separators common to all rows. A rare, but genuine case.
        indices = col_break_indices(nonseps, 'intersection_update')
        if not indices:
            # Vertical separators not found. Fall back to using vertical
            # whitespace rivers as column separators. Find where spaces are in
            # every column.
            indices = intspan.from_range(0, maxlen - 1)

            for l in lines:
                line_spaces = intspan(all_indices(l, ' '))
                indices.intersection_update(line_spaces)

        # indices is now intspan showing where spaces or vertical seps are
        # Find inclusive ranges where content would be
        iranges = (universe - indices).ranges()

    # Convert inclusive ranges to half-open Python ranges
    hranges = [(s, e+1) for s,e in iranges]
    return seps, nonseps, hranges


def discover_table(text, header=False, evaluate=True, cstrip=True):
    """
    Return a list of lists representing a table.
    """
    if cstrip:
        text = CSTRIP.sub('', text)

    # import text into lines
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]

    # remove common indentation (needs round trip back to string, then to lines)
    cleanlines = '\n'.join(lines)
    lines = textwrap.dedent(cleanlines).splitlines()

    # find the columns
    seps, nonseps, column_indices = find_columns(lines)
    n_columns = len(column_indices)

    # extend evaluate for each column, as needed
    if isinstance(evaluate, (list, tuple)):
        # already a sequence; determine if needs extension
        needed = n_columns - len(evaluate)
        if needed > 0:
            evaluates = list(evaluate) + ([evaluate[-1]] * needed)
        else:
            evaluates = evaluate[:]
    else:
        # multiple scalar to n_columns copies
        evaluates = [evaluate] * n_columns

    # construct table based on discovered understanding
    # of where column breaks are
    rows = []
    if header is True:
        # use header from table; remove before evaluation
        header = []
        for c in column_indices:
            segment = nonseps[0][c[0]:c[1]]
            header.append(evaluation(segment, 'minimal'))
        nonseps = nonseps[1:]

    for l in nonseps:
        row = []
        for c, col_evaluate in zip(column_indices, evaluates):
            segment = l[c[0]:c[1]]
            row.append(evaluation(segment, col_evaluate))
        rows.append(row)
    if header:
        rows.insert(0, header)
    return rows


def table(source, header=None, evaluate=True, cstrip=True):
    """
    Return a list of lists representing a table.

    Args:
        source (Union[str, List[str]]): Text to parse (as string or list of lines)
        header (Union[str, List, None]): Header for the table
        evaluate (Union[str, function, None]): Indicates how to post-process
            table cells. By default, True or "natural" means as Python literals.
            Other options are False or 'minimal' (just string trimming), or
            None or 'none'.  Can also provide a custom function.
        cstrip (bool): strip comments?
        
    Returns:
        List of lists, where each inner list represents a row.
    """

    text = ensure_text(source)

    if header:
        if isinstance(header, basestring):
            header = words(header)

    rows = discover_table(text, header=header, evaluate=evaluate, cstrip=True)

    return rows


def keyclean(key):
    """
    Default way to clean table headers so they make good
    dictionary keys.
    """
    clean = re.sub(r'\s+', '_', key.strip())
    clean = re.sub(r'[^\w]', '', clean)
    return clean

keyclean.lc = lambda k: keyclean(k).lower()


def records(source, dict=Dict, keyclean=keyclean, **kwargs):
    """
    Alternate table parser. Renders not a list of lists, but a list of
    attribute-accessible Dict (dict subclasses).

    Args:
        source (Union[str, List[str]]): Text to parse (as string or list of lines)
        dict (type): dictionary subtype in which to return results
        keyclean (Union[Function, None]): function to clean table headers
            into more suitable dictionary keys
        **kwargs: All other kw args passed to textdata.table

    Returns:
        list of dictionaries, one per non-header row
    """
    text = ensure_text(source)

    rows = table(text, **kwargs)
    header, rows = rows[0], rows[1:]
    if keyclean:
        header = [keyclean(h) for h in header]

    return [dict(zip(header, row)) for row in rows]
