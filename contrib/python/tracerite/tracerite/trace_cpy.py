# Copied from https://github.com/python/cpython/blob/main/Lib/traceback.py
# We need to use internal functions that are not part of the public API,
# and that are not available in earlier Python versions.

# Unused functionality is removed and we run a formatter.
# One modification is made for Python 3.9 and 3.10 compatibility (see comment).
# ruff: noqa

"""Extract, format and print information about Python stack traces."""

import collections.abc
import itertools
import sys


class _Sentinel:
    def __repr__(self):
        return "<implicit>"


_sentinel = _Sentinel()


def _parse_value_tb(exc, value, tb):
    if (value is _sentinel) != (tb is _sentinel):
        raise ValueError("Both or neither of value and tb must be given")
    if value is tb is _sentinel:
        if exc is not None:
            if isinstance(exc, BaseException):
                return exc, exc.__traceback__

            raise TypeError(f"Exception expected for value, {type(exc).__name__} found")
        else:
            return None, None
    return value, tb


BUILTIN_EXCEPTION_LIMIT = object()


def _safe_string(value, what, func=str):
    try:
        return func(value)
    except:
        return f"<{what} {func.__name__}() failed>"


def _walk_tb_with_full_positions(tb):
    # Internal version of walk_tb that yields full code positions including
    # end line and column information.
    while tb is not None:
        positions = _get_code_position(tb.tb_frame.f_code, tb.tb_lasti)
        # Yield tb_lineno when co_positions does not have a line number to
        # maintain behavior with walk_tb.
        if positions[0] is None:
            yield tb.tb_frame, (tb.tb_lineno,) + positions[1:]
        else:
            yield tb.tb_frame, positions
        tb = tb.tb_next


def _get_code_position(code, instruction_index):
    if instruction_index < 0:
        return (None, None, None, None)
    # TRACERITE MODIFICATION: co_positions() was added in Python 3.11
    # Fallback for Python 3.9 and 3.10 compatibility
    if not hasattr(code, "co_positions"):
        return (None, None, None, None)
    positions_gen = code.co_positions()
    return next(itertools.islice(positions_gen, instruction_index // 2, None))


def _byte_offset_to_character_offset(str, offset):
    as_utf8 = str.encode("utf-8")
    return len(as_utf8[:offset].decode("utf-8", errors="replace"))


_Anchors = collections.namedtuple(
    "_Anchors",
    [
        "left_end_lineno",
        "left_end_offset",
        "right_start_lineno",
        "right_start_offset",
        "primary_char",
        "secondary_char",
    ],
    defaults=["~", "^"],
)


def _extract_caret_anchors_from_line_segment(segment):
    """
    Given source code `segment` corresponding to a FrameSummary, determine:
        - for binary ops, the location of the binary op
        - for indexing and function calls, the location of the brackets.
    `segment` is expected to be a valid Python expression.
    """
    import ast

    try:
        # Without parentheses, `segment` is parsed as a statement.
        # Binary ops, subscripts, and calls are expressions, so
        # we can wrap them with parentheses to parse them as
        # (possibly multi-line) expressions.
        # e.g. if we try to highlight the addition in
        # x = (
        #     a +
        #     b
        # )
        # then we would ast.parse
        #     a +
        #     b
        # which is not a valid statement because of the newline.
        # Adding brackets makes it a valid expression.
        # (
        #     a +
        #     b
        # )
        # Line locations will be different than the original,
        # which is taken into account later on.
        tree = ast.parse(f"(\n{segment}\n)")
    except SyntaxError:
        return None

    if len(tree.body) != 1:
        return None

    lines = segment.splitlines()

    def normalize(lineno, offset):
        """Get character index given byte offset"""
        return _byte_offset_to_character_offset(lines[lineno], offset)

    def next_valid_char(lineno, col):
        """Gets the next valid character index in `lines`, if
        the current location is not valid. Handles empty lines.
        """
        while lineno < len(lines) and col >= len(lines[lineno]):
            col = 0
            lineno += 1
        assert lineno < len(lines) and col < len(lines[lineno])
        return lineno, col

    def increment(lineno, col):
        """Get the next valid character index in `lines`."""
        col += 1
        lineno, col = next_valid_char(lineno, col)
        return lineno, col

    def nextline(lineno, col):
        """Get the next valid character at least on the next line"""
        col = 0
        lineno += 1
        lineno, col = next_valid_char(lineno, col)
        return lineno, col

    def increment_until(lineno, col, stop):
        """Get the next valid non-"\\#" character that satisfies the `stop` predicate"""
        while True:
            ch = lines[lineno][col]
            if ch in "\\#":
                lineno, col = nextline(lineno, col)
            elif not stop(ch):
                lineno, col = increment(lineno, col)
            else:
                break
        return lineno, col

    def setup_positions(expr, force_valid=True):
        """Get the lineno/col position of the end of `expr`. If `force_valid` is True,
        forces the position to be a valid character (e.g. if the position is beyond the
        end of the line, move to the next line)
        """
        # -2 since end_lineno is 1-indexed and because we added an extra
        # bracket + newline to `segment` when calling ast.parse
        lineno = expr.end_lineno - 2
        col = normalize(lineno, expr.end_col_offset)
        return next_valid_char(lineno, col) if force_valid else (lineno, col)

    statement = tree.body[0]
    if isinstance(statement, ast.Expr):
        expr = statement.value
        if isinstance(expr, ast.BinOp):
            # ast gives these locations for BinOp subexpressions
            # ( left_expr ) + ( right_expr )
            #   left^^^^^       right^^^^^
            lineno, col = setup_positions(expr.left)

            # First operator character is the first non-space/')' character
            lineno, col = increment_until(
                lineno, col, lambda x: not x.isspace() and x != ")"
            )

            # binary op is 1 or 2 characters long, on the same line,
            # before the right subexpression
            right_col = col + 1
            if (
                right_col < len(lines[lineno])
                and (
                    # operator char should not be in the right subexpression
                    expr.right.lineno - 2 > lineno
                    or right_col
                    < normalize(expr.right.lineno - 2, expr.right.col_offset)
                )
                and not (ch := lines[lineno][right_col]).isspace()
                and ch not in "\\#"
            ):
                right_col += 1

            # right_col can be invalid since it is exclusive
            return _Anchors(lineno, col, lineno, right_col)
        if isinstance(expr, ast.Subscript):
            # ast gives these locations for value and slice subexpressions
            # ( value_expr ) [ slice_expr ]
            #   value^^^^^     slice^^^^^
            # subscript^^^^^^^^^^^^^^^^^^^^

            # find left bracket
            left_lineno, left_col = setup_positions(expr.value)
            left_lineno, left_col = increment_until(
                left_lineno, left_col, lambda x: x == "["
            )
            # find right bracket (final character of expression)
            right_lineno, right_col = setup_positions(expr, force_valid=False)
            return _Anchors(left_lineno, left_col, right_lineno, right_col)
        if isinstance(expr, ast.Call):
            # ast gives these locations for function call expressions
            # ( func_expr ) (args, kwargs)
            #   func^^^^^
            # call^^^^^^^^^^^^^^^^^^^^^^^^

            # find left bracket
            left_lineno, left_col = setup_positions(expr.func)
            left_lineno, left_col = increment_until(
                left_lineno, left_col, lambda x: x == "("
            )
            # find right bracket (final character of expression)
            right_lineno, right_col = setup_positions(expr, force_valid=False)
            return _Anchors(left_lineno, left_col, right_lineno, right_col)

    return None


_MAX_CANDIDATE_ITEMS = 750
_MAX_STRING_SIZE = 40
_MOVE_COST = 2
_CASE_COST = 1


def _substitution_cost(ch_a, ch_b):
    if ch_a == ch_b:
        return 0
    if ch_a.lower() == ch_b.lower():
        return _CASE_COST
    return _MOVE_COST


def _compute_suggestion_error(exc_value, tb, wrong_name):
    if wrong_name is None or not isinstance(wrong_name, str):
        return None
    if isinstance(exc_value, AttributeError):
        obj = exc_value.obj
        try:
            try:
                d = dir(obj)
            except TypeError:  # Attributes are unsortable, e.g. int and str
                d = list(obj.__class__.__dict__.keys()) + list(obj.__dict__.keys())
            d = sorted([x for x in d if isinstance(x, str)])
            hide_underscored = wrong_name[:1] != "_"
            if hide_underscored and tb is not None:
                while tb.tb_next is not None:
                    tb = tb.tb_next
                frame = tb.tb_frame
                if "self" in frame.f_locals and frame.f_locals["self"] is obj:
                    hide_underscored = False
            if hide_underscored:
                d = [x for x in d if x[:1] != "_"]
        except Exception:
            return None
    elif isinstance(exc_value, ImportError):
        try:
            mod = __import__(exc_value.name)
            try:
                d = dir(mod)
            except TypeError:  # Attributes are unsortable, e.g. int and str
                d = list(mod.__dict__.keys())
            d = sorted([x for x in d if isinstance(x, str)])
            if wrong_name[:1] != "_":
                d = [x for x in d if x[:1] != "_"]
        except Exception:
            return None
    else:
        assert isinstance(exc_value, NameError)
        # find most recent frame
        if tb is None:
            return None
        while tb.tb_next is not None:
            tb = tb.tb_next
        frame = tb.tb_frame
        d = list(frame.f_locals) + list(frame.f_globals) + list(frame.f_builtins)
        d = [x for x in d if isinstance(x, str)]

        # Check first if we are in a method and the instance
        # has the wrong name as attribute
        if "self" in frame.f_locals:
            self = frame.f_locals["self"]
            try:
                has_wrong_name = hasattr(self, wrong_name)
            except Exception:
                has_wrong_name = False
            if has_wrong_name:
                return f"self.{wrong_name}"

    try:
        import _suggestions  # type: ignore[import]
    except ImportError:
        pass
    else:
        return _suggestions._generate_suggestions(d, wrong_name)

    # Compute closest match

    if len(d) > _MAX_CANDIDATE_ITEMS:
        return None
    wrong_name_len = len(wrong_name)
    if wrong_name_len > _MAX_STRING_SIZE:
        return None
    best_distance = wrong_name_len
    suggestion = None
    for possible_name in d:
        if possible_name == wrong_name:
            # A missing attribute is "found". Don't suggest it (see GH-88821).
            continue
        # No more than 1/3 of the involved characters should need changed.
        max_distance = (len(possible_name) + wrong_name_len + 3) * _MOVE_COST // 6
        # Don't take matches we've already beaten.
        max_distance = min(max_distance, best_distance - 1)
        current_distance = _levenshtein_distance(
            wrong_name, possible_name, max_distance
        )
        if current_distance > max_distance:
            continue
        if not suggestion or current_distance < best_distance:
            suggestion = possible_name
            best_distance = current_distance
    return suggestion


def _levenshtein_distance(a, b, max_cost):
    # A Python implementation of Python/suggestions.c:levenshtein_distance.

    # Both strings are the same
    if a == b:
        return 0

    # Trim away common affixes
    pre = 0
    while a[pre:] and b[pre:] and a[pre] == b[pre]:
        pre += 1
    a = a[pre:]
    b = b[pre:]
    post = 0
    while a[: post or None] and b[: post or None] and a[post - 1] == b[post - 1]:
        post -= 1
    a = a[: post or None]
    b = b[: post or None]
    if not a or not b:
        return _MOVE_COST * (len(a) + len(b))
    if len(a) > _MAX_STRING_SIZE or len(b) > _MAX_STRING_SIZE:
        return max_cost + 1

    # Prefer shorter buffer
    if len(b) < len(a):
        a, b = b, a

    # Quick fail when a match is impossible
    if (len(b) - len(a)) * _MOVE_COST > max_cost:
        return max_cost + 1

    # Instead of producing the whole traditional len(a)-by-len(b)
    # matrix, we can update just one row in place.
    # Initialize the buffer row
    row = list(range(_MOVE_COST, _MOVE_COST * (len(a) + 1), _MOVE_COST))

    result = 0
    for bindex in range(len(b)):
        bchar = b[bindex]
        distance = result = bindex * _MOVE_COST
        minimum = sys.maxsize
        for index in range(len(a)):
            # 1) Previous distance in this row is cost(b[:b_index], a[:index])
            substitute = distance + _substitution_cost(bchar, a[index])
            # 2) cost(b[:b_index], a[:index+1]) from previous row
            distance = row[index]
            # 3) existing result is cost(b[:b_index+1], a[index])

            insert_delete = min(result, distance) + _MOVE_COST
            result = min(insert_delete, substitute)

            # cost(b[:b_index+1], a[:index+1])
            row[index] = result
            if result < minimum:
                minimum = result
        if minimum > max_cost:
            # Everything in this row is too big, so bail early.
            return max_cost + 1
    return result
