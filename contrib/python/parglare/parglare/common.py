from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from parglare.parser import LRStackNode

from parglare import termui as t
from parglare.termui import s_attention as _a


class Location:
    """
    Represents a location (point or span) of the object in the source code.

    Args:
    context(Context): Parsing context used to populate this object.

    Attributes:
    input_str: The input string (from context) being parsed.
    file_name(str): The name (path) to the file this location refers to.
    start_position(int): The position of the span if applicable
    end_position(int): The end of the span if applicable.
    line, column (int): The line/column calculated from the position start and
        input_str.
    line_end, column_end (int): The line/column calculated from the position
        end and input_str.
    """

    __slots__ = [
        "start_position",
        "end_position",
        "input_str",
        "file_name",
        "_line",
        "_column",
        "_line_end",
        "_column_end",
    ]

    def __init__(
        self,
        context: Optional[Union["LRStackNode", "ErrorContext"]] = None,
        file_name: Optional[str] = None,
    ):
        self.start_position: Union[int, None] = (
            context.start_position if context else None
        )
        self.end_position: Union[int, None] = context.end_position if context else None
        self.input_str: Union[str, None] = context.input_str if context else None
        self.file_name: Union[str, None] = (
            file_name or context.file_name if context else None
        )

        # Evaluate this only when string representation is needed.
        # E.g. during error reporting
        self._line = None
        self._column = None

        self._line_end = None
        self._column_end = None

    @property
    def line(self):
        if self._line is None:
            self.evaluate_line_col()
        return self._line

    @property
    def line_end(self):
        if self._line_end is None:
            self.evaluate_line_col_end()
        return self._line_end

    @property
    def column(self):
        if self._column is None:
            self.evaluate_line_col()
        return self._column

    @property
    def column_end(self):
        if self._column_end is None:
            self.evaluate_line_col_end()
        return self._column_end

    def is_eof(self):
        return self.input_str is not None and self.start_position == len(self.input_str)

    def evaluate_line_col(self):
        self._line, self._column = pos_to_line_col(self.input_str, self.start_position)

    def evaluate_line_col_end(self):
        if self.end_position:
            self._line_end, self._column_end = pos_to_line_col(
                self.input_str, self.end_position
            )

    def __str__(self):
        line, column = self.line, self.column
        if line is not None:
            return "{}{}:{}".format(
                f"{self.file_name}:" if self.file_name else "", line, column
            )
        if self.file_name:
            return _a(self.file_name)
        return "<Unknown location>"

    def __repr__(self):
        return str(self)


def position_context(input_str, position):
    """
    Returns position context string.
    """
    start = max(position - 10, 0)
    c = (
        str(input_str[start:position])
        + _a(" **> ")
        + str(input_str[position : position + 10])
    )
    return replace_newlines(c)


def replace_newlines(in_str):
    try:
        return in_str.replace("\n", "\\n")
    except AttributeError:
        return in_str


def load_python_module(mod_name, mod_path):
    """
    Loads Python module from an arbitrary location.
    See https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
    """  # noqa: E501
    import importlib.util

    spec = importlib.util.spec_from_file_location(mod_name, mod_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def get_collector():
    """
    Produces action/recognizers collector/decorator that will collect all
    decorated objects under dictionary attribute `all`.
    """
    all = {}

    class Collector:
        def __call__(self, name_or_f):
            """
            If called with action/recognizer name return decorator.
            If called over function apply decorator.
            """
            is_name = isinstance(name_or_f, str)

            def decorator(f):
                name = name_or_f if is_name else f.__name__
                objects = all.get(name)
                if objects:
                    if isinstance(objects, list):
                        objects.append(f)
                    else:
                        all[name] = [objects, f]
                else:
                    all[name] = f
                return f

            if is_name:
                return decorator
            else:
                return decorator(name_or_f)

    objects = Collector()
    objects.all = all
    return objects


def pos_to_line_col(input_str, position):
    """
    Returns position in the (line,column) form.
    """

    if position is None:
        return None, None

    if not isinstance(input_str, str):
        # If we are not parsing string
        return 1, position

    line = input_str[:position].count("\n") + 1
    line_start_pos = input_str.rfind("\n", 0, position)
    column = position - line_start_pos - 1

    return line, column


def dot_escape(s):
    colors = t.colors
    t.colors = False
    s = str(s)
    out = (
        s.replace("\n", r"\n")
        .replace("\\", "\\\\")
        .replace('"', r"\"")
        .replace("|", r"\|")
        .replace("{", r"\{")
        .replace("}", r"\}")
        .replace(">", r"\>")
        .replace("<", r"\<")
        .replace("?", r"\?")
    )
    t.colors = colors
    return out


class ErrorContext:
    """
    Context for errors.  Errors are constructed from parsing heads and are
    represented as location span.  Initially, the start and end of the span are
    set to the position where the error is found but end of the span can be
    moved forward during error recovery.
    """

    __slots__ = ["input_str", "file_name", "start_position", "end_position"]

    def __init__(self, context):
        self.start_position = self.end_position = context.position
        self.input_str = context.input_str
        self.file_name = context.file_name
