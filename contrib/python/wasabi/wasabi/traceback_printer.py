import os
from typing import Optional, Tuple, Union

from .util import NO_UTF8, color, supports_ansi

LINE_EDGE = "└─" if not NO_UTF8 else "|_"
LINE_FORK = "├─" if not NO_UTF8 else "|__"
LINE_PATH = "──" if not NO_UTF8 else "__"


class TracebackPrinter(object):
    def __init__(
        self,
        color_error: Union[str, int] = "red",
        color_tb: Union[str, int] = "blue",
        color_highlight: Union[str, int] = "yellow",
        indent: int = 2,
        tb_base: Optional[str] = None,
        tb_exclude: Tuple = tuple(),
        tb_range_start: int = -5,
        tb_range_end: int = -2,
    ):
        """Initialize a traceback printer.

        color_error (Union[str, int]): Color name or code for errors.
        color_tb (Union[str, int]): Color name or code for traceback headline.
        color_highlight (Union[str, int]): Color name or code for highlights.
        indent (int): Indentation in spaces.
        tb_base (Optional[str]): Optional name of directory to use to show relative paths. For
            example, "thinc" will look for the last occurence of "/thinc/" in
            a path and only show path to the right of it.
        tb_exclude (tuple): List of filenames to exclude from traceback.
        tb_range_start (int): The starting index from a traceback to include.
        tb_range_end (int): The final index from a traceback to include. If None
            the traceback will continue until the last record.
        RETURNS (TracebackPrinter): The traceback printer.
        """
        self.color_error = color_error
        self.color_tb = color_tb
        self.color_highlight = color_highlight
        self.indent = " " * indent
        if tb_base == ".":
            tb_base = "{}{}".format(os.getcwd(), os.path.sep)
        elif tb_base is not None:
            tb_base = "/{}/".format(tb_base)
        self.tb_base = tb_base
        self.tb_exclude = tuple(tb_exclude)
        self.tb_range_start = tb_range_start
        self.tb_range_end = tb_range_end
        self.supports_ansi = supports_ansi()

    def __call__(self, title: str, *texts, **settings) -> str:
        """Output custom formatted tracebacks and errors.

        title (str): The message title.
        *texts (str): The texts to print (one per line).
        RETURNS (str): The formatted traceback. Can be printed or raised
            by custom exception.
        """
        highlight = settings.get("highlight", False)
        tb = settings.get("tb", None)
        if self.supports_ansi:  # use first line as title
            title = color(title, fg=self.color_error, bold=True)
        info = "\n" + "\n".join([self.indent + text for text in texts]) if texts else ""
        tb = self._get_traceback(tb, highlight) if tb else ""
        msg = "\n\n{}{}{}{}\n".format(self.indent, title, info, tb)
        return msg

    def _get_traceback(self, tb, highlight):
        # Exclude certain file names from traceback
        tb = [record for record in tb if not record[0].endswith(self.tb_exclude)]
        tb_range = (
            tb[self.tb_range_start : self.tb_range_end]
            if self.tb_range_end is not None
            else tb[self.tb_range_start :]
        )
        tb_list = [
            self._format_traceback(path, line, fn, text, i, len(tb_range), highlight)
            for i, (path, line, fn, text) in enumerate(tb_range)
        ]
        tb_data = "\n".join(tb_list).strip()
        title = "Traceback:"
        if self.supports_ansi:
            title = color(title, fg=self.color_tb, bold=True)
        return "\n\n{indent}{title}\n{indent}{tb}".format(
            title=title, tb=tb_data, indent=self.indent
        )

    def _format_traceback(self, path, line, fn, text, i, count, highlight):
        template = "{base_indent}{indent} {fn} in {path}:{line}{text}"
        indent = (LINE_EDGE if i == count - 1 else LINE_FORK) + LINE_PATH * i
        if self.tb_base and self.tb_base in path:
            path = path.rsplit(self.tb_base, 1)[1]
        text = self._format_user_error(text, i, highlight) if i == count - 1 else ""
        if self.supports_ansi:
            fn = color(fn, bold=True)
            path = color(path, underline=True)
        return template.format(
            base_indent=self.indent,
            line=line,
            indent=indent,
            text=text,
            fn=fn,
            path=path,
        )

    def _format_user_error(self, text, i, highlight):
        spacing = "  " * i + " >>>"
        if self.supports_ansi:
            spacing = color(spacing, fg=self.color_error)
        if highlight and self.supports_ansi:
            formatted_highlight = color(highlight, fg=self.color_highlight)
            text = text.replace(highlight, formatted_highlight)
        return "\n{}  {} {}".format(self.indent, spacing, text)
