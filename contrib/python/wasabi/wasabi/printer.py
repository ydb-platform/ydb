import datetime
import itertools
import os
import sys
import time
import traceback
from collections import Counter
from contextlib import contextmanager
from multiprocessing import Process
from typing import Any, Collection, Dict, NoReturn, Optional, Union, cast, overload

from .compat import Literal
from .tables import row, table
from .util import COLORS, ICONS, MESSAGES, can_render
from .util import color as _color
from .util import locale_escape, supports_ansi, wrap


class Printer(object):
    def __init__(
        self,
        pretty: bool = True,
        no_print: bool = False,
        colors: Optional[Dict] = None,
        icons: Optional[Dict] = None,
        line_max: int = 80,
        animation: str = "⠙⠹⠸⠼⠴⠦⠧⠇⠏",
        animation_ascii: str = "|/-\\",
        hide_animation: bool = False,
        ignore_warnings: bool = False,
        env_prefix: str = "WASABI",
        timestamp: bool = False,
    ):
        """Initialize the command-line printer.

        pretty (bool): Pretty-print output (colors, icons).
        no_print (bool): Don't actually print, just return.
        colors (Optional[Dict]): Optional color values to add or overwrite, name mapped to value.
        icons (Optional[Dict]): Optional icons to add or overwrite. Name mapped to unicode icon.
        line_max (int): Maximum line length (for divider).
        animation (str): Steps of loading animation for loading() method.
        animation_ascii (str): Alternative animation for ASCII terminals.
        hide_animation (bool): Don't display animation, e.g. for logs.
        ignore_warnings (bool): Do not output messages of type MESSAGE.WARN.
        env_prefix (str): Prefix for environment variables, e.g.
            WASABI_LOG_FRIENDLY.
        timestamp (bool): Print a timestamp (default False).
        RETURNS (Printer): The initialized printer.
        """
        env_log_friendly = os.getenv("{}_LOG_FRIENDLY".format(env_prefix), False)
        env_no_pretty = os.getenv("{}_NO_PRETTY".format(env_prefix), False)
        self._counts: Counter = Counter()
        self.pretty = pretty and not env_no_pretty
        self.no_print = no_print
        self.show_color = supports_ansi() and not env_log_friendly
        self.hide_animation = hide_animation or env_log_friendly
        self.ignore_warnings = ignore_warnings
        self.line_max = line_max
        self.colors = dict(COLORS)
        self.icons = dict(ICONS)
        self.timestamp = timestamp
        if colors:
            self.colors.update(colors)
        if icons:
            self.icons.update(icons)
        self.anim = animation if can_render(animation) else animation_ascii

    @property
    def counts(self) -> Counter:
        """Get the counts of how often the special printers were fired,
        e.g. MESSAGES.GOOD. Can be used to print an overview like "X warnings".
        """
        return self._counts

    def good(
        self,
        title: Any = "",
        text: Any = "",
        show: bool = True,
        spaced: bool = False,
        exits: Optional[int] = None,
    ):
        """Print a success message.

        title (Any): The main text to print.
        text (Any): Optional additional text to print.
        show (bool): Whether to print or not. Can be used to only output
            messages under certain condition, e.g. if --verbose flag is set.
        spaced (bool): Whether to add newlines around the output.
        exits (Optional[int]): Optional toggle to perform a system exit.
        """
        return self._get_msg(
            title, text, style=MESSAGES.GOOD, show=show, spaced=spaced, exits=exits
        )

    @overload
    def fail(
        self,
        title: Any = "",
        text: Any = "",
        show: bool = True,
        spaced: bool = False,
        exits: Optional[Literal[0, False]] = None,
    ):
        ...

    @overload
    def fail(
        self,
        title: Any = "",
        text: Any = "",
        show: bool = True,
        spaced: bool = False,
        exits: Literal[1, True] = True,
    ) -> NoReturn:
        ...

    def fail(
        self,
        title: Any = "",
        text: Any = "",
        show: bool = True,
        spaced: bool = False,
        exits: Optional[Union[int, bool]] = None,
    ) -> Union[str, None, NoReturn]:
        """Print an error message.

        title (Any): The main text to print.
        text (Any): Optional additional text to print.
        show (bool): Whether to print or not. Can be used to only output
            messages under certain condition, e.g. if --verbose flag is set.
        spaced (bool): Whether to add newlines around the output.
        exits (Optional[int]): Optional toggle to perform a system exit.
        """
        return self._get_msg(
            title, text, style=MESSAGES.FAIL, show=show, spaced=spaced, exits=exits
        )

    def warn(
        self,
        title: Any = "",
        text: Any = "",
        show: bool = True,
        spaced: bool = False,
        exits: Optional[int] = None,
    ):
        """Print a warning message.

        title (Any): The main text to print.
        text (Any): Optional additional text to print.
        show (bool): Whether to print or not. Can be used to only output
            messages under certain condition, e.g. if --verbose flag is set.
        spaced (bool): Whether to add newlines around the output.
        exits (Optional[int]): Optional toggle to perform a system exit.
        """
        return self._get_msg(
            title, text, style=MESSAGES.WARN, show=show, spaced=spaced, exits=exits
        )

    def info(
        self,
        title: Any = "",
        text: Any = "",
        show: bool = True,
        spaced: bool = False,
        exits: Optional[int] = None,
    ):
        """Print an informational message.

        title (Any): The main text to print.
        text (Any): Optional additional text to print.
        show (bool): Whether to print or not. Can be used to only output
            messages under certain condition, e.g. if --verbose flag is set.
        spaced (bool): Whether to add newlines around the output.
        exits (Optional[int]): Optional toggle to perform a system exit.
        """
        return self._get_msg(
            title, text, style=MESSAGES.INFO, show=show, spaced=spaced, exits=exits
        )

    def text(
        self,
        title: Any = "",
        text: Any = "",
        color: Optional[Union[str, int]] = None,
        bg_color: Optional[Union[str, int]] = None,
        icon: Optional[str] = None,
        spaced: bool = False,
        show: bool = True,
        no_print: bool = False,
        exits: Optional[int] = None,
    ):
        """Print a message.

        title (Any): The main text to print.
        text (Any): Optional additional text to print.
        color (Optional[Union[str, int]]): Optional foreground color.
        bg_color (Optional[Union[str, int]]): Optional background color.
        icon (Optional[str]): Optional name of icon to add.
        spaced (bool): Whether to add newlines around the output.
        show (bool): Whether to print or not. Can be used to only output
            messages under certain condition, e.g. if --verbose flag is set.
        no_print (bool): Don't actually print, just return.
        exits (Optional[int]): Perform a system exit. Optional.
        """
        if not show:
            return
        if self.pretty:
            color = self.colors.get(cast(str, color), color)
            bg_color = self.colors.get(cast(str, bg_color), bg_color)
            icon = self.icons.get(cast(str, icon))
            if icon:
                title = locale_escape("{} {}".format(icon, title)).strip()
            if self.show_color:
                title = _color(title, fg=color, bg=bg_color)
            title = wrap(title, indent=0)
        if text:
            title = "{}\n{}".format(title, wrap(text, indent=0))
        if self.timestamp:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            title = "{}\t{}".format(now, title)
        if exits is not None or spaced:
            title = "\n{}\n".format(title)
        if not self.no_print and not no_print:
            print(title)
        if exits is not None:
            sys.stdout.flush()
            sys.stderr.flush()
            if self.no_print or no_print and exits != 0:
                try:
                    raise RuntimeError(title.strip())
                except Exception as e:
                    # Remove wasabi from the traceback and re-raise
                    tb = "\n".join(traceback.format_stack()[:-3])
                    raise SystemExit("{}\n{}".format(tb, e))
            sys.exit(exits)
        if self.no_print or no_print:
            return title

    def divider(
        self,
        text: str = "",
        char: str = "=",
        show: bool = True,
        icon: Optional[str] = None,
    ):
        """Print a divider with a headline:
        ============================ Headline here ===========================

        text (str): Headline text. If empty, only the line is printed.
        char (str): Line character to repeat, e.g. =.
        show (bool): Whether to print or not.
        icon (Optional[str]): Optional icon to display with title.
        """
        if not show:
            return
        if len(char) != 1:
            raise ValueError(
                "Divider chars need to be one character long. "
                "Received: {}".format(char)
            )
        if self.pretty:
            icon = self.icons.get(cast(str, icon))
            if icon:
                text = locale_escape("{} {}".format(icon, text)).strip()
            deco = char * (int(round((self.line_max - len(text))) / 2) - 2)
            text = " {} ".format(text) if text else ""
            text = _color(
                "\n{deco}{text}{deco}".format(deco=deco, text=text), bold=True
            )
            if len(text) < self.line_max:
                text = text + char * (self.line_max - len(text))
        if self.no_print:
            return text
        print(text)

    def table(self, data: Union[Collection, Dict], **kwargs):
        """Print data as a table.

        data (Union[Collection, Dict]): The data to render. Either a list of lists
            (one per row) or a dict for two-column tables.
        kwargs: Table settings. See tables.table for details.
        """
        title = kwargs.pop("title", None)
        text = table(data, **kwargs)
        if title:
            self.divider(title)
        if self.no_print:
            return text
        print(text)

    def row(self, data: Collection, **kwargs):
        """Print a table row.

        data (Collection): The individual columns to format.
        kwargs: Row settings. See tables.row for details.
        """
        text = row(data, **kwargs)
        if self.no_print:
            return text
        print(text)

    @contextmanager
    def loading(self, text: str = "Loading..."):
        if self.no_print:
            yield
        elif self.hide_animation:
            print(text)
            yield
        else:
            sys.stdout.flush()
            t = Process(target=self._spinner, args=(text,))
            t.start()
            try:
                yield
            except Exception as e:
                # Handle exception inside the with block
                t.terminate()
                sys.stdout.write("\n")
                raise (e)
            t.terminate()
            sys.stdout.write("\r\x1b[2K")  # erase line
            sys.stdout.flush()

    def _spinner(self, text: str = "Loading..."):
        for char in itertools.cycle(self.anim):
            sys.stdout.write("\r{} {}".format(char, text))
            sys.stdout.flush()
            time.sleep(0.1)

    def _get_msg(
        self,
        title: Any,
        text: Any,
        style: Optional[str] = None,
        show: bool = False,
        spaced: bool = False,
        exits: Optional[int] = None,
    ):
        if self.ignore_warnings and style == MESSAGES.WARN:
            show = False
        self._counts[style] += 1
        return self.text(
            title, text, color=style, icon=style, show=show, spaced=spaced, exits=exits
        )
