from __future__ import annotations

import abc
import collections
import colorsys
import enum
import threading
from collections import defaultdict

# Ruff is being stupid and doesn't understand `ClassVar` if it comes from the
# `types` module
from typing import ClassVar

from python_utils import converters, types

from .. import (
    base as pbase,
    env,
)
from .os_specific import getch

ESC = '\x1b'


class CSI:
    _code: str
    _template = ESC + '[{args}{code}'

    def __init__(self, code: str, *default_args) -> None:
        self._code = code
        self._default_args = default_args

    def __call__(self, *args):
        return self._template.format(
            args=';'.join(map(str, args or self._default_args)),
            code=self._code,
        )

    def __str__(self):
        return self()


class CSINoArg(CSI):
    def __call__(self):
        return super().__call__()


#: Cursor Position [row;column] (default = [1,1])
CUP: CSI = CSI('H', 1, 1)

#: Cursor Up Ps Times (default = 1) (CUU)
UP: CSI = CSI('A', 1)

#: Cursor Down Ps Times (default = 1) (CUD)
DOWN: CSI = CSI('B', 1)

#: Cursor Forward Ps Times (default = 1) (CUF)
RIGHT: CSI = CSI('C', 1)

#: Cursor Backward Ps Times (default = 1) (CUB)
LEFT: CSI = CSI('D', 1)

#: Cursor Next Line Ps Times (default = 1) (CNL)
#: Same as Cursor Down Ps Times
NEXT_LINE: CSI = CSI('E', 1)

#: Cursor Preceding Line Ps Times (default = 1) (CPL)
#: Same as Cursor Up Ps Times
PREVIOUS_LINE: CSI = CSI('F', 1)

#: Cursor Character Absolute  [column] (default = [row,1]) (CHA)
COLUMN: CSI = CSI('G', 1)

#: Erase in Display (ED)
CLEAR_SCREEN: CSI = CSI('J', 0)

#: Erase till end of screen
CLEAR_SCREEN_TILL_END: CSINoArg = CSINoArg('0J')

#: Erase till start of screen
CLEAR_SCREEN_TILL_START: CSINoArg = CSINoArg('1J')

#: Erase whole screen
CLEAR_SCREEN_ALL: CSINoArg = CSINoArg('2J')

#: Erase whole screen and history
CLEAR_SCREEN_ALL_AND_HISTORY: CSINoArg = CSINoArg('3J')

#: Erase in Line (EL)
CLEAR_LINE_ALL: CSI = CSI('K')

#: Erase in Line from Cursor to End of Line (default)
CLEAR_LINE_RIGHT: CSINoArg = CSINoArg('0K')

#: Erase in Line from Cursor to Beginning of Line
CLEAR_LINE_LEFT: CSINoArg = CSINoArg('1K')

#: Erase Line containing Cursor
CLEAR_LINE: CSINoArg = CSINoArg('2K')

#: Scroll up Ps lines (default = 1) (SU)
#: Scroll down Ps lines (default = 1) (SD)
SCROLL_UP: CSI = CSI('S')
SCROLL_DOWN: CSI = CSI('T')

#: Save Cursor Position (SCP)
SAVE_CURSOR: CSINoArg = CSINoArg('s')

#: Restore Cursor Position (RCP)
RESTORE_CURSOR: CSINoArg = CSINoArg('u')

#: Cursor Visibility (DECTCEM)
HIDE_CURSOR: CSINoArg = CSINoArg('?25l')
SHOW_CURSOR: CSINoArg = CSINoArg('?25h')


#
# UP = CSI + '{n}A'  # Cursor Up
# DOWN = CSI + '{n}B'  # Cursor Down
# RIGHT = CSI + '{n}C'  # Cursor Forward
# LEFT = CSI + '{n}D'  # Cursor Backward
# NEXT = CSI + '{n}E'  # Cursor Next Line
# PREV = CSI + '{n}F'  # Cursor Previous Line
# MOVE_COLUMN = CSI + '{n}G'  # Cursor Horizontal Absolute
# MOVE = CSI + '{row};{column}H'  # Cursor Position [row;column] (default = [
# 1,1])
#
# CLEAR = CSI + '{n}J'  # Clear (part of) the screen
# CLEAR_BOTTOM = CLEAR.format(n=0)  # Clear from cursor to end of screen
# CLEAR_TOP = CLEAR.format(n=1)  # Clear from cursor to beginning of screen
# CLEAR_SCREEN = CLEAR.format(n=2)  # Clear Screen
# CLEAR_WIPE = CLEAR.format(n=3)  # Clear Screen and scrollback buffer
#
# CLEAR_LINE = CSI + '{n}K'  # Erase in Line
# CLEAR_LINE_RIGHT = CLEAR_LINE.format(n=0)  # Clear from cursor to end of line
# CLEAR_LINE_LEFT = CLEAR_LINE.format(n=1)  # Clear from cursor to beginning
# of line
# CLEAR_LINE_ALL = CLEAR_LINE.format(n=2)  # Clear Line


def clear_line(n):
    return UP(n) + CLEAR_LINE_ALL() + DOWN(n)


# Report Cursor Position (CPR), response = [row;column] as row;columnR
class _CPR(str):  # pragma: no cover
    _response_lock = threading.Lock()

    def __call__(self, stream) -> tuple[int, int]:
        res: str = ''

        with self._response_lock:
            stream.write(str(self))
            stream.flush()

            while not res.endswith('R'):
                char = getch()

                if char is not None:
                    res += char

            res_list = res[2:-1].split(';')

            res_list = tuple(
                int(item) if item.isdigit() else item for item in res_list
            )

            if len(res_list) == 1:
                return types.cast(types.Tuple[int, int], res_list[0])

            return types.cast(types.Tuple[int, int], tuple(res_list))

    def row(self, stream) -> int:
        row, _ = self(stream)
        return row

    def column(self, stream) -> int:
        _, column = self(stream)
        return column


class WindowsColors(enum.Enum):
    BLACK = 0, 0, 0
    BLUE = 0, 0, 128
    GREEN = 0, 128, 0
    CYAN = 0, 128, 128
    RED = 128, 0, 0
    MAGENTA = 128, 0, 128
    YELLOW = 128, 128, 0
    GREY = 192, 192, 192
    INTENSE_BLACK = 128, 128, 128
    INTENSE_BLUE = 0, 0, 255
    INTENSE_GREEN = 0, 255, 0
    INTENSE_CYAN = 0, 255, 255
    INTENSE_RED = 255, 0, 0
    INTENSE_MAGENTA = 255, 0, 255
    INTENSE_YELLOW = 255, 255, 0
    INTENSE_WHITE = 255, 255, 255

    @staticmethod
    def from_rgb(rgb: types.Tuple[int, int, int]) -> WindowsColors:
        """
        Find the closest WindowsColors to the given RGB color.

        >>> WindowsColors.from_rgb((0, 0, 0))
        <WindowsColors.BLACK: (0, 0, 0)>

        >>> WindowsColors.from_rgb((255, 255, 255))
        <WindowsColors.INTENSE_WHITE: (255, 255, 255)>

        >>> WindowsColors.from_rgb((0, 255, 0))
        <WindowsColors.INTENSE_GREEN: (0, 255, 0)>

        >>> WindowsColors.from_rgb((45, 45, 45))
        <WindowsColors.BLACK: (0, 0, 0)>

        >>> WindowsColors.from_rgb((128, 0, 128))
        <WindowsColors.MAGENTA: (128, 0, 128)>
        """

        def color_distance(rgb1, rgb2):
            return sum((c1 - c2) ** 2 for c1, c2 in zip(rgb1, rgb2))

        return min(
            WindowsColors,
            key=lambda color: color_distance(color.value, rgb),
        )


class WindowsColor:
    """
    Windows compatible color class for when ANSI is not supported.
    Currently a no-op because it is not possible to buffer these colors.

    >>> WindowsColor(WindowsColors.RED)('test')
    'test'
    """

    __slots__ = ('color',)

    def __init__(self, color: Color) -> None:
        self.color = color

    def __call__(self, text):
        return text
        ## In the future we might want to use this, but it requires direct
        ## printing to stdout and all of our surrounding functions expect
        ## buffered output so it's not feasible right now. Additionally,
        ## recent Windows versions all support ANSI codes without issue so
        ## there is little need.
        # from progressbar.terminal.os_specific import windows
        # windows.print_color(text, WindowsColors.from_rgb(self.color.rgb))


class RGB(collections.namedtuple('RGB', ['red', 'green', 'blue'])):
    __slots__ = ()

    def __str__(self):
        return self.rgb

    @property
    def rgb(self) -> str:
        return f'rgb({self.red}, {self.green}, {self.blue})'

    @property
    def hex(self) -> str:
        return f'#{self.red:02x}{self.green:02x}{self.blue:02x}'

    @property
    def to_ansi_16(self) -> int:
        # Using int instead of round because it maps slightly better
        red = int(self.red / 255)
        green = int(self.green / 255)
        blue = int(self.blue / 255)
        return (blue << 2) | (green << 1) | red

    @property
    def to_ansi_256(self) -> int:
        red = round(self.red / 255 * 5)
        green = round(self.green / 255 * 5)
        blue = round(self.blue / 255 * 5)
        return 16 + 36 * red + 6 * green + blue

    @property
    def to_windows(self):
        """
        Convert an RGB color (0-255 per channel) to the closest color in the
        Windows 16 color scheme.
        """
        return WindowsColors.from_rgb((self.red, self.green, self.blue))

    def interpolate(self, end: RGB, step: float) -> RGB:
        return RGB(
            int(self.red + (end.red - self.red) * step),
            int(self.green + (end.green - self.green) * step),
            int(self.blue + (end.blue - self.blue) * step),
        )


class HSL(collections.namedtuple('HSL', ['hue', 'saturation', 'lightness'])):
    """
    Hue, Saturation, Lightness color.

    Hue is a value between 0 and 360, saturation and lightness are between 0(%)
    and 100(%).

    """

    __slots__ = ()

    @classmethod
    def from_rgb(cls, rgb: RGB) -> HSL:
        """
        Convert a 0-255 RGB color to a 0-255 HLS color.
        """
        hls = colorsys.rgb_to_hls(
            rgb.red / 255,
            rgb.green / 255,
            rgb.blue / 255,
        )
        return cls(
            round(hls[0] * 360),
            round(hls[2] * 100),
            round(hls[1] * 100),
        )

    def interpolate(self, end: HSL, step: float) -> HSL:
        return HSL(
            self.hue + (end.hue - self.hue) * step,
            self.lightness + (end.lightness - self.lightness) * step,
            self.saturation + (end.saturation - self.saturation) * step,
        )


class ColorBase(abc.ABC):
    def get_color(self, value: float) -> Color:
        raise NotImplementedError()


class Color(
    collections.namedtuple(
        'Color',
        [
            'rgb',
            'hls',
            'name',
            'xterm',
        ],
    ),
    ColorBase,
):
    """
    Color base class.

    This class contains the colors in RGB (Red, Green, Blue), HSL (Hue,
    Lightness, Saturation) and Xterm (8-bit) formats. It also contains the
    color name.

    To make a custom color the only required arguments are the RGB values.
    The other values will be automatically interpolated from that if needed,
    but you can be more explicitly if you wish.
    """

    __slots__ = ()

    def __call__(self, value: str) -> str:
        return self.fg(value)

    @property
    def fg(self) -> SGRColor | WindowsColor:
        if env.COLOR_SUPPORT is env.ColorSupport.WINDOWS:
            return WindowsColor(self)
        else:
            return SGRColor(self, 38, 39)

    @property
    def bg(self) -> DummyColor | SGRColor:
        if env.COLOR_SUPPORT is env.ColorSupport.WINDOWS:
            return DummyColor()
        else:
            return SGRColor(self, 48, 49)

    @property
    def underline(self) -> DummyColor | SGRColor:
        if env.COLOR_SUPPORT is env.ColorSupport.WINDOWS:
            return DummyColor()
        else:
            return SGRColor(self, 58, 59)

    @property
    def ansi(self) -> types.Optional[str]:
        if (
            env.COLOR_SUPPORT is env.ColorSupport.XTERM_TRUECOLOR
        ):  # pragma: no branch
            return f'2;{self.rgb.red};{self.rgb.green};{self.rgb.blue}'

        if self.xterm:  # pragma: no branch
            color = self.xterm
        elif (
            env.COLOR_SUPPORT is env.ColorSupport.XTERM_256
        ):  # pragma: no branch
            color = self.rgb.to_ansi_256
        elif env.COLOR_SUPPORT is env.ColorSupport.XTERM:  # pragma: no branch
            color = self.rgb.to_ansi_16
        else:  # pragma: no branch
            return None

        return f'5;{color}'

    def interpolate(self, end: Color, step: float) -> Color:
        return Color(
            self.rgb.interpolate(end.rgb, step),
            self.hls.interpolate(end.hls, step),
            self.name if step < 0.5 else end.name,
            self.xterm if step < 0.5 else end.xterm,
        )

    def __str__(self):
        return self.name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r})'

    def __hash__(self) -> int:
        return hash(self.rgb)


class Colors:
    by_name: ClassVar[defaultdict[str, types.List[Color]]] = (
        collections.defaultdict(list)
    )
    by_lowername: ClassVar[defaultdict[str, types.List[Color]]] = (
        collections.defaultdict(list)
    )
    by_hex: ClassVar[defaultdict[str, types.List[Color]]] = (
        collections.defaultdict(list)
    )
    by_rgb: ClassVar[defaultdict[RGB, types.List[Color]]] = (
        collections.defaultdict(list)
    )
    by_hls: ClassVar[defaultdict[HSL, types.List[Color]]] = (
        collections.defaultdict(list)
    )
    by_xterm: ClassVar[dict[int, Color]] = dict()

    @classmethod
    def register(
        cls,
        rgb: RGB,
        hls: types.Optional[HSL] = None,
        name: types.Optional[str] = None,
        xterm: types.Optional[int] = None,
    ) -> Color:
        color = Color(rgb, hls, name, xterm)

        if name:
            cls.by_name[name].append(color)
            cls.by_lowername[name.lower()].append(color)

        if hls is None:
            hls = HSL.from_rgb(rgb)

        cls.by_hex[rgb.hex].append(color)
        cls.by_rgb[rgb].append(color)
        cls.by_hls[hls].append(color)

        if xterm is not None:
            cls.by_xterm[xterm] = color

        return color

    @classmethod
    def interpolate(cls, color_a: Color, color_b: Color, step: float) -> Color:
        return color_a.interpolate(color_b, step)


class ColorGradient(ColorBase):
    def __init__(self, *colors: Color, interpolate=Colors.interpolate) -> None:
        assert colors
        self.colors = colors
        self.interpolate = interpolate

    def __call__(self, value: float) -> Color:
        return self.get_color(value)

    def get_color(self, value: float) -> Color:
        "Map a value from 0 to 1 to a color."
        if (
            value == pbase.Undefined
            or value == pbase.UnknownLength
            or value <= 0
        ):
            return self.colors[0]
        elif value >= 1:
            return self.colors[-1]

        max_color_idx = len(self.colors) - 1
        if max_color_idx == 0:
            return self.colors[0]
        elif self.interpolate:
            if max_color_idx > 1:
                index = round(
                    converters.remap(value, 0, 1, 0, max_color_idx - 1),
                )
            else:
                index = 0

            step = converters.remap(
                value,
                index / (max_color_idx),
                (index + 1) / (max_color_idx),
                0,
                1,
            )
            color = self.interpolate(
                self.colors[index],
                self.colors[index + 1],
                float(step),
            )
        else:
            index = round(converters.remap(value, 0, 1, 0, max_color_idx))
            color = self.colors[index]

        return color


OptionalColor = types.Union[Color, ColorGradient, None]


def get_color(value: float, color: OptionalColor) -> Color | None:
    if isinstance(color, ColorGradient):
        color = color(value)
    return color


def apply_colors(
    text: str,
    percentage: float | None = None,
    *,
    fg: OptionalColor = None,
    bg: OptionalColor = None,
    fg_none: Color | None = None,
    bg_none: Color | None = None,
    **kwargs: types.Any,
) -> str:
    """Apply colors/gradients to a string depending on the given percentage.

    When percentage is `None`, the `fg_none` and `bg_none` colors will be used.
    Otherwise, the `fg` and `bg` colors will be used. If the colors are
    gradients, the color will be interpolated depending on the percentage.
    """
    if percentage is None:
        if fg_none is not None:
            text = fg_none.fg(text)
        if bg_none is not None:
            text = bg_none.bg(text)
    elif fg is not None or bg is not None:
        fg = get_color(percentage * 0.01, fg)
        bg = get_color(percentage * 0.01, bg)

        if fg is not None:  # pragma: no branch
            text = fg.fg(text)
        if bg is not None:  # pragma: no branch
            text = bg.bg(text)

    return text


class DummyColor:
    def __call__(self, text):
        return text

    def __repr__(self) -> str:
        return 'DummyColor()'


class SGR(CSI):
    _start_code: int
    _end_code: int
    _code = 'm'
    __slots__ = '_start_code', '_end_code'

    def __init__(self, start_code: int, end_code: int) -> None:
        self._start_code = start_code
        self._end_code = end_code

    @property
    def _start_template(self):
        return super().__call__(self._start_code)

    @property
    def _end_template(self):
        return super().__call__(self._end_code)

    def __call__(self, text, *args):
        return self._start_template + text + self._end_template


class SGRColor(SGR):
    __slots__ = '_color', '_start_code', '_end_code'

    def __init__(self, color: Color, start_code: int, end_code: int) -> None:
        self._color = color
        super().__init__(start_code, end_code)

    @property
    def _start_template(self):
        return CSI.__call__(self, self._start_code, self._color.ansi)


encircled: SGR = SGR(52, 54)
framed: SGR = SGR(51, 54)
overline: SGR = SGR(53, 55)
bold: SGR = SGR(1, 22)
gothic: SGR = SGR(20, 10)
italic: SGR = SGR(3, 23)
strike_through: SGR = SGR(9, 29)
fast_blink: SGR = SGR(6, 25)
slow_blink: SGR = SGR(5, 25)
underline: SGR = SGR(4, 24)
double_underline: SGR = SGR(21, 24)
faint: SGR = SGR(2, 22)
inverse: SGR = SGR(7, 27)
