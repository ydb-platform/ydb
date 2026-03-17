import os
from itertools import zip_longest
from typing import Collection, Dict, Iterable, List, Optional, Sequence, Union
from typing import cast

from .compat import Literal
from .util import COLORS
from .util import color as _color
from .util import supports_ansi

ALIGN_MAP = {"l": "<", "r": ">", "c": "^"}


def table(
    # fmt: off
    data: Union[Collection, Dict],
    header: Optional[Iterable] = None,
    footer: Optional[Iterable] = None,
    divider: bool = False,
    widths: Union[Iterable[int], Literal["auto"]] = "auto",
    max_col: int = 30,
    spacing: int = 3,
    aligns: Optional[Union[Iterable[Literal["r", "c", "l"]], Literal["r", "c", "l"]]] = None,
    multiline: bool = False,
    env_prefix: str = "WASABI",
    color_values: Optional[Dict] = None,
    fg_colors: Optional[Iterable] = None,
    bg_colors: Optional[Iterable] = None,
    # fmt: on
) -> str:
    """Format tabular data.

    data (Union[Collection, Dict]): The data to render. Either a list of lists (one per
        row) or a dict for two-column tables.
    header (Optional[Iterable]): Optional header columns.
    footer (Optional[Iterable]): Optional footer columns.
    divider (bool): Show a divider line between header/footer and body.
    widths (Union[Iterable[int], Literal['auto']]): Column widths in order. If "auto", widths
        will be calculated automatically based on the largest value.
    max_col (int): Maximum column width.
    spacing (int): Spacing between columns, in spaces.
    aligns (Optional[Union[Iterable[str], str]]): Optional column alignments
        in order. 'l' (left, default), 'r' (right) or 'c' (center). If a string,
        value is used for all columns.
    multiline (bool): If a cell value is a list of a tuple, render it on
        multiple lines, with one value per line.
    env_prefix (str): Prefix for environment variables, e.g.
        WASABI_LOG_FRIENDLY.
    color_values (Optional[Dict]): Optional color values to add or overwrite, name mapped to value.
    fg_colors (Optional[Iterable]): Optional foreground colors, one per column. None can be specified
        for individual columns to retain the default foreground color.
    bg_colors (Optional[Iterable]): Optional background colors, one per column. None can be specified
        for individual columns to retain the default background color.
    RETURNS (str): The formatted table.
    """
    if fg_colors is not None or bg_colors is not None:
        colors = dict(COLORS)
        if color_values is not None:
            colors.update(color_values)
        if fg_colors is not None:
            fg_colors = [colors.get(fg_color, fg_color) for fg_color in fg_colors]
        if bg_colors is not None:
            bg_colors = [colors.get(bg_color, bg_color) for bg_color in bg_colors]
    if isinstance(data, dict):
        data = list(data.items())
    if multiline:
        zipped_data = []
        for i, item in enumerate(data):
            vals = [v if isinstance(v, (list, tuple)) else [v] for v in item]
            zipped_data.extend(list(zip_longest(*vals, fillvalue="")))
            if i < len(data) - 1:
                zipped_data.append(tuple(["" for i in item]))
        data = zipped_data
    if widths == "auto":
        widths = _get_max_widths(data, header, footer, max_col)
    settings = {
        "widths": widths,
        "spacing": spacing,
        "aligns": aligns,
        "env_prefix": env_prefix,
        "fg_colors": fg_colors,
        "bg_colors": bg_colors,
    }
    divider_row = row(["-" * width for width in widths], **settings)  # type: ignore
    rows = []
    if header:
        rows.append(row(header, **settings))  # type: ignore
        if divider:
            rows.append(divider_row)
    for i, item in enumerate(data):
        rows.append(row(item, **settings))  # type: ignore
    if footer:
        if divider:
            rows.append(divider_row)
        rows.append(row(footer, **settings))  # type: ignore
    return "\n{}\n".format("\n".join(rows))


def row(
    data: Collection,
    widths: Union[Sequence[int], int, Literal["auto"]] = "auto",
    spacing: int = 3,
    aligns: Optional[Union[Sequence[Literal["r", "c", "l"]], str]] = None,
    env_prefix: str = "WASABI",
    fg_colors: Optional[Sequence] = None,
    bg_colors: Optional[Sequence] = None,
) -> str:
    """Format data as a table row.

    data (Collection): The individual columns to format.
    widths (Union[Sequence[int], int, Literal['auto']]): Column widths, either one integer for all
        columns or an iterable of values. If "auto", widths will be calculated
        automatically based on the largest value.
    spacing (int): Spacing between columns, in spaces.
    aligns (Optional[Union[Sequence[Literal['r', 'c', 'l']], str]]): Optional column
        alignments in order. 'l' (left, default), 'r' (right) or 'c' (center).
        If a string, value is used for all columns.
    env_prefix (str): Prefix for environment variables, e.g.
        WASABI_LOG_FRIENDLY.
    fg_colors (Optional[Sequence]): Optional foreground colors for the columns, in order. None can be
        specified for individual columns to retain the default foreground color.
    bg_colors (Optional[Sequence]): Optional background colors for the columns, in order. None can be
        specified for individual columns to retain the default background color.
    RETURNS (str): The formatted row.
    """
    env_log_friendly = os.getenv("{}_LOG_FRIENDLY".format(env_prefix), False)
    show_colors = (
        supports_ansi()
        and not env_log_friendly
        and (fg_colors is not None or bg_colors is not None)
    )
    cols: List[str] = []
    _aligns = (
        [aligns for _ in data] if isinstance(aligns, str) else cast(List[str], aligns)
    )
    if not hasattr(widths, "__iter__") and widths != "auto":  # single number
        widths = cast(List[int], [widths for _ in range(len(data))])
    for i, col in enumerate(data):
        align = ALIGN_MAP.get(_aligns[i] if _aligns and i < len(_aligns) else "l")
        col_width = len(col) if widths == "auto" else cast(List[int], widths)[i]
        tpl = "{:%s%d}" % (align, col_width)
        col = tpl.format(str(col))
        if show_colors:
            fg = fg_colors[i] if fg_colors is not None else None
            bg = bg_colors[i] if bg_colors is not None else None
            col = _color(col, fg=fg, bg=bg)
        cols.append(col)
    return (" " * spacing).join(cols)


def _get_max_widths(data, header, footer, max_col):
    all_data = list(data)
    if header:
        all_data.append(header)
    if footer:
        all_data.append(footer)
    widths = [[len(str(col)) for col in item] for item in all_data]
    return [min(max(w), max_col) for w in list(zip(*widths))]
