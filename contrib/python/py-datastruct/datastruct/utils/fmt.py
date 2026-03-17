#  Copyright (c) Kuba Szczodrzyński 2023-1-3.

from typing import Union

from datastruct.types import Context, Endianness, FormatType, Value

from .context import evaluate

FMT_ENDIAN = "@=<>!"
FMT_SPEC = "cbB?hHiIlLqQnNefds"


def fmt_check(fmt: Value[str]) -> None:
    """
    Check the passed format specifier.
    Do nothing for lambdas; that needs to be checked later on.
    """
    if callable(fmt) or isinstance(fmt, int):
        return
    orig_fmt = fmt
    if fmt[0] in FMT_ENDIAN:
        fmt = fmt[1:]
    count = fmt[:-1]
    spec = fmt[-1]
    if spec not in FMT_SPEC:
        raise ValueError(f"Format specifier '{orig_fmt}' is invalid or unsupported")
    if count and not count.isnumeric():
        raise ValueError(f"Format specifier '{orig_fmt}' has non-numeric count")


def fmt_evaluate(
    ctx: Context,
    fmt_val: FormatType,
    endianness: Endianness,
) -> Union[str, int]:
    """
    First evaluate(), then fmt_check() the given format (if it's a lambda).
    Set endianness if not set already.

    :return: a valid format specifier, with endianness applied
    """
    fmt = evaluate(ctx, fmt_val)
    if isinstance(fmt, int):
        return fmt
    if not fmt:
        raise ValueError("Field has no format specifier")
    if callable(fmt_val):
        fmt_check(fmt)
    if fmt[0] not in FMT_ENDIAN:
        fmt = endianness.value + fmt
    if fmt[-1] == "s" and fmt[1:-1].isnumeric():
        return int(fmt[1:-1])
    return fmt
