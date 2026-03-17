#  Copyright (c) 2021, Manfred Moitzi
#  License: MIT License
from typing import Sequence


def group_chars(s: str, n: int = 4, sep="-") -> str:
    chars = []
    for index, char in enumerate(reversed(s)):
        if index % n == 0:
            chars.append(sep)
        chars.append(char)
    if chars:
        chars.reverse()
        chars.pop()
        return "".join(chars)
    return ""


def bitmask_strings(
    value: int, base: int = 10, sep: str = "-"
) -> Sequence[str]:
    if base == 10:
        top, bottom = (
            "3322-2222-2222-1111-1111-1100-0000-0000",
            "1098-7654-3210-9876-5432-1098-7654-3210",
        )
    elif base == 16:
        top, bottom = (
            "1111-1111-1111-1111-0000-0000-0000-0000",
            "FEDC-BA98-7654-3210-FEDC-BA98-7654-3210",
        )
    else:
        raise ValueError(f"invalid base {base}, valid bases: 10, 16")
    top = top.replace("-", sep)
    bottom = bottom.replace("-", sep)
    l0 = len(top)
    bin_str = group_chars(bin(value)[2:], n=4, sep=sep)
    l1 = len(bin_str)
    return [
        top[l0 - l1 :],
        bottom[l0 - l1 :],
        bin_str,
    ]


def print_bitmask(value: int, *, base=10, sep="-"):
    lines = bitmask_strings(value, base, sep)
    assert len(lines) > 2
    divider_line = "=" * (max(map(len, lines)) + 4)
    print(divider_line)
    print("x0 :" + lines[0])
    print("0x :" + lines[1])
    print(divider_line)
    print("bin:" + lines[2])
