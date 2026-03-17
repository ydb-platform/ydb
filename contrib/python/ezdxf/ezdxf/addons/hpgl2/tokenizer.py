#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import NamedTuple, Sequence


class Command(NamedTuple):
    name: str
    args: Sequence[bytes]


ESCAPE = 27
SEMICOLON = ord(";")
PERCENT = ord("%")
MINUS = ord("-")
QUOTE_CHAR = ord('"')
CHAR_A = ord("A")
CHAR_B = ord("B")
CHAR_Z = ord("Z")
DEFAULT_TEXT_TERMINATOR = 3

# Enter HPGL/2 mode commands
# b"%-0B",  # ??? not documented (assumption)
# b"%-1B",  # ??? not documented (really exist)
# b"%-2B",  # ??? not documented (assumption)
# b"%-3B",  # ??? not documented (assumption)
# b"%0B",  # documented in the HPGL2 reference by HP
# b"%1B",  # documented
# b"%2B",  # documented
# b"%3B",  # documented


def get_enter_hpgl2_mode_command_length(s: bytes, i: int) -> int:
    try:
        if s[i] != ESCAPE:
            return 0
        if s[i + 1] != PERCENT:
            return 0
        length = 4
        if s[i + 2] == MINUS:
            i += 1
            length = 5
        # 0, 1, 2 or 3 + "B"
        if 47 < s[i + 2] < 52 and s[i + 3] == CHAR_B:
            return length
    except IndexError:
        pass
    return 0


KNOWN_START_SEQUENCES = [b"BPIN", b"BP;IN", b"INPS", b"IN;PS", b"INDF", b"IN;DF"]


def has_known_start_sequence(b: bytes) -> bool:
    for start_sequence in KNOWN_START_SEQUENCES:
        if b.startswith(start_sequence):
            return True
    return False


def find_hpgl2_entry_point(s: bytes, start: int) -> int:
    while True:
        try:
            index = s.index(b"%", start)
        except ValueError:
            return len(s)
        length = get_enter_hpgl2_mode_command_length(s, index)
        if length:
            return index + length
        start += 2


def hpgl2_commands(s: bytes) -> list[Command]:
    """Low level plot file parser, extracts the HPGL/2 from the byte stream `b`.

    .. Important::

        This parser expects the "Enter HPGL/2 mode" escape sequence to recognize
        HPGL/2 commands. The sequence looks like this: ``[ESC]%1B``, multiple variants
        of this sequence are supported.

    """
    text_terminator = DEFAULT_TEXT_TERMINATOR

    def find_terminator(i: int) -> int:
        while i < length:
            c = s[i]
            if c == QUOTE_CHAR:
                i = find_mark(i + 1, QUOTE_CHAR)
            elif (CHAR_A <= c <= CHAR_Z) or c == SEMICOLON or c == ESCAPE:
                break
            i += 1
        return i

    def find_mark(i: int, mark: int) -> int:
        while i < length and s[i] != mark:
            i += 1
        return i + 1

    def append_command(b: bytes) -> None:
        if b[:2] == b"DT":
            nonlocal text_terminator
            if len(b) > 2:
                text_terminator = b[2]
            else:
                text_terminator = DEFAULT_TEXT_TERMINATOR
        else:
            commands.append(make_command(b))

    commands: list[Command] = []
    length = len(s)
    if has_known_start_sequence(s):
        index = 0
    else:
        index = find_hpgl2_entry_point(s, 0)
    while index < length:
        char = s[index]
        start = index

        if char == ESCAPE:
            # HPGL/2 does not use escape sequences, whatever this sequence is,
            # HPGL/2 mode was left. Find next entry point into HPGL/2 mode:
            index = find_hpgl2_entry_point(s, index)
            continue

        if char <= 32:  # skip all white space and control chars between commands
            index += 1
            continue

        index_plus_2 = index + 2
        if index_plus_2 >= length:
            append_command(s[index:])
            break

        command = s[start:index_plus_2]

        if command == b"PE":
            index = find_mark(index_plus_2, SEMICOLON)
            index -= 1  # exclude terminator ";" from command args
        elif command == b"LB":
            index = find_mark(index_plus_2, text_terminator)
            # include special terminator in command args,
            # otherwise the parser is confused
        else:
            index = find_terminator(index_plus_2)

        append_command(s[start:index])
        if index < length and s[index] == SEMICOLON:
            index += 1
    return commands


def make_command(cmd: bytes) -> Command:
    if not cmd:
        return Command("NOOP", tuple())
    name = cmd[:2].decode()
    if name == "PE":
        args = (bytes([c for c in cmd[2:] if c > 32]),)
    else:
        args = tuple(s for s in cmd[2:].split(b","))  # type: ignore
    return Command(name, args)


def fractional_bits(decimal_places: int) -> int:
    return round(decimal_places * 3.33)


def pe_encode(value: float, frac_bits: int = 0, base: int = 64) -> bytes:
    if frac_bits:
        value *= 1 << frac_bits
        x = round(value)
    else:
        x = round(value)
    if x >= 0:
        x *= 2
    else:
        x = abs(x) * 2 + 1

    chars = bytearray()
    while x >= base:
        x, r = divmod(x, base)
        chars.append(63 + r)
    if base == 64:
        chars.append(191 + x)
    else:
        chars.append(95 + x)
    return bytes(chars)


def pe_decode(
    s: bytes, frac_bits: int = 0, base=64, start: int = 0
) -> tuple[list[float], int]:
    def _decode():
        factors.reverse()
        x = 0
        for f in factors:
            x = x * base + f
        factors.clear()
        if x & 1:
            x = -(x - 1)
        x = x >> 1
        return x

    n = 1 << frac_bits
    if base == 64:
        terminator = 191
    else:
        terminator = 95
    values: list[float] = []
    factors = []
    for index in range(start, len(s)):
        value = s[index]
        if value < 63:
            return values, index
        if value >= terminator:
            factors.append(value - terminator)
            x = _decode()
            if frac_bits:
                values.append(x / n)
            else:
                values.append(float(x))
        else:
            factors.append(value - 63)
    return values, len(s)
