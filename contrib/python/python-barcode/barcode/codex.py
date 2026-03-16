"""Module: barcode.codex

:Provided barcodes: Code 39, Code 128, PZN
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Collection
from typing import Literal

from barcode.base import Barcode
from barcode.charsets import code39
from barcode.charsets import code128
from barcode.errors import BarcodeError
from barcode.errors import IllegalCharacterError
from barcode.errors import NumberOfDigitsError

if TYPE_CHECKING:
    from barcode.writer import BaseWriter

__docformat__ = "restructuredtext en"

# Sizes
MIN_SIZE = 0.2
MIN_QUIET_ZONE = 2.54


def check_code(code: str, name: str, allowed: Collection[str]) -> None:
    wrong = []
    for char in code:
        if char not in allowed:
            wrong.append(char)
    if wrong:
        raise IllegalCharacterError(
            "The following characters are not valid for {name}: {wrong}".format(
                name=name, wrong=", ".join(wrong)
            )
        )


class Code39(Barcode):
    """A Code39 barcode implementation"""

    name = "Code 39"

    def __init__(self, code: str, writer=None, add_checksum: bool = True) -> None:
        r"""
        :param code: Code 39 string without \* and without checksum.
        :param writer: A ``barcode.writer`` instance used to render the barcode
            (default: SVGWriter).
        :param add_checksum: Add the checksum to code or not
        """

        self.code = code.upper()
        if add_checksum:
            self.code += self.calculate_checksum()
        self.writer = writer or self.default_writer()
        check_code(self.code, self.name, code39.REF)

    def __str__(self) -> str:
        return self.code

    def get_fullcode(self) -> str:
        """:returns: The full code as it will be encoded."""
        return self.code

    def calculate_checksum(self):
        check = sum(code39.MAP[x][0] for x in self.code) % 43
        for k, v in code39.MAP.items():
            if check == v[0]:
                return k
        return None

    def build(self) -> list[str]:
        chars = [code39.EDGE]
        for char in self.code:
            chars.append(code39.MAP[char][1])
        chars.append(code39.EDGE)
        result = code39.MIDDLE.join(chars)
        return [result]

    def render(self, writer_options=None, text=None):
        options = {"module_width": MIN_SIZE, "quiet_zone": MIN_QUIET_ZONE}
        options.update(writer_options or {})
        return super().render(options, text)


class PZN7(Code39):
    """Initializes new German number for pharmaceutical products.

    :parameters:
        pzn : String
            Code to render.
        writer : barcode.writer Instance
            The writer to render the barcode (default: SVGWriter).
    """

    name = "Pharmazentralnummer"

    digits = 6

    def __init__(self, pzn, writer=None) -> None:
        pzn = pzn[: self.digits]
        if not pzn.isdigit():
            raise IllegalCharacterError("PZN can only contain numbers.")
        if len(pzn) != self.digits:
            raise NumberOfDigitsError(
                f"PZN must have {self.digits} digits, not {len(pzn)}."
            )
        self.pzn = pzn
        self.pzn = f"{pzn}{self.calculate_checksum()}"
        super().__init__(f"PZN-{self.pzn}", writer, add_checksum=False)

    def get_fullcode(self):
        return f"PZN-{self.pzn}"

    def calculate_checksum(self):
        sum_ = sum(int(x) * int(y) for x, y in enumerate(self.pzn, start=2))
        checksum = sum_ % 11
        if checksum == 10:
            raise BarcodeError("Checksum can not be 10 for PZN.")

        return checksum


class PZN8(PZN7):
    """Will be fully added in v0.9."""

    digits = 7


class Code128(Barcode):
    """Initializes a new Code128 instance. The checksum is added automatically
    when building the bars.

    :parameters:
        code : String
            Code 128 string without checksum (added automatically).
        writer : barcode.writer Instance
            The writer to render the barcode (default: SVGWriter).
    """

    name = "Code 128"
    _charset: Literal["A", "B", "C"]
    code: str
    writer: BaseWriter
    buffer: str

    def __init__(self, code: str, writer=None) -> None:
        self.code = code
        self.writer = writer or self.default_writer()
        self._charset = "C"
        self._buffer = ""
        check_code(self.code, self.name, code128.ALL)

    def __str__(self) -> str:
        return self.code

    @property
    def encoded(self) -> list[int]:
        return self._build()

    def get_fullcode(self) -> str:
        return self.code

    def _new_charset(self, which: Literal["A", "B", "C"]) -> list[int]:
        if which == self._charset:
            raise ValueError(f"Already in charset {which}")
        if which == "A":
            code = self._convert("TO_A")
        elif which == "B":
            code = self._convert("TO_B")
        elif which == "C":
            code = self._convert("TO_C")
        self._charset = which
        return [code]

    # to be redefined in subclass if required
    def _is_char_fnc1_char(self, char):
        """Whether a character is the FNC1 character.

        May be redefined by subclasses if required. FNC1 char is defined in GS1-128
        specification and it is defined just the same for all encodings therefore this
        sign should be treated in a special way.
        """
        return False

    def _maybe_switch_charset(self, pos: int) -> list[int]:
        char = self.code[pos]
        next_ = self.code[pos : pos + 10]

        def look_next() -> bool:
            digits = 0
            for c in next_:
                if c.isdigit():
                    digits += 1
                else:
                    break
            return digits > 3

        codes: list[int] = []
        if self._charset == "C" and not char.isdigit():
            if self._is_char_fnc1_char(char) and not self._buffer:
                return codes
            if char in code128.B:
                codes = self._new_charset("B")
            elif char in code128.A:
                codes = self._new_charset("A")
            if len(self._buffer) == 1:
                codes.append(self._convert(self._buffer[0]))
                self._buffer = ""
        elif self._charset == "B":
            if look_next():
                codes = self._new_charset("C")
            elif char not in code128.B and char in code128.A:
                codes = self._new_charset("A")
        elif self._charset == "A":
            if look_next():
                codes = self._new_charset("C")
            elif char not in code128.A and char in code128.B:
                codes = self._new_charset("B")
        return codes

    def _convert(self, char: str):
        if self._charset == "A":
            return code128.A[char]
        if self._charset == "B":
            return code128.B[char]
        if self._charset == "C":
            if char in code128.C:
                return code128.C[char]
            if char.isdigit():
                self._buffer += char
                if len(self._buffer) == 2:
                    value = int(self._buffer)
                    self._buffer = ""
                    return value
                return None
        raise RuntimeError(
            f"Character {char} could not be converted in charset {self._charset}."
        )

    def _try_to_optimize(self, encoded: list[int]) -> list[int]:
        if encoded[1] in code128.TO:
            encoded[:2] = [code128.TO[encoded[1]]]
        return encoded

    def _calculate_checksum(self, encoded: list[int]) -> int:
        cs = [encoded[0]]
        for i, code_num in enumerate(encoded[1:], start=1):
            cs.append(i * code_num)
        return sum(cs) % 103

    def _build(self) -> list[int]:
        encoded: list[int] = [code128.START_CODES[self._charset]]
        for i, char in enumerate(self.code):
            encoded.extend(self._maybe_switch_charset(i))
            code_num = self._convert(char)
            if code_num is not None:
                encoded.append(code_num)
        # Finally look in the buffer
        if len(self._buffer) == 1:
            encoded.extend(self._new_charset("B"))
            encoded.append(self._convert(self._buffer[0]))
            self._buffer = ""
        return self._try_to_optimize(encoded)

    def build(self) -> list[str]:
        encoded = self._build()
        encoded.append(self._calculate_checksum(encoded))
        code = ""
        for code_num in encoded:
            code += code128.CODES[code_num]
        code += code128.STOP
        code += "11"
        return [code]

    def render(self, writer_options=None, text=None):
        options = {"module_width": MIN_SIZE, "quiet_zone": MIN_QUIET_ZONE}
        options.update(writer_options or {})
        return super().render(options, text)


class Gs1_128(Code128):  # noqa: N801
    """
    following the norm, a gs1-128 barcode is a subset of code 128 barcode,
    it can be generated by prepending the code with the FNC1 character
    https://en.wikipedia.org/wiki/GS1-128
    https://www.gs1-128.info/
    """

    name = "GS1-128"

    FNC1_CHAR = "\xf1"

    def __init__(self, code, writer=None) -> None:
        code = self.FNC1_CHAR + code
        super().__init__(code, writer)

    def get_fullcode(self):
        return super().get_fullcode()[1:]

    def _is_char_fnc1_char(self, char):
        return char == self.FNC1_CHAR


# For pre 0.8 compatibility
PZN = PZN7
