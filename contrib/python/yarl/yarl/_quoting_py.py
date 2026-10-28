import re
from string import ascii_letters, ascii_lowercase, digits, hexdigits
from typing import overload

BASCII_LOWERCASE = ascii_lowercase.encode("ascii")
SUB_DELIMS_WITHOUT_QS = "!$'()*,"
QS = "+&=;"
UNRESERVED = ascii_letters + digits + "-._~"
ALLOWED = UNRESERVED + SUB_DELIMS_WITHOUT_QS


_IS_HEX = re.compile(b"[A-Z0-9][A-Z0-9]")
# Every two hex digit escape body, in any case, to the byte it encodes
_PCT_BYTES = {a + b: int(a + b, 16) for a in hexdigits for b in hexdigits}
_ASCII_LIMIT = 0x80
_ASCII_CHARS = tuple(map(chr, range(_ASCII_LIMIT)))
_UTF8_CONT_MIN = 0x80  # continuation bytes are 10xxxxxx
_UTF8_CONT_MAX = 0xBF
_UTF8_CONT_PAYLOAD = 0x3F
_UTF8_CONT_BITS = 6
_UTF8_LEAD2_PAYLOAD = 0x1F
_UTF8_LEAD3_PAYLOAD = 0x0F
_UTF8_LEAD4_PAYLOAD = 0x07
# Strict UTF-8 as accepted by CPython's decoder, see table 3-7 of the Unicode
# standard: lead byte to the sequence length, the range of the second byte and
# the payload bits of the lead byte.
_UTF8_LEADS = {
    **{
        lead: (2, _UTF8_CONT_MIN, _UTF8_CONT_MAX, _UTF8_LEAD2_PAYLOAD)
        for lead in range(0xC2, 0xE0)  # 0xC0 and 0xC1 only start overlong forms
    },
    0xE0: (3, 0xA0, _UTF8_CONT_MAX, _UTF8_LEAD3_PAYLOAD),  # E0 80..9F is overlong
    **{
        lead: (3, _UTF8_CONT_MIN, _UTF8_CONT_MAX, _UTF8_LEAD3_PAYLOAD)
        for lead in (*range(0xE1, 0xED), 0xEE, 0xEF)
    },
    0xED: (3, _UTF8_CONT_MIN, 0x9F, _UTF8_LEAD3_PAYLOAD),  # ED A0..BF is a surrogate
    0xF0: (4, 0x90, _UTF8_CONT_MAX, _UTF8_LEAD4_PAYLOAD),  # F0 80..8F is overlong
    **{
        lead: (4, _UTF8_CONT_MIN, _UTF8_CONT_MAX, _UTF8_LEAD4_PAYLOAD)
        for lead in range(0xF1, 0xF4)
    },
    0xF4: (4, _UTF8_CONT_MIN, 0x8F, _UTF8_LEAD4_PAYLOAD),  # F4 90..BF is past U+10FFFF
}


class _Quoter:
    def __init__(
        self,
        *,
        safe: str = "",
        protected: str = "",
        qs: bool = False,
        requote: bool = True,
    ) -> None:
        if not safe.isascii() or not protected.isascii():
            raise ValueError("Only safe symbols with ORD < 128 are allowed")
        # A safe '%' would be left alone while requoting decodes '%XX', and a
        # safe ' ' would be left alone while qs turns it into '+'
        if requote and ("%" in safe or "%" in protected):
            raise ValueError(
                "safe and protected cannot contain '%' when requote is enabled"
            )
        if qs and (" " in safe or " " in protected):
            raise ValueError("safe and protected cannot contain ' ' when qs is enabled")
        self._safe = safe
        self._protected = protected
        self._qs = qs
        self._requote = requote

    @overload
    def __call__(self, val: str) -> str: ...
    @overload
    def __call__(self, val: None) -> None: ...
    def __call__(self, val: str | None) -> str | None:
        if val is None:
            return None
        if not isinstance(val, str):
            raise TypeError("Argument should be str")
        if not val:
            return ""
        bval = val.encode("utf8", errors="ignore")
        ret = bytearray()
        pct = bytearray()
        safe = self._safe
        safe += ALLOWED
        if not self._qs:
            safe += QS
        safe += self._protected
        bsafe = safe.encode("ascii")
        idx = 0
        while idx < len(bval):
            ch = bval[idx]
            idx += 1

            if pct:
                if ch in BASCII_LOWERCASE:
                    ch = ch - 32  # convert to uppercase
                pct.append(ch)
                if len(pct) == 3:  # pragma: no branch   # peephole optimizer
                    buf = pct[1:]
                    if not _IS_HEX.match(buf):
                        ret.extend(b"%25")
                        pct.clear()
                        idx -= 2
                        continue
                    try:
                        unquoted = chr(int(pct[1:].decode("ascii"), base=16))
                    except ValueError:
                        ret.extend(b"%25")
                        pct.clear()
                        idx -= 2
                        continue

                    if unquoted in self._protected:
                        ret.extend(pct)
                    elif unquoted in safe:
                        ret.append(ord(unquoted))
                    else:
                        ret.extend(pct)
                    pct.clear()

                # special case, if we have only one char after "%"
                elif len(pct) == 2 and idx == len(bval):
                    ret.extend(b"%25")
                    pct.clear()
                    idx -= 1

                continue

            elif ch == ord("%") and self._requote:
                pct.clear()
                pct.append(ch)

                # special case if "%" is last char
                if idx == len(bval):
                    ret.extend(b"%25")

                continue

            if self._qs and ch == ord(" "):
                ret.append(ord("+"))
                continue
            if ch in bsafe:
                ret.append(ch)
                continue

            ret.extend((f"%{ch:02X}").encode("ascii"))

        ret2 = ret.decode("ascii")
        if ret2 == val:
            return val
        return ret2


class _Unquoter:
    def __init__(
        self,
        *,
        ignore: str = "",
        qs: bool = False,
        plus: bool = False,
        replace_invalid: bool = False,
    ) -> None:
        # Requoting leaves these characters as is, so they would be decoded
        # even when they are in ignore
        decoded_anyway = ALLOWED if qs else ALLOWED + QS
        for ch in ignore:
            if not ch.isascii():
                raise ValueError(f"ignore cannot contain {ch!r}, it is not ASCII")
            if ch in decoded_anyway:
                raise ValueError(f"ignore cannot contain {ch!r}, it is decoded anyway")
        # Write U+FFFD for escapes that are not valid UTF-8, like urllib does,
        # instead of keeping them as is
        self._invalid = "\ufffd" if replace_invalid else ""
        # '+' means a space in query strings and in urllib.parse.unquote_plus
        self._plus_is_space = qs or plus
        # What to write for each decoded ASCII character, its escape when it
        # is ignored or a query string delimiter
        self._ascii_output = list(_ASCII_CHARS)
        for ch in ignore + (QS if qs else ""):
            self._ascii_output[ord(ch)] = f"%{ord(ch):02X}"

    @overload
    def __call__(self, val: str) -> str: ...
    @overload
    def __call__(self, val: None) -> None: ...
    def __call__(self, val: str | None) -> str | None:
        if val is None:
            return None
        if not isinstance(val, str):
            raise TypeError("Argument should be str")
        if not val:
            return ""
        if self._plus_is_space and "+" in val:
            val = val.replace("+", " ")
        if (pos := val.find("%")) == -1:
            return val
        ascii_output = self._ascii_output
        invalid = self._invalid
        ret = []
        # An incomplete UTF-8 sequence: the number of bytes seen, where its
        # escapes start in val and the code point decoded so far
        pending = pending_start = code_point = need = low = high = 0
        # idx is the end of the part of val already handled; plain runs
        # between '%' characters are appended as a single slice.
        idx = 0
        while pos != -1:
            byte = _PCT_BYTES.get(val[pos + 1 : pos + 3])
            if pending and (pos > idx or byte is None or not low <= byte <= high):
                # Not a valid sequence, write the pending escapes as invalid
                ret.append(invalid or val[pending_start:idx])
                pending = 0
            if pos > idx:
                ret.append(val[idx:pos])
            if byte is None:
                # A '%' that does not start an escape is kept as is, as part
                # of the next plain run
                idx = pos
                pos = val.find("%", pos + 1)
                continue
            idx = pos + 3
            if pending:
                code_point = code_point << _UTF8_CONT_BITS | byte & _UTF8_CONT_PAYLOAD
                pending += 1
                low, high = _UTF8_CONT_MIN, _UTF8_CONT_MAX
                if pending == need:
                    ret.append(chr(code_point))
                    pending = 0
            elif byte < _ASCII_LIMIT:
                ret.append(ascii_output[byte])
            elif (lead := _UTF8_LEADS.get(byte)) is not None:
                need, low, high, payload = lead
                code_point = byte & payload
                pending = 1
                pending_start = pos
            else:
                ret.append(invalid or val[pos:idx])
            pos = val.find("%", idx)

        if pending:
            ret.append(invalid or val[pending_start:idx])
        ret.append(val[idx:])

        ret2 = "".join(ret)
        if ret2 == val:
            return val
        return ret2
