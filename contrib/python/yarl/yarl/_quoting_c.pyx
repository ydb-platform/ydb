from cpython.exc cimport PyErr_NoMemory
from cpython.mem cimport PyMem_Free, PyMem_Malloc, PyMem_Realloc
from cpython.pyport cimport PY_SSIZE_T_MAX
from cpython.unicode cimport (
    Py_UCS1,
    Py_UCS2,
    PyUnicode_DATA,
    PyUnicode_DecodeASCII,
    PyUnicode_1BYTE_KIND,
    PyUnicode_2BYTE_KIND,
    PyUnicode_4BYTE_KIND,
    PyUnicode_FindChar,
    PyUnicode_FromKindAndData,
    PyUnicode_GET_LENGTH,
    PyUnicode_KIND,
    PyUnicode_READ,
)
from libc.stdint cimport uint8_t, uint64_t
from libc.string cimport memcpy, memset

from string import ascii_letters, digits


cdef str SUB_DELIMS_WITHOUT_QS = "!$'()*,"
cdef str UNRESERVED = ascii_letters + digits + '-._~'
cdef str ALLOWED = UNRESERVED + SUB_DELIMS_WITHOUT_QS
cdef str QS = '+&=;'

cdef enum:
    BUF_SIZE = 8 * 1024  # 8KiB

    ASCII_LIMIT = 0x80  # code points below this are ASCII
    # Bitmaps with one bit per ASCII character
    BYTE_BITS_SHIFT = 3  # log2 of the 8 bits in a byte
    BYTE_BIT_MASK = (1 << BYTE_BITS_SHIFT) - 1
    ASCII_TABLE_SIZE = ASCII_LIMIT >> BYTE_BITS_SHIFT
    HEX_DIGIT_BITS = 4
    HEX_DIGIT_MASK = (1 << HEX_DIGIT_BITS) - 1
    HEX_LETTER_VALUE = 10  # value of the hex digit A

    # UTF-8, see table 3-7 of the Unicode standard
    UTF8_2BYTE_LIMIT = 0x800  # code points below these limits use 2, 3 bytes
    UTF8_3BYTE_LIMIT = 0x10000
    MAX_CODE_POINT = 0x10FFFF
    SURROGATE_MIN = 0xD800
    SURROGATE_MAX = 0xDFFF
    UTF8_CONT_MARKER = 0x80  # 10xxxxxx
    UTF8_LEAD2_MARKER = 0xC0  # 110xxxxx
    UTF8_LEAD3_MARKER = 0xE0  # 1110xxxx
    UTF8_LEAD4_MARKER = 0xF0  # 11110xxx
    UTF8_CONT_BITS = 6
    UTF8_CONT_PAYLOAD = (1 << UTF8_CONT_BITS) - 1

    # Unquoter output buffer on the stack, longer inputs use the heap
    UCS4_BUF_SIZE = 256
    PCT_HEX_LEN = 2  # the XX in %XX
    PCT_ESCAPE_LEN = 3  # %XX

    # UTF-8 decoding, as strict as CPython's decoder
    UTF8_MAX_BYTES = 4
    UTF8_CONT_MIN = UTF8_CONT_MARKER  # continuation bytes are 10xxxxxx
    UTF8_CONT_MAX = UTF8_CONT_MARKER | UTF8_CONT_PAYLOAD
    UTF8_LEAD2_MIN = 0xC2  # 0xC0 and 0xC1 only start overlong encodings
    UTF8_LEAD2_MAX = 0xDF
    UTF8_LEAD2_PAYLOAD = 0x1F
    UTF8_LEAD3_MIN = UTF8_LEAD3_MARKER
    UTF8_LEAD3_MAX = 0xEF
    UTF8_LEAD3_PAYLOAD = 0x0F
    UTF8_LEAD4_MIN = UTF8_LEAD4_MARKER
    UTF8_LEAD4_MAX = 0xF4  # higher lead bytes encode past U+10FFFF
    UTF8_LEAD4_PAYLOAD = 0x07
    # Lead bytes that narrow the range of the byte right after them
    UTF8_LEAD_E0 = UTF8_LEAD3_MIN
    UTF8_E0_CONT_MIN = 0xA0  # E0 80..9F would be overlong
    UTF8_LEAD_ED = 0xED
    UTF8_ED_CONT_MAX = 0x9F  # ED A0..BF would be a surrogate
    UTF8_LEAD_F0 = UTF8_LEAD4_MIN
    UTF8_F0_CONT_MIN = 0x90  # F0 80..8F would be overlong
    UTF8_LEAD_F4 = UTF8_LEAD4_MAX
    UTF8_F4_CONT_MAX = 0x8F  # F4 90..BF would be past U+10FFFF
    REPLACEMENT_CHARACTER = 0xFFFD


cdef inline Py_UCS4 _to_hex(uint8_t v) noexcept:
    if v < HEX_LETTER_VALUE:
        return <Py_UCS4>(v + ord('0'))
    return <Py_UCS4>(v - HEX_LETTER_VALUE + ord('A'))


cdef inline int _from_hex(Py_UCS4 v) noexcept:
    if '0' <= v <= '9':
        return <int>v - ord('0')
    if 'A' <= v <= 'F':
        return <int>v - ord('A') + HEX_LETTER_VALUE
    if 'a' <= v <= 'f':
        return <int>v - ord('a') + HEX_LETTER_VALUE
    return -1


cdef inline int _is_lower_hex(Py_UCS4 v) noexcept:
    return 'a' <= v <= 'f'


cdef inline bint _is_surrogate(Py_UCS4 ch) noexcept:
    return SURROGATE_MIN <= ch <= SURROGATE_MAX


cdef inline Py_ssize_t _skip_surrogates(
    int kind, const void *data, Py_ssize_t idx, Py_ssize_t length
) noexcept:
    # Advance past lone surrogates; they cannot be UTF-8 encoded and are
    # dropped, so they must not break up a percent escape during requoting.
    while idx < length and _is_surrogate(PyUnicode_READ(kind, data, idx)):
        idx += 1
    return idx


cdef inline long _restore_ch(Py_UCS4 d1, Py_UCS4 d2) noexcept:
    cdef int digit1 = _from_hex(d1)
    if digit1 < 0:
        return -1
    cdef int digit2 = _from_hex(d2)
    if digit2 < 0:
        return -1
    return digit1 << HEX_DIGIT_BITS | digit2


cdef uint8_t ALLOWED_TABLE[ASCII_TABLE_SIZE]
cdef uint8_t ALLOWED_NOTQS_TABLE[ASCII_TABLE_SIZE]


cdef inline bint bit_at(uint8_t array[], uint64_t ch) noexcept:
    return array[ch >> BYTE_BITS_SHIFT] & (1 << (ch & BYTE_BIT_MASK))


cdef inline void set_bit(uint8_t array[], uint64_t ch) noexcept:
    array[ch >> BYTE_BITS_SHIFT] |= (1 << (ch & BYTE_BIT_MASK))


memset(ALLOWED_TABLE, 0, sizeof(ALLOWED_TABLE))
memset(ALLOWED_NOTQS_TABLE, 0, sizeof(ALLOWED_NOTQS_TABLE))

for i in range(ASCII_LIMIT):
    if chr(i) in ALLOWED:
        set_bit(ALLOWED_TABLE, i)
        set_bit(ALLOWED_NOTQS_TABLE, i)
    if chr(i) in QS:
        set_bit(ALLOWED_NOTQS_TABLE, i)

# ----------------- writer ---------------------------

cdef struct Writer:
    char *buf
    bint heap_allocated_buf
    Py_ssize_t size
    Py_ssize_t pos
    bint changed


cdef inline void _init_writer(Writer* writer, char* buf):
    writer.buf = buf
    writer.heap_allocated_buf = False
    writer.size = BUF_SIZE
    writer.pos = 0
    writer.changed = 0


cdef inline void _release_writer(Writer* writer):
    if writer.heap_allocated_buf:
        PyMem_Free(writer.buf)


cdef inline int _write_char(Writer* writer, Py_UCS4 ch, bint changed):
    cdef char * buf
    cdef Py_ssize_t size

    if writer.pos == writer.size:
        # reallocate
        size = writer.size + BUF_SIZE
        if not writer.heap_allocated_buf:
            buf = <char*>PyMem_Malloc(size)
            if buf == NULL:
                PyErr_NoMemory()
                return -1
            memcpy(buf, writer.buf, writer.size)
            writer.heap_allocated_buf = True
        else:
            buf = <char*>PyMem_Realloc(writer.buf, size)
            if buf == NULL:
                PyErr_NoMemory()
                return -1
        writer.buf = buf
        writer.size = size
    writer.buf[writer.pos] = <char>ch
    writer.pos += 1
    writer.changed |= changed
    return 0


cdef inline int _write_pct(Writer* writer, uint8_t ch, bint changed):
    if _write_char(writer, '%', changed) < 0:
        return -1
    if _write_char(writer, _to_hex(<uint8_t>ch >> HEX_DIGIT_BITS), changed) < 0:
        return -1
    return _write_char(writer, _to_hex(<uint8_t>ch & HEX_DIGIT_MASK), changed)


cdef inline int _write_utf8(Writer* writer, Py_UCS4 symbol):
    cdef uint64_t utf = <uint64_t> symbol

    if utf < ASCII_LIMIT:
        return _write_pct(writer, <uint8_t>utf, True)
    if utf < UTF8_2BYTE_LIMIT:
        if _write_pct(
            writer, <uint8_t>(UTF8_LEAD2_MARKER | (utf >> UTF8_CONT_BITS)), True
        ) < 0:
            return -1
        return _write_pct(
            writer, <uint8_t>(UTF8_CONT_MARKER | (utf & UTF8_CONT_PAYLOAD)), True
        )
    if _is_surrogate(symbol):
        # lone surrogate; invalid in UTF-8 so it is dropped, matching the
        # pure-Python quoter's errors="ignore" encode. Mark the writer as
        # changed so _do_quote returns the surrogate-free buffer rather than
        # the untouched input string.
        writer.changed = True
        return 0
    if utf < UTF8_3BYTE_LIMIT:
        if _write_pct(
            writer, <uint8_t>(UTF8_LEAD3_MARKER | (utf >> 2 * UTF8_CONT_BITS)), True
        ) < 0:
            return -1
        if _write_pct(
            writer,
            <uint8_t>(UTF8_CONT_MARKER | ((utf >> UTF8_CONT_BITS) & UTF8_CONT_PAYLOAD)),
            True,
        ) < 0:
            return -1
        return _write_pct(
            writer, <uint8_t>(UTF8_CONT_MARKER | (utf & UTF8_CONT_PAYLOAD)), True
        )
    if utf > MAX_CODE_POINT:
        # symbol is too large
        return 0
    if _write_pct(
        writer, <uint8_t>(UTF8_LEAD4_MARKER | (utf >> 3 * UTF8_CONT_BITS)), True
    ) < 0:
        return -1
    if _write_pct(
        writer,
        <uint8_t>(
            UTF8_CONT_MARKER | ((utf >> 2 * UTF8_CONT_BITS) & UTF8_CONT_PAYLOAD)
        ),
        True,
    ) < 0:
        return -1
    if _write_pct(
        writer,
        <uint8_t>(UTF8_CONT_MARKER | ((utf >> UTF8_CONT_BITS) & UTF8_CONT_PAYLOAD)),
        True,
    ) < 0:
        return -1
    return _write_pct(
        writer, <uint8_t>(UTF8_CONT_MARKER | (utf & UTF8_CONT_PAYLOAD)), True
    )


# --------------------- end writer --------------------------


cdef class _Quoter:
    cdef bint _qs
    cdef bint _requote

    cdef uint8_t _safe_table[ASCII_TABLE_SIZE]
    cdef uint8_t _protected_table[ASCII_TABLE_SIZE]

    def __init__(
            self, *, str safe='', str protected='', bint qs=False, bint requote=True,
    ):
        cdef Py_UCS4 ch

        self._qs = qs
        self._requote = requote

        if not self._qs:
            memcpy(self._safe_table,
                   ALLOWED_NOTQS_TABLE,
                   sizeof(self._safe_table))
        else:
            memcpy(self._safe_table,
                   ALLOWED_TABLE,
                   sizeof(self._safe_table))
        for ch in safe:
            if ord(ch) >= ASCII_LIMIT:
                raise ValueError("Only safe symbols with ORD < 128 are allowed")
            set_bit(self._safe_table, ch)

        memset(self._protected_table, 0, sizeof(self._protected_table))
        for ch in protected:
            if ord(ch) >= ASCII_LIMIT:
                raise ValueError("Only safe symbols with ORD < 128 are allowed")
            set_bit(self._safe_table, ch)
            set_bit(self._protected_table, ch)

        # Protected characters are in the safe table too. A safe '%' would be
        # left alone while requoting decodes '%XX', and a safe ' ' would be
        # left alone while qs turns it into '+'
        if self._requote and bit_at(self._safe_table, c'%'):
            raise ValueError(
                "safe and protected cannot contain '%' when requote is enabled"
            )
        if self._qs and bit_at(self._safe_table, c' '):
            raise ValueError(
                "safe and protected cannot contain ' ' when qs is enabled"
            )

    def __call__(self, val):
        if val is None:
            return None
        if type(val) is not str:
            if not isinstance(val, str):
                raise TypeError("Argument should be str")
            # derived from str
            val = str(val)
        return self._do_quote_or_skip(<str>val)

    cdef str _do_quote_or_skip(self, str val):
        cdef char[BUF_SIZE] buffer
        cdef Py_UCS4 ch
        cdef Py_ssize_t length = PyUnicode_GET_LENGTH(val)
        cdef Py_ssize_t idx = length
        cdef bint must_quote = 0
        cdef Writer writer
        cdef int kind = PyUnicode_KIND(val)
        cdef const void *data = PyUnicode_DATA(val)

        # If everything in the string is in the safe
        # table and all ASCII, we can skip quoting
        while idx:
            idx -= 1
            ch = PyUnicode_READ(kind, data, idx)
            if ch >= ASCII_LIMIT or not bit_at(self._safe_table, ch):
                must_quote = 1
                break

        if not must_quote:
            return val

        _init_writer(&writer, &buffer[0])
        try:
            return self._do_quote(<str>val, length, kind, data, &writer)
        finally:
            _release_writer(&writer)

    cdef str _do_quote(
        self,
        str val,
        Py_ssize_t length,
        int kind,
        const void *data,
        Writer *writer
    ):
        cdef Py_UCS4 ch
        cdef Py_UCS4 d1
        cdef Py_UCS4 d2
        cdef long chl
        cdef int changed
        cdef bint surrogate_skipped
        cdef Py_ssize_t idx = 0
        cdef Py_ssize_t pos1
        cdef Py_ssize_t pos2

        while idx < length:
            ch = PyUnicode_READ(kind, data, idx)
            idx += 1
            if ch == '%' and self._requote and idx < length:
                # Lone surrogates are dropped (see _skip_surrogates), so look
                # through them for the two hex digits of the "%XX" escape; this
                # keeps the C quoter consistent with the pure-Python backend,
                # which strips surrogates before scanning.
                pos1 = _skip_surrogates(kind, data, idx, length)
                pos2 = _skip_surrogates(kind, data, pos1 + 1, length)
                if pos2 < length:
                    d1 = PyUnicode_READ(kind, data, pos1)
                    d2 = PyUnicode_READ(kind, data, pos2)
                    chl = _restore_ch(d1, d2)
                else:
                    chl = -1
                if chl != -1:
                    ch = <Py_UCS4>chl
                    surrogate_skipped = pos1 != idx or pos2 != pos1 + 1
                    idx = pos2 + 1
                    if ch < ASCII_LIMIT:
                        if bit_at(self._protected_table, ch):
                            if _write_pct(writer, ch, True) < 0:
                                raise
                            continue

                        if bit_at(self._safe_table, ch):
                            if _write_char(writer, ch, True) < 0:
                                raise
                            continue

                    changed = (surrogate_skipped or
                               _is_lower_hex(d1) or _is_lower_hex(d2))
                    if _write_pct(writer, ch, changed) < 0:
                        raise
                    continue
                else:
                    ch = '%'

            if self._write(writer, ch) < 0:
                raise

        if not writer.changed:
            return val
        return PyUnicode_DecodeASCII(writer.buf, writer.pos, "strict")

    cdef inline int _write(self, Writer *writer, Py_UCS4 ch):
        if self._qs:
            if ch == ' ':
                return _write_char(writer, '+', True)

        if ch < ASCII_LIMIT and bit_at(self._safe_table, ch):
            return _write_char(writer, ch, False)

        return _write_utf8(writer, ch)


cdef inline Py_ssize_t _utf8_sequence_length(uint8_t lead) noexcept:
    if UTF8_LEAD2_MIN <= lead <= UTF8_LEAD2_MAX:
        return 2
    if UTF8_LEAD3_MIN <= lead <= UTF8_LEAD3_MAX:
        return 3
    if UTF8_LEAD4_MIN <= lead <= UTF8_LEAD4_MAX:
        return 4
    return 0


cdef inline bint _utf8_is_continuation(
    uint8_t lead, Py_ssize_t pos, Py_UCS4 byte
) noexcept:
    if pos == 1:
        if lead == UTF8_LEAD_E0:
            return UTF8_E0_CONT_MIN <= byte <= UTF8_CONT_MAX
        if lead == UTF8_LEAD_ED:
            return UTF8_CONT_MIN <= byte <= UTF8_ED_CONT_MAX
        if lead == UTF8_LEAD_F0:
            return UTF8_F0_CONT_MIN <= byte <= UTF8_CONT_MAX
        if lead == UTF8_LEAD_F4:
            return UTF8_CONT_MIN <= byte <= UTF8_F4_CONT_MAX
    return UTF8_CONT_MIN <= byte <= UTF8_CONT_MAX


cdef inline Py_UCS4 _utf8_decode(const uint8_t *buf, Py_ssize_t length) noexcept:
    if length == 2:
        return (
            (buf[0] & UTF8_LEAD2_PAYLOAD) << UTF8_CONT_BITS
            | (buf[1] & UTF8_CONT_PAYLOAD)
        )
    if length == 3:
        return (
            (buf[0] & UTF8_LEAD3_PAYLOAD) << 2 * UTF8_CONT_BITS
            | (buf[1] & UTF8_CONT_PAYLOAD) << UTF8_CONT_BITS
            | (buf[2] & UTF8_CONT_PAYLOAD)
        )
    return (
        (buf[0] & UTF8_LEAD4_PAYLOAD) << 3 * UTF8_CONT_BITS
        | (buf[1] & UTF8_CONT_PAYLOAD) << 2 * UTF8_CONT_BITS
        | (buf[2] & UTF8_CONT_PAYLOAD) << UTF8_CONT_BITS
        | (buf[3] & UTF8_CONT_PAYLOAD)
    )


# Output buffer for _Unquoter, holding code points instead of bytes. Unquoting
# never makes a string longer (escapes are decoded, written back as escapes of
# the same length or copied as is), so the buffer is sized to the input once and
# writes need no capacity checks.
cdef struct UCS4Writer:
    Py_UCS4 *buf
    bint heap_allocated_buf
    Py_ssize_t pos


cdef inline int _init_ucs4_writer(
    UCS4Writer* writer, Py_UCS4* stack_buf, Py_ssize_t length
) except -1:
    writer.pos = 0
    writer.heap_allocated_buf = False
    if length <= UCS4_BUF_SIZE:
        writer.buf = stack_buf
        return 0
    if <size_t>length > <size_t>PY_SSIZE_T_MAX // sizeof(Py_UCS4):
        writer.buf = NULL
        PyErr_NoMemory()
        return -1
    writer.buf = <Py_UCS4*>PyMem_Malloc(length * sizeof(Py_UCS4))
    if writer.buf == NULL:
        PyErr_NoMemory()
        return -1
    writer.heap_allocated_buf = True
    return 0


cdef inline void _release_ucs4_writer(UCS4Writer* writer) noexcept:
    if writer.heap_allocated_buf:
        PyMem_Free(writer.buf)


cdef inline void _ucs4_write_char(UCS4Writer* writer, Py_UCS4 ch) noexcept:
    writer.buf[writer.pos] = ch
    writer.pos += 1


ctypedef fused _narrow_ucs:
    Py_UCS1
    Py_UCS2


cdef inline void _widen_to_ucs4(
    Py_UCS4 *out, const _narrow_ucs *src, Py_ssize_t length
) noexcept:
    # No public API copies part of a str into a UCS4 buffer; CPython widens
    # with the same plain loop internally.
    cdef Py_ssize_t i
    for i in range(length):
        out[i] = src[i]


cdef inline void _ucs4_write_slice(
    UCS4Writer* writer,
    int kind,
    const void *data,
    Py_ssize_t start,
    Py_ssize_t end,
) noexcept:
    cdef Py_ssize_t length = end - start
    cdef Py_UCS4 *out
    if length <= 0:
        return
    out = writer.buf + writer.pos
    if kind == PyUnicode_1BYTE_KIND:
        _widen_to_ucs4(out, <const Py_UCS1*>data + start, length)
    elif kind == PyUnicode_2BYTE_KIND:
        _widen_to_ucs4(out, <const Py_UCS2*>data + start, length)
    else:
        memcpy(out, <const Py_UCS4*>data + start, length * sizeof(Py_UCS4))
    writer.pos += length


cdef inline void _ucs4_write_pct(UCS4Writer* writer, uint8_t byte) noexcept:
    _ucs4_write_char(writer, '%')
    _ucs4_write_char(writer, _to_hex(byte >> HEX_DIGIT_BITS))
    _ucs4_write_char(writer, _to_hex(byte & HEX_DIGIT_MASK))


cdef inline Py_ssize_t _find_percent(
    str val, Py_ssize_t start, Py_ssize_t end
) except -1:
    """Return the index of the next '%' in val[start:end], or end."""
    cdef Py_ssize_t found = PyUnicode_FindChar(val, '%', start, end, 1)
    return end if found == -1 else found


cdef class _Unquoter:
    # '+' means a space in query strings and in urllib.parse.unquote_plus
    cdef bint _plus_is_space
    # Write U+FFFD for escapes that are not valid UTF-8, like urllib does,
    # instead of keeping them as is
    cdef bint _replace_invalid
    # Decoded ASCII characters that are written back as their escape
    cdef uint8_t _keep_escaped[ASCII_TABLE_SIZE]

    def __init__(
        self,
        *,
        str ignore="",
        bint qs=False,
        bint plus=False,
        bint replace_invalid=False,
    ):
        cdef Py_UCS4 ch
        # These characters are never escaped by quoting, so they are always
        # decoded and cannot be ignored
        cdef uint8_t *decoded_anyway = ALLOWED_NOTQS_TABLE
        memset(self._keep_escaped, 0, sizeof(self._keep_escaped))
        if qs:
            decoded_anyway = ALLOWED_TABLE
            for ch in QS:
                set_bit(self._keep_escaped, ch)
        for ch in ignore:
            if ch >= ASCII_LIMIT:
                raise ValueError(f"ignore cannot contain {ch!r}, it is not ASCII")
            if bit_at(decoded_anyway, ch):
                raise ValueError(f"ignore cannot contain {ch!r}, it is decoded anyway")
            set_bit(self._keep_escaped, ch)
        self._replace_invalid = replace_invalid
        self._plus_is_space = qs or plus

    def __call__(self, val):
        if val is None:
            return None
        if type(val) is not str:
            if not isinstance(val, str):
                raise TypeError("Argument should be str")
            # derived from str
            val = str(val)
        return self._do_unquote(<str>val)

    cdef str _do_unquote(self, str val):
        cdef Py_ssize_t length = PyUnicode_GET_LENGTH(val)
        if length == 0:
            return val

        # A literal '+' never takes part in an escape sequence, so turning
        # every '+' into a space up front gives the same result as doing it
        # in the loop below.
        if self._plus_is_space and PyUnicode_FindChar(val, '+', 0, length, 1) != -1:
            val = val.replace('+', ' ')
        # Skip straight to the first '%'; most strings have none and are
        # returned as is.
        cdef Py_ssize_t idx = _find_percent(val, 0, length)
        if idx == length:
            return val

        cdef Py_UCS4 stack_buf[UCS4_BUF_SIZE]
        cdef UCS4Writer writer
        _init_ucs4_writer(&writer, stack_buf, length)
        try:
            return self._unquote_from(&writer, val, length, idx)
        finally:
            _release_ucs4_writer(&writer)

    cdef str _unquote_from(
        self, UCS4Writer* writer, str val, Py_ssize_t length, Py_ssize_t idx
    ):
        cdef uint8_t buffer[UTF8_MAX_BYTES]
        cdef Py_ssize_t buflen = 0
        cdef Py_ssize_t need = 0
        cdef Py_UCS4 ch
        cdef long chl
        cdef Py_ssize_t run_end
        cdef bint changed = 0
        cdef int kind = PyUnicode_KIND(val)
        cdef const void *data = PyUnicode_DATA(val)
        _ucs4_write_slice(writer, kind, data, 0, idx)
        while idx < length:
            ch = PyUnicode_READ(kind, data, idx)
            idx += 1
            if ch == '%' and idx <= length - PCT_HEX_LEN:
                chl = _restore_ch(
                    PyUnicode_READ(kind, data, idx),
                    PyUnicode_READ(kind, data, idx + 1)
                )
                if chl != -1:
                    changed = 1
                    ch = <Py_UCS4>chl
                    idx += PCT_HEX_LEN
                    if buflen:
                        if _utf8_is_continuation(buffer[0], buflen, ch):
                            buffer[buflen] = <uint8_t>ch
                            buflen += 1
                            if buflen == need:
                                _ucs4_write_char(writer, _utf8_decode(buffer, buflen))
                                buflen = 0
                            continue
                        # Not a valid sequence, write the pending escapes as invalid
                        # and start over from this byte.
                        self._write_invalid(
                            writer,
                            kind,
                            data,
                            idx - PCT_ESCAPE_LEN - buflen * PCT_ESCAPE_LEN,
                            idx - PCT_ESCAPE_LEN,
                        )
                        buflen = 0
                    if ch < ASCII_LIMIT:
                        if bit_at(self._keep_escaped, ch):
                            _ucs4_write_pct(writer, <uint8_t>ch)
                        else:
                            _ucs4_write_char(writer, ch)
                        continue
                    need = _utf8_sequence_length(<uint8_t>ch)
                    if need:
                        buffer[0] = <uint8_t>ch
                        buflen = 1
                    else:
                        self._write_invalid(
                            writer, kind, data, idx - PCT_ESCAPE_LEN, idx
                        )
                    continue

            if buflen:
                self._write_invalid(
                    writer, kind, data, idx - 1 - buflen * PCT_ESCAPE_LEN, idx - 1
                )
                buflen = 0

            # Copy everything up to the next '%' in one go.
            run_end = _find_percent(val, idx, length)
            _ucs4_write_slice(writer, kind, data, idx - 1, run_end)
            idx = run_end

        if not changed:
            return val

        if buflen:
            self._write_invalid(
                writer, kind, data, length - buflen * PCT_ESCAPE_LEN, length
            )

        return PyUnicode_FromKindAndData(
            PyUnicode_4BYTE_KIND, writer.buf, writer.pos
        )

    cdef inline void _write_invalid(
        self,
        UCS4Writer* writer,
        int kind,
        const void *data,
        Py_ssize_t start,
        Py_ssize_t end,
    ) noexcept:
        """Write the escapes in val[start:end] that are not valid UTF-8."""
        if self._replace_invalid:
            _ucs4_write_char(writer, REPLACEMENT_CHARACTER)
        else:
            _ucs4_write_slice(writer, kind, data, start, end)
