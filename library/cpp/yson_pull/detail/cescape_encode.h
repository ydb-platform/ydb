#pragma once

#include <util/system/types.h>

// Whether to ensure strict ASCII compatibility
// Turns UTF-8 strings into unreadable garbage for no known reason
//#define CESCAPE_STRICT_ASCII

namespace NYsonPull {
    namespace NDetail {
        namespace NCEscape {
            namespace NImpl {
                inline ui8 hex_digit(ui8 value) {
                    constexpr ui8 hex_digits[] = "0123456789ABCDEF";
                    return hex_digits[value];
                }

                inline ui8 oct_digit(ui8 value) {
                    return '0' + value;
                }

                inline bool is_printable(ui8 c) {
#ifdef CESCAPE_STRICT_ASCII
                    return c >= 32 && c <= 126;
#else
                    return c >= 32;
#endif
                }

                inline bool is_hex_digit(ui8 c) {
                    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
                }

                inline bool is_oct_digit(ui8 c) {
                    return c >= '0' && c <= '7';
                }

                constexpr size_t ESCAPE_C_BUFFER_SIZE = 4;

                inline size_t escape_char(
                    ui8 c,
                    ui8 next,
                    ui8 r[ESCAPE_C_BUFFER_SIZE]) {
                    // (1) Printable characters go as-is, except backslash and double quote.
                    // (2) Characters \r, \n, \t and \0 ... \7 replaced by their simple escape characters (if possible).
                    // (3) Otherwise, character is encoded using hexadecimal escape sequence (if possible), or octal.
                    if (c == '\"') {
                        r[0] = '\\';
                        r[1] = '\"';
                        return 2;
                    } else if (c == '\\') {
                        r[0] = '\\';
                        r[1] = '\\';
                        return 2;
                    } else if (is_printable(c)) {
                        r[0] = c;
                        return 1;
                    } else if (c == '\r') {
                        r[0] = '\\';
                        r[1] = 'r';
                        return 2;
                    } else if (c == '\n') {
                        r[0] = '\\';
                        r[1] = 'n';
                        return 2;
                    } else if (c == '\t') {
                        r[0] = '\\';
                        r[1] = 't';
                        return 2;
                    } else if (c < 8 && !is_oct_digit(next)) {
                        r[0] = '\\';
                        r[1] = oct_digit(c);
                        return 2;
                    } else if (!is_hex_digit(next)) {
                        r[0] = '\\';
                        r[1] = 'x';
                        r[2] = hex_digit((c & 0xF0) >> 4);
                        r[3] = hex_digit((c & 0x0F) >> 0);
                        return 4;
                    } else {
                        r[0] = '\\';
                        r[1] = oct_digit((c & 0700) >> 6);
                        r[2] = oct_digit((c & 0070) >> 3);
                        r[3] = oct_digit((c & 0007) >> 0);
                        return 4;
                    }
                }

                template <typename T>
                inline void escape_impl(const ui8* str, size_t len, T&& consume) {
                    ui8 buffer[ESCAPE_C_BUFFER_SIZE];

                    size_t i, j;
                    for (i = 0, j = 0; i < len; ++i) {
                        auto next_char = i + 1 < len ? str[i + 1] : 0;
                        size_t rlen = escape_char(str[i], next_char, buffer);

                        if (rlen > 1) {
                            consume(str + j, i - j);
                            j = i + 1;
                            consume(buffer, rlen);
                        }
                    }

                    if (j > 0) {
                        consume(str + j, len - j);
                    } else {
                        consume(str, len);
                    }
                }
            }
        }     // namespace NCEscape
    }         // namespace NDetail
}
