#pragma once

#include <util/system/types.h>

#include <algorithm>
#include <cstring>

namespace NYsonPull {
    namespace NDetail {
        namespace NCEscape {
            namespace NImpl {
                inline ui8 as_digit(ui8 c) {
                    return c - ui8{'0'};
                }

                inline ui8 as_hexdigit(ui8 c) {
                    static constexpr ui8 hex_decode_map[256] = {
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255, 255,
                        255, 255, 255, 255, 255, 10, 11, 12, 13, 14, 15, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 10, 11, 12, 13, 14, 15, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                        255, 255, 255, 255};

                    return hex_decode_map[c];
                }

                inline const ui8* read_oct(ui8& result, const ui8* p, ui8 n) {
                    auto digit = ui8{0};
                    while (n-- && (digit = as_digit(*p)) < 8) {
                        result = result * 8 + digit;
                        ++p;
                    }
                    return p;
                }

                inline const ui8* read_hex(ui8& result, const ui8* p, ui8 n) {
                    auto digit = ui8{0};
                    while (n-- && (digit = as_hexdigit(*p)) < 16) {
                        result = result * 16 + digit;
                        ++p;
                    }
                    return p;
                }

                inline const ui8* unescape_char_and_advance(
                    ui8& result,
                    const ui8* p,
                    const ui8* end) {
                    switch (*p) {
                        default:
                            result = *p;
                            ++p;
                            break;
                        case 'b':
                            result = '\b';
                            ++p;
                            break;
                        case 'f':
                            result = '\f';
                            ++p;
                            break;
                        case 'n':
                            result = '\n';
                            ++p;
                            break;
                        case 'r':
                            result = '\r';
                            ++p;
                            break;
                        case 't':
                            result = '\t';
                            ++p;
                            break;

                        case 'x': {
                            ++p;
                            result = 0;
                            auto* next = read_hex(
                                result,
                                p, std::min<ptrdiff_t>(2, end - p));
                            if (next > p) {
                                p = next;
                            } else {
                                result = 'x';
                            }
                        } break;

                        case '0':
                        case '1':
                        case '2':
                        case '3':
                            result = 0;
                            p = read_oct(
                                result,
                                p, std::min<ptrdiff_t>(3, end - p));
                            break;

                        case '4':
                        case '5':
                        case '6':
                        case '7':
                            result = 0;
                            p = read_oct(
                                result,
                                p, std::min<ptrdiff_t>(2, end - p));
                            break;
                    }
                    return p;
                }

                template <typename T, typename U>
                inline void unescape_impl(
                    const ui8* p,
                    const ui8* end,
                    T&& consume_one,
                    U&& consume_span) {
                    while (p < end) {
                        auto* escaped = static_cast<const ui8*>(
                            ::memchr(p, '\\', end - p));
                        if (escaped == nullptr) {
                            consume_span(p, end - p);
                            return;
                        } else {
                            consume_span(p, escaped - p);
                            auto c = ui8{'\\'};
                            p = escaped + 1;
                            if (p < end) {
                                p = unescape_char_and_advance(c, p, end);
                            }
                            consume_one(c);
                        }
                    }
                }
            }
        }     // namespace NCEscape
    }         // namespace NDetail
}
