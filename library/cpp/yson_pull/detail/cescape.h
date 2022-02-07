#pragma once

#include "byte_writer.h"
#include "cescape_decode.h"
#include "cescape_encode.h"
#include "macros.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

/* REFERENCES FOR ESCAPE SEQUENCE INTERPRETATION:
 *   C99 p. 6.4.3   Universal character names.
 *   C99 p. 6.4.4.4 Character constants.
 *
 * <simple-escape-sequence> ::= {
 *      \' , \" , \? , \\ ,
 *      \a , \b , \f , \n , \r , \t , \v
 * }
 *
 * <octal-escape-sequence>       ::= \  <octal-digit> {1, 3}
 * <hexadecimal-escape-sequence> ::= \x <hexadecimal-digit> +
 * <universal-character-name>    ::= \u <hexadecimal-digit> {4}
 *                                || \U <hexadecimal-digit> {8}
 *
 * NOTE (6.4.4.4.7):
 * Each octal or hexadecimal escape sequence is the longest sequence of characters that can
 * constitute the escape sequence.
 *
 * THEREFORE:
 *  - Octal escape sequence spans until rightmost non-octal-digit character.
 *  - Octal escape sequence always terminates after three octal digits.
 *  - Hexadecimal escape sequence spans until rightmost non-hexadecimal-digit character.
 *  - Universal character name consists of exactly 4 or 8 hexadecimal digit.
 *
 */

namespace NYsonPull {
    namespace NDetail {
        namespace NCEscape {
            inline void encode(TString& dest, TStringBuf data) {
                NImpl::escape_impl(
                    reinterpret_cast<const ui8*>(data.data()),
                    data.size(),
                    [&](const ui8* str, size_t size) {
                        dest.append(
                            reinterpret_cast<const char*>(str),
                            size);
                    });
            }

            // dest must have at least 4*data.size() bytes available
            inline size_t encode(ui8* dest, TStringBuf data) {
                auto* dest_begin = dest;
                NImpl::escape_impl(
                    reinterpret_cast<const ui8*>(data.data()),
                    data.size(),
                    [&](const ui8* str, size_t size) {
                        ::memcpy(dest, str, size);
                        dest += size;
                    });
                return dest - dest_begin;
            }

            template <typename U>
            void encode(byte_writer<U>& dest, TStringBuf data) {
                auto& buffer = dest.stream().buffer();
                if (Y_LIKELY(buffer.available() >= data.size() * 4)) {
                    auto size = encode(buffer.pos(), data);
                    dest.advance(size);
                } else {
                    NImpl::escape_impl(
                        reinterpret_cast<const ui8*>(data.data()),
                        data.size(),
                        [&](const ui8* str, size_t size) {
                            dest.write(str, size);
                        });
                }
            }

            inline TString encode(TStringBuf data) {
                TString result;
                result.reserve(data.size());
                encode(result, data);
                return result;
            }

            inline void decode(TString& dest, TStringBuf data) {
                NImpl::unescape_impl(
                    reinterpret_cast<const ui8*>(data.begin()),
                    reinterpret_cast<const ui8*>(data.end()),
                    [&](ui8 c) {
                        dest += c;
                    },
                    [&](const ui8* p, size_t len) {
                        dest.append(reinterpret_cast<const char*>(p), len);
                    });
            }

            inline void decode_inplace(TVector<ui8>& data) {
                auto* out = static_cast<ui8*>(
                    ::memchr(data.data(), '\\', data.size()));
                if (out == nullptr) {
                    return;
                }
                NImpl::unescape_impl(
                    out,
                    data.data() + data.size(),
                    [&](ui8 c) {
                        *out++ = c;
                    },
                    [&](const ui8* p, size_t len) {
                        ::memmove(out, p, len);
                        out += len;
                    });
                data.resize(out - &data[0]);
            }

            inline TString decode(TStringBuf data) {
                TString result;
                result.reserve(data.size());
                decode(result, data);
                return result;
            }

            ATTRIBUTE(noinline, cold)
            inline TString quote(TStringBuf str) {
                TString result;
                result.reserve(str.size() + 16);
                result += '"';
                encode(result, str);
                result += '"';
                return result;
            }

            ATTRIBUTE(noinline, cold)
            inline TString quote(ui8 ch) {
                char c = ch;
                return quote(TStringBuf(&c, 1));
            }
        }
    }     // namespace NDetail
}
