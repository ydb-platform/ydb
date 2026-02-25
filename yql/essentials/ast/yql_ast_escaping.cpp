#include "yql_ast_escaping.h"

#include <util/charset/wide.h>
#include <util/stream/output.h>
#include <util/string/hex.h>

#include <array>

namespace NYql {

namespace {

char HexDigit(char c)
{
    return (c < 10 ? '0' + c : 'A' + (c - 10));
}

void EscapedPrintChar(ui8 c, IOutputStream* out)
{
    switch (c) {
        case '\\':
            out->Write("\\\\", 2);
            break;
        case '"':
            out->Write("\\\"", 2);
            break;
        case '\t':
            out->Write("\\t", 2);
            break;
        case '\n':
            out->Write("\\n", 2);
            break;
        case '\r':
            out->Write("\\r", 2);
            break;
        case '\b':
            out->Write("\\b", 2);
            break;
        case '\f':
            out->Write("\\f", 2);
            break;
        case '\a':
            out->Write("\\a", 2);
            break;
        case '\v':
            out->Write("\\v", 2);
            break;
        default: {
            if (isprint(c)) {
                out->Write(static_cast<char>(c));
            } else {
                std::array<char, 4> buf = {"\\x"};
                buf[2] = HexDigit((c & 0xf0) >> 4);
                buf[3] = HexDigit((c & 0x0f));
                out->Write(buf.data(), 4);
            }
        }
    }
}

void EscapedPrintUnicode(wchar32 rune, IOutputStream* out)
{
    static const int MaxEscapeLen = 10;

    if (rune < 0x80) {
        EscapedPrintChar(static_cast<ui8>(rune & 0xff), out);
    } else {
        int i = 0;
        std::array<char, MaxEscapeLen> buf;

        if (rune < 0x10000) {
            buf[i++] = '\\';
            buf[i++] = 'u';
        } else {
            buf[i++] = '\\';
            buf[i++] = 'U';
            buf[i++] = HexDigit((rune & 0xf0000000) >> 28);
            buf[i++] = HexDigit((rune & 0x0f000000) >> 24);
            buf[i++] = HexDigit((rune & 0x00f00000) >> 20);
            buf[i++] = HexDigit((rune & 0x000f0000) >> 16);
        }

        buf[i++] = HexDigit((rune & 0xf000) >> 12);
        buf[i++] = HexDigit((rune & 0x0f00) >> 8);
        buf[i++] = HexDigit((rune & 0x00f0) >> 4);
        buf[i++] = HexDigit((rune & 0x000f));

        out->Write(buf.data(), i);
    }
}

bool TryParseOctal(const char*& p, const char* e, int maxlen, wchar32* value)
{
    while (maxlen-- && p != e) {
        if (*value > 255) {
            return false;
        }

        char ch = *p++;
        if (ch >= '0' && ch <= '7') {
            *value = *value * 8 + (ch - '0');
            continue;
        }

        break;
    }

    return (maxlen == -1);
}

bool TryParseHex(const char*& p, const char* e, int maxlen, wchar32* value)
{
    while (maxlen-- > 0 && p != e) {
        char ch = *p++;
        if (ch >= '0' && ch <= '9') {
            *value = *value * 16 + (ch - '0');
            continue;
        }

        // to lower case
        ch |= 0x20;

        if (ch >= 'a' && ch <= 'f') {
            *value = *value * 16 + (ch - 'a') + 10;
            continue;
        }

        break;
    }

    return (maxlen == -1);
}

bool IsValidUtf8Rune(wchar32 value) {
    return value <= 0x10ffff && (value < 0xd800 || value > 0xdfff);
}

} // namespace

TStringBuf UnescapeResultToString(EUnescapeResult result)
{
    switch (result) {
        case EUnescapeResult::OK:
            return "OK";
        case EUnescapeResult::INVALID_ESCAPE_SEQUENCE:
            return "Expected escape sequence";
        case EUnescapeResult::INVALID_BINARY:
            return "Invalid binary value";
        case EUnescapeResult::INVALID_OCTAL:
            return "Invalid octal value";
        case EUnescapeResult::INVALID_HEXADECIMAL:
            return "Invalid hexadecimal value";
        case EUnescapeResult::INVALID_UNICODE:
            return "Invalid unicode value";
        case EUnescapeResult::INVALID_END:
            return "Unexpected end of atom";
    }
    return "Unknown unescape error";
}

void EscapeArbitraryAtom(TStringBuf atom, char quoteChar, IOutputStream* out)
{
    out->Write(quoteChar);
    const ui8 *p = reinterpret_cast<const ui8*>(atom.begin()),
              *e = reinterpret_cast<const ui8*>(atom.end());
    while (p != e) {
        wchar32 rune = 0;
        size_t rune_len = 0;

        if (SafeReadUTF8Char(rune, rune_len, p, e) == RECODE_RESULT::RECODE_OK && IsValidUtf8Rune(rune)) {
            EscapedPrintUnicode(rune, out);
            p += rune_len;
        } else {
            EscapedPrintChar(*p++, out);
        }
    }
    out->Write(quoteChar);
}

EUnescapeResult UnescapeArbitraryAtom(
    TStringBuf atom, char endChar, IOutputStream* out, size_t* readBytes)
{
    const char *p = atom.begin(),
               *e = atom.end();

    while (p != e) {
        char current = *p++;

        // C-style escape sequences
        if (current == '\\') {
            if (p == e) {
                *readBytes = p - atom.begin();
                return EUnescapeResult::INVALID_ESCAPE_SEQUENCE;
            }

            char next = *p++;
            switch (next) {
                case 't':
                    current = '\t';
                    break;
                case 'n':
                    current = '\n';
                    break;
                case 'r':
                    current = '\r';
                    break;
                case 'b':
                    current = '\b';
                    break;
                case 'f':
                    current = '\f';
                    break;
                case 'a':
                    current = '\a';
                    break;
                case 'v':
                    current = '\v';
                    break;
                case '0':
                case '1':
                case '2':
                case '3': {
                    wchar32 value = (next - '0');
                    if (!TryParseOctal(p, e, 2, &value)) {
                        *readBytes = p - atom.begin();
                        return EUnescapeResult::INVALID_OCTAL;
                    }
                    current = value & 0xff;
                    break;
                }
                case 'x': {
                    wchar32 value = 0;
                    if (!TryParseHex(p, e, 2, &value)) {
                        *readBytes = p - atom.begin();
                        return EUnescapeResult::INVALID_HEXADECIMAL;
                    }
                    current = value & 0xff;
                    break;
                }
                case 'u':
                case 'U': {
                    wchar32 value = 0;
                    int len = (next == 'u' ? 4 : 8);
                    if (!TryParseHex(p, e, len, &value) || !IsValidUtf8Rune(value)) {
                        *readBytes = p - atom.begin();
                        return EUnescapeResult::INVALID_UNICODE;
                    }
                    size_t written = 0;
                    std::array<char, 4> buf;
                    WideToUTF8(&value, 1, buf.data(), written);
                    out->Write(buf.data(), written);
                    continue;
                }
                default: {
                    current = next;
                }
            }
        } else if (endChar == '`') {
            if (current == '`') {
                if (p == e) {
                    *readBytes = p - atom.begin();
                    return EUnescapeResult::OK;
                } else {
                    if (*p != '`') {
                        *readBytes = p - atom.begin();
                        return EUnescapeResult::INVALID_ESCAPE_SEQUENCE;
                    } else {
                        p++;
                    }
                }
            }
        } else if (current == endChar) {
            *readBytes = p - atom.begin();
            return EUnescapeResult::OK;
        }

        out->Write(current);
    }

    *readBytes = p - atom.begin();
    return EUnescapeResult::INVALID_END;
}

void EscapeBinaryAtom(TStringBuf atom, char quoteChar, IOutputStream* out)
{
    auto prefix = std::to_array<char>({'x', quoteChar});
    out->Write(prefix.data(), 2);
    out->Write(HexEncode(atom.data(), atom.size()));
    out->Write(quoteChar);
}

EUnescapeResult UnescapeBinaryAtom(
    TStringBuf atom, char endChar, IOutputStream* out, size_t* readBytes)
{
    const char *p = atom.begin(),
               *e = atom.end();

    while (p != e) {
        char current = *p;
        if (current == endChar) {
            *readBytes = p - atom.begin();
            return EUnescapeResult::OK;
        }

        wchar32 byte = 0;
        if (!TryParseHex(p, e, 2, &byte)) {
            *readBytes = p - atom.begin();
            return EUnescapeResult::INVALID_BINARY;
        }

        out->Write(byte & 0xff);
    }

    *readBytes = p - atom.begin();
    return EUnescapeResult::INVALID_END;
}

} // namespace NYql
