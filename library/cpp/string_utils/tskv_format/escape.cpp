#include <util/generic/yexception.h>
#include "escape.h"

namespace NTskvFormat {
    namespace {
        const TStringBuf ESCAPE_CHARS("\t\n\r\\\0=\"", 7);

        TString& EscapeImpl(const char* src, size_t len, TString& dst) {
            TStringBuf srcStr(src, len);
            size_t noEscapeStart = 0;

            while (noEscapeStart < len) {
                size_t noEscapeEnd = srcStr.find_first_of(ESCAPE_CHARS, noEscapeStart);

                if (noEscapeEnd == TStringBuf::npos) {
                    dst.append(src + noEscapeStart, len - noEscapeStart);
                    break;
                }

                dst.append(src + noEscapeStart, noEscapeEnd - noEscapeStart);

                switch (src[noEscapeEnd]) {
                    case '\t':
                        dst.append(TStringBuf("\\t"));
                        break;
                    case '\n':
                        dst.append(TStringBuf("\\n"));
                        break;
                    case '\r':
                        dst.append(TStringBuf("\\r"));
                        break;
                    case '\0':
                        dst.append(TStringBuf("\\0"));
                        break;
                    case '\\':
                        dst.append(TStringBuf("\\\\"));
                        break;
                    case '=':
                        dst.append(TStringBuf("\\="));
                        break;
                    case '"':
                        dst.append(TStringBuf("\\\""));
                        break;
                }

                noEscapeStart = noEscapeEnd + 1;
            }

            return dst;
        }

        TString& UnescapeImpl(const char* src, const size_t len, TString& dst) {
            TStringBuf srcStr(src, len);
            size_t noEscapeStart = 0;

            while (noEscapeStart < len) {
                size_t noEscapeEnd = srcStr.find('\\', noEscapeStart);

                if (noEscapeEnd == TStringBuf::npos) {
                    dst.append(src + noEscapeStart, len - noEscapeStart);
                    break;
                }

                dst.append(src + noEscapeStart, noEscapeEnd - noEscapeStart);

                if (noEscapeEnd + 1 >= len) {
                    throw yexception() << "expected (t|n|r|0|\\|=|\"|) after \\. Got end of line.";
                }

                switch (src[noEscapeEnd + 1]) {
                    case 't':
                        dst.append('\t');
                        break;
                    case 'n':
                        dst.append('\n');
                        break;
                    case 'r':
                        dst.append('\r');
                        break;
                    case '0':
                        dst.append('\0');
                        break;
                    case '\\':
                        dst.append('\\');
                        break;
                    case '=':
                        dst.append('=');
                        break;
                    case '"':
                        dst.append('"');
                        break;
                    default:
                        throw yexception() << "unexpected symbol '" << src[noEscapeEnd + 1] << "' after \\";
                }

                noEscapeStart = noEscapeEnd + 2;
            }

            return dst;
        }

    }

    TString& Escape(const TStringBuf& src, TString& dst) {
        return EscapeImpl(src.data(), src.size(), dst);
    }

    TString& Unescape(const TStringBuf& src, TString& dst) {
        return UnescapeImpl(src.data(), src.size(), dst);
    }

}
