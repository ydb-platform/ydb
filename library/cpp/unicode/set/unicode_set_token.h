#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NUnicode {
    namespace NPrivate {
        enum EUnicodeSetTokenType {
            USTT_EOS /* "eos" */,
            USTT_SYMBOL /* "symbol" */,
            USTT_QUOTED_PAIR /* "quoted-pair" */,
            USTT_CODEPOINT8 /* "codepoint8" */,
            USTT_CODEPOINT16 /* "codepoint16" */,
            USTT_CODEPOINT32 /* "codepoint32" */,
            USTT_CATEGORY /* "category" */,
            USTT_NEGATION /* "negation" */,
            USTT_RANGE /* "range" */,
            USTT_LBRACKET /* "lbracket" */,
            USTT_RBRACKET /* "rbracket" */,
        };

        struct TUnicodeSetToken {
            EUnicodeSetTokenType Type;
            wchar16 Symbol;
            TWtringBuf Data;

            explicit TUnicodeSetToken()
                : Type(USTT_EOS)
                , Symbol(0)
                , Data()
            {
            }

            explicit TUnicodeSetToken(EUnicodeSetTokenType tokenType)
                : Type(tokenType)
                , Symbol(0)
                , Data()
            {
            }

            explicit TUnicodeSetToken(EUnicodeSetTokenType tokenType, wchar16 symbol)
                : Type(tokenType)
                , Symbol(symbol)
                , Data()
            {
            }

            explicit TUnicodeSetToken(EUnicodeSetTokenType tokenType, const wchar16* dataBegin, size_t dataSize)
                : Type(tokenType)
                , Symbol(0)
                , Data(dataBegin, dataSize)
            {
            }
        };

    }
}

Y_DECLARE_OUT_SPEC(inline, NUnicode::NPrivate::TUnicodeSetToken, output, token) {
    output << token.Type;
    if (token.Symbol) {
        output << ":" << TUtf16String(1, token.Symbol).Quote();
    }
    if (!token.Data.empty()) {
        output << ":" << TUtf16String(token.Data).Quote();
    }
}
