#pragma once

#include "unicode_set_token.h"

#include <util/generic/strbuf.h>
#include <util/system/yassert.h>

namespace NUnicode {
    namespace NPrivate {
        class TUnicodeSetLexer {
        private:
            const TWtringBuf& Data;

            int cs;
            //int* stack;
            //int top;
            int act;
            const wchar16* ts;
            const wchar16* te;
            const wchar16* p;
            const wchar16* pe;
            const wchar16* eof;

            TUnicodeSetToken LastToken;
            bool UseLast;

        private:
            EUnicodeSetTokenType YieldToken(EUnicodeSetTokenType type);
            EUnicodeSetTokenType YieldToken(EUnicodeSetTokenType type, wchar16 symbol);
            EUnicodeSetTokenType YieldToken(EUnicodeSetTokenType type, const wchar16* dataBegin, size_t dataSize);
            void Reset();

        public:
            explicit TUnicodeSetLexer(const TWtringBuf& data);

            EUnicodeSetTokenType GetToken();

            const TUnicodeSetToken& GetLastToken() {
                return LastToken;
            }

            inline void PushBack() {
                Y_ABORT_UNLESS(!UseLast, "Double TUnicodeSetLexer::PushBack()");
                UseLast = true;
            }
        };

    }
}
