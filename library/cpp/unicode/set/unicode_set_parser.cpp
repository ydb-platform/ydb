#include "quoted_pair.h"
#include "unicode_set_lexer.h"

#include <util/string/cast.h>
#include <util/charset/wide.h>

namespace NUnicode {
    namespace NPrivate {
#define UNEXPECTED_TOKEN throw yexception() << "Unexpected token: " << lexer.GetLastToken()

#define EXPECT_TOKEN(type)          \
    if (lexer.GetToken() != type) { \
        UNEXPECTED_TOKEN;           \
    }

        void ParseUnicodeSet(TUnicodeSet& set, TUnicodeSetLexer& lexer);

        void ParseCharSequence(TUnicodeSet& set, TUnicodeSetLexer& lexer) {
            wchar32 prevChar = 0;
            bool range = false;
            for (EUnicodeSetTokenType type = lexer.GetToken(); type != USTT_RBRACKET; type = lexer.GetToken()) {
                wchar32 curChar = 0;
                switch (type) {
                    case USTT_SYMBOL:
                        curChar = lexer.GetLastToken().Symbol;
                        break;
                    case USTT_NEGATION:
                        curChar = '^';
                        break;
                    case USTT_QUOTED_PAIR:
                        ResolveUnicodeQuotedPair(lexer.GetLastToken().Symbol, curChar, set);
                        break;
                    case USTT_CODEPOINT8:
                    case USTT_CODEPOINT16:
                    case USTT_CODEPOINT32:
                        curChar = IntFromString<ui32, 16>(lexer.GetLastToken().Data);
                        if (curChar >= TUnicodeSet::CODEPOINT_HIGH) {
                            throw yexception() << "Invalid unicode codepoint: " << lexer.GetLastToken();
                        }
                        break;
                    case USTT_RANGE:
                        if (0 == prevChar) {
                            UNEXPECTED_TOKEN;
                        }
                        range = true;
                        continue;
                    case USTT_LBRACKET: {
                        lexer.PushBack();
                        TUnicodeSet inner;
                        ParseUnicodeSet(inner, lexer);
                        set.Add(inner);
                        break;
                    }
                    default:
                        UNEXPECTED_TOKEN;
                }
                if (curChar) {
                    if (range) {
                        if (prevChar >= curChar) {
                            throw yexception() << "Invalid character range";
                        }
                        set.Add(prevChar, curChar);
                        curChar = 0;
                    } else {
                        set.Add(curChar);
                    }
                } else if (range) {
                    UNEXPECTED_TOKEN;
                }
                range = false;
                prevChar = curChar;
            }
            if (range) {
                UNEXPECTED_TOKEN;
            }
            lexer.PushBack();
        }

        void ParseUnicodeSet(TUnicodeSet& set, TUnicodeSetLexer& lexer) {
            EXPECT_TOKEN(USTT_LBRACKET);
            bool invert = false;
            if (USTT_NEGATION == lexer.GetToken()) {
                invert = true;
            } else {
                lexer.PushBack();
            }

            if (USTT_CATEGORY == lexer.GetToken()) {
                set.AddCategory(WideToUTF8(lexer.GetLastToken().Data));
            } else {
                lexer.PushBack();
                ParseCharSequence(set, lexer);
            }

            EXPECT_TOKEN(USTT_RBRACKET);

            if (invert) {
                set.Invert();
            }
        }

        void ParseUnicodeSet(TUnicodeSet& set, const TWtringBuf& data) {
            TUnicodeSetLexer lexer(data);
            ParseUnicodeSet(set, lexer);
            EXPECT_TOKEN(USTT_EOS);
        }

    } // NPrivate
}
