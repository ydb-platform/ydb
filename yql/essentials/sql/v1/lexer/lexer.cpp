#include "lexer.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr3/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr3_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

namespace NSQLTranslationV1 {

namespace {

#if defined(_tsan_enabled_)
TMutex SanitizerSQLTranslationMutex;
#endif

using NSQLTranslation::ILexer;
using NSQLTranslation::MakeDummyLexerFactory;

class TV1Lexer : public ILexer {
public:
    explicit TV1Lexer(const TLexers& lexers, bool ansi, bool antlr4, bool pure)
        : Factory(GetFactory(lexers, ansi, antlr4, pure))
    {
    }

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) override {
#if defined(_tsan_enabled_)
        TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
        return Factory->MakeLexer()->Tokenize(query, queryName, onNextToken, issues, maxErrors);
    }

private:
    static NSQLTranslation::TLexerFactoryPtr GetFactory(const TLexers& lexers, bool ansi, bool antlr4, bool pure = false) {
        if (!ansi && !antlr4 && !pure) {
            if (lexers.Antlr3) {
                return lexers.Antlr3;
            }
            return MakeDummyLexerFactory("antlr3");
        } else if (ansi && !antlr4 && !pure) {
            if (lexers.Antlr3Ansi) {
                return lexers.Antlr3Ansi;
            }
            return MakeDummyLexerFactory("antlr3_ansi");
        } else if (!ansi && antlr4 && !pure) {
            if (lexers.Antlr4) {
                return lexers.Antlr4;
            }
            return MakeDummyLexerFactory("antlr4");
        } else if (ansi && antlr4 && !pure) {
            if (lexers.Antlr4Ansi) {
                return lexers.Antlr4Ansi;
            }
            return MakeDummyLexerFactory("antlr4_ansi");
        } else if (!ansi && antlr4 && pure) {
            if (lexers.Antlr4Pure) {
                return lexers.Antlr4Pure;
            }
            return MakeDummyLexerFactory("antlr4_pure");
        } else if (ansi && antlr4 && pure) {
            if (lexers.Antlr4PureAnsi) {
                return lexers.Antlr4PureAnsi;
            }
            return MakeDummyLexerFactory("antlr4_pure_ansi");
        } else if (!ansi && !antlr4 && pure) {
            return MakeDummyLexerFactory("antlr3_pure");
        } else {
            return MakeDummyLexerFactory("antlr3_pure_ansi");
        }
    }

private:
    NSQLTranslation::TLexerFactoryPtr Factory;
};

} // namespace

NSQLTranslation::ILexer::TPtr MakeLexer(const TLexers& lexers, bool ansi, bool antlr4, bool pure) {
    return NSQLTranslation::ILexer::TPtr(new TV1Lexer(lexers, ansi, antlr4, pure));
}

bool IsProbablyKeyword(const NSQLTranslation::TParsedToken& token) {
    return AsciiEqualsIgnoreCase(token.Name, token.Content);
}

using NSQLTranslation::TParsedTokenList;
using TTokenIterator = TParsedTokenList::const_iterator;

namespace {

enum EParenType {
    Open,
    Close,
    None
};

using TAdvanceCallback = std::function<EParenType(TTokenIterator& curr, TTokenIterator end)>;

TTokenIterator SkipWS(TTokenIterator curr, TTokenIterator end) {
    while (curr != end && curr->Name == "WS") {
        ++curr;
    }
    return curr;
}

TTokenIterator SkipWSOrComment(TTokenIterator curr, TTokenIterator end) {
    while (curr != end && (curr->Name == "WS" || curr->Name == "COMMENT")) {
        ++curr;
    }
    return curr;
}

TTokenIterator SkipToNextBalanced(TTokenIterator begin, TTokenIterator end, const TAdvanceCallback& advance) {
    i64 level = 0;
    TTokenIterator curr = begin;
    while (curr != end) {
        switch (advance(curr, end)) {
            case EParenType::Open: {
                ++level;
                break;
            }
            case EParenType::Close: {
                --level;
                if (level < 0) {
                    return end;
                } else if (level == 0) {
                    return curr;
                }
                break;
            }
            case EParenType::None:
                break;
        }
    }
    return curr;
}

TTokenIterator GetNextStatementBegin(TTokenIterator begin, TTokenIterator end) {
    TAdvanceCallback advanceLambdaBody = [](TTokenIterator& curr, TTokenIterator end) -> EParenType {
        Y_UNUSED(end);
        if (curr->Name == "LBRACE_CURLY") {
            ++curr;
            return EParenType::Open;
        } else if (curr->Name == "RBRACE_CURLY") {
            ++curr;
            return EParenType::Close;
        } else {
            ++curr;
            return EParenType::None;
        }
    };

    TAdvanceCallback advanceAction = [](TTokenIterator& curr, TTokenIterator end) -> EParenType {
        auto tmp = curr;
        if (curr->Name == "DEFINE") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && (curr->Name == "ACTION" || curr->Name == "SUBQUERY")) {
                ++curr;
                return EParenType::Open;
            }
        } else if (curr->Name == "END") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && curr->Name == "DEFINE") {
                ++curr;
                return EParenType::Close;
            }
        }

        curr = tmp;
        ++curr;
        return EParenType::None;
    };

    TAdvanceCallback advanceInlineAction = [](TTokenIterator& curr, TTokenIterator end) -> EParenType {
        auto tmp = curr;
        if (curr->Name == "DO") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && curr->Name == "BEGIN") {
                ++curr;
                return EParenType::Open;
            }
        } else if (curr->Name == "END") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && curr->Name == "DO") {
                ++curr;
                return EParenType::Close;
            }
        }

        curr = tmp;
        ++curr;
        return EParenType::None;
    };

    TTokenIterator curr = begin;
    while (curr != end) {
        bool matched = false;
        for (auto cb : {advanceLambdaBody, advanceAction, advanceInlineAction}) {
            TTokenIterator tmp = curr;
            if (cb(tmp, end) == EParenType::Open) {
                curr = SkipToNextBalanced(curr, end, cb);
                matched = true;
                if (curr == end) {
                    return curr;
                }
            }
        }
        if (matched) {
            continue;
        }
        if (curr->Name == "SEMICOLON") {
            auto next = SkipWS(curr + 1, end);
            while (next != end && next->Name == "COMMENT" && curr->Line == next->Line) {
                curr = next;
                next = SkipWS(next + 1, end);
            }
            ++curr;
            break;
        }
        ++curr;
    }

    return curr;
}

void SplitByStatements(TTokenIterator begin, TTokenIterator end, TVector<TTokenIterator>& output) {
    output.clear();
    if (begin == end) {
        return;
    }
    output.push_back(begin);
    auto curr = begin;
    while (curr != end) {
        curr = GetNextStatementBegin(curr, end);
        output.push_back(curr);
    }
}

}

bool SplitQueryToStatements(const TString& query, NSQLTranslation::ILexer::TPtr& lexer, TVector<TString>& statements, NYql::TIssues& issues, const TString& file) {
    TParsedTokenList allTokens;
    auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        if (token.Name != "EOF") {
            allTokens.push_back(token);
        }
    };

    if (!lexer->Tokenize(query, file, onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        return false;
    }

    TVector<TTokenIterator> statementsTokens;
    SplitByStatements(allTokens.begin(), allTokens.end(), statementsTokens);

    for (size_t i = 1; i < statementsTokens.size(); ++i) {
        TStringBuilder currentQueryBuilder;
        for (auto it = statementsTokens[i - 1]; it != statementsTokens[i]; ++it) {
            currentQueryBuilder << it->Content;
        }
        TString statement = currentQueryBuilder;
        statement = StripStringLeft(statement);

        bool isBlank = true;
        for (auto c : statement) {
            if (c != ';') {
                isBlank = false;
                break;
            }
        };

        if (isBlank) {
            continue;
        }

        statements.push_back(statement);
    }

    return true;
}

} //  namespace NSQLTranslationV1
