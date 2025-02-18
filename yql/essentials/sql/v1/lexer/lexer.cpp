#include "lexer.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/parser/proto_ast/collect_issues/collect_issues.h>
#include <yql/essentials/parser/proto_ast/antlr3/proto_ast_antlr3.h>
#include <yql/essentials/parser/proto_ast/antlr4/proto_ast_antlr4.h>
#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/sql/v1/sql.h>

#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

namespace NALPDefault {
extern ANTLR_UINT8 *SQLv1ParserTokenNames[];
}

namespace NALPAnsi {
extern ANTLR_UINT8 *SQLv1ParserTokenNames[];
}


namespace NSQLTranslationV1 {

namespace {

#if defined(_tsan_enabled_)
TMutex SanitizerSQLTranslationMutex;
#endif

using NSQLTranslation::ILexer;

class TV1Lexer : public ILexer {
public:
    explicit TV1Lexer(bool ansi, bool antlr4)
        : Ansi(ansi), Antlr4(antlr4)
    {
    }

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) override {
        NYql::TIssues newIssues;
#if defined(_tsan_enabled_)
        TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
        NSQLTranslation::TErrorCollectorOverIssues collector(newIssues, maxErrors, queryName);
        if (Ansi && !Antlr4) {
            NProtoAST::TLexerTokensCollector3<NALPAnsi::SQLv1Lexer> tokensCollector(query, (const char**)NALPAnsi::SQLv1ParserTokenNames, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else if (!Ansi && !Antlr4) {
            NProtoAST::TLexerTokensCollector3<NALPDefault::SQLv1Lexer> tokensCollector(query, (const char**)NALPDefault::SQLv1ParserTokenNames, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else if (Ansi && Antlr4) {
            NProtoAST::TLexerTokensCollector4<NALPAnsiAntlr4::SQLv1Antlr4Lexer> tokensCollector(query, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else {
            NProtoAST::TLexerTokensCollector4<NALPDefaultAntlr4::SQLv1Antlr4Lexer> tokensCollector(query, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        }

        issues.AddIssues(newIssues);
        return !AnyOf(newIssues.begin(), newIssues.end(), [](auto issue) { return issue.GetSeverity() == NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR; });
    }

private:
    const bool Ansi;
    const bool Antlr4;
};

} // namespace

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi, bool antlr4) {
    return NSQLTranslation::ILexer::TPtr(new TV1Lexer(ansi, antlr4));
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
