#include "lexer.h"

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/parser/proto_ast/collect_issues/collect_issues.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi/SQLv1Lexer.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

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
    explicit TV1Lexer(bool ansi)
        : Ansi(ansi)
    {
    }

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) override {
        NYql::TIssues newIssues;
#if defined(_tsan_enabled_)
        TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
        NSQLTranslation::TErrorCollectorOverIssues collector(newIssues, maxErrors, "");
        if (Ansi) {
            NProtoAST::TLexerTokensCollector<NALPAnsi::SQLv1Lexer> tokensCollector(query, (const char**)NALPAnsi::SQLv1ParserTokenNames, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else {
            NProtoAST::TLexerTokensCollector<NALPDefault::SQLv1Lexer> tokensCollector(query, (const char**)NALPDefault::SQLv1ParserTokenNames, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        }

        issues.AddIssues(newIssues);
        return !AnyOf(newIssues.begin(), newIssues.end(), [](auto issue) { return issue.GetSeverity() == NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR; });
    }

private:
    const bool Ansi;
};

} // namespace

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi) {
    return NSQLTranslation::ILexer::TPtr(new TV1Lexer(ansi));
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

bool SplitQueryToStatements(const TString& query, NSQLTranslation::ILexer::TPtr& lexer, TVector<TString>& statements, NYql::TIssues& issues) {
    TParsedTokenList allTokens;
    auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        if (token.Name != "EOF") {
            allTokens.push_back(token);
        }
    };

    if (!lexer->Tokenize(query, "Query", onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
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
