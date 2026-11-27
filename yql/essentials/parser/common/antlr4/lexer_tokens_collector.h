#pragma once

#include "error_listener.h"

#include <yql/essentials/parser/common/error.h>
#include <yql/essentials/parser/lexer_common/lexer.h>

#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

#include <utility>

namespace NAST {

template <typename TLexer>
class TLexerTokensCollector4 {
public:
    explicit TLexerTokensCollector4(TStringBuf data, TString queryName = "query")
        : QueryName_(std::move(queryName))
        , InputStream_(data)
        , Lexer_(&InputStream_)
    {
    }

    void CollectTokens(NAST::IErrorCollector& errors, const NSQLTranslation::ILexer::TTokenCallback& onNextToken) {
        try {
            bool error = false;
            typename antlr4::YqlErrorListener listener(&errors, &error);
            Lexer_.removeErrorListeners();
            Lexer_.addErrorListener(&listener);

            for (;;) {
                auto token = Lexer_.nextToken();
                auto type = token->getType();
                const bool isEOF = type == TLexer::EOF;
                NSQLTranslation::TParsedToken last;
                last.Name = GetTokenName(type);
                last.Content = token->getText();
                last.Line = token->getLine();
                last.LinePos = token->getCharPositionInLine();
                onNextToken(std::move(last));
                if (isEOF) {
                    break;
                }
            }
        } catch (const NAST::TTooManyErrors&) {
        } catch (...) {
            errors.Error(0, 0, CurrentExceptionMessage());
        }
    }

private:
    TString GetTokenName(size_t type) const {
        auto res = Lexer_.getVocabulary().getSymbolicName(type);
        if (res != "") {
            return TString(res);
        }
        return TString(NAST::INVALID_TOKEN_NAME);
    }

    TString QueryName_;
    antlr4::ANTLRInputStream InputStream_;
    TLexer Lexer_;
};

} // namespace NAST
