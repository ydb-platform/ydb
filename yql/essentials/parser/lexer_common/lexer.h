#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

#include <functional>

namespace NYql {

class TIssues;

}

namespace NSQLTranslation {

struct TParsedToken {
    // TODO: TStringBuf for Name & Content
    TString Name;
    TString Content;
    // Position of first token byte/symbol
    // When antlr3 lexer is used, LinePos is a position as in a byte array,
    // but when antlr4 lexer is used, LinePos is a position as in a symbol array,
    ui32 Line = 0;    // starts from 1
    ui32 LinePos = 0; // starts from 0
};

class ILexer {
public:
    using TPtr = THolder<ILexer>;
    using TTokenCallback = std::function<void(TParsedToken&& token)>;

    virtual bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) = 0;
    virtual ~ILexer() = default;
};

using TParsedTokenList = TVector<TParsedToken>;

IOutputStream& OutputTokens(IOutputStream& out, TParsedTokenList::const_iterator begin, TParsedTokenList::const_iterator end);
bool Tokenize(ILexer& lexer, const TString& query, const TString& queryName, TParsedTokenList& tokens, NYql::TIssues& issues, size_t maxErrors);

class ILexerFactory : public TThrRefBase {
public:
    virtual ~ILexerFactory() = default;

    virtual ILexer::TPtr MakeLexer() const = 0;
};

using TLexerFactoryPtr = TIntrusivePtr<ILexerFactory>;

TLexerFactoryPtr MakeDummyLexerFactory(const TString& name);

}

