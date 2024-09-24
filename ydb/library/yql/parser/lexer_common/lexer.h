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
    // Position of first token symbol
    ui32 Line = 0;    // starts from 1
    ui32 LinePos = 0; // starts from 0
    // Position of first and last token symbol
    ui32 StartPos; // starts from 0
    ui32 StopPos;   // starts from 0
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

// "Probably" because YQL keyword can be an identifier
// depending on a query context. For example
// in SELECT * FROM group - group is an identifier, but
// in SELECT * FROM ... GROUP BY ... - group is a keyword.
bool IsProbablyKeyword(const TParsedToken& token);

}

