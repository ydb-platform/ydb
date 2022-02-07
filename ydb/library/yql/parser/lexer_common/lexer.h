#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

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
};

using TParsedTokenList = TVector<TParsedToken>;

IOutputStream& OutputTokens(IOutputStream& out, TParsedTokenList::const_iterator begin, TParsedTokenList::const_iterator end);

class ILexer {
public:
    using TPtr = THolder<ILexer>;

    virtual bool Tokenize(const TString& query, const TString& queryName, TParsedTokenList& tokens, NYql::TIssues& issues, size_t maxErrors) = 0;
    virtual ~ILexer() = default;
};

}

