#include "lexer.h"


namespace NSQLTranslation {

IOutputStream& OutputTokens(IOutputStream& out, TParsedTokenList::const_iterator begin, TParsedTokenList::const_iterator end) {
    for (auto it = begin; it != end; ++it) {
        out << it->Content;
    }
    return out;
}

bool Tokenize(ILexer& lexer, const TString& query, const TString& queryName, TParsedTokenList& tokens, NYql::TIssues& issues, size_t maxErrors) {
    auto onNextToken = [&tokens](TParsedToken&& token) {
        tokens.push_back(std::move(token));
    };

    return lexer.Tokenize(query, queryName, onNextToken, issues, maxErrors);
}


}
