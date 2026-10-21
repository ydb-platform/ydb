#include "format.h"

#include "grammar.h"

#include <yql/essentials/sql/v1/ide/completion/antlr4/vocabulary.h>

#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>

#include <util/generic/hash_set.h>
#include <util/charset/utf8.h>

namespace NSQLComplete {

namespace {

const THashSet<std::string> Separated = [] {
    using SQLv1 = NALADefaultAntlr4::SQLv1Antlr4Lexer;

    const auto& grammar = GetSqlGrammar();
    const auto& vocabulary = grammar.GetVocabulary();

    const auto& keywordTokens = grammar.GetKeywordTokens();
    const std::initializer_list<TTokenId> extraTokens = {
        SQLv1::TOKEN_EQUALS,
    };

    THashSet<std::string> keywords;
    keywords.reserve(keywordTokens.size() + extraTokens.size());
    for (const auto& token : keywordTokens) {
        keywords.emplace(Display(vocabulary, token));
    }
    for (const auto& token : extraTokens) {
        keywords.emplace(Display(vocabulary, token));
    }
    return keywords;
}();

} // namespace

TString FormatKeywords(const TVector<TString>& seq) {
    if (seq.empty()) {
        return "";
    }

    TString text = seq[0];
    for (size_t i = 1; i < seq.size(); ++i) {
        const auto& token = seq[i];
        if (Separated.contains(token)) {
            text += " ";
        }
        text += token;
    }
    return text;
}

bool IsPlain(TStringBuf content) {
    return GetSqlGrammar().IsPlainIdentifier(content);
}

bool IsBinding(TStringBuf content) {
    return !content.empty() && content.front() == '$';
}

TStringBuf Unbinded(TStringBuf content) {
    Y_ENSURE(IsBinding(content));
    return content.SubStr(1);
}

} // namespace NSQLComplete
