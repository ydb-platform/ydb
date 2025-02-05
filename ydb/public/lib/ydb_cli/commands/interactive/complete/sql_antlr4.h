#pragma once

#include "sql_syntax.h"

#include <contrib/libs/antlr4_cpp_runtime/src/Token.h>
#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>

#include <unordered_set>

namespace NSQLComplete {

    using TTokenId = size_t;
    using TRuleId = size_t;

    constexpr TTokenId TOKEN_EOF = antlr4::Token::EOF;

    const antlr4::dfa::Vocabulary& GetVocabulary(ESqlSyntaxMode mode);

    std::unordered_set<TTokenId> GetAllTokens(ESqlSyntaxMode mode);

    std::unordered_set<TTokenId> GetKeywordTokens(ESqlSyntaxMode mode);

    const TVector<TRuleId>& GetKeywordRules(ESqlSyntaxMode mode);

} // namespace NSQLComplete
