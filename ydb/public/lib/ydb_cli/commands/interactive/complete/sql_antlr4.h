#pragma once

#include "sql_syntax.h"

#include <contrib/libs/antlr4_cpp_runtime/src/Token.h>
#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>

#include <unordered_set>

namespace NSQLComplete {

    using TTokenId = size_t;
    using TRuleId = size_t;

    constexpr TTokenId TOKEN_EOF = antlr4::Token::EOF;

    class ISqlGrammar {
    public:
        using TPtr = ISqlGrammar*;

        virtual const antlr4::dfa::Vocabulary& GetVocabulary() = 0;
        virtual const std::unordered_set<TTokenId>& GetAllTokens() = 0;
        virtual const std::unordered_set<TTokenId>& GetKeywordTokens() = 0;
        virtual const TVector<TRuleId>& GetKeywordRules() = 0;
        virtual ~ISqlGrammar() = default;
    };

    ISqlGrammar::TPtr MakeSqlGrammar(ESqlSyntaxMode mode);

} // namespace NSQLComplete
