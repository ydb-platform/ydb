#pragma once

#include <yql/essentials/sql/v1/complete/antlr4/defs.h>

#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>

#include <util/generic/string.h>

#include <unordered_set>
#include <string>
#include <string_view>

#ifdef TOKEN_QUERY // Conflict with the winnt.h
    #undef TOKEN_QUERY
#endif
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>

#define RULE(name) NALADefaultAntlr4::SQLv1Antlr4Parser::Rule##name

namespace NSQLComplete {

    class ISqlGrammar {
    public:
        virtual ~ISqlGrammar() = default;
        virtual const antlr4::dfa::Vocabulary& GetVocabulary() const = 0;
        virtual const std::string& SymbolizedRule(TRuleId rule) const = 0;
        virtual TTokenId GetTokenId(std::string_view symbolized) const = 0;
        virtual TRuleId GetRuleId(std::string_view symbolized) const = 0;
        virtual const std::vector<std::string>& GetAllRules() const = 0;
        virtual const std::unordered_set<TTokenId>& GetAllTokens() const = 0;
        virtual const std::unordered_set<TTokenId>& GetKeywordTokens() const = 0;
        virtual const std::unordered_set<TTokenId>& GetPunctuationTokens() const = 0;
        virtual bool IsPlainIdentifier(TStringBuf content) const = 0;
    };

    const ISqlGrammar& GetSqlGrammar();

} // namespace NSQLComplete
