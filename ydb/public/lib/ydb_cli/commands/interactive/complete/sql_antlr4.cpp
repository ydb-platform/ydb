#include "sql_antlr4.h"

#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

namespace NSQLComplete {
    
    // Returning a reference is okay as vocabulary storage is static
    const antlr4::dfa::Vocabulary& GetVocabulary(ESqlSyntaxMode mode) {
        switch (mode) {
            case ESqlSyntaxMode::Default:
                return NALPDefaultAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
            case ESqlSyntaxMode::ANSI:
                return NALPAnsiAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
        }
    }

    std::unordered_set<TTokenId> GetAllTokens(ESqlSyntaxMode mode) {
        const auto& vocabulary = GetVocabulary(mode);

        std::unordered_set<TTokenId> allTokens;

        for (size_t type = 1; type <= vocabulary.getMaxTokenType(); ++type) {
            allTokens.emplace(type);
        }

        return allTokens;
    }

    std::unordered_set<TTokenId> GetKeywordTokens(ESqlSyntaxMode mode) {
        const auto& vocabulary = GetVocabulary(mode);
        const auto keywords = NSQLFormat::GetKeywords();

        auto keywordTokens = GetAllTokens(mode);
        std::erase_if(keywordTokens, [&](TTokenId token) {
            return !keywords.contains(vocabulary.getSymbolicName(token));
        });
        keywordTokens.erase(TOKEN_EOF);

        return keywordTokens;
    }

}
