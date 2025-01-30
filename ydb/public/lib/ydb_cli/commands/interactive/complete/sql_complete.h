#pragma once

#include "c3_engine.h"
#include "sql_syntax.h"

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TCompletionInput final {
        TStringBuf Text;
        size_t CursorPosition = Text.length();
    };

    // std::string is used to prevent copying into replxx api
    struct TCompletionContext final {
        TVector<std::string> Keywords;
    };

    class TSqlCompletionEngine final {
        using TDefaultYQLGrammar = TAntlrGrammar<
            NALPDefaultAntlr4::SQLv1Antlr4Lexer,
            NALPDefaultAntlr4::SQLv1Antlr4Parser>;

        using TAnsiYQLGrammar = TAntlrGrammar<
            NALPAnsiAntlr4::SQLv1Antlr4Lexer,
            NALPAnsiAntlr4::SQLv1Antlr4Parser>;

    public:
        TSqlCompletionEngine();

        TCompletionContext Complete(TCompletionInput input);

    private:
        IC3Engine& GetEngine(ESqlSyntaxMode mode);

        TVector<std::string> SiftedKeywords(const TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode);
        const std::unordered_set<TTokenId>& GetKeywordTokens(ESqlSyntaxMode mode);

        TC3Engine<TDefaultYQLGrammar> DefaultEngine;
        TC3Engine<TAnsiYQLGrammar> AnsiEngine;

        std::unordered_set<TTokenId> DefaultKeywordTokens;
        std::unordered_set<TTokenId> AnsiKeywordTokens;
    };

} // namespace NSQLComplete
