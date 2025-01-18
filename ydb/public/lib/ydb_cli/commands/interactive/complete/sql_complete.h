#pragma once

#include "c3_engine.h"
#include "sql_syntax.h"

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <string>

namespace NSQLComplete {

    struct TCompletionInput final {
        TStringBuf Text;
        size_t CursorPosition = Text.length();
    };

    struct TCompletedToken final {
        TStringBuf Content;
        size_t SourcePosition;
    };

    enum class ECandidateKind {
        Keyword,
    };

    // std::string is used to prevent copying into replxx api
    struct TCandidate final {
        ECandidateKind Kind;
        std::string Content;

        friend bool operator==(const TCandidate& lhs, const TCandidate& rhs) = default;
    };

    struct TCompletion final {
        TCompletedToken CompletedToken;
        TVector<TCandidate> Candidates;
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

        TCompletion Complete(TCompletionInput input);

    private:
        IC3Engine& GetEngine(ESqlSyntaxMode mode);

        TVector<TString> SiftedKeywords(const TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode);
        const std::unordered_set<TTokenId>& GetKeywordTokens(ESqlSyntaxMode mode);

        TC3Engine<TDefaultYQLGrammar> DefaultEngine;
        TC3Engine<TAnsiYQLGrammar> AnsiEngine;

        std::unordered_set<TTokenId> DefaultKeywordTokens;
        std::unordered_set<TTokenId> AnsiKeywordTokens;
    };

} // namespace NSQLComplete
