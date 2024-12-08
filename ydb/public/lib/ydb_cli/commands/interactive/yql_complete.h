#pragma once

#include "c3_engine.h"
#include "yql_syntax.h"

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <string>

namespace NYdb {
    namespace NConsoleClient {

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
        };

        struct TCompletion final {
            TCompletedToken CompletedToken;
            TVector<TCandidate> Candidates;
        };

        class TYQLCompletionEngine final {
            using TDefaultYQLGrammar = TAntlrGrammar<
                NALPDefaultAntlr4::SQLv1Antlr4Lexer,
                NALPDefaultAntlr4::SQLv1Antlr4Parser>;

            using TAnsiYQLGrammar = TAntlrGrammar<
                NALPAnsiAntlr4::SQLv1Antlr4Lexer,
                NALPAnsiAntlr4::SQLv1Antlr4Parser>;

        public:
            TYQLCompletionEngine();

            TCompletion Complete(TStringBuf prefix);

        private:
            IC3Engine& GetEngine(EYQLSyntaxMode mode);

            TC3Engine<TDefaultYQLGrammar> DefaultEngine;
            TC3Engine<TAnsiYQLGrammar> AnsiEngine;
        };

    } // namespace NConsoleClient
} // namespace NYdb
