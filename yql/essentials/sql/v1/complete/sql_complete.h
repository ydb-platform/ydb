#pragma once

#include <yql/essentials/sql/v1/complete/name/name_service.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TCompletionInput {
        TStringBuf Text;
        size_t CursorPosition = Text.length();
    };

    struct TCompletedToken {
        TStringBuf Content;
        size_t SourcePosition;
    };

    enum class ECandidateKind {
        Keyword,
        PragmaName,
        TypeName,
        FunctionName,
        HintName,
    };

    struct TCandidate {
        ECandidateKind Kind;
        TString Content;

        friend bool operator==(const TCandidate& lhs, const TCandidate& rhs) = default;
    };

    struct TCompletion {
        TCompletedToken CompletedToken;
        TVector<TCandidate> Candidates;
    };

    class ISqlCompletionEngine {
    public:
        using TPtr = THolder<ISqlCompletionEngine>;

        struct TConfiguration {
            size_t Limit = 256;
        };

        virtual TCompletion Complete(TCompletionInput input) = 0;
        virtual ~ISqlCompletionEngine() = default;
    };

    using TLexerSupplier = std::function<NSQLTranslation::ILexer::TPtr(bool ansi)>;

    // FIXME(YQL-19747): unwanted dependency on a lexer implementation
    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine();

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(
        TLexerSupplier lexer,
        INameService::TPtr names,
        ISqlCompletionEngine::TConfiguration configuration = {});

} // namespace NSQLComplete
