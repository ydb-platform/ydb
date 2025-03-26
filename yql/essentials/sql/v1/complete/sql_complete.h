#pragma once

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

        virtual TCompletion Complete(TCompletionInput input) = 0;
        virtual ~ISqlCompletionEngine() = default;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine();

} // namespace NSQLComplete
