#pragma once

#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

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
        FolderName,
        TableName,
        ClusterName,
        UnknownName,
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

        virtual ~ISqlCompletionEngine() = default;
        virtual TCompletion Complete(TCompletionInput input) = 0; // TODO(YQL-19747): migrate YDB CLI to CompleteAsync
        virtual NThreading::TFuture<TCompletion> CompleteAsync(TCompletionInput input) = 0;
    };

    using TLexerSupplier = std::function<NSQLTranslation::ILexer::TPtr(bool ansi)>;

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(
        TLexerSupplier lexer,
        INameService::TPtr names,
        ISqlCompletionEngine::TConfiguration configuration = {});

} // namespace NSQLComplete
