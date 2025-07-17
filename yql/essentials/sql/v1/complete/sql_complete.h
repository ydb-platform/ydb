#pragma once

#include "configuration.h"

#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/sql/v1/complete/core/environment.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NSQLComplete {

    struct TCompletedToken {
        TStringBuf Content;
        size_t SourcePosition = 0;
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
        ColumnName,
        BindingName,
        UnknownName,
    };

    struct TCandidate {
        ECandidateKind Kind;
        TString Content;
        size_t CursorShift = 0;
        TMaybe<TString> Documentation = Nothing();

        friend bool operator==(const TCandidate& lhs, const TCandidate& rhs) = default;

        TString FilterText() const;
    };

    struct TCompletion {
        TCompletedToken CompletedToken;
        TVector<TCandidate> Candidates;
    };

    // TODO(YQL-19747): Make it thread-safe.
    class ISqlCompletionEngine {
    public:
        using TPtr = THolder<ISqlCompletionEngine>;

        virtual ~ISqlCompletionEngine() = default;

        virtual NThreading::TFuture<TCompletion>
        Complete(TCompletionInput input, TEnvironment env = {}) = 0;

        virtual NThreading::TFuture<TCompletion> // TODO(YQL-19747): Migrate YDB CLI to `Complete` method
        CompleteAsync(TCompletionInput input, TEnvironment env = {}) = 0;
    };

    using TLexerSupplier = std::function<NSQLTranslation::ILexer::TPtr(bool ansi)>;

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(
        TLexerSupplier lexer,
        INameService::TPtr names,
        TConfiguration configuration = {});

} // namespace NSQLComplete
