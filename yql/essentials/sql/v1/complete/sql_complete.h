#pragma once

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

        friend bool operator==(const TCandidate& lhs, const TCandidate& rhs) = default;
    };

    struct TCompletion {
        TCompletedToken CompletedToken;
        TVector<TCandidate> Candidates;
    };

    // TODO(YQL-19747): Make it thread-safe.
    class ISqlCompletionEngine {
    public:
        using TPtr = THolder<ISqlCompletionEngine>;

        struct TConfiguration {
            friend class TSqlCompletionEngine;
            friend ISqlCompletionEngine::TConfiguration MakeYDBConfiguration();
            friend ISqlCompletionEngine::TConfiguration MakeYQLConfiguration();
            friend ISqlCompletionEngine::TConfiguration MakeConfiguration(THashSet<TString> allowedStmts);

        public:
            size_t Limit = 256;

        private:
            THashSet<TString> IgnoredRules;
            THashMap<TString, THashSet<TString>> DisabledPreviousByToken;
            THashMap<TString, THashSet<TString>> ForcedPreviousByToken;
        };

        virtual ~ISqlCompletionEngine() = default;

        virtual TCompletion
        Complete(TCompletionInput input, TEnvironment env = {}) = 0;

        virtual NThreading::TFuture<TCompletion> // TODO(YQL-19747): Migrate YDB CLI to `Complete` method
        CompleteAsync(TCompletionInput input, TEnvironment env = {}) = 0;
    };

    using TLexerSupplier = std::function<NSQLTranslation::ILexer::TPtr(bool ansi)>;

    ISqlCompletionEngine::TConfiguration MakeYDBConfiguration();

    ISqlCompletionEngine::TConfiguration MakeYQLConfiguration();

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(
        TLexerSupplier lexer,
        INameService::TPtr names,
        ISqlCompletionEngine::TConfiguration configuration = {});

} // namespace NSQLComplete
