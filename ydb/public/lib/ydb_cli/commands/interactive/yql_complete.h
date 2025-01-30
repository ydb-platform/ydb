#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/complete/sql_complete.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <string>

namespace NYdb {
    namespace NConsoleClient {

        using NSQLComplete::TCompletionInput;
        using NSQLComplete::TSqlCompletionEngine;

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

        class TYqlCompletionEngine final {
        public:
            TYqlCompletionEngine();

            TCompletion Complete(TCompletionInput input);

        private:
            TSqlCompletionEngine Engine;
        };

    } // namespace NConsoleClient
} // namespace NYdb
