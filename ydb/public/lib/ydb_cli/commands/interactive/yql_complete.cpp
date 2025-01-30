#include "yql_complete.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/complete/string_util.h>

#include <util/generic/algorithm.h>

namespace NYdb {
    namespace NConsoleClient {

        using NSQLComplete::LastWord;
        using NSQLComplete::LastWordIndex;

        TCompletedToken GetCompletedToken(TStringBuf prefix);

        void EnrichWithKeywords(TVector<TCandidate>& candidates, TVector<std::string> keywords);

        void FilterByContent(TVector<TCandidate>& candidates, TStringBuf prefix);

        void RankingSort(TVector<TCandidate>& candidates);

        TYqlCompletionEngine::TYqlCompletionEngine() {
        }

        TCompletion TYqlCompletionEngine::Complete(TCompletionInput input) {
            auto prefix = input.Text.Head(input.CursorPosition);
            auto completedToken = GetCompletedToken(prefix);

            auto context = Engine.Complete(input);

            TVector<TCandidate> candidates;
            EnrichWithKeywords(candidates, context.Keywords);

            FilterByContent(candidates, completedToken.Content);

            RankingSort(candidates);

            return {
                .CompletedToken = std::move(completedToken),
                .Candidates = std::move(candidates),
            };
        }

        TCompletedToken GetCompletedToken(TStringBuf prefix) {
            return {
                .Content = LastWord(prefix),
                .SourcePosition = LastWordIndex(prefix),
            };
        }

        void EnrichWithKeywords(TVector<TCandidate>& candidates, TVector<TString> keywords) {
            for (auto keyword : keywords) {
                candidates.push_back({
                    .Kind = ECandidateKind::Keyword,
                    .Content = std::move(keyword),
                });
            }
        }

        void FilterByContent(TVector<TCandidate>& candidates, TStringBuf prefix) {
            const auto lowerPrefix = ToLowerUTF8(prefix);
            auto removed = std::ranges::remove_if(candidates, [&](const auto& candidate) {
                return !ToLowerUTF8(candidate.Content).StartsWith(lowerPrefix);
            });
            candidates.erase(std::begin(removed), std::end(removed));
        }

        void RankingSort(TVector<TCandidate>& candidates) {
            Sort(candidates, [](const TCandidate& lhs, const TCandidate& rhs) {
                return std::tie(lhs.Content, lhs.Kind) < std::tie(rhs.Content, rhs.Kind);
            });
        }

    } // namespace NConsoleClient
} // namespace NYdb

template <>
void Out<NYdb::NConsoleClient::ECandidateKind>(IOutputStream& out, NYdb::NConsoleClient::ECandidateKind kind) {
    switch (kind) {
        case NYdb::NConsoleClient::ECandidateKind::Keyword:
            out << "Keyword";
            break;
    }
}

template <>
void Out<NYdb::NConsoleClient::TCandidate>(IOutputStream& out, const NYdb::NConsoleClient::TCandidate& candidate) {
    out << "(" << candidate.Kind << ": " << candidate.Content << ")";
}
