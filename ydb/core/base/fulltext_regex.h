#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <algorithm>

namespace NKikimr::NFulltext {

enum class ERegexCandidateOp { Any, Term, And, Or };

// A necessary condition for an RE2 match. Leaves hold grams during extraction
// and posting stream indices during the merge.
template <typename TTerm>
struct TRegexCandidateExpr {
    using EOp = ERegexCandidateOp;
    EOp Op = EOp::Any;
    TTerm Term{};
    TVector<TRegexCandidateExpr> Children;

    bool Matches(const auto& hasTerm) const {
        if (Op == EOp::Any) {
            return true;
        }
        if (Op == EOp::Term) {
            return hasTerm(Term);
        }
        auto matches = [&](const auto& child) { return child.Matches(hasTerm); };
        if (Op == EOp::And) {
            return std::all_of(Children.begin(), Children.end(), matches);
        }
        return std::any_of(Children.begin(), Children.end(), matches);
    }

    void ForEachTerm(const auto& visit) const {
        if (Op == EOp::Term) {
            visit(Term);
        }
        for (const auto& child : Children) {
            child.ForEachTerm(visit);
        }
    }

    TVector<TTerm> UniqueTerms() const {
        TVector<TTerm> terms;
        ForEachTerm([&](const auto& term) { terms.push_back(term); });
        SortUnique(terms);
        return terms;
    }
};

using TRegexCandidates = TRegexCandidateExpr<TString>;

TRegexCandidates BuildRegexCandidates(TStringBuf pattern, const Ydb::Table::FulltextIndexSettings::Analyzers& settings);

}
