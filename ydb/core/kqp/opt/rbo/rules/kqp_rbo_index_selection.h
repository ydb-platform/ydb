#pragma once

#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_predicate_normalize.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp {

// The rule-based order of candidates, compared as a tuple: greater is better.
struct TIndexScore {
    bool SortMatchesAndNoResidual = false;
    bool PointCoversKey = false;
    size_t PointPrefixLen = 0;
    bool UsedCoversKey = false;
    size_t UsedPrefixLen = 0;
    bool SortMatches = false;

    std::tuple<bool, bool, size_t, bool, size_t> AsTuple() const {
        return std::make_tuple(SortMatchesAndNoResidual, PointCoversKey, PointPrefixLen, UsedCoversKey, UsedPrefixLen);
    }

    bool operator<(const TIndexScore& other) const { return AsTuple() < other.AsTuple(); }
};

struct TReadCandidate {
    TIntrusivePtr<NYql::TKikimrTableMetadata> IndexMeta;
    TString IndexName;
    TVector<TString> KeyColumns;
    TKeyMatch Match;
    bool Covering = true;
    TIndexScore Score;

    double TableRows = 0;
    double TableBytes = 0;
    double SelRange = 1.0;
    double SelTotal = 1.0;
    double Cost = 0;
};

TIndexScore ScoreKeyOrder(const TKeyMatch& result, size_t keyLen, const TVector<TString>& sortColumns,
                          const TVector<TString>& keyColumns, bool covering);

void SetTableSize(TReadCandidate& candidate, const NYql::TKikimrTableMetadata& mainMeta);

// In the units of the join costs: a byte read costs as much as a byte of join input
struct TAccessPathCostConstants {
    static constexpr double Seek = 5000.0;
    static constexpr double Byte = 1.0;
    static constexpr double MainLookup = 10000.0;
    static constexpr double ResidualRow = 5.0;
};

double EstimateAccessPathCost(const TReadCandidate& candidate, double mainRows);

// Fills the selectivities and costs of the candidates.
bool EstimateCandidateCosts(TVector<TReadCandidate>& candidates, TOpRead& read, const std::shared_ptr<TTreeNode>& predicate,
                            double mainRows, TRBOContext& rboCtx, TPlanProps& props);

// The candidates from best to worst, by cost or by score. The main table wins ties.
TVector<size_t> OrderCandidates(const TVector<TReadCandidate>& candidates, bool costBased);

} // namespace NKikimr::NKqp
