#pragma once

#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_predicate_normalize.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp {

// Index selection is cost-based where the CBO runs
bool IsCostBasedIndexSelectionEnabled(const NOpt::TKqpOptimizeContext& kqpCtx, NYql::EStorageType storageType);

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
};

TIndexScore ScoreKeyOrder(const TKeyMatch& result, size_t keyLen, const TVector<TString>& sortColumns,
                          const TVector<TString>& keyColumns, bool covering);

// Ties are broken deterministically, independently of the order indexes are declared.
bool IsBetterCandidate(const TIndexScore& score, bool covering, const TString& name, const TIndexScore& bestScore, bool bestCovering,
                       const TString& bestName);

// The rule-based choice: the main table is the first candidate and an index has to beat it.
size_t ChooseCandidateByScore(const TVector<TReadCandidate>& candidates, const TVector<bool>& rejected);

struct TReadAccessPath {
    TString Table;
    TString IndexName;
    TVector<TString> KeyColumns;
    bool Covering = true;
    size_t UsedPrefixLen = 0;
    size_t PointPrefixLen = 0;
    TMaybe<size_t> ExpectedMaxRanges;
    bool HasResidual = true;
    bool SortMatches = false;
    bool Chosen = false;

    double TableRows = 0;
    double TableBytes = 0;
    double SelRange = 1.0;
    double SelTotal = 1.0;
    double Cost = 0;
};

struct TAccessPathCostConstants {
    double Seek = 0;
    double Byte = 0;
    double MainLookup = 0;
    double ResidualRow = 0;
};

double EstimateAccessPathCost(const TReadAccessPath& path, double mainRows, const TAccessPathCostConstants& k);

size_t ChooseCheapestAccessPath(const TVector<TReadAccessPath>& paths, double relativeEpsilon = 1e-9);

TVector<TReadAccessPath> BuildAccessPaths(const TVector<TReadCandidate>& candidates, const NYql::TKikimrTableMetadata& mainMeta);

// Fills the selectivities and costs of the paths.
bool EstimateAccessPathCosts(TVector<TReadAccessPath>& paths, const TVector<TReadCandidate>& candidates, TOpRead& read,
                             const std::shared_ptr<TTreeNode>& predicate, double mainRows, TRBOContext& rboCtx, TPlanProps& props);

void LogAccessPaths(const TVector<TReadAccessPath>& paths);

} // namespace NKikimr::NKqp
