#include "kqp_rbo_index_selection.h"

#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_predicate_selectivity.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_statistics.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <yql/essentials/utils/log/log.h>

#include <numeric>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

// Lexicographic on (score, main table, covering, sort match, name), so a strict weak order as std::stable_sort needs
bool IsBetterCandidate(const TReadCandidate& candidate, const TReadCandidate& other) {
    if (other.Score < candidate.Score) {
        return true;
    }
    if (candidate.Score < other.Score) {
        return false;
    }
    // Index never wins a tie against the main table
    if (candidate.IndexName.empty() != other.IndexName.empty()) {
        return candidate.IndexName.empty();
    }
    // Covering index beats non-covering one
    if (candidate.Covering != other.Covering) {
        return candidate.Covering;
    }
    if (candidate.Score.SortMatches != other.Score.SortMatches) {
        return candidate.Score.SortMatches;
    }
    // Lexicographically smallest index name wins
    return candidate.IndexName < other.IndexName;
}

} // anonymous namespace

TIndexScore ScoreKeyOrder(const TKeyMatch& result, size_t keyLen, const TVector<TString>& sortColumns,
                          const TVector<TString>& keyColumns, bool covering) {
    TIndexScore score;
    score.PointCoversKey = keyLen != 0 && result.PointPrefixLen == keyLen;
    score.PointPrefixLen = score.PointCoversKey ? 0 : result.PointPrefixLen;
    score.UsedCoversKey = keyLen != 0 && result.UsedPrefixLen == keyLen;
    score.UsedPrefixLen = score.UsedCoversKey ? 0 : result.UsedPrefixLen;

    if (covering && !sortColumns.empty()) {
        const size_t pointPrefixLen =
            (result.ExpectedMaxRanges && *result.ExpectedMaxRanges == 1) ? std::min(result.PointPrefixLen, keyColumns.size()) : 0;
        score.SortMatches = SortMatchesKeyOrder(sortColumns, keyColumns, pointPrefixLen);
        score.SortMatchesAndNoResidual = score.SortMatches && !result.HasResidual;
    }

    return score;
}

void SetTableSize(TReadCandidate& candidate, const TKikimrTableMetadata& mainMeta) {
    const auto& meta = candidate.IndexMeta ? *candidate.IndexMeta : mainMeta;
    candidate.TableRows = meta.RecordsCount;
    candidate.TableBytes = meta.DataSize;

    // An index of unknown size holds a row per main table row and its share of the columns
    if (candidate.IndexMeta && candidate.TableRows <= 0) {
        candidate.TableRows = mainMeta.RecordsCount;
        const double columnShare = mainMeta.Columns.empty() ? 1.0
            : static_cast<double>(meta.Columns.size()) / static_cast<double>(mainMeta.Columns.size());
        candidate.TableBytes = mainMeta.DataSize * std::min(columnShare, 1.0);
    }
}

double EstimateAccessPathCost(const TReadCandidate& candidate, double mainRows) {
    using K = TAccessPathCostConstants;
    const double rowsRead = candidate.TableRows * candidate.SelRange;
    const double rowBytes = candidate.TableRows > 0 ? candidate.TableBytes / candidate.TableRows : 0.0;
    const double probes = candidate.Match.ExpectedMaxRanges ? static_cast<double>(*candidate.Match.ExpectedMaxRanges) : 1.0;

    const double mainLookups = candidate.Covering ? 0.0 : mainRows * candidate.SelTotal;
    const double residualRows = candidate.Match.HasResidual ? rowsRead : 0.0;
    return K::Seek * probes + K::Byte * rowsRead * rowBytes + K::MainLookup * mainLookups + K::ResidualRow * residualRows;
}

bool EstimateCandidateCosts(TVector<TReadCandidate>& candidates, TOpRead& read, const std::shared_ptr<TTreeNode>& predicate,
                            double mainRows, TRBOContext& rboCtx, TPlanProps& props) {
    if (mainRows <= 0 || !predicate) {
        return false;
    }

    // The rule does not require plan metadata, so the read's own is computed here.
    read.ComputeMetadata(rboCtx, props);
    if (!read.Props.Metadata) {
        return false;
    }

    auto stats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(read, props.ColumnLineage, false, rboCtx.TypeCtx));

    stats->Nrows = mainRows;

    TPredicateSelectivityComputer computer(stats);
    const double selTotal = computer.ComputeSelectivity(predicate);

    for (auto& candidate : candidates) {
        const auto& consumed = candidate.Match.Consumed;
        candidate.SelTotal = selTotal;
        candidate.SelRange = consumed ? computer.ComputeSelectivity(consumed) : 1.0;
        candidate.Cost = EstimateAccessPathCost(candidate, mainRows);
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Access path " << (candidate.IndexName.empty() ? "main" : candidate.IndexName)
                                     << ": used " << candidate.Match.UsedPrefixLen << ", points " << candidate.Match.PointPrefixLen
                                     << ", covering " << candidate.Covering << ", residual " << candidate.Match.HasResidual
                                     << ", sel range " << candidate.SelRange << ", cost " << candidate.Cost;
    }

    return true;
}

TVector<size_t> OrderCandidates(const TVector<TReadCandidate>& candidates, bool costBased) {
    TVector<size_t> order(candidates.size());
    std::iota(order.begin(), order.end(), 0);

    // Stable, so the main table, the first candidate, stays ahead on equal costs
    std::stable_sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
        return costBased ? candidates[lhs].Cost < candidates[rhs].Cost : IsBetterCandidate(candidates[lhs], candidates[rhs]);
    });

    return order;
}

} // namespace NKikimr::NKqp
