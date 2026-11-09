#include "kqp_rbo_index_selection.h"

#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_predicate_selectivity.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_cbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_statistics.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

bool IsCostBasedIndexSelectionEnabled(const NOpt::TKqpOptimizeContext& kqpCtx, NYql::EStorageType storageType) {
    const auto& config = *kqpCtx.Config;
    const auto optLevel = config.CostBasedOptimizationLevel.Get().GetOrElse(config.GetDefaultCostBasedOptimizationLevel());
    return optLevel > 2 || (optLevel == 2 && storageType != NYql::EStorageType::RowStorage);
}

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

bool IsBetterCandidate(const TIndexScore& score, bool covering, const TString& name, const TIndexScore& bestScore, bool bestCovering,
                       const TString& bestName) {
    if (bestScore < score) {
        return true;
    }
    // Index never wins a tie against the main table
    if (score < bestScore || bestName.empty()) {
        return false;
    }
    // Covering index beats non-covering one
    if (covering != bestCovering) {
        return covering;
    }
    if (score.SortMatches != bestScore.SortMatches) {
        return score.SortMatches;
    }
    // Lexicographically smallest index name wins
    return name < bestName;
}

size_t ChooseCandidateByScore(const TVector<TReadCandidate>& candidates, const TVector<bool>& rejected) {
    size_t best = 0;
    for (size_t i = 1; i < candidates.size(); ++i) {
        if (rejected[i]) {
            continue;
        }
        const auto& candidate = candidates[i];
        const auto& current = candidates[best];
        if (IsBetterCandidate(candidate.Score, candidate.Covering, candidate.IndexName, current.Score, current.Covering,
                              current.IndexName)) {
            best = i;
        }
    }
    return best;
}

double EstimateAccessPathCost(const TReadAccessPath& path, double mainRows, const TAccessPathCostConstants& k) {
    const double rowsRead = path.TableRows * path.SelRange;
    const double rowBytes = path.TableRows > 0 ? path.TableBytes / path.TableRows : 0.0;
    const double probes = path.ExpectedMaxRanges ? static_cast<double>(*path.ExpectedMaxRanges) : 1.0;

    const double mainLookups = path.Covering ? 0.0 : mainRows * path.SelTotal;
    const double residualRows = path.HasResidual ? rowsRead : 0.0;
    return k.Seek * probes + k.Byte * rowsRead * rowBytes + k.MainLookup * mainLookups + k.ResidualRow * residualRows;
}

size_t ChooseCheapestAccessPath(const TVector<TReadAccessPath>& paths, double relativeEpsilon) {
    size_t best = 0;
    for (size_t i = 1; i < paths.size(); ++i) {
        const double margin = relativeEpsilon * std::max(paths[best].Cost, 1.0);
        if (paths[i].Cost < paths[best].Cost - margin) {
            best = i;
        }
    }
    return best;
}

TVector<TReadAccessPath> BuildAccessPaths(const TVector<TReadCandidate>& candidates, const TKikimrTableMetadata& mainMeta) {
    const double mainRows = mainMeta.RecordsCount;
    const double mainBytes = mainMeta.DataSize;
    TVector<TReadAccessPath> paths;
    paths.reserve(candidates.size());
    for (const auto& candidate : candidates) {
        const auto& meta = candidate.IndexMeta ? *candidate.IndexMeta : mainMeta;
        auto& path = paths.emplace_back(TReadAccessPath{
            .Table = meta.Name,
            .IndexName = candidate.IndexName,
            .KeyColumns = meta.KeyColumnNames,
            .Covering = candidate.Covering,
            .UsedPrefixLen = candidate.Match.UsedPrefixLen,
            .PointPrefixLen = candidate.Match.PointPrefixLen,
            .ExpectedMaxRanges = candidate.Match.ExpectedMaxRanges,
            .HasResidual = candidate.Match.HasResidual,
            .SortMatches = candidate.Score.SortMatches,
            .TableRows = static_cast<double>(meta.RecordsCount),
            .TableBytes = static_cast<double>(meta.DataSize),
        });
        // An index holds a row per main table row. When its own size is unknown, scale the main table size by
        // the share of columns the index stores.
        if (candidate.IndexMeta && path.TableRows <= 0) {
            path.TableRows = mainRows;
            const double columnShare = mainMeta.Columns.empty() ? 1.0
                : static_cast<double>(meta.Columns.size()) / static_cast<double>(mainMeta.Columns.size());
            path.TableBytes = mainBytes * std::min(columnShare, 1.0);
        }
    }
    return paths;
}

bool EstimateAccessPathCosts(TVector<TReadAccessPath>& paths, const TVector<TReadCandidate>& candidates, TOpRead& read,
                             const std::shared_ptr<TTreeNode>& predicate, double mainRows, TRBOContext& rboCtx, TPlanProps& props) {
    if (mainRows <= 0 || !predicate) {
        return false;
    }

    // The rule does not require plan metadata, so the read's own is computed here: column statistics are looked up
    // through the lineage it records.
    read.ComputeMetadata(rboCtx, props);
    if (!read.Props.Metadata) {
        return false;
    }

    auto stats = std::make_shared<TOptimizerStatistics>(
        BuildOptimizerStatistics(read, props.ColumnLineage, /*withStatsAndCosts=*/false, rboCtx.TypeCtx));
    // Estimators divide sketch and histogram counts by the row count.
    stats->Nrows = mainRows;

    const NOpt::TRBOProviderContext costModel(rboCtx.KqpCtx, rboCtx.CBOCtx.OptLevel, rboCtx.CBOCtx.BlockJoinEnabled, props.ColumnLineage);
    TPredicateSelectivityComputer computer(stats);
    const double selTotal = std::min(1.0, computer.ComputeSelectivity(predicate));
    for (size_t i = 0; i < paths.size(); ++i) {
        const auto& consumed = candidates[i].Match.Consumed;
        paths[i].SelTotal = selTotal;
        paths[i].SelRange = consumed ? std::min(1.0, computer.ComputeSelectivity(consumed)) : 1.0;
        paths[i].Cost = costModel.ComputeAccessPathCost(paths[i], mainRows);
    }
    return true;
}

void LogAccessPaths(const TVector<TReadAccessPath>& paths) {
    for (const auto& path : paths) {
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Access path " << path.Table << ": used " << path.UsedPrefixLen << ", points "
                                     << path.PointPrefixLen << ", covering " << path.Covering << ", residual " << path.HasResidual
                                     << ", sel range " << path.SelRange << ", cost " << path.Cost << (path.Chosen ? ", chosen" : "");
    }
}

} // namespace NKikimr::NKqp
