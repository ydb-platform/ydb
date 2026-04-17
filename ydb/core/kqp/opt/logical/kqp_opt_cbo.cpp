#include "kqp_opt_cbo.h"
#include "kqp_opt_log_impl.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>


namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TMaybeNode<TKqlKeyInc> GetRightTableKeyPrefix(const TKqlKeyRange& range) {
    if (!range.From().Maybe<TKqlKeyInc>() || !range.To().Maybe<TKqlKeyInc>()) {
        return {};
    }
    auto rangeFrom = range.From().Cast<TKqlKeyInc>();
    auto rangeTo = range.To().Cast<TKqlKeyInc>();

    if (rangeFrom.ArgCount() != rangeTo.ArgCount()) {
        return {};
    }
    for (ui32 i = 0; i < rangeFrom.ArgCount(); ++i) {
        if (rangeFrom.Arg(i).Raw() != rangeTo.Arg(i).Raw()) {
            return {};
        }
    }

    return rangeFrom;
}

/**
 * KQP specific rule to check if a LookupJoin is applicable
*/
bool IsLookupJoinApplicableDetailed(const std::shared_ptr<TRelOptimizerNode>& node, const TVector<TJoinColumn>& joinColumns, const TKqpProviderContext& ctx) {

    auto rel = std::static_pointer_cast<TKqpRelOptimizerNode>(node);
    auto expr = TExprBase(rel->Node);

    if (ctx.KqpCtx.IsScanQuery() && !ctx.KqpCtx.Config->GetEnableKqpScanQueryStreamIdxLookupJoin()) {
        return false;
    }

    if (std::find_if(joinColumns.begin(), joinColumns.end(), [&] (const TJoinColumn& c) { return node->Stats.KeyColumns->Data[0] == c.AttributeName;}) != joinColumns.end()) {
        return true;
    }

    auto readMatch = MatchRead<TKqlReadTable>(expr);
    TMaybeNode<TKqlKeyInc> maybeTablePrefix;
    size_t prefixSize;

    if (readMatch) {
        if (readMatch->FlatMap && !IsPassthroughFlatMap(readMatch->FlatMap.Cast(), nullptr)){
            return false;
        }
        auto read = readMatch->Read.Cast<TKqlReadTable>();
        maybeTablePrefix = GetRightTableKeyPrefix(read.Range());

        if (!maybeTablePrefix) {
            return false;
        }

         prefixSize = maybeTablePrefix.Cast().ArgCount();

        if (!prefixSize) {
            return true;
        }
    }
    else {
        readMatch = MatchRead<TKqlReadTableRangesBase>(expr);
        if (readMatch) {
            if (readMatch->FlatMap && !IsPassthroughFlatMap(readMatch->FlatMap.Cast(), nullptr)){
                return false;
            }
            auto read = readMatch->Read.Cast<TKqlReadTableRangesBase>();
            if (TCoVoid::Match(read.Ranges().Raw())) {
                return true;
            } else {
                auto prompt = TKqpReadTableExplainPrompt::Parse(read);

                if (prompt.PointPrefixLen != prompt.UsedKeyColumns.size()) {
                    return false;
                }

                if (prompt.ExpectedMaxRanges != TMaybe<ui64>(1)) {
                    return false;
                }
                prefixSize = prompt.PointPrefixLen;
            }
        }
    }
    if (! readMatch) {
        return false;
    }

    if (prefixSize < node->Stats.KeyColumns->Data.size() && (std::find_if(joinColumns.begin(), joinColumns.end(), [&] (const TJoinColumn& c) {
            return node->Stats.KeyColumns->Data[prefixSize] == c.AttributeName;
        }) == joinColumns.end())){
            return false;
        }

    return true;
}

bool IsLookupJoinApplicable(std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    TKqpProviderContext& ctx
) {
    Y_UNUSED(left, leftJoinKeys);

    if (!(right->Stats.StorageType == EStorageType::RowStorage)) {
        return false;
    }

    auto rightStats = right->Stats;

    if (!rightStats.KeyColumns) {
        return false;
    }

    if (rightStats.Type != EStatisticsType::BaseTable) {
        return false;
    }

    for (auto rightCol : rightJoinKeys) {
        if (find(rightStats.KeyColumns->Data.begin(), rightStats.KeyColumns->Data.end(), rightCol.AttributeName) == rightStats.KeyColumns->Data.end()) {
            return false;
        }
    }

    return IsLookupJoinApplicableDetailed(std::static_pointer_cast<TRelOptimizerNode>(right), rightJoinKeys, ctx);
}

}

void TKqpProviderContext::SetConstants(const TKikimrConfiguration::TPtr& config) {
    CONSTS_MAX_DEPTH = config->OptCBOConstsMaxDepth.Get().GetOrElse(CONSTS_MAX_DEPTH);
    CONSTS_SEL_MULT = config->OptCBOConstsSelMult.Get().GetOrElse(CONSTS_SEL_MULT);
    CONSTS_SEL_POW = config->OptCBOConstsSelPow.Get().GetOrElse(CONSTS_SEL_POW);

    CONSTS_SHUFFLE_LEFT_SIDE_MULT = config->OptCBOConstsShuffleLeftSideMult.Get().GetOrElse(CONSTS_SHUFFLE_LEFT_SIDE_MULT);
    CONSTS_SHUFFLE_LEFT_SIDE_POW = config->OptCBOConstsShuffleLeftSidePow.Get().GetOrElse(CONSTS_SHUFFLE_LEFT_SIDE_POW);
    CONSTS_SHUFFLE_RIGHT_SIDE_MULT = config->OptCBOConstsShuffleRightSideMult.Get().GetOrElse(CONSTS_SHUFFLE_RIGHT_SIDE_MULT);
    CONSTS_SHUFFLE_RIGHT_SIDE_POW = config->OptCBOConstsShuffleRightSidePow.Get().GetOrElse(CONSTS_SHUFFLE_RIGHT_SIDE_POW);

    CONSTS_INTERACTION_MULT = config->OptCBOConstsInteractionsMult.Get().GetOrElse(CONSTS_INTERACTION_MULT);
    CONSTS_INTERACTION_POW = config->OptCBOConstsInteractionsPow.Get().GetOrElse(CONSTS_INTERACTION_POW);

    CONSTS_MAPJOIN_LEFT_SIDE_MULT = config->OptCBOConstsMapJoinLeftSideMult.Get().GetOrElse(CONSTS_MAPJOIN_LEFT_SIDE_MULT);
    CONSTS_MAPJOIN_LEFT_SIDE_POW = config->OptCBOConstsMapJoinLeftSidePow.Get().GetOrElse(CONSTS_MAPJOIN_LEFT_SIDE_POW);
    CONSTS_MAPJOIN_RIGHT_SIDE_MULT = config->OptCBOConstsMapJoinRightSideMult.Get().GetOrElse(CONSTS_MAPJOIN_RIGHT_SIDE_MULT);
    CONSTS_MAPJOIN_RIGHT_SIDE_POW = config->OptCBOConstsMapJoinRightSidePow.Get().GetOrElse(CONSTS_MAPJOIN_RIGHT_SIDE_POW);
    CONSTS_MAPJOIN_OUTPUT_MULT = config->OptCBOConstsMapJoinOutputMult.Get().GetOrElse(CONSTS_MAPJOIN_OUTPUT_MULT);
    CONSTS_MAPJOIN_OUTPUT_POW = config->OptCBOConstsMapJoinOutputPow.Get().GetOrElse(CONSTS_MAPJOIN_OUTPUT_POW);

    CONSTS_GRACEJOIN_LEFT_SIDE_MULT = config->OptCBOConstsGraceJoinLeftSideMult.Get().GetOrElse(CONSTS_GRACEJOIN_LEFT_SIDE_MULT);
    CONSTS_GRACEJOIN_LEFT_SIDE_POW = config->OptCBOConstsGraceJoinLeftSidePow.Get().GetOrElse(CONSTS_GRACEJOIN_LEFT_SIDE_POW);
    CONSTS_GRACEJOIN_RIGHT_SIDE_MULT = config->OptCBOConstsGraceJoinRightSideMult.Get().GetOrElse(CONSTS_GRACEJOIN_RIGHT_SIDE_MULT);
    CONSTS_GRACEJOIN_RIGHT_SIDE_POW = config->OptCBOConstsGraceJoinRightSidePow.Get().GetOrElse(CONSTS_GRACEJOIN_RIGHT_SIDE_POW);
    CONSTS_GRACEJOIN_OUTPUT_MULT = config->OptCBOConstsGraceJoinOutputMult.Get().GetOrElse(CONSTS_GRACEJOIN_OUTPUT_MULT);
    CONSTS_GRACEJOIN_OUTPUT_POW = config->OptCBOConstsGraceJoinOutputPow.Get().GetOrElse(CONSTS_GRACEJOIN_OUTPUT_POW);
}

bool TKqpProviderContext::IsJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinAlgoType joinAlgo,
    EJoinKind joinKind) {

    switch( joinAlgo ) {
        case EJoinAlgoType::LookupJoin:
            if ((OptLevel != 3) && (left->Stats.Nrows > 1000)) {
                return false;
            }
            return IsLookupJoinApplicable(left, right, leftJoinKeys, rightJoinKeys, *this);

        case EJoinAlgoType::LookupJoinReverse:
            if (joinKind != EJoinKind::LeftSemi) {
                return false;
            }
            if ((OptLevel != 3) && (right->Stats.Nrows > 1000)) {
                return false;
            }
            return IsLookupJoinApplicable(right, left, rightJoinKeys, leftJoinKeys, *this);

        case EJoinAlgoType::MapJoin:
            return joinKind != EJoinKind::OuterJoin && joinKind != EJoinKind::Exclusion && right->Stats.ByteSize < 1e6;
        case EJoinAlgoType::GraceJoin:
            return true;
        case EJoinAlgoType::ReverseBlockJoin:
            return BlockJoinEnabled && (joinKind == EJoinKind::LeftJoin | joinKind == EJoinKind::LeftOnly | joinKind == EJoinKind::LeftSemi);
        default:
            return false;
    }
}

double TKqpProviderContext::ComputeJoinCost(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const double outputRows,
    const double outputByteSize,
    EJoinAlgoType joinAlgo
) const  {
    double interactionPenalty = CONSTS_INTERACTION_MULT * std::pow(leftStats.Nrows * rightStats.Nrows, CONSTS_INTERACTION_POW);

    switch(joinAlgo) {
        case EJoinAlgoType::LookupJoin:
            if (OptLevel == 3) {
                return -1;
            }
            return leftStats.Nrows + outputRows;

        case EJoinAlgoType::LookupJoinReverse:
            if (OptLevel == 3) {
                return -1;
            }
            return rightStats.Nrows + outputRows;

        case EJoinAlgoType::MapJoin:
            return 1.5 * (CONSTS_MAPJOIN_LEFT_SIDE_MULT * std::pow(leftStats.Nrows, CONSTS_MAPJOIN_LEFT_SIDE_POW)
                + CONSTS_MAPJOIN_RIGHT_SIDE_MULT * std::pow(rightStats.Nrows, CONSTS_MAPJOIN_RIGHT_SIDE_POW)
                + CONSTS_MAPJOIN_OUTPUT_MULT * std::pow(outputRows, CONSTS_MAPJOIN_OUTPUT_POW) + interactionPenalty);
        case EJoinAlgoType::GraceJoin:
            return 1.5 * (CONSTS_GRACEJOIN_LEFT_SIDE_MULT * std::pow(leftStats.Nrows, CONSTS_GRACEJOIN_LEFT_SIDE_POW)
                + CONSTS_GRACEJOIN_RIGHT_SIDE_MULT * std::pow(rightStats.Nrows, CONSTS_GRACEJOIN_RIGHT_SIDE_POW)
                + CONSTS_GRACEJOIN_OUTPUT_MULT * std::pow(outputRows, CONSTS_GRACEJOIN_OUTPUT_POW) + interactionPenalty);
        default:
            return TBaseProviderContext::ComputeJoinCost(leftStats, rightStats, outputRows, outputByteSize, joinAlgo);
    }
}

TOptimizerStatistics TKqpProviderContext::ComputeJoinStatsV1(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinAlgoType joinAlgo,
    EJoinKind joinKind,
    TCardinalityHints::TCardinalityHint* maybeHint,
    bool shuffleLeftSide,
    bool shuffleRightSide) const {
    auto stats = ComputeJoinStats(leftStats, rightStats, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind, maybeHint);
    if (shuffleLeftSide) {
        stats.Cost += CONSTS_SHUFFLE_LEFT_SIDE_MULT * std::pow(leftStats.Nrows, CONSTS_SHUFFLE_LEFT_SIDE_POW);
    }
    if (shuffleRightSide) {
        stats.Cost += CONSTS_SHUFFLE_RIGHT_SIDE_MULT * std::pow(rightStats.Nrows, CONSTS_SHUFFLE_RIGHT_SIDE_POW);
    }

    return stats;
}

TOptimizerStatistics TKqpProviderContext::ComputeJoinStatsV2(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinAlgoType joinAlgo,
    EJoinKind joinKind,
    TCardinalityHints::TCardinalityHint* maybeHint,
    bool shuffleLeftSide,
    bool shuffleRightSide,
    TCardinalityHints::TCardinalityHint* maybeBytesHint) const {
    auto stats = ComputeJoinStatsV1(leftStats, rightStats, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind, maybeHint, shuffleLeftSide, shuffleRightSide);

    if (maybeBytesHint) {
        stats.ByteSize = maybeBytesHint->ApplyHint(stats.ByteSize);
    }

    return stats;
}

bool IsPKJoin(const TOptimizerStatistics& stats, const TVector<TJoinColumn>& joinKeys) {
    if (!stats.KeyColumns) {
        return false;
    }

    for (const auto& name : stats.KeyColumns->Data) {
        if (std::find_if(joinKeys.begin(), joinKeys.end(),
                         [&](const TJoinColumn& c) { return c.AttributeName == name; }) == joinKeys.end()) {
            return false;
        }
    }
    return true;
}

TMaybe<double> ComputeSelectivityCorrection(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys
) {
    if (leftStats.Type == EStatisticsType::BaseTable && leftStats.ColumnStatistics && rightStats.ColumnStatistics && !leftJoinKeys.empty() && !rightJoinKeys.empty()) {
        auto lhs = leftJoinKeys[0].AttributeName;
        auto rhs = rightJoinKeys[0].AttributeName;

        const auto& leftData = leftStats.ColumnStatistics->Data;
        const auto& rightData = rightStats.ColumnStatistics->Data;

        auto lhsIt = leftData.find(lhs);
        auto rhsIt = rightData.find(rhs);

        if (lhsIt == leftData.end() || rhsIt == rightData.end()) {
            return Nothing();
        }

        auto leftHist = lhsIt->second.EqWidthHistogramEstimator;
        auto rightHist = rhsIt->second.EqWidthHistogramEstimator;

        if (leftHist && rightHist) {
            // Note, leftHist is PK and rightHist is FK
            auto overlapCard = leftHist->GetOverlappingCardinality(*rightHist);
            if (!overlapCard.Defined() || overlapCard.GetRef() == 0) {
                YQL_CLOG(TRACE, CoreDq) << "Skipping selectivity correction: no overlap";
            } else {
                // correction = total PK / overlapping PK
                auto selectivityCorrection = leftStats.Nrows / static_cast<double>(overlapCard.GetRef());
                return selectivityCorrection;
            }
        }
    }
    return Nothing();
}

std::pair<TMaybe<double>, TMaybe<double>> GetJoinKeyUniqueVals(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys
) {
    TMaybe<double> lhsUniqueVals;
    TMaybe<double> rhsUniqueVals;
    if (leftStats.ColumnStatistics && rightStats.ColumnStatistics && !leftJoinKeys.empty() && !rightJoinKeys.empty()) {
        auto lhs = leftJoinKeys[0].AttributeName;
        auto rhs = rightJoinKeys[0].AttributeName;

        const auto& leftData = leftStats.ColumnStatistics->Data;
        const auto& rightData = rightStats.ColumnStatistics->Data;

        auto lhsIt = leftData.find(lhs);
        auto rhsIt = rightData.find(rhs);

        if (lhsIt != leftData.end() && lhsIt->second.NumUniqueVals) {
            lhsUniqueVals = lhsIt->second.NumUniqueVals.value();
        }

        if (rhsIt != rightData.end() && rhsIt->second.NumUniqueVals) {
            rhsUniqueVals = rhsIt->second.NumUniqueVals.value();
        }
    }

    return {lhsUniqueVals, rhsUniqueVals};
}

double ComputeBothSidesByteSize(double newCardinality,
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    ui32 commonRightJoinKeys
) {
    double lhsRowBytes = leftStats.Nrows ? (leftStats.ByteSize / leftStats.Nrows) : 0;
    double rhsRowBytes = rightStats.Nrows ? (rightStats.ByteSize / rightStats.Nrows) : 0;

    /* columns with duplicate names are removed */
    double rhsColBytes = rightStats.Ncols ? (rhsRowBytes / rightStats.Ncols) : 0;
    double duplicateWidth = commonRightJoinKeys * rhsColBytes;

    double rowWidth = std::max(0.0, lhsRowBytes + rhsRowBytes - duplicateWidth);
    return rowWidth * newCardinality;
}

double ComputeOneSideByteSize(double newCardinality,
    const TOptimizerStatistics& stats
) {
    return stats.Nrows ? (stats.ByteSize / stats.Nrows) * newCardinality : 0;
}

ui32 FindCommonJoinAttributes(
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys
) {
    ui32 commonJoinKeys = 0;
    for (const auto& leftCol : leftJoinKeys) {
        for (const auto& rightCol : rightJoinKeys) {
            if (leftCol == rightCol) {
                commonJoinKeys++;
                break;
            }
        }
    }
    return commonJoinKeys;
}

/**
 * Compute the cost and output cardinality of a join
 *
 * Currently a very basic computation targeted at GraceJoin
 *
 * The build is on the right side, so we make the build side a bit more expensive than the probe
 */

TOptimizerStatistics TKqpProviderContext::ComputeJoinStats(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinAlgoType joinAlgo,
    EJoinKind joinKind,
    TCardinalityHints::TCardinalityHint* maybeHint) const {

    EStatisticsType outputType;
    bool leftKeyColumns = false;
    bool rightKeyColumns = false;
    double newCard{};
    double newByteSize{};
    double selectivity = 1.0;

    /* only columns used within a given query */
    ui32 commonJoinKeys = FindCommonJoinAttributes(leftJoinKeys, rightJoinKeys);
    ui32 newNCols = std::max<ui32>(0, leftStats.Ncols + rightStats.Ncols - commonJoinKeys);

    bool isAntiOrSemiJoin = (joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::RightSemi || joinKind == EJoinKind::LeftOnly || joinKind == EJoinKind::RightOnly);
    bool isCrossJoin = (joinKind == EJoinKind::Cross);
    // bool isExclusionJoin = (joinKind == EJoinKind::Exclusion);

    /* it doesn't matter for these joins (semi, anti, cross) to be pk join or not. We process them separately */
    bool isRightPKJoin = !isAntiOrSemiJoin && !isCrossJoin && IsPKJoin(rightStats, rightJoinKeys);
    bool isLeftPKJoin = !isAntiOrSemiJoin && !isCrossJoin && IsPKJoin(leftStats, leftJoinKeys);

    if (isRightPKJoin && isLeftPKJoin) {
        auto rightPKJoinCard = leftStats.Nrows * leftStats.Selectivity;
        auto leftPKJoinCard = rightStats.Nrows * rightStats.Selectivity;
        if (rightPKJoinCard > leftPKJoinCard) {
            isRightPKJoin = false;
        } else {
            isLeftPKJoin = false;
        }
    }

    if (isRightPKJoin) {
        switch (joinKind) {
            case EJoinKind::LeftJoin:
                selectivity = leftStats.Selectivity;
                newCard = leftStats.Nrows * selectivity;
                break;
            default: { // when left side is FK
                TMaybe<double> correction = ComputeSelectivityCorrection(rightStats, leftStats, rightJoinKeys, leftJoinKeys);
                if (correction.Defined()) {
                    selectivity = correction.GetRef();
                } else {
                    selectivity = leftStats.Selectivity * rightStats.Selectivity;
                }
                newCard = leftStats.Nrows * selectivity;
            }
        }

        newByteSize = ComputeBothSidesByteSize(newCard, leftStats, rightStats, commonJoinKeys);

        leftKeyColumns = true;
        if (leftStats.Type == EStatisticsType::BaseTable) {
            outputType = EStatisticsType::FilteredFactTable;
        } else {
            outputType = leftStats.Type;
        }
    } else if (isLeftPKJoin) {
        switch (joinKind) {
            case EJoinKind::RightJoin:
                selectivity = rightStats.Selectivity;
                newCard = rightStats.Nrows * selectivity;
                break;
            default: { // when right side is FK
                TMaybe<double> correction = ComputeSelectivityCorrection(leftStats, rightStats, leftJoinKeys, rightJoinKeys);
                if (correction.Defined()) {
                    selectivity = correction.GetRef();
                } else {
                    selectivity = leftStats.Selectivity * rightStats.Selectivity;
                }
                newCard = rightStats.Nrows * selectivity;
            }
        }

        newByteSize = ComputeBothSidesByteSize(newCard, leftStats, rightStats, commonJoinKeys);

        rightKeyColumns = true;
        if (rightStats.Type == EStatisticsType::BaseTable) {
            outputType = EStatisticsType::FilteredFactTable;
        } else {
            outputType = rightStats.Type;
        }
    } else if (isCrossJoin) {
        selectivity = std::min(1.0, leftStats.Selectivity * rightStats.Selectivity);
        newCard = leftStats.Nrows * rightStats.Nrows * selectivity;

        newByteSize = ComputeBothSidesByteSize(newCard, leftStats, rightStats, commonJoinKeys);
        outputType = EStatisticsType::ManyManyJoin;
    } 
    else if (isAntiOrSemiJoin) {
        if (joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly) {
            selectivity = leftStats.Selectivity;
            newCard = leftStats.Nrows * selectivity;

            leftKeyColumns = true;
            newNCols = leftStats.Ncols;
            newByteSize = ComputeOneSideByteSize(newCard, leftStats);
        } else {
            selectivity = rightStats.Selectivity;
            newCard = rightStats.Nrows * selectivity;

            rightKeyColumns = true;
            newNCols = rightStats.Ncols;
            newByteSize = ComputeOneSideByteSize(newCard, rightStats);
        }

        outputType = EStatisticsType::FilteredFactTable;
    } else {
        auto [lhsUniqueVals, rhsUniqueVals] = GetJoinKeyUniqueVals(leftStats, rightStats, leftJoinKeys, rightJoinKeys);

        double effectiveLeft = leftStats.Nrows * leftStats.Selectivity;
        double effectiveRight = rightStats.Nrows * rightStats.Selectivity;

        if (lhsUniqueVals.Defined() && rhsUniqueVals.Defined()) {
            newCard = effectiveLeft * effectiveRight / std::max(lhsUniqueVals.GetRef(), rhsUniqueVals.GetRef());
        } else if (lhsUniqueVals.Defined()) {
            newCard = effectiveRight * (effectiveLeft / lhsUniqueVals.GetRef());
        } else if (rhsUniqueVals.Defined()) {
            newCard = effectiveLeft * (effectiveRight / rhsUniqueVals.GetRef());
        } else {
            /* for example, join predicate between a column and a scalar aggregate */
            newCard = 0.2 * effectiveLeft * effectiveRight;
        }

        selectivity = std::min(1.0, newCard / (leftStats.Nrows * rightStats.Nrows));

        newByteSize = ComputeBothSidesByteSize(newCard, leftStats, rightStats, commonJoinKeys);
        outputType = EStatisticsType::ManyManyJoin;
    }

    newCard = std::min(newCard, leftStats.Nrows * rightStats.Nrows);

    if (maybeHint) {
        newCard = maybeHint->ApplyHint(newCard);
    }

    double currentCost = ComputeJoinCost(leftStats, rightStats, newCard, newByteSize, joinAlgo);

    // cost model is dominated by inputs (i.e. not output size). Also, double counting costs.
    double cost = currentCost + leftStats.Cost + rightStats.Cost;

    if (isCrossJoin) {
        /* in case of cross join we broadcast the right part to the left */
        // cost += ComputeOneSideByteSize(rightStats.Nrows * rightStats.Selectivity, rightStats);
        cost += rightStats.Nrows * rightStats.Selectivity;
    }

    auto result = TOptimizerStatistics(outputType, newCard, newNCols, newByteSize, cost,
                                       leftKeyColumns ? leftStats.KeyColumns : (rightKeyColumns ? rightStats.KeyColumns : TIntrusivePtr<TOptimizerStatistics::TKeyColumns>()));

    result.JoinDepth = std::max(leftStats.JoinDepth, rightStats.JoinDepth) + 1;

    /*
        - to avoid selectivity underflow and stop over-propagation
        - prevent exponential collapse
        - avoid keeping early joins from dominating plan choice
    */
    const ui32 MAX_DEPTH = CONSTS_MAX_DEPTH;
    if (result.JoinDepth > MAX_DEPTH) {
        result.Selectivity = 1.0;
    } else {
        result.Selectivity = std::min(1.0, CONSTS_SEL_MULT * std::pow(selectivity, CONSTS_SEL_POW));
        result.Selectivity = std::max(result.Selectivity, 1e-4);
    }

    return result;
}


}
