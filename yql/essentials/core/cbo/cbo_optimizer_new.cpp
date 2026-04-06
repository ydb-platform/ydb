#include "cbo_optimizer_new.h"

#include <yql/essentials/utils/log/log.h>

#include <array>
#include <utility>

#include <util/string/builder.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/printf.h>

const TString& ToString(NYql::EJoinKind);
const TString& ToString(NYql::EJoinAlgoType);

namespace NYql {

using namespace NYql::NDq;

namespace {

THashMap<TString, EJoinKind> JoinKindMap = {
    {"Inner", EJoinKind::InnerJoin},
    {"Left", EJoinKind::LeftJoin},
    {"Right", EJoinKind::RightJoin},
    {"Full", EJoinKind::OuterJoin},
    {"LeftOnly", EJoinKind::LeftOnly},
    {"RightOnly", EJoinKind::RightOnly},
    {"Exclusion", EJoinKind::Exclusion},
    {"LeftSemi", EJoinKind::LeftSemi},
    {"RightSemi", EJoinKind::RightSemi},
    {"Cross", EJoinKind::Cross}};

THashMap<TString, TCardinalityHints::ECardOperation> HintOpMap = {
    {"+", TCardinalityHints::ECardOperation::Add},
    {"-", TCardinalityHints::ECardOperation::Subtract},
    {"*", TCardinalityHints::ECardOperation::Multiply},
    {"/", TCardinalityHints::ECardOperation::Divide},
    {"#", TCardinalityHints::ECardOperation::Replace}};

} // namespace

EJoinKind ConvertToJoinKind(const TString& joinString) {
    auto maybeKind = JoinKindMap.find(joinString);
    Y_ENSURE(maybeKind != JoinKindMap.end());

    return maybeKind->second;
}

TString ConvertToJoinString(const EJoinKind kind) {
    for (auto [k, v] : JoinKindMap) {
        if (v == kind) {
            return k;
        }
    }

    Y_ENSURE(false, "Unknown join kind");
}

TVector<TString> TRelOptimizerNode::Labels() {
    TVector<TString> res;
    res.emplace_back(Label);
    return res;
}

void TRelOptimizerNode::Print(std::stringstream& stream, int ntabs) {
    for (int i = 0; i < ntabs; i++) {
        stream << "    ";
    }
    stream << "Rel: " << Label << "\n";

    for (int i = 0; i < ntabs; i++) {
        stream << "    ";
    }
    stream << Stats << "\n";
}

TJoinOptimizerNode::TJoinOptimizerNode(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    TVector<TJoinColumn> leftKeys,
    TVector<TJoinColumn> rightKeys,
    const EJoinKind joinType,
    const EJoinAlgoType joinAlgo,
    bool leftAny,
    bool rightAny,
    bool nonReorderable)
    : IBaseOptimizerNode(JoinNodeType)
    , LeftArg(left)
    , RightArg(right)
    , LeftJoinKeys(std::move(leftKeys))
    , RightJoinKeys(std::move(rightKeys))
    , JoinType(joinType)
    , JoinAlgo(joinAlgo)
    , LeftAny(leftAny)
    , RightAny(rightAny)
    , IsReorderable(!nonReorderable)
{
}

TVector<TString> TJoinOptimizerNode::Labels() {
    auto res = LeftArg->Labels();
    auto rightLabels = RightArg->Labels();
    res.insert(res.begin(), rightLabels.begin(), rightLabels.end());
    return res;
}

void TJoinOptimizerNode::Print(std::stringstream& stream, int ntabs) {
    for (int i = 0; i < ntabs; i++) {
        stream << "    ";
    }

    stream << "Join: (" << ToString(JoinType) << "," << ToString(JoinAlgo);
    if (LeftAny) {
        stream << ",LeftAny";
    }
    if (RightAny) {
        stream << ",RightAny";
    }
    stream << ") ";

    for (size_t i = 0; i < LeftJoinKeys.size(); i++) {
        stream << LeftJoinKeys[i].RelName << "." << LeftJoinKeys[i].AttributeName
               << "=" << RightJoinKeys[i].RelName << "."
               << RightJoinKeys[i].AttributeName << ",";
    }
    stream << "\n";

    for (int i = 0; i < ntabs; i++) {
        stream << "    ";
    }
    stream << Stats << "\n";

    LeftArg->Print(stream, ntabs + 1);
    RightArg->Print(stream, ntabs + 1);
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

bool TBaseProviderContext::IsJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& left,
                                            const std::shared_ptr<IBaseOptimizerNode>& right,
                                            const TVector<TJoinColumn>& leftJoinKeys,
                                            const TVector<TJoinColumn>& rightJoinKeys,
                                            EJoinAlgoType joinAlgo,
                                            EJoinKind joinKind) {
    Y_UNUSED(left);
    Y_UNUSED(right);
    Y_UNUSED(leftJoinKeys);
    Y_UNUSED(rightJoinKeys);
    Y_UNUSED(joinKind);

    return joinAlgo == EJoinAlgoType::MapJoin;
}

double TBaseProviderContext::ComputeJoinCost(const TOptimizerStatistics& leftStats, const TOptimizerStatistics& rightStats, const double outputRows, const double outputByteSize, EJoinAlgoType joinAlgo) const {
    Y_UNUSED(outputRows);
    Y_UNUSED(joinAlgo);
    return leftStats.ByteSize + 2.0 * rightStats.ByteSize + outputByteSize;
}

TOptimizerStatistics TBaseProviderContext::ComputeJoinStatsV1(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<NDq::TJoinColumn>& leftJoinKeys,
    const TVector<NDq::TJoinColumn>& rightJoinKeys,
    EJoinAlgoType joinAlgo,
    EJoinKind joinKind,
    TCardinalityHints::TCardinalityHint* maybeHint,
    bool shuffleLeftSide,
    bool shuffleRightSide) const {
    auto stats = ComputeJoinStats(leftStats, rightStats, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind, maybeHint);
    if (shuffleLeftSide) {
        stats.Cost += 0.5 * leftStats.ByteSize;
    }
    if (shuffleRightSide) {
        stats.Cost += 0.5 * rightStats.ByteSize;
    }

    return stats;
}

TOptimizerStatistics TBaseProviderContext::ComputeJoinStatsV2(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<NDq::TJoinColumn>& leftJoinKeys,
    const TVector<NDq::TJoinColumn>& rightJoinKeys,
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

TOptimizerStatistics TBaseProviderContext::ComputeJoinStats(
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
                newByteSize = ComputeOneSideByteSize(newCard, leftStats);
                break;
            default: { // when left side is FK
                TMaybe<double> correction = ComputeSelectivityCorrection(rightStats, leftStats, rightJoinKeys, leftJoinKeys);
                if (correction.Defined()) {
                    selectivity = correction.GetRef();
                } else {
                    selectivity = leftStats.Selectivity * rightStats.Selectivity;
                }
                newCard = leftStats.Nrows * selectivity;
                newByteSize = ComputeBothSidesByteSize(newCard, leftStats, rightStats, commonJoinKeys);
            }
        }

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
                newByteSize = ComputeOneSideByteSize(newCard, rightStats);
                break;
            default: { // when right side is FK
                TMaybe<double> correction = ComputeSelectivityCorrection(leftStats, rightStats, leftJoinKeys, rightJoinKeys);
                if (correction.Defined()) {
                    selectivity = correction.GetRef();
                } else {
                    selectivity = leftStats.Selectivity * rightStats.Selectivity;
                }
                newCard = rightStats.Nrows * selectivity;
                newByteSize = ComputeBothSidesByteSize(newCard, leftStats, rightStats, commonJoinKeys);
            }
        }

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
        cost += ComputeOneSideByteSize(rightStats.Nrows * rightStats.Selectivity, rightStats);
    }

    auto result = TOptimizerStatistics(outputType, newCard, newNCols, newByteSize, cost,
                                       leftKeyColumns ? leftStats.KeyColumns : (rightKeyColumns ? rightStats.KeyColumns : TIntrusivePtr<TOptimizerStatistics::TKeyColumns>()));

    result.JoinDepth = std::max(leftStats.JoinDepth, rightStats.JoinDepth) + 1;

    /*
        - to avoid selectivity underflow and stop over-propagation
        - prevent exponential collapse
        - avoid keeping early joins from dominating plan choice
    */
    const ui32 MAX_DEPTH = 3;
    if (result.JoinDepth > MAX_DEPTH) {
        result.Selectivity = 1.0;
    } else {
        result.Selectivity = std::min(1.0, std::pow(selectivity, 0.8));
        result.Selectivity = std::max(result.Selectivity, 1e-4);
    }

    return result;
}

const TBaseProviderContext& TBaseProviderContext::Instance() {
    static TBaseProviderContext StaticContext;
    return StaticContext;
}

TVector<TString> TOptimizerHints::GetUnappliedString() {
    TVector<TString> res;

    for (const auto& hint : JoinAlgoHints->Hints) {
        if (!hint.Applied) {
            res.push_back(hint.StringRepr);
        }
    }

    for (const auto& hint : JoinOrderHints->Hints) {
        if (!hint.Applied) {
            res.push_back(hint.StringRepr);
        }
    }

    for (const auto& hint : CardinalityHints->Hints) {
        if (!hint.Applied) {
            res.push_back(hint.StringRepr);
        }
    }

    for (const auto& hint : BytesHints->Hints) {
        if (!hint.Applied) {
            res.push_back(hint.StringRepr);
        }
    }

    return res;
}

} // namespace NYql
