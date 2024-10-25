#include "cbo_optimizer_new.h"

#include <array>

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

    THashMap<TString,EJoinKind> JoinKindMap = {
        {"Inner",EJoinKind::InnerJoin},
        {"Left",EJoinKind::LeftJoin},
        {"Right",EJoinKind::RightJoin},
        {"Full",EJoinKind::OuterJoin},
        {"LeftOnly",EJoinKind::LeftOnly},
        {"RightOnly",EJoinKind::RightOnly},
        {"Exclusion",EJoinKind::Exclusion},
        {"LeftSemi",EJoinKind::LeftSemi},
        {"RightSemi",EJoinKind::RightSemi},
        {"Cross",EJoinKind::Cross}};

    THashMap<TString,TCardinalityHints::ECardOperation> HintOpMap = {
        {"+",TCardinalityHints::ECardOperation::Add},
        {"-",TCardinalityHints::ECardOperation::Subtract},
        {"*",TCardinalityHints::ECardOperation::Multiply},
        {"/",TCardinalityHints::ECardOperation::Divide},
        {"#",TCardinalityHints::ECardOperation::Replace}};

}

EJoinKind ConvertToJoinKind(const TString& joinString) {
    auto maybeKind = JoinKindMap.find(joinString);
    Y_ENSURE(maybeKind != JoinKindMap.end());

    return maybeKind->second;
}

TString ConvertToJoinString(const EJoinKind kind) {
    for (auto [k,v] : JoinKindMap) {
        if (v == kind) {
            return k;
        }
    }

    Y_ENSURE(false,"Unknown join kind");
}

TVector<TString> TRelOptimizerNode::Labels()  {
    TVector<TString> res;
    res.emplace_back(Label);
    return res;
}

void TRelOptimizerNode::Print(std::stringstream& stream, int ntabs) {
    for (int i = 0; i < ntabs; i++){
        stream << "    ";
    }
    stream << "Rel: " << Label << "\n";

    for (int i = 0; i < ntabs; i++){
        stream << "    ";
    }
    stream << *Stats << "\n";
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
    bool nonReorderable
)   : IBaseOptimizerNode(JoinNodeType)
    , LeftArg(left)
    , RightArg(right)
    , LeftJoinKeys(leftKeys)
    , RightJoinKeys(rightKeys)
    , JoinType(joinType)
    , JoinAlgo(joinAlgo)
    , LeftAny(leftAny)
    , RightAny(rightAny)
    , IsReorderable(!nonReorderable)
{}

TVector<TString> TJoinOptimizerNode::Labels() {
    auto res = LeftArg->Labels();
    auto rightLabels = RightArg->Labels();
    res.insert(res.begin(),rightLabels.begin(),rightLabels.end());
    return res;
}

void TJoinOptimizerNode::Print(std::stringstream& stream, int ntabs) {
    for (int i = 0; i < ntabs; i++){
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

    for (size_t i=0; i<LeftJoinKeys.size(); i++){
        stream << LeftJoinKeys[i].RelName << "." << LeftJoinKeys[i].AttributeName
            << "=" << RightJoinKeys[i].RelName << "."
            << RightJoinKeys[i].AttributeName << ",";
    }
    stream << "\n";

    for (int i = 0; i < ntabs; i++){
        stream << "    ";
    }

    if (Stats) {
        stream << *Stats << "\n";
    }

    LeftArg->Print(stream, ntabs+1);
    RightArg->Print(stream, ntabs+1);
}

bool IsPKJoin(const TOptimizerStatistics& stats, const TVector<TJoinColumn>& joinKeys) {
    if (!stats.KeyColumns) {
        return false;
    }

    for(size_t i = 0; i < stats.KeyColumns->Data.size(); i++){
        if (std::find_if(joinKeys.begin(), joinKeys.end(), 
        [&] (const TJoinColumn& c) { return c.AttributeName == stats.KeyColumns->Data[i];}) == joinKeys.end()) {
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
    Y_UNUSED(outputByteSize);
    Y_UNUSED(joinAlgo);
    return leftStats.Nrows + 2.0 * rightStats.Nrows + outputRows;
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
    TCardinalityHints::TCardinalityHint* maybeHint) const
{
    double newCard{};
    EStatisticsType outputType;
    bool leftKeyColumns = false;
    bool rightKeyColumns = false;
    double selectivity = 1.0;

    bool isRightPKJoin = IsPKJoin(rightStats,rightJoinKeys);
    bool isLeftPKJoin = IsPKJoin(leftStats,leftJoinKeys);

    if (isRightPKJoin && isLeftPKJoin) {
        auto rightPKJoinCard = leftStats.Nrows * rightStats.Selectivity;
        auto leftPKJoinCard = rightStats.Nrows * leftStats.Selectivity;
        if (rightPKJoinCard > leftPKJoinCard) {
            isRightPKJoin = false;
        }
    }

    if (isRightPKJoin) {
        switch (joinKind) {
            case EJoinKind::LeftJoin:
            case EJoinKind::LeftOnly:
                newCard = leftStats.Nrows; break;
            default: {
                newCard = leftStats.Nrows * rightStats.Selectivity;
            }
        }

        selectivity = leftStats.Selectivity * rightStats.Selectivity;
        leftKeyColumns = true;
        if (leftStats.Type == EStatisticsType::BaseTable){
            outputType = EStatisticsType::FilteredFactTable;
        } else {
            outputType = leftStats.Type;
        }
    } else if (isLeftPKJoin) {
        switch (joinKind) {
            case EJoinKind::RightJoin:
            case EJoinKind::RightOnly:
                newCard = rightStats.Nrows; break;
            default: {
                newCard = leftStats.Selectivity * rightStats.Nrows;
            }
        }
        
        selectivity = leftStats.Selectivity * rightStats.Selectivity;
        rightKeyColumns = true;
        if (rightStats.Type == EStatisticsType::BaseTable){
            outputType = EStatisticsType::FilteredFactTable;
        } else {
            outputType = rightStats.Type;
        }
    } else {
        std::optional<double> lhsUniqueVals;
        std::optional<double> rhsUniqueVals;
        if (leftStats.ColumnStatistics && rightStats.ColumnStatistics && !leftJoinKeys.empty() && !rightJoinKeys.empty()) {
            auto lhs = leftJoinKeys[0].AttributeName;
            lhsUniqueVals = leftStats.ColumnStatistics->Data[lhs].NumUniqueVals;
            auto rhs = rightJoinKeys[0].AttributeName;
            rightStats.ColumnStatistics->Data[rhs];
            rhsUniqueVals = leftStats.ColumnStatistics->Data[lhs].NumUniqueVals;
        }

        if (lhsUniqueVals.has_value() && rhsUniqueVals.has_value()) {
            newCard = leftStats.Nrows * rightStats.Nrows / std::max(*lhsUniqueVals, *rhsUniqueVals);
        } else {
            newCard = 0.2 * leftStats.Nrows * rightStats.Nrows;
        }

        outputType = EStatisticsType::ManyManyJoin;
    }

    if (maybeHint) {
        newCard = maybeHint->ApplyHint(newCard);
    }

    int newNCols = leftStats.Ncols + rightStats.Ncols;
    double newByteSize = leftStats.Nrows ? (leftStats.ByteSize / leftStats.Nrows) * newCard : 0 +
            rightStats.Nrows ? (rightStats.ByteSize / rightStats.Nrows) * newCard : 0;

    double cost = ComputeJoinCost(leftStats, rightStats, newCard, newByteSize, joinAlgo)
        + leftStats.Cost + rightStats.Cost;

    auto result = TOptimizerStatistics(outputType, newCard, newNCols, newByteSize, cost,
        leftKeyColumns ? leftStats.KeyColumns : ( rightKeyColumns ? rightStats.KeyColumns : TIntrusivePtr<TOptimizerStatistics::TKeyColumns>()));
    result.Selectivity = selectivity;
    return result;
}

const TBaseProviderContext& TBaseProviderContext::Instance() {
    static TBaseProviderContext staticContext;
    return staticContext;
}

TVector<TString> TOptimizerHints::GetUnappliedString() {
    TVector<TString> res;

    for (const auto& hint: JoinAlgoHints->Hints) {
        if (!hint.Applied) {
            res.push_back(hint.StringRepr);
        }
    }

    for (const auto& hint: JoinOrderHints->Hints) {
        if (!hint.Applied) {
            res.push_back(hint.StringRepr);
        }
    }

    for (const auto& hint: CardinalityHints->Hints) {
        if (!hint.Applied) {
            res.push_back(hint.StringRepr);
        }
    }

    return res;
}

} // namespace NYql
