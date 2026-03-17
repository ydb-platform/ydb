// NKikimr::NKqp::IProviderContext overloads of the three join-stat inference functions.
// The provider-independent functions (InferStatisticsForFilter etc.) and their
// helpers live in ydb/library/yql/dq/opt/dq_opt_stat.cpp.  Only the three functions
// that call ctx.ComputeJoinStatsV2() need to be provided here, because after
// the independence refactoring NKikimr::NKqp::IProviderContext is a separate type
// from NYql::IProviderContext, so the library overloads are not callable.

#include "dq_opt_stat.h"

#include <yql/essentials/utils/log/log.h>

#include "util/string/join.h"

namespace NYql::NDq {
using namespace NKikimr::NKqp;
using namespace NNodes;

namespace {

TString RemoveAliases(TString attributeName) {
    if (auto idx = attributeName.find('.'); idx != TString::npos) {
        return attributeName.substr(idx+1);
    }
    return attributeName;
}

TString ExtractAlias(TString attributeName) {
    if (auto idx = attributeName.find('.'); idx != TString::npos) {
        auto substr = attributeName.substr(0, idx);
        if (auto idx2 = substr.find('.'); idx != TString::npos) {
            substr = substr.substr(idx2+1);
        }
        return substr;
    }
    return TString();
}

TVector<TString> InferLabels(std::shared_ptr<TOptimizerStatistics>& stats, TCoAtomList joinColumns) {
    if (stats->Labels) {
        return *stats->Labels;
    }

    if (!joinColumns.Size()) {
        return TVector<TString>();
    }

    auto fullColumnName = joinColumns.Item(0).StringValue();
    for (size_t i = 0; i < fullColumnName.size(); i++) {
        if (fullColumnName[i]=='.') {
            fullColumnName = fullColumnName.substr(0, i);
        }
        else if (i == fullColumnName.size() - 1) {
            return TVector<TString>();
        }
    }

    auto res = TVector<TString>();
    res.push_back(fullColumnName);
    return res;
}

TVector<TString> UnionLabels(TVector<TString>& leftLabels, TVector<TString>& rightLabels) {
    auto res = TVector<TString>();
    res.insert(res.begin(), leftLabels.begin(), leftLabels.end());
    res.insert(res.end(), rightLabels.begin(), rightLabels.end());
    return res;
}

} // namespace

NKikimr::NKqp::TCardinalityHints::TCardinalityHint* FindCardHint(TVector<TString>& labels, NKikimr::NKqp::TCardinalityHints& hints) {
    THashSet<TString> labelsSet;
    labelsSet.insert(labels.begin(), labels.end());

    for (auto& h : hints.Hints) {
        THashSet<TString> hintLabels;
        hintLabels.insert(h.JoinLabels.begin(), h.JoinLabels.end());
        if (labelsSet == hintLabels) {
            return &h;
        }
    }
    return nullptr;
}

NKikimr::NKqp::TCardinalityHints::TCardinalityHint* FindBytesHint(TVector<TString>& labels, NKikimr::NKqp::TCardinalityHints& hints) {
    THashSet<TString> labelsSet;
    labelsSet.insert(labels.begin(), labels.end());

    for (auto& h : hints.Hints) {
        THashSet<TString> hintLabels;
        hintLabels.insert(h.JoinLabels.begin(), h.JoinLabels.end());
        if (labelsSet == hintLabels) {
            return &h;
        }
    }
    return nullptr;
}

std::shared_ptr<TOptimizerStatistics> ApplyRowsHints(
    std::shared_ptr<TOptimizerStatistics>& inputStats,
    TVector<TString>& labels,
    NKikimr::NKqp::TCardinalityHints hints
) {
    if (labels.size() != 1) {
        return inputStats;
    }

    for (auto h : hints.Hints) {
        if (h.JoinLabels.size() == 1 && h.JoinLabels == labels) {
            auto outputStats = std::make_shared<TOptimizerStatistics>(
                inputStats->Type,
                h.ApplyHint(inputStats->Nrows),
                inputStats->Ncols,
                inputStats->ByteSize,
                inputStats->Cost,
                inputStats->KeyColumns,
                inputStats->ColumnStatistics,
                inputStats->StorageType);
            outputStats->Labels = inputStats->Labels;
            return outputStats;
        }
    }
    return inputStats;
}

std::shared_ptr<TOptimizerStatistics> ApplyBytesHints(
    std::shared_ptr<TOptimizerStatistics>& inputStats,
    TVector<TString>& labels,
    NKikimr::NKqp::TCardinalityHints hints
) {
    if (labels.size() != 1) {
        return inputStats;
    }

    for (auto h : hints.Hints) {
        if (h.JoinLabels.size() == 1 && h.JoinLabels == labels) {
            auto outputStats = std::make_shared<TOptimizerStatistics>(
                inputStats->Type,
                inputStats->Nrows,
                inputStats->Ncols,
                h.ApplyHint(inputStats->ByteSize),
                inputStats->Cost,
                inputStats->KeyColumns,
                inputStats->ColumnStatistics,
                inputStats->StorageType);
            outputStats->Labels = inputStats->Labels;
            return outputStats;
        }
    }
    return inputStats;
}

void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const NKikimr::NKqp::IProviderContext& ctx, NKikimr::NKqp::TOptimizerHints hints) {

    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoMapJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightDict();

    auto leftStats = kqpStats->GetStats(leftArg.Raw());
    auto rightStats = kqpStats->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    auto leftLabels = InferLabels(leftStats, join.LeftKeysColumnNames());
    auto rightLabels = InferLabels(rightStats, join.RightKeysColumnNames());

    leftStats = ApplyRowsHints(leftStats, leftLabels, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightLabels, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftLabels, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightLabels, *hints.BytesHints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftKeysColumnNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightKeysColumnNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);
    auto resStats = std::make_shared<TOptimizerStatistics>(
        ctx.ComputeJoinStatsV2(
            *leftStats,
            *rightStats,
            leftJoinKeys,
            rightJoinKeys,
            EJoinAlgoType::MapJoin,
            ConvertToJoinKind(join.JoinKind().StringValue()),
            FindCardHint(unionOfLabels, *hints.CardinalityHints),
            false,
            false,
            FindBytesHint(unionOfLabels, *hints.BytesHints)
        )
    );
    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());
    kqpStats->SetStats(join.Raw(), resStats);
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for MapJoin: " << resStats->ToString();
}

void InferStatisticsForGraceJoin(
    const TExprNode::TPtr& input,
    TKqpStatsStore* kqpStats,
    const NKikimr::NKqp::IProviderContext& ctx,
    NKikimr::NKqp::TOptimizerHints hints,
    NYql::TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels
) {
    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoGraceJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightInput();

    auto leftStats = kqpStats->GetStats(leftArg.Raw());
    auto rightStats = kqpStats->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    auto leftLabels = InferLabels(leftStats, join.LeftKeysColumnNames());
    auto rightLabels = InferLabels(rightStats, join.RightKeysColumnNames());

    leftStats = ApplyRowsHints(leftStats, leftLabels, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightLabels, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftLabels, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightLabels, *hints.BytesHints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftKeysColumnNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightKeysColumnNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);

    auto joinAlgo = EJoinAlgoType::GraceJoin;
    for (size_t i=0; i<join.Flags().Size(); i++) {
        if (join.Flags().Item(i).StringValue() == "Broadcast") {
            joinAlgo = EJoinAlgoType::MapJoin;
            break;
        }
    }

    auto resStats = std::make_shared<TOptimizerStatistics>(
            ctx.ComputeJoinStatsV2(
                *leftStats,
                *rightStats,
                leftJoinKeys,
                rightJoinKeys,
                joinAlgo,
                ConvertToJoinKind(join.JoinKind().StringValue()),
                FindCardHint(unionOfLabels, *hints.CardinalityHints),
                join.LeftInput().Maybe<TDqCnHashShuffle>().IsValid(),
                join.RightInput().Maybe<TDqCnHashShuffle>().IsValid(),
                FindBytesHint(unionOfLabels, *hints.BytesHints)
            )
        );

    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());

    if (shufflingOrderingsByJoinLabels) {
        auto maybeShufflingOrdering = shufflingOrderingsByJoinLabels->GetShufflingOrderigsByJoinLabels(unionOfLabels);
        if (maybeShufflingOrdering) {
            resStats->LogicalOrderings = *maybeShufflingOrdering;
        }
    }

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for GraceJoin with labels: " << "[" << JoinSeq(", ", unionOfLabels) << "]" << ", stats: " << resStats->ToString();
    kqpStats->SetStats(join.Raw(), std::move(resStats));
}

void InferStatisticsForDqJoinBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const NKikimr::NKqp::IProviderContext& ctx, NKikimr::NKqp::TOptimizerHints hints) {
    if (auto stats = kqpStats->GetStats(TExprBase(input).Raw())) {
        return;
    }

    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TDqJoinBase>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightInput();

    auto leftStats = kqpStats->GetStats(leftArg.Raw());
    auto rightStats = kqpStats->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    EJoinAlgoType joinAlgo = EJoinAlgoType::Undefined;
    if (auto dqJoin = TMaybeNode<TDqJoin>(input)) {
        joinAlgo = FromString<EJoinAlgoType>(dqJoin.Cast().JoinAlgo().StringValue());
        if (joinAlgo == EJoinAlgoType::Undefined && join.JoinType().StringValue() != "Cross" /* we don't set any join algo to cross join */) {
            return;
        }
    }

    auto leftLabels = InferLabels(leftStats, join.LeftJoinKeyNames());
    auto rightLabels = InferLabels(rightStats, join.RightJoinKeyNames());

    leftStats = ApplyRowsHints(leftStats, leftLabels, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightLabels, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftLabels, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightLabels, *hints.BytesHints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftJoinKeyNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftJoinKeyNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftJoinKeyNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightJoinKeyNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightJoinKeyNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightJoinKeyNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);

    EJoinKind joinKind = ConvertToJoinKind(join.JoinType().StringValue());

    auto resStats = std::make_shared<TOptimizerStatistics>(
            ctx.ComputeJoinStatsV2(
                *leftStats,
                *rightStats,
                leftJoinKeys,
                rightJoinKeys,
                joinAlgo,
                joinKind,
                FindCardHint(unionOfLabels, *hints.CardinalityHints),
                false,
                false,
                FindBytesHint(unionOfLabels, *hints.BytesHints)
            )
        );

    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());

    if (auto maybeMapJoin = TMaybeNode<TDqPhyMapJoin>(inputNode.Raw())) {
        resStats->SortingOrderings = leftStats->SortingOrderings;
    }

    kqpStats->SetStats(join.Raw(), resStats);
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for DqJoin: " << resStats->ToString();
}

} // namespace NYql::NDq
