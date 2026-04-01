#include "kqp_opt_join.h"

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

EJoinAlgoType JoinAlgoFromYql(NYql::EJoinAlgoType yqlAlgo) {
    switch (yqlAlgo) {
        case NYql::EJoinAlgoType::Undefined:        return EJoinAlgoType::Undefined;
        case NYql::EJoinAlgoType::LookupJoin:       return EJoinAlgoType::LookupJoin;
        case NYql::EJoinAlgoType::LookupJoinReverse:return EJoinAlgoType::LookupJoinReverse;
        case NYql::EJoinAlgoType::MapJoin:          return EJoinAlgoType::MapJoin;
        case NYql::EJoinAlgoType::GraceJoin:        return EJoinAlgoType::GraceJoin;
        case NYql::EJoinAlgoType::ReverseBlockJoin: return EJoinAlgoType::ReverseBlockJoin;
        case NYql::EJoinAlgoType::StreamLookupJoin: return EJoinAlgoType::StreamLookupJoin;
        case NYql::EJoinAlgoType::MergeJoin:        return EJoinAlgoType::MergeJoin;
    }
    Y_ABORT("Unknown NYql::EJoinAlgoType value: %d", static_cast<int>(yqlAlgo));
}

NYql::EJoinAlgoType JoinAlgoToYql(EJoinAlgoType kqpAlgo) {
    switch (kqpAlgo) {
        case EJoinAlgoType::Undefined:        return NYql::EJoinAlgoType::Undefined;
        case EJoinAlgoType::LookupJoin:       return NYql::EJoinAlgoType::LookupJoin;
        case EJoinAlgoType::LookupJoinReverse:return NYql::EJoinAlgoType::LookupJoinReverse;
        case EJoinAlgoType::MapJoin:          return NYql::EJoinAlgoType::MapJoin;
        case EJoinAlgoType::GraceJoin:        return NYql::EJoinAlgoType::GraceJoin;
        case EJoinAlgoType::ReverseBlockJoin: return NYql::EJoinAlgoType::ReverseBlockJoin;
        case EJoinAlgoType::StreamLookupJoin: return NYql::EJoinAlgoType::StreamLookupJoin;
        case EJoinAlgoType::MergeJoin:        return NYql::EJoinAlgoType::MergeJoin;
    }
    Y_ABORT("Unknown NKikimr::NKqp::EJoinAlgoType value: %d", static_cast<int>(kqpAlgo));
}

namespace {

std::shared_ptr<NYql::TJoinOrderHints::ITreeNode> ConvertJoinOrderNode(
    const std::shared_ptr<TJoinOrderHints::ITreeNode>& node)
{
    if (node->IsRelation()) {
        auto* rel = static_cast<TJoinOrderHints::TRelationNode*>(node.get());
        return std::make_shared<NYql::TJoinOrderHints::TRelationNode>(rel->Label);
    } else {
        auto* join = static_cast<TJoinOrderHints::TJoinNode*>(node.get());
        return std::make_shared<NYql::TJoinOrderHints::TJoinNode>(
            ConvertJoinOrderNode(join->Lhs),
            ConvertJoinOrderNode(join->Rhs)
        );
    }
}

NYql::TOptimizerHints ConvertHints(const TOptimizerHints& kqpHints) {
    NYql::TOptimizerHints result;

    for (const auto& hint : kqpHints.CardinalityHints->Hints) {
        result.CardinalityHints->PushBack(
            hint.JoinLabels,
            static_cast<NYql::TCardinalityHints::ECardOperation>(hint.Operation),
            hint.Value,
            hint.StringRepr
        );
        result.CardinalityHints->Hints.back().Applied = hint.Applied;
    }

    for (const auto& hint : kqpHints.BytesHints->Hints) {
        result.BytesHints->PushBack(
            hint.JoinLabels,
            static_cast<NYql::TCardinalityHints::ECardOperation>(hint.Operation),
            hint.Value,
            hint.StringRepr
        );
        result.BytesHints->Hints.back().Applied = hint.Applied;
    }

    for (const auto& hint : kqpHints.JoinAlgoHints->Hints) {
        result.JoinAlgoHints->PushBack(
            hint.JoinLabels,
            JoinAlgoToYql(hint.Algo),
            hint.StringRepr
        );
        result.JoinAlgoHints->Hints.back().Applied = hint.Applied;
    }

    for (const auto& hint : kqpHints.JoinOrderHints->Hints) {
        result.JoinOrderHints->PushBack(
            ConvertJoinOrderNode(hint.Tree),
            hint.StringRepr
        );
        result.JoinOrderHints->Hints.back().Applied = hint.Applied;
    }

    return result;
}

// Sync Applied flags from YQL hints back to KQP hints (through shared_ptr mutability).
void SyncAppliedFlags(const TOptimizerHints& kqpHints, const NYql::TOptimizerHints& yqlHints) {
    {
        auto& kqp = kqpHints.CardinalityHints->Hints;
        const auto& yql = yqlHints.CardinalityHints->Hints;
        Y_ASSERT(kqp.size() == yql.size());
        for (size_t i = 0; i < kqp.size(); ++i) {
            kqp[i].Applied = yql[i].Applied;
        }
    }
    {
        auto& kqp = kqpHints.BytesHints->Hints;
        const auto& yql = yqlHints.BytesHints->Hints;
        Y_ASSERT(kqp.size() == yql.size());
        for (size_t i = 0; i < kqp.size(); ++i) {
            kqp[i].Applied = yql[i].Applied;
        }
    }
    {
        auto& kqp = kqpHints.JoinAlgoHints->Hints;
        const auto& yql = yqlHints.JoinAlgoHints->Hints;
        Y_ASSERT(kqp.size() == yql.size());
        for (size_t i = 0; i < kqp.size(); ++i) {
            kqp[i].Applied = yql[i].Applied;
        }
    }
    {
        auto& kqp = kqpHints.JoinOrderHints->Hints;
        const auto& yql = yqlHints.JoinOrderHints->Hints;
        Y_ASSERT(kqp.size() == yql.size());
        for (size_t i = 0; i < kqp.size(); ++i) {
            kqp[i].Applied = yql[i].Applied;
        }
    }
}

} // namespace

NYql::NNodes::TExprBase DqRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    EHashJoinMode mode,
    bool useCBO,
    TExprContext& ctx,
    TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    const TOptimizerHints& kqpHints)
{
    int dummyJoinCounter = 0;
    return DqRewriteEquiJoin(node, mode, useCBO, ctx, typeCtx, kqpStats, dummyJoinCounter, kqpHints);
}

NYql::NNodes::TExprBase DqRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    EHashJoinMode mode,
    bool useCBO,
    TExprContext& ctx,
    TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    int& joinCounter,
    const TOptimizerHints& kqpHints)
{
    auto yqlHints = ConvertHints(kqpHints);
    auto result = NYql::NDq::DqRewriteEquiJoin(
        node, mode, useCBO, ctx, typeCtx, joinCounter, yqlHints,
        [&kqpStats](const NYql::TExprNode* from, const NYql::TExprNode* to) {
            kqpStats.SetStats(to, kqpStats.GetStats(from));
        });
    SyncAppliedFlags(kqpHints, yqlHints);
    return result;
}

} // namespace NKikimr::NKqp
