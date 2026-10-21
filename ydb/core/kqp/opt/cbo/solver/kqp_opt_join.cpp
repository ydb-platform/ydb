#include "kqp_opt_join.h"

#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>

#include <yql/essentials/core/yql_cost_function.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

namespace {

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

NYql::NDq::TEquiJoinCallbacks MakeCallbacks(TKqpStatsStore& kqpStats, const TOptimizerHints& kqpHints) {
    NYql::NDq::TEquiJoinCallbacks callbacks;

    callbacks.GetAlgoHint = [&kqpHints](const TVector<TString>& labels) -> NYql::EJoinAlgoType {
        const std::unordered_set<std::string> labelSet(labels.begin(), labels.end());
        for (const auto& hint : kqpHints.JoinAlgoHints->Hints) {
            if (std::unordered_set<std::string>(hint.JoinLabels.begin(), hint.JoinLabels.end()) == labelSet) {
                return JoinAlgoToYql(hint.Algo);
            }
        }
        return NYql::EJoinAlgoType::Undefined;
    };

    callbacks.OnAlgoHintApplied = [&kqpHints](const TVector<TString>& labels) {
        const std::unordered_set<std::string> labelSet(labels.begin(), labels.end());
        for (auto& hint : kqpHints.JoinAlgoHints->Hints) {
            if (std::unordered_set<std::string>(hint.JoinLabels.begin(), hint.JoinLabels.end()) == labelSet) {
                hint.Applied = true;
            }
        }
    };

    callbacks.TransferStats = [&kqpStats](const NYql::TExprNode* from, const NYql::TExprNode* to) {
        kqpStats.SetStats(to, kqpStats.GetStats(from));
    };

    return callbacks;
}

} // anonymous namespace

TMaybeNode<TExprBase> KqpRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    EHashJoinMode mode,
    bool useCBO,
    TExprContext& ctx,
    TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    const TOptimizerHints& kqpHints)
{
    int dummyJoinCounter = 0;
    return KqpRewriteEquiJoin(node, mode, useCBO, ctx, typeCtx, kqpStats, dummyJoinCounter, kqpHints);
}

TMaybeNode<TExprBase> KqpRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    EHashJoinMode mode,
    bool useCBO,
    TExprContext& ctx,
    TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    int& joinCounter,
    const TOptimizerHints& kqpHints)
{
    return NYql::NDq::DqRewriteEquiJoin(node, mode, useCBO, ctx, typeCtx, joinCounter,
        MakeCallbacks(kqpStats, kqpHints));
}

} // namespace NKikimr::NKqp
