
#include <ydb/library/yql/providers/yt/provider/yql_yt_transformer.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_transformer_helper.h>

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/xrange.h>
#include <util/string/type.h>

namespace NYql {

using namespace NNodes;
using namespace NDq;
using namespace NPrivate;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::FlatMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    auto flatMap = node.Cast<TCoFlatMapBase>();

    const auto disableOptimizers = State_->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>());
    if (!disableOptimizers.contains("EquiJoinPremap") && CanBePulledIntoParentEquiJoin(flatMap, getParents)) {
        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": " << flatMap.Ref().Content() << " can be pulled into parent EquiJoin";
        return node;
    }

    auto input = flatMap.Input();
    if (!IsYtProviderInput(input, true)) {
        return node;
    }

    auto cluster = TString{GetClusterName(input)};
    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(flatMap.Lambda().Ref(), syncList, cluster, true, false)) {
        return node;
    }

    auto outItemType = SilentGetSequenceItemType(flatMap.Lambda().Body().Ref(), true);
    if (!outItemType || !outItemType->IsPersistable()) {
        return node;
    }

    auto cleanup = CleanupWorld(flatMap.Lambda(), ctx);
    if (!cleanup) {
        return {};
    }

    auto mapper = ctx.Builder(node.Pos())
        .Lambda()
            .Param("stream")
            .Callable(flatMap.Ref().Content())
                .Arg(0, "stream")
                .Lambda(1)
                    .Param("item")
                    .Apply(cleanup.Cast().Ptr())
                        .With(0, "item")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    bool sortedOutput = false;
    TVector<TYtOutTable> outTables = ConvertOutTablesWithSortAware(mapper, sortedOutput, flatMap.Pos(),
        outItemType, ctx, State_, flatMap.Ref().GetConstraintSet());

    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, flatMap.Pos());
    if (TCoOrderedFlatMap::Match(flatMap.Raw()) || sortedOutput) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Ordered))
                .Build()
            .Build();
    }
    if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Flow))
                .Build()
            .Build();
    }

    auto ytMap = Build<TYtMap>(ctx, node.Pos())
        .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
        .DataSink(GetDataSink(input, ctx))
        .Input(ConvertInputTable(input, ctx))
        .Output()
            .Add(outTables)
        .Build()
        .Settings(settingsBuilder.Done())
        .Mapper(std::move(mapper))
        .Done();

    return WrapOp(ytMap, ctx);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() != 1) {
        return node;
    }
    if (auto map = op.Maybe<TYtMap>()) {
        return LambdaFieldsSubset(op, TYtMap::idx_Mapper, ctx, getParents);
    } else if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaFieldsSubset(op, TYtMapReduce::idx_Mapper, ctx, getParents);
    }

    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() != 1) {
        return node;
    }
    if (auto reduce = op.Maybe<TYtReduce>()) {
        return LambdaFieldsSubset(op, TYtReduce::idx_Reducer, ctx, getParents);
    } else if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaFieldsSubset(op, TYtMapReduce::idx_Reducer, ctx, getParents);
    }

    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MultiMapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() < 2) {
        return node;
    }
    if (auto map = op.Maybe<TYtMap>()) {
        return LambdaVisitFieldsSubset(op, TYtMap::idx_Mapper, ctx, getParents);
    } else if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaVisitFieldsSubset(op, TYtMapReduce::idx_Mapper, ctx, getParents);
    }

    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MultiReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() < 2) {
        return node;
    }
    if (auto reduce = op.Maybe<TYtReduce>()) {
        return LambdaVisitFieldsSubset(op, TYtReduce::idx_Reducer, ctx, getParents);
    } else if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaVisitFieldsSubset(op, TYtMapReduce::idx_Reducer, ctx, getParents);
    }

    return node;
}

}  // namespace NYql
