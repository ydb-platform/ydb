#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_join.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

bool TYtPhysicalOptProposalTransformer::CanBePulledIntoParentEquiJoin(const TCoFlatMapBase& flatMap, const TGetParents& getParents) {
    const TParentsMap* parents = getParents();
    YQL_ENSURE(parents);

    auto equiJoinParents = CollectEquiJoinOnlyParents(flatMap, *parents);
    if (equiJoinParents.empty()) {
        return false;
    }

    bool suitable = true;
    for (auto it = equiJoinParents.begin(); it != equiJoinParents.end() && suitable; ++it) {
        TCoEquiJoin equiJoin(it->Node);
        auto inputIndex = it->Index;

        auto equiJoinTree = equiJoin.Arg(equiJoin.ArgCount() - 2);
        THashMap<TStringBuf, THashSet<TStringBuf>> tableKeysMap =
            CollectEquiJoinKeyColumnsByLabel(equiJoinTree.Ref());

        auto input = equiJoin.Arg(inputIndex).Cast<TCoEquiJoinInput>();

        suitable = suitable && IsLambdaSuitableForPullingIntoEquiJoin(flatMap, input.Scope().Ref(), tableKeysMap,
                                                                      it->ExtractedMembers);
    }

    return suitable;
}

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

template <typename TLMapType>
TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::LMap(TExprBase node, TExprContext& ctx) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    auto lmap = node.Cast<TLMapType>();

    if (!IsYtProviderInput(lmap.Input(), true)) {
        return node;
    }

    const auto inItemType = GetSequenceItemType(lmap.Input(), true, ctx);
    if (!inItemType) {
        return {};
    }
    const auto outItemType = SilentGetSequenceItemType(lmap.Lambda().Body().Ref(), true);
    if (!outItemType || !outItemType->IsPersistable()) {
        return node;
    }

    auto cluster = TString{GetClusterName(lmap.Input())};
    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(lmap.Lambda().Ref(), syncList, cluster, true, false)) {
        return node;
    }

    auto cleanup = CleanupWorld(lmap.Lambda(), ctx);
    if (!cleanup) {
        return {};
    }

    auto mapper = cleanup.Cast().Ptr();
    bool sortedOutput = false;
    TVector<TYtOutTable> outTables = NPrivate::ConvertOutTablesWithSortAware(mapper, sortedOutput, lmap.Pos(),
        outItemType, ctx, State_, lmap.Ref().GetConstraintSet());

    const bool useFlow = State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW);

    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, lmap.Pos());
    if (std::is_same<TLMapType, TCoOrderedLMap>::value) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Ordered))
                .Build()
            .Build();
    }

    if (useFlow) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Flow))
                .Build()
            .Build();
    }

    auto map = Build<TYtMap>(ctx, lmap.Pos())
        .World(ApplySyncListToWorld(NPrivate::GetWorld(lmap.Input(), {}, ctx).Ptr(), syncList, ctx))
        .DataSink(NPrivate::GetDataSink(lmap.Input(), ctx))
        .Input(NPrivate::ConvertInputTable(lmap.Input(), ctx))
        .Output()
            .Add(outTables)
        .Build()
        .Settings(settingsBuilder.Done())
        .Mapper(MakeJobLambda<false>(TCoLambda(mapper), useFlow, ctx))
        .Done();

    return NPrivate::WrapOp(map, ctx);
}

template TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::LMap<TCoLMap>(TExprBase node, TExprContext& ctx) const;
template TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::LMap<TCoOrderedLMap>(TExprBase node, TExprContext& ctx) const;


TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::CombineByKey(TExprBase node, TExprContext& ctx) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    auto combineByKey = node.Cast<TCoCombineByKey>();

    auto input = combineByKey.Input();
    if (!IsYtProviderInput(input)) {
        return node;
    }

    if (!GetSequenceItemType(input, false, ctx)) {
        return {};
    }

    const TStructExprType* outItemType = nullptr;
    if (auto type = SilentGetSequenceItemType(combineByKey.FinishHandlerLambda().Body().Ref(), false); type && type->IsPersistable()) {
        outItemType = type->Cast<TStructExprType>();
    } else {
        return node;
    }

    auto cluster = TString{GetClusterName(input)};
    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(combineByKey.PreMapLambda().Ref(), syncList, cluster, true, false)) {
        return node;
    }
    if (!IsYtCompleteIsolatedLambda(combineByKey.KeySelectorLambda().Ref(), syncList, cluster, true, false)) {
        return node;
    }
    if (!IsYtCompleteIsolatedLambda(combineByKey.InitHandlerLambda().Ref(), syncList, cluster, true, false)) {
        return node;
    }
    if (!IsYtCompleteIsolatedLambda(combineByKey.UpdateHandlerLambda().Ref(), syncList, cluster, true, false)) {
        return node;
    }
    if (!IsYtCompleteIsolatedLambda(combineByKey.FinishHandlerLambda().Ref(), syncList, cluster, true, false)) {
        return node;
    }

    auto preMapLambda = CleanupWorld(combineByKey.PreMapLambda(), ctx);
    auto keySelectorLambda = CleanupWorld(combineByKey.KeySelectorLambda(), ctx);
    auto initHandlerLambda = CleanupWorld(combineByKey.InitHandlerLambda(), ctx);
    auto updateHandlerLambda = CleanupWorld(combineByKey.UpdateHandlerLambda(), ctx);
    auto finishHandlerLambda = CleanupWorld(combineByKey.FinishHandlerLambda(), ctx);
    if (!preMapLambda || !keySelectorLambda || !initHandlerLambda || !updateHandlerLambda || !finishHandlerLambda) {
        return {};
    }

    auto lambdaBuilder = Build<TCoLambda>(ctx, combineByKey.Pos());
    TMaybe<TCoLambda> lambdaRet;
    if (!IsDepended(keySelectorLambda.Cast().Body().Ref(), keySelectorLambda.Cast().Args().Arg(0).Ref())) {
        lambdaBuilder
            .Args({TStringBuf("stream")})
            .Body<TCoFlatMap>()
                .Input<TCoCondense1>()
                    .Input<TCoFlatMap>()
                        .Input(TStringBuf("stream"))
                        .Lambda()
                            .Args({TStringBuf("item")})
                            .Body<TExprApplier>()
                                .Apply(preMapLambda.Cast())
                                .With(0, TStringBuf("item"))
                            .Build()
                        .Build()
                    .Build()
                    .InitHandler()
                        .Args({TStringBuf("item")})
                        .Body<TExprApplier>()
                            .Apply(initHandlerLambda.Cast())
                            .With(0, keySelectorLambda.Cast().Body())
                            .With(1, TStringBuf("item"))
                        .Build()
                    .Build()
                    .SwitchHandler()
                        .Args({TStringBuf("item"), TStringBuf("state")})
                        .Body<TCoBool>()
                            .Literal().Build("false")
                        .Build()
                    .Build()
                    .UpdateHandler()
                        .Args({TStringBuf("item"), TStringBuf("state")})
                        .Body<TExprApplier>()
                            .Apply(updateHandlerLambda.Cast())
                            .With(0, keySelectorLambda.Cast().Body())
                            .With(1, TStringBuf("item"))
                            .With(2, TStringBuf("state"))
                        .Build()
                    .Build()
                .Build()
                .Lambda()
                    .Args({TStringBuf("state")})
                    .Body<TExprApplier>()
                        .Apply(finishHandlerLambda.Cast())
                        .With(0, keySelectorLambda.Cast().Body())
                        .With(1, TStringBuf("state"))
                    .Build()
                .Build()
            .Build();

        lambdaRet = lambdaBuilder.Done();
    } else {
        lambdaBuilder
            .Args({TStringBuf("stream")})
            .Body<TCoCombineCore>()
                .Input<TCoFlatMap>()
                    .Input(TStringBuf("stream"))
                    .Lambda()
                        .Args({TStringBuf("item")})
                        .Body<TExprApplier>()
                            .Apply(preMapLambda.Cast())
                            .With(0, TStringBuf("item"))
                        .Build()
                    .Build()
                .Build()
                .KeyExtractor()
                    .Args({TStringBuf("item")})
                    .Body<TExprApplier>()
                        .Apply(keySelectorLambda.Cast())
                        .With(0, TStringBuf("item"))
                    .Build()
                .Build()
                .InitHandler()
                    .Args({TStringBuf("key"), TStringBuf("item")})
                    .Body<TExprApplier>()
                        .Apply(initHandlerLambda.Cast())
                        .With(0, TStringBuf("key"))
                        .With(1, TStringBuf("item"))
                    .Build()
                .Build()
                .UpdateHandler()
                    .Args({TStringBuf("key"), TStringBuf("item"), TStringBuf("state")})
                    .Body<TExprApplier>()
                        .Apply(updateHandlerLambda.Cast())
                        .With(0, TStringBuf("key"))
                        .With(1, TStringBuf("item"))
                        .With(2, TStringBuf("state"))
                    .Build()
                .Build()
                .FinishHandler()
                    .Args({TStringBuf("key"), TStringBuf("state")})
                    .Body<TExprApplier>()
                        .Apply(finishHandlerLambda.Cast())
                        .With(0, TStringBuf("key"))
                        .With(1, TStringBuf("state"))
                    .Build()
                .Build()
                .MemLimit()
                    .Value(ToString(State_->Configuration->CombineCoreLimit.Get().GetOrElse(0)))
                .Build()
            .Build();

        lambdaRet = lambdaBuilder.Done();
    }

    if (HasContextFuncs(*lambdaRet->Ptr())) {
        lambdaRet = Build<TCoLambda>(ctx, combineByKey.Pos())
            .Args({ TStringBuf("stream") })
            .Body<TCoWithContext>()
                .Name()
                    .Value("Agg")
                .Build()
                .Input<TExprApplier>()
                    .Apply(*lambdaRet)
                    .With(0, TStringBuf("stream"))
                .Build()
            .Build()
            .Done();
    }

    TYtOutTableInfo combineOut(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);

    return Build<TYtOutput>(ctx, combineByKey.Pos())
        .Operation<TYtMap>()
            .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
            .DataSink(GetDataSink(input, ctx))
            .Input(ConvertInputTable(input, ctx))
            .Output()
                .Add(combineOut.ToExprNode(ctx, combineByKey.Pos()).Cast<TYtOutTable>())
            .Build()
            .Settings(GetFlowSettings(combineByKey.Pos(), *State_, ctx))
            .Mapper(*lambdaRet)
        .Build()
        .OutIndex().Value(0U).Build()
        .Done();
}

}  // namespace NYql
