
#include "yql_yt_transformer.h"
#include "yql_yt_transformer_helper.h"

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

TYtPhysicalOptProposalTransformer::TYtPhysicalOptProposalTransformer(TYtState::TPtr state)
    : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
    , State_(state)
{
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TYtPhysicalOptProposalTransformer::name)
    AddHandler(0, &TCoMux::Match, HNDL(Mux));
    AddHandler(0, &TYtWriteTable::Match, HNDL(Write));
    AddHandler(0, &TYtWriteTable::Match, HNDL(DqWrite));
    AddHandler(0, Names({TCoLength::CallableName(), TCoHasItems::CallableName()}), HNDL(Length));
    AddHandler(0, &TCoSort::Match, HNDL(Sort<false>));
    AddHandler(0, &TCoTopSort::Match, HNDL(Sort<true>));
    AddHandler(0, &TCoTop::Match, HNDL(Sort<true>));
    AddHandler(0, &TYtSort::Match, HNDL(YtSortOverAlreadySorted));
    AddHandler(0, &TCoPartitionByKeyBase::Match, HNDL(PartitionByKey));
    AddHandler(0, &TCoFlatMapBase::Match, HNDL(FlatMap));
    AddHandler(0, &TCoCombineByKey::Match, HNDL(CombineByKey));
    AddHandler(0, &TCoLMap::Match, HNDL(LMap<TCoLMap>));
    AddHandler(0, &TCoOrderedLMap::Match, HNDL(LMap<TCoOrderedLMap>));
    AddHandler(0, &TCoEquiJoin::Match, HNDL(EquiJoin));
    AddHandler(0, &TCoCountBase::Match, HNDL(TakeOrSkip));
    AddHandler(0, &TYtWriteTable::Match, HNDL(Fill));
    AddHandler(0, &TResPull::Match, HNDL(ResPull));
    if (State_->Configuration->UseNewPredicateExtraction.Get().GetOrElse(DEFAULT_USE_NEW_PREDICATE_EXTRACTION)) {
        AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRange));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(ExtractKeyRangeDqReadWrap));
    } else {
        AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRangeLegacy));
    }
    AddHandler(0, &TCoExtendBase::Match, HNDL(Extend));
    AddHandler(0, &TCoAssumeSorted::Match, HNDL(AssumeSorted));
    AddHandler(0, &TYtMapReduce::Match, HNDL(AddTrivialMapperForNativeYtTypes));
    AddHandler(0, &TYtDqWrite::Match, HNDL(YtDqWrite));
    AddHandler(0, &TYtDqProcessWrite::Match, HNDL(YtDqProcessWrite));
    AddHandler(0, &TYtEquiJoin::Match, HNDL(EarlyMergeJoin));

    AddHandler(1, &TYtMap::Match, HNDL(FuseInnerMap));
    AddHandler(1, &TYtMap::Match, HNDL(FuseOuterMap));
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MapFieldsSubset));
    AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(ReduceFieldsSubset));
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MultiMapFieldsSubset));
    AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(MultiReduceFieldsSubset));
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(WeakFields));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(BypassMerge));
    AddHandler(1, &TYtPublish::Match, HNDL(BypassMergeBeforePublish));
    AddHandler(1, &TYtOutputOpBase::Match, HNDL(TableContentWithSettings));
    AddHandler(1, &TYtOutputOpBase::Match, HNDL(NonOptimalTableContent));
    AddHandler(1, &TCoRight::Match, HNDL(ReadWithSettings));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(PushDownKeyExtract));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(TransientOpWithSettings));
    AddHandler(1, &TYtSort::Match, HNDL(TopSort));
    AddHandler(1, &TYtWithUserJobsOpBase::Match, HNDL(EmbedLimit));
    AddHandler(1, &TYtMerge::Match, HNDL(PushMergeLimitToInput));
    AddHandler(1, &TYtReduce::Match, HNDL(FuseReduce));

    AddHandler(2, &TYtEquiJoin::Match, HNDL(RuntimeEquiJoin));
    AddHandler(2, &TStatWriteTable::Match, HNDL(ReplaceStatWriteTable));
    AddHandler(2, &TYtMap::Match, HNDL(MapToMerge));
    AddHandler(2, &TYtPublish::Match, HNDL(UnorderedPublishTarget));
    AddHandler(2, &TYtMap::Match, HNDL(PushDownYtMapOverSortedMerge));
    AddHandler(2, &TYtMerge::Match, HNDL(MergeToCopy));
#undef HNDL
}

template<bool IsTop>
TMaybeNode<TExprBase>
TYtPhysicalOptProposalTransformer::Sort(TExprBase node, TExprContext& ctx) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    const auto sort = node.Cast<std::conditional_t<IsTop, TCoTopBase, TCoSort>>();
    if (!IsYtProviderInput(sort.Input())) {
        return node;
    }

    auto sortDirections = sort.SortDirections();
    if (!IsConstExpSortDirections(sortDirections)) {
        return node;
    }

    auto keySelectorLambda = sort.KeySelectorLambda();
    auto cluster = TString{GetClusterName(sort.Input())};
    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(keySelectorLambda.Ref(), syncList, cluster, true, false)) {
        return node;
    }

    const TStructExprType* outType = nullptr;
    if (auto type = GetSequenceItemType(node, false, ctx)) {
        outType = type->Cast<TStructExprType>();
    } else {
        return {};
    }

    TVector<TYtPathInfo::TPtr> inputInfos = GetInputPaths(sort.Input());

    TMaybe<NYT::TNode> firstNativeType;
    if (!inputInfos.empty()) {
        firstNativeType = inputInfos.front()->GetNativeYtType();
    }
    auto maybeReadSettings = sort.Input().template Maybe<TCoRight>().Input().template Maybe<TYtReadTable>().Input().Item(0).Settings();
    const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES)
         ? GetNativeYtTypeFlags(*outType)
         : 0ul;
    const bool needMap = (maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::SysColumns))
        || AnyOf(inputInfos, [nativeTypeFlags, firstNativeType] (const TYtPathInfo::TPtr& path) {
            return path->RequiresRemap()
                || nativeTypeFlags != path->GetNativeYtTypeFlags()
                || firstNativeType != path->GetNativeYtType();
        });

    bool useExplicitColumns = AnyOf(inputInfos, [] (const TYtPathInfo::TPtr& path) {
        return !path->Table->IsTemp || (path->Table->RowSpec && path->Table->RowSpec->HasAuxColumns());
    });

    const bool needMerge = maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Sample);

    const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);

    TKeySelectorBuilder builder(node.Pos(), ctx, useNativeDescSort, outType);
    builder.ProcessKeySelector(keySelectorLambda.Ptr(), sortDirections.Ptr());

    TYtOutTableInfo sortOut(outType, nativeTypeFlags);
    builder.FillRowSpecSort(*sortOut.RowSpec);
    sortOut.SetUnique(sort.Ref().template GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);

    TExprBase sortInput = sort.Input();
    TExprBase world = TExprBase(ApplySyncListToWorld(NPrivate::GetWorld(sortInput, {}, ctx).Ptr(), syncList, ctx));
    bool unordered = ctx.IsConstraintEnabled<TSortedConstraintNode>();
    if (needMap || builder.NeedMap()) {
        auto mapper = builder.MakeRemapLambda();

        auto mapperClean = CleanupWorld(TCoLambda(mapper), ctx);
        if (!mapperClean) {
            return {};
        }

        TYtOutTableInfo mapOut(builder.MakeRemapType(), nativeTypeFlags);
        mapOut.SetUnique(sort.Ref().template GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);

        sortInput = Build<TYtOutput>(ctx, node.Pos())
            .Operation<TYtMap>()
                .World(world)
                .DataSink(NPrivate::GetDataSink(sort.Input(), ctx))
                .Input(NPrivate::ConvertInputTable(sort.Input(), ctx, NPrivate::TConvertInputOpts().MakeUnordered(unordered)))
                .Output()
                    .Add(mapOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
                .Build()
                .Settings(GetFlowSettings(node.Pos(), *State_, ctx))
                .Mapper(mapperClean.Cast())
            .Build()
            .OutIndex()
                .Value(0U)
            .Build()
            .Done();
        world = TExprBase(ctx.NewWorld(node.Pos()));
        unordered = false;
    }
    else if (needMerge) {
        TYtOutTableInfo mergeOut(outType, nativeTypeFlags);
        mergeOut.SetUnique(sort.Ref().template GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);
        if (firstNativeType) {
            mergeOut.RowSpec->CopyTypeOrders(*firstNativeType);
            sortOut.RowSpec->CopyTypeOrders(*firstNativeType);
        }

        NPrivate::TConvertInputOpts opts;
        if (useExplicitColumns) {
            opts.ExplicitFields(*mergeOut.RowSpec, node.Pos(), ctx);
            useExplicitColumns = false;
        }

        sortInput = Build<TYtOutput>(ctx, node.Pos())
            .Operation<TYtMerge>()
                .World(world)
                .DataSink(NPrivate::GetDataSink(sort.Input(), ctx))
                .Input(NPrivate::ConvertInputTable(sort.Input(), ctx, opts.MakeUnordered(unordered)))
                .Output()
                    .Add(mergeOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
                .Build()
                .Settings()
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::ForceTransform), TNodeFlags::Default)
                        .Build()
                    .Build()
                .Build()
            .Build()
            .OutIndex()
                .Value(0U)
            .Build()
            .Done();
        world = TExprBase(ctx.NewWorld(node.Pos()));
        unordered = false;
    } else if (firstNativeType) {
        sortOut.RowSpec->CopyTypeOrders(*firstNativeType);
    }

    bool canUseMerge = !needMap && !needMerge;
    if (auto maxTablesForSortedMerge = State_->Configuration->MaxInputTablesForSortedMerge.Get()) {
        if (inputInfos.size() > *maxTablesForSortedMerge) {
            canUseMerge = false;
        }
    }

    if (canUseMerge) {
        TYqlRowSpecInfo commonSorted = *sortOut.RowSpec;
        for (auto& pathInfo: inputInfos) {
            commonSorted.MakeCommonSortness(*pathInfo->Table->RowSpec);
        }
        // input is sorted at least as strictly as output
        if (!sortOut.RowSpec->CompareSortness(commonSorted)) {
            canUseMerge = false;
        }
    }

    sortOut.RowSpec->SetConstraints(sort.Ref().GetConstraintSet());

    NPrivate::TConvertInputOpts opts;
    if (useExplicitColumns) {
        opts.ExplicitFields(*sortOut.RowSpec, node.Pos(), ctx);
    }

    auto res = canUseMerge ?
        TExprBase(Build<TYtMerge>(ctx, node.Pos())
            .World(world)
            .DataSink(NPrivate::GetDataSink(sortInput, ctx))
            .Input(NPrivate::ConvertInputTable(sortInput, ctx, opts.ClearUnordered()))
            .Output()
                .Add(sortOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
            .Build()
            .Settings()
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::KeepSorted), TNodeFlags::Default)
                    .Build()
                .Build()
            .Build()
        .Done()):
        TExprBase(Build<TYtSort>(ctx, node.Pos())
            .World(world)
            .DataSink(NPrivate::GetDataSink(sortInput, ctx))
            .Input(NPrivate::ConvertInputTable(sortInput, ctx, opts.MakeUnordered(unordered)))
            .Output()
                .Add(sortOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
            .Build()
            .Settings().Build()
        .Done());

    res = Build<TYtOutput>(ctx, node.Pos())
        .Operation(res)
        .OutIndex().Value(0U).Build()
        .Done();


    if constexpr (IsTop) {
        res = Build<TCoTake>(ctx, node.Pos())
            .Input(res)
            .Count(sort.Count())
            .Done();
    }

    return res;
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

}  // namespace NYql 
