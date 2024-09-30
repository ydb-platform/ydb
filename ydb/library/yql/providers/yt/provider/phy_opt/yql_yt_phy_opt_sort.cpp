#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/xrange.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::YtSortOverAlreadySorted(TExprBase node, TExprContext& ctx) const {
    auto sort = node.Cast<TYtSort>();

    if (auto maxTablesForSortedMerge = State_->Configuration->MaxInputTablesForSortedMerge.Get()) {
        if (sort.Input().Item(0).Paths().Size() > *maxTablesForSortedMerge) {
            return node;
        }
    }

    const TYqlRowSpecInfo outRowSpec(sort.Output().Item(0).RowSpec());

    TYqlRowSpecInfo commonSorted = outRowSpec;
    auto section = sort.Input().Item(0);
    for (auto path: section.Paths()) {
        commonSorted.MakeCommonSortness(ctx, *TYtTableBaseInfo::GetRowSpec(path.Table()));
    }

    if (outRowSpec.CompareSortness(commonSorted)) {
        // input is sorted at least as strictly as output
        auto res = ctx.RenameNode(sort.Ref(), TYtMerge::CallableName());
        res = ctx.ChangeChild(*res, TYtMerge::idx_Settings,
            Build<TCoNameValueTupleList>(ctx, sort.Pos())
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::KeepSorted))
                    .Build()
                .Build()
            .Done().Ptr()
        );
        section = ClearUnorderedSection(section, ctx);
        if (section.Ptr() != sort.Input().Item(0).Ptr()) {
            res = ctx.ChangeChild(*res, TYtMerge::idx_Input, Build<TYtSectionList>(ctx, sort.Input().Pos()).Add(section).Done().Ptr());
        }
        return TExprBase(res);
    }

    return node;
}

template<bool IsTop>
TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Sort(TExprBase node, TExprContext& ctx) const {
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
    if (!IsYtCompleteIsolatedLambda(keySelectorLambda.Ref(), syncList, cluster, false)) {
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
            commonSorted.MakeCommonSortness(ctx, *pathInfo->Table->RowSpec);
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

template TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Sort<true>(TExprBase node, TExprContext& ctx) const;
template TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Sort<false>(TExprBase node, TExprContext& ctx) const;


TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::TopSort(TExprBase node, TExprContext& ctx) const {
    auto sort = node.Cast<TYtSort>();

    auto settings = sort.Settings();
    auto limitSetting = NYql::GetSetting(settings.Ref(), EYtSettingType::Limit);
    if (!limitSetting) {
        return node;
    }
    if (HasNodesToCalculate(node.Ptr())) {
        return node;
    }

    if (NYql::HasAnySetting(sort.Input().Item(0).Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip)) {
        return node;
    }

    if (NYql::HasNonEmptyKeyFilter(sort.Input().Item(0))) {
        return node;
    }

    const ui64 maxLimit = State_->Configuration->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT);
    TMaybe<ui64> limit = GetLimit(settings.Ref());
    if (!limit || *limit == 0 || *limit > maxLimit) {
        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << " !!! TopSort - zero limit";
        // Keep empty "limit" setting to prevent repeated Limits optimization
        if (limitSetting->ChildPtr(1)->ChildrenSize() != 0) {
            auto updatedSettings = NYql::RemoveSetting(settings.Ref(), EYtSettingType::Limit, ctx);
            updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::Limit, ctx.NewList(node.Pos(), {}), ctx);
            return TExprBase(ctx.ChangeChild(node.Ref(), TYtSort::idx_Settings, std::move(updatedSettings)));
        }
        return node;
    }

    ui64 size = 0;
    ui64 rows = 0;
    for (auto path: sort.Input().Item(0).Paths()) {
        auto tableInfo = TYtTableBaseInfo::Parse(path.Table());
        if (!tableInfo->Stat) {
            return node;
        }
        ui64 tableSize = tableInfo->Stat->DataSize;
        ui64 tableRows = tableInfo->Stat->RecordsCount;

        if (!path.Ranges().Maybe<TCoVoid>()) {
            if (TMaybe<ui64> usedRows = TYtRangesInfo(path.Ranges()).GetUsedRows(tableRows)) {
                // Make it proportional to used rows
                tableSize = tableSize * usedRows.GetRef() / tableRows;
                tableRows = usedRows.GetRef();
            } else {
                // non-row ranges are present
                return node;
            }
        }
        size += tableSize;
        rows += tableRows;
    }

    if (rows <= *limit) {
        // Just do YtSort
        // Keep empty "limit" setting to prevent repeated Limits optimization
        auto updatedSettings = NYql::RemoveSetting(settings.Ref(), EYtSettingType::Limit, ctx);
        updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::Limit, ctx.NewList(node.Pos(), {}), ctx);
        return TExprBase(ctx.ChangeChild(node.Ref(), TYtSort::idx_Settings, std::move(updatedSettings)));
    }

    const ui64 sizePerJob = State_->Configuration->TopSortSizePerJob.Get().GetOrElse(128_MB);
    const ui64 rowMultiplierPerJob = State_->Configuration->TopSortRowMultiplierPerJob.Get().GetOrElse(10u);
    ui64 partsBySize = size / sizePerJob;
    ui64 partsByRecords = rows / (rowMultiplierPerJob * limit.GetRef());
    ui64 jobCount = Max<ui64>(Min<ui64>(partsBySize, partsByRecords), 1);
    if (partsBySize <= 1 || partsByRecords <= 10) {
        jobCount = 1;
    }

    auto sortedBy = TYqlRowSpecInfo(sort.Output().Item(0).RowSpec()).GetForeignSort();
    auto updatedSettings = NYql::AddSettingAsColumnPairList(sort.Settings().Ref(), EYtSettingType::SortLimitBy, sortedBy, ctx);
    if (jobCount <= 5000) {
        updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::JobCount, ctx.NewAtom(sort.Pos(), ToString(jobCount)), ctx);
    }

    auto inputItemType = GetSequenceItemType(sort.Input().Item(0), false, ctx);
    if (!inputItemType) {
        return {};
    }

    if (jobCount == 1) {
        updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::KeepSorted, {}, ctx);

        auto mapper = Build<TCoLambda>(ctx, sort.Pos())
            .Args({"stream"})
            .Body<TCoTopSort>()
                .Input("stream")
                .Count<TCoUint64>()
                    .Literal()
                        .Value(ToString(*limit))
                    .Build()
                .Build()
                .SortDirections([&sortedBy] (TExprNodeBuilder& builder) {
                    auto listBuilder = builder.List();
                    for (size_t i: xrange(sortedBy.size())) {
                        listBuilder.Callable(i, TCoBool::CallableName())
                            .Atom(0, sortedBy[i].second ? "True" : "False")
                            .Seal();
                    }
                    listBuilder.Seal();
                })
                .KeySelectorLambda()
                    .Args({"item"})
                    .Body([&sortedBy] (TExprNodeBuilder& builder) {
                        auto listBuilder = builder.List();
                        for (size_t i: xrange(sortedBy.size())) {
                            listBuilder.Callable(i, TCoMember::CallableName())
                                .Arg(0, "item")
                                .Atom(1, sortedBy[i].first)
                                .Seal();
                        }
                        listBuilder.Seal();
                    })
                .Build()
            .Build().Done();

        // Final map
        return Build<TYtMap>(ctx, sort.Pos())
            .World(sort.World())
            .DataSink(sort.DataSink())
            .Input(sort.Input())
            .Output(sort.Output())
            .Settings(GetFlowSettings(sort.Pos(), *State_, ctx, updatedSettings))
            .Mapper(mapper)
            .Done();
    }

    auto mapper = Build<TCoLambda>(ctx, sort.Pos())
        .Args({"stream"})
        .Body<TCoTop>()
            .Input("stream")
            .Count<TCoUint64>()
                .Literal()
                    .Value(ToString(*limit))
                .Build()
            .Build()
            .SortDirections([&sortedBy] (TExprNodeBuilder& builder) {
                auto listBuilder = builder.List();
                for (size_t i: xrange(sortedBy.size())) {
                    listBuilder.Callable(i, TCoBool::CallableName())
                        .Atom(0, sortedBy[i].second ? "True" : "False")
                        .Seal();
                }
                listBuilder.Seal();
            })
            .KeySelectorLambda()
                .Args({"item"})
                .Body([&sortedBy] (TExprNodeBuilder& builder) {
                    auto listBuilder = builder.List();
                    for (size_t i: xrange(sortedBy.size())) {
                        listBuilder.Callable(i, TCoMember::CallableName())
                            .Arg(0, "item")
                            .Atom(1, sortedBy[i].first)
                            .Seal();
                    }
                    listBuilder.Seal();
                })
            .Build()
        .Build().Done();

    TYtOutTableInfo outTable(inputItemType->Cast<TStructExprType>(), State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    outTable.RowSpec->SetConstraints(sort.Ref().GetConstraintSet());

    return Build<TYtSort>(ctx, sort.Pos())
        .InitFrom(sort)
        .World<TCoWorld>().Build()
        .Input()
            .Add()
                .Paths()
                    .Add()
                        .Table<TYtOutput>()
                            .Operation<TYtMap>()
                                .World(sort.World())
                                .DataSink(sort.DataSink())
                                .Input(sort.Input())
                                .Output()
                                    .Add(outTable.SetUnique(sort.Ref().GetConstraint<TDistinctConstraintNode>(), sort.Pos(), ctx).ToExprNode(ctx, sort.Pos()).Cast<TYtOutTable>())
                                .Build()
                                .Settings(GetFlowSettings(sort.Pos(), *State_, ctx, updatedSettings))
                                .Mapper(mapper)
                            .Build()
                            .OutIndex()
                                .Value(0U)
                            .Build()
                        .Build()
                        .Columns<TCoVoid>().Build()
                        .Ranges<TCoVoid>().Build()
                        .Stat<TCoVoid>().Build()
                    .Build()
                .Build()
                .Settings()
                .Build()
            .Build()
        .Build()
        .Done();
}


TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::AssumeSorted(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    auto assume = node.Cast<TCoAssumeSorted>();
    auto input = assume.Input();
    if (!IsYtProviderInput(input)) {
        return node;
    }

    auto sorted = node.Ref().GetConstraint<TSortedConstraintNode>();
    if (!sorted) {
        // Drop AssumeSorted with unsupported sort modes
        return input;
    }

    auto maybeOp = input.Maybe<TYtOutput>().Operation();
    bool needSeparateOp = !maybeOp
        || maybeOp.Raw()->StartsExecution()
        || (maybeOp.Raw()->HasResult() && maybeOp.Raw()->GetResult().Type() == TExprNode::World)
        || IsOutputUsedMultipleTimes(maybeOp.Ref(), *getParents())
        || maybeOp.Maybe<TYtMapReduce>()
        || maybeOp.Maybe<TYtEquiJoin>();

    bool canMerge = false;
    bool equalSort = false;
    if (auto inputSort = input.Ref().GetConstraint<TSortedConstraintNode>()) {
        if (sorted->IsPrefixOf(*inputSort)) {
            canMerge = true;
            equalSort = sorted->Equals(*inputSort);
        }
    }
    if (equalSort && maybeOp.Maybe<TYtSort>()) {
        return input;
    }

    const TStructExprType* outItemType = nullptr;
    if (auto type = GetSequenceItemType(node, false, ctx)) {
        outItemType = type->Cast<TStructExprType>();
    } else {
        return {};
    }

    const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);

    TKeySelectorBuilder builder(assume.Pos(), ctx, useNativeDescSort, outItemType);
    builder.ProcessConstraint(*sorted);
    needSeparateOp = needSeparateOp || (builder.NeedMap() && !equalSort);

    if (needSeparateOp) {
        TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
        outTable.RowSpec->SetConstraints(assume.Ref().GetConstraintSet());

        if (auto maybeReadSettings = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Settings()) {
            if (NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::SysColumns)) {
                canMerge = false;
            }
        }
        auto inputPaths = GetInputPaths(input);
        TMaybe<NYT::TNode> firstNativeType;
        if (!inputPaths.empty()) {
            firstNativeType = inputPaths.front()->GetNativeYtType();
        }

        canMerge = canMerge && AllOf(inputPaths, [&outTable, firstNativeType] (const TYtPathInfo::TPtr& path) {
            return !path->RequiresRemap()
                && path->GetNativeYtTypeFlags() == outTable.RowSpec->GetNativeYtTypeFlags()
                && firstNativeType == path->GetNativeYtType();
        });
        if (canMerge) {
            outTable.RowSpec->CopySortness(ctx, *inputPaths.front()->Table->RowSpec, TYqlRowSpecInfo::ECopySort::WithDesc);
            outTable.RowSpec->ClearSortness(ctx, sorted->GetContent().size());
            outTable.SetUnique(assume.Ref().GetConstraint<TDistinctConstraintNode>(), assume.Pos(), ctx);
            if (firstNativeType) {
                outTable.RowSpec->CopyTypeOrders(*firstNativeType);
            }

            YQL_ENSURE(sorted->GetContent().size() == outTable.RowSpec->SortMembers.size());
            const bool useExplicitColumns = AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
                return !path->Table->IsTemp || (path->Table->RowSpec && path->Table->RowSpec->HasAuxColumns());
            });

            TConvertInputOpts opts;
            if (useExplicitColumns) {
                opts.ExplicitFields(*outTable.RowSpec, assume.Pos(), ctx);
            }

            return Build<TYtOutput>(ctx, assume.Pos())
                .Operation<TYtMerge>()
                    .World(GetWorld(input, {}, ctx))
                    .DataSink(GetDataSink(input, ctx))
                    .Input(ConvertInputTable(input, ctx, opts))
                    .Output()
                        .Add(outTable.ToExprNode(ctx, assume.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Settings()
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::KeepSorted))
                            .Build()
                        .Build()
                    .Build()
                .Build()
                .OutIndex().Value(0U).Build()
                .Done();
        }
        else {
            builder.FillRowSpecSort(*outTable.RowSpec);
            outTable.SetUnique(assume.Ref().GetConstraint<TDistinctConstraintNode>(), assume.Pos(), ctx);

            TCoLambda mapper = builder.NeedMap()
                ? Build<TCoLambda>(ctx, assume.Pos())
                    .Args({"stream"})
                    .Body<TExprApplier>()
                        .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                        .With(0, "stream")
                    .Build()
                .Done()
                : Build<TCoLambda>(ctx, assume.Pos())
                    .Args({"stream"})
                    .Body("stream")
                .Done();

            auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, assume.Pos());
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::KeepSorted))
                    .Build()
                .Build()
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Ordered))
                    .Build()
                .Build();
            if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
                settingsBuilder
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::Flow))
                        .Build()
                    .Build();
            }

            return Build<TYtOutput>(ctx, assume.Pos())
                .Operation<TYtMap>()
                    .World(GetWorld(input, {}, ctx))
                    .DataSink(GetDataSink(input, ctx))
                    .Input(ConvertInputTable(input, ctx))
                    .Output()
                        .Add(outTable.ToExprNode(ctx, assume.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Settings(settingsBuilder.Done())
                    .Mapper(mapper)
                .Build()
                .OutIndex().Value(0U).Build()
                .Done();
        }
    }

    auto op = GetOutputOp(input.Cast<TYtOutput>());
    TExprNode::TPtr newOp = op.Ptr();
    if (!op.Maybe<TYtSort>()) {
        if (auto settings = op.Maybe<TYtTransientOpBase>().Settings()) {
            if (!NYql::HasSetting(settings.Ref(), EYtSettingType::KeepSorted)) {
                newOp = ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Settings, NYql::AddSetting(settings.Ref(), EYtSettingType::KeepSorted, {}, ctx));
            }
        } else if (auto settings = op.Maybe<TYtFill>().Settings()) {
            if (!NYql::HasSetting(settings.Ref(), EYtSettingType::KeepSorted)) {
                newOp = ctx.ChangeChild(op.Ref(), TYtFill::idx_Settings, NYql::AddSetting(settings.Ref(), EYtSettingType::KeepSorted, {}, ctx));
            }
        }
    }
    if (!equalSort) {
        const size_t index = FromString(input.Cast<TYtOutput>().OutIndex().Value());
        TYtOutTableInfo outTable(op.Output().Item(index));
        builder.FillRowSpecSort(*outTable.RowSpec);
        outTable.RowSpec->SetConstraints(assume.Ref().GetConstraintSet());
        outTable.SetUnique(assume.Ref().GetConstraint<TDistinctConstraintNode>(), assume.Pos(), ctx);

        TVector<TYtOutTable> outputs;
        for (size_t i = 0; i < op.Output().Size(); ++i) {
            if (index == i) {
                outputs.push_back(outTable.ToExprNode(ctx, op.Pos()).Cast<TYtOutTable>());
            } else {
                outputs.push_back(op.Output().Item(i));
            }
        }

        newOp = ctx.ChangeChild(*newOp, TYtOutputOpBase::idx_Output, Build<TYtOutSection>(ctx, op.Pos()).Add(outputs).Done().Ptr());
    }

    return Build<TYtOutput>(ctx, assume.Pos())
        .Operation(newOp)
        .OutIndex(input.Cast<TYtOutput>().OutIndex())
        .Done();
}

}  // namespace NYql
