
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

using namespace NPrivate;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::EmbedLimit(TExprBase node, TExprContext& ctx) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Output().Size() != 1) {
        return node;
    }
    auto settings = op.Settings();
    auto limitSetting = NYql::GetSetting(settings.Ref(), EYtSettingType::Limit);
    if (!limitSetting) {
        return node;
    }
    if (HasNodesToCalculate(node.Ptr())) {
        return node;
    }

    TMaybe<ui64> limit = GetLimit(settings.Ref());
    if (!limit) {
        return node;
    }

    auto sortLimitBy = NYql::GetSettingAsColumnPairList(settings.Ref(), EYtSettingType::SortLimitBy);
    if (!sortLimitBy.empty() && *limit > State_->Configuration->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT)) {
        return node;
    }

    size_t lambdaIdx = op.Maybe<TYtMapReduce>()
        ? TYtMapReduce::idx_Reducer
        : op.Maybe<TYtReduce>()
            ? TYtReduce::idx_Reducer
            : TYtMap::idx_Mapper;

    auto lambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));
    if (IsEmptyContainer(lambda.Body().Ref()) || IsEmpty(lambda.Body().Ref(), *State_->Types)) {
        return node;
    }

    if (sortLimitBy.empty()) {
        if (lambda.Body().Maybe<TCoTake>()) {
            return node;
        }

        lambda = Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TCoTake>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With(0, "stream")
                .Build()
                .Count<TCoUint64>()
                    .Literal()
                        .Value(ToString(*limit))
                    .Build()
                .Build()
            .Build()
            .Done();
    } else {
        if (lambda.Body().Maybe<TCoTopBase>()) {
            return node;
        }

        if (const auto& body = lambda.Body().Ref(); body.IsCallable("ExpandMap") && body.Head().IsCallable({"Top", "TopSort"})) {
            return node;
        }

        lambda = Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TCoTop>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With(0, "stream")
                .Build()
                .Count<TCoUint64>()
                    .Literal()
                        .Value(ToString(*limit))
                    .Build()
                .Build()
                .SortDirections([&sortLimitBy] (TExprNodeBuilder& builder) {
                    auto listBuilder = builder.List();
                    for (size_t i: xrange(sortLimitBy.size())) {
                        listBuilder.Callable(i, TCoBool::CallableName())
                            .Atom(0, sortLimitBy[i].second ? "True" : "False")
                            .Seal();
                    }
                    listBuilder.Seal();
                })
                .KeySelectorLambda()
                    .Args({"item"})
                    .Body([&sortLimitBy] (TExprNodeBuilder& builder) {
                        auto listBuilder = builder.List();
                        for (size_t i: xrange(sortLimitBy.size())) {
                            listBuilder.Callable(i, TCoMember::CallableName())
                                .Arg(0, "item")
                                .Atom(1, sortLimitBy[i].first)
                                .Seal();
                        }
                        listBuilder.Seal();
                    })
                .Build()
            .Build().Done();

        if (auto& l = lambda.Ref(); l.Tail().Head().IsCallable("ExpandMap")) {
            lambda = TCoLambda(ctx.ChangeChild(l, 1U, ctx.SwapWithHead(l.Tail())));
        }
     }

    return TExprBase(ctx.ChangeChild(op.Ref(), lambdaIdx, lambda.Ptr()));
}

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
        commonSorted.MakeCommonSortness(*TYtTableBaseInfo::GetRowSpec(path.Table()));
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
            outTable.RowSpec->CopySortness(*inputPaths.front()->Table->RowSpec, TYqlRowSpecInfo::ECopySort::WithDesc);
            outTable.RowSpec->ClearSortness(sorted->GetContent().size());
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
