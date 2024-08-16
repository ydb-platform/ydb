#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_optimize.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

#include <util/generic/xrange.h>
namespace NYql {

using namespace NNodes;
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

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Mux(TExprBase node, TExprContext& ctx) const {
    auto mux = node.Cast<TCoMux>();
    const TTypeAnnotationNode* muxItemTypeNode = GetSeqItemType(mux.Ref().GetTypeAnn());
    if (!muxItemTypeNode) {
        return node;
    }
    auto muxItemType = muxItemTypeNode->Cast<TVariantExprType>();
    if (muxItemType->GetUnderlyingType()->GetKind() != ETypeAnnotationKind::Tuple) {
        return node;
    }

    const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
    bool allAreTables = true;
    bool hasTables = false;
    bool allAreTableContents = true;
    bool hasContents = false;
    TString resultCluster;
    TMaybeNode<TYtDSource> dataSource;
    for (auto child: mux.Input().Cast<TExprList>()) {
        bool isTable = IsYtProviderInput(child);
        bool isContent = child.Maybe<TYtTableContent>().IsValid();
        if (!isTable && !isContent) {
            // Don't match foreign provider input
            if (child.Maybe<TCoRight>()) {
                return node;
            }
        } else {
            if (!dataSource) {
                dataSource = GetDataSource(child, ctx);
            }

            if (!resultCluster) {
                resultCluster = TString{dataSource.Cast().Cluster().Value()};
            }
            else if (resultCluster != dataSource.Cast().Cluster().Value()) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                    << "Different source clusters in Mux: " << resultCluster
                    << " and " << dataSource.Cast().Cluster().Value()));
                return {};
            }
        }
        allAreTables = allAreTables && isTable;
        hasTables = hasTables || isTable;
        allAreTableContents = allAreTableContents && isContent;
        hasContents = hasContents || isContent;
    }

    if (!hasTables && !hasContents) {
        return node;
    }

    auto dataSink = TYtDSink(ctx.RenameNode(dataSource.Ref(), "DataSink"));
    if (allAreTables || allAreTableContents) {
        TVector<TExprBase> worlds;
        TVector<TYtSection> sections;
        for (auto child: mux.Input().Cast<TExprList>()) {
            auto read = child.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
            if (!read) {
                read = child.Maybe<TYtTableContent>().Input().Maybe<TYtReadTable>();
            }
            if (read) {
                YQL_ENSURE(read.Cast().Input().Size() == 1);
                auto section = read.Cast().Input().Item(0);
                sections.push_back(section);
                if (allAreTables) {
                    worlds.push_back(GetWorld(child, {}, ctx));
                }
            } else {
                YQL_ENSURE(child.Maybe<TYtOutput>(), "Unknown Mux element: " << child.Ref().Content());
                sections.push_back(
                    Build<TYtSection>(ctx, child.Pos())
                        .Paths()
                            .Add()
                                .Table(child) // child is TYtOutput
                                .Columns<TCoVoid>().Build()
                                .Ranges<TCoVoid>().Build()
                                .Stat<TCoVoid>().Build()
                            .Build()
                        .Build()
                        .Settings()
                        .Build()
                        .Done()
                    );
            }
        }

        auto world = worlds.empty()
            ? TExprBase(ctx.NewWorld(mux.Pos()))
            : worlds.size() == 1
                ? worlds.front()
                : Build<TCoSync>(ctx, mux.Pos()).Add(worlds).Done();

        auto resRead = Build<TYtReadTable>(ctx, mux.Pos())
            .World(world)
            .DataSource(dataSource.Cast())
            .Input()
                .Add(sections)
            .Build()
            .Done();

        return allAreTables
            ? Build<TCoRight>(ctx, mux.Pos())
                .Input(resRead)
                .Done().Cast<TExprBase>()
            : Build<TYtTableContent>(ctx, mux.Pos())
                .Input(resRead)
                .Settings().Build()
                .Done().Cast<TExprBase>();
    }

    if (!hasTables) {
        return node;
    }

    TVector<TExprBase> newMuxParts;
    for (auto child: mux.Input().Cast<TExprList>()) {
        if (!IsYtProviderInput(child)) {
            if (State_->Types->EvaluationInProgress) {
                return node;
            }
            TSyncMap syncList;
            if (!IsYtCompleteIsolatedLambda(child.Ref(), syncList, resultCluster, false)) {
                return node;
            }

            const TStructExprType* outItemType = nullptr;
            if (auto type = GetSequenceItemType(child, false, ctx)) {
                if (!EnsurePersistableType(child.Pos(), *type, ctx)) {
                    return {};
                }
                outItemType = type->Cast<TStructExprType>();
            } else {
                return {};
            }

            TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
            auto content = child;
            if (auto sorted = child.Ref().GetConstraint<TSortedConstraintNode>()) {
                TKeySelectorBuilder builder(child.Pos(), ctx, useNativeDescSort, outItemType);
                builder.ProcessConstraint(*sorted);
                builder.FillRowSpecSort(*outTable.RowSpec);

                if (builder.NeedMap()) {
                    content = Build<TExprApplier>(ctx, child.Pos())
                        .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                        .With(0, content)
                        .Done();
                }

            } else if (auto unordered = content.Maybe<TCoUnorderedBase>()) {
                content = unordered.Cast().Input();
            }
            outTable.RowSpec->SetConstraints(child.Ref().GetConstraintSet());
            outTable.SetUnique(child.Ref().GetConstraint<TDistinctConstraintNode>(), child.Pos(), ctx);

            auto cleanup = CleanupWorld(content, ctx);
            if (!cleanup) {
                return {};
            }

            newMuxParts.push_back(
                Build<TYtOutput>(ctx, child.Pos())
                    .Operation<TYtFill>()
                        .World(ApplySyncListToWorld(ctx.NewWorld(child.Pos()), syncList, ctx))
                        .DataSink(dataSink)
                        .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                        .Output()
                            .Add(outTable.ToExprNode(ctx, child.Pos()).Cast<TYtOutTable>())
                        .Build()
                        .Settings(GetFlowSettings(child.Pos(), *State_, ctx))
                    .Build()
                    .OutIndex().Value(0U).Build()
                .Done()
            );
        }
        else {
            newMuxParts.push_back(child);
        }
    }

    return Build<TCoMux>(ctx, mux.Pos())
        .Input<TExprList>()
            .Add(newMuxParts)
        .Build()
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::TakeOrSkip(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto countBase = node.Cast<TCoCountBase>();
    auto input = countBase.Input();
    if (!IsYtProviderInput(input)) {
        return node;
    }

    auto cluster = TString{GetClusterName(input)};
    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(countBase.Count().Ref(), syncList, cluster, false)) {
        return node;
    }

    auto count = State_->PassiveExecution ? countBase.Count() : CleanupWorld(countBase.Count(), ctx);
    if (!count) {
        return {};
    }

    EYtSettingType settingType = node.Maybe<TCoSkip>() ? EYtSettingType::Skip : EYtSettingType::Take;

    auto settings = Build<TCoNameValueTupleList>(ctx, countBase.Pos())
        .Add()
            .Name()
                .Value(ToString(settingType))
            .Build()
            .Value(count.Cast())
        .Build()
        .Done();

    if (!ctx.IsConstraintEnabled<TSortedConstraintNode>()) {
        if (auto maybeMap = input.Maybe<TYtOutput>().Operation().Maybe<TYtMap>()) {
            TYtMap map = maybeMap.Cast();
            if (!IsOutputUsedMultipleTimes(map.Ref(), *getParents())) {
                TYtOutTableInfo mapOut(map.Output().Item(0));
                if (mapOut.RowSpec->IsSorted()) {
                    mapOut.RowSpec->ClearSortness();
                    input = Build<TYtOutput>(ctx, input.Pos())
                        .InitFrom(input.Cast<TYtOutput>())
                        .Operation<TYtMap>()
                            .InitFrom(map)
                            .Output()
                                .Add(mapOut.ToExprNode(ctx, map.Output().Item(0).Pos()).Cast<TYtOutTable>())
                            .Build()
                        .Build()
                        .Done();
                }
            }
        }
    }

    auto res = Build<TCoRight>(ctx, countBase.Pos())
        .Input<TYtReadTable>()
            .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
            .DataSource(GetDataSource(input, ctx))
            .Input(ConvertInputTable(input, ctx, TConvertInputOpts().KeepDirecRead(true).Settings(settings)))
        .Build()
        .Done();
    return KeepColumnOrder(res.Ptr(), node.Ref(), ctx, *State_->Types);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Extend(TExprBase node, TExprContext& ctx) const {
    if (State_->PassiveExecution) {
        return node;
    }

    auto extend = node.Cast<TCoExtendBase>();

    bool allAreTables = true;
    bool hasTables = false;
    bool allAreTableContents = true;
    bool hasContents = false;
    bool keepSort = !ctx.IsConstraintEnabled<TSortedConstraintNode>() || (bool)extend.Ref().GetConstraint<TSortedConstraintNode>();
    TString resultCluster;
    TMaybeNode<TYtDSource> dataSource;

    for (auto child: extend) {
        bool isTable = IsYtProviderInput(child);
        bool isContent = child.Maybe<TYtTableContent>().IsValid();
        if (!isTable && !isContent) {
            // Don't match foreign provider input
            if (child.Maybe<TCoRight>()) {
                return node;
            }
        } else {
            auto currentDataSource = GetDataSource(child, ctx);
            auto currentCluster = TString{currentDataSource.Cluster().Value()};
            if (!dataSource) {
                dataSource = currentDataSource;
                resultCluster = currentCluster;
            } else if (resultCluster != currentCluster) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                    << "Different source clusters in " << extend.Ref().Content() << ": " << resultCluster
                    << " and " << currentCluster));
                return {};
            }
        }
        allAreTables = allAreTables && isTable;
        hasTables = hasTables || isTable;
        allAreTableContents = allAreTableContents && isContent;
        hasContents = hasContents || isContent;
    }

    if (!hasTables && !hasContents) {
        return node;
    }

    auto dataSink = TYtDSink(ctx.RenameNode(dataSource.Ref(), "DataSink"));
    if (allAreTables || allAreTableContents) {
        TVector<TExprBase> worlds;
        TVector<TYtPath> paths;
        TExprNode::TListType newExtendParts;
        bool updateChildren = false;
        bool unordered = false;
        bool nonUniq = false;
        for (auto child: extend) {
            newExtendParts.push_back(child.Ptr());

            auto read = child.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
            if (!read) {
                read = child.Maybe<TYtTableContent>().Input().Maybe<TYtReadTable>();
            }
            if (read) {
                YQL_ENSURE(read.Cast().Input().Size() == 1);
                auto section = read.Cast().Input().Item(0);
                unordered = unordered || NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered);
                nonUniq = nonUniq || NYql::HasSetting(section.Settings().Ref(), EYtSettingType::NonUnique);
                TExprNode::TPtr settings = NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx);
                if (settings->ChildrenSize() != 0) {
                    if (State_->Types->EvaluationInProgress || allAreTableContents) {
                        return node;
                    }
                    auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                    auto path = CopyOrTrivialMap(section.Pos(),
                        read.Cast().World(), dataSink,
                        *scheme,
                        TYtSection(ctx.ChangeChild(section.Ref(), TYtSection::idx_Settings, std::move(settings))),
                        {}, ctx, State_,
                        TCopyOrTrivialMapOpts().SetTryKeepSortness(keepSort).SetRangesResetSort(!keepSort).SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>()).SetConstraints(extend.Ref().GetConstraintSet()));
                    updateChildren = true;
                    newExtendParts.back() = allAreTableContents
                        ? ctx.ChangeChild(child.Ref(), TYtTableContent::idx_Input, path.Table().Ptr())
                        : path.Table().Ptr();
                } else {
                    paths.insert(paths.end(), section.Paths().begin(), section.Paths().end());
                    if (allAreTables) {
                        worlds.push_back(GetWorld(child, {}, ctx));
                    }
                }
            } else {
                YQL_ENSURE(child.Maybe<TYtOutput>(), "Unknown extend element: " << child.Ref().Content());
                paths.push_back(
                    Build<TYtPath>(ctx, child.Pos())
                        .Table(child) // child is TYtOutput
                        .Columns<TCoVoid>().Build()
                        .Ranges<TCoVoid>().Build()
                        .Stat<TCoVoid>().Build()
                        .Done()
                    );
            }
        }

        if (updateChildren) {
            return TExprBase(ctx.ChangeChildren(extend.Ref(), std::move(newExtendParts)));
        }

        newExtendParts.clear();

        auto world = worlds.empty()
            ? TExprBase(ctx.NewWorld(extend.Pos()))
            : worlds.size() == 1
                ? worlds.front()
                : Build<TCoSync>(ctx, extend.Pos()).Add(worlds).Done();

        auto scheme = extend.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

        if (keepSort && extend.Maybe<TCoMerge>() && paths.size() > 1) {
            if (State_->Types->EvaluationInProgress) {
                return node;
            }
            auto path = CopyOrTrivialMap(extend.Pos(),
                world, dataSink,
                *scheme,
                Build<TYtSection>(ctx, extend.Pos())
                    .Paths()
                        .Add(paths)
                    .Build()
                    .Settings()
                    .Build()
                    .Done(),
                {}, ctx, State_,
                TCopyOrTrivialMapOpts().SetTryKeepSortness(keepSort).SetRangesResetSort(!keepSort).SetSectionUniq(extend.Ref().GetConstraint<TDistinctConstraintNode>()).SetConstraints(extend.Ref().GetConstraintSet()));
            world = TExprBase(ctx.NewWorld(extend.Pos()));
            paths.assign(1, path);
        }

        if (paths.size() == 1 && paths.front().Columns().Maybe<TCoVoid>() && paths.front().Ranges().Maybe<TCoVoid>()) {
            return allAreTables
                ? paths.front().Table()
                : Build<TYtTableContent>(ctx, extend.Pos())
                    .Input(paths.front().Table())
                    .Settings().Build()
                    .Done().Cast<TExprBase>();
        }

        auto newSettings = ctx.NewList(extend.Pos(), {});
        if (nonUniq) {
            newSettings = NYql::AddSetting(*newSettings, EYtSettingType::NonUnique, {}, ctx);
        }
        auto newSection = Build<TYtSection>(ctx, extend.Pos())
            .Paths()
                .Add(paths)
            .Build()
            .Settings(newSettings)
            .Done();
        if (unordered) {
            newSection = MakeUnorderedSection<true>(newSection, ctx);
        }

        auto resRead = Build<TYtReadTable>(ctx, extend.Pos())
            .World(world)
            .DataSource(dataSource.Cast())
            .Input()
                .Add(newSection)
            .Build()
            .Done();

        return allAreTables
            ? Build<TCoRight>(ctx, extend.Pos())
                .Input(resRead)
                .Done().Cast<TExprBase>()
            : Build<TYtTableContent>(ctx, extend.Pos())
                .Input(resRead)
                .Settings().Build()
                .Done().Cast<TExprBase>();
    }

    if (!hasTables) {
        return node;
    }

    const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
    TExprNode::TListType newExtendParts;
    for (auto child: extend) {
        if (!IsYtProviderInput(child)) {
            if (State_->Types->EvaluationInProgress) {
                return node;
            }
            TSyncMap syncList;
            if (!IsYtCompleteIsolatedLambda(child.Ref(), syncList, resultCluster, false)) {
                return node;
            }

            const TStructExprType* outItemType = nullptr;
            if (auto type = GetSequenceItemType(child, false, ctx)) {
                if (!EnsurePersistableType(child.Pos(), *type, ctx)) {
                    return {};
                }
                outItemType = type->Cast<TStructExprType>();
            } else {
                return {};
            }

            TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
            auto content = child;
            auto sorted = child.Ref().GetConstraint<TSortedConstraintNode>();
            if (keepSort && sorted) {
                TKeySelectorBuilder builder(child.Pos(), ctx, useNativeDescSort, outItemType);
                builder.ProcessConstraint(*sorted);
                builder.FillRowSpecSort(*outTable.RowSpec);

                if (builder.NeedMap()) {
                    content = Build<TExprApplier>(ctx, child.Pos())
                        .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                        .With(0, content)
                        .Done();
                    outItemType = builder.MakeRemapType();
                }

            } else if (auto unordered = content.Maybe<TCoUnorderedBase>()) {
                content = unordered.Cast().Input();
            }
            outTable.RowSpec->SetConstraints(child.Ref().GetConstraintSet());
            outTable.SetUnique(child.Ref().GetConstraint<TDistinctConstraintNode>(), child.Pos(), ctx);

            auto cleanup = CleanupWorld(content, ctx);
            if (!cleanup) {
                return {};
            }

            newExtendParts.push_back(
                Build<TYtOutput>(ctx, child.Pos())
                    .Operation<TYtFill>()
                        .World(ApplySyncListToWorld(ctx.NewWorld(child.Pos()), syncList, ctx))
                        .DataSink(dataSink)
                        .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                        .Output()
                            .Add(outTable.ToExprNode(ctx, child.Pos()).Cast<TYtOutTable>())
                        .Build()
                        .Settings(GetFlowSettings(child.Pos(), *State_, ctx))
                    .Build()
                    .OutIndex().Value(0U).Build()
                .Done().Ptr()
            );
        }
        else {
            newExtendParts.push_back(child.Ptr());
        }
    }

    return TExprBase(ctx.ChangeChildren(extend.Ref(), std::move(newExtendParts)));
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Length(TExprBase node, TExprContext& ctx) const {
    TExprBase list = node.Maybe<TCoLength>()
        ? node.Cast<TCoLength>().List()
        : node.Cast<TCoHasItems>().List();

    TExprBase ytLengthInput = list;
    if (auto content = list.Maybe<TYtTableContent>()) {
        ytLengthInput = content.Cast().Input();
    } else if (!IsYtProviderInput(list)) {
        return node;
    }

    if (auto right = ytLengthInput.Maybe<TCoRight>()) {
        ytLengthInput = right.Cast().Input();
    }
    // Now ytLengthInput is either YtReadTable or YtOutput

    TVector<TCoNameValueTuple> takeSkip;
    if (auto maybeRead = ytLengthInput.Maybe<TYtReadTable>()) {
        auto read = maybeRead.Cast();
        YQL_ENSURE(read.Input().Size() == 1);
        TYtSection section = read.Input().Item(0);
        bool needMaterialize = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample)
            || AnyOf(section.Paths(), [](const TYtPath& path) { return !path.Ranges().Maybe<TCoVoid>() || TYtTableBaseInfo::GetMeta(path.Table())->IsDynamic; });
        for (auto s: section.Settings()) {
            switch (FromString<EYtSettingType>(s.Name().Value())) {
            case EYtSettingType::Take:
            case EYtSettingType::Skip:
                takeSkip.push_back(s);
                break;
            default:
                // Skip other settings
                break;
            }
        }

        if (needMaterialize) {
            if (State_->Types->EvaluationInProgress) {
                return node;
            }

            auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            ytLengthInput = CopyOrTrivialMap(section.Pos(),
                TExprBase(ctx.NewWorld(section.Pos())),
                TYtDSink(ctx.RenameNode(read.DataSource().Ref(), "DataSink")),
                *scheme,
                Build<TYtSection>(ctx, section.Pos())
                    .Paths(section.Paths())
                    .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip
                        | EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx))
                    .Done(),
                {}, ctx, State_, TCopyOrTrivialMapOpts()).Table();
        }
        else {
            auto settings = section.Settings().Ptr();
            if (!takeSkip.empty()) {
                settings = NYql::RemoveSettings(*settings, EYtSettingType::Take | EYtSettingType::Skip
                    | EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx);
            }

            if (read.World().Ref().Type() == TExprNode::World && settings->ChildrenSize() == 0 && section.Paths().Size() == 1 && section.Paths().Item(0).Table().Maybe<TYtOutput>()) {
                // Simplify
                ytLengthInput = Build<TYtOutput>(ctx, section.Paths().Item(0).Table().Pos())
                    .InitFrom(section.Paths().Item(0).Table().Cast<TYtOutput>())
                    .Mode()
                        .Value(ToString(EYtSettingType::Unordered))
                    .Build()
                    .Done();

            } else {
                ytLengthInput = Build<TYtReadTable>(ctx, read.Pos())
                    .InitFrom(read)
                    .Input()
                        .Add()
                            .InitFrom(MakeUnorderedSection(section, ctx))
                            .Settings(settings)
                        .Build()
                    .Build()
                    .Done();
            }
        }
    }
    else {
        ytLengthInput = Build<TYtOutput>(ctx, ytLengthInput.Pos())
            .InitFrom(ytLengthInput.Cast<TYtOutput>())
            .Mode()
                .Value(ToString(EYtSettingType::Unordered))
            .Build()
            .Done();
    }

    TExprBase res = Build<TYtLength>(ctx, node.Pos())
        .Input(ytLengthInput)
        .Done();

    for (TCoNameValueTuple s: takeSkip) {
        switch (FromString<EYtSettingType>(s.Name().Value())) {
        case EYtSettingType::Take:
            res = Build<TCoMin>(ctx, node.Pos())
                .Add(res)
                .Add(s.Value().Cast())
                .Done();
            break;
        case EYtSettingType::Skip:
            res = Build<TCoSub>(ctx, node.Pos())
                .Left<TCoMax>()
                    .Add(res)
                    .Add(s.Value().Cast())
                .Build()
                .Right(s.Value().Cast())
                .Done();
            break;
        default:
            break;
        }
    }

    if (node.Maybe<TCoHasItems>()) {
        res = Build<TCoAggrNotEqual>(ctx, node.Pos())
            .Left(res)
            .Right<TCoUint64>()
                .Literal()
                    .Value(0U)
                .Build()
            .Build()
            .Done();
    }

    return res;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ResPull(TExprBase node, TExprContext& ctx) const {
    auto resPull = node.Cast<TResPull>();

    auto maybeRead = resPull.Data().Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
    if (!maybeRead) {
        // Nothing to optimize in case of ResPull! over YtOutput!
        return node;
    }

    auto read = maybeRead.Cast();
    if (read.Input().Size() != 1) {
        return node;
    }
    auto section = read.Input().Item(0);
    auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    bool directRead = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::DirectRead);
    const bool hasSettings = NYql::HasAnySetting(section.Settings().Ref(),
        EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample | EYtSettingType::SysColumns);

    const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES)
         ? GetNativeYtTypeFlags(*scheme->Cast<TStructExprType>())
         : 0ul;

    bool requiresMapOrMerge = false;
    bool hasRanges = false;
    bool hasNonTemp = false;
    bool hasDynamic = false;
    bool first = true;
    TMaybe<NYT::TNode> firstNativeType;
    for (auto path: section.Paths()) {
        TYtPathInfo pathInfo(path);
        if (first) {
            first = false;
            firstNativeType = pathInfo.GetNativeYtType();
        }
        requiresMapOrMerge = requiresMapOrMerge || pathInfo.Table->RequiresRemap()
            || !IsSameAnnotation(*scheme, *pathInfo.Table->RowSpec->GetType())
            || nativeTypeFlags != pathInfo.GetNativeYtTypeFlags()
            || firstNativeType != pathInfo.GetNativeYtType();
        hasRanges = hasRanges || pathInfo.Ranges;
        hasNonTemp = hasNonTemp || !pathInfo.Table->IsTemp;
        hasDynamic = hasDynamic || pathInfo.Table->Meta->IsDynamic;
    }

    if (!requiresMapOrMerge && !hasRanges && !hasSettings)
        return node;

    // Ignore DirectRead pragma for temporary tables and dynamic tables with sampling or ranges
    if (!hasNonTemp || (hasDynamic && (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample) || hasRanges))) {
        directRead = false;
    }

    if (directRead) {
        return node;
    }

    bool keepSorted = ctx.IsConstraintEnabled<TSortedConstraintNode>()
        ? (!NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered) && !hasNonTemp && section.Paths().Size() == 1) // single sorted input from operation
        : (!hasDynamic || !NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip)); // compatibility - all except dynamic with limit
    auto path = CopyOrTrivialMap(read.Pos(),
        read.World(),
        TYtDSink(ctx.RenameNode(read.DataSource().Ref(), "DataSink")),
        *scheme,
        Build<TYtSection>(ctx, section.Pos())
            .Paths(section.Paths())
            .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx))
            .Done(),
        {}, ctx, State_,
        TCopyOrTrivialMapOpts().SetTryKeepSortness(keepSorted).SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>()).SetConstraints(read.Ref().GetConstraintSet()));

    auto newData = path.Columns().Maybe<TCoVoid>() && path.Ranges().Maybe<TCoVoid>()
        ? path.Table()
        : Build<TCoRight>(ctx, resPull.Pos())
            .Input<TYtReadTable>()
                .World(ctx.NewWorld(resPull.Pos()))
                .DataSource(read.DataSource())
                .Input()
                    .Add()
                        .Paths()
                            .Add(path)
                        .Build()
                        .Settings()
                        .Build()
                    .Build()
                .Build()
            .Build()
            .Done();

    return ctx.ChangeChild(resPull.Ref(), TResPull::idx_Data, newData.Ptr());
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::TransientOpWithSettings(TExprBase node, TExprContext& ctx) const {
    auto op = node.Cast<TYtTransientOpBase>();

    if (node.Ref().HasResult() && node.Ref().GetResult().Type() == TExprNode::World) {
        return node;
    }

    TYqlRowSpecInfo::TPtr outRowSpec;
    bool keepSortness = false;
    if (op.Maybe<TYtReduce>()) {
        keepSortness = true;
    } else if (op.Maybe<TYtCopy>() || op.Maybe<TYtMerge>()) {
        outRowSpec = MakeIntrusive<TYqlRowSpecInfo>(op.Output().Item(0).RowSpec());
        keepSortness = outRowSpec->IsSorted();
    } else if (op.Maybe<TYtMap>()) {
        keepSortness = AnyOf(op.Output(), [] (const TYtOutTable& out) {
            return TYqlRowSpecInfo(out.RowSpec()).IsSorted();
        });
    }

    bool hasUpdates = false;
    TVector<TExprBase> updatedSections;
    TSyncMap syncList;
    for (auto section: op.Input()) {
        updatedSections.push_back(section);

        if (auto updatedSection = UpdateSectionWithSettings(op.World(), section, op.DataSink(), outRowSpec, keepSortness, true, true, syncList, State_, ctx)) {
            updatedSections.back() = updatedSection.Cast();
            hasUpdates = true;
        }
    }
    if (!hasUpdates) {
        return node;
    }

    auto res = ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Input,
        Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(updatedSections)
            .Done().Ptr());
    if (!syncList.empty()) {
        res = ctx.ChangeChild(*res, TYtTransientOpBase::idx_World,
            ApplySyncListToWorld(res->ChildPtr(TYtTransientOpBase::idx_World), syncList, ctx));
    }
    // Transform YtCopy to YtMerge in case of ranges
    if (op.Maybe<TYtCopy>()) {
        if (AnyOf(updatedSections.front().Cast<TYtSection>().Paths(), [](TYtPath path) { return !path.Ranges().Maybe<TCoVoid>(); })) {
            res = ctx.RenameNode(*res, TYtMerge::CallableName());
        }
    }
    return TExprBase(res);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::AddTrivialMapperForNativeYtTypes(TExprBase node, TExprContext& ctx) const {
    if (State_->Configuration->UseIntermediateSchema.Get().GetOrElse(DEFAULT_USE_INTERMEDIATE_SCHEMA)) {
        return node;
    }

    auto op = node.Cast<TYtMapReduce>();
    if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
        return node;
    }

    bool needMapper = AnyOf(op.Input(), [](const TYtSection& section) {
        return AnyOf(section.Paths(), [](const TYtPath& path) {
            auto rowSpec = TYtTableBaseInfo::GetRowSpec(path.Table());
            return rowSpec && 0 != rowSpec->GetNativeYtTypeFlags();
        });
    });

    if (!needMapper) {
        return node;
    }

    auto mapper = Build<TCoLambda>(ctx, node.Pos()).Args({"stream"}).Body("stream").Done();

    return ctx.ChangeChild(node.Ref(), TYtMapReduce::idx_Mapper, mapper.Ptr());
}

}  // namespace NYql
