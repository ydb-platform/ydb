#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_optimize.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::DqWrite(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
    if (State_->PassiveExecution) {
        return node;
    }

    auto write = node.Cast<TYtWriteTable>();
    if (!TDqCnUnionAll::Match(write.Content().Raw())) {
        return node;
    }

    const TStructExprType* outItemType;
    if (auto type = GetSequenceItemType(write.Content(), false, ctx)) {
        outItemType = type->Cast<TStructExprType>();
    } else {
        return node;
    }

    if (!NDq::IsSingleConsumerConnection(write.Content().Cast<TDqCnUnionAll>(), *getParents())) {
        return node;
    }

    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(write.Content().Ref(), syncList, true)) {
        return node;
    }

    const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
    TYtOutTableInfo outTable(outItemType, nativeTypeFlags);

    const auto dqUnion = write.Content().Cast<TDqCnUnionAll>();

    TMaybeNode<TCoSort> sort;
    TMaybeNode<TCoTopSort> topSort;
    TMaybeNode<TDqCnMerge> mergeConnection;
    auto topLambdaBody = dqUnion.Output().Stage().Program().Body();

    // Look for the sort-only stage or DcCnMerge connection.
    bool sortOnlyLambda = true;
    auto& topNode = SkipCallables(topLambdaBody.Ref(), {"ToFlow", "FromFlow", "ToStream"});
    if (auto maybeSort = TMaybeNode<TCoSort>(&topNode)) {
        sort = maybeSort;
    } else if (auto maybeTopSort = TMaybeNode<TCoTopSort>(&topNode)) {
        topSort = maybeTopSort;
    } else {
        sortOnlyLambda = false;
        if (auto inputs = dqUnion.Output().Stage().Inputs(); inputs.Size() == 1 && inputs.Item(0).Maybe<TDqCnMerge>().IsValid()) {
            if (SkipCallables(topNode, {"Skip", "Take"}).IsArgument()) {
                mergeConnection = inputs.Item(0).Maybe<TDqCnMerge>();
            } else if (topNode.IsCallable(TDqReplicate::CallableName()) && topNode.Head().IsArgument()) {
                auto ndx = FromString<size_t>(dqUnion.Output().Index().Value());
                YQL_ENSURE(ndx + 1 < topNode.ChildrenSize());
                if (&topNode.Child(ndx + 1)->Head().Head() == &topNode.Child(ndx + 1)->Tail()) { // trivial lambda
                    mergeConnection = inputs.Item(0).Maybe<TDqCnMerge>();
                }
            }
        }
    }

    if (sortOnlyLambda) {
        auto& bottomNode = SkipCallables(topNode.Head(), {"ToFlow", "FromFlow", "ToStream"});
        sortOnlyLambda = bottomNode.IsArgument();
    }

    TCoLambda writeLambda = Build<TCoLambda>(ctx, write.Pos())
        .Args({"stream"})
        .Body<TDqWrite>()
            .Input("stream")
            .Provider().Value(YtProviderName).Build()
            .Settings().Build()
        .Build()
        .Done();

    if (sortOnlyLambda && (sort || topSort)) {
        // Add Unordered callable to kill sort in a stage. Sorting will be handled by YtSort.
        writeLambda = Build<TCoLambda>(ctx, write.Pos())
            .Args({"stream"})
            .Body<TExprApplier>()
                .Apply(writeLambda)
                .With<TCoUnordered>(0)
                    .Input("stream")
                .Build()
            .Build()
            .Done();
    }

    TMaybeNode<TDqConnection> result;
    if (NDq::GetStageOutputsCount(dqUnion.Output().Stage()) > 1) {
        result = Build<TDqCnUnionAll>(ctx, write.Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(dqUnion)
                    .Build()
                    .Program(writeLambda)
                    .Settings(NDq::TDqStageSettings().BuildNode(ctx, write.Pos()))
                .Build()
                .Index().Build("0")
            .Build()
            .Done().Ptr();
    } else {
        result = NDq::DqPushLambdaToStageUnionAll(dqUnion, writeLambda, {}, ctx, optCtx);
        if (!result) {
            return {};
        }
    }

    result = CleanupWorld(result.Cast(), ctx);

    auto dqCnResult = Build<TDqCnResult>(ctx, write.Pos())
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add(result.Cast())
                .Build()
                .Program()
                    .Args({"row"})
                    .Body("row")
                .Build()
                .Settings(NDq::TDqStageSettings().BuildNode(ctx, write.Pos()))
            .Build()
            .Index().Build("0")
        .Build()
        .ColumnHints() // TODO: set column hints
        .Build()
        .Done().Ptr();

    auto writeOp = Build<TYtDqProcessWrite>(ctx, write.Pos())
        .World(ApplySyncListToWorld(ctx.NewWorld(write.Pos()), syncList, ctx))
        .DataSink(write.DataSink().Ptr())
        .Output()
            .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
        .Build()
        .Input(dqCnResult)
        .Done().Ptr();

    auto writeOutput = Build<TYtOutput>(ctx, write.Pos())
        .Operation(writeOp)
        .OutIndex().Value(0U).Build()
        .Done().Ptr();

    if (sort) {
        writeOutput = Build<TCoSort>(ctx, write.Pos())
            .Input(writeOutput)
            .SortDirections(sort.SortDirections().Cast())
            .KeySelectorLambda(sort.KeySelectorLambda().Cast())
            .Done().Ptr();
    } else if (topSort) {
        writeOutput = Build<TCoTopSort>(ctx, write.Pos())
            .Input(writeOutput)
            .Count(topSort.Count().Cast())
            .SortDirections(topSort.SortDirections().Cast())
            .KeySelectorLambda(topSort.KeySelectorLambda().Cast())
            .Done().Ptr();
    } else if (mergeConnection) {
        writeOutput = Build<TCoSort>(ctx, write.Pos())
            .Input(writeOutput)
            .SortDirections(
                ctx.Builder(write.Pos())
                    .List()
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            size_t index = 0;
                            for (auto col: mergeConnection.Cast().SortColumns()) {
                                parent.Callable(index++, "Bool")
                                    .Atom(0, col.SortDirection().Value() == "Asc" ? "1" : "0", TNodeFlags::Default)
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                    .Build()
            )
            .KeySelectorLambda(
                ctx.Builder(write.Pos())
                    .Lambda()
                        .Param("item")
                        .List()
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                size_t index = 0;
                                for (auto col: mergeConnection.Cast().SortColumns()) {
                                    parent.Callable(index++, "Member")
                                        .Arg(0, "item")
                                        .Atom(1, col.Column().Value())
                                    .Seal();
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                    .Build()
            )
            .Done().Ptr();
    }

    return ctx.ChangeChild(write.Ref(), TYtWriteTable::idx_Content, std::move(writeOutput));
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::YtDqProcessWrite(TExprBase node, TExprContext& ctx) const {
    const auto& write = node.Cast<TYtDqProcessWrite>();
    if (const auto& contents = FindNodes(write.Input().Ptr(),
        [] (const TExprNode::TPtr& node) { return !TYtOutputOpBase::Match(node.Get()); },
        [] (const TExprNode::TPtr& node) { return node->IsCallable({TCoToFlow::CallableName(), TCoIterator::CallableName()}) && node->Head().IsCallable(TYtTableContent::CallableName()); });
        !contents.empty()) {
        TNodeOnNodeOwnedMap replaces(contents.size());
        const bool addToken = !State_->Configuration->Auth.Get().GetOrElse(TString()).empty();

        for (const auto& cont : contents) {
            const TYtTableContent content(cont->HeadPtr());
            auto input = content.Input();
            const auto output = input.Maybe<TYtOutput>();
            const auto structType = GetSeqItemType(output ? output.Cast().Ref().GetTypeAnn() : input.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back())->Cast<TStructExprType>();
            if (output) {
                input = ConvertContentInputToRead(output.Cast(), {}, ctx);
            }
            TMaybeNode<TCoSecureParam> secParams;
            if (addToken) {
                const auto cluster = input.Cast<TYtReadTable>().DataSource().Cluster();
                secParams = Build<TCoSecureParam>(ctx, node.Pos()).Name().Build(TString("cluster:default_").append(cluster)).Done();
            }

            TExprNode::TListType flags;
            if (!NYql::HasSetting(content.Settings().Ref(), EYtSettingType::Split))
                flags.emplace_back(ctx.NewAtom(cont->Pos(), "Solid", TNodeFlags::Default));

            const auto read = Build<TDqReadWideWrap>(ctx, cont->Pos())
                    .Input(input)
                    .Flags().Add(std::move(flags)).Build()
                    .Token(secParams)
                    .Done();

            auto narrow = ctx.Builder(cont->Pos())
                .Callable("NarrowMap")
                    .Add(0, read.Ptr())
                    .Lambda(1)
                        .Params("fields", structType->GetSize())
                        .Callable(TCoAsStruct::CallableName())
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                ui32 i = 0U;
                                for (const auto& item : structType->GetItems()) {
                                    parent.List(i)
                                        .Atom(0, item->GetName())
                                        .Arg(1, "fields", i)
                                    .Seal();
                                    ++i;
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal().Build();

            narrow = ctx.WrapByCallableIf(cont->IsCallable(TCoIterator::CallableName()), TCoFromFlow::CallableName(), std::move(narrow));
            replaces.emplace(cont.Get(), std::move(narrow));
        }

        return Build<TYtDqProcessWrite>(ctx, write.Pos())
            .InitFrom(write)
            .Input(ctx.ReplaceNodes(write.Input().Ptr(), replaces))
        .Done();
    }

    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Write(TExprBase node, TExprContext& ctx) const {
    auto write = node.Cast<TYtWriteTable>();
    if (!IsYtProviderInput(write.Content())) {
        return node;
    }

    auto cluster = TString{write.DataSink().Cluster().Value()};
    auto srcCluster = GetClusterName(write.Content());
    if (cluster != srcCluster) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Result from cluster " << TString{srcCluster}.Quote()
            << " cannot be written to a different destination cluster " << cluster.Quote()));
        return {};
    }

    TVector<TYtPathInfo::TPtr> inputPaths = GetInputPaths(write.Content());
    TYtTableInfo::TPtr outTableInfo = MakeIntrusive<TYtTableInfo>(write.Table());

    const auto mode = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Mode);
    const bool renew = !mode || FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Renew;
    const bool flush = mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Flush;
    const bool transactionalOverrideTarget = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Initial)
        && !flush && (renew || !outTableInfo->Meta->DoesExist);

    const TStructExprType* outItemType = nullptr;
    if (auto type = GetSequenceItemType(write.Content(), false, ctx)) {
        outItemType = type->Cast<TStructExprType>();
    } else {
        return {};
    }

    auto maybeReadSettings = write.Content().Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Settings();

    const TYtTableDescription& nextDescription = State_->TablesData->GetTable(cluster, outTableInfo->Name, outTableInfo->CommitEpoch);
    const ui64 nativeTypeFlags = nextDescription.RowSpec->GetNativeYtTypeFlags();

    TMaybe<NYT::TNode> firstNativeType;
    ui64 firstNativeTypeFlags = 0;
    if (!inputPaths.empty()) {
        firstNativeType = inputPaths.front()->GetNativeYtType();
        firstNativeTypeFlags = inputPaths.front()->GetNativeYtTypeFlags();
    }

    bool requiresMap = (maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::SysColumns))
        || AnyOf(inputPaths, [firstNativeType] (const TYtPathInfo::TPtr& path) {
            return path->RequiresRemap() || firstNativeType != path->GetNativeYtType();
        });

    const bool requiresMerge = !requiresMap && (
        AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
            return path->Ranges || path->HasColumns() || path->Table->Meta->IsDynamic || path->Table->FromNode.Maybe<TYtTable>();
        })
        || (maybeReadSettings && NYql::HasAnySetting(maybeReadSettings.Ref(),
            EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Sample))
        || (nextDescription.RowSpec->GetColumnOrder().Defined() && AnyOf(inputPaths, [colOrder = *nextDescription.RowSpec->GetColumnOrder()] (const TYtPathInfo::TPtr& path) {
            return path->Table->RowSpec->GetColumnOrder().Defined() && *path->Table->RowSpec->GetColumnOrder() != colOrder;
        }))
    );

    TMaybeNode<TCoAtom> outMode;
    if (ctx.IsConstraintEnabled<TSortedConstraintNode>() && maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Unordered)) {
        outMode = Build<TCoAtom>(ctx, write.Pos()).Value(ToString(EYtSettingType::Unordered)).Done();
    }

    TVector<TYtOutput> publishInput;
    if (requiresMap || requiresMerge) {
        TExprNode::TPtr mapper;
        if (requiresMap) {
            mapper = Build<TCoLambda>(ctx, write.Pos())
                .Args({"stream"})
                .Body("stream")
                .Done().Ptr();
        }

        // For YtMerge passthrough native flags as is. AlignPublishTypes optimizer will add additional remapping
        TYtOutTableInfo outTable(outItemType, requiresMerge ? firstNativeTypeFlags : nativeTypeFlags);
        if (firstNativeType) {
            outTable.RowSpec->CopyTypeOrders(*firstNativeType);
        }
        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, write.Pos());
        bool useExplicitColumns = requiresMerge && AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
            return !path->Table->IsTemp;
        });
        if (requiresMap) {
            if (ctx.IsConstraintEnabled<TSortedConstraintNode>()) {
                if (auto sorted = write.Content().Ref().GetConstraint<TSortedConstraintNode>()) {
                    const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
                    TKeySelectorBuilder builder(write.Pos(), ctx, useNativeDescSort, outItemType);
                    builder.ProcessConstraint(*sorted);
                    builder.FillRowSpecSort(*outTable.RowSpec);

                    if (builder.NeedMap()) {
                        mapper = ctx.Builder(write.Pos())
                            .Lambda()
                                .Param("stream")
                                .Apply(builder.MakeRemapLambda(true))
                                    .With(0)
                                        .Apply(mapper)
                                            .With(0, "stream")
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                            .Build();
                    }
                }
            } else {
                if (inputPaths.size() == 1 && inputPaths.front()->Table->RowSpec && inputPaths.front()->Table->RowSpec->IsSorted()) {
                    outTable.RowSpec->CopySortness(ctx, *inputPaths.front()->Table->RowSpec);
                }
            }
        }
        else { // requiresMerge
            // TODO: should we keep sort if multiple inputs?
            if (outMode || AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) { return path->Table->IsUnordered; })) {
                useExplicitColumns = useExplicitColumns || AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) { return path->Table->RowSpec->HasAuxColumns(); });
            }
            else {
                const bool exactCopySort = inputPaths.size() == 1 && !inputPaths.front()->HasColumns();
                bool hasAux = inputPaths.front()->Table->RowSpec->HasAuxColumns();
                bool sortIsChanged = inputPaths.front()->Table->IsUnordered
                    ? inputPaths.front()->Table->RowSpec->IsSorted()
                    : outTable.RowSpec->CopySortness(ctx, *inputPaths.front()->Table->RowSpec,
                        exactCopySort ? TYqlRowSpecInfo::ECopySort::Exact : TYqlRowSpecInfo::ECopySort::WithDesc);
                useExplicitColumns = useExplicitColumns || (inputPaths.front()->HasColumns() && hasAux);

                for (size_t i = 1; i < inputPaths.size(); ++i) {
                    sortIsChanged = outTable.RowSpec->MakeCommonSortness(ctx, *inputPaths[i]->Table->RowSpec) || sortIsChanged;
                    const bool tableHasAux = inputPaths[i]->Table->RowSpec->HasAuxColumns();
                    hasAux = hasAux || tableHasAux;
                    if (inputPaths[i]->HasColumns() && tableHasAux) {
                        useExplicitColumns = true;
                    }
                }
                useExplicitColumns = useExplicitColumns || (sortIsChanged && hasAux);
            }

            if (maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Sample)) {
                settingsBuilder.Add()
                    .Name().Value(ToString(EYtSettingType::ForceTransform)).Build()
                .Build();
            }
        }
        outTable.SetUnique(write.Content().Ref().GetConstraint<TDistinctConstraintNode>(), write.Pos(), ctx);
        outTable.RowSpec->SetConstraints(write.Content().Ref().GetConstraintSet());

        TMaybeNode<TYtTransientOpBase> op;
        if (requiresMap) {
            if (outTable.RowSpec->IsSorted()) {
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

            op = Build<TYtMap>(ctx, write.Pos())
                .World(GetWorld(write.Content(), {}, ctx))
                .DataSink(write.DataSink())
                .Input(ConvertInputTable(write.Content(), ctx))
                .Output()
                    .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                .Build()
                .Settings(settingsBuilder.Done())
                .Mapper(mapper)
                .Done();
        }
        else {
            TConvertInputOpts opts;
            if (useExplicitColumns) {
                opts.ExplicitFields(*outTable.RowSpec, write.Pos(), ctx);
            }
            op = Build<TYtMerge>(ctx, write.Pos())
                .World(GetWorld(write.Content(), {}, ctx))
                .DataSink(write.DataSink())
                .Input(ConvertInputTable(write.Content(), ctx, opts))
                .Output()
                    .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                .Build()
                .Settings(settingsBuilder.Done())
                .Done();
        }

        publishInput.push_back(Build<TYtOutput>(ctx, write.Pos())
            .Operation(op.Cast())
            .OutIndex().Value(0U).Build()
            .Mode(outMode)
            .Done());
    }
    else {
        if (auto out = write.Content().Maybe<TYtOutput>()) {
            publishInput.push_back(out.Cast());
        } else {
            for (auto path: write.Content().Cast<TCoRight>().Input().Cast<TYtReadTable>().Input().Item(0).Paths()) {
                publishInput.push_back(Build<TYtOutput>(ctx, path.Table().Pos())
                    .InitFrom(path.Table().Cast<TYtOutput>())
                    .Mode(outMode)
                    .Done());
            }
        }
    }

    auto publishSettings = write.Settings();
    if (transactionalOverrideTarget) {
        publishSettings = TCoNameValueTupleList(NYql::RemoveSetting(publishSettings.Ref(), EYtSettingType::Mode, ctx));
    }

    return Build<TYtPublish>(ctx, write.Pos())
        .World(write.World())
        .DataSink(write.DataSink())
        .Input()
            .Add(publishInput)
        .Build()
        .Publish(write.Table().Cast<TYtTable>())
        .Settings(publishSettings)
        .Done();
}
TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ReadWithSettings(TExprBase node, TExprContext& ctx) const {
    if (State_->PassiveExecution) {
        return node;
    }

    auto maybeRead = node.Cast<TCoRight>().Input().Maybe<TYtReadTable>();
    if (!maybeRead) {
        return node;
    }

    auto read = maybeRead.Cast().Ptr();
    TSyncMap syncList;
    auto ret = OptimizeReadWithSettings(read, true, true, syncList, State_, ctx);
    if (ret != read) {
        if (ret) {
            if (!syncList.empty()) {
                ret = ctx.ChangeChild(*ret, TYtReadTable::idx_World,
                    ApplySyncListToWorld(ret->ChildPtr(TYtReadTable::idx_World), syncList, ctx));
            }
            return Build<TCoRight>(ctx, node.Pos())
                .Input(ret)
                .Done();
        }
        return {};
    }
    return node;
}
TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ReplaceStatWriteTable(TExprBase node, TExprContext& ctx) const {
    if (State_->PassiveExecution) {
        return node;
    }

    auto write = node.Cast<TStatWriteTable>();
    auto input = write.Input();

    TMaybeNode<TYtOutput> newInput;

    if (!IsYtProviderInput(input, false)) {
        if (!EnsurePersistable(input.Ref(), ctx)) {
            return {};
        }

        TString cluster;
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(input.Ref(), syncList, cluster, false)) {
            return node;
        }

        const TTypeAnnotationNode* outItemType;
        if (!GetSequenceItemType(input.Ref(), outItemType, ctx)) {
            return {};
        }
        if (!EnsurePersistableType(input.Pos(), *outItemType, ctx)) {
            return {};
        }

        auto cleanup = CleanupWorld(input, ctx);
        if (!cleanup) {
            return {};
        }

        TYtOutTableInfo outTable {outItemType->Cast<TStructExprType>(),
            State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE};
        outTable.RowSpec->SetConstraints(input.Ref().GetConstraintSet());
        outTable.SetUnique(input.Ref().GetConstraint<TDistinctConstraintNode>(), input.Pos(), ctx);

        if (!cluster) {
            cluster = State_->Configuration->DefaultCluster
                .Get().GetOrElse(State_->Gateway->GetDefaultClusterName());
        }

        input = Build<TYtOutput>(ctx, write.Pos())
            .Operation<TYtFill>()
                .World(ApplySyncListToWorld(ctx.NewWorld(write.Pos()), syncList, ctx))
                .DataSink<TYtDSink>()
                    .Category()
                        .Value(YtProviderName)
                    .Build()
                    .Cluster()
                        .Value(cluster)
                    .Build()
                .Build()
                .Output()
                    .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                .Build()
                .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                .Settings(GetFlowSettings(write.Pos(), *State_, ctx))
            .Build()
            .OutIndex()
                .Value("0")
            .Build()
            .Done();
    }

    if (auto maybeRead = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
        auto read = maybeRead.Cast();
        if (read.Input().Size() != 1) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Input().Pos()), TStringBuilder() <<
                "Unexpected read with several sections on " << node.Ptr()->Content()
            ));
            return {};
        }

        auto section = read.Input().Item(0);
        auto scheme = section.Ptr()->GetTypeAnn()->Cast<TListExprType>()->GetItemType();

        auto path = CopyOrTrivialMap(section.Pos(),
            GetWorld(input, {}, ctx),
            GetDataSink(input, ctx),
            *scheme,
            Build<TYtSection>(ctx, section.Pos())
                .InitFrom(section)
                .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Unordered | EYtSettingType::Unordered, ctx))
            .Done(),
            {}, ctx, State_,
            TCopyOrTrivialMapOpts().SetTryKeepSortness(true).SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>()));

        YQL_ENSURE(
            path.Ranges().Maybe<TCoVoid>(),
            "Unexpected slices: " << path.Ranges().Ref().Content()
        );

        YQL_ENSURE(
            path.Table().Maybe<TYtOutput>().Operation(),
            "Unexpected node: " << path.Table().Ref().Content()
        );

        newInput = path.Table().Cast<TYtOutput>();
    } else if (auto op = input.Maybe<TYtOutput>().Operation()) {
        newInput = input.Cast<TYtOutput>();
    } else {
        YQL_ENSURE(false, "Unexpected operation input: " << input.Ptr()->Content());
    }

    auto table = ctx.RenameNode(write.Table().Ref(), TYtStatOutTable::CallableName());

    return Build<TYtStatOut>(ctx, write.Pos())
        .World(GetWorld(input, {}, ctx))
        .DataSink(GetDataSink(input, ctx))
        .Input(newInput.Cast())
        .Table(table)
        .ReplaceMask(write.ReplaceMask())
        .Settings(write.Settings())
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::YtDqWrite(TExprBase node, TExprContext& ctx) const {
    const auto write = node.Cast<TYtDqWrite>();

    if (ETypeAnnotationKind::Stream == write.Ref().GetTypeAnn()->GetKind()) {
        return Build<TCoFromFlow>(ctx, write.Pos())
            .Input<TYtDqWrite>()
                .Settings(write.Settings())
                .Input<TCoToFlow>()
                    .Input(write.Input())
                .Build()
            .Build().Done();
    }

    const auto& items = GetSeqItemType(write.Input().Ref().GetTypeAnn())->Cast<TStructExprType>()->GetItems();
    auto expand = ctx.Builder(write.Pos())
        .Callable("ExpandMap")
            .Add(0, write.Input().Ptr())
            .Lambda(1)
                .Param("item")
                .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                    ui32 i = 0U;
                    for (const auto& item : items) {
                        lambda.Callable(i++, "Member")
                            .Arg(0, "item")
                            .Atom(1, item->GetName())
                        .Seal();
                    }
                    return lambda;
                })
            .Seal()
        .Seal().Build();

    return Build<TCoDiscard>(ctx, write.Pos())
        .Input<TYtDqWideWrite>()
            .Input(std::move(expand))
            .Settings(write.Settings())
        .Build().Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Fill(TExprBase node, TExprContext& ctx) const {
    if (State_->PassiveExecution) {
        return node;
    }

    auto write = node.Cast<TYtWriteTable>();

    auto mode = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Mode);

    if (mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Drop) {
        return node;
    }

    auto cluster = TString{write.DataSink().Cluster().Value()};
    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(write.Content().Ref(), syncList, cluster, false)) {
        return node;
    }

    if (FindNode(write.Content().Ptr(),
        [] (const TExprNode::TPtr& node) { return !TMaybeNode<TYtOutputOpBase>(node).IsValid(); },
        [] (const TExprNode::TPtr& node) { return TMaybeNode<TDqConnection>(node).IsValid(); })) {
        return node;
    }

    const TStructExprType* outItemType = nullptr;
    if (auto type = GetSequenceItemType(write.Content(), false, ctx)) {
        if (!EnsurePersistableType(write.Content().Pos(), *type, ctx)) {
            return {};
        }
        outItemType = type->Cast<TStructExprType>();
    } else {
        return {};
    }
    TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);

    {
        auto path = write.Table().Name().StringValue();
        auto commitEpoch = TEpochInfo::Parse(write.Table().CommitEpoch().Ref()).GetOrElse(0);
        auto dstRowSpec = State_->TablesData->GetTable(cluster, path, commitEpoch).RowSpec;
        outTable.RowSpec->SetColumnOrder(dstRowSpec->GetColumnOrder());
    }
    auto content = write.Content();
    if (auto sorted = content.Ref().GetConstraint<TSortedConstraintNode>()) {
        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        TKeySelectorBuilder builder(node.Pos(), ctx, useNativeDescSort, outItemType);
        builder.ProcessConstraint(*sorted);
        builder.FillRowSpecSort(*outTable.RowSpec);

        if (builder.NeedMap()) {
            content = Build<TExprApplier>(ctx, content.Pos())
                .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                .With(0, content)
                .Done();
            outItemType = builder.MakeRemapType();
        }

    } else if (auto unordered = content.Maybe<TCoUnorderedBase>()) {
        content = unordered.Cast().Input();
    }
    outTable.RowSpec->SetConstraints(write.Content().Ref().GetConstraintSet());
    outTable.SetUnique(write.Content().Ref().GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);

    TYtTableInfo::TPtr pubTableInfo = MakeIntrusive<TYtTableInfo>(write.Table());
    const bool renew = !mode || FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Renew;
    const bool flush = mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Flush;
    const bool transactionalOverrideTarget = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Initial)
        && !flush && (renew || !pubTableInfo->Meta->DoesExist);

    auto publishSettings = write.Settings();
    if (transactionalOverrideTarget) {
        publishSettings = TCoNameValueTupleList(NYql::RemoveSetting(publishSettings.Ref(), EYtSettingType::Mode, ctx));
    }

    auto cleanup = CleanupWorld(content, ctx);
    if (!cleanup) {
        return {};
    }

    return Build<TYtPublish>(ctx, write.Pos())
        .World(write.World())
        .DataSink(write.DataSink())
        .Input()
            .Add()
                .Operation<TYtFill>()
                    .World(ApplySyncListToWorld(ctx.NewWorld(write.Pos()), syncList, ctx))
                    .DataSink(write.DataSink())
                    .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                    .Output()
                        .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Settings(GetFlowSettings(write.Pos(), *State_, ctx))
                .Build()
                .OutIndex().Value(0U).Build()
            .Build()
        .Build()
        .Publish(write.Table())
        .Settings(publishSettings)
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::UnorderedPublishTarget(TExprBase node, TExprContext& ctx) const {
    auto publish = node.Cast<TYtPublish>();

    auto cluster = TString{publish.DataSink().Cluster().Value()};
    auto pubTableInfo = TYtTableInfo(publish.Publish());
    if (auto commitEpoch = pubTableInfo.CommitEpoch.GetOrElse(0)) {
        const TYtTableDescription& nextDescription = State_->TablesData->GetTable(cluster, pubTableInfo.Name, commitEpoch);
        YQL_ENSURE(nextDescription.RowSpec);
        if (!nextDescription.RowSpec->IsSorted()) {
            bool modified = false;
            TVector<TYtOutput> outs;
            for (auto out: publish.Input()) {
                if (!IsUnorderedOutput(out) && TYqlRowSpecInfo(GetOutTable(out).Cast<TYtOutTable>().RowSpec()).IsSorted()) {
                    outs.push_back(Build<TYtOutput>(ctx, out.Pos())
                        .InitFrom(out)
                        .Mode()
                            .Value(ToString(EYtSettingType::Unordered))
                        .Build()
                        .Done());
                    modified = true;
                } else {
                    outs.push_back(out);
                }
            }
            if (modified) {
                return Build<TYtPublish>(ctx, publish.Pos())
                    .InitFrom(publish)
                    .Input()
                        .Add(outs)
                    .Build()
                    .Done();
            }
        }
    }
    return node;
}

}  // namespace NYql
