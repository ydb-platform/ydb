#include "kqp_rbo_physical_query_builder.h"
#include "kqp_rbo_compatibility.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_convertion_utils.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_yql_ast_trace.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <cctype>
#include <optional>
#include <sstream>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr;

namespace {

std::string TraceRootId(const std::string& title) {
    std::string id = "physical-ast";
    for (const char ch : title) {
        id += (std::isalnum(static_cast<unsigned char>(ch)) ? ch : '-');
    }
    return id;
}

std::string ToStdString(TStringBuf value) {
    return std::string(value.data(), value.size());
}

std::string FormatExprForTrace(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!node) {
        return {};
    }

    return ToStdString(KqpExprToPrettyString(*node, ctx));
}

std::string FormatPhysicalStagesForTrace(const TVector<TExprNode::TPtr>& stages, TExprContext& ctx) {
    std::ostringstream out;
    for (size_t i = 0; i < stages.size(); ++i) {
        if (i) {
            out << "\n";
        }
        out << "----- Stage " << i << " -----\n"
            << FormatExprForTrace(stages[i], ctx)
            << "\n";
    }

    return out.str();
}

void AddAstInfoTabs(
    optimizer_trace::Trace::Tile& tile,
    const std::optional<optimizer_trace::Widget>& linkGraph,
    const std::string& text)
{
    if (linkGraph) {
        tile.info().tab("dag-links", "DAG links")
            .widget(*linkGraph);
    }
    if (!text.empty()) {
        tile.info().tab("yql-ast-text", "YQL AST text")
            .widget(optimizer_trace::Widget::unwrappedText("Regular YQL AST", text, true));
    }
}

void SubmitPhysicalAstTrace(TRBOContext& rboCtx, const std::string& title, NYqlAstTrace::TBuildResult astTrace, std::string text) {
    if (!rboCtx.NeedToLog()) {
        return;
    }

    auto& tile = rboCtx.TraceLog.currentStage().tree(title, astTrace.Root);
    AddAstInfoTabs(tile, astTrace.LinkGraph, text);
    rboCtx.TraceLog.Submit(tile);
}

void SubmitPhysicalStagesTrace(TRBOContext& rboCtx, const std::string& title, const TVector<TExprNode::TPtr>& stages) {
    if (!rboCtx.NeedToLog()) {
        return;
    }

    SubmitPhysicalAstTrace(
        rboCtx,
        title,
        NYqlAstTrace::BuildStageListTreeWithInfo(stages, TraceRootId(title)),
        FormatPhysicalStagesForTrace(stages, rboCtx.ExprCtx));
}

void SubmitPhysicalExprTrace(TRBOContext& rboCtx, const std::string& title, const TExprNode::TPtr& node) {
    if (!rboCtx.NeedToLog() || !node) {
        return;
    }

    SubmitPhysicalAstTrace(
        rboCtx,
        title,
        NYqlAstTrace::BuildExprTreeWithInfo(node, TraceRootId(title)),
        FormatExprForTrace(node, rboCtx.ExprCtx));
}

} // anonymous namespace

TPhysicalQueryBuilder::TPhysicalQueryBuilder(TVector<TIntrusivePtr<TOpRoot>> roots, 
    TVector<TStageGraph>&& graphs, TVector<THashMap<ui32, TExprNode::TPtr>>&& stages, 
    TVector<THashMap<ui32, TVector<TExprNode::TPtr>>>&& stageArgs,
    TVector<THashMap<ui32, TPositionHandle>>&& stagePos, TRBOContext& rboCtx)
    : Roots(roots)
    , Graphs(std::move(graphs))
    , Stages(std::move(stages))
    , StageArgs(std::move(stageArgs))
    , StagePos(std::move(stagePos))
    , RBOCtx(rboCtx)
{
    Materialize.resize(roots.size());
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery() {
    TVector<TVector<TExprNode::TPtr>> allPhyStages;

    for (size_t i=0; i<Roots.size(); i++) {
        auto phyStages = BuildPhysicalStageGraph(i);
        SubmitPhysicalStagesTrace(RBOCtx, "After physical stage graph build", phyStages);
        const bool fullPeephole = RBOCtx.KqpCtx.Config->GetEnableNewRBOPhysicalStagePeephole();
        phyStages = PreparePhysicalStages(std::move(phyStages), fullPeephole);
        SubmitPhysicalStagesTrace(RBOCtx, "After physical stage preparation", phyStages);
        if (!fullPeephole) {
            phyStages = LowerPhysicalStageCompatibility(std::move(phyStages));
            SubmitPhysicalStagesTrace(RBOCtx, "After compatibility lowering", phyStages);
        } else {
            phyStages = PeepHoleOptimizePhysicalStages(std::move(phyStages));
            SubmitPhysicalStagesTrace(RBOCtx, "After physical peephole", phyStages);
        }
        allPhyStages.push_back(std::move(phyStages));
    }
    auto physicalQuery = BuildPhysicalQuery(std::move(allPhyStages));
    SubmitPhysicalExprTrace(RBOCtx, "Final physical query", physicalQuery);
    return physicalQuery;
}

TVector<TExprNode::TPtr> TPhysicalQueryBuilder::LowerPhysicalStageCompatibility(TVector<TExprNode::TPtr>&& physicalStages) {
    Y_ENSURE(!physicalStages.empty());
    auto root = physicalStages.back();
    if (!NeedsRboCompatibilityLowering(root)) {
        return std::move(physicalStages);
    }

    TOptimizeExprSettings settings(&RBOCtx.TypeCtx);
    settings.CustomInstantTypeTransformer = RBOCtx.TypeCtx.CustomInstantTypeTransformer.Get();
    constexpr size_t MaxPasses = 64;
    for (size_t pass = 0; pass < MaxPasses && NeedsRboCompatibilityLowering(root); ++pass) {
        TExprNode::TPtr output;
        const auto status = OptimizeExpr(
            root,
            output,
            [&](const TExprNode::TPtr& node, TExprContext& ctx) {
                return RewriteRboCompatibilityNode(node, ctx, RBOCtx.TypeCtx);
            },
            RBOCtx.ExprCtx,
            settings);
        YQL_ENSURE(status != IGraphTransformer::TStatus::Error,
            "Failed to lower execution-incompatible callables in new RBO physical stages");
        if (status == IGraphTransformer::TStatus::Ok) {
            break;
        }

        YQL_ENSURE(status == IGraphTransformer::TStatus::Repeat);
        YQL_ENSURE(output != root, "RBO compatibility lowering made no progress");
        root = std::move(output);
        TypeAnnotate(root);
    }

    EnsureRboCompatibilityLowered(root);
    TVector<TExprNode::TPtr> stagesTopSorted;
    TopologicalSort(TDqPhyStage(root), stagesTopSorted);
    return stagesTopSorted;
}

TVector<TExprNode::TPtr> TPhysicalQueryBuilder::BuildPhysicalStageGraph(int rootIdx) {
    TVector<TExprNode::TPtr> phyStages;
    auto& graph = Graphs[rootIdx];
    graph.TopologicalSort();
    const auto& stageIds = graph.StageIds;
    const auto& stageInputIds = graph.StageInputs;
    auto& ctx = RBOCtx.ExprCtx;

    THashMap<ui32, TExprNode::TPtr> finalizedStages;
    for (const auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputConnections;
        THashSet<ui32> processedInputsIds;
        for (const auto inputStageId : stageInputIds.at(id)) {
            if (processedInputsIds.contains(inputStageId)) {
                continue;
            }
            processedInputsIds.insert(inputStageId);

            const auto inputStage = finalizedStages.at(inputStageId);
            const auto connections = graph.GetConnections(inputStageId, id);
            for (const auto& connection : connections) {
                YQL_CLOG(TRACE, CoreDq) << "Building connection: " << inputStageId << "->" << id << ", " << connection->Type;
                auto dqConnection = connection->BuildConnection(inputStage, StagePos[rootIdx].at(inputStageId), ctx);
                YQL_CLOG(TRACE, CoreDq) << "Built connection: " << inputStageId << "->" << id << ", " << connection->Type;
                inputConnections.push_back(dqConnection);
            }
        }

        TExprNode::TPtr stage;
        if (graph.IsSourceStageRowType(id)) {
            stage = Stages[rootIdx].at(id);
            // Want to build materialize for ranges.
            // TODO: Actually old optimizer has some machinery to compute ranges during compilation for some cases, for
            // example when `LiteralRange` is defined, but currenlty we put any case in separate tx. Performance improvement is possible here.
            auto rowSettingsPtr = FindNode(stage, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TKqpReadRangesSourceSettings>(node); });
            if (rowSettingsPtr) {
                auto rowSettings = TExprBase(rowSettingsPtr).Cast<TKqpReadRangesSourceSettings>();
                if (!rowSettings.RangesExpr().Maybe<TCoVoid>()) {
                    const auto materializeResult = BuildMaterialize(rootIdx, rowSettings.RangesExpr().Ptr());
                    // clang-fomrat off
                    const auto newRowSettings = Build<TKqpReadRangesSourceSettings>(ctx, rowSettingsPtr->Pos())
                        .Table(rowSettings.Table())
                        .Columns(rowSettings.Columns())
                        .Settings(rowSettings.Settings())
                        .RangesExpr(materializeResult)
                        .ExplainPrompt(rowSettings.ExplainPrompt())
                    .Done().Ptr();
                    // clang-format on
                    stage = ctx.ReplaceNode(std::move(stage), rowSettings.Ref(), newRowSettings);
                }
            }
        } else {
            TVector<TExprNode::TPtr> stageInputConnections;
            TVector<TExprNode::TPtr> stageInputArgs;
            if (!graph.IsSourceStageColumnType(id)) {
                stageInputConnections = inputConnections;
                stageInputArgs = StageArgs[rootIdx].at(id);
            }

            auto stageGUID = graph.StageGUIDs.at(id);
            if (graph.IsSinkStage(id)) {
                stage = BuildDqPhySinkStage(stageInputConnections, stageInputArgs, Stages[rootIdx].at(id), NYql::NDq::TDqStageSettings().New(stageGUID).BuildNode(ctx, StagePos[rootIdx].at(id)),
                                    graph.GetSinkSettings(id), ctx, StagePos[rootIdx].at(id));
            }
            else {
                stage = BuildDqPhyStage(stageInputConnections, stageInputArgs, Stages[rootIdx].at(id), NYql::NDq::TDqStageSettings().New(stageGUID).BuildNode(ctx, StagePos[rootIdx].at(id)),
                                    ctx, StagePos[rootIdx].at(id));
            }
            phyStages.emplace_back(stage);
            YQL_CLOG(TRACE, CoreDq) << "Added stage " << stage->UniqueId();
        }

        if (graph.IsSourceStageColumnType(id)) {
            auto readPtr = FindNode(stage, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TKqpBlockReadOlapTableRanges>(node); });
            if (readPtr) {
                auto read = TExprBase(readPtr).Cast<TKqpBlockReadOlapTableRanges>();
                if (!read.Ranges().Maybe<TCoVoid>()) {
                    const auto materializeResult = BuildMaterialize(rootIdx, read.Ranges().Ptr());
                    // clang-fomrat off
                    const auto newRead = Build<TKqpBlockReadOlapTableRanges>(ctx, readPtr->Pos())
                        .Table(read.Table())
                        .Ranges(materializeResult)
                        .Columns(read.Columns())
                        .Settings(read.Settings())
                        .ExplainPrompt(read.ExplainPrompt())
                        .Process(read.Process())
                    .Done().Ptr();
                    // clang-format on
                    stage = ctx.ReplaceNode(std::move(stage), read.Ref(), newRead);
                    phyStages.back() = stage;
                }
            }
        }

        finalizedStages[id] = stage;
        YQL_CLOG(TRACE, CoreDq) << "Finalized stage " << id;
    }

    // The root stage is last in the topologically sorted StageIds. It is usually also phyStages.back(),
    // but a lone row-source stage is built into Stages without being appended to phyStages (see above),
    // so deriving the final stage from finalizedStages is correct even when phyStages is empty.
    Y_ENSURE(!stageIds.empty());
    const auto maybeFinalStage = finalizedStages.at(stageIds.back());
    const auto finalStage = GetFinalStage(maybeFinalStage);
    const bool needFinalNarrowing = NeedFinalNarrowing(*Roots[rootIdx]);
    const auto finalResultStage = needFinalNarrowing ? BuildFinalNarrowStage(rootIdx, finalStage) : finalStage;
    if (finalStage.Get() != maybeFinalStage.Get()) {
        phyStages.push_back(finalResultStage);
    } else if (needFinalNarrowing) {
        Y_ENSURE(!phyStages.empty());
        Y_ENSURE(phyStages.back().Get() == finalStage.Get());
        phyStages.back() = finalResultStage;
    }

    return phyStages;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildMaterialize(int rootIdx, TExprNode::TPtr node) {
    auto& ctx = RBOCtx.ExprCtx;

    TExprNode::TPtr afterPeephole;
    auto status =
        ::NKikimr::NKqp::NOpt::PeepHoleOptimize(TExprBase(node), afterPeephole, ctx, RBOCtx.TypeCtx, RBOCtx.KqpCtx.Config, false, true, {});
    if (status != IGraphTransformer::TStatus::Ok) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Peephole optimization failed for materialize in NEW RBO"));
        return nullptr;
    }

    // clang-format off
    auto rangesProgram = Build<TCoToStream>(ctx, node->Pos())
        .Input<TCoJust>()
            .Input<TExprList>()
                .Add({afterPeephole})
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on

    auto stageSettings = NYql::NDq::TDqStageSettings().New().SetPartitionMode(NYql::NDq::TDqStageSettings::EPartitionMode::Single).BuildNode(ctx, node->Pos());
    auto phyStage = BuildDqPhyStage({}, {}, rangesProgram, std::move(stageSettings), ctx, node->Pos());

    // clang-format off
    auto result = Build<TDqCnValue>(ctx, node->Pos())
        .Output()
            .Stage(phyStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
    // clang-format on

    TypeAnnotate(result);
    Y_ENSURE(result->GetTypeAnn());

    // clang-format off
    auto param = Build<TCoParameter>(ctx, node->Pos())
        .Name<TCoAtom>()
            .Value(ParamBindingName + ToString(UniqueParamsId++))
        .Build()
        .Type(ExpandType(node->Pos(), *result->GetTypeAnn(), ctx))
    .Done().Ptr();
    // clang-format on

    Materialize[rootIdx].push_back({param, result});
    return param;
}

bool TPhysicalQueryBuilder::IsSingleTaskConnection(const TExprBase& input) const {
    return input.Maybe<TDqCnUnionAll>() || input.Maybe<TDqCnMerge>();
}

bool TPhysicalQueryBuilder::NeedFinalNarrowing(TOpRoot& root) {
    const auto outputIUs = root.GetInput()->GetOutputIUs();
    if (outputIUs.size() != root.ColumnOrder.size()) {
        return true;
    }

    for (ui32 i = 0; i < root.ColumnOrder.size(); ++i) {
        if (outputIUs[i] != TInfoUnit(root.ColumnOrder[i])) {
            return true;
        }
    }

    return false;
}

TExprNode::TPtr TPhysicalQueryBuilder::GetFinalStage(const TExprNode::TPtr& stage) const {
    auto& ctx = RBOCtx.ExprCtx;
    TExprNode::TPtr finalStage;
    bool needFinalUnionStage = false;

    const auto inputs = TDqPhyStage(stage).Inputs();
    // If no inputs - need a final stage.
    if (inputs.Empty()) {
        needFinalUnionStage = true;
    } else {
        // Final stage, which is input for DqCnResult, should have only one 1 task.
        for (const auto& input : TDqPhyStage(stage).Inputs()) {
            if (!IsSingleTaskConnection(input)) {
                needFinalUnionStage = true;
                break;
            }
        }
    }

    if (needFinalUnionStage) {
        // clang-format off
        auto input = Build<TDqCnUnionAll>(ctx, stage->Pos())
            .Output()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .Done().Ptr();

        finalStage = Build<TDqPhyStage>(ctx, stage->Pos())
            .Inputs()
                .Add({input})
            .Build()
            .Program<TCoLambda>()
                .Args({"arg"})
                .Body("arg")
            .Build()
            .Settings(NYql::NDq::TDqStageSettings().BuildNode(ctx, stage->Pos()))
        .Done().Ptr();
    // clang-format on
    } else {
        finalStage = stage;
    }
    return finalStage;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildFinalNarrowStage(int rootIdx, const TExprNode::TPtr& stage) const {
    auto& ctx = RBOCtx.ExprCtx;
    const auto dqStage = TDqPhyStage(stage);

    TVector<TInfoUnit> finalColumns;
    finalColumns.reserve(Roots[rootIdx]->ColumnOrder.size());
    for (const auto& column : Roots[rootIdx]->ColumnOrder) {
        finalColumns.emplace_back(column);
    }

    const auto narrowBody =
        NPhysicalConvertionUtils::ExtractMembers(dqStage.Program().Body().Ptr(), ctx, std::move(finalColumns));

    // clang-format off
    return Build<TDqPhyStage>(ctx, stage->Pos())
        .InitFrom(dqStage)
        .Program()
            .Args(dqStage.Program().Args())
            .Body(narrowBody)
        .Build()
    .Done().Ptr();
    // clang-format on
}

TVector<TKqpParamBinding> TPhysicalQueryBuilder::CollectParamBindings(int rootIdx, const TVector<TExprNode::TPtr>& physicalStages) {
    auto& ctx = RBOCtx.ExprCtx;
    auto pos = Roots[rootIdx]->Pos;

    TVector<TKqpParamBinding> paramBindings;
    THashSet<TString> paramsCollected;
    for (const auto& physicalStage : physicalStages) {
        const auto params = FindNodes(physicalStage, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TCoParameter>(node); });
        for (const auto& param : params) {
            const auto paramName = TExprBase(param).Cast<TCoParameter>().Name().StringValue();
            if (!paramsCollected.contains(paramName) && paramName.find(ParamBindingName) == TString::npos) {
                // clang-format off
                const auto paramBinding = Build<TKqpParamBinding>(ctx, pos)
                    .Name<TCoAtom>()
                        .Value(paramName)
                    .Build()
                .Done();
                // clang-format on
                paramBindings.push_back(paramBinding);
                paramsCollected.insert(paramName);
            }
        }
    }

    return paramBindings;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery(TVector<TVector<TExprNode::TPtr>>&& physicalStages) {
    Y_ENSURE(physicalStages.size());
    auto& ctx = RBOCtx.ExprCtx;
    auto pos = Roots[0]->Pos;

    // FIXME: Support paramenter binding and materialize for multiple statements
    TVector<TExprBase> phyTxs;
    TVector<TKqpParamBinding> paramBindingsAllRoots;

    TVector<TExprBase> phyStagesForMaterialize;
    TVector<TExprBase> resultsForMaterialize;
    TVector<TExprBase> paramBindingsForMaterialize;

    // Prepare physical txs and bindings for materialize if needed.
    ui32 materializeSize = 0;
    ui32 materializeIdx = 0;

    for (size_t i=0; i<Roots.size(); i++) {
        auto paramBindingsCurr = CollectParamBindings(i, physicalStages[i]);
        paramBindingsAllRoots.insert(paramBindingsAllRoots.end(), paramBindingsCurr.begin(), paramBindingsCurr.end());

        materializeSize += Materialize[i].size();

        for (ui32 j = 0; j < materializeSize; ++j) {
            auto param = TExprBase(Materialize[i][j].first).Cast<TCoParameter>();
            auto materializeResult = TExprBase(Materialize[i][j].second).Cast<TDqCnValue>();

            // clang-format off
            auto resultBinding = Build<TKqpTxResultBinding>(ctx, pos)
                .Type(ExpandType(pos, *materializeResult.Ptr()->GetTypeAnn(), ctx))
                .TxIndex().Build("0")
                .ResultIndex().Build(ToString(materializeIdx++))
            .Done();
            // clang-format on

            // clang-format off
            auto paramBinding = Build<TKqpParamBinding>(ctx, pos)
                .Name(param.Name())
                .Binding(resultBinding.Ptr())
            .Done();
            // clang-format on
            // Binding from materialize to main tx.
            paramBindingsAllRoots.emplace_back(paramBinding);

            auto materializeStage = materializeResult.Output().Stage();
            const auto paramBindingsMaterialize = CollectParamBindings(i, {materializeStage.Ptr()});
            // Bindings params in materialize.
            paramBindingsForMaterialize.insert(paramBindingsForMaterialize.end(), paramBindingsMaterialize.begin(), paramBindingsMaterialize.end());
            // Stages for phy tx.
            phyStagesForMaterialize.emplace_back(materializeStage);
            resultsForMaterialize.emplace_back(materializeResult);
        }
    }

    if (materializeSize) {
        TKqpPhyTxSettings txSettings;
        txSettings.Type = EPhysicalTxType::Compute;

        // clang-format off
        auto phyTx = Build<TKqpPhysicalTx>(ctx, pos)
            .Stages()
                .Add(phyStagesForMaterialize)
            .Build()
            .Results()
                .Add(resultsForMaterialize)
            .Build()
            .ParamBindings()
                .Add(paramBindingsForMaterialize)
            .Build()
            .Settings(txSettings.BuildNode(ctx, pos))
        .Done().Ptr();
        // clang-format on

        phyTxs.emplace_back(phyTx);
    }

    TExprNode::TPtr columnOrder;
    TVector<TExprNode::TPtr> dqResults;
    TVector<TExprNode::TPtr> resultBindings;

    TString txIndex = materializeSize ? "1" : "0";
    int resultIndex = 0;

    for (size_t i=0; i<Roots.size(); i++) {
        TVector<TCoAtom> columnAtomList;

        for (const auto& column : Roots[i]->ColumnOrder) {
            columnAtomList.push_back(Build<TCoAtom>(ctx, Roots[i]->Pos).Value(column).Done());
        }
        columnOrder = Build<TCoAtomList>(ctx, Roots[i]->Pos).Add(columnAtomList).Done().Ptr();

        // clang-format off
        // wrap in DqResult
        auto dqResult = Build<TDqCnResult>(ctx, Roots[i]->Pos)
            .Output()
                .Stage(physicalStages[i].back())
                .Index().Build("0")
            .Build()
            .ColumnHints(columnOrder)
        .Done().Ptr();
        // clang-format on

        TypeAnnotate(dqResult);
        YQL_CLOG(TRACE, CoreDq) << "Inferred final type: " << *dqResult->GetTypeAnn();

        dqResults.push_back(dqResult);

        // clang-format off
        auto resultBinding = Build<TKqpTxResultBinding>(ctx, Roots[i]->Pos)
            .Type(ExpandType(Roots[i]->Pos, *dqResult->GetTypeAnn(), ctx))
            .TxIndex().Build(txIndex)
            .ResultIndex().Build(TStringBuilder() << resultIndex++)
        .Done().Ptr();
        // clang-format on

        resultBindings.push_back(resultBinding);
    }

    TVector<TExprNode::TPtr> allPhysicalStages;
    for (auto & stages : physicalStages) {
        allPhysicalStages.insert(allPhysicalStages.end(), stages.begin(), stages.end());
    }

    TExprNode::TPtr mainTx;

    auto phyTxSettings = GetPhysicalTxSettings();
    if (phyTxSettings.WithEffects) {
        // clang-format off
        // Build PhysicalTx
        mainTx = Build<TKqpPhysicalTx>(ctx, Roots[0]->Pos)
                .Stages()
                    .Add(allPhysicalStages)
                .Build()
                .Results().Build()
                .ParamBindings()
                    .Add(paramBindingsAllRoots)
                .Build()
                .Settings(phyTxSettings.BuildNode(ctx, Roots[0]->Pos))
            .Done().Ptr();
        // clang-format on
    }
    else {
        // clang-format off
        // Build PhysicalTx
        mainTx = Build<TKqpPhysicalTx>(ctx, Roots[0]->Pos)
                .Stages()
                    .Add(allPhysicalStages)
                .Build()
                .Results()
                    .Add(dqResults)
                .Build()
                .ParamBindings()
                    .Add(paramBindingsAllRoots)
                .Build()
                .Settings(phyTxSettings.BuildNode(ctx, Roots[0]->Pos))
            .Done().Ptr();
        // clang-format on
    }
    phyTxs.emplace_back(mainTx);


    auto phyQuerySettings = GetPhysicalQuerySettings();
    TExprNode::TPtr phyQuery;

    // Build Physical query
    if (phyTxSettings.WithEffects) {
        // clang-format off
        phyQuery = Build<TKqpPhysicalQuery>(ctx, Roots[0]->Pos)
            .Transactions()
                .Add(phyTxs)
            .Build()
            .Results().Build()
            .Settings(phyQuerySettings.BuildNode(ctx, Roots[0]->Pos))
        .Done().Ptr();
        // clang-format on
    } else {
        // clang-format off
        phyQuery = Build<TKqpPhysicalQuery>(ctx, Roots[0]->Pos)
            .Transactions()
                .Add(phyTxs)
            .Build()
            .Results()
                .Add(resultBindings)
            .Build()
            .Settings(phyQuerySettings.BuildNode(ctx, Roots[0]->Pos))
        .Done().Ptr();
        // clang-format on
    }

    TVector<TCoAtom> queryColumnAtomList;

    for (const auto& column : Roots[Roots.size()-1]->QueryColumns) {
        queryColumnAtomList.push_back(Build<TCoAtom>(ctx, Roots[Roots.size()-1]->Pos).Value(column).Done());
    }
    auto queryColumns = Build<TCoAtomList>(ctx, Roots[Roots.size()-1]->Pos).Add(queryColumnAtomList).Done().Ptr();

    TVector<TExprNode::TPtr> listElements;
    if (RBOCtx.EmptyPreamble) {
        listElements.push_back(ctx.NewList(Roots[0]->Pos, {}));
    }

    if (!phyTxSettings.WithEffects) {
        listElements.push_back(ctx.NewList(Roots[0]->Pos, {phyQuery, queryColumns}));
    } else {
        listElements.push_back(ctx.NewList(Roots[0]->Pos, {phyQuery}));
    }

    return ctx.NewList(Roots[0]->Pos, std::move(listElements));
}

TKqpPhyQuerySettings TPhysicalQueryBuilder::GetPhysicalQuerySettings() const {
    auto& kqpCtx = RBOCtx.KqpCtx;
    TKqpPhyQuerySettings querySettings;
    switch (kqpCtx.QueryCtx->Type) {
        case EKikimrQueryType::Dml: {
            querySettings.Type = EPhysicalQueryType::Data;
            break;
        }
        case EKikimrQueryType::Query: {
            querySettings.Type = EPhysicalQueryType::GenericQuery;
            break;
        }
        case EKikimrQueryType::Scan: {
            querySettings.Type = EPhysicalQueryType::Scan;
            break;
        }
        default: {
            // Should fallback to old pipeline.
            YQL_ENSURE(false, "Unsupported query type for NEW RBO " << kqpCtx.QueryCtx->Type);
        }
    }
    return querySettings;
}

TKqpPhyTxSettings TPhysicalQueryBuilder::GetPhysicalTxSettings() const {
    auto& kqpCtx = RBOCtx.KqpCtx;
    TKqpPhyTxSettings txSettings;
    bool withEffects = false;
    for (auto & root : Roots) {
        withEffects |= root->PlanProps.WithEffects;
    }
    txSettings.WithEffects = withEffects;

    switch (kqpCtx.QueryCtx->Type) {
        case EKikimrQueryType::Dml: {
            txSettings.Type = EPhysicalTxType::Compute;
            break;
        }
        case EKikimrQueryType::Query: {
            txSettings.Type = EPhysicalTxType::Generic;
            break;
        }
        case EKikimrQueryType::Scan: {
            txSettings.Type = EPhysicalTxType::Scan;
            break;
        }
        default: {
            YQL_ENSURE(false, "Unsupported tx type for NEW RBO " << kqpCtx.QueryCtx->Type);
        }
    }
    return txSettings;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildDqPhyStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args,
                                                       TExprNode::TPtr physicalStageBody, NNodes::TCoNameValueTupleList&& settings, TExprContext& ctx,
                                                       TPositionHandle pos) const {
    // clang-format off
    return Build<TDqPhyStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
        .Build()
        .Program()
            .Args(args)
            .Body(physicalStageBody)
        .Build()
        .Settings(settings)
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildDqPhySinkStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args,
                                                       TExprNode::TPtr physicalStageBody, NNodes::TCoNameValueTupleList&& settings, 
                                                       const TExprNode::TPtr& sinkSettings, TExprContext& ctx, TPositionHandle pos) const {

    TVector<TExprNode::TPtr> outputs;
    auto dataSink = ctx.NewCallable(pos, "DataSink", {ctx.NewAtom(pos, "KqpTableSink"), ctx.NewAtom(pos, "db")});
    outputs.push_back(Build<TDqSink>(ctx, pos)
                        .Index().Value("0").Build()
                        .DataSink(dataSink)
                        .Settings(sinkSettings)
                        .Done().Ptr()
    );

    // clang-format off
    return Build<TDqPhyStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
        .Build()
        .Program()
            .Args(args)
            .Body(physicalStageBody)
        .Build()
        .Settings(settings)
        .Outputs()
            .Add(outputs)
        .Build()
    .Done().Ptr();
    // clang-format on
}

void TPhysicalQueryBuilder::TopologicalSort(TDqPhyStage& dqStage, TVector<TExprNode::TPtr>& result, THashSet<const TExprNode*>& visited) const {
    visited.insert(dqStage.Raw());

    for (const auto& item : dqStage.Inputs()) {
        auto maybeConnection = item.Maybe<TDqConnection>();
        // DataSource stage as input.
        if (!maybeConnection) {
            continue;
        }

        TDqPhyStage inputStage = maybeConnection.Cast().Output().Stage().Cast<TDqPhyStage>();
        if (!visited.contains(inputStage.Raw())) {
            TopologicalSort(inputStage, result, visited);
        }
    }

    result.push_back(dqStage.Ptr());
}

void TPhysicalQueryBuilder::TopologicalSort(TDqPhyStage&& dqStage, TVector<TExprNode::TPtr>& result) const {
    THashSet<const TExprNode*> visited;
    TopologicalSort(dqStage, result, visited);
}

void TPhysicalQueryBuilder::KeepTypeAnnotationForStageAndFirstLevelChilds(TDqPhyStage& newStage, const TDqPhyStage& oldStage) const {
    Y_ENSURE(oldStage.Ref().GetTypeAnn());
    Y_ENSURE(oldStage.Inputs().Size() == newStage.Inputs().Size());

    newStage.MutableRef().SetTypeAnn(oldStage.Ref().GetTypeAnn());
    for (ui32 i = 0; i < newStage.Inputs().Size(); ++i) {
        newStage.Inputs().Item(i).MutableRef().SetTypeAnn(oldStage.Inputs().Item(i).Ref().GetTypeAnn());
        newStage.Program().Args().Arg(i).MutableRef().SetTypeAnn(oldStage.Program().Args().Arg(i).Ref().GetTypeAnn());
        if (newStage.Inputs().Item(i).Maybe<TDqConnection>()) {
            newStage.Inputs().Item(i).Cast<TDqConnection>().Output().Stage().MutableRef().SetTypeAnn(
                oldStage.Inputs().Item(i).Cast<TDqConnection>().Output().Stage().Ref().GetTypeAnn());
        }
    }
}

TVector<TExprNode::TPtr> TPhysicalQueryBuilder::PreparePhysicalStages(TVector<TExprNode::TPtr>&& physicalStages, bool enableWideChannels) {
    Y_ENSURE(physicalStages.size());
    auto root = physicalStages.back();
    if (!root->GetTypeAnn() && enableWideChannels) {
        TypeAnnotate(root);
    }
    auto& ctx = RBOCtx.ExprCtx;

    TNodeOnNodeOwnedMap replaces;
    TExprNode::TPtr rootStage;
    for (auto& stage : physicalStages) {
        auto dqPhyStage = TDqPhyStage(stage);
        // clang-format off
        auto newStage = Build<TDqPhyStage>(ctx, stage->Pos())
            .Inputs(ctx.ReplaceNodes(dqPhyStage.Inputs().Ptr(), replaces))
            .Program(dqPhyStage.Program())
            .Settings(dqPhyStage.Settings())
            .Outputs(dqPhyStage.Outputs())
        .Done().Ptr();
        // clang-format on

        // We don't need to run type annotation when wide channels is off.
        if (enableWideChannels) {
            TypeAnnotate(newStage);
            rootStage = NYql::NDq::RebuildStageInputsAsWide(TDqPhyStage(newStage), ctx).Ptr();
        } else {
            rootStage = newStage;
        }

        replaces[dqPhyStage.Raw()] = rootStage;
    }

    TypeAnnotate(rootStage);
    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical stages] " << KqpExprToPrettyString(TExprBase(rootStage), ctx);

    TVector<TExprNode::TPtr> stagesTopSorted;
    TopologicalSort(TDqPhyStage(rootStage), stagesTopSorted);
    return stagesTopSorted;
}

// The idea was taken from kqp_opt_peephole with some changes.
// This function assumes that stages already sorted in topological orders.
TVector<TExprNode::TPtr> TPhysicalQueryBuilder::PeepHoleOptimizePhysicalStages(TVector<TExprNode::TPtr>&& physicalStages) {
    Y_ENSURE(physicalStages.size());
    auto root = physicalStages.back();
    // Type is required to wrap stage lambda to `KqpProgram`
    if (!root->GetTypeAnn()) {
        TypeAnnotate(root);
    }
    auto& ctx = RBOCtx.ExprCtx;

    TNodeOnNodeOwnedMap programsMap;
    for (auto& stage : physicalStages) {
        TNodeOnNodeOwnedMap argReplaces;
        auto dqPhyStage = TDqPhyStage(stage);
        auto program = dqPhyStage.Program();

        const bool isSuitableHashShuffleConnections = IsSuitableToPropagateWideBlocksThroughHashShuffleConnections(dqPhyStage);
        TVector<TExprNode::TPtr> stageArgs;
        for (ui32 i = 0, e = dqPhyStage.Inputs().Size(); i < e; ++i) {
            auto stageArg = program.Args().Arg(i);
            auto newStageArg = stageArg.Ptr();
            auto input = dqPhyStage.Inputs().Item(i);

            if (auto maybeConnection = input.Maybe<TDqConnection>();
                isSuitableHashShuffleConnections && maybeConnection && IsSuitableToPropagateWideBlocksThroughConnection(maybeConnection.Cast().Output())) {
                auto inputStage = maybeConnection.Cast().Output().Stage();
                auto program = TCoLambda(programsMap.at(inputStage.Program().Raw()));
                auto body = program.Body().Ptr();

                // If the body of input stage is `FromBlocks` propagate it through connection.
                if (body->IsCallable("WideFromBlocks")) {
                    body = body->ChildPtr(0);
                    const TTypeAnnotationNode* blockType = body->GetTypeAnn();
                    Y_ENSURE(blockType);

                    // New arg for the current stage has a `Blocks` type, so we need to add `FromBlocks` here.
                    // clang-format off
                    auto fromBlocks = Build<TCoWideFromBlocks>(ctx, stageArg.Pos())
                        .Input<TCoArgument>()
                            .Name("new_stage_arg")
                        .Build()
                    .Done();
                    // clang-format on

                    // Update a stage arg.
                    newStageArg = fromBlocks.Input().Ptr();
                    // Replace an original arg with arg wrapped to `FromBlocks`.
                    argReplaces[stageArg.Raw()] = fromBlocks.Ptr();

                    // clang-format off
                    auto newProgram = Build<TCoLambda>(ctx, program.Pos())
                        .Args(program.Args())
                        .Body(body)
                    .Done().Ptr();
                    // clang-format on

                    // Update the type to `Blocks`.
                    newStageArg->SetTypeAnn(blockType);
                    // Update map, since stage body was updated.
                    programsMap[inputStage.Program().Raw()] = newProgram;
                }
            }
            stageArgs.push_back(newStageArg);
        }

        // clang-format off
        auto newProgram = Build<TCoLambda>(ctx, stage->Pos())
            .Args(stageArgs)
            .Body(ctx.ReplaceNodes(program.Body().Ptr(), argReplaces))
        .Done().Ptr();
        // clang-format on

        TVector<const TTypeAnnotationNode*> argsType;
        for (const auto& arg : stageArgs) {
            const TTypeAnnotationNode* argTypeAnn = arg->GetTypeAnn();
            Y_ENSURE(argTypeAnn);
            argsType.push_back(argTypeAnn);
        }

        newProgram = PeepHoleOptimize(newProgram, argsType);
        Y_ENSURE(newProgram);
        // Collect program after peephole.
        programsMap[program.Raw()] = newProgram;
    }

    auto rootStage = ctx.ReplaceNodes(std::move(physicalStages.back()), programsMap);
    TVector<TExprNode::TPtr> stagesTopSorted;
    TopologicalSort(TDqPhyStage(rootStage), stagesTopSorted);
    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO After peephole] " << KqpExprToPrettyString(TExprBase(stagesTopSorted.back()), ctx);
    return stagesTopSorted;
}

bool TPhysicalQueryBuilder::IsSuitableToPropagateWideBlocksThroughHashShuffleConnections(const TDqPhyStage& stage) const {
    // Workaround to mitigate https://github.com/ydb-platform/ydb/issues/20440
    // do not mix scalar and block HashShuffle HashV1 connections,
    // if we find any scalar connection then don't propagate blocks through other connections.
    ui32 scalarHashShuffleCount = 0;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        auto connection = stage.Inputs().Item(i).Maybe<TDqCnHashShuffle>();
        if (connection) {
            auto hashFuncType = RBOCtx.KqpCtx.Config->GetDqDefaultHashShuffleFuncType();
            if (connection.Cast().HashFunc().IsValid()) {
                hashFuncType = FromString<NDq::EHashShuffleFuncType>(connection.Cast().HashFunc().Cast().StringValue());
            }
            scalarHashShuffleCount += (hashFuncType == NDq::EHashShuffleFuncType::HashV1);
        }
    }
    return scalarHashShuffleCount <= 1;
}

bool TPhysicalQueryBuilder::IsCompatibleWithBlocks(const TStructExprType& type, TPositionHandle pos) const {
    TVector<const TTypeAnnotationNode*> types;
    for (const auto& item : type.GetItems()) {
        types.emplace_back(item->GetItemType());
    }
    auto& ctx = RBOCtx.ExprCtx;

    const auto resolveStatus = RBOCtx.TypeCtx.ArrowResolver->AreTypesSupported(ctx.GetPosition(pos), types, ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    return resolveStatus == IArrowResolver::OK;
}

bool TPhysicalQueryBuilder::IsSuitableToPropagateWideBlocksThroughConnection(const TDqOutput& output) const {
    if (RBOCtx.KqpCtx.Config->GetBlockChannelsMode() != NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO) {
        return false;
    }

    auto stageSettings = NYql::NDq::TDqStageSettings::Parse(output.Stage());
    return stageSettings.WideChannels && stageSettings.OutputNarrowType &&
           IsCompatibleWithBlocks(*stageSettings.OutputNarrowType, output.Stage().Program().Pos());
}

TVector<const TTypeAnnotationNode*> TPhysicalQueryBuilder::GetArgsType(TExprNode::TPtr input) const {
    Y_ENSURE(input->IsLambda());
    auto lambda = TCoLambda(input);

    TVector<const TTypeAnnotationNode*> argsTypes;
    for (const auto& arg : lambda.Args()) {
        const TTypeAnnotationNode* argTypeAnn = arg.Ptr()->GetTypeAnn();
        Y_ENSURE(argTypeAnn);
        argsTypes.push_back(argTypeAnn);
    }

    return argsTypes;
}

TExprNode::TPtr TPhysicalQueryBuilder::TypeAnnotateProgram(TExprNode::TPtr input, const TVector<const TTypeAnnotationNode*>& argsType) {
    auto lambda = TCoLambda(input);
    auto& ctx = RBOCtx.ExprCtx;
    // clang-format off
    auto program = Build<TKqpProgram>(ctx, input->Pos())
        .Lambda(ctx.DeepCopyLambda(*input.Get()))
        .ArgsType(ExpandType(input->Pos(), *ctx.MakeType<TTupleExprType>(argsType), ctx))
    .Done().Ptr();
    // clang-format on

    TypeAnnotate(program);
    return TKqpProgram(program).Lambda().Ptr();
}

TExprNode::TPtr TPhysicalQueryBuilder::PeepHoleOptimize(TExprNode::TPtr input, const TVector<const TTypeAnnotationNode*>& argsType) const {
    auto lambda = TCoLambda(input);
    auto& ctx = RBOCtx.ExprCtx;

    const bool withFinalStageRules = true;
    // clang-format off
    auto program = Build<TKqpProgram>(ctx, input->Pos())
        .Lambda(ctx.DeepCopyLambda(*input.Get()))
        .ArgsType(ExpandType(input->Pos(), *ctx.MakeType<TTupleExprType>(argsType), ctx))
    .Done();
    // clang-format on

    TExprNode::TPtr newProgram;
    auto status =
        ::NKikimr::NKqp::NOpt::PeepHoleOptimize(program, newProgram, ctx, RBOCtx.TypeCtx, RBOCtx.KqpCtx.Config, false, withFinalStageRules, {});
    if (status != IGraphTransformer::TStatus::Ok) {
        ctx.AddError(TIssue(ctx.GetPosition(program.Pos()), "Peephole optimization failed for stage in NEW RBO"));
        return nullptr;
    }

    return TKqpProgram(newProgram).Lambda().Ptr();
}

void TPhysicalQueryBuilder::TypeAnnotate(TExprNode::TPtr& input) {
    RBOCtx.TypeAnnTransformer.Rewind();
    TExprNode::TPtr output;
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = RBOCtx.TypeAnnTransformer.Transform(input, output, RBOCtx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    if (status != IGraphTransformer::TStatus::Ok) {
        RBOCtx.ExprCtx.AddError(TIssue(RBOCtx.ExprCtx.GetPosition(input->Pos()), "Type inference failed for stage in NEW RBO"));
        Y_ENSURE(false);
    }

    input = output;
}

} // namespace NKikimr::NKqp
