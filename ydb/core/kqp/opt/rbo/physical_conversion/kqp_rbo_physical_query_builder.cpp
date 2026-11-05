#include "kqp_rbo_physical_query_builder.h"
#include "kqp_rbo_compatibility.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_convertion_utils.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_yql_ast_trace.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/core/yql_expr_csee.h>
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

TPhysicalQueryBuilder::TPhysicalQueryBuilder(const TVector<TIntrusivePtr<TOpRoot>>& roots,
    TVector<TStageGraph>&& graphs, TVector<THashMap<ui32, TExprNode::TPtr>>&& stages, 
    TVector<THashMap<ui32, TVector<TExprNode::TPtr>>>&& stageArgs,
    TVector<THashMap<ui32, TPositionHandle>>&& stagePos, TVector<TPhysicalNames>&& names, TRBOContext& rboCtx)
    : Roots(roots)
    , Graphs(std::move(graphs))
    , Stages(std::move(stages))
    , StageArgs(std::move(stageArgs))
    , StagePos(std::move(stagePos))
    , Names(std::move(names))
    , RBOCtx(rboCtx)
{
    Materialize.resize(roots.size());
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery() {
    TVector<TVector<TExprNode::TPtr>> allPhyStages;

    auto* trace = RBOCtx.GetCompileTrace();
    const auto traced = [&](const char* name, auto&& func) {
        TCompileTraceScope traceScope(trace, TCompileTraceLevels::Step, name);
        return func();
    };

    for (size_t i=0; i<Roots.size(); i++) {
        auto phyStages = traced("Build stage graph", [&] { return BuildPhysicalStageGraph(i); });
        SubmitPhysicalStagesTrace(RBOCtx, "After physical stage graph build", phyStages);
        const bool fullPeephole = RBOCtx.KqpCtx.Config->GetEnableNewRBOPhysicalStagePeephole();
        phyStages = traced("Prepare physical stages", [&] { return PreparePhysicalStages(std::move(phyStages), fullPeephole); });
        SubmitPhysicalStagesTrace(RBOCtx, "After physical stage preparation", phyStages);
        if (!fullPeephole) {
            phyStages = traced("Lower incompatible callables", [&] { return LowerPhysicalStageCompatibility(std::move(phyStages)); });
            SubmitPhysicalStagesTrace(RBOCtx, "After compatibility lowering", phyStages);
        } else {
            phyStages = traced("Peephole physical stages", [&] { return PeepHoleOptimizePhysicalStages(std::move(phyStages)); });
            SubmitPhysicalStagesTrace(RBOCtx, "After physical peephole", phyStages);
        }
        allPhyStages.push_back(std::move(phyStages));
    }
    auto physicalQuery = traced("Build physical query", [&] { return BuildPhysicalQuery(std::move(allPhyStages)); });
    TypeAnnotate(physicalQuery);
    // Physical rewriting replaces lambdas; refresh their non-owning scope
    // pointers before the MiniKQL compiler uses them for memoization.
    Y_ENSURE(UpdateCompletness(physicalQuery, physicalQuery, RBOCtx.ExprCtx) == IGraphTransformer::TStatus::Ok);
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
    const auto stages = graph.TopologicalOrder();
    auto& ctx = RBOCtx.ExprCtx;

    THashMap<ui32, TExprNode::TPtr> finalizedStages;
    for (const auto* graphStage : stages) {
        const auto id = graphStage->Id();
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputConnections;
        for (const auto* edge : graph.IncomingEdges(*graphStage)) {
            const auto inputStageId = edge->Producer().Id();
            const auto& connection = edge->Connection();
            auto dqConnection = connection.BuildConnection(finalizedStages.at(inputStageId),
                StagePos[rootIdx].at(inputStageId), ctx, Names[rootIdx]);
            inputConnections.push_back(dqConnection);
        }

        TExprNode::TPtr stage;
        if ((graphStage->StorageType() == NYql::EStorageType::RowStorage)) {
            stage = Stages[rootIdx].at(id);
            // Computed ranges are materialized in a separate transaction. A literal range is taken by the source as is.
            auto rowSettingsPtr = FindNode(stage, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TKqpReadRangesSourceSettings>(node); });
            if (rowSettingsPtr) {
                auto rowSettings = TExprBase(rowSettingsPtr).Cast<TKqpReadRangesSourceSettings>();
                if (!rowSettings.RangesExpr().Maybe<TCoVoid>() && !rowSettings.RangesExpr().Maybe<TKqlKeyRange>()) {
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
            if ((graphStage->StorageType() != NYql::EStorageType::ColumnStorage)) {
                stageInputConnections = inputConnections;
                stageInputArgs = StageArgs[rootIdx].at(id);
            }

            auto stageGUID = graphStage->Guid();
            if (graphStage->IsSink()) {
                stage = BuildDqPhySinkStage(stageInputConnections, stageInputArgs, Stages[rootIdx].at(id), NYql::NDq::TDqStageSettings().New(stageGUID).BuildNode(ctx, StagePos[rootIdx].at(id)),
                                    graphStage->SinkSettings(), CastOperator<TOpTableEffect>(*Roots[rootIdx]->GetInput()), ctx, StagePos[rootIdx].at(id));
            }
            else {
                stage = BuildDqPhyStage(stageInputConnections, stageInputArgs, Stages[rootIdx].at(id), NYql::NDq::TDqStageSettings().New(stageGUID).BuildNode(ctx, StagePos[rootIdx].at(id)),
                                    ctx, StagePos[rootIdx].at(id));
            }
            phyStages.emplace_back(stage);
            YQL_CLOG(TRACE, CoreDq) << "Added stage " << stage->UniqueId();
        }

        if ((graphStage->StorageType() == NYql::EStorageType::ColumnStorage)) {
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

    // The result stage is explicit, including a lone row source which does
    // not contribute an entry to phyStages.
    auto maybeFinalStage = finalizedStages.at(*Roots[rootIdx]->GetInput()->Props.StageId);
    if (Roots[rootIdx]->PlanProps.WithReturning) {
        // ReturningSink exposes storage column names. Introduce the returned
        // IU definitions at this boundary before ordinary result projection.
        const auto& effect = CastOperator<TOpTableEffect>(*Roots[rootIdx]->GetInput());
        auto connection = Build<TDqCnUnionAll>(ctx, effect.Pos)
            .Output().Stage(maybeFinalStage).Index().Build("0").Build().Done();
        auto arg = ctx.NewArgument(effect.Pos, "returned_rows");
        TVector<std::pair<TString, TString>> columns;
        for (const auto& [id, name] : effect.GetReturningColumns().Items()) {
            columns.emplace_back(name, Names[rootIdx].Get(id));
        }
        auto resultRows = NPhysicalConvertionUtils::BuildRenameMap(arg, columns, ctx, /*ordered=*/true);
        if (const auto& limit = Roots[rootIdx]->PlanProps.ReturningRowsLimit) {
            resultRows = Build<TCoTake>(ctx, effect.Pos).Input(resultRows).Count(limit).Done().Ptr();
        }
        maybeFinalStage = BuildDqPhyStage({connection.Ptr()}, {arg}, resultRows,
            NYql::NDq::TDqStageSettings().New().BuildNode(ctx, effect.Pos), ctx, effect.Pos);
        phyStages.push_back(maybeFinalStage);
    } else if (Roots[rootIdx]->PlanProps.WithEffects) {
        // A sink consumes its stream and exposes no result output. In
        // particular, do not add a union/result stage on its output zero.
        Y_ENSURE(!phyStages.empty() && phyStages.back() == maybeFinalStage);
        return phyStages;
    }
    const auto finalStage = GetFinalStage(maybeFinalStage);
    const bool needFinalNarrowing = NeedFinalNarrowing(rootIdx);
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

bool TPhysicalQueryBuilder::NeedFinalNarrowing(int rootIdx) {
    auto& root = *Roots[rootIdx];
    const auto& outputIUs = root.GetInput()->GetOutputIUs();
    if (outputIUs.Size() != root.GetColumns().Items().size()) {
        return true;
    }

    for (const auto& [id, label] : root.GetColumns().Items()) {
        if (!outputIUs.Contains(id) || Names[rootIdx].Get(id) != label) {
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
            .Settings(NYql::NDq::TDqStageSettings().New().BuildNode(ctx, stage->Pos()))
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

    TVector<std::pair<TString, TString>> finalColumns;
    for (const auto& [id, label] : Roots[rootIdx]->GetColumns().Items()) {
        finalColumns.emplace_back(Names[rootIdx].Get(id), label);
    }

    const auto narrowBody =
        NPhysicalConvertionUtils::BuildRenameMap(dqStage.Program().Body().Ptr(), finalColumns, ctx, /*ordered=*/true);

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

// Collects param bindings walking from root stage.
TVector<TKqpParamBinding> TPhysicalQueryBuilder::CollectParamBindings(int rootIdx, const TExprNode::TPtr& rootStage) {
    auto& ctx = RBOCtx.ExprCtx;
    auto pos = Roots[rootIdx]->Pos;

    TVector<TKqpParamBinding> paramBindings;
    THashSet<TString> paramsCollected;
    const auto params = FindNodes(rootStage, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TCoParameter>(node); });
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

    return paramBindings;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery(TVector<TVector<TExprNode::TPtr>>&& physicalStages) {
    Y_ENSURE(physicalStages.size() == Roots.size() && !Roots.empty());
    auto& ctx = RBOCtx.ExprCtx;
    const auto pos = Roots.front()->Pos;
    TVector<TExprNode::TPtr> transactions;
    TVector<TExprNode::TPtr> resultBindings;
    TVector<std::pair<ui32, TExprNode::TPtr>> orderedResults;

    // A query block's reads share a snapshot. Its effects also share a
    // snapshot, with sink priorities preserving their write order. Only cross
    // a transaction boundary when the frontend starts another block/phase.
    for (size_t begin = 0; begin < Roots.size();) {
        size_t end = begin + 1;
        while (end < Roots.size() && Roots[end]->PlanProps.PhysicalTxGroup == Roots[begin]->PlanProps.PhysicalTxGroup) {
            ++end;
        }
        TVector<TKqpParamBinding> paramBindings;
        THashSet<TString> paramNames;
        TVector<TExprNode::TPtr> materializeStages;
        TVector<TExprNode::TPtr> materializeResults;
        TVector<TKqpParamBinding> materializeParams;
        THashSet<TString> materializeParamNames;
        for (size_t i = begin; i < end; ++i) {
            for (const auto& parameter : CollectParamBindings(i, physicalStages[i].back())) {
                if (paramNames.insert(parameter.Name().StringValue()).second) {
                    paramBindings.push_back(parameter);
                }
            }
            for (const auto& [paramNode, resultNode] : Materialize[i]) {
                const auto param = TCoParameter(paramNode);
                const auto result = TDqCnValue(resultNode);
                auto binding = Build<TKqpTxResultBinding>(ctx, pos)
                    .Type(ExpandType(pos, *resultNode->GetTypeAnn(), ctx))
                    .TxIndex().Build(ToString(transactions.size()))
                    .ResultIndex().Build(ToString(materializeResults.size()))
                    .Done();
                paramBindings.push_back(Build<TKqpParamBinding>(ctx, pos)
                    .Name(param.Name()).Binding(binding.Ptr()).Done());
                for (const auto& parameter : CollectParamBindings(i, result.Output().Stage().Ptr())) {
                    if (materializeParamNames.insert(parameter.Name().StringValue()).second) {
                        materializeParams.push_back(parameter);
                    }
                }
                materializeStages.push_back(result.Output().Stage().Ptr());
                materializeResults.push_back(resultNode);
            }
        }
        if (!materializeResults.empty()) {
            TKqpPhyTxSettings settings;
            settings.Type = EPhysicalTxType::Compute;
            transactions.push_back(Build<TKqpPhysicalTx>(ctx, pos)
                .Stages().Add(materializeStages).Build()
                .Results().Add(materializeResults).Build()
                .ParamBindings().Add(materializeParams).Build()
                .Settings(settings.BuildNode(ctx, pos)).Done().Ptr());
        }

        TVector<TExprNode::TPtr> stages;
        TVector<TExprNode::TPtr> results;
        const auto settings = GetPhysicalTxSettings(begin);
        for (size_t i = begin; i < end; ++i) {
            Y_ENSURE(Roots[i]->PlanProps.WithEffects == settings.WithEffects);
            stages.insert(stages.end(), physicalStages[i].begin(), physicalStages[i].end());
            if (settings.WithEffects && !Roots[i]->PlanProps.WithReturning) {
                continue;
            }
            TVector<TCoAtom> columns;
            // Export the resolved projection order for SELECT and RETURNING.
            // Original query hints can be empty even for an ordered projection;
            // exporting them would fall back to alphabetical struct-field order.
            for (const auto& [id, label] : Roots[i]->GetColumns().Items()) {
                columns.push_back(Build<TCoAtom>(ctx, pos).Value(label).Done());
            }
            auto result = Build<TDqCnResult>(ctx, pos)
                .Output().Stage(physicalStages[i].back()).Index().Build("0").Build()
                .ColumnHints().Add(columns).Build().Done().Ptr();
            TypeAnnotate(result);
            orderedResults.emplace_back(Roots[i]->PlanProps.ResultIndex.value_or(orderedResults.size()),
                Build<TKqpTxResultBinding>(ctx, pos)
                    .Type(ExpandType(pos, *result->GetTypeAnn(), ctx))
                    .TxIndex().Build(ToString(transactions.size()))
                    .ResultIndex().Build(ToString(results.size())).Done().Ptr());
            results.push_back(result);
        }
        transactions.push_back(Build<TKqpPhysicalTx>(ctx, pos)
            .Stages().Add(stages).Build()
            .Results().Add(results).Build()
            .ParamBindings().Add(paramBindings).Build()
            .Settings(settings.BuildNode(ctx, pos)).Done().Ptr());
        begin = end;
    }
    std::sort(orderedResults.begin(), orderedResults.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    for (auto& [index, binding] : orderedResults) {
        resultBindings.push_back(std::move(binding));
    }
    return Build<TKqpPhysicalQuery>(ctx, pos)
        .Transactions().Add(transactions).Build()
        .Results().Add(resultBindings).Build()
        .Settings(GetPhysicalQuerySettings().BuildNode(ctx, pos)).Done().Ptr();
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

TKqpPhyTxSettings TPhysicalQueryBuilder::GetPhysicalTxSettings(size_t rootIdx) const {
    auto& kqpCtx = RBOCtx.KqpCtx;
    TKqpPhyTxSettings txSettings;
    const bool withEffects = Roots[rootIdx]->PlanProps.WithEffects;
    txSettings.WithEffects = withEffects;

    switch (kqpCtx.QueryCtx->Type) {
        case EKikimrQueryType::Dml: {
            txSettings.Type = withEffects ? EPhysicalTxType::Data : EPhysicalTxType::Compute;
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
                                                       const TExprNode::TPtr& sinkSettings, const TOpTableEffect& effect,
                                                       TExprContext& ctx, TPositionHandle pos) {
    if (!effect.GetReturningColumns().Items().empty()) {
        auto stage = BuildDqPhyStage(inputs, args, physicalStageBody, std::move(settings), ctx, pos);
        TypeAnnotate(stage);
        TVector<const TItemExprType*> items;
        const auto* row = effect.Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        for (const auto& [id, name] : effect.GetReturningColumns().Items()) {
            const auto* item = row->FindItemType(ctx.GetIndexAsString(id));
            Y_ENSURE(item);
            items.push_back(ctx.MakeType<TItemExprType>(name, item));
        }
        const auto transform = Build<TDqTransform>(ctx, pos)
            .Index().Build("0")
            .DataSink<TKqpTableSink>().Category().Build("KqpTableSink").Cluster().Build("db").Build()
            .Type().Build("ReturningSink")
            .InputType(ExpandType(pos, GetSeqItemType(*TDqPhyStage(stage).Program().Body().Ref().GetTypeAnn()), ctx))
            .OutputType(ExpandType(pos, *ctx.MakeType<TStructExprType>(items), ctx))
            .Settings(sinkSettings)
            .Done();
        // ReturningSink performs the write and emits its result. A separate
        // sink or a copy of the input stream would duplicate effects or expose
        // rows that were rejected by a conflict/constraint check.
        return Build<TDqPhyStage>(ctx, pos).InitFrom(TDqPhyStage(stage))
            .Outputs().Add(transform).Build().Done().Ptr();
    }

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

        {
            TCompileTraceScope traceScope(RBOCtx.GetCompileTrace(), TCompileTraceLevels::Detail, "Peephole stage");
            newProgram = PeepHoleOptimize(newProgram, argsType);
            traceScope.SetSuccess(newProgram != nullptr);
        }
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
    TCompileTraceScope traceScope(RBOCtx.GetCompileTrace(), TCompileTraceLevels::Detail,
        "Annotate physical stage");
    RBOCtx.TypeAnnTransformer.Rewind();
    TExprNode::TPtr output;
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = RBOCtx.TypeAnnTransformer.Transform(input, output, RBOCtx.ExprCtx);
        input = output;
    } while (status == IGraphTransformer::TStatus::Repeat);

    if (status != IGraphTransformer::TStatus::Ok) {
        RBOCtx.ExprCtx.AddError(TIssue(RBOCtx.ExprCtx.GetPosition(input->Pos()), "Type inference failed for stage in NEW RBO"));
        Y_ENSURE(false);
    }

    input = output;
}

} // namespace NKikimr::NKqp
