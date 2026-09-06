#include "kqp_rbo_transformer.h"
#include "kqp_operator.h"
#include "kqp_plan_conversion_utils.h"
#include "kqp_rbo_rules.h"
#include "traces/kqp_rbo_trace_output.h"
#include "verification/semantic_snapshot.h"

#include <ydb/core/kqp/host/kqp_transform.h>

#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/system/env.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

#include <memory>
#include <algorithm>
#include <optional>
#include <utility>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

// BuildKqlQuery preserves block order and each block's result order. Do not
// infer client-visible result slots from a recursive traversal of the plan.
TString CollectReadOnlyResultSlots(const TExprNode::TPtr& input, TStringBuf rootCallable,
    TVector<TExprNode::TPtr>& slots)
{
    if (!input->IsList()) {
        return "expected a query-block list";
    }
    THashSet<const TExprNode*> seen;
    THashSet<const TExprNode*> ownedNodes;
    for (const auto& block : input->Children()) {
        if (!block->IsList() || block->ChildrenSize() != 2 ||
            !block->Child(0)->IsList() || !block->Child(1)->IsList()) {
            return "malformed query block";
        }
        if (block->Child(1)->ChildrenSize()) {
            return "effects are not supported";
        }
        for (const auto& result : block->Child(0)->Children()) {
            if (!result->IsList() || result->ChildrenSize() != 2 || !result->Child(1)->IsList()) {
                return "malformed result slot";
            }
            if (!result->Child(0)->IsCallable(rootCallable)) {
                return TStringBuilder() << "expected result root " << rootCallable << ", found "
                    << result->Child(0)->Content() << "; result wrappers are not supported";
            }
            if (!seen.insert(result->Child(0)).second) {
                return "shared result roots are not supported";
            }
            for (const auto& owned : FindNodes(result->ChildPtr(0), [](const TExprNode::TPtr& node) {
                return (node->IsCallable() && node->Content().StartsWith("KqpOp")) ||
                    TKqpPhysicalTx::Match(node.Get()) || TDqPhyStage::Match(node.Get());
            })) {
                if (!ownedNodes.insert(owned.Get()).second) {
                    return "result roots share a relational plan or physical stage";
                }
            }
            slots.push_back(result->ChildPtr(0));
        }
    }
    return slots.empty() ? "query has no result slots" : TString();
}

class TRootSnapshotSink final : public IRBOSemanticSnapshotSink {
public:
    TRootSnapshotSink(IRBOSemanticSnapshotSink& sink, ui32 ordinal, ui32 count,
        std::optional<ui32> resultOrdinal)
        : Sink(sink), Ordinal(ordinal), Count(count), ResultOrdinal(resultOrdinal) {}

    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        result.RootOrdinal = Ordinal;
        result.RootCount = Count;
        result.ResultOrdinal = ResultOrdinal;
        result.ResultCount = ResultOrdinal ? Count : 0;
        Sink.OnSemanticSnapshot(std::move(result));
    }

    std::optional<ui64> GetTransformationPrefixTarget() const override {
        return Sink.GetTransformationPrefixTarget();
    }

private:
    IRBOSemanticSnapshotSink& Sink;
    const ui32 Ordinal;
    const ui32 Count;
    const std::optional<ui32> ResultOrdinal;
};

NJson::TJsonValue MakeNewRBOOptimizerStats(const NOpt::TKqpOptimizeContext& kqpCtx) {
    const auto& cboStats = kqpCtx.CBOStats;

    NJson::TJsonValue optimizerStats(NJson::EJsonValueType::JSON_MAP);
    optimizerStats["CBOTreesTotal"] = cboStats.TreesTotal;
    optimizerStats["CBOTreesOptimized"] = cboStats.TreesOptimized;
    return optimizerStats;
}

TExprNode::TPtr PushTakeIntoPlan(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& typeCtx) {
    Y_UNUSED(typeCtx);
    auto take = TCoTake(node);
    auto takeInput = take.Input();
    if (takeInput.Maybe<TCoUnordered>()) {
        takeInput = takeInput.Cast<TCoUnordered>().Input();
    }

    if (auto root = takeInput.Maybe<TKqpOpRoot>()) {
        // clang-format off
        return Build<TKqpOpRoot>(ctx, node->Pos())
            .Input<TKqpOpLimit>()
                .Input(root.Cast().Input())
                .Count(take.Count())
            .Build()
            .ColumnOrder(root.Cast().ColumnOrder())
        .Done().Ptr();
        // clang-format on
    } else {
        return node;
    }
}

TExprNode::TPtr PushUnorderedIntoPlan(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto root = TCoUnordered(node).Input().Maybe<TKqpOpRoot>();
    if (!root) {
        return node;
    }
    // Forget only the public result order, after the complete SELECT (including
    // Sort/Limit). The initial snapshot must carry this observation explicitly.
    TVector<TExprBase> columns;
    for (const auto& column : root.Cast().ColumnOrder()) {
        columns.emplace_back(Build<TKqpOpMapElementRename>(ctx, node->Pos())
            .Input(root.Cast().Input())
            .Variable(column)
            .From(column)
            .Done());
    }
    return Build<TKqpOpRoot>(ctx, node->Pos())
        .Input<TKqpOpMap>()
            .Input(root.Cast().Input())
            .MapElements().Add(columns).Build()
            .Project().Build("true")
            .Ordered().Build("false")
        .Build()
        .ColumnOrder(root.Cast().ColumnOrder())
        .Done().Ptr();
}

void CollectTopLevelSelects(TExprNode::TPtr input, THashSet<TExprNode*>& topLevelSelects, THashSet<TExprNode*>& visited) {
    if (visited.contains(input.Get())) {
        return;
    }

    if (input->IsCallable("KqpOpRoot")) {
        visited.insert(input.Get());
        return;
    }

    if (input->IsCallable("YqlSelect")) {
        topLevelSelects.insert(input.Get());
        visited.insert(input.Get());
        return;
    }
    for (auto c: input->Children()) {
        CollectTopLevelSelects(c, topLevelSelects, visited);
    }
    return;
}

bool IsRboTraceLogEnabled() {
    TMaybe<TString> htmlTracePath = TryGetEnv("NEW_RBO_LOG");
    return htmlTracePath.Defined() && !htmlTracePath->empty();
}

} // anonymous namespace

IGraphTransformer::TStatus TKqpRewriteSelectTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    if (KqpCtx.Config->OptFallbackToLegacyOptimizer.Get()) {
        Y_ENSURE(false, "Forced fallback to legacy optimizer");
    }
    
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);
    const bool needTraceAst = IsRboTraceLogEnabled();
    if (needTraceAst) {
        if (!RboTraceRewriteSelectStarted) {
            KqpCtx.RboTraceAstBeforeRewriteSelect = input;
            KqpCtx.RboTraceAstAfterRewriteSelect = nullptr;
            RboTraceRewriteSelectStarted = true;
        }
    } else {
        KqpCtx.RboTraceAstBeforeRewriteSelect = nullptr;
        KqpCtx.RboTraceAstAfterRewriteSelect = nullptr;
        RboTraceRewriteSelectStarted = false;
    }

    THashSet<TExprNode*> topLevelSelects;
    THashSet<TExprNode*> visited;

    CollectTopLevelSelects(input, topLevelSelects, visited);

    auto status = OptimizeExpr(
        output, output,
        [this, &topLevelSelects](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            
            // YQL AST rewriting
            if (TCoYqlSelect::Match(node.Get()) && topLevelSelects.contains(node.Get())) {
                THashMap<const TExprNode*, TExprNode::TPtr> translated;
                return RewriteSelect(node, ctx, TypeCtx, KqpCtx, UniqueSourceIdCounter, translated, true);
            } else if (TCoUnordered::Match(node.Get())) {
                return PushUnorderedIntoPlan(node, ctx);
            }  else if (TCoTake::Match(node.Get())) {
                return PushTakeIntoPlan(node, ctx, TypeCtx);
            } else {
                return node;
            }
        },
        ctx, settings);

    if (needTraceAst && status == TStatus::Ok) {
        KqpCtx.RboTraceAstAfterRewriteSelect = output;
        RboTraceRewriteSelectStarted = false;
    } else if (status == TStatus::Error) {
        RboTraceRewriteSelectStarted = false;
    }

    return status;
}

void TKqpRewriteSelectTransformer::Rewind() {
    RboTraceRewriteSelectStarted = false;
    KqpCtx.RboTraceAstBeforeRewriteSelect = nullptr;
    KqpCtx.RboTraceAstAfterRewriteSelect = nullptr;
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);
    OpRoots.clear();
    CMColumnsByTableName.clear();
    HistColumnsByTableName.clear();

    // At first step convert KqpOps to RBO Ops.
    auto status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            Y_UNUSED(ctx);
            if (TKqpOpRoot::Match(node.Get())) {
                auto root = PlanConverter(TypeCtx, ctx).ConvertRoot(node);
                root->ComputeParents();
                OpRoots.push_back({node, std::move(root)});
                return node;
            } else {
                return node;
            }
        },
        ctx, settings);

    if (status != TStatus::Ok) {
        return status;
    }

    if (OpRoots.empty()) {
        return TStatus::Ok;
    }

    TVector<TExprNode::TPtr> resultSlots;
    if (CollectReadOnlyResultSlots(input, "KqpOpRoot", resultSlots).empty() &&
        resultSlots.size() == OpRoots.size() &&
        std::all_of(OpRoots.begin(), OpRoots.end(), [&](const auto& root) {
            return std::find(resultSlots.begin(), resultSlots.end(), root.Source) != resultSlots.end();
        })) {
        for (auto& root : OpRoots) {
            const auto slot = std::find(resultSlots.begin(), resultSlots.end(), root.Source);
            root.ResultOrdinal = slot - resultSlots.begin();
        }
    }

    if (IsSuitableToRequestStatistics()) {
        // Async request for statistics.
        auto status = RequestColumnStatistics(ctx);
        if (status == TStatus::Async || status == TStatus::Error) {
            return status;
        }
    }

    // Continue optimizations without statistics.
    return ContinueOptimizations(input, output, ctx);
}

NThreading::TFuture<void> TKqpNewRBOTransformer::DoGetAsyncFuture(const TExprNode& input) {
    Y_UNUSED(input);
    return ColumnStatisticsReadiness;
}

bool TKqpNewRBOTransformer::IsSuitableToCollectStatistics(const TIntrusivePtr<IOperator>& op) const {
    return op->Props.Metadata.has_value();
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(const TIntrusivePtr<IOperator>& op) {
    if (MatchOperator<TOpFilter>(op)) {
        CollectTablesAndColumnsNames(CastOperator<TOpFilter>(op)->FilterExpr, op->Props);
    } else if (MatchOperator<TOpJoin>(op)) {
        // Fetching statistics for join cardinality correction.
        CollectJoinKeysColumns(CastOperator<TOpJoin>(op), op->Props);
    } else if (MatchOperator<TOpRead>(op)) {
        // Fetching statistics for filters already pushed down into the read.
        const auto read = CastOperator<TOpRead>(op);
        if (read->OriginalPredicate.has_value()) {
            CollectTablesAndColumnsNames(read->OriginalPredicate.value(), op->Props);
        }
    }
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(const TExpression& expr, const TPhysicalOpProps& props) {
    const auto& mapping = props.Metadata->ColumnLineage.Mapping;
    auto lambda = TCoLambda(expr.GetLambda());

    // Request only the statistic each filter predicate actually consumes during selectivity estimation: 
    // equality predicates probe the count-min sketch, while 
    // range/inequality predicates use the equi-width histogram.
    TPredicateSelectivityComputer computer(nullptr, true);
    computer.Compute(lambda.Body());

    using TUsedMember = TPredicateSelectivityComputer::TColumnStatisticsUsedMembers::TColumnStatisticsUsedMember;
    for (const auto& item : computer.GetColumnStatsUsedMembers().Data) {
        const auto it = mapping.find(TInfoUnit(item.Member.Name().StringValue()));
        if (it == mapping.end() || it->second.TableName == "") {
            continue;
        }
        const auto& tableName = it->second.TableName;
        const auto& colName = it->second.ColumnName;
        switch (item.PredicateType) {
            case TUsedMember::EEquality:
                CMColumnsByTableName[tableName].insert(colName);
                break;
            case TUsedMember::EInequality:
                HistColumnsByTableName[tableName].insert(colName);
                break;
        }
    }
}

void TKqpNewRBOTransformer::CollectJoinKeysColumns(const TIntrusivePtr<TOpJoin>& join, const TPhysicalOpProps& props) {
    const auto& mapping = props.Metadata->ColumnLineage.Mapping;

    // For join cardinality correction, only the equi-width histogram of both join-key columns are needed.
    auto requestHistogram = [&](const TInfoUnit& key) {
        const auto it = mapping.find(TInfoUnit(key.GetFullName()));
        if (it == mapping.end() || it->second.TableName == "") {
            return;
        }
        const auto& tableName = it->second.TableName;
        const auto& colName = it->second.ColumnName;
        HistColumnsByTableName[tableName].insert(colName);
    };

    for (const auto& [lhsKey, rhsKey] : join->JoinKeys) {
        requestHistogram(lhsKey);
        requestHistogram(rhsKey);
    }
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(TExprContext& ctx) {
    for (const auto& root : OpRoots) {
        TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), FuncRegistry);
        root.Plan->ComputePlanMetadata(rboCtx);
        for (const auto& it : *root.Plan) {
            if (IsSuitableToCollectStatistics(it.Current)) {
                CollectTablesAndColumnsNames(it.Current);
            }
        }
    }
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::RequestColumnStatistics(TExprContext& ctx) {
    CollectTablesAndColumnsNames(ctx);

    TVector<NThreading::TFuture<TColumnStatisticsResponse>> futures;
    AddStatRequest(ActorSystem, futures, Tables, Cluster, Database, TypeCtx, NStat::EStatType::COUNT_MIN_SKETCH, CMColumnsByTableName,
                   [](const NYql::TColumnStatistics& stats) { return !!stats.CountMinSketch; });
    AddStatRequest(ActorSystem, futures, Tables, Cluster, Database, TypeCtx, NStat::EStatType::EQ_WIDTH_HISTOGRAM, HistColumnsByTableName,
                   [](const NYql::TColumnStatistics& stats) { return !!stats.EqWidthHistogramEstimator; });

    if (futures.empty()) {
        return TStatus::Ok;
    }

    auto sharedState = std::make_shared<TColumnStatisticsSharedState>();
    ColumnStatisticsReadiness = NThreading::WaitAll(futures).Apply(
        [weakSharedState = std::weak_ptr{sharedState}, futures = std::move(futures)](const NThreading::TFuture<void>&) mutable {
            for (auto& fut : futures) {
                if (fut.HasException()) {
                    fut.TryRethrow();
                }

                auto newStats = fut.ExtractValue();
                auto sharedState = weakSharedState.lock();
                if (!sharedState) {
                    // parent already deleted, just return
                    return;
                }
                if (!sharedState->Response.has_value()) {
                    sharedState->Response = std::move(newStats);
                } else {
                    // merge statistics
                    for (const auto& [table, column2Stat] : newStats.ColumnStatisticsByTableName) {
                        auto& oldColumn2Stat = sharedState->Response->ColumnStatisticsByTableName[table];
                        for (const auto& [column, newStat] : column2Stat.Data) {
                            auto& oldStat = oldColumn2Stat.Data[column];
                            if (newStat.CountMinSketch) {
                                oldStat.CountMinSketch = newStat.CountMinSketch;
                            }
                            if (newStat.EqWidthHistogramEstimator) {
                                oldStat.EqWidthHistogramEstimator = newStat.EqWidthHistogramEstimator;
                            }
                            if (!newStat.Type.empty()) {
                                oldStat.Type = newStat.Type;
                            }
                            if (newStat.NumUniqueVals) {
                                oldStat.NumUniqueVals = newStat.NumUniqueVals;
                            }
                            if (newStat.HyperLogLog) {
                                oldStat.HyperLogLog = newStat.HyperLogLog;
                            }
                        }
                    }
                }
            }
        });

    SharedState = sharedState;
    return TStatus::Async;
}

bool TKqpNewRBOTransformer::IsSuitableToRequestStatistics() {
    // Currently just checking for a flag.
    return KqpCtx.Config->FeatureFlags.GetEnableColumnStatistics();
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::ContinueOptimizations(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);
    Y_ENSURE(!OpRoots.empty(), "NEW RBO roots are not initialized.");

    // Apply optimizations.
    auto status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (TKqpOpRoot::Match(node.Get())) {
                const auto root = std::find_if(OpRoots.begin(), OpRoots.end(), [&](const auto& candidate) {
                    return candidate.Source == node;
                });
                Y_ENSURE(root != OpRoots.end(), "NEW RBO root was not converted");
                TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), FuncRegistry);
                TRBOTraceOutput traceOutput(rboCtx);
                const auto semanticSnapshotSink = TransformCtx->RBOSemanticSnapshotSink;
                std::optional<TRootSnapshotSink> rootSink;
                if (semanticSnapshotSink) {
                    rootSink.emplace(*semanticSnapshotSink, root - OpRoots.begin(), OpRoots.size(), root->ResultOrdinal);
                }
                auto output = RBO.Optimize(*root->Plan, rboCtx, rootSink ? &*rootSink : nullptr);
                traceOutput.Flush();
                AddPlans(rboCtx.ExecutionJson, rboCtx.ExplainJson);
                return output;
            } else {
                return node;
            }
        },
        ctx, settings);

    return status;
}

void TKqpNewRBOTransformer::ApplyColumnStatistics() {
    Y_ENSURE(ColumnStatisticsReadiness.IsReady());
    if (!SharedState->Response->Issues().Empty()) {
        TStringStream ss;
        SharedState->Response->Issues().PrintTo(ss);
        YQL_CLOG(TRACE, ProviderKikimr) << "Can't load columns statistics for request: " << ss.Str();
    } else {
        for (auto&& [tableName, columnStatistics] : SharedState->Response->ColumnStatisticsByTableName) {
            TypeCtx.ColumnStatisticsByTableName.insert({std::move(tableName), new NYql::TOptimizerStatistics::TColumnStatMap(std::move(columnStatistics))});
        }
    }
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    ApplyColumnStatistics();
    return ContinueOptimizations(input, output, ctx);
}

void TKqpNewRBOTransformer::AddPlans(std::optional<NJson::TJsonValue> execPlan, std::optional<NJson::TJsonValue> explainPlan) {
    if (!execPlan.has_value() || !explainPlan.has_value()) {
        Y_ENSURE(false, "Explain plan wasn't computed in the optimizer");
    }

    if (!TransformCtx->PlanJson) {
        TransformCtx->PlanJson.emplace(NJson::JSON_MAP);
        (*TransformCtx->PlanJson)["Plans"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        if (OpRoots.size() > 1) {
            (*TransformCtx->PlanJson)["SimplifiedPlan"]["Node Type"] = "Query";
            (*TransformCtx->PlanJson)["SimplifiedPlan"]["Plans"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        }
    }
    auto& planJson = *TransformCtx->PlanJson;
    planJson["Plans"].AppendValue(std::move(*execPlan));
    if (OpRoots.size() > 1) {
        planJson["SimplifiedPlan"]["Plans"].AppendValue(std::move(*explainPlan));
    } else {
        planJson["SimplifiedPlan"] = std::move(*explainPlan);
    }
    planJson["SimplifiedPlan"]["OptimizerStats"] = MakeNewRBOOptimizerStats(KqpCtx);
}

void TKqpNewRBOTransformer::Rewind() {
    OpRoots.clear();
    CMColumnsByTableName.clear();
    HistColumnsByTableName.clear();
    SharedState.reset();
    ColumnStatisticsReadiness = {};
}

IGraphTransformer::TStatus TKqpRBOCleanupTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    YQL_CLOG(TRACE, CoreDq) << "Cleanup input plan: " << KqpExprToPrettyString(TExprBase(input), ctx) << Endl;
    if (TKqpPhysicalQuery::Match(input.Get())) {
        output = input;
        return TStatus::Ok;
    }
    const auto fail = [&](const TString& reason) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "NEW RBO read-only result bundle: " + reason));
        return TStatus::Error;
    };
    TVector<TExprNode::TPtr> slots;
    if (const auto reason = CollectReadOnlyResultSlots(input, "KqpPhysicalQuery", slots); !reason.empty()) {
        return fail(reason);
    }

    TVector<TExprBase> transactions;
    TVector<TExprBase> results;
    const auto querySettings = TKqpPhysicalQuery(slots.front()).Settings();
    for (const auto& slot : slots) {
        const auto query = TKqpPhysicalQuery(slot);
        const TExprNode* settings = query.Settings().Raw();
        const TExprNode* expectedSettings = querySettings.Raw();
        if (query.Results().Size() != 1 || !CompareExprTrees(settings, expectedSettings)) {
            return fail("roots must have one public result and identical query settings");
        }
        // All bindings, including materialization parameters inside transactions,
        // use query-local transaction indexes. Result indexes stay transaction-local.
        TNodeOnNodeOwnedMap replacements;
        for (const auto& node : FindNodes(slot, [](const TExprNode::TPtr& node) {
            return TKqpTxResultBinding::Match(node.Get());
        })) {
            const auto binding = TKqpTxResultBinding(node);
            ui32 txIndex = 0;
            ui32 resultIndex = 0;
            if (!TryFromString(binding.TxIndex().Value(), txIndex) ||
                !TryFromString(binding.ResultIndex().Value(), resultIndex) ||
                txIndex >= query.Transactions().Size() ||
                resultIndex >= query.Transactions().Item(txIndex).Results().Size()) {
                return fail("transaction-result binding is outside its root's transaction list");
            }
            auto rebasedBinding = Build<TKqpTxResultBinding>(ctx, node->Pos())
                .Type(binding.Type())
                .TxIndex().Build(ToString(transactions.size() + txIndex))
                .ResultIndex(binding.ResultIndex())
                .Done().Ptr();
            rebasedBinding->SetTypeAnn(node->GetTypeAnn());
            replacements[node.Get()] = std::move(rebasedBinding);
        }
        if (!query.Results().Item(0).Maybe<TKqpTxResultBinding>()) {
            return fail("public result is not a transaction-result binding");
        }
        if (slots.size() == 1) {
            output = slot;
            return TStatus::Ok;
        }
        // Rebasing changes no types; keep annotations required by compilation.
        const auto rebased = TKqpPhysicalQuery(ctx.ReplaceNodes<true>({slot}, replacements).front());
        for (const auto& tx : rebased.Transactions()) {
            transactions.emplace_back(tx);
        }
        results.emplace_back(rebased.Results().Item(0));
    }
    output = Build<TKqpPhysicalQuery>(ctx, input->Pos())
        .Transactions().Add(transactions).Build()
        .Results().Add(results).Build()
        .Settings(querySettings)
        .Done().Ptr();
    return TStatus::Ok;
}

TKqpNewRBOTransformer::TKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                             TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer,
                                             TKikimrTablesData& tables, const TString& cluster, const TString& database, TActorSystem* actorSystem,
                                             const NMiniKQL::IFunctionRegistry& funcRegistry, TIntrusivePtr<TKqlTransformContext> transformCtx)
    : TypeCtx(typeCtx)
    , KqpCtx(*kqpCtx)
    , RBOTypeAnnTransformer(std::move(rboTypeAnnTransformer))
    , FuncRegistry(funcRegistry)
    , TransformCtx(transformCtx)
    , Tables(tables)
    , Cluster(cluster)
    , Database(database)
    , ActorSystem(actorSystem) {
    // Finally initializes all RBO optimization stages.
    InitializeRBOOptimizationStages();
}

void TKqpNewRBOTransformer::InitializeRBOOptimizationStages() {
    const bool inlineJoinFiltersAfterCBO = KqpCtx.Config->GetEnableInlineJoinFiltersAfterCBO();

    auto addMapAliasRules = [](TVector<std::unique_ptr<IRule>>& rules) {
        rules.emplace_back(std::make_unique<TRemoveIdenityMapRule>());
        rules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>(/*pruneKeyColumns=*/false));
        rules.emplace_back(std::make_unique<TRenameToAppendRule>());
        rules.emplace_back(std::make_unique<TPushMapElementsIntoMapRule>());
        rules.emplace_back(std::make_unique<TPushMapElementsThroughInputRule>());
        rules.emplace_back(std::make_unique<TPushMapElementsThroughAggregateRule>());
        rules.emplace_back(std::make_unique<TPushMapElementsThroughUnionAllRule>());
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        rules.emplace_back(std::make_unique<TPushRenameIntoProducerRule>());
        rules.emplace_back(std::make_unique<TPruneDeadReadColumnsRule>(/*pruneKeyColumns=*/false));
        rules.emplace_back(std::make_unique<TPruneDeadUnionAllColumnsRule>());
        rules.emplace_back(std::make_unique<TPruneDeadAggregateTraitsRule>());
    };

    // Initial stages.
    // Expand aggregation.
    TVector<std::unique_ptr<IRule>> expandAggregationRules;
    expandAggregationRules.emplace_back(std::make_unique<TExpandDistinctAggregationRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Expand aggregation", std::move(expandAggregationRules)));

    // Predicate pull-up and subplan inlining and decorelation stages.
    TVector<std::unique_ptr<IRule>> filterPullUpRules;
    filterPullUpRules.emplace_back(std::make_unique<TPullUpCorrelatedFilterRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Correlated predicate pullup", std::move(filterPullUpRules)));

    TVector<std::unique_ptr<IRule>> inlineScalarSubPlanStageRules;
    inlineScalarSubPlanStageRules.emplace_back(std::make_unique<TInlineScalarSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline scalar subplans", std::move(inlineScalarSubPlanStageRules)));
    RBO.AddStage(std::make_unique<TConstantFoldingStage>());

    TVector<std::unique_ptr<IRule>> inlineSimpleSubPlanStageRules;
    inlineSimpleSubPlanStageRules.emplace_back(std::make_unique<TInlineSimpleInExistsSubplanRule>());
    inlineSimpleSubPlanStageRules.emplace_back(std::make_unique<TInlineGenericInExistsSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline in/exists subplans", std::move(inlineSimpleSubPlanStageRules)));

    // Rewrite all right joins into left joins
    TVector<std::unique_ptr<IRule>> rewriteRightJoinsStageRules;
    rewriteRightJoinsStageRules.emplace_back(std::make_unique<TRewriteRightJoinRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Rewrite right joins", std::move(rewriteRightJoinsStageRules)));

    // Normalize aliases and simple maps before the broader logical rewrites start.
    TVector<std::unique_ptr<IRule>> mapAliasRules;
    addMapAliasRules(mapAliasRules);
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Normalize maps and aliases", std::move(mapAliasRules)));

    // Logical state I
    TVector<std::unique_ptr<IRule>> logicalStage_I_Rules;
    logicalStage_I_Rules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TExtractCommonConjunctsRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Logical rewrites I", std::move(logicalStage_I_Rules)));

    TVector<std::unique_ptr<IRule>> logicalStage_II_Rules;
    if (!inlineJoinFiltersAfterCBO) {
        logicalStage_II_Rules.emplace_back(std::make_unique<TInlineJoinFiltersRule>());
    }
    logicalStage_II_Rules.emplace_back(std::make_unique<TFuseFiltersRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TExtractCommonConjunctsRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TEliminateLeftJoinRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushLimitIntoSortRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Logical rewrites II", std::move(logicalStage_II_Rules)));

    const bool pruneKeyColumnsEarly = !inlineJoinFiltersAfterCBO;
    TVector<std::unique_ptr<IRule>> pruningStageRules;
    pruningStageRules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>(pruneKeyColumnsEarly));
    pruningStageRules.emplace_back(std::make_unique<TPruneDeadAggregateTraitsRule>());
    pruningStageRules.emplace_back(std::make_unique<TPruneDeadUnionAllColumnsRule>());
    pruningStageRules.emplace_back(std::make_unique<TPruneDeadReadColumnsRule>(pruneKeyColumnsEarly));
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Pruning I", std::move(pruningStageRules)));

    // Physical stage.
    TVector<std::unique_ptr<IRule>> physicalStageRules;
    physicalStageRules.emplace_back(std::make_unique<TPushRangesRule>());
    physicalStageRules.emplace_back(std::make_unique<TPushOlapFilterRule>());
    physicalStageRules.emplace_back(std::make_unique<TPushOlapProjectionRule>());
    physicalStageRules.emplace_back(std::make_unique<TDisableBlocksOnColumnsLimitRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Physical rewrites I", std::move(physicalStageRules)));

    // CBO stages.
    TVector<std::unique_ptr<IRule>> initialCBOStageRules;
    initialCBOStageRules.emplace_back(std::make_unique<TBuildInitialCBOTreeRule>());
    initialCBOStageRules.emplace_back(std::make_unique<TExpandCBOTreeRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Prepare for CBO", std::move(initialCBOStageRules)));

    TVector<std::unique_ptr<IRule>> cboStageRules;
    cboStageRules.emplace_back(std::make_unique<TOptimizeCBOTreeRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Invoke CBO", std::move(cboStageRules)));

    TVector<std::unique_ptr<IRule>> cleanUpCBOStageRules;
    cleanUpCBOStageRules.emplace_back(std::make_unique<TInlineCBOTreeRule>());
    cleanUpCBOStageRules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    cleanUpCBOStageRules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>(pruneKeyColumnsEarly));
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Clean up after CBO", std::move(cleanUpCBOStageRules)));

    if (inlineJoinFiltersAfterCBO) {
        TVector<std::unique_ptr<IRule>> inlineJoinFiltersAfterCBORules;
        inlineJoinFiltersAfterCBORules.emplace_back(std::make_unique<TInlineJoinFiltersRule>());
        inlineJoinFiltersAfterCBORules.emplace_back(std::make_unique<TFuseFiltersRule>());
        inlineJoinFiltersAfterCBORules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
        RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline join filters after CBO", std::move(inlineJoinFiltersAfterCBORules)));

        TVector<std::unique_ptr<IRule>> pruningStage_II_Rules;
        pruningStage_II_Rules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>(/*pruneKeyColumns=*/true));
        pruningStage_II_Rules.emplace_back(std::make_unique<TPruneDeadAggregateTraitsRule>());
        pruningStage_II_Rules.emplace_back(std::make_unique<TPruneDeadUnionAllColumnsRule>());
        pruningStage_II_Rules.emplace_back(std::make_unique<TPruneDeadReadColumnsRule>(/*pruneKeyColumns=*/true));
        RBO.AddStage(std::make_unique<TRuleBasedStage>("Pruning II", std::move(pruningStage_II_Rules)));
    }

    // Assign physical stages.
    TVector<std::unique_ptr<IRule>> assignPhysicalStageRules;
    assignPhysicalStageRules.emplace_back(std::make_unique<TAssignStagesRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Assign physical stages", std::move(assignPhysicalStageRules)));

    // Optimize physical stages.
    TVector<std::unique_ptr<IRule>> optimizePhysicalStagesRules;
    optimizePhysicalStagesRules.emplace_back(std::make_unique<TPropagateAggregateThroughStageRule>());
    optimizePhysicalStagesRules.emplace_back(std::make_unique<TPropagateTopSortThroughStageRule>());
    optimizePhysicalStagesRules.emplace_back(std::make_unique<TPropagateLimitThroughStageRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Optimize physical stages", std::move(optimizePhysicalStagesRules)));

    RBO.AddStage(std::make_unique<TPropagateHashFuncStage>());
}

void TKqpRBOCleanupTransformer::Rewind() {
}

TAutoPtr<IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx) {
    return new TKqpRewriteSelectTransformer(kqpCtx, typeCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                                       TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer, TKikimrTablesData& tables,
                                                       const TString& cluster, const TString& database, TActorSystem* actorSystem,
                                                       const NMiniKQL::IFunctionRegistry& funcRegistry, TIntrusivePtr<TKqlTransformContext> transformCtx) {
    return new TKqpNewRBOTransformer(kqpCtx, typeCtx, std::move(rboTypeAnnTransformer), tables, cluster, database,
                                     actorSystem, funcRegistry, transformCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext &typeCtx) {
    return new TKqpRBOCleanupTransformer(typeCtx);
}

} // namespace NKikimr::NKqp
