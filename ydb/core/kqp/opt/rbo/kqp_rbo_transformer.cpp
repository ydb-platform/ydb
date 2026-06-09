#include "kqp_rbo_transformer.h"
#include "kqp_operator.h"
#include "kqp_plan_conversion_utils.h"
#include "kqp_rbo_rules.h"

#include <ydb/core/kqp/host/kqp_transform.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

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

} // anonymous namespace

IGraphTransformer::TStatus TKqpRewriteSelectTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

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
            }  else if (TCoTake::Match(node.Get())) {
                return PushTakeIntoPlan(node, ctx, TypeCtx);
            } else {
                return node;
            }
        },
        ctx, settings);

    return status;
}

void TKqpRewriteSelectTransformer::Rewind() {}

IGraphTransformer::TStatus TKqpNewRBOTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    // At first step convert KqpOps to RBO Ops.
    auto status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            Y_UNUSED(ctx);
            if (TKqpOpRoot::Match(node.Get())) {
                OpRoot = PlanConverter(TypeCtx, ctx).ConvertRoot(node);
                OpRoot->ComputeParents();
                return node;
            } else {
                return node;
            }
        },
        ctx, settings);

    if (status != TStatus::Ok) {
        return status;
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
    }
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(const TExpression& expr, const TPhysicalOpProps& props) {
    const auto& mapping = props.Metadata->ColumnLineage.Mapping;
    auto lambda = TCoLambda(expr.GetLambda());
    const auto members = FindNodes(lambda.Body().Ptr(), [](const TExprNode::TPtr& node) { return node->IsCallable("Member"); });

    TVector<TInfoUnit> colNames;
    for (const auto& member : members) {
        const auto memberName = TCoMember(member).Name().StringValue();
        const auto pos = memberName.find(".");
        if (pos != TString::npos) {
            const auto aliasName = memberName.substr(0, pos);
            Y_ENSURE(pos + 1 < memberName.size());
            const auto colName = memberName.substr(pos + 1);
            colNames.emplace_back(aliasName, colName);
        }
    }

    for (const auto& column : colNames) {
        const auto it = mapping.find(column.GetFullName());
        if (it != mapping.end() && it->second.TableName != "") {
            const auto& tableName = it->second.TableName;
            const auto& colName = it->second.ColumnName;
            CMColumnsByTableName[tableName].insert(colName);
            HistColumnsByTableName[tableName].insert(colName);
        }
    }
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(TExprContext& ctx) {
    Y_ENSURE(OpRoot);
    TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), FuncRegistry);
    OpRoot->ComputePlanMetadata(rboCtx);
    for (auto it : *OpRoot) {
        if (IsSuitableToCollectStatistics(it.Current)) {
            CollectTablesAndColumnsNames(it.Current);
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

    ColumnStatisticsReadiness = NThreading::WaitAll(futures).Apply([this, futures = std::move(futures)](const NThreading::TFuture<void>&) mutable {
        for (auto& fut : futures) {
            if (fut.HasException()) {
                fut.TryRethrow();
            }

            auto newStats = fut.ExtractValue();
            if (!ColumnStatisticsResponse) {
                ColumnStatisticsResponse = std::move(newStats);
            } else {
                // merge statistics
                for (const auto& [table, column2Stat] : newStats.ColumnStatisticsByTableName) {
                    auto& oldColumn2Stat = ColumnStatisticsResponse->ColumnStatisticsByTableName[table];
                    for (const auto& [column, newStat] : column2Stat.Data) {
                        auto& oldStat = oldColumn2Stat.Data[column];
                        if (newStat.CountMinSketch) {
                            oldStat.CountMinSketch = newStat.CountMinSketch;
                        }
                        if (newStat.EqWidthHistogramEstimator) {
                            oldStat.EqWidthHistogramEstimator = newStat.EqWidthHistogramEstimator;
                        }
                    }
                }
            }
        }
    });

    return TStatus::Async;
}

bool TKqpNewRBOTransformer::IsSuitableToRequestStatistics() {
    // Currently just checking for a flag.
    return KqpCtx.Config->FeatureFlags.GetEnableColumnStatistics();
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::ContinueOptimizations(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);
    Y_ENSURE(OpRoot, "NEW RBO OpRoot is not initialized.");

    // Apply optimizations.
    auto status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (TKqpOpRoot::Match(node.Get())) {
                TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), FuncRegistry);
                auto output = RBO.Optimize(*OpRoot, rboCtx);
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
    if (!ColumnStatisticsResponse->Issues().Empty()) {
        TStringStream ss;
        ColumnStatisticsResponse->Issues().PrintTo(ss);
        YQL_CLOG(TRACE, ProviderKikimr) << "Can't load columns statistics for request: " << ss.Str();
    } else {
        for (auto&& [tableName, columnStatistics] : ColumnStatisticsResponse->ColumnStatisticsByTableName) {
            TypeCtx.ColumnStatisticsByTableName.insert({std::move(tableName), new NYql::TOptimizerStatistics::TColumnStatMap(std::move(columnStatistics))});
        }
    }
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    ApplyColumnStatistics();
    return ContinueOptimizations(input, output, ctx);
}

//FIXME: We currently support only a single plan, throw an exception if that's not the case
void TKqpNewRBOTransformer::AddPlans(std::optional<NJson::TJsonValue> execPlan, std::optional<NJson::TJsonValue> explainPlan) {
    if (!execPlan.has_value() || !explainPlan.has_value()) {
        Y_ENSURE(false, "Explain plan wasn't computed in the optimizer");
    }

    Y_ENSURE(!TransformCtx->PlanJson.has_value(), "Only a single explain is supported");

    auto planJson = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
    auto plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    plans.AppendValue(execPlan.value());
    planJson["Plans"] = plans;
    planJson["SimplifiedPlan"] = explainPlan.value();
    planJson["SimplifiedPlan"]["OptimizerStats"] = MakeNewRBOOptimizerStats(KqpCtx);

    TransformCtx->PlanJson = planJson;
}

void TKqpNewRBOTransformer::Rewind() {
}

IGraphTransformer::TStatus TKqpRBOCleanupTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    TOptimizeExprSettings settings(&TypeCtx);
    Y_UNUSED(ctx);
    YQL_CLOG(TRACE, CoreDq) << "Cleanup input plan: " << KqpExprToPrettyString(TExprBase(input), ctx) << Endl;

    // We just need to find a physical query callable.
    auto physicalQueries = FindNodes(input, [](const TExprNode::TPtr& node) { return TKqpPhysicalQuery::Match(node.Get()); });
    if (physicalQueries.size() == 1) {
        output = physicalQueries.front();
        return IGraphTransformer::TStatus::Ok;
    }

    return IGraphTransformer::TStatus::Error;
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
    auto addMapAliasRules = [](TVector<std::unique_ptr<IRule>>& rules, bool pushAppendsUnderFilter) {
        rules.emplace_back(std::make_unique<TRemoveIdenityMapRule>());
        rules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>());
        rules.emplace_back(std::make_unique<TRenameToAppendRule>());
        rules.emplace_back(std::make_unique<TPushAppendIntoMapRule>());
        rules.emplace_back(std::make_unique<TPushAppendThroughUnaryRule>(pushAppendsUnderFilter));
        rules.emplace_back(std::make_unique<TPushAppendThroughAggregateRule>());
        rules.emplace_back(std::make_unique<TPushAppendThroughJoinRule>());
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        rules.emplace_back(std::make_unique<TPushRenameIntoReadRule>());
        rules.emplace_back(std::make_unique<TPushRenameIntoMapProducerRule>());
        rules.emplace_back(std::make_unique<TPushRenameIntoAggregateResultRule>());
        rules.emplace_back(std::make_unique<TPushRenameThroughTransparentUnaryRule>(pushAppendsUnderFilter));
        rules.emplace_back(std::make_unique<TPushRenameThroughPassThroughMapRule>());
        rules.emplace_back(std::make_unique<TPushRenameThroughAggregateKeyRule>());
        rules.emplace_back(std::make_unique<TPushRenameThroughJoinSideRule>());
        rules.emplace_back(std::make_unique<TPruneDeadReadColumnsRule>());
        rules.emplace_back(std::make_unique<TPruneDeadAggregateTraitsRule>());
    };

    // Initial stages.
    // Inline join filters. FIXME: Move after inlining when adding support for more advanced decorelation
    TVector<std::unique_ptr<IRule>> joinFiltersInlineRules;
    joinFiltersInlineRules.emplace_back(std::make_unique<TInlineJoinFiltersRule>());
    joinFiltersInlineRules.emplace_back(std::make_unique<TFuseFiltersRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline join filters", std::move(joinFiltersInlineRules)));

    // Predicate pull-up stage.
    TVector<std::unique_ptr<IRule>> filterPullUpRules;
    filterPullUpRules.emplace_back(std::make_unique<TPullUpCorrelatedFilterRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Correlated predicte pullup", std::move(filterPullUpRules)));

    TVector<std::unique_ptr<IRule>> inlineScalarSubPlanStageRules;
    inlineScalarSubPlanStageRules.emplace_back(std::make_unique<TInlineJoinFiltersRule>());
    inlineScalarSubPlanStageRules.emplace_back(std::make_unique<TFuseFiltersRule>());
    inlineScalarSubPlanStageRules.emplace_back(std::make_unique<TInlineScalarSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline scalar subplans", std::move(inlineScalarSubPlanStageRules)));
    RBO.AddStage(std::make_unique<TConstantFoldingStage>());

    TVector<std::unique_ptr<IRule>> inlineSimpleSubPlanStageRules;
    inlineSimpleSubPlanStageRules.emplace_back(std::make_unique<TInlineSimpleInExistsSubplanRule>());
    inlineSimpleSubPlanStageRules.emplace_back(std::make_unique<TInlineGenericInExistsSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline in/exists subplans", std::move(inlineSimpleSubPlanStageRules)));

    // Normalize aliases and simple maps before the broader logical rewrites start.
    TVector<std::unique_ptr<IRule>> mapAliasRules;
    addMapAliasRules(mapAliasRules, /*pushAppendsUnderFilter*/ true);
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Normalize maps and aliases", std::move(mapAliasRules)));

    // Logical stage.
    TVector<std::unique_ptr<IRule>> logicalStageRules;
    addMapAliasRules(logicalStageRules, /*pushAppendsUnderFilter*/ false);
    logicalStageRules.emplace_back(std::make_unique<TInlineJoinFiltersRule>());
    logicalStageRules.emplace_back(std::make_unique<TFuseFiltersRule>());
    logicalStageRules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
    logicalStageRules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    logicalStageRules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    logicalStageRules.emplace_back(std::make_unique<TEliminateLeftJoinRule>());
    logicalStageRules.emplace_back(std::make_unique<TPushLimitIntoSortRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Logical rewrites I", std::move(logicalStageRules)));

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
    cleanUpCBOStageRules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Clean up after CBO", std::move(cleanUpCBOStageRules)));

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

    RBO.AddStage(std::make_unique<TLogicalOutputPruningStage>());

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
