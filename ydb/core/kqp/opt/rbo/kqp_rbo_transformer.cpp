#include "kqp_rbo_transformer.h"
#include "kqp_operator.h"
#include "kqp_plan_conversion_utils.h"

#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

TExprNode::TPtr PushTakeIntoPlan(const TExprNode::TPtr &node, TExprContext &ctx, const TTypeAnnotationContext &typeCtx) {
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
            .PgSyntax(root.Cast().PgSyntax())
        .Done().Ptr();
        // clang-format on
    } else {
        return node;
    }
}

TExprNode::TPtr RewriteSublink(const TExprNode::TPtr &node, TExprContext &ctx, bool pgSyntax) {
    if (node->Child(0)->Content() == "expr") {
        // clang-format off
        return Build<TKqpExprSublink>(ctx, node->Pos())
            .Subquery(node->Child(4))
            .Done().Ptr();
        // clang-format on
    } else if (node->Child(0)->Content() == "any") {
        // clang-format off
        return Build<TKqpInSublink>(ctx, node->Pos())
            .Subquery(node->Child(4))
            .ReturnPgBool().Value(std::to_string(pgSyntax)).Build()
            .InTuple(node->Child(2))
            .Done().Ptr();
        // clang-format on
    } else if (node->Child(0)->Content() == "exists") {
        // clang-format off
        return Build<TKqpExistsSublink>(ctx, node->Pos())
            .Subquery(node->Child(4))
            .ReturnPgBool().Value(std::to_string(pgSyntax)).Build()
            .Done().Ptr();
        // clang-format on
    }
    else {
        Y_ENSURE(false, "Uknown sublink type in query");
    }

}

TExprNode::TPtr RemoveRootFromSublink(const TExprNode::TPtr &node, TExprContext &ctx) {
    auto sublink = TKqpSublinkBase(node);
    if (auto root = sublink.Subquery().Maybe<TKqpOpRoot>()) {
        if (TKqpExprSublink::Match(node.Get())) {
            // clang-format off
            return Build<TKqpExprSublink>(ctx, node->Pos())
                .Subquery(root.Cast().Input())
                .Done().Ptr();
            // clang-format on
        } else if (TKqpExistsSublink::Match(node.Get())) {
            // clang-format off
            return Build<TKqpExistsSublink>(ctx, node->Pos())
                .Subquery(root.Cast().Input())
                .ReturnPgBool(node->Child(TKqpExistsSublink::idx_ReturnPgBool))
                .Done().Ptr();
            // clang-format on
        } else if (TKqpInSublink::Match(node.Get())) {
            // clang-format off
            return Build<TKqpInSublink>(ctx, node->Pos())
                .Subquery(root.Cast().Input())
                .ReturnPgBool(node->Child(TKqpInSublink::idx_ReturnPgBool))
                .InTuple(sublink.Cast<TKqpInSublink>().InTuple())
                .Done().Ptr();
            // clang-format on
        }
    }
    return node;
}
} // namespace

namespace NKikimr {
namespace NKqp {

IGraphTransformer::TStatus TKqpRewriteSelectTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(
        output, output,
        [](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            if (node->IsCallable("PgSubLink")) {
                return RewriteSublink(node, ctx, true);
            } else if (node->IsCallable("YqlSubLink")) {
                return RewriteSublink(node, ctx, false);
            } else {
                return node;
            }
        },
        ctx, settings);
    
    if (status != TStatus::Ok) {
        return status;
    }

    status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            // PostgreSQL AST rewrtiting
            if (TCoPgSelect::Match(node.Get())) {
                return RewriteSelect(node, ctx, TypeCtx, KqpCtx, UniqueSourceIdCounter,  true);
            }
            
            // YQL AST rewriting
            else if (TCoYqlSelect::Match(node.Get())) {
                return RewriteSelect(node, ctx, TypeCtx, KqpCtx, UniqueSourceIdCounter, false);
            } else if (TKqpSublinkBase::Match(node.Get())) {
                return RemoveRootFromSublink(node, ctx);
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
        if (it != mapping.end()) {
            const auto& tableName = it->second.TableName;
            const auto& colName = it->second.ColumnName;
            CMColumnsByTableName[tableName].insert(colName);
            HistColumnsByTableName[tableName].insert(colName);
        }
    }
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(TExprContext& ctx) {
    Y_ENSURE(OpRoot);
    TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), *PeepholeTypeAnnTransformer.Get(), FuncRegistry);
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
                   [](const TColumnStatistics& stats) { return !!stats.CountMinSketch; });
    AddStatRequest(ActorSystem, futures, Tables, Cluster, Database, TypeCtx, NStat::EStatType::EQ_WIDTH_HISTOGRAM, HistColumnsByTableName,
                   [](const TColumnStatistics& stats) { return !!stats.EqWidthHistogramEstimator; });

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
                TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), *PeepholeTypeAnnTransformer.Get(), FuncRegistry);
                return RBO.Optimize(*OpRoot, rboCtx);
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
            TypeCtx.ColumnStatisticsByTableName.insert({std::move(tableName), new TOptimizerStatistics::TColumnStatMap(std::move(columnStatistics))});
        }
    }
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    ApplyColumnStatistics();
    return ContinueOptimizations(input, output, ctx);
}

void TKqpNewRBOTransformer::Rewind() {
}

IGraphTransformer::TStatus TKqpRBOCleanupTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    Y_UNUSED(ctx);

    YQL_CLOG(TRACE, CoreDq) << "Cleanup input plan: " << KqpExprToPrettyString(TExprBase(output), ctx) << Endl;


    if (output->IsList() && output->ChildrenSize() >= 1) {
        auto child_level_1 = output->Child(0);
        YQL_CLOG(TRACE, CoreDq) << "Matched level 0";

        if (child_level_1->IsList() && child_level_1->ChildrenSize() >= 1) {
            auto child_level_2 = child_level_1->Child(0);
            YQL_CLOG(TRACE, CoreDq) << "Matched level 1";

            if (child_level_2->IsList() && child_level_2->ChildrenSize() >= 1) {
                auto child_level_3 = child_level_2->Child(0);
                YQL_CLOG(TRACE, CoreDq) << "Matched level 2";

                if (child_level_3->IsList() && child_level_2->ChildrenSize() >= 1) {
                    auto maybeQuery = child_level_3->Child(0);

                    if (TKqpPhysicalQuery::Match(maybeQuery)) {
                        YQL_CLOG(TRACE, CoreDq) << "Found query node";
                        output = maybeQuery;
                    }
                }
            }
        }
    }

    return IGraphTransformer::TStatus::Ok;
}

TKqpNewRBOTransformer::TKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                             TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer, TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer,
                                             TKikimrTablesData& tables, const TString& cluster, const TString& database, TActorSystem* actorSystem,
                                             const NMiniKQL::IFunctionRegistry& funcRegistry)
    : TypeCtx(typeCtx)
    , KqpCtx(*kqpCtx)
    , RBOTypeAnnTransformer(std::move(rboTypeAnnTransformer))
    , PeepholeTypeAnnTransformer(std::move(peepholeTypeAnnTransformer))
    , FuncRegistry(funcRegistry)
    , Tables(tables)
    , Cluster(cluster)
    , Database(database)
    , ActorSystem(actorSystem) {
    // Finally initializes all RBO optimization stages.
    InitializeRBOOptimizationStages();
}

void TKqpNewRBOTransformer::InitializeRBOOptimizationStages() {
    // Initial stages.
    // Predicate pull-up stage.
    TVector<std::unique_ptr<IRule>> filterPullUpRules;
    filterPullUpRules.emplace_back(std::make_unique<TPullUpCorrelatedFilterRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Correlated predicte pullup", std::move(filterPullUpRules)));

    TVector<std::unique_ptr<IRule>> inlineScalarSubPlanStageRules;
    inlineScalarSubPlanStageRules.emplace_back(std::make_unique<TInlineScalarSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline scalar subplans", std::move(inlineScalarSubPlanStageRules)));
    RBO.AddStage(std::make_unique<TRenameStage>());
    RBO.AddStage(std::make_unique<TConstantFoldingStage>());

    // Logical stage.
    TVector<std::unique_ptr<IRule>> logicalStageRules;
    logicalStageRules.emplace_back(std::make_unique<TRemoveIdenityMapRule>());
    logicalStageRules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
    logicalStageRules.emplace_back(std::make_unique<TPushMapRule>());
    logicalStageRules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    logicalStageRules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    logicalStageRules.emplace_back(std::make_unique<TPushLimitIntoSortRule>());
    logicalStageRules.emplace_back(std::make_unique<TInlineSimpleInExistsSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Logical rewrites I", std::move(logicalStageRules)));

    // Prune column stage.
    RBO.AddStage(std::make_unique<TPruneColumnsStage>());

    // Physical stage.
    TVector<std::unique_ptr<IRule>> physicalStageRules;
    physicalStageRules.emplace_back(std::make_unique<TPeepholePredicate>());
    physicalStageRules.emplace_back(std::make_unique<TPushOlapFilterRule>());
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
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Clean up after CBO", std::move(cleanUpCBOStageRules)));

    // Assign physical stages.
    TVector<std::unique_ptr<IRule>> assignPhysicalStageRules;
    assignPhysicalStageRules.emplace_back(std::make_unique<TAssignStagesRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Assign stages", std::move(assignPhysicalStageRules)));
}

void TKqpRBOCleanupTransformer::Rewind() {
}

TAutoPtr<IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx) {
    return new TKqpRewriteSelectTransformer(kqpCtx, typeCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                                       TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer,
                                                       TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer, TKikimrTablesData& tables,
                                                       const TString& cluster, const TString& database, TActorSystem* actorSystem,
                                                       const NMiniKQL::IFunctionRegistry& funcRegistry) {
    return new TKqpNewRBOTransformer(kqpCtx, typeCtx, std::move(rboTypeAnnTransformer), std::move(peepholeTypeAnnTransformer), tables, cluster, database,
                                     actorSystem, funcRegistry);
}

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext &typeCtx) {
    return new TKqpRBOCleanupTransformer(typeCtx);
}

} // namespace NKqp
} // namespace NKikimr