#include "kqp_rbo_transformer.h"
#include "kqp_operator.h"
#include "kqp_plan_conversion_utils.h"
#include "kqp_rbo_rules.h"
#include "traces/kqp_rbo_trace_output.h"

#include <ydb/core/kqp/host/kqp_transform.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/system/env.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

#include <memory>
#include <optional>
#include <utility>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

THashMap<TString, THashSet<TString>> TakeNotRequested(
    const THashMap<TString, THashSet<TString>>& columnsByTableName,
    THashMap<TString, THashSet<TString>>& requestedByTableName)
{
    THashMap<TString, THashSet<TString>> notRequested;
    for (const auto& [table, columns] : columnsByTableName) {
        auto& requested = requestedByTableName[table];
        for (const auto& column : columns) {
            if (requested.insert(column).second) {
                notRequested[table].insert(column);
            }
        }
    }
    return notRequested;
}

THashMap<TString, THashMap<TString, TVector<TString>>> TakeNotRequested(
    const THashMap<TString, THashMap<TString, TVector<TString>>>& tuplesByTableName,
    THashMap<TString, THashSet<TString>>& requestedByTableName) {
    THashMap<TString, THashMap<TString, TVector<TString>>> notRequested;
    for (const auto& [table, tuples] : tuplesByTableName) {
        auto& requested = requestedByTableName[table];
        for (const auto& [tupleKey, columns] : tuples) {
            if (requested.insert(tupleKey).second) {
                notRequested[table].emplace(tupleKey, columns);
            }
        }
    }
    return notRequested;
}

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

bool IsRboTraceLogEnabled() {
    TMaybe<TString> htmlTracePath = TryGetEnv("NEW_RBO_LOG");
    return htmlTracePath.Defined() && !htmlTracePath->empty();
}

struct TInputRepeatability {
    // The subtree contains a value that may change when evaluated again.
    bool HasNonRepeatableExpression = false;
    // The subtree contains a row producer that depends on such a value.
    // Sharing this producer across roots may require evaluating its rows only once.
    bool HasNonRepeatableProducer = false;
};

// Summarize the original expression DAG, caching each node's subtree properties.
// Keep scalar nondeterminism separate from row production: a shared RandomNumber
// node alone does not imply that two statements share the same input rows.
TInputRepeatability AnalyzeInputRepeatability(const TExprNode* node, THashMap<const TExprNode*, TInputRepeatability>& memo) {
    if (const auto it = memo.find(node); it != memo.end()) {
        return it->second;
    }
    // Match the conservative treatment of opaque functions used by logical
    // subtree copying. Sequencers also must not be evaluated independently.
    TInputRepeatability result;
    result.HasNonRepeatableExpression = TCoNonDeterministicBase::Match(node) || TKqlSequencer::Match(node)
        || node->IsCallable({"Udf", "ScriptUdf", "SqlCall"});
    for (const auto& child : node->Children()) {
        const auto childInfo = AnalyzeInputRepeatability(child.Get(), memo);
        result.HasNonRepeatableExpression |= childInfo.HasNonRepeatableExpression;
        result.HasNonRepeatableProducer |= childInfo.HasNonRepeatableProducer;
    }
    const auto type = node->GetTypeAnn();
    const bool producesRows = TCoYqlSelect::Match(node) || TKqlSequencer::Match(node)
        || (type && (type->GetKind() == ETypeAnnotationKind::List
            || type->GetKind() == ETypeAnnotationKind::Stream || type->GetKind() == ETypeAnnotationKind::Flow));
    // Annotation may intern equal scalar calls in otherwise independent inputs.
    // Only sharing a row producer requires preserving its evaluation across roots.
    result.HasNonRepeatableProducer |= producesRows && result.HasNonRepeatableExpression;
    memo.emplace(node, result);
    return result;
}

// Each query root is imported independently by RBO. If two writes consume the
// same random row producer, they can consequently store different generated keys.
// Reject shared non-repeatable inputs involving an effect while compiling, so
// fallback can retry the whole query with the legacy optimizer before execution.
void CheckSharedNonRepeatableInputs(const TExprNode::TPtr& input) {
    TVector<std::pair<const TExprNode*, bool>> roots;
    bool hasEffects = false;
    for (const auto query : TKqlQueryList(input)) {
        for (const auto result : query.Results()) {
            auto value = result.Value().Ptr();
            while (TCoUnordered::Match(value.Get()) || TCoTake::Match(value.Get())) {
                value = value->HeadPtr();
            }
            // RETURNING observes the effect below; it does not evaluate its
            // input a second time and must not count as an independent root.
            if (!TKqlReturningList::Match(value.Get())) {
                roots.emplace_back(value.Get(), false);
            }
        }
        for (const auto effect : query.Effects()) {
            roots.emplace_back(effect.Raw(), true);
            hasEffects = true;
        }
    }
    if (!hasEffects || roots.size() < 2) {
        return;
    }

    // Pointer identity detects shared expressions before rewriting clones them.
    // Repeated visits within one root do not require cross-root coordination.
    THashMap<const TExprNode*, size_t> owners;
    THashMap<const TExprNode*, TInputRepeatability> nonRepeatable;
    for (size_t rootIndex = 0; rootIndex < roots.size(); ++rootIndex) {
        TVector<const TExprNode*> pending{roots[rootIndex].first};
        while (!pending.empty()) {
            const auto* node = pending.back();
            pending.pop_back();
            const auto [it, inserted] = owners.emplace(node, rootIndex);
            if (!inserted) {
                if (it->second != rootIndex && (roots[it->second].second || roots[rootIndex].second)) {
                    Y_ENSURE(!AnalyzeInputRepeatability(node, nonRepeatable).HasNonRepeatableProducer,
                        "New RBO cannot independently evaluate shared non-repeatable input across query roots with effects");
                }
                // Descendants already belong to the first root. Checking the
                // shared subtree here avoids walking it again for every root.
                continue;
            }
            for (const auto& child : node->Children()) {
                pending.push_back(child.Get());
            }
        }
    }
}

} // anonymous namespace

IGraphTransformer::TStatus TKqpRewriteSelectTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    if (KqpCtx.Config->OptFallbackToLegacyOptimizer.Get()) {
        Y_ENSURE(false, "Forced fallback to legacy optimizer");
    }

    // RewriteSelect clones lambdas, and each root is later imported by a fresh
    // PlanConverter. Check sharing while original expression identity survives.
    CheckSharedNonRepeatableInputs(input);
    
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
                return RewriteSelect(node, ctx, TypeCtx, KqpCtx, UniqueSourceIdCounter, UniqueColumnIdCounter, translated, true);
            }  else if (TCoTake::Match(node.Get())) {
                return PushTakeIntoPlan(node, ctx, TypeCtx);
            } else if (TKqlTableEffect::Match(node.Get())) {
                return RewriteTableEffect(node, ctx, KqpCtx, UniqueSourceIdCounter);
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
    std::optional<TCompileTraceScope> traceScope(std::in_place, KqpCtx.GetCompileTrace(), TCompileTraceLevels::Step,
        "Convert to new RBO operators");
    Roots.clear();
    // A query block contains a list of (value, column hints) results and a
    // separate list of effects. Walk those slots explicitly: an effects list
    // with two roots is not a result tuple.
    ui64 physicalTxGroup = 0;
    ui64 effectPriority = 0;
    auto addRoot = [&](TExprNode::TPtr root, TExprNode::TPtr columns) {
        if (TCoUnordered::Match(root.Get())) {
            root = root->HeadPtr();
        }
        Y_ENSURE(TKqpOpRoot::Match(root.Get()), "Expected an RBO query root, got " << root->Content());
        auto opRoot = PlanConverter(TypeCtx, ctx).ConvertRoot(root, columns);
        opRoot->PlanProps.PhysicalTxGroup = physicalTxGroup;
        opRoot->PlanProps.EffectPriority = effectPriority;
        opRoot->ComputeParents();
        Roots.push_back(opRoot);
        return opRoot;
    };
    ui32 resultIndex = 0;
    for (const auto query : TKqlQueryList(input)) {
        struct TReturningResult {
            ui32 Index;
            TExprNode::TPtr Columns;
            TExprNode::TPtr Hints;
            TExprNode::TPtr RowsLimit;
        };
        THashMap<const TExprNode*, TReturningResult> returningResults;
        for (const auto result : query.Results()) {
            auto value = result.Value().Ptr();
            TExprNode::TPtr rowsLimit;
            while (TCoUnordered::Match(value.Get()) || TCoTake::Match(value.Get())) {
                if (auto take = TMaybeNode<TCoTake>(value)) {
                    Y_ENSURE(!rowsLimit, "Nested result limits are not supported");
                    rowsLimit = take.Cast().Count().Ptr();
                }
                value = value->HeadPtr();
            }
            if (auto returning = TMaybeNode<TKqlReturningList>(value)) {
                // RETURNING is a view of an effect's output, not another write.
                // Import that effect only in the effects phase, retaining the
                // result's original slot for mixed SELECT/RETURNING queries.
                Y_ENSURE(returningResults.emplace(returning.Cast().Update().Raw(),
                    TReturningResult{resultIndex, returning.Cast().Columns().Ptr(), result.ColumnHints().Ptr(), rowsLimit}).second);
            } else {
                // Ordinary result limits were pushed into their logical root
                // during rewriting. Only RETURNING keeps a post-write limit.
                Y_ENSURE(!rowsLimit, "Unexpected result limit outside an RBO root");
                addRoot(value, result.ColumnHints().Ptr())->PlanProps.ResultIndex = resultIndex;
            }
            ++resultIndex;
        }
        ++physicalTxGroup;
        effectPriority = 0;
        for (const auto effect : query.Effects()) {
            auto root = effect.Ptr();
            const auto returning = returningResults.find(root.Get());
            if (returning != returningResults.end()) {
                root = ctx.ChangeChild(*root, TKqpOpRoot::idx_ColumnOrder, std::move(returning->second.Columns));
                auto converted = addRoot(root, returning->second.Hints);
                converted->PlanProps.ResultIndex = returning->second.Index;
                converted->PlanProps.ReturningRowsLimit = returning->second.RowsLimit;
                returningResults.erase(returning);
            } else {
                addRoot(root, nullptr);
            }
            ++effectPriority;
        }
        Y_ENSURE(returningResults.empty(), "RETURNING must reference an effect in the same query block");
        ++physicalTxGroup;
    }
    if (Roots.size() > 1) {
        Y_ENSURE(!KqpCtx.Config->GetEnableFallbackOnMultipleStatements(),
            "Fallback due to multiple statements flag");
    }

    traceScope->SetSuccess(true);
    traceScope.reset();

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

bool TKqpNewRBOTransformer::IsSuitableToCollectStatistics(IOperator* op) const {
    return op->Props.Metadata.has_value();
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(IOperator* op, const TColumnLineage& lineage) {
    if (MatchOperator<TOpFilter>(op)) {
        const auto& filter = CastOperator<TOpFilter>(*op);
        CollectTablesAndColumnsNames(filter.GetFilterExpression(), *filter.GetInput(), lineage);
    } else if (MatchOperator<TOpJoin>(op)) {
        // Fetching statistics for join cardinality correction.
        CollectJoinKeysColumns(CastOperator<TOpJoin>(op), lineage);
    } else if (MatchOperator<TOpRead>(op)) {
        // Fetching statistics for filters already pushed down into the read.
        const auto read = CastOperator<TOpRead>(op);
        if (read->OriginalPredicate.has_value()) {
            CollectTablesAndColumnsNames(read->OriginalPredicate.value(), *read, lineage);
        }
    }
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(const TExpression& expr, const IOperator& input, const TColumnLineage& lineage) {
    auto lambda = TCoLambda(expr.GetLambda());

    // Request only the statistic each filter predicate actually consumes during selectivity estimation: 
    // equality predicates probe the count-min sketch, while 
    // range/inequality predicates use the equi-width histogram.
    TPredicateSelectivityComputer computer(nullptr, /*collectColumnsStatUsedMembers=*/true,
        /*collectMemberEqualities=*/true);
    computer.Compute(lambda.Body());

    using TUsedMember = TPredicateSelectivityComputer::TColumnStatisticsUsedMembers::TColumnStatisticsUsedMember;
    for (const auto& item : computer.GetColumnStatsUsedMembers().Data) {
        if (item.Member.Struct().Raw() != lambda.Args().Arg(0).Raw()) {
            continue; // Nested struct fields are not IU IDs, even if numeric.
        }
        const auto* entry = FindSourceStatistics(input, GetMemberId(item.Member.Ref()), lineage);
        if (!entry || entry->TableName == "") {
            continue;
        }
        const auto& tableName = entry->TableName;
        const auto& colName = entry->ColumnName;
        switch (item.PredicateType) {
            case TUsedMember::EEquality:
                CMColumnsByTableName[tableName].insert(colName);
                break;
            case TUsedMember::EInequality:
                HistColumnsByTableName[tableName].insert(colName);
                break;
        }
    }

    THashMap<TString, std::pair<TVector<const TColumnLineageEntry*>, TVector<const TColumnLineageEntry*>>> keysByTablePair;
    for (const auto& [lhsMember, rhsMember] : computer.GetMemberEqualities()) {
        if (lhsMember.Struct().Raw() != lambda.Args().Arg(0).Raw() || rhsMember.Struct().Raw() != lambda.Args().Arg(0).Raw()) {
            continue; // Nested struct fields are not IU IDs, even if numeric.
        }
        const auto* lhsEntry = FindSourceStatistics(input, GetMemberId(lhsMember.Ref()), lineage);
        const auto* rhsEntry = FindSourceStatistics(input, GetMemberId(rhsMember.Ref()), lineage);
        if (!lhsEntry || !rhsEntry) {
            continue;
        }
        const auto& lhsTable = lhsEntry->TableName;
        const auto& rhsTable = rhsEntry->TableName;
        if (lhsTable.empty() || rhsTable.empty() || lhsTable == rhsTable) {
            continue;
        }

        HistColumnsByTableName[lhsTable].insert(lhsEntry->ColumnName);
        HistColumnsByTableName[rhsTable].insert(rhsEntry->ColumnName);
        SimpleColumnsByTableName[lhsTable].insert(lhsEntry->ColumnName);
        SimpleColumnsByTableName[rhsTable].insert(rhsEntry->ColumnName);

        auto& keys = keysByTablePair[TStringBuilder() << lhsTable << '\0' << rhsTable];
        keys.first.push_back(lhsEntry);
        keys.second.push_back(rhsEntry);
    }

    for (const auto& [_, keys] : keysByTablePair) {
        CollectJoinKeysTuple(keys.first);
        CollectJoinKeysTuple(keys.second);
    }
}

void TKqpNewRBOTransformer::CollectJoinKeysColumns(TOpJoin* join, const TColumnLineage& lineage) {
    // For join cardinality correction, only the equi-width histogram of both join-key columns are needed.
    // Keys of a side the join does not output are skipped.
    const auto& outputIUs = join->GetOutputIUs();
    auto findSource = [&](TInfoUnitId key) {
        return outputIUs.Contains(key) ? FindSourceStatistics(*join, key, lineage) : nullptr;
    };
    auto requestHistogram = [&](const TColumnLineageEntry* entry) {
        if (!entry || entry->TableName == "") {
            return;
        }
        const auto& tableName = entry->TableName;
        const auto& colName = entry->ColumnName;
        HistColumnsByTableName[tableName].insert(colName);
        SimpleColumnsByTableName[tableName].insert(colName);
    };

    TVector<const TColumnLineageEntry*> lhsKeys;
    TVector<const TColumnLineageEntry*> rhsKeys;
    for (const auto& [lhsKey, rhsKey, equalNulls] : join->JoinKeys.Items()) {
        lhsKeys.push_back(findSource(lhsKey));
        rhsKeys.push_back(findSource(rhsKey));
        requestHistogram(lhsKeys.back());
        requestHistogram(rhsKeys.back());
    }

    CollectJoinKeysTuple(lhsKeys);
    CollectJoinKeysTuple(rhsKeys);
}

// Each key is its source column, or null when it has none.
void TKqpNewRBOTransformer::CollectJoinKeysTuple(const TVector<const TColumnLineageEntry*>& joinKeys) {
    if (joinKeys.size() < 2) {
        return;
    }

    TString tableName;
    THashSet<TString> columns;
    for (const auto* entry : joinKeys) {
        if (!entry || entry->TableName == "") {
            return;
        }
        if (!tableName.empty() && tableName != entry->TableName) {
            return;
        }
        tableName = entry->TableName;
        columns.insert(entry->ColumnName);
    }

    if (columns.size() != joinKeys.size()) {
        return;
    }

    if (auto tuple = FindEqHeightHistogramTuple(tableName, columns)) {
        EqHeightHistTuplesByTableName[tableName].emplace(NYql::MakeMultiColumnKey(*tuple), *tuple);
    }
}

std::optional<TVector<TString>> TKqpNewRBOTransformer::FindEqHeightHistogramTuple(
    const TString& tableName,
    const THashSet<TString>& columns) const
{
    const auto& tableMeta = Tables.GetTable(Cluster, tableName).Metadata;
    if (!tableMeta) {
        return std::nullopt;
    }

    auto sameColumns = [&columns](const TVector<TString>& candidate) {
        return candidate.size() == columns.size()
            && AllOf(candidate, [&columns](const TString& column) { return columns.contains(column); });
    };

    for (const auto& description : tableMeta->MultiColumnStatistics) {
        if (Find(description.Types, "EQ_HEIGHT_HISTOGRAM") == description.Types.end()) {
            continue;
        }
        if (sameColumns(description.Columns)) {
            return description.Columns;
        }
    }

    if (sameColumns(tableMeta->KeyColumnNames)) {
        return tableMeta->KeyColumnNames;
    }

    return std::nullopt;
}

void TKqpNewRBOTransformer::CollectTablesAndColumnsNames(TExprContext& ctx) {
    TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), FuncRegistry);
    for (auto & root : Roots) {
        root->ComputePlanMetadata(rboCtx);
        for (const auto& it : *root) {
            if (IsSuitableToCollectStatistics(it.Current)) {
                CollectTablesAndColumnsNames(it.Current, root->PlanProps.ColumnLineage);
            }
        }
    }
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::RequestColumnStatistics(TExprContext& ctx) {
    CollectTablesAndColumnsNames(ctx);

    TVector<NThreading::TFuture<TColumnStatisticsResponse>> futures;
    TCompileTraceDependencies trace(KqpCtx.GetCompileTrace(), "Await column statistics");
    AddStatRequest(ActorSystem, futures, Tables, Cluster, Database, TypeCtx, NStat::EStatType::COUNT_MIN_SKETCH,
                   TakeNotRequested(CMColumnsByTableName, RequestedCMColumns),
                   [](const NYql::TColumnStatistics& stats) { return !!stats.CountMinSketch; }, &trace);
    AddStatRequest(ActorSystem, futures, Tables, Cluster, Database, TypeCtx, NStat::EStatType::EQ_WIDTH_HISTOGRAM,
                   TakeNotRequested(HistColumnsByTableName, RequestedHistColumns),
                   [](const NYql::TColumnStatistics& stats) { return !!stats.EqWidthHistogramEstimator; }, &trace);
    AddStatRequest(ActorSystem, futures, Tables, Cluster, Database, TypeCtx, NStat::EStatType::SIMPLE_COLUMN,
                   TakeNotRequested(SimpleColumnsByTableName, RequestedSimpleColumns),
                   [](const NYql::TColumnStatistics& stats) { return stats.HyperLogLog.has_value(); }, &trace);
    AddStatRequest(ActorSystem, futures, Tables, Cluster, Database, TypeCtx, NStat::EStatType::EQ_HEIGHT_HISTOGRAM,
                   TakeNotRequested(EqHeightHistTuplesByTableName, RequestedEqHeightTuples),
                   [](const NYql::TMultiColumnStatistics& stats) { return !!stats.EqHeightHistogram; }, &trace);

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
                        for (const auto& [tuple, newStat] : column2Stat.MultiData) {
                            auto& oldStat = oldColumn2Stat.MultiData[tuple];
                            oldStat.Columns = newStat.Columns;
                            oldStat.Types = newStat.Types;
                            if (newStat.EqHeightHistogram) {
                                oldStat.EqHeightHistogram = newStat.EqHeightHistogram;
                            }
                            if (newStat.CountMinSketch) {
                                oldStat.CountMinSketch = newStat.CountMinSketch;
                            }
                        }
                    }
                }
            }
        });
    // Each request span records the outcome of its request.
    ColumnStatisticsReadiness = trace.Await(ColumnStatisticsReadiness);

    SharedState = sharedState;
    return TStatus::Async;
}

bool TKqpNewRBOTransformer::IsSuitableToRequestStatistics() {
    // Currently just checking for a flag.
    return KqpCtx.Config->FeatureFlags.GetEnableColumnStatistics();
}

IGraphTransformer::TStatus TKqpNewRBOTransformer::ContinueOptimizations(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) try {
    Y_ENSURE(!Roots.empty(), "NEW RBO OpRoot is not initialized.");
    TRBOContext rboCtx(KqpCtx, ctx, TypeCtx, *RBOTypeAnnTransformer.Get(), FuncRegistry);
    TRBOTraceOutput traceOutput(rboCtx);
    // This is the logical-to-physical query boundary. Replace the whole query,
    // rather than replacing differently shaped result/effect tuples in place.
    output = RBO.Optimize(Roots, rboCtx);
    traceOutput.Flush();
    AddPlans(rboCtx.ExecutionJson, rboCtx.ExplainJson);
    return TStatus::Ok;
} catch (const std::exception& e) {
    ctx.AddError(ExceptionToIssue(e, ctx.GetPosition(input->Pos())));
    return TStatus::Error;
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
    if (auto* trace = KqpCtx.GetCompileTrace()) {
        trace->ResumeWaits();
    }
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
    Roots.clear();
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
    const bool pruneKeyColumns = KqpCtx.Config->GetEnablePruneKeyColumns();
    // Match the compiler's AllowWithSpilling setting: shared stage outputs need
    // channel spilling, otherwise every consumer needs its own producer.
    const bool allowChannelSpilling = KqpCtx.Config->GetEnableQueryServiceSpilling()
        && (KqpCtx.IsGenericQuery() || KqpCtx.IsScanQuery()) && KqpCtx.Config->SpillingEnabled();

    // Full joins have no native filter implementation. Expand them while both
    // input keys are still available, before pruning and read pushdown. Their
    // shared inputs then go through the usual expansion when spilling is off.
    TVector<std::unique_ptr<IRule>> fullJoinFilterRules;
    fullJoinFilterRules.emplace_back(std::make_unique<TInlineJoinFiltersRule>(/*fullJoinsOnly=*/true));
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline full join filters", std::move(fullJoinFilterRules)));

    // Other plans can prune unused outputs before requiring type information.
    RBO.AddStage(std::make_unique<TGlobalPruningStage>("Early pruning"));

    TVector<std::unique_ptr<IRule>> effectRules;
    effectRules.emplace_back(std::make_unique<TExpandInsertRevertRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Expand effect conflict handling", std::move(effectRules)));

    // Expand aggregation.
    TVector<std::unique_ptr<IRule>> expandAggregationRules;
    expandAggregationRules.emplace_back(std::make_unique<TExpandGroupingSetsRule>());
    expandAggregationRules.emplace_back(std::make_unique<TExpandDistinctAggregationRule>());
    expandAggregationRules.emplace_back(std::make_unique<TExpandWholePartitionWindowRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Expand aggregation", std::move(expandAggregationRules)));

    // Rewrite all right joins into left joins
    TVector<std::unique_ptr<IRule>> rewriteRightJoinsStageRules;
    rewriteRightJoinsStageRules.emplace_back(std::make_unique<TRewriteRightJoinRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Rewrite right joins", std::move(rewriteRightJoinsStageRules)));

    // Push predicates before inlining.
    TVector<std::unique_ptr<IRule>> earlyPushFilterRules;
    earlyPushFilterRules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
    earlyPushFilterRules.emplace_back(std::make_unique<TExtractCommonConjunctsRule>());
    earlyPushFilterRules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    earlyPushFilterRules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    earlyPushFilterRules.emplace_back(std::make_unique<TPushFilterThroughAggregateRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Push filters before inlining", std::move(earlyPushFilterRules)));

    // Subplan inlining. For correlated subqueries we create dependent join.
    TVector<std::unique_ptr<IRule>> inlineScalarSubPlanStageRules;
    inlineScalarSubPlanStageRules.emplace_back(std::make_unique<TInlineScalarSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline scalar subplans", std::move(inlineScalarSubPlanStageRules)));
    RBO.AddStage(std::make_unique<TConstantFoldingStage>());

    TVector<std::unique_ptr<IRule>> inlineSimpleSubPlanStageRules;
    // At first try to pull correlated predicates up. The rules below create a dependent join for the rest.
    inlineSimpleSubPlanStageRules.emplace_back(std::make_unique<TPullUpCorrelatedPredicateRule>());
    inlineSimpleSubPlanStageRules.emplace_back(std::make_unique<TInlineSimpleInExistsSubplanRule>());
    inlineSimpleSubPlanStageRules.emplace_back(std::make_unique<TInlineGenericInExistsSubplanRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline in/exists subplans", std::move(inlineSimpleSubPlanStageRules)));

    // Decorrelation stage.
    TVector<std::unique_ptr<IRule>> decorrelationStageRules;
    // At first try to rewrite or completely eliminate a dependent join.
    decorrelationStageRules.emplace_back(std::make_unique<TRewriteDependentJoinToCrossJoinNoFreeVarsRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TRewriteDependentJoinToCrossJoinRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TEliminateDependentJoinDomainRule>());
    // Try to push dependent join.
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughFilterRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughMapRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughAggregateRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughUnionAllRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughJoinRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughReplicateRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughSortRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TPushDependentJoinThroughLimitRule>());
    decorrelationStageRules.emplace_back(std::make_unique<TDependentJoinNotSupportedRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Decorrelation", std::move(decorrelationStageRules)));

    // Rewrites can introduce new definitions. Remove dead ones and collapse
    // copies globally.
    RBO.AddStage(std::make_unique<TGlobalPruningStage>("Prune before inlining"));
    RBO.AddStage(std::make_unique<TGlobalInliningStage>("Inline definitions"));

    TVector<std::unique_ptr<IRule>> pushMapRules;
    pushMapRules.emplace_back(std::make_unique<TPushMapElementsThroughInputRule>());
    pushMapRules.emplace_back(std::make_unique<TPushMapElementsIntoMapRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Push map elements", std::move(pushMapRules)));

    // Logical state I
    TVector<std::unique_ptr<IRule>> logicalStage_I_Rules;
    logicalStage_I_Rules.emplace_back(std::make_unique<TMergeUnionAllRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TExtractCommonConjunctsRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TPushSimpleJoinFilterRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TPushFilterThroughAggregateRule>());
    logicalStage_I_Rules.emplace_back(std::make_unique<TPushFilterThroughUnionAllRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Logical rewrites I", std::move(logicalStage_I_Rules)));

    TVector<std::unique_ptr<IRule>> logicalStage_II_Rules;
    logicalStage_II_Rules.emplace_back(std::make_unique<TFuseFiltersRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TExtractCommonConjunctsRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushFilterThroughAggregateRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushFilterThroughUnionAllRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TEliminateLeftJoinRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushLimitIntoSortRule>());
    logicalStage_II_Rules.emplace_back(std::make_unique<TPushTopSortThroughUnionAllRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Logical rewrites II", std::move(logicalStage_II_Rules)));

    RBO.AddStage(std::make_unique<TGlobalPruningStage>("Pruning I", pruneKeyColumns));

    // Duplicate only after logical rewrites have removed unused work, while
    // reads and joins still have a logical representation supported by Copy().
    if (!allowChannelSpilling) {
        TVector<std::unique_ptr<IRule>> expandReplicateRules;
        expandReplicateRules.emplace_back(std::make_unique<TExpandReplicateRule>());
        RBO.AddStage(std::make_unique<TRuleBasedStage>("Expand shared subtrees", std::move(expandReplicateRules)));
        RBO.AddStage(std::make_unique<TGlobalInliningStage>("Inline duplicated definitions"));
    }

    // Physical stage.
    TVector<std::unique_ptr<IRule>> physicalStageRules;
    // For columnstore we push ranges before CBO.
    physicalStageRules.emplace_back(std::make_unique<TPushRangesRule>(NYql::EStorageType::ColumnStorage));
    physicalStageRules.emplace_back(std::make_unique<TPushOlapFilterRule>());
    physicalStageRules.emplace_back(std::make_unique<TPushOlapProjectionRule>());
    physicalStageRules.emplace_back(std::make_unique<TDisableBlocksOnColumnsLimitRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Physical rewrites I", std::move(physicalStageRules)));

    TVector<std::unique_ptr<IRule>> indexSelectionRules;
    indexSelectionRules.emplace_back(std::make_unique<TPushRangesRule>(NYql::EStorageType::RowStorage));
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Index selection rules", std::move(indexSelectionRules)));

    // CBO stages.
    TVector<std::unique_ptr<IRule>> initialCBOStageRules;
    initialCBOStageRules.emplace_back(std::make_unique<TPullUpMapOverCBORule>());
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
    // Only dead Map elements are pruned here, key columns included.
    RBO.AddStage(std::make_unique<TGlobalPruningStage>("Prune after CBO", /*pruneKeyColumns=*/true,
        EPruningScope::MapDefinitions));
    RBO.AddStage(std::make_unique<TGlobalInliningStage>("Inline after CBO"));


    TVector<std::unique_ptr<IRule>> inlineJoinFiltersAfterCBORules;
    inlineJoinFiltersAfterCBORules.emplace_back(std::make_unique<TInlineJoinFiltersRule>());
    inlineJoinFiltersAfterCBORules.emplace_back(std::make_unique<TFuseFiltersRule>());
    inlineJoinFiltersAfterCBORules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
    inlineJoinFiltersAfterCBORules.emplace_back(std::make_unique<TPushSimpleJoinFilterRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Inline join filters after CBO", std::move(inlineJoinFiltersAfterCBORules)));

    RBO.AddStage(std::make_unique<TGlobalPruningStage>("Pruning II"));
    RBO.AddStage(std::make_unique<TGlobalInliningStage>("Inline after join filters"));

    // Index lookup join has a different representation from regular join, so we need a special rewrite rule.
    TVector<std::unique_ptr<IRule>> physicalJoinRules;
    physicalJoinRules.emplace_back(std::make_unique<TRewriteJoinToIndexLookupJoinRule>());
    RBO.AddStage(std::make_unique<TRuleBasedStage>("Physical rewrites II", std::move(physicalJoinRules)));

    // Assign physical stages.
    RBO.AddStage(std::make_unique<TAssignStagesStage>());

    // Optimize physical stages.
    TVector<std::unique_ptr<IRule>> optimizePhysicalStagesRules;
    optimizePhysicalStagesRules.emplace_back(std::make_unique<TPropagateAggregateThroughStageRule>());
    // Matches an intermediate aggregate once it shares the stage of a column table read.
    optimizePhysicalStagesRules.emplace_back(std::make_unique<TPushOlapAggregateRule>());
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
