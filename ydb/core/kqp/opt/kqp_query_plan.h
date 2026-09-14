#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

#include <util/generic/string.h>

namespace NKqpProto {

class TKqpPhyQuery;
class TKqpStatsQuery;

} // namespace NKqpProto

namespace NYql::NDqProto {

class TDqExecutionStats;

} // namespace NYql::NDqProto

namespace NYql {

class TKikimrTablesData;
struct TKikimrConfiguration;
struct TTypeAnnotationContext;

} // namespace NYql

namespace NKikimrMiniKQL {

class TResult;

} // namespace NKikimrMiniKQL

namespace NKikimr::NKqp {

namespace NOpt {

struct TKqpOptimizeContext;

} // namespace NOpt

enum class EPlanTableReadType {
    Unspecified,
    FullScan,
    Scan,
    Lookup,
    MultiLookup,
};

enum class EPlanTableWriteType {
    Unspecified,
    Upsert,
    MultiUpsert,
    Erase,
    MultiErase,
    MultiReplace,
    MultiInsert,
    MultiUpdate,
    MultiUpsertIncrement,
};

/*
 * Set dqPlan in each physical transaction (TKqpPhyQuery.Transactions[].Plan). Common query plan with all
 * table accesses is stored in top-level TKqpPhyQuery.QueryPlan.
 */
void PhyQuerySetTxPlans(NKqpProto::TKqpPhyQuery& queryProto, const NYql::NNodes::TKqpPhysicalQuery& query,
    const NYql::NNodes::TKqpPhysicalQuery& peepHoleOptimizedQuery,
    TVector<TVector<NKikimrMiniKQL::TResult>> pureTxResults, NYql::TExprContext& ctx, const TString& database,
    const TString& cluster, const TIntrusivePtr<NYql::TKikimrTablesData> tablesData, TIntrusivePtr<NYql::TKikimrConfiguration> config,
    NYql::TTypeAnnotationContext& typeCtx, TIntrusivePtr<NOpt::TKqpOptimizeContext> optCtx);

/*
 * Fill stages in given txPlan with ExecutionStats fields. Each plan stage stores StageGuid which is
 * used to find corresponding TKqpStatsExecution object.
 */
TString AddExecStatsToTxPlan(const TString& txPlan, const NYql::NDqProto::TDqExecutionStats& stats, bool newRboEnabled);

TString SerializeAnalyzePlan(const NKqpProto::TKqpStatsQuery& queryStats, bool newRboEnabled, const TString& poolId = "");

TString SerializeScriptPlan(const TVector<const TString>& queryPlans);

} // namespace NKikimr::NKqp
