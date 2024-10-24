#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_log.h>

#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/json/writer/json.h>

namespace NKikimr {
namespace NKqp {

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
};

/*
 * Set dqPlan in each physical transaction (TKqpPhyQuery.Transactions[].Plan). Common query plan with all
 * table accesses is stored in top-level TKqpPhyQuery.QueryPlan.
 */
void PhyQuerySetTxPlans(NKqpProto::TKqpPhyQuery& queryProto, const NYql::NNodes::TKqpPhysicalQuery& query,
    TVector<TVector<NKikimrMiniKQL::TResult>> pureTxResults, NYql::TExprContext& ctx, const TString& cluster,
    const TIntrusivePtr<NYql::TKikimrTablesData> tablesData, NYql::TKikimrConfiguration::TPtr config,
    NYql::TTypeAnnotationContext& typeCtx, TIntrusivePtr<NOpt::TKqpOptimizeContext> optCtx);

/*
 * Fill stages in given txPlan with ExecutionStats fields. Each plan stage stores StageGuid which is
 * used to find corresponding TKqpStatsExecution object.
 */
TString AddExecStatsToTxPlan(const TString& txPlan, const NYql::NDqProto::TDqExecutionStats& stats);

TString SerializeAnalyzePlan(const NKqpProto::TKqpStatsQuery& queryStats, const TString& poolId = "");

TString SerializeScriptPlan(const TVector<const TString>& queryPlans);

} // namespace NKqp
} // namespace NKikimr
