#pragma once

#include "datashard.h"
#include "datashard_active_transaction.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NDataShard {

bool KqpValidateTransaction(const ::google::protobuf::RepeatedPtrField<::NYql::NDqProto::TDqTask> & tasks, bool isImmediate,
    ui64 txId, const TActorContext& ctx, bool& hasPersistentChannels);

void KqpSetTxKeys(ui64 tabletId, ui64 taskId, const TUserTable* tableInfo,
    const NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta& meta, const NScheme::TTypeRegistry& typeRegistry,
    const TActorContext& ctx, TEngineBay& engineBay);

void KqpSetTxLocksKeys(const NKikimrTxDataShard::TKqpLocks& locks, const TSysLocks& sysLocks, TEngineBay& engineBay);

NYql::NDq::ERunStatus KqpRunTransaction(const TActorContext& ctx, ui64 txId,
    const NKikimrTxDataShard::TKqpLocks& kqpLocks, bool useGenericReadSets, NKqp::TKqpTasksRunner& tasksRunner);

THolder<TEvDataShard::TEvProposeTransactionResult> KqpCompleteTransaction(const TActorContext& ctx,
    ui64 origin, ui64 txId, const TInputOpData::TInReadSets* inReadSets,
    const NKikimrTxDataShard::TKqpLocks& kqpLocks, bool useGenericReadSets, NKqp::TKqpTasksRunner& tasksRunner,
    const NMiniKQL::TKqpDatashardComputeContext& computeCtx);

void KqpFillOutReadSets(TOutputOpData::TOutReadSets& outReadSets, const NKikimrTxDataShard::TKqpLocks& kqpLocks, 
    bool hasKqpLocks, bool useGenericReadSets,
    NKqp::TKqpTasksRunner& tasksRunner, TSysLocks& sysLocks, ui64 tabletId);

void KqpPrepareInReadsets(TInputOpData::TInReadSets& inReadSets,
    const NKikimrTxDataShard::TKqpLocks& kqpLocks, const NKqp::TKqpTasksRunner& tasksRunner, ui64 tabletId);

bool KqpValidateLocks(ui64 origin, TActiveTransaction* tx, TSysLocks& sysLocks);
bool KqpValidateVolatileTx(ui64 origin, TActiveTransaction* tx, TSysLocks& sysLocks);

void KqpEraseLocks(ui64 origin, TActiveTransaction* tx, TSysLocks& sysLocks);
void KqpCommitLocks(ui64 origin, TActiveTransaction* tx, const TRowVersion& writeVersion, TDataShard& dataShard);

void KqpUpdateDataShardStatCounters(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters);

void KqpFillTxStats(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters,
    TEvDataShard::TEvProposeTransactionResult& result);

void KqpFillStats(TDataShard& dataShard, const NKqp::TKqpTasksRunner& tasksRunner,
    NMiniKQL::TKqpDatashardComputeContext& computeCtx, const NYql::NDqProto::EDqStatsMode& statsMode,
    TEvDataShard::TEvProposeTransactionResult& result);

NYql::NDq::TDqTaskRunnerMemoryLimits DefaultKqpDataReqMemoryLimits();
THolder<NYql::NDq::IDqTaskRunnerExecutionContext> DefaultKqpExecutionContext();

} // namespace NDataShard
} // namespace NKikimr
