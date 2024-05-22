#pragma once

#include "datashard.h"

#include "operation.h"
#include "key_validator.h"
#include "datashard_user_db.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/locks/locks_db.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NDataShard {

bool KqpValidateTransaction(const ::google::protobuf::RepeatedPtrField<::NYql::NDqProto::TDqTask> & tasks, bool isImmediate,
    ui64 txId, const TActorContext& ctx, bool& hasPersistentChannels);

void KqpSetTxKeys(ui64 tabletId, ui64 taskId, const TUserTable* tableInfo,
    const NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta& meta, const NScheme::TTypeRegistry& typeRegistry,
    const TActorContext& ctx, TKeyValidator& keyValidator);

void KqpSetTxLocksKeys(const NKikimrDataEvents::TKqpLocks& locks, const TSysLocks& sysLocks, TKeyValidator& keyValidator);

NYql::NDq::ERunStatus KqpRunTransaction(const TActorContext& ctx, ui64 txId, bool useGenericReadSets, NKqp::TKqpTasksRunner& tasksRunner);

THolder<TEvDataShard::TEvProposeTransactionResult> KqpCompleteTransaction(const TActorContext& ctx,
    ui64 tabletId, ui64 txId, const TInputOpData::TInReadSets* inReadSets, bool useGenericReadSets, NKqp::TKqpTasksRunner& tasksRunner,
    const NMiniKQL::TKqpDatashardComputeContext& computeCtx);

void KqpFillOutReadSets(TOutputOpData::TOutReadSets& outReadSets, const NKikimrDataEvents::TKqpLocks& kqpLocks, 
    bool useGenericReadSets, NKqp::TKqpTasksRunner* tasksRunner, TSysLocks& sysLocks, ui64 tabletId);

void KqpPrepareInReadsets(TInputOpData::TInReadSets& inReadSets,
    const NKikimrDataEvents::TKqpLocks& kqpLocks, const NKqp::TKqpTasksRunner* tasksRunner, ui64 tabletId);

std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateLocks(ui64 tabletId, TSysLocks& sysLocks, 
    const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, const TInputOpData::TInReadSets& inReadSets);
std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateVolatileTx(ui64 tabletId, TSysLocks& sysLocks, 
    const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, ui64 txId, const TVector<NKikimrTx::TEvReadSet>& delayedInReadSets, 
    TInputOpData::TAwaitingDecisions& awaitingDecisions, TOutputOpData::TOutReadSets& outReadSets);

bool KqpLocksHasArbiter(const NKikimrDataEvents::TKqpLocks* kqpLocks);
bool KqpLocksIsArbiter(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks);

void KqpEraseLocks(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks);
void KqpCommitLocks(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks, const TRowVersion& writeVersion, IDataShardUserDb& userDb);

void KqpUpdateDataShardStatCounters(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters);

void KqpFillTxStats(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters, NKikimrQueryStats::TTxStats& stats);

void KqpFillStats(TDataShard& dataShard, const NKqp::TKqpTasksRunner& tasksRunner,
    NMiniKQL::TKqpDatashardComputeContext& computeCtx, const NYql::NDqProto::EDqStatsMode& statsMode,
    TEvDataShard::TEvProposeTransactionResult& result);

NYql::NDq::TDqTaskRunnerMemoryLimits DefaultKqpDataReqMemoryLimits();
THolder<NYql::NDq::IDqTaskRunnerExecutionContext> DefaultKqpExecutionContext();

} // namespace NDataShard
} // namespace NKikimr
