#pragma once

#include "datashard.h"

#include "operation.h"
#include "key_validator.h"
#include "datashard_user_db.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/locks/locks_db.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NDataShard {

void KqpSetTxLocksKeys(const NKikimrDataEvents::TKqpLocks& locks, const TSysLocks& sysLocks, TKeyValidator& keyValidator);

void KqpPrepareInReadsets(TInputOpData::TInReadSets& inReadSets,
    const NKikimrDataEvents::TKqpLocks& kqpLocks, ui64 tabletId);

std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateLocks(ui64 tabletId, TSysLocks& sysLocks,
    const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, const TInputOpData::TInReadSets& inReadSets);
std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateVolatileTx(ui64 tabletId, TSysLocks& sysLocks,
    const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, ui64 txId, const TVector<NKikimrTx::TEvReadSet>& delayedInReadSets,
    TInputOpData::TAwaitingDecisions& awaitingDecisions, TOutputOpData::TOutReadSets& outReadSets);
void KqpFillOutReadSets(TOutputOpData::TOutReadSets& outReadSets, const NKikimrDataEvents::TKqpLocks* kqpLocks,
    NKikimrTx::TReadSetData::EDecision decision, ui64 origin);
void KqpFillOutReadSets(TOutputOpData::TOutReadSets& outReadSets, const NKikimrDataEvents::TKqpLocks& kqpLocks, bool useGenericReadSets, TSysLocks& sysLocks, ui64 tabletId);


bool KqpLocksHasArbiter(const NKikimrDataEvents::TKqpLocks* kqpLocks);
bool KqpLocksIsArbiter(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks);

void KqpEraseLocks(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks);
void KqpCommitLocks(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks, IDataShardUserDb& userDb);

void KqpUpdateDataShardStatCounters(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters);

void KqpFillTxStats(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters, NKikimrQueryStats::TTxStats& stats);


} // namespace NDataShard
} // namespace NKikimr
