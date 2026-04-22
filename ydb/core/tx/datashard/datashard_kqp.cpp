#include "datashard_kqp.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/locks/locks.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>

#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NDataShard {

namespace {


using namespace NYql;

bool NeedValidateLocks(NKikimrDataEvents::TKqpLocks::ELocksOp op) {
    switch (op) {
        case NKikimrDataEvents::TKqpLocks::Commit:
            return true;

        case NKikimrDataEvents::TKqpLocks::Rollback:
        case NKikimrDataEvents::TKqpLocks::Unspecified:
            return false;
    }
}

bool NeedEraseLocks(NKikimrDataEvents::TKqpLocks::ELocksOp op) {
    switch (op) {
        case NKikimrDataEvents::TKqpLocks::Commit:
        case NKikimrDataEvents::TKqpLocks::Rollback:
            return true;

        case NKikimrDataEvents::TKqpLocks::Unspecified:
            return false;
    }
}

bool NeedCommitLocks(NKikimrDataEvents::TKqpLocks::ELocksOp op) {
    switch (op) {
        case NKikimrDataEvents::TKqpLocks::Commit:
            return true;

        case NKikimrDataEvents::TKqpLocks::Rollback:
        case NKikimrDataEvents::TKqpLocks::Unspecified:
            return false;
    }
}

TVector<TCell> MakeLockKey(const NKikimrDataEvents::TLock& lockProto) {
    auto lockId = lockProto.GetLockId();
    auto lockDatashard = lockProto.GetDataShard();
    auto lockSchemeShard = lockProto.GetSchemeShard();
    auto lockPathId = lockProto.GetPathId();

    Y_ASSERT(TCell::CanInline(sizeof(lockId)));
    Y_ASSERT(TCell::CanInline(sizeof(lockDatashard)));
    Y_ASSERT(TCell::CanInline(sizeof(lockSchemeShard)));
    Y_ASSERT(TCell::CanInline(sizeof(lockPathId)));

    TVector<TCell> lockKey{
        TCell(reinterpret_cast<const char*>(&lockId), sizeof(lockId)),
        TCell(reinterpret_cast<const char*>(&lockDatashard), sizeof(lockDatashard)),
        TCell(reinterpret_cast<const char*>(&lockSchemeShard), sizeof(lockSchemeShard)),
        TCell(reinterpret_cast<const char*>(&lockPathId), sizeof(lockPathId))};

    return lockKey;
}

// returns list of broken locks
TVector<NKikimrDataEvents::TLock> ValidateLocks(const NKikimrDataEvents::TKqpLocks& txLocks, TSysLocks& sysLocks, ui64 tabletId)
{
    TVector<NKikimrDataEvents::TLock> brokenLocks;

    if (!NeedValidateLocks(txLocks.GetOp())) {
        return {};
    }

    for (auto& lockProto : txLocks.GetLocks()) {
        if (lockProto.GetDataShard() != tabletId) {
            continue;
        }

        auto lockKey = MakeLockKey(lockProto);

        auto lock = sysLocks.GetLock(lockKey);
        if (lock.Generation != lockProto.GetGeneration() || lock.Counter != lockProto.GetCounter()) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "ValidateLocks: broken lock " << lockProto.GetLockId() << " expected " << lockProto.GetGeneration() << ":" << lockProto.GetCounter() << " found " << lock.Generation << ":" << lock.Counter);
            brokenLocks.emplace_back(lockProto);
        }
    }

    return brokenLocks;
}

bool SendLocks(const NKikimrDataEvents::TKqpLocks& locks, ui64 shardId) {
    auto& sendingShards = locks.GetSendingShards();
    auto it = std::find(sendingShards.begin(), sendingShards.end(), shardId);
    return it != sendingShards.end();
}

bool ReceiveLocks(const NKikimrDataEvents::TKqpLocks& locks, ui64 shardId) {
    auto& receivingShards = locks.GetReceivingShards();
    auto it = std::find(receivingShards.begin(), receivingShards.end(), shardId);
    return it != receivingShards.end();
}

}  // namespace


void KqpSetTxLocksKeys(const NKikimrDataEvents::TKqpLocks& locks, const TSysLocks& sysLocks, TKeyValidator& keyValidator) {
    if (locks.LocksSize() == 0) {
        return;
    }

    static TTableId sysLocksTableId = TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks2);
    static TVector<NScheme::TTypeInfo> lockRowType = {
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
    };

    for (auto& lock : locks.GetLocks()) {
        auto lockKey = MakeLockKey(lock);
        if (sysLocks.IsMyKey(lockKey)) {
            auto point = TTableRange(lockKey, true, {}, true, /* point */ true);
            if (NeedValidateLocks(locks.GetOp())) {
                keyValidator.AddReadRange(sysLocksTableId, {}, point, lockRowType);
            }
            if (NeedEraseLocks(locks.GetOp())) {
                keyValidator.AddWriteRange(sysLocksTableId, point, lockRowType, {}, /* isPureEraseOp */ true);
            }
        }
    }
}


std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateLocks(ui64 origin, TSysLocks& sysLocks, const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, const TInputOpData::TInReadSets& inReadSets) {
    if (kqpLocks == nullptr || !NeedValidateLocks(kqpLocks->GetOp())) {
        return {true, {}};
    }

    auto brokenLocks = ValidateLocks(*kqpLocks, sysLocks, origin);

    if (!brokenLocks.empty()) {
        return {false, std::move(brokenLocks)};
    }

    for (const auto& readSet : inReadSets) {
        for (const auto& data : readSet.second) {
            if (useGenericReadSets) {
                NKikimrTx::TReadSetData genericData;
                bool ok = genericData.ParseFromString(data.Body);
                Y_ENSURE(ok, "Failed to parse generic readset from " << readSet.first.first << " to " << readSet.first.second << " tabletId " << data.Origin);

                if (genericData.GetDecision() != NKikimrTx::TReadSetData::DECISION_COMMIT) {
                    // Note: we don't know details on what failed at that shard
                    return {false, {}};
                }
            } else {
                NKikimrTxDataShard::TKqpReadset kqpReadset;
                Y_PROTOBUF_SUPPRESS_NODISCARD kqpReadset.ParseFromString(data.Body);

                if (kqpReadset.HasValidateLocksResult()) {
                    auto& validateResult = kqpReadset.GetValidateLocksResult();
                    if (!validateResult.GetSuccess()) {
                        TVector<NKikimrDataEvents::TLock> brokenLocks;
                        brokenLocks.reserve(validateResult.GetBrokenLocks().size());
                        std::copy(validateResult.GetBrokenLocks().begin(), validateResult.GetBrokenLocks().end(), std::back_inserter(brokenLocks));
                        return {false, std::move(brokenLocks)};
                    }
                }
            }
        }
    }

    return {true, {}};
}

bool KqpLocksHasArbiter(const NKikimrDataEvents::TKqpLocks* kqpLocks) {
    return kqpLocks && kqpLocks->GetArbiterShard() != 0;
}

bool KqpLocksIsArbiter(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks) {
    return KqpLocksHasArbiter(kqpLocks) && kqpLocks->GetArbiterShard() == tabletId;
}

std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateVolatileTx(ui64 origin, TSysLocks& sysLocks, const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, ui64 txId, const TVector<NKikimrTx::TEvReadSet>& delayedInReadSets, TInputOpData::TAwaitingDecisions& awaitingDecisions, TOutputOpData::TOutReadSets& outReadSets) {
    if (kqpLocks == nullptr || !NeedValidateLocks(kqpLocks->GetOp())) {
        return {true, {}};
    }

    // Volatile transactions cannot work with non-generic readsets
    Y_ENSURE(useGenericReadSets);

    // We may have some stale data since before the restart
    // We expect all stale data to be cleared on restarts
    Y_ENSURE(outReadSets.empty());
    Y_ENSURE(awaitingDecisions.empty());

    const bool hasArbiter = KqpLocksHasArbiter(kqpLocks);
    const bool isArbiter = KqpLocksIsArbiter(origin, kqpLocks);

    // Note: usually all shards send locks, since they either have side effects or need to validate locks
    // However it is technically possible to have pure-read shards, that don't contribute to the final decision
    bool sendLocks = SendLocks(*kqpLocks, origin);
    if (sendLocks) {
        // Note: it is possible to have no locks
        auto brokenLocks = ValidateLocks(*kqpLocks, sysLocks, origin);

        if (!brokenLocks.empty()) {
            return {false, std::move(brokenLocks)};
        }
    } else {
        Y_ENSURE(!isArbiter, "Arbiter is not in the sending shards set");
    }

    bool receiveLocks = ReceiveLocks(*kqpLocks, origin);
    if (receiveLocks) {
        // Note: usually only shards with side-effects receive locks, since they
        //       need the final outcome to decide whether to commit or abort.
        for (ui64 srcTabletId : kqpLocks->GetSendingShards()) {
            if (srcTabletId == origin) {
                // Don't await decision from ourselves
                continue;
            }

            if (hasArbiter && !isArbiter && srcTabletId != kqpLocks->GetArbiterShard()) {
                // Non-arbiter shards only await decision from the arbiter
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Will wait for volatile decision from " << srcTabletId << " to " << origin);

            awaitingDecisions.insert(srcTabletId);
        }

        bool aborted = false;

        for (const auto& record : delayedInReadSets) {
            ui64 srcTabletId = record.GetTabletSource();
            ui64 dstTabletId = record.GetTabletDest();
            if (dstTabletId != origin) {
                LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Ignoring unexpected readset from " << srcTabletId << " to " << dstTabletId << " for txId# " << txId << " at tablet " << origin);
                continue;
            }
            if (!awaitingDecisions.contains(srcTabletId)) {
                continue;
            }

            if (record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA) {
                Y_ENSURE(!(record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET), "Unexpected FLAG_EXPECT_READSET + FLAG_NO_DATA in delayed readsets");

                // No readset data: participant aborted the transaction
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Processed readset without data from" << srcTabletId << " to " << dstTabletId << " will abort txId# " << txId);
                aborted = true;
                break;
            }

            NKikimrTx::TReadSetData data;
            bool ok = data.ParseFromString(record.GetReadSet());
            Y_ENSURE(ok, "Failed to parse readset from " << srcTabletId << " to " << dstTabletId);

            if (data.GetDecision() != NKikimrTx::TReadSetData::DECISION_COMMIT) {
                // Explicit decision that is not a commit, need to abort
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Processed decision " << ui32(data.GetDecision()) << " from " << srcTabletId << " to " << dstTabletId << " for txId# " << txId);
                aborted = true;
                break;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Processed commit decision from " << srcTabletId << " to " << dstTabletId << " for txId# " << txId);
            awaitingDecisions.erase(srcTabletId);
        }

        if (aborted) {
            awaitingDecisions.clear();
            return {false, {}};
        }
    } else {
        Y_ENSURE(!isArbiter, "Arbiter is not in the receiving shards set");
    }

    if (sendLocks) {
        // We need to form decision readsets for all other participants
        for (ui64 dstTabletId : kqpLocks->GetReceivingShards()) {
            if (dstTabletId == origin) {
                // Don't send readsets to ourselves
                continue;
            }

            if (hasArbiter && !isArbiter && dstTabletId != kqpLocks->GetArbiterShard()) {
                // Non-arbiter shards only send locks to the arbiter
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Send commit decision from " << origin << " to " << dstTabletId);

            auto key = std::make_pair(origin, dstTabletId);
            NKikimrTx::TReadSetData data;
            data.SetDecision(NKikimrTx::TReadSetData::DECISION_COMMIT);

            TString bodyStr;
            bool ok = data.SerializeToString(&bodyStr);
            Y_ENSURE(ok, "Failed to serialize readset from " << key.first << " to " << key.second);

            outReadSets[key] = std::move(bodyStr);
        }
    }

    return {true, {}};
}

void KqpFillOutReadSets(TOutputOpData::TOutReadSets& outReadSets, const NKikimrDataEvents::TKqpLocks& kqpLocks, bool useGenericReadSets, TSysLocks& sysLocks, ui64 tabletId)
{
    TMap<std::pair<ui64, ui64>, NKikimrTxDataShard::TKqpReadset> readsetData;

    NKikimrTx::TReadSetData::EDecision decision = NKikimrTx::TReadSetData::DECISION_COMMIT;
    TMap<std::pair<ui64, ui64>, NKikimrTx::TReadSetData> genericData;

    if (SendLocks(kqpLocks, tabletId) && !kqpLocks.GetReceivingShards().empty()) {
        auto brokenLocks = ValidateLocks(kqpLocks, sysLocks, tabletId);

        NKikimrTxDataShard::TKqpValidateLocksResult validateLocksResult;
        validateLocksResult.SetSuccess(brokenLocks.empty());

        for (auto& lock : brokenLocks) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Found broken lock: " << lock.ShortDebugString());
            if (useGenericReadSets) {
                decision = NKikimrTx::TReadSetData::DECISION_ABORT;
            } else {
                validateLocksResult.AddBrokenLocks()->Swap(&lock);
            }
        }

        for (auto& dstTabletId : kqpLocks.GetReceivingShards()) {
            if (tabletId == dstTabletId) {
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Send locks from " << tabletId << " to " << dstTabletId << ", locks: " << validateLocksResult.ShortDebugString());

            auto key = std::make_pair(tabletId, dstTabletId);
            if (useGenericReadSets) {
                genericData[key].SetDecision(decision);
            } else {
                readsetData[key].MutableValidateLocksResult()->CopyFrom(validateLocksResult);
            }
        }
    }

    if (useGenericReadSets) {
        for (const auto& [key, data] : readsetData) {
            bool ok = genericData[key].MutableData()->PackFrom(data);
            Y_ENSURE(ok, "Failed to pack readset data from " << key.first << " to " << key.second);
        }

        for (auto& [key, data] : genericData) {
            if (!data.HasDecision()) {
                data.SetDecision(decision);
            }

            TString bodyStr;
            bool ok = data.SerializeToString(&bodyStr);
            Y_ENSURE(ok, "Failed to serialize readset from " << key.first << " to " << key.second);

            outReadSets[key] = std::move(bodyStr);
        }
    } else {
        for (auto& [key, data] : readsetData) {
            TString bodyStr;
            Y_PROTOBUF_SUPPRESS_NODISCARD data.SerializeToString(&bodyStr);

            outReadSets[key] = bodyStr;
        }
    }
}

void KqpFillOutReadSets(TOutputOpData::TOutReadSets& outReadSets, const NKikimrDataEvents::TKqpLocks* kqpLocks,
    NKikimrTx::TReadSetData::EDecision decision, ui64 origin)
{
    if (kqpLocks && NeedValidateLocks(kqpLocks->GetOp())) {
        bool sendLocks = SendLocks(*kqpLocks, origin);

        if (sendLocks && !kqpLocks->GetReceivingShards().empty()) {
            const bool hasArbiter = KqpLocksHasArbiter(kqpLocks);
            const bool isArbiter = KqpLocksIsArbiter(origin, kqpLocks);

            NKikimrTx::TReadSetData data;
            data.SetDecision(decision);

            TString bodyStr;
            bool ok = data.SerializeToString(&bodyStr);
            Y_ENSURE(ok, "Failed to serialize a generic decision readset");

            for (ui64 dstTabletId : kqpLocks->GetReceivingShards()) {
                if (dstTabletId == origin) {
                    // Don't send readsets to ourselves
                    continue;
                }

                if (hasArbiter && !isArbiter && dstTabletId != kqpLocks->GetArbiterShard()) {
                    // Non-arbiter shards only send locks to the arbiter
                    continue;
                }

                auto key = std::make_pair(origin, dstTabletId);
                outReadSets[key] = bodyStr;
            }
        }
    }
}

void KqpEraseLocks(ui64 origin, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks) {
    if (kqpLocks == nullptr || !NeedEraseLocks(kqpLocks->GetOp())) {
        return;
    }

    for (const auto& lockProto : kqpLocks->GetLocks()) {
        if (lockProto.GetDataShard() != origin) {
            continue;
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "KqpEraseLock " << lockProto.ShortDebugString());

        auto lockKey = MakeLockKey(lockProto);
        sysLocks.EraseLock(lockKey);
    }
}

void KqpCommitLocks(ui64 origin, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks, IDataShardUserDb& userDb) {
    if (kqpLocks == nullptr) {
        return;
    }

    if (NeedCommitLocks(kqpLocks->GetOp())) {
        for (const auto& lockProto : kqpLocks->GetLocks()) {
            if (lockProto.GetDataShard() != origin) {
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "KqpCommitLock " << lockProto.ShortDebugString());

            auto lockKey = MakeLockKey(lockProto);
            sysLocks.CommitLock(lockKey);

            TTableId tableId(lockProto.GetSchemeShard(), lockProto.GetPathId());
            auto txId = lockProto.GetLockId();

            userDb.CommitChanges(tableId, txId);
        }
    } else {
        KqpEraseLocks(origin, kqpLocks, sysLocks);
    }
}

void KqpPrepareInReadsets(TInputOpData::TInReadSets& inReadSets, const NKikimrDataEvents::TKqpLocks& kqpLocks, ui64 tabletId)
{

    if (ReceiveLocks(kqpLocks, tabletId)) {
        for (ui64 shardId : kqpLocks.GetSendingShards()) {
            if (shardId == tabletId) {
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Prepare InReadsets from " << shardId << " to " << tabletId);

            auto key = std::make_pair(shardId, tabletId);
            inReadSets.emplace(key, TVector<TRSData>());
        }
    }
}

void KqpUpdateDataShardStatCounters(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters) {
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_ROW, counters.NSelectRow);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE, counters.NSelectRange);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_UPDATE_ROW, counters.NUpdateRow);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_ERASE_ROW, counters.NEraseRow);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_ROW_BYTES, counters.SelectRowBytes);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS, counters.SelectRangeRows);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES, counters.SelectRangeBytes);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_ROW_SKIPS, counters.SelectRangeDeletedRowSkips);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_UPDATE_ROW_BYTES, counters.UpdateRowBytes);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_ERASE_ROW_BYTES, counters.EraseRowBytes);
    if (counters.NSelectRow > 0) {
        dataShard.IncCounter(COUNTER_SELECT_ROWS_PER_REQUEST, counters.NSelectRow);
    }
    if (counters.NSelectRange > 0) {
        dataShard.IncCounter(COUNTER_RANGE_READ_ROWS_PER_REQUEST, counters.SelectRangeRows);
    }
}

void KqpFillTxStats(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters, NKikimrQueryStats::TTxStats& stats)
{
    auto& perTable = *stats.AddTableAccessStats();
    perTable.MutableTableInfo()->SetSchemeshardId(dataShard.GetPathOwnerId());
    Y_ENSURE(dataShard.GetUserTables().size() == 1, "TODO: Fix handling of collocated tables");
    auto tableInfo = dataShard.GetUserTables().begin();
    perTable.MutableTableInfo()->SetPathId(tableInfo->first);
    perTable.MutableTableInfo()->SetName(tableInfo->second->Path);
    if (counters.NSelectRow) {
        perTable.MutableSelectRow()->SetCount(counters.NSelectRow);
        perTable.MutableSelectRow()->SetRows(counters.SelectRowRows);
        perTable.MutableSelectRow()->SetBytes(counters.SelectRowBytes);
    }
    if (counters.NSelectRange) {
        perTable.MutableSelectRange()->SetCount(counters.NSelectRange);
        perTable.MutableSelectRange()->SetRows(counters.SelectRangeRows);
        perTable.MutableSelectRange()->SetBytes(counters.SelectRangeBytes);
    }
    if (counters.NUpdateRow) {
        perTable.MutableUpdateRow()->SetCount(counters.NUpdateRow);
        perTable.MutableUpdateRow()->SetRows(counters.NUpdateRow);
        perTable.MutableUpdateRow()->SetBytes(counters.UpdateRowBytes);
    }
    if (counters.NEraseRow) {
        perTable.MutableEraseRow()->SetCount(counters.NEraseRow);
        perTable.MutableEraseRow()->SetRows(counters.NEraseRow);
        perTable.MutableEraseRow()->SetBytes(counters.EraseRowBytes);
    }
}

}  // namespace NDataShard
}  // namespace NKikimr
