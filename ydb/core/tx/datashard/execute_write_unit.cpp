#include "datashard_write_operation.h"
#include "datashard_pipeline.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"
#include "datashard_kqp.h"
#include "datashard_integrity_trails.h"

#include <ydb/core/engine/mkql_engine_flat_host.h>
#include <ydb/library/aclib/user_context.h>

#include <algorithm>
#include <vector>

namespace NKikimr {
namespace NDataShard {

// Adds optional Count/Rows/Bytes fields of src on top of dst (both absent in dst stay absent).
template <typename T>
static void MergeOpStats(T& dst, const T& src) {
    if (src.HasCount()) {
        dst.SetCount(src.GetCount() + (dst.HasCount() ? dst.GetCount() : 0));
    }
    if (src.HasRows()) {
        dst.SetRows(src.GetRows() + (dst.HasRows() ? dst.GetRows() : 0));
    }
    if (src.HasBytes()) {
        dst.SetBytes(src.GetBytes() + (dst.HasBytes() ? dst.GetBytes() : 0));
    }
}

// Merges the additive counters of src on top of dst; descriptive sizes missing
// in dst are taken from src.
static void MergePerShardStatsCounters(
        NKikimrQueryStats::TPerShardStats& dst,
        const NKikimrQueryStats::TPerShardStats& src) {
    if (src.HasCpuTimeUsec()) {
        dst.SetCpuTimeUsec(src.GetCpuTimeUsec() + (dst.HasCpuTimeUsec() ? dst.GetCpuTimeUsec() : 0));
    }
    if (src.HasOutgoingReadSetsCount()) {
        dst.SetOutgoingReadSetsCount(src.GetOutgoingReadSetsCount() + (dst.HasOutgoingReadSetsCount() ? dst.GetOutgoingReadSetsCount() : 0));
    }
    if (!dst.HasProgramSize() && src.HasProgramSize()) {
        dst.SetProgramSize(src.GetProgramSize());
    }
    if (!dst.HasReplySize() && src.HasReplySize()) {
        dst.SetReplySize(src.GetReplySize());
    }
}

// Adds the statistics of already applied (duplicate) operations on top of the fresh
// statistics of the newly applied operations: the writer never saw the replies of the
// lost deliveries, so the reply of a partially duplicate batch must report the sum.
// Duration and CPU time describe the current delivery and are not merged.
static void MergeDuplicateTxStats(NKikimrQueryStats::TTxStats& dst, const NKikimrQueryStats::TTxStats& src) {
    Y_ENSURE(src.TableAccessStatsSize() <= 1);
    if (src.TableAccessStatsSize() == 1) {
        const auto& srcTable = src.GetTableAccessStats(0);
        auto* dstTable = dst.MutableTableAccessStats(0);
        if (srcTable.HasSelectRow()) {
            MergeOpStats(*dstTable->MutableSelectRow(), srcTable.GetSelectRow());
        }
        if (srcTable.HasSelectRange()) {
            MergeOpStats(*dstTable->MutableSelectRange(), srcTable.GetSelectRange());
        }
        if (srcTable.HasUpdateRow()) {
            MergeOpStats(*dstTable->MutableUpdateRow(), srcTable.GetUpdateRow());
        }
        if (srcTable.HasEraseRow()) {
            MergeOpStats(*dstTable->MutableEraseRow(), srcTable.GetEraseRow());
        }
        if (srcTable.HasAffectedRows()) {
            dstTable->SetAffectedRows(srcTable.GetAffectedRows() + (dstTable->HasAffectedRows() ? dstTable->GetAffectedRows() : 0));
        }
    }

    // Sorts dst's per-shard entries in place and merges src's sorted entries
    // into it from the back, so no extra copies are made: every entry is written
    // exactly once, to its final position. Entries of the same shard add their
    // counters up. The fresh reply gets its own entry from FillExecutionStats
    // when it is sent.
    std::vector<NKikimrQueryStats::TPerShardStats> srcShards(
        src.GetPerShardStats().begin(), src.GetPerShardStats().end());
    const auto byShardId = [](const auto& a, const auto& b) {
        return a.GetShardId() < b.GetShardId();
    };
    std::sort(srcShards.begin(), srcShards.end(), byShardId);

    auto* dstList = dst.MutablePerShardStats();
    // Reserve up front so that adding the extension entries never reallocates
    // the pointer array: the reverse iterators taken below stay valid.
    dstList->Reserve(dstList->size() + static_cast<int>(srcShards.size()));
    std::sort(dstList->begin(), dstList->end(), byShardId);
    auto dstRead = dstList->rbegin();
    const auto dstReadEnd = dstList->rend();
    std::for_each(srcShards.cbegin(), srcShards.cend(), [dstList](const auto&) {
        dstList->Add();
    });
    auto write = dstList->rbegin();
    auto srcRead = srcShards.crbegin();
    while (srcRead != srcShards.crend()) {
        if (dstRead == dstReadEnd || dstRead->GetShardId() < srcRead->GetShardId()) {
            *write++ = *srcRead++;
        } else if (dstRead->GetShardId() > srcRead->GetShardId()) {
            *write++ = *dstRead++;
        } else {
            *write = *dstRead++;
            MergePerShardStatsCounters(*write++, *srcRead++);
        }
    }

    if (src.HasLocksBrokenAsBreaker()) {
        dst.SetLocksBrokenAsBreaker(src.GetLocksBrokenAsBreaker() + (dst.HasLocksBrokenAsBreaker() ? dst.GetLocksBrokenAsBreaker() : 0));
    }
    if (src.HasLocksBrokenAsVictim()) {
        dst.SetLocksBrokenAsVictim(src.GetLocksBrokenAsVictim() + (dst.HasLocksBrokenAsVictim() ? dst.GetLocksBrokenAsVictim() : 0));
    }
    for (ui64 id : src.GetBreakerQuerySpanIds()) {
        dst.AddBreakerQuerySpanIds(id);
    }
    for (ui64 id : src.GetDeferredBreakerQuerySpanIds()) {
        dst.AddDeferredBreakerQuerySpanIds(id);
    }
    for (ui32 id : src.GetDeferredBreakerNodeIds()) {
        dst.AddDeferredBreakerNodeIds(id);
    }
}

class TExecuteWriteUnit : public TExecutionUnit {
public:
    TExecuteWriteUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::ExecuteWrite, true, self, pipeline)
    {
    }

    ~TExecuteWriteUnit()
    {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

        if (writeOp->GetWriteResult() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
            return true;
        }

        if (op->HasWaitingForGlobalTxIdFlag()) {
            return false;
        }

        if (DataShard.IsStopping()) {
            // Avoid doing any new work when datashard is stopping
            return false;
        }

        return !op->HasRuntimeConflicts();
    }

    void FillDeferredBreakerInfo(ui64 lockTxId, NKikimrQueryStats::TTxStats* txStats) {
        auto lockInfo = DataShard.SysLocksTable().GetRawLock(lockTxId);
        if (!lockInfo || !lockInfo->GetBreakVersion()) {
            return;
        }
        if (lockInfo->GetBreakerQuerySpanId() && !lockInfo->IsBreakerConsumed()) {
            txStats->AddDeferredBreakerQuerySpanIds(lockInfo->GetBreakerQuerySpanId());
            txStats->AddDeferredBreakerNodeIds(lockInfo->GetBreakerNodeId());
        }
    }

    // Filter out self-breaks from locksBrokenByTx, log breaker TLI events, and update TxStats.
    // Returns the count of locks broken by OTHER transactions (excluding self-breaks).
    size_t HandleBreakerLocks(
        const TVector<ui64>& locksBrokenByTx, ui64 selfTxId, ui64 querySpanId,
        TSysLocks& sysLocks, NKikimrDataEvents::TEvWriteResult& record,
        const TActorContext& ctx, TStringBuf message,
        const NKikimrDataEvents::TKqpLocks* kqpLocks = nullptr)
    {
        // Collect all lock IDs that belong to the current transaction.
        // In the OLTP sink model, a transaction reads with lockTxId=X and commits
        // with txId=Y. The commit write may break its own read lock (lockId=X),
        // but X != Y, so checking selfTxId alone is insufficient. The kqpLocks
        // contain the transaction's own lock descriptions with their LockIds.
        THashSet<ui64> selfLockIds;
        selfLockIds.insert(selfTxId);
        if (kqpLocks) {
            for (const auto& lock : kqpLocks->GetLocks()) {
                selfLockIds.insert(lock.GetLockId());
            }
        }

        TVector<ui64> otherLocksBroken;
        otherLocksBroken.reserve(locksBrokenByTx.size());
        for (const auto& lockId : locksBrokenByTx) {
            if (!selfLockIds.contains(lockId)) {
                otherLocksBroken.push_back(lockId);
            }
        }
        record.MutableTxStats()->SetLocksBrokenAsBreaker(otherLocksBroken.size());
        if (!otherLocksBroken.empty()) {
            auto victimQuerySpanIds = sysLocks.ExtractVictimQuerySpanIds(otherLocksBroken);

            // Resolve BreakerQuerySpanId: prefer the conflict-derived ID (set during CommitLock
            // from the stored conflict data — the actual query that wrote to the conflicting key).
            // Fall back to EvWrite's QuerySpanId when unavailable.
            ui64 primaryBreakerSpanId = 0;
            if (auto breakerQuerySpanId = sysLocks.GetCurrentBreakerQuerySpanId()) {
                primaryBreakerSpanId = *breakerQuerySpanId;
            }

            if (primaryBreakerSpanId != 0) {
                // Conflict-derived: report only the actual breaker query, not all shard writers.
                record.MutableTxStats()->AddBreakerQuerySpanIds(primaryBreakerSpanId);
            } else {
                // Direct write (no commit locks): use EvWrite's QuerySpanId.
                if (querySpanId != 0) {
                    record.MutableTxStats()->AddBreakerQuerySpanIds(querySpanId);
                    primaryBreakerSpanId = querySpanId;
                }
            }

            NDataIntegrity::LogLocksBroken(ctx, DataShard.TabletID(), message,
                otherLocksBroken, primaryBreakerSpanId, victimQuerySpanIds);

            for (ui64 lockId : otherLocksBroken) {
                auto lockInfo = DataShard.SysLocksTable().GetRawLock(lockId);
                if (lockInfo) {
                    lockInfo->ConsumeBreakerInfo();
                }
            }
        }
        return otherLocksBroken.size();
    }

    // Status, Issues, TxStats — stored with the seq num state of the delivery.
    static TString SerializeWriteSeqNumResult(const NKikimrDataEvents::TEvWriteResult& record) {
        NKikimrDataEvents::TEvWriteResult stored;
        stored.SetStatus(record.GetStatus());
        *stored.MutableIssues() = record.GetIssues();
        if (record.HasTxStats()) {
            *stored.MutableTxStats() = record.GetTxStats();
        }
        TString serialized;
        Y_ENSURE(stored.SerializeToString(&serialized));
        return serialized;
    }

    // The stored result describes the last delivery of the chain position. Fatal
    // errors mean the transaction is already dead: a redelivery must replay the
    // same error instead of re-executing. Retryable failures and non-errors are
    // not replayed: the writer retries them with a fresh execution.
    static bool IsFatalWriteResultStatus(NKikimrDataEvents::TEvWriteResult::EStatus status) {
        switch (status) {
            case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE:
                return false;
            default:
                return true;
        }
    }

    // The stored result of a duplicate delivery carries the reply the writer never
    // saw. A missing result degrades to "nothing known"; a corrupt one is a bug
    // (corrupted state or a buggy migration peer) and is reported to the writer
    // as an internal error instead of failing silently.
    enum class EStoredWriteResultParse {
        Missing,
        Parsed,
        Corrupt,
    };

    static EStoredWriteResultParse ParseStoredWriteSeqNumResult(
            const TWriteSeqNumState& state,
            NKikimrDataEvents::TEvWriteResult& parsed) {
        if (state.SerializedResult.empty()) {
            return EStoredWriteResultParse::Missing;
        }
        if (!parsed.ParseFromString(state.SerializedResult)) {
            return EStoredWriteResultParse::Corrupt;
        }
        return EStoredWriteResultParse::Parsed;
    }

    // Validates this write's position in its writer's chain; on success records the new
    // position for ApplyLocks to persist. Returns a status when the operation must stop here.
    std::optional<EExecutionStatus> CheckWriteSeqNum(
            TWriteOperation* writeOp, TSetupSysLocks& guardLocks) {
        // The op may re-execute after a restart; clear stats collected by a previous
        // attempt so they aren't appended twice and don't inflate the merged stats.
        writeOp->DuplicateTxStats.clear();

        const auto& operations = writeOp->GetWriteTx()->GetOperations();

        // Determine the writer index from the first operation that has a WriteSeqNum.
        ui64 writerIndex = 0;
        bool hasWriteSeqNum = false;
        for (const auto& op : operations) {
            if (op.GetWriteSeqNum().WriteSeqNum) {
                writerIndex = op.GetWriteSeqNum().WriterIndex;
                hasWriteSeqNum = true;
                break;
            }
        }
        if (!hasWriteSeqNum) {
            return std::nullopt;
        }
        auto& seqNumUpdate = guardLocks.WriteSeqNumUpdate.Emplace(writerIndex);

        const ui64 tabletId = DataShard.TabletID();
        auto lock = guardLocks.Lock;
        Y_ENSURE(lock);

        if (auto other = lock->FindOtherWriter(writerIndex)) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder()
                << "Multiple writers per lock are not supported: lock already has writer "
                << *other << ", got " << writerIndex);
            return EExecutionStatus::Executed;
        }

        // Group operations by DataShard: per-group min/max requested seqnum.
        // A DataShard of 0 in the operation means the current shard; treat non-zero as ancestor.
        // We normalize: treat 0 and tabletId as the same (current shard).
        struct TShardSeqNums {
            ui64 MinRequested = 0;
            ui64 MaxRequested = 0;
            ui64 Current = 0;
            bool isDuplicate = false;
            bool SawFreshOp = false;
        };
        THashMap<ui64, TShardSeqNums> shard2seqnums;
        for (const auto& op : operations) {
            const ui64 requested = op.GetWriteSeqNum().WriteSeqNum;
            if (!requested) {
                continue;
            }
            const ui64 shardId = op.GetOriginalShard();
            const ui64 shardKey = (shardId == 0 || shardId == tabletId) ? 0 : shardId;

            auto& seqnums = shard2seqnums[shardKey];
            if (!seqnums.MinRequested) {
                seqnums.MinRequested = requested;
            }
            if (seqnums.MaxRequested && requested != seqnums.MaxRequested + 1) {
                writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder()
                    << "Uncommitted write seq nums must be ascending and contiguous within one request, got "
                    << writerIndex << ":" << seqnums.MaxRequested << " followed by "
                    << writerIndex << ":" << requested
                    << " for DataShard " << shardKey);
                return EExecutionStatus::Executed;
            }
            seqnums.MaxRequested = requested;
        }

        // For each shard, determine current seqnum and classify as duplicate/stale/continuation.
        // Groups with already applied positions remember the stored result of the lost
        // delivery: its statistics are added to the reply, and a fatal error is replayed
        // verbatim (the transaction is already dead and must not be re-executed).
        // Returns true when a fatal stored result was replayed.
        auto collectStoredResult = [&](ui64 shardKey) -> bool {
            const TWriteSeqNumState* stored = shardKey == 0
                ? lock->FindWriteSeqNumState(writerIndex)
                : lock->FindAncestorWriteSeqNumState(shardKey, writerIndex);
            Y_ENSURE(stored);
            NKikimrDataEvents::TEvWriteResult parsed;
            const auto parse = ParseStoredWriteSeqNumResult(*stored, parsed);
            if (parse == EStoredWriteResultParse::Corrupt) {
                // Crashing on the corrupt state would turn every redelivery into
                // another tablet crash: report a deterministic error to the
                // writer instead, so the request fails but the tablet keeps
                // serving. The state is left as is: further deliveries of these
                // positions see the same error.
                writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder()
                    << "Failed to parse stored write result for writer " << writerIndex
                    << " seq num " << stored->WriteSeqNum << " for DataShard " << shardKey);
                return true;
            }
            if (parse == EStoredWriteResultParse::Missing) {
                return false;
            }
            if (IsFatalWriteResultStatus(parsed.GetStatus())) {
                // The previous delivery of this position failed fatally and the
                // writer never saw the reply: replay the stored error instead of
                // executing.
                writeOp->SetError(parsed.GetStatus(), "Replaying the stored result of the previous delivery");
                auto& record = writeOp->GetWriteResult()->Record;
                *record.MutableIssues() = parsed.GetIssues();
                if (parsed.HasTxStats()) {
                    *record.MutableTxStats() = parsed.GetTxStats();
                }
                return true;
            }
            if (parsed.HasTxStats()) {
                writeOp->DuplicateTxStats.push_back(parsed.GetTxStats());
            }
            return false;
        };

        for (auto& [shardKey, seqnums] : shard2seqnums) {
            ui64 current = 0;
            if (shardKey == 0) {
                // Current shard
                current = lock->GetWriteSeqNum(writerIndex);
            } else {
                // Ancestor shard
                if (lock->GetWriteSeqNum(writerIndex) != 0) {
                    writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST,
                        TStringBuilder() << "Ancestor shard " << shardKey
                        << " write " << writerIndex << ":" << seqnums.MaxRequested
                        << " not allowed after current shard writes");
                    return EExecutionStatus::Executed;
                }
                auto ancestorIt = lock->GetAncestorLocks().find(shardKey);
                if (ancestorIt == lock->GetAncestorLocks().end()) {
                    writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder()
                        << "Ancestor shard " << shardKey << " not found in lock " << guardLocks.LockTxId);
                    return EExecutionStatus::Executed;
                }
                for (const auto& [idx, state] : ancestorIt->second.WriteSeqNumStates) {
                    if (idx == writerIndex) {
                        current = state.WriteSeqNum;
                    } else {
                        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST,
                            TStringBuilder() << "Ancestor shard " << shardKey
                            << " has another writer: " << idx
                            << " for lock id: " << guardLocks.LockTxId
                            << " (requested write: " << writerIndex << ":" << seqnums.MaxRequested << ")");
                        return EExecutionStatus::Executed;
                    }
                }
            }

            if (seqnums.MaxRequested == current) {
                // Duplicate
                seqnums.isDuplicate = true;
                if (collectStoredResult(shardKey)) {
                    return EExecutionStatus::Executed;
                }
            } else if (seqnums.MaxRequested < current) {
                // Stale
                writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST,
                    TStringBuilder() << "Uncommitted write " << writerIndex << ":" << seqnums.MaxRequested
                    << " for DataShard " << shardKey << " is already applied, writer is at " << current);
                return EExecutionStatus::Executed;
            } else {
                // Continuation: positions up to current were already applied (e.g. a
                // re-routed batch after a split may mix already applied positions of
                // the chain with new ones). Such positions are duplicates: their ops
                // are skipped. The remaining tail must continue without a gap.
                if (seqnums.MinRequested > current + 1) {
                    writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST,
                        TStringBuilder() << "Uncommitted write " << writerIndex << ":" << seqnums.MinRequested
                        << " for DataShard " << shardKey
                        << " must continue the writer chain at " << (current + 1)
                        << ", writer is at " << current);
                    return EExecutionStatus::Executed;
                }
                if (seqnums.MinRequested <= current) {
                    // Mixed group: leading positions are duplicates of a lost delivery;
                    // remember its stored statistics for the reply. A fatal stored
                    // result is replayed before any fresh tail operation is applied.
                    if (collectStoredResult(shardKey)) {
                        return EExecutionStatus::Executed;
                    }
                }
            }
            seqnums.Current = current;
        }

        // Mark already applied operations as duplicates so their rows are not
        // written again: all operations of a fully duplicate group (Max == current)
        // and leading positions of a mixed group (positions <= current).
        // The writer acks batches of each shard in order and resends only
        // unacknowledged operations, so already applied operations always form a
        // prefix of their shard's operations in the request: a duplicate after a
        // fresh operation of the same shard is a writer bug. Shards are tracked
        // independently: their operations may interleave in any order (e.g. a
        // merge re-route mixes the chains of several removed shards in one
        // request, and the order depends on the re-route's map iteration).
        // Operations of the current shard must come last: the writer flushes the
        // re-routed batches of removed shards (ancestor chains) before its fresh
        // current-shard writes, so an ancestor operation positioned after a
        // current-shard operation is a writer bug.
        bool sawCurrentShardOp = false;
        for (size_t iOp = 0; iOp < operations.size(); ++iOp) {
            const auto& op = operations[iOp];
            if (!op.GetWriteSeqNum().WriteSeqNum) {
                continue;
            }
            const ui64 shardId = op.GetOriginalShard();
            const ui64 shardKey = (shardId == 0 || shardId == tabletId) ? 0 : shardId;
            auto& seqnums = shard2seqnums.at(shardKey);
            if (shardKey != 0 && sawCurrentShardOp) {
                writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder()
                    << "Uncommitted write " << writerIndex << ":" << op.GetWriteSeqNum().WriteSeqNum
                    << " for DataShard " << shardKey
                    << " is positioned after current shard writes in the request");
                return EExecutionStatus::Executed;
            }
            if (op.GetWriteSeqNum().WriteSeqNum <= seqnums.Current) {
                if (seqnums.SawFreshOp) {
                    writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder()
                        << "Uncommitted write " << writerIndex << ":" << op.GetWriteSeqNum().WriteSeqNum
                        << " for DataShard " << shardKey
                        << " duplicates a fresh operation: duplicates must form a prefix"
                        << " of the shard's operations in the request");
                    return EExecutionStatus::Executed;
                }
                writeOp->DuplicateWriteOpIdxs.insert(iOp);
            } else {
                seqnums.SawFreshOp = true;
                if (shardKey == 0) {
                    sawCurrentShardOp = true;
                }
            }
        }

        // All continuations: track max per group for ApplyLocks to persist.
        for (const auto& [shardId, seqnums] : shard2seqnums) {
            if (!seqnums.isDuplicate) {
                if (shardId == 0) {
                    seqNumUpdate.SetWriteSeqNum = seqnums.MaxRequested;
                } else {
                    seqNumUpdate.SetAncestorWriteSeqNums.emplace(shardId, seqnums.MaxRequested);
                }
            } else {
                if (shardId == 0) {
                    seqNumUpdate.ThisShardDuplicateWrite = true;
                } else {
                    seqNumUpdate.AncestorShardsWithDuplicateWrites.insert(shardId);
                }
            }
        }
        return std::nullopt;
    }

    void AddLocksToResult(TWriteOperation* writeOp, ui64 querySpanId, const TActorContext& ctx,
        const NKikimrDataEvents::TKqpLocks* kqpLocks = nullptr)
    {
        NEvents::TDataEvents::TEvWriteResult& writeResult = *writeOp->GetWriteResult();

        auto [locks, locksBrokenByTx] = DataShard.SysLocksTable().ApplyLocks();
        HandleBreakerLocks(locksBrokenByTx, writeOp->GetTxId(), querySpanId,
            DataShard.SysLocksTable(), writeResult.Record, ctx,
            "Write transaction broke other locks", kqpLocks);
        YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "Add locks",
            {"result", locks.size()});
        for (const auto& lock : locks) {
            if (lock.IsError()) {
                YDB_LOG_NOTICE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::AddLocksToResult: lock is not set",
                    {"operation", *writeOp},
                    {"tabletId", DataShard.TabletID()},
                    {"lock", lock});
            }

            writeResult.AddTxLock(lock.LockId, lock.DataShard, lock.Generation, lock.Counter, lock.SchemeShard, lock.PathId, lock.HasWrites,
                lock.WriterIndex, lock.WriteSeqNum);

            YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "Add lock",
                {"result", writeResult.Record.GetTxLocks().rbegin()->ShortDebugString()});
        }
        DataShard.SubscribeNewLocks(ctx);

        // If all seqnums target the current shard (DataShard == 0), validate the lock's seqnum.
        // For ancestor-shard-only batches, the current-shard seqnum on the lock is 0 (not set).
        ui64 maxCurrentShardRequested = 0;
        ui64 writerIndex = 0;
        bool hasCurrentShardSeqNum = false;
        bool hasAncestorShardSeqNum = false;
        const ui64 tabletId = DataShard.TabletID();
        for (const auto& op : writeOp->GetWriteTx()->GetOperations()) {
            const ui64 requested = op.GetWriteSeqNum().WriteSeqNum;
            if (requested) {
                writerIndex = op.GetWriteSeqNum().WriterIndex;
                const ui64 ds = op.GetOriginalShard();
                if (ds == 0 || ds == tabletId) {
                    hasCurrentShardSeqNum = true;
                    if (requested > maxCurrentShardRequested) {
                        maxCurrentShardRequested = requested;
                    }
                } else {
                    hasAncestorShardSeqNum = true;
                }
            }
        }
        if (hasCurrentShardSeqNum && !hasAncestorShardSeqNum) {
            for (const auto& lock : locks) {
                Y_ENSURE(lock.IsError()
                             || (lock.WriterIndex == writerIndex && lock.WriteSeqNum == maxCurrentShardRequested),
                         "Uncommitted write " << writerIndex << ":" << maxCurrentShardRequested
                         << " reported lock with " << lock.WriterIndex << ":" << lock.WriteSeqNum);
            }
        }
    }

    void StoreWriteSeqNumResult(TWriteOperation* writeOp, TSetupSysLocks& guardLocks, ILocksDb* db) {
        if (!guardLocks.WriteSeqNumUpdate) {
            return;
        }
        auto& update = *guardLocks.WriteSeqNumUpdate;

        auto lock = guardLocks.Lock;
        if (!lock) {
            return;
        }

        auto serialized = SerializeWriteSeqNumResult(writeOp->GetWriteResult()->Record);
        if (update.SetWriteSeqNum) {
            const auto& seqNum = *update.SetWriteSeqNum;
            if (lock->GetWriteSeqNum(update.WriterIndex) == seqNum) {
                lock->SetWriteSeqNumResult(update.WriterIndex, serialized, db);
            }
        }
        for (const auto& [shardId, seqNum] : update.SetAncestorWriteSeqNums) {
            auto* state = lock->FindAncestorWriteSeqNumState(shardId, update.WriterIndex);
            if (state && state->WriteSeqNum == seqNum) {
                lock->SetAncestorWriteSeqNumResult(shardId, update.WriterIndex, serialized, db);
            }
        }
    }

    void ResetChanges(TDataShardUserDb& userDb, TTransactionContext& txc) {
        userDb.ResetCollectedChanges();

        if (txc.DB.HasChanges()) {
            txc.DB.RollbackChanges();
        }
    }

    bool CheckForVolatileReadDependencies(TDataShardUserDb& userDb, TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        if (!userDb.GetVolatileReadDependencies().empty()) {
            YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::CheckForVolatileReadDependencies: aborting because volatile read dependencies",
                {"writeOp", writeOp},
                {"tabletId", DataShard.TabletID()});

            for (ui64 txId : userDb.GetVolatileReadDependencies()) {
                writeOp.AddVolatileDependency(txId);
                bool ok = DataShard.GetVolatileTxManager().AttachBlockedOperation(txId, writeOp.GetTxId());
                Y_ENSURE(ok, "Unexpected failure to attach TxId# " << writeOp.GetTxId() << " to volatile tx " << txId);
            }

            ResetChanges(userDb, txc);
            writeOp.ReleaseTxData(txc);

            return true;
        }

        return false;
    }

    void FillOps(const NTable::TScheme& scheme, const TUserTable& userTable, const NTable::TScheme::TTableInfo& tableInfo, const TValidatedWriteTxOperation& validatedOperation, ui32 rowIdx, TSmallVec<NTable::TUpdateOp>& ops) {
        const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();
        const auto& columnIds = validatedOperation.GetColumnIds();

        ops.clear();
        Y_ENSURE(matrix.GetColCount() >= userTable.KeyColumnIds.size());
        ops.reserve(matrix.GetColCount() - userTable.KeyColumnIds.size());

        for (ui16 valueColIdx = userTable.KeyColumnIds.size(); valueColIdx < matrix.GetColCount(); ++valueColIdx) {
            ui32 columnTag = columnIds[valueColIdx];
            const TCell& cell = matrix.GetCell(rowIdx, valueColIdx);

            const NScheme::TTypeId vtypeId = scheme.GetColumnInfo(&tableInfo, columnTag)->PType.GetTypeId();
            ops.emplace_back(columnTag, NTable::ECellOp::Set, cell.IsNull() ? TRawTypeValue() : TRawTypeValue(cell.Data(), cell.Size(), vtypeId));
        }
    };

    void FillKey(const NTable::TScheme& scheme, const TUserTable& userTable, const NTable::TScheme::TTableInfo& tableInfo, const TValidatedWriteTxOperation& validatedOperation, ui32 rowIdx, TSmallVec<TRawTypeValue>& key) {
        const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();

        key.clear();
        key.reserve(userTable.KeyColumnIds.size());
        for (ui16 keyColIdx = 0; keyColIdx < userTable.KeyColumnIds.size(); ++keyColIdx) {
            const TCell& cell = matrix.GetCell(rowIdx, keyColIdx);
            ui32 keyCol = tableInfo.KeyColumns[keyColIdx];
            if (cell.IsNull()) {
                key.emplace_back();
            } else {
                NScheme::TTypeId vtypeId = scheme.GetColumnInfo(&tableInfo, keyCol)->PType.GetTypeId();
                key.emplace_back(cell.Data(), cell.Size(), vtypeId);
            }
        }
    };

    EExecutionStatus OnTabletNotReadyException(TDataShardUserDb& userDb, TWriteOperation& writeOp, size_t operationIndexToPrecharge, TTransactionContext& txc, const TActorContext& ctx) {
        YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::OnTabletNotReadyException: tablet is not ready for execution",
            {"tabletId", DataShard.TabletID()},
            {"writeOp", writeOp});

        // Precharge
        if (operationIndexToPrecharge != SIZE_MAX) {
            const TValidatedWriteTx::TPtr& writeTx = writeOp.GetWriteTx();
            for (size_t operationIndex = operationIndexToPrecharge; operationIndex < writeTx->GetOperations().size(); ++operationIndex) {
                const TValidatedWriteTxOperation& validatedOperation = writeTx->GetOperations()[operationIndex];
                const ui64 tableId = validatedOperation.GetTableId().PathId.LocalPathId;
                const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);
                const TUserTable& userTable = *DataShard.GetUserTables().at(tableId);

                const NTable::TScheme& scheme = txc.DB.GetScheme();
                const NTable::TScheme::TTableInfo& tableInfo = *scheme.GetTableInfo(userTable.LocalTid);

                const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();
                const auto operationType = validatedOperation.GetOperationType();

                TSmallVec<TRawTypeValue> key;

                const bool operationNeedsRead =
                    operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT ||
                    operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE ||
                    operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT ||
                    operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT ||
                    (operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT
                        && validatedOperation.GetDefaultFilledColumnCount() != 0) ||
                    (operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE
                        && userDb.GetCollectAffectedRows());

                if (operationNeedsRead || userDb.NeedToReadBeforeWrite(fullTableId)) {
                    const auto* collector = userDb.GetChangeCollector(fullTableId);
                    for (ui32 rowIdx = 0; rowIdx < matrix.GetRowCount(); ++rowIdx) {
                        FillKey(scheme, userTable, tableInfo, validatedOperation, rowIdx, key);
                        const bool needsMvccRead = collector && collector->NeedHistoricalPrecharge(fullTableId, key);
                        userDb.PrechargeRow(fullTableId, key, needsMvccRead);
                    }
                }
            }
        }

        DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

        ResetChanges(userDb, txc);
        writeOp.ReleaseTxData(txc);
        return EExecutionStatus::Restart;
    }

    bool OnUniqueConstrainException(TDataShardUserDb& userDb, TWriteOperation& writeOp, ui64 lockTxId, TTransactionContext& txc, const TActorContext& ctx) {
        if (CheckForVolatileReadDependencies(userDb, writeOp, txc, ctx)) {
            return false;
        }

        if (userDb.GetSnapshotReadConflict()) {
            YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::OnUniqueConstrainException: aborting, conflict with another transaction",
                {"writeOp", writeOp},
                {"tabletId", DataShard.TabletID()});
            writeOp.SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Read conflict with concurrent transaction.");
            writeOp.GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(1);
            NDataIntegrity::LogVictimDetected(ctx, DataShard.TabletID(), "Write transaction was a victim of broken locks",
                                              lockTxId ? DataShard.SysLocksTable().GetVictimQuerySpanIdForLock(lockTxId) : Nothing());
            if (lockTxId) {
                FillDeferredBreakerInfo(lockTxId, writeOp.GetWriteResult()->Record.MutableTxStats());
            }
        } else {
            YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::OnUniqueConstrainException: aborting, conflict with existing key",
                {"writeOp", writeOp},
                {"tabletId", DataShard.TabletID()});
            writeOp.SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, "Conflict with existing key.");
        }

        ResetChanges(userDb, txc);
        return true;
    }

    void DoUnsafeTruncateToUserDb(const TValidatedWriteTxOperation& validatedOperation, TConstArrayRef<ui64> preserveLockTxIds,
        ui64 txId, const TRowVersion& mvccVersion, TTransactionContext& txc, const TActorContext& ctx)
    {
        const ui64 tableId = validatedOperation.GetTableId().PathId.LocalPathId;
        const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);
        const TUserTable& userTable = *DataShard.GetUserTables().at(tableId);
        const ui32 localTid = userTable.LocalTid;

        // Every lock of this table is broken, except those of the user transaction that issued
        // the unsafe truncate - it must survive its own statement. Unlike the schema TRUNCATE
        // this does not go through SysLocks.RemoveSchema, which cannot spare anything.
        const auto lockStats = DataShard.SysLocksTable().BreakAllLocksExcept(fullTableId, preserveLockTxIds);

        DataShard.GetConflictsCache().GetTableCache(localTid).RemoveAllUncommittedWrites(txc.DB);

        txc.DB.Truncate(localTid);

        // Unlike the schema TRUNCATE (see truncate_unit.cpp), the snapshot low watermark is
        // deliberately left alone. Reads of the issuing transaction carry the MVCC version pinned
        // when that transaction started, so advancing the watermark would break the transaction
        // that asked for the unsafe truncate. The cost is that a concurrent reader holding an older
        // snapshot silently observes an empty table - that is what "unsafe" stands for.

        DataShard.IncCounter(COUNTER_UNSAFE_TRUNCATE);

        NDataIntegrity::LogIntegrityTrailsUnsafeTruncate(ctx, DataShard.TabletID(), txId, tableId,
            TStringBuilder() << mvccVersion.Step << ":" << mvccVersion.TxId,
            lockStats.Broken, lockStats.Preserved);
    }

    void DoUpdateToUserDb(TDataShardUserDb& userDb, const TValidatedWriteTxOperation& validatedOperation, TTransactionContext& txc) {
        const ui64 tableId = validatedOperation.GetTableId().PathId.LocalPathId;
        const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);
        const TUserTable& userTable = *DataShard.GetUserTables().at(tableId);

        const NTable::TScheme& scheme = txc.DB.GetScheme();
        const NTable::TScheme::TTableInfo& tableInfo = *scheme.GetTableInfo(userTable.LocalTid);

        const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();
        const auto operationType = validatedOperation.GetOperationType();
        auto userCtx = validatedOperation.GetUserCtx();
        if (userCtx == nullptr) {
            userCtx = NACLib::TUserContextBuilder().Build();
        }

        TSmallVec<TRawTypeValue> key;
        TSmallVec<NTable::TUpdateOp> ops;
        const ui32 defaultFilledColumnCount = validatedOperation.GetDefaultFilledColumnCount();

        // Main update cycle

        bool notReady = false;
        for (ui32 rowIdx = 0; rowIdx < matrix.GetRowCount(); ++rowIdx)
        {
            FillKey(scheme, userTable, tableInfo, validatedOperation, rowIdx, key);

            switch (operationType) {
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.UpsertRow(fullTableId, key, ops, defaultFilledColumnCount, userCtx);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.ReplaceRow(fullTableId, key, ops, userCtx);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE: {
                    userDb.EraseRow(fullTableId, key, userCtx);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.InsertRow(fullTableId, key, ops, userCtx);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.UpdateRow(fullTableId, key, ops, userCtx);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.IncrementRow(fullTableId, key, ops, false /*insertMissing*/, userCtx);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.IncrementRow(fullTableId, key, ops, true /*insertMissing*/, userCtx);
                    break;
                }
                default:
                    // Checked before in TWriteOperation
                    Y_ENSURE(false, operationType << " operation is not supported now");
            }
        }
        if (notReady) {
            throw TNotReadyTabletException();
        }

        // Counters

        switch (operationType) {
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT: {
                DataShard.IncCounter(COUNTER_WRITE_ROWS, matrix.GetRowCount());
                DataShard.IncCounter(COUNTER_WRITE_BYTES, matrix.GetBuffer().size());
                break;
            }
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE: {
                DataShard.IncCounter(COUNTER_ERASE_ROWS, matrix.GetRowCount());
                break;
            }
            default:
                // Checked before in TWriteOperation
                Y_ENSURE(false, operationType << " operation is not supported now");
        }
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);
        const ui64 tabletId = DataShard.TabletID();

        const bool allowAncestorLocks =
            AppData()->FeatureFlags.GetEnableDataShardLocksTransferOnSplit();

        YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::Execute: executing write operation",
            {"operation", *op},
            {"tabletId", tabletId});

        if (writeOp->GetWriteResult() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
            return EExecutionStatus::Executed;
        }

        if (op->HasWaitingForGlobalTxIdFlag()) {
            return EExecutionStatus::Continue;
        }

        if (op->IsImmediate()) {
            // Every time we execute immediate transaction we may choose a new mvcc version
            op->CachedMvccVersion.reset();
        }

        const TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();
        size_t validatedOperationIndex = SIZE_MAX;

        DataShard.ReleaseCache(*writeOp);

        if (writeOp->IsTxDataReleased()) {
            switch (Pipeline.RestoreWriteTx(writeOp, txc)) {
                case ERestoreDataStatus::Ok:
                    break;

                case ERestoreDataStatus::Restart:
                    return EExecutionStatus::Restart;

                case ERestoreDataStatus::Error:
                    // For immediate transactions we want to translate this into a propose failure
                    if (op->IsImmediate()) {
                        Y_ENSURE(!writeTx->Ready());
                        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, writeTx->GetErrStr());
                        return EExecutionStatus::Executed;
                    }

                    // For planned transactions errors are not expected
                    Y_ENSURE(false, "Failed to restore tx data: " << writeTx->GetErrStr());
            }
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        if (op->IsImmediate() && !writeOp->ReValidateKeys(txc.DB.GetScheme())) {
            // Immediate transactions may be reordered with schema changes and become invalid
            Y_ENSURE(!writeTx->Ready());
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, writeTx->GetErrStr());
            return EExecutionStatus::Executed;
        }

        if (writeTx->CheckCancelled()) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Tx was cancelled");
            DataShard.IncCounter(COUNTER_WRITE_CANCELLED);
            writeOp->ReleaseTxData(txc);
            return EExecutionStatus::Executed;
        }

        NMiniKQL::TEngineHostCounters engineHostCounters;
        const ui64 txId = op->GetTxId();
        const auto mvccVersion = DataShard.GetMvccVersion(writeOp);

        TDataShardUserDb userDb(DataShard, txc.DB, op->GetGlobalTxId(), mvccVersion, engineHostCounters, TAppData::TimeProvider->Now());
        userDb.SetIsWriteTx(true);
        userDb.SetIsImmediateTx(op->IsImmediate());
        userDb.SetLockTxId(writeTx->GetLockTxId());
        userDb.SetLockNodeId(writeTx->GetLockNodeId());
        userDb.SetLockMode(writeTx->GetLockMode());
        userDb.SetCollectAffectedRows(writeTx->GetCollectAffectedRows());

        if (op->HasVolatilePrepareFlag() || op->GetRemainReadSets()) {
            userDb.SetVolatileTxId(txId);
        }

        if (auto mvccSnapshot = writeTx->GetMvccSnapshot()) {
            userDb.SetSnapshotVersion(*mvccSnapshot);
        }

        bool preparedOutReadSets = false;
        bool keepOutReadSets = writeOp->IsOutRSStored();

        const auto* kqpLocks = writeTx->GetKqpLocks() ? &writeTx->GetKqpLocks().value() : nullptr;

        auto ensureAbortOutReadSets = [&]() -> std::optional<EExecutionStatus> {
            if (!op->IsImmediate() && !keepOutReadSets) {
                op->OutReadSets().clear();
                op->AwaitingDecisions().clear();
                KqpFillOutReadSets(op->OutReadSets(), kqpLocks, NKikimrTx::TReadSetData::DECISION_ABORT, tabletId);
                if (!op->OutReadSets().empty()) {
                    DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(), op->PreparedOutReadSets(), txc, ctx);
                    preparedOutReadSets = true;
                }
                keepOutReadSets = true;

                if (preparedOutReadSets && !op->PreparedOutReadSets().empty()) {
                    op->SetWaitCompletionFlag(true);
                    return EExecutionStatus::DelayCompleteNoMoreRestarts;
                }
            }

            return std::nullopt;
        };

        try {
            const auto& inReadSets = op->InReadSets();
            auto& awaitingDecisions = op->AwaitingDecisions();
            auto& outReadSets = op->OutReadSets();
            bool useGenericReadSets = true;
            TSysLocks& sysLocks = DataShard.SysLocksTable();

            ui64 consumedMemory = writeTx->GetTxSize();
            if (MaybeRequestMoreTxMemory(consumedMemory, txc)) {
                YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::Execute: requested more memory",
                    {"operation", *op},
                    {"tabletId", tabletId},
                    {"requestedMemory", txc.GetRequestedMemory()});

                DataShard.IncCounter(COUNTER_TX_WAIT_RESOURCE);
                return EExecutionStatus::Restart;
            }

            if (guardLocks.LockTxId) {
                auto abortLock = [&]() {
                    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::Execute: aborting because it cannot acquire locks",
                        {"operation", *op},
                        {"tabletId", tabletId});
                    writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Operation is aborting because it cannot acquire locks");
                    writeOp->GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(1);
                    NDataIntegrity::LogVictimDetected(ctx, tabletId, "Write transaction was a victim of broken locks",
                                                      DataShard.SysLocksTable().GetVictimQuerySpanIdForLock(guardLocks.LockTxId));
                    FillDeferredBreakerInfo(guardLocks.LockTxId, writeOp->GetWriteResult()->Record.MutableTxStats());
                    return EExecutionStatus::Executed;
                };

                switch (DataShard.SysLocksTable().EnsureCurrentLock()) {
                    case EEnsureCurrentLock::Success:
                        // Lock is valid, we may continue with reads and side-effects
                        break;

                    case EEnsureCurrentLock::Broken:
                        // Lock is valid, but broken, we could abort early in some
                        // cases, but it doesn't affect correctness.
                        if (!op->IsReadOnly()) {
                            return abortLock();
                        }
                        break;

                    case EEnsureCurrentLock::TooMany:
                        // Lock cannot be created, it's not necessarily a problem
                        // for read-only transactions, for non-readonly we need to
                        // abort;
                        if (!op->IsReadOnly()) {
                            return abortLock();
                        }
                        break;

                    case EEnsureCurrentLock::Abort:
                        // Lock cannot be created and we must abort
                        return abortLock();

                    case EEnsureCurrentLock::Missing:
                        Y_ENSURE(false, "unreachable");
                }

                if (auto status = CheckWriteSeqNum(writeOp, guardLocks)) {
                    return *status;
                }
            }

            Y_DEFER {
                // We need to clear OutReadSets and AwaitingDecisions for
                // volatile transactions, except when we commit them.
                if (!keepOutReadSets) {
                    outReadSets.clear();
                    awaitingDecisions.clear();
                }
            };

            auto [validated, brokenLocks] = op->HasVolatilePrepareFlag()
                                                ? KqpValidateVolatileTx(sysLocks, kqpLocks, useGenericReadSets, txId, op->DelayedInReadSets(), awaitingDecisions, outReadSets, allowAncestorLocks)
                                                : KqpValidateLocks(sysLocks, kqpLocks, useGenericReadSets, inReadSets, allowAncestorLocks);

            if (!validated) {
                YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::Execute: aborting because locks are not valid",
                    {"operation", *op},
                    {"tabletId", tabletId});
                writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Operation is aborting because locks are not valid");
                writeOp->GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(brokenLocks.size());

                // Get victim QuerySpanId from the first broken lock
                TMaybe<ui64> victimQuerySpanId = brokenLocks.empty() ? Nothing()
                    : sysLocks.GetVictimQuerySpanIdForLock(brokenLocks[0].GetLockId());
                NDataIntegrity::LogVictimDetected(ctx, tabletId, "Write transaction was a victim of broken locks",
                                                  victimQuerySpanId);

                {
                    auto* txStats = writeOp->GetWriteResult()->Record.MutableTxStats();
                    for (const auto& brokenLock : brokenLocks) {
                        FillDeferredBreakerInfo(brokenLock.GetLockId(), txStats);
                    }
                }

                for (auto& brokenLock : brokenLocks) {
                    writeOp->GetWriteResult()->Record.MutableTxLocks()->Add()->Swap(&brokenLock);
                }

                KqpEraseLocks(kqpLocks, sysLocks, allowAncestorLocks);
                auto [_, locksBrokenByTxCleanup] = sysLocks.ApplyLocks();
                // The chain position has been recorded, but the delivery failed
                // fatally: store the error result so a redelivery of the same
                // positions replays it instead of answering with a fresh success.
                // Unreachable today: seq num writes carry no TKqpLocks, so lock
                // validation never fails for them, but this must hold if that
                // ever changes.
                StoreWriteSeqNumResult(writeOp, guardLocks, &locksDb);
                HandleBreakerLocks(locksBrokenByTxCleanup, writeOp->GetTxId(), guardLocks.QuerySpanId,
                    sysLocks, writeOp->GetWriteResult()->Record, ctx,
                    "Write transaction aborted, broke other transaction locks during cleanup");
                DataShard.SubscribeNewLocks(ctx);

                if (auto status = ensureAbortOutReadSets()) {
                    return *status;
                }

                if (locksDb.HasChanges()) {
                    op->SetWaitCompletionFlag(true);
                    return EExecutionStatus::ExecutedNoMoreRestarts;
                }
                return EExecutionStatus::Executed;
            }

            const bool isArbiter = op->HasVolatilePrepareFlag() && KqpLocksIsArbiter(tabletId, kqpLocks);

            KqpCommitLocks(kqpLocks, sysLocks, userDb, allowAncestorLocks);

            if (writeTx->HasOperations()) {
                for (validatedOperationIndex = 0; validatedOperationIndex < writeTx->GetOperations().size(); ++validatedOperationIndex) {
                    if (writeOp->DuplicateWriteOpIdxs.contains(validatedOperationIndex)) {
                        continue;
                    }
                    const TValidatedWriteTxOperation& validatedOperation = writeTx->GetOperations()[validatedOperationIndex];
                    if (writeOp->WriteRequest) {
                        const auto& protoOperations = writeOp->WriteRequest->Record.GetOperations();
                        if (validatedOperationIndex < static_cast<size_t>(protoOperations.size())) {
                            const auto& protoOp = protoOperations.Get(validatedOperationIndex);
                            if (protoOp.HasQuerySpanId() && protoOp.GetQuerySpanId() != 0) {
                                guardLocks.QuerySpanId = protoOp.GetQuerySpanId();
                            }
                        }
                    }
                    if (validatedOperation.GetOperationType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSAFE_TRUNCATE) {
                        DoUnsafeTruncateToUserDb(validatedOperation, writeTx->GetPreserveLockTxIds(),
                            writeOp->GetTxId(), mvccVersion, txc, ctx);
                        continue;
                    }

                    DoUpdateToUserDb(userDb, validatedOperation, txc);
                    YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::Execute: executed write operation for row",
                        {"operation", *writeOp},
                        {"tabletId", DataShard.TabletID()},
                        {"count", validatedOperation.GetMatrix().GetRowCount()});
                }
                validatedOperationIndex = SIZE_MAX;
                DataShard.AddRecentWriteForTli(mvccVersion, guardLocks.QuerySpanId, writeOp->GetTarget().NodeId());
            } else {
                YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::Execute: skip empty write operation",
                    {"operation", *writeOp},
                    {"tabletId", DataShard.TabletID()});
            }

            if (CheckForVolatileReadDependencies(userDb, *writeOp, txc, ctx))
                return EExecutionStatus::Continue;

            writeOp->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildCompleted(tabletId, writeOp->GetTxId()));

            auto& writeResult = writeOp->GetWriteResult();
            writeResult->Record.SetOrderId(op->GetTxId());
            if (!op->IsImmediate())
                writeResult->Record.SetStep(op->GetStep());

            if (Pipeline.AddLockDependencies(op, guardLocks)) {
                userDb.ResetCollectedChanges();
                if (txc.DB.HasChanges()) {
                    txc.DB.RollbackChanges();
                }
                writeOp->ReleaseTxData(txc);
                return EExecutionStatus::Continue;
            }

            // Note: any transaction (e.g. immediate or non-volatile) may decide to commit as volatile due to dependencies
            // Such transactions would have no participants and become immediately committed
            auto commitTxIds = userDb.GetVolatileCommitTxIds();
            if (commitTxIds || isArbiter) {
                if (op->GetRemainReadSets()) {
                    for (const auto& [key, received] : inReadSets) {
                        if (received.empty()) {
                            awaitingDecisions.insert(key.first);
                        }
                    }
                }
                TVector<ui64> participants(awaitingDecisions.begin(), awaitingDecisions.end());
                DataShard.GetVolatileTxManager().PersistAddVolatileTx(
                    userDb.GetVolatileTxId(),
                    mvccVersion,
                    commitTxIds,
                    userDb.GetVolatileDependencies(),
                    participants,
                    userDb.GetChangeGroup(),
                    userDb.GetVolatileCommitOrdered(),
                    isArbiter,
                    /* disable expectations */ !participants.empty() && !op->HasVolatilePrepareFlag(),
                    txc
                );
            } else {
                awaitingDecisions.clear();
            }

            if (userDb.GetPerformedUserReads()) {
                op->SetPerformedUserReads(true);
            }

            if (op->HasVolatilePrepareFlag()) {
                // Notify other shards about our expectations as soon as possible, even before we commit
                for (ui64 target : op->AwaitingDecisions()) {
                    if (DataShard.AddExpectation(target, op->GetStep(), op->GetTxId())) {
                        DataShard.SendReadSetExpectation(ctx, op->GetStep(), op->GetTxId(), tabletId, target);
                    }
                }
            }

            if (!op->IsImmediate() && !keepOutReadSets) {
                if (op->OutReadSets().empty() && !op->HasVolatilePrepareFlag()) {
                    KqpFillOutReadSets(outReadSets, kqpLocks, NKikimrTx::TReadSetData::DECISION_COMMIT, tabletId);
                }
                if (!op->OutReadSets().empty()) {
                    DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(), op->PreparedOutReadSets(), txc, ctx);
                    preparedOutReadSets = true;
                }
                keepOutReadSets = true;
            }

            // Note: may erase persistent locks, must be after we persist volatile tx
            AddLocksToResult(writeOp, guardLocks.QuerySpanId, ctx, kqpLocks);

            if (!guardLocks.LockTxId) {
                mvccVersion.ToProto(writeResult->Record.MutableCommitVersion());
            }

            if (auto changes = std::move(userDb.GetCollectedChanges())) {
                op->ChangeRecords() = std::move(changes);
            }

            const auto& counters = userDb.GetCounters();
            KqpUpdateDataShardStatCounters(DataShard, counters);
            auto* txStats = writeResult->Record.MutableTxStats();
            KqpFillTxStats(DataShard, counters, *txStats);
            for (const auto& stats : writeOp->DuplicateTxStats) {
                MergeDuplicateTxStats(*txStats, stats);
            }

            StoreWriteSeqNumResult(writeOp, guardLocks, &locksDb);

        } catch (const TNeedGlobalTxId&) {
            Y_ENSURE(op->GetGlobalTxId() == 0,
                "Unexpected TNeedGlobalTxId exception for write operation with TxId# " << op->GetGlobalTxId());
            Y_ENSURE(op->IsImmediate(),
                "Unexpected TNeedGlobalTxId exception for a non-immediate write operation with TxId# " << op->GetTxId());

            ctx.Send(MakeTxProxyID(),
                new TEvTxUserProxy::TEvAllocateTxId(),
                0, op->GetTxId());
            op->SetWaitingForGlobalTxIdFlag();

            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        } catch (const TNotReadyTabletException&) {
            return OnTabletNotReadyException(userDb, *writeOp, validatedOperationIndex, txc, ctx);
        } catch (const TLockedWriteLimitException&) {
            userDb.ResetCollectedChanges();

            // Note: we don't return TxLocks in the reply since all changes are rolled back and lock is unaffected
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Shard " << tabletId << " cannot write more uncommitted changes");

            // Transaction may have made some changes before it hit the limit,
            // so we need to roll them back.
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }

            guardLocks.WriteSeqNumUpdate.Clear();

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            writeOp->ReleaseTxData(txc);
            return EExecutionStatus::Executed;
        } catch (const TUniqueConstrainException&) {
            if (!OnUniqueConstrainException(userDb, *writeOp, guardLocks.LockTxId, txc, ctx)) {
                return EExecutionStatus::Continue;
            }

            // INSERT with unique constrain violation are also counted as read for stats.
            const auto& counters = userDb.GetCounters();
            KqpUpdateDataShardStatCounters(DataShard, counters);
            KqpFillTxStats(DataShard, counters, *writeOp->GetWriteResult()->Record.MutableTxStats());

            guardLocks.WriteSeqNumUpdate.Clear();

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            writeOp->ReleaseTxData(txc);
            return EExecutionStatus::Executed;
        } catch (const TSerializableIsolationException&) {
            if (CheckForVolatileReadDependencies(userDb, *writeOp, txc, ctx)) {
                return EExecutionStatus::Continue;
            }

            YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TX_DATASHARD, "TExecuteWriteUnit::Execute: aborting, write conflict with concurrent transaction",
                {"operation", *writeOp},
                {"tabletId", DataShard.TabletID()});
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Write conflict with concurrent transaction.");
            writeOp->GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(1);
            NDataIntegrity::LogVictimDetected(ctx, DataShard.TabletID(), "Write transaction was a victim of broken locks",
                                              guardLocks.LockTxId ? DataShard.SysLocksTable().GetVictimQuerySpanIdForLock(guardLocks.LockTxId) : Nothing());
            if (guardLocks.LockTxId) {
                FillDeferredBreakerInfo(guardLocks.LockTxId, writeOp->GetWriteResult()->Record.MutableTxStats());
            }

            ResetChanges(userDb, txc);
            guardLocks.WriteSeqNumUpdate.Clear();

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            return EExecutionStatus::Executed;
        } catch (const TKeySizeConstraintException&) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, TStringBuilder() << "Size of key in secondary index is more than " << NLimits::MaxWriteKeySize);
            txc.DB.RollbackChanges();
            YDB_LOG_ERROR_CTX_COMP(ctx, NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, "TExecuteWriteUnit::Execute: aborting, secondary index key is too big",
                {"operation", *writeOp},
                {"tabletId", DataShard.TabletID()});

            guardLocks.WriteSeqNumUpdate.Clear();

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            return EExecutionStatus::Executed;
        }

        Pipeline.AddCommittingOp(op);

        op->ResetCurrentTimer();

        if (txc.DB.HasChanges()) {
            op->SetWaitCompletionFlag(true);
        }

        if (preparedOutReadSets && !op->PreparedOutReadSets().empty()) {
            return EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        if (!op->PreparedOutReadSets().empty()) {
            DataShard.SendReadSets(ctx, std::move(op->PreparedOutReadSets()));
        }
    }

};  // TExecuteWriteUnit

THolder<TExecutionUnit> CreateExecuteWriteUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TExecuteWriteUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
