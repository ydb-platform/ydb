#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "setup_sys_locks.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/generic/set.h>
#include <util/string/escape.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {


class TDataShard::TTxInitSplitMergeDestination : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvInitSplitMergeDestination::TPtr Ev;

public:
    TTxInitSplitMergeDestination(TDataShard* ds, TEvDataShard::TEvInitSplitMergeDestination::TPtr ev)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SPLIT_MERGE_DESTINATION; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        if (Self->State != TShardState::WaitScheme) {
            // TODO: check if this is really a repeated messages and not a buggy one
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        const bool initializeSchema = Ev->Get()->Record.HasCreateTable();
        if (initializeSchema) {
            // Schema changes must go first in a transaction
            const NKikimrSchemeOp::TTableDescription& createTable = Ev->Get()->Record.GetCreateTable();

            TPathId tableId(Self->GetPathOwnerId(), createTable.GetId_Deprecated());
            if (createTable.HasPathId()) {
                Y_ENSURE(Self->GetPathOwnerId() == createTable.GetPathId().GetOwnerId() || Self->GetPathOwnerId() == INVALID_TABLET_ID);
                tableId = TPathId::FromProto(createTable.GetPathId());
            } else if (tableId.OwnerId == INVALID_TABLET_ID) {
                // Legacy schemeshard before migrations, shouldn't be possible
                tableId.OwnerId = Ev->Get()->Record.GetSchemeshardTabletId();
            }

            auto info = Self->CreateUserTable(txc, createTable);
            Self->AddUserTable(tableId, info);

            if (Self->GetPathOwnerId() == INVALID_TABLET_ID) {
                Self->PersistOwnerPathId(tableId.OwnerId, txc);
            }

            if (info->NeedSchemaSnapshots()) {
                const ui64 txId = Ev->Get()->Record.GetOperationCookie();
                Self->AddSchemaSnapshot(tableId, info->GetTableSchemaVersion(), 0, txId, txc, ctx);
            }

            for (const auto& [streamId, stream] : info->CdcStreams) {
                if (const auto heartbeatInterval = stream.ResolvedTimestampsInterval) {
                    Self->GetCdcStreamHeartbeatManager().AddCdcStream(txc.DB, tableId, streamId, heartbeatInterval);
                }
            }
        }

        Self->DstSplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>(Ev->Get()->Record.GetSplitDescription());

        for (ui32 i = 0; i < Self->DstSplitDescription->SourceRangesSize(); ++i) {
            ui64 srcTabletId = Self->DstSplitDescription->GetSourceRanges(i).GetTabletID();
            Self->ReceiveSnapshotsFrom.insert(srcTabletId);
        }

        // Persist split description
        TString splitDescr;
        bool serilaizeOk = Self->DstSplitDescription->SerializeToString(&splitDescr);
        Y_ENSURE(serilaizeOk, "Failed to serialize split/merge description");
        Self->PersistSys(db, Schema::Sys_DstSplitDescription, splitDescr);

        if (initializeSchema) {
            Self->DstSplitSchemaInitialized = true;
            Self->PersistSys(db, Schema::Sys_DstSplitSchemaInitialized, ui64(1));
        }

        Self->State = TShardState::SplitDstReceivingSnapshot;
        Self->PersistSys(db, Schema::Sys_State, Self->State);

        Self->CurrentSchemeShardId = Ev->Get()->Record.GetSchemeshardTabletId();
        Self->PersistSys(db, Schema::Sys_CurrentSchemeShardId, Self->CurrentSchemeShardId);

        if (!Self->ProcessingParams && Ev->Get()->Record.HasProcessingParams()) {
            Self->ProcessingParams.reset(new NKikimrSubDomains::TProcessingParams());
            Self->ProcessingParams->CopyFrom(Ev->Get()->Record.GetProcessingParams());
            Self->PersistSys(db, Schema::Sys_SubDomainInfo, Self->ProcessingParams->SerializeAsString());
        }

        if (Self->CurrentSchemeShardId && Ev->Get()->Record.HasSubDomainPathId()) {
            Self->PersistSubDomainPathId(Self->CurrentSchemeShardId, Ev->Get()->Record.GetSubDomainPathId(), txc);
            Self->StopFindSubDomainPathId();
            Self->StartWatchingSubDomainPathId();
        } else {
            Self->StartFindSubDomainPathId();
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        // Send Ack
        TActorId ackTo = Ev->Sender;
        ui64 opId = Ev->Get()->Record.GetOperationCookie();

        YDB_LOG_DEBUG_CTX(ctx, "Ack init split/merge destination",
            {"tabletId", Self->TabletID()},
            {"opId", opId});

        ctx.Send(ackTo, new TEvDataShard::TEvInitSplitMergeDestinationAck(opId, Self->TabletID()));
        Self->SendRegistrationRequestTimeCast(ctx);
    }
};


class TDataShard::TTxSplitTransferSnapshot : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvSplitTransferSnapshot::TPtr Ev;

    bool RequiresLockCopy(const TAncestorLock& ancestor,
            const NKikimrTxDataShard::TKeyRangeToTablet& source) const {
        Y_ENSURE(ancestor.CreatedOnRange);
        const auto& tableInfo = *Self->TableInfos.begin()->second;
        const TSerializedTableRange sourceRange(source.GetKeyRangeBegin(), source.GetKeyRangeEnd(), true, false);
        const auto intersection = Intersect(tableInfo.KeyColumnTypes,
            ancestor.CreatedOnRange->ToTableRange(), sourceRange.ToTableRange());
        return !Intersect(tableInfo.KeyColumnTypes, intersection,
            tableInfo.GetTableRange()).IsEmptyRange(tableInfo.KeyColumnTypes);
    }

    // An empty result means success; otherwise it describes why this tx is rejected.
    TString ParseLockFromSnapshot(const NKikimrTxDataShard::TSplitSrcLockInfo& protoLock,
            const NKikimrTxDataShard::TKeyRangeToTablet& sourceRange,
            THashSet<TLockKey, TLockKeyHash>& keys, ILocksDb::TLockRow& row) const {
        if (!protoLock.GetLockId()) {
            return "Missing LockId";
        }
        if (protoLock.GetCounter() == Max<ui64>()) {
            return "Source lock is broken";
        }
        if (!protoLock.GetReadTables().empty() || protoLock.WriteSeqNumStatesSize()) {
            // Do not synthesize identities or paths from legacy metadata.
            return "Read locks or legacy root WSN state are not supported";
        }
        const auto previous = Self->SysLocksTable().GetRawLock(protoLock.GetLockId());
        if (previous && previous->IsBroken()) {
            return "Lock was already rejected by a previous snapshot";
        }
        row.LockId = protoLock.GetLockId();
        row.LockNodeId = protoLock.GetLockNodeId();
        row.Generation = protoLock.GetGeneration();
        row.Counter = protoLock.GetCounter();
        row.CreateTs = protoLock.GetCreateTimestamp();
        row.Flags = protoLock.GetFlags();
        if (!!(ELockFlags(row.Flags) & ELockFlags::TrackingOnly)) {
            // This is a physical lifetime record, not an acquired logical lock.
            if (!row.LockNodeId || row.Flags != ui64(ELockFlags::TrackingOnly)
                    || protoLock.AncestorLocksSize() || protoLock.WriteTablesSize()
                    || protoLock.ConflictsSize()) {
                return "TrackingOnly requires an owner and no logical state";
            }
            return {};
        }
        if (!protoLock.AncestorLocksSize()) {
            return "Missing logical locks";
        }
        for (const auto& proto : protoLock.GetAncestorLocks()) {
            if (!proto.GetTabletId() || !proto.GetCreatedOn() || !proto.HasCreatedOnRange()
                    || !!(ELockFlags(proto.GetFlags()) & ELockFlags::TrackingOnly)) {
                return "Logical lock has no identity/range or has the TrackingOnly flag";
            }
            auto& ancestor = row.AncestorLocks.emplace_back();
            ancestor.TabletId = proto.GetTabletId();
            ancestor.CreatedOn = proto.GetCreatedOn();
            ancestor.Generation = proto.GetGeneration();
            ancestor.Counter = proto.GetCounter();
            ancestor.CreationTime = TInstant::MicroSeconds(proto.GetCreateTimestamp());
            ancestor.Flags = ELockFlags(proto.GetFlags());
            ancestor.CreatedOnRange.emplace(proto.GetCreatedOnRange());
            if (!keys.insert(ancestor.GetKey()).second) {
                return "Repeated logical lock key";
            }
            if (!RequiresLockCopy(ancestor, sourceRange)) {
                return "Logical lock range does not intersect source and destination";
            }
            if (!previous || !previous->FindAncestorLock(ancestor.GetKey())) {
                for (const auto& source : Self->DstSplitDescription->GetSourceRanges()) {
                    if (!Self->ReceiveSnapshotsFrom.contains(source.GetTabletID()) && RequiresLockCopy(ancestor, source)) {
                        // This key was already required in a previous snapshot.
                        return TStringBuilder() << "Logical lock copy missing from previous source " << source.GetTabletID();
                    }
                }
            }
            for (const auto& protoState : proto.GetWriteSeqNumStates()) {
                TWriteSeqNumState state;
                state.WriterIndex = protoState.GetWriterIndex();
                state.WriteSeqNum = protoState.GetWriteSeqNum();
                // An absent writer is valid, but a present state must record
                // an applied operation, even if that operation wrote no rows.
                if (!state.WriteSeqNum) {
                    return TStringBuilder() << "Zero WSN for writer " << state.WriterIndex;
                }
                state.SerializedResult = protoState.GetSerializedResult();
                if (protoState.HasGcWriteSeqNum()) {
                    state.GcWriteSeqNum = protoState.GetGcWriteSeqNum();
                }
                ui64 maxBranchWriteSeqNum = 0;
                TSet<TVector<ui64>> paths;
                for (const auto& item : protoState.GetBranches()) {
                    auto& branch = state.Branches.emplace_back();
                    branch.ShardsPath.assign(item.GetShardsPath().begin(), item.GetShardsPath().end());
                    branch.WriteSeqNum = item.GetWriteSeqNum();
                    branch.SerializedResult = item.GetSerializedResult();
                    if (branch.ShardsPath.empty() || branch.ShardsPath.front() != ancestor.TabletId
                            || !branch.WriteSeqNum || branch.WriteSeqNum > state.WriteSeqNum) {
                        return TStringBuilder() << "Invalid branch path or WSN for writer " << state.WriterIndex;
                    }
                    THashSet<ui64> visited;
                    for (ui64 shard : branch.ShardsPath) {
                        if (!shard || !visited.insert(shard).second) {
                            return TStringBuilder() << "Zero or repeated shard in branch of writer " << state.WriterIndex;
                        }
                    }
                    if (!paths.insert(branch.ShardsPath).second) {
                        return TStringBuilder() << "Repeated branch path for writer " << state.WriterIndex;
                    }
                    maxBranchWriteSeqNum = Max(maxBranchWriteSeqNum, branch.WriteSeqNum);
                }
                // Deduplication uses branches. An aggregate without its latest
                // branch could let a retry execute again with the same WSN.
                if (state.WriteSeqNum != maxBranchWriteSeqNum) {
                    return TStringBuilder() << "Aggregate WSN does not match branches for writer " << state.WriterIndex;
                }
                if (!ancestor.WriteSeqNumStates.emplace(state.WriterIndex, std::move(state)).second) {
                    return TStringBuilder() << "Repeated writer " << protoState.GetWriterIndex();
                }
            }
        }
        for (const auto& path : protoLock.GetWriteTables()) {
            row.WriteTables.push_back(TPathId::FromProto(path));
        }
        return {};
    }

    void RestoreLocksFromSnapshot(const NKikimrTxDataShard::TEvSplitTransferSnapshot& record,
            TTransactionContext& txc, const TActorContext& ctx) {
        // Each retained logical lock has a copy from every already received
        // source whose range requires it. Both the locks and the received-source
        // set are persistent, so this check also works after a destination reboot.
        Y_ENSURE(Self->TableInfos.size() == 1);
        const auto& tableInfo = *Self->TableInfos.begin()->second;
        if (tableInfo.IsReplicated() || tableInfo.IsIncrementalRestore()) {
            // Replication and restore track UpdateTx through offsets, not locks.
            return;
        }
        const ui64 srcTabletId = record.GetSrcTabletId();
        const NKikimrTxDataShard::TKeyRangeToTablet* sourceRange = nullptr;
        for (const auto& source : Self->DstSplitDescription->GetSourceRanges()) {
            if (source.GetTabletID() == srcTabletId) {
                sourceRange = &source;
                break;
            }
        }
        Y_ENSURE(sourceRange);

        TDataShardLocksDb locksDb(*Self, txc);
        THashSet<ui64> invalidLocks;
        auto reject = [&](ui64 lockId, const TString& reason) {
            if (invalidLocks.insert(lockId).second) {
                YDB_LOG_WARN_CTX(ctx, "Rejecting transferred lock",
                    {"tabletId", Self->TabletID()},
                    {"srcTabletId", srcTabletId},
                    {"lockId", lockId},
                    {"reason", reason});
            }
        };
        THashMap<ui64, ui32> lockOwners;
        THashMap<ui64, THashSet<TLockKey, TLockKeyHash>> incomingKeys;
        THashSet<ui64> seenIds;
        TVector<ILocksDb::TLockRow> rows;
        TVector<std::pair<ui64, ui64>> pendingConflicts;
        for (const auto& protoLock : record.GetLocks()) {
            const ui64 lockId = protoLock.GetLockId();
            // Preserve the owner before validating logical state: even rejected
            // ancestors/WSN must leave a broken lock that follows its owner's life.
            lockOwners.emplace(lockId, protoLock.GetLockNodeId());
            THashSet<TLockKey, TLockKeyHash> keys;
            ILocksDb::TLockRow row{};
            if (!seenIds.insert(lockId).second) {
                reject(lockId, "Repeated physical lock id");
                continue;
            }
            if (auto error = ParseLockFromSnapshot(protoLock, *sourceRange, keys, row)) {
                reject(lockId, error);
                continue;
            }
            if (!(ELockFlags(row.Flags) & ELockFlags::TrackingOnly)) {
                incomingKeys.emplace(lockId, std::move(keys));
            }
            for (ui64 conflictId : protoLock.GetConflicts()) {
                pendingConflicts.emplace_back(lockId, conflictId);
            }
            rows.push_back(std::move(row));
        }
        // CreatedOnRange determines which sources must supply each logical copy.
        // A TrackingOnly record carries no logical state and cannot satisfy it.
        for (const auto& [lockId, lock] : Self->SysLocksTable().GetLocks()) {
            const auto* keys = incomingKeys.FindPtr(lockId);
            for (const auto& [key, ancestor] : lock->GetAncestorLocks()) {
                if (!ancestor.CreatedOnRange || (RequiresLockCopy(ancestor, *sourceRange) && (!keys || !keys->contains(key)))) {
                    reject(lockId, "Required logical copy is missing or has no creation range");
                }
            }
        }
        for (auto& row : rows) {
            const ui64 lockId = row.LockId;
            TString error;
            if (!invalidLocks.contains(lockId)
                    && !Self->SysLocksTable().RestoreLockFromSplitSrc(std::move(row), locksDb, &error)) {
                reject(lockId, error);
            }
        }
        if (!invalidLocks.empty()) {
            // LoanTable must finish before any writes to the borrowed user
            // table, including RemoveTx. The ordinary persistent broken lock
            // prevents further writes and follows its owner's lifetime.
            for (ui64 lockId : invalidLocks) {
                ILocksDb::TLockRow row{};
                row.LockId = lockId;
                // A completely missing Locks entry may leave no known owner.
                // Restore preserves an existing owner's node from other sources;
                // otherwise keep node 0 and reject writes without a subscription.
                row.LockNodeId = lockOwners.Value(lockId, 0);
                row.Counter = Max<ui64>();
                Y_ENSURE(Self->SysLocksTable().RestoreLockFromSplitSrc(std::move(row), locksDb));
            }
        }
        for (auto [lockId, conflictId] : pendingConflicts) {
            Self->SysLocksTable().RestoreConflictFromSplitSrc(lockId, conflictId, locksDb);
        }
    }

public:
    TTxSplitTransferSnapshot(TDataShard* ds, TEvDataShard::TEvSplitTransferSnapshot::TPtr& ev)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_SPLIT_TRANSFER_SNAPSHOT; }

    /**
     * Initialize schema based on the first received snapshot
     *
     * Legacy code path for splits initiated by old-style schemeshard
     */
    void LegacyInitSchema(TTransactionContext& txc) {
        const auto& tableScheme = Ev->Get()->Record.GetUserTableScheme();
        TPathId tableId(Self->GetPathOwnerId(), tableScheme.GetId_Deprecated());
        if (tableScheme.HasPathId()) {
            Y_ENSURE(Self->GetPathOwnerId() == tableScheme.GetPathId().GetOwnerId()
                || Self->GetPathOwnerId() == INVALID_TABLET_ID);
            tableId = TPathId::FromProto(tableScheme.GetPathId());
        } else if (tableId.OwnerId == INVALID_TABLET_ID) {
            tableId.OwnerId = Self->CurrentSchemeShardId;
        }
        TString tableName = TDataShard::Schema::UserTablePrefix + tableScheme.GetName();
        if (!txc.DB.GetScheme().TableNames.contains(tableName)) { // TODO: properly check if table has already been created
            NKikimrSchemeOp::TTableDescription newTableScheme(tableScheme);

            // Get this shard's range boundaries from the split/merge description
            TString rangeBegin, rangeEnd;
            for (ui32 di = 0; di < Self->DstSplitDescription->DestinationRangesSize(); ++di) {
                const auto& dstRange = Self->DstSplitDescription->GetDestinationRanges(di);
                if (dstRange.GetTabletID() != Self->TabletID())
                    continue;
                rangeBegin = dstRange.GetKeyRangeBegin();
                rangeEnd = dstRange.GetKeyRangeEnd();
            }

            newTableScheme.SetPartitionRangeBegin(rangeBegin);
            newTableScheme.SetPartitionRangeEnd(rangeEnd);
            newTableScheme.SetPartitionRangeBeginIsInclusive(true);
            newTableScheme.SetPartitionRangeEndIsInclusive(false);

            auto info = Self->CreateUserTable(txc, newTableScheme);
            // Lock restoration in this transaction needs the schema in memory.
            Self->AddUserTable(tableId, info);
        }
        // On restart an existing table has already been loaded by TxInit.
        Y_ENSURE(Self->TableInfos.contains(tableId.LocalPathId));
        if (Self->GetPathOwnerId() == INVALID_TABLET_ID) {
            Self->PersistOwnerPathId(tableId.OwnerId, txc);
        }
        NIceDb::TNiceDb db(txc.DB);
        Self->DstSplitSchemaInitialized = true;
        Self->PersistSys(db, Schema::Sys_DstSplitSchemaInitialized, ui64(1));
    }

    void RestoreLocalHnswMetadata(TTransactionContext& txc) {
        // Modern split initialization receives the logical schema from SchemeShard.
        // Local graph annotations belong to the source DataShard's persisted schema.
        const auto& source = Ev->Get()->Record.GetUserTableScheme();
        const ui64 tableId = source.HasPathId() ? source.GetPathId().GetLocalId() : source.GetId_Deprecated();
        if (source.HasLocalHnswIndex()) {
            TUserTable::TPtr updated = new TUserTable(*Self->TableInfos.at(tableId));
            updated->UpdateLocalHnswIndex(source);
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistUserTable(db, tableId, *updated);
            Self->SetUserTable(TPathId(Self->GetPathOwnerId(), tableId), std::move(updated));
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Ev->Get()->Record;

        ui64 srcTabletId = Ev->Get()->Record.GetSrcTabletId();
        ui64 opId = Ev->Get()->Record.GetOperationCookie();

        if (Self->State != TShardState::SplitDstReceivingSnapshot || !Self->ReceiveSnapshotsFrom.contains(srcTabletId)) {
            YDB_LOG_DEBUG_CTX(ctx, "Ignoring received snapshot for split/merge TxId from tabletId",
                {"tabletId", Self->TabletID()},
                {"opId", opId},
                {"srcTabletId", srcTabletId});
            return true;
        }

        YDB_LOG_DEBUG_CTX(ctx, "Received snapshot for split/merge from srcTabletId",
            {"tabletId", Self->TabletID()},
            {"opId", opId},
            {"srcTabletId", srcTabletId});
        YDB_LOG_TRACE_CTX(ctx, "Received snapshot",
            {"tabletId", Self->TabletID()},
            {"snapshot", record.DebugString()});

        if (!Self->DstSplitSchemaInitialized) {
            LegacyInitSchema(txc);
        } else {
            RestoreLocalHnswMetadata(txc);
        }

        bool restoreLegacyRemovedRowVersions = false;
        for (ui32 i = 0 ; i < Ev->Get()->Record.TableSnapshotSize(); ++i) {
            ui32 localTableId = Ev->Get()->Record.GetTableSnapshot(i).GetTableId();
            TString compressedBody = Ev->Get()->Record.GetTableSnapshot(i).GetSnapshotData();
            TString snapBody = NBlockCodecs::Codec("lz4fast")->Decode(compressedBody);
            txc.Env.LoanTable(localTableId, snapBody);
            const auto& tableSnapshot = record.GetTableSnapshot(i);
            if (tableSnapshot.HasRemovedRowVersions()) {
                // Union across merge sources: a snapshot is readable only if
                // its history is available in every source key range.
                TSnapshotManager::RestoreRemovedRowVersions(txc.DB, localTableId,
                    tableSnapshot.GetRemovedRowVersions());
            } else if (localTableId >= Schema::MinLocalTid) {
                restoreLegacyRemovedRowVersions = true;
            }
        }

        NIceDb::TNiceDb db(txc.DB);

        // Choose the highest write version, so we won't overwrite any important data
        TRowVersion minWriteVersion(record.GetMinWriteVersionStep(), record.GetMinWriteVersionTxId());

        if (Self->GetSnapshotManager().GetMinWriteVersion() < minWriteVersion)
            Self->GetSnapshotManager().SetMinWriteVersion(db, minWriteVersion);

        TRowVersion completeEdge(record.GetMvccCompleteEdgeStep(), record.GetMvccCompleteEdgeTxId());
        if (Self->GetSnapshotManager().GetCompleteEdge() < completeEdge)
            Self->GetSnapshotManager().SetCompleteEdge(db, completeEdge);
        TRowVersion incompleteEdge(record.GetMvccIncompleteEdgeStep(), record.GetMvccIncompleteEdgeTxId());
        if (Self->GetSnapshotManager().GetIncompleteEdge() < incompleteEdge)
            Self->GetSnapshotManager().SetIncompleteEdge(db, incompleteEdge);
        TRowVersion immediateWriteEdge(record.GetMvccImmediateWriteEdgeStep(), record.GetMvccImmediateWriteEdgeTxId());
        if (Self->GetSnapshotManager().GetImmediateWriteEdge() < immediateWriteEdge)
            Self->GetSnapshotManager().SetImmediateWriteEdge(db, immediateWriteEdge);
        TRowVersion lowWatermark(record.GetMvccLowWatermarkStep(), record.GetMvccLowWatermarkTxId());
        if (Self->GetSnapshotManager().GetLowWatermark() < lowWatermark)
            Self->GetSnapshotManager().SetLowWatermark(db, lowWatermark);

        // Would be true for the first snapshot we receive, e.g. during a merge
        const bool isFirstSnapshot = Self->ReceiveSnapshotsFrom.size() == Self->DstSplitDescription->SourceRangesSize();

        THashSet<TSnapshotKey> receivedKeys;
        for (const auto& snapshot : record.GetPersistentSnapshots()) {
            TSnapshotKey key(snapshot.GetOwnerId(), snapshot.GetPathId(), snapshot.GetStep(), snapshot.GetTxId());
            if (!isFirstSnapshot) {
                // The same key should have the same data
                receivedKeys.insert(key);
                continue;
            }

            Self->GetSnapshotManager().PersistAddSnapshot(
                db, key,
                snapshot.GetName(),
                snapshot.GetFlags(),
                TDuration::MilliSeconds(snapshot.GetTimeoutMs()));
        }

        // Snapshots are only valid when received from all source shards
        if (!isFirstSnapshot) {
            TVector<TSnapshotKey> invalidKeys;
            for (const auto& kv : Self->GetSnapshotManager().GetSnapshots()) {
                if (!receivedKeys.contains(kv.first)) {
                    invalidKeys.push_back(kv.first);
                }
            }
            for (const auto& key : invalidKeys) {
                Self->GetSnapshotManager().PersistRemoveSnapshot(db, key);
            }
        }

        if (restoreLegacyRemovedRowVersions) {
            // Old sources only guaranteed persistent snapshots below their
            // watermark. Restore those read guarantees even if the registry is
            // unready, independently of optional additional cleanup below.
            Self->GetSnapshotManager().RemoveRowVersions(txc.DB, TRowVersion::Min(), lowWatermark);
        }

        RestoreLocksFromSnapshot(record, txc, ctx);

        // Persist the fact that the snapshot has been received, so that duplicate event can be ignored
        db.Table<Schema::SplitDstReceivedSnapshots>().Key(srcTabletId).Update();
        Self->ReceiveSnapshotsFrom.erase(srcTabletId);

        if (record.GetWaitForActivation()) {
            Self->ReceiveActivationsFrom.insert(srcTabletId);
            db.Table<Schema::DstChangeSenderActivations>().Key(srcTabletId).Update();
        }

        if (Self->ReceiveSnapshotsFrom.empty()) {
            const auto minVersion = Self->GetSnapshotManager().GetLowWatermark();

            // Source deletions (or the legacy fallback) were restored above
            // without consulting retention. Only additional cleanup is gated:
            // the merged watermark may still contain retained snapshot points.
            Self->GetSnapshotManager().CleanupUnretainedRowVersions(txc.DB, TRowVersion::Min(), minVersion);

            for (auto& kv : Self->ReplicatedTables) {
                TReplicationSourceOffsetsDb rdb(txc);
                kv.second.OptimizeSplitKeys(rdb);
            }

            Self->PromoteFollowerReadEdge(txc);

            // We could perform snapshot reads after becoming ready
            // Make sure older versions restore mediator state in that case
            Self->PersistUnprotectedReadsEnabled(db);

            // Schedule a new transaction that will move shard to the Ready state
            // and finish initialization. This new transaction is guaranteed to
            // wait until async LoanTable above is complete and new parts are
            // fully merged into the table.
            Self->Execute(Self->CreateTxSplitLastSnapshotReceived());
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        TActorId ackTo = Ev->Sender;
        ui64 opId = Ev->Get()->Record.GetOperationCookie();

        YDB_LOG_DEBUG_CTX(ctx, "Ack snapshot",
            {"tabletId", Self->TabletID()},
            {"opId", opId});

        ctx.Send(ackTo, new TEvDataShard::TEvSplitTransferSnapshotAck(opId, Self->TabletID()));
    }

};

class TDataShard::TTxSplitLastSnapshotReceived : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    explicit TTxSplitLastSnapshotReceived(TDataShard* self)
        : TTransactionBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (Self->State != TShardState::SplitDstReceivingSnapshot || !Self->ReceiveSnapshotsFrom.empty()) {
            return true;
        }

        // Imported commit/abort decisions are visible after LoanTable completes.
        // Remove restored locks contradicted by these decisions before Ready.
        TDataShardLocksDb locksDb(*Self, txc);
        TSetupSysLocks guardLocks(*Self, &locksDb);
        for (const auto& [lockId, lock] : Self->SysLocksTable().GetLocks()) {
            for (const auto& [_, table] : Self->GetUserTables()) {
                if (txc.DB.HasRemovedTx(table->LocalTid, lockId)
                        || (lock->IsTrackingOnly() && txc.DB.HasCommittedTx(table->LocalTid, lockId))) {
                    Self->SysLocksTable().EraseLock(lockId);
                    break;
                }
            }
        }
        // Apply erases after traversal to keep the GetLocks() iterator valid.
        // TODO: Bound erase redo and continue in another transaction. LockLimit
        // does not bound the number of ancestor branches deleted with each lock.
        Self->SysLocksTable().ApplyLocks();
        // SwitchToWork starts the ordinary orphan cleanup after Ready. Leave
        // unknown localdb ids without a lock so that cleanup can identify them.
        Self->SetPersistState(TShardState::Ready, txc);
        Self->State = TShardState::SplitDstReceivingSnapshot;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Self->State == TShardState::SplitDstReceivingSnapshot && Self->ReceiveSnapshotsFrom.empty()) {
            // We have received all the data, finish shard initialization
            // Note: previously we used TxInit, however received system tables
            // have been empty for years now, and since pipes are still open we
            // may receive requests between TxInit loading the Ready state and
            // its Complete method initializing everything properly. Instead
            // necessary steps are repeated here.
            Self->State = TShardState::Ready;

            // We are already in StateWork, but we need to repeat many steps now that we are Ready
            Self->SwitchToWork(ctx);

            // Start subscriptions deferred by TTxInit and snapshot reception.
            // Owner notifications may now remove logical, broken and tracking
            // records after LoanTable and the final transition have committed.
            Self->SubscribeNewLocks(ctx);

            // We can send the registration request now that we are ready
            Self->SendRegistrationRequestTimeCast(ctx);

            // Initialize snapshot expiration queue with current context time
            Self->GetSnapshotManager().InitExpireQueue(ctx.Now());
            Self->PlanCleanup(ctx);

            // Initialize change senders
            Self->KillChangeSender(ctx);
            Self->CreateChangeSender(ctx);
            Self->MaybeActivateChangeSender(ctx);
            Self->EmitHeartbeats();
        }
    }
};

NTabletFlatExecutor::ITransaction* TDataShard::CreateTxSplitLastSnapshotReceived() {
    return new TTxSplitLastSnapshotReceived(this);
}

class TDataShard::TTxSplitReplicationSourceOffsets : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvPrivate::TEvReplicationSourceOffsets::TPtr Ev;

public:
    TTxSplitReplicationSourceOffsets(TDataShard* ds, TEvPrivate::TEvReplicationSourceOffsets::TPtr& ev)
        : TTransactionBase(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_SPLIT_REPLICATION_SOURCE_OFFSETS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto* msg = Ev->Get();

        YDB_LOG_DEBUG_CTX(ctx, "Received ReplicationSourceOffsets from srcTablet for table with pathId",
            {"tabletId", Self->TabletID()},
            {"srcTabletId", msg->SrcTabletId},
            {"pathId", msg->PathId});

        auto itSrcTablets = Self->ReceiveReplicationSourceOffsetsFrom.find(msg->SrcTabletId);
        if (itSrcTablets == Self->ReceiveReplicationSourceOffsetsFrom.end() ||
            !itSrcTablets->second.Pending.contains(msg->PathId))
        {
            // Shouldn't really happen, but just ignore
            YDB_LOG_WARN_CTX(ctx, "Ignoring unexpected ReplicationSourceOffsets from srcTablet for table with pathId",
                {"tabletId", Self->TabletID()},
                {"srcTabletId", msg->SrcTabletId},
                {"pathId", msg->PathId});
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        TReplicationSourceOffsetsDb rdb(txc);

        itSrcTablets->second.Pending.erase(msg->PathId);
        itSrcTablets->second.Received.insert(msg->PathId);
        db.Table<Schema::DstReplicationSourceOffsetsReceived>()
            .Key(msg->SrcTabletId, msg->PathId.OwnerId, msg->PathId.LocalPathId)
            .Update();

        if (itSrcTablets->second.Pending.empty() && itSrcTablets->second.Snapshot) {
            // Schedule transfer snapshot transaction
            auto snapshot = std::move(itSrcTablets->second.Snapshot);
            Self->Execute(new TTxSplitTransferSnapshot(Self, snapshot), ctx);
        }

        if (Self->State != TShardState::SplitDstReceivingSnapshot || !Self->ReceiveSnapshotsFrom.contains(msg->SrcTabletId)) {
            // We may have received snapshot from an old unsupported version
            YDB_LOG_WARN_CTX(ctx, "Ignoring valid ReplicationSourceOffsets from srcTablet for table with pathId due to unexpected state, possible protocol violation",
                {"tabletId", Self->TabletID()},
                {"srcTabletId", msg->SrcTabletId},
                {"pathId", msg->PathId});
            return true;
        }

        const auto& userTables = Self->GetUserTables();
        Y_ENSURE(msg->PathId.OwnerId == Self->GetPathOwnerId());
        auto itUserTables = userTables.find(msg->PathId.LocalPathId);
        Y_ENSURE(itUserTables != userTables.end());
        TUserTable::TCPtr tableInfo = itUserTables->second;
        TConstArrayRef<NScheme::TTypeInfo> keyColumnTypes = tableInfo->KeyColumnTypes;

        auto* replTable = Self->EnsureReplicatedTable(msg->PathId);
        if (!replTable) {
            // An older source may send offsets after replication was disabled.
            // Keep the receipt bookkeeping without recreating replication state.
            return true;
        }

        if (Self->SrcTabletToRange.empty()) {
            for (const auto& srcRange : Self->DstSplitDescription->GetSourceRanges()) {
                Self->SrcTabletToRange.emplace(
                    srcRange.GetTabletID(),
                    TSerializedTableRange(
                        srcRange.GetKeyRangeBegin(),
                        srcRange.GetKeyRangeEnd(),
                        true, false));
            }
        }

        if (!Self->SrcTabletToRange.contains(msg->SrcTabletId)) {
            // This should be impossible, since shard list is constructed from source ranges
            YDB_LOG_WARN_CTX(ctx, "Ignoring valid ReplicationSourceOffsets from srcTablet for table with pathId without a known range description",
                {"tabletId", Self->TabletID()},
                {"srcTabletId", msg->SrcTabletId},
                {"pathId", msg->PathId});
            return true;
        }

        const auto& srcRange = Self->SrcTabletToRange.at(msg->SrcTabletId);
        const auto& dstRange = tableInfo->Range;

        // Source tablet range constrained to the destination range
        TSerializedTableRange range = srcRange;

        // True when left or right border matches destination range
        bool leftFull = false;
        bool rightFull = false;

        if (ComparePrefixBorders(
            keyColumnTypes,
            range.From.GetCells(), PrefixModeLeftBorderInclusive,
            dstRange.From.GetCells(), PrefixModeLeftBorderInclusive) <= 0)
        {
            range.From = dstRange.From;
            leftFull = true;
        }

        if (ComparePrefixBorders(
            keyColumnTypes,
            dstRange.To.GetCells(), dstRange.To.GetCells() ? PrefixModeRightBorderNonInclusive : PrefixModeRightBorderInclusive,
            range.To.GetCells(), range.To.GetCells() ? PrefixModeRightBorderNonInclusive : PrefixModeRightBorderInclusive) <= 0)
        {
            range.To = dstRange.To;
            rightFull = true;
        }

        YDB_LOG_TRACE_CTX(ctx, "Calculated ReplicationSourceOffsets range",
            {"tabletId", Self->TabletID()},
            {"rangeFrom", EscapeC(range.From.GetBuffer())},
            {"leftFull", leftFull},
            {"rangeTo", EscapeC(range.To.GetBuffer())},
            {"rightFull", rightFull});

        // Sanity check that range.From < range.To, otherwise we won't compute split points correctly
        // This shouldn't happen in practice though
        if (range.To.GetCells() &&
            ComparePrefixBorders(
                keyColumnTypes,
                range.From.GetCells(), PrefixModeLeftBorderInclusive,
                range.To.GetCells(), PrefixModeRightBorderNonInclusive) >= 0)
        {
            YDB_LOG_WARN_CTX(ctx, "Ignoring ReplicationSourceOffsets from srcTabletId for table with pathId with an incorrect range",
                {"tabletId", Self->TabletID()},
                {"srcTabletId", msg->SrcTabletId},
                {"pathId", msg->PathId});
            return true;
        }

        auto keyLess = [keyColumnTypes](TConstArrayRef<TCell> a, TConstArrayRef<TCell> b) -> bool {
            int r = ComparePrefixBorders(
                keyColumnTypes,
                a, PrefixModeLeftBorderInclusive,
                b, PrefixModeLeftBorderInclusive);
            return r < 0;
        };

        for (auto& kvSource : msg->SourceOffsets) {
            const TString& sourceName = kvSource.first;
            auto& source = replTable->EnsureSource(rdb, sourceName);

            // Sort split keys, since originally they are unsorted
            auto splitKeyLess = [keyLess](const auto& a, const auto& b) -> bool {
                const auto aCells = a.SplitKey.GetCells();
                const auto bCells = b.SplitKey.GetCells();
                return keyLess(aCells, bCells);
            };
            std::sort(kvSource.second.begin(), kvSource.second.end(), splitKeyLess);

            auto leftLess = [keyLess](const TSerializedCellVec& a, const auto& b) -> bool {
                const auto aCells = a.GetCells();
                const auto bCells = b.SplitKey.GetCells();
                return keyLess(aCells, bCells);
            };

            auto rightLess = [keyColumnTypes](const auto& a, const TSerializedCellVec& b) -> bool {
                const auto aCells = a.SplitKey.GetCells();
                const auto bCells = b.GetCells();
                if (bCells.empty()) {
                    return true; // all keys are less than empty right border (+inf)
                }
                int r = ComparePrefixBorders(
                    keyColumnTypes,
                    aCells, PrefixModeLeftBorderInclusive,
                    bCells, PrefixModeRightBorderNonInclusive);
                return r < 0;
            };

            // Find split keys that are in the (From, To) range
            auto itBegin = std::upper_bound(kvSource.second.begin(), kvSource.second.end(), range.From, leftLess);
            auto itEnd = std::lower_bound(kvSource.second.begin(), kvSource.second.end(), range.To, rightLess);
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                << " Source " << EscapeC(sourceName)
                << " " << kvSource.second.size() << " split keys"
                << ", in-range [" << (itBegin - kvSource.second.begin())
                << ", " << (itEnd - kvSource.second.begin()) << ")");

            // Add the shard right border first
            if (!range.To.GetCells().empty() && !rightFull) {
                YDB_LOG_TRACE_CTX(ctx, "Source adding right split at key",
                    {"tabletId", Self->TabletID()},
                    {"sourceName", EscapeC(sourceName)},
                    {"splitKey", EscapeC(range.To.GetBuffer())});
                source.EnsureSplitKey(rdb, range.To);
            }

            for (auto it = itEnd; it != itBegin;) {
                --it;
                YDB_LOG_TRACE_CTX(ctx, "Source adding middle split at key",
                    {"tabletId", Self->TabletID()},
                    {"sourceName", EscapeC(sourceName)},
                    {"splitKey", EscapeC(it->SplitKey.GetBuffer())},
                    {"offset", it->MaxOffset});
                source.EnsureSplitKey(rdb, it->SplitKey, it->MaxOffset);
            }

            if (itBegin != kvSource.second.begin()) {
                --itBegin;
                const TSerializedCellVec& leftKey = leftFull ? TSerializedCellVec() : range.From;
                YDB_LOG_TRACE_CTX(ctx, "Source adding left split at key",
                    {"tabletId", Self->TabletID()},
                    {"sourceName", EscapeC(sourceName)},
                    {"splitKey", EscapeC(leftKey.GetBuffer())},
                    {"offset", itBegin->MaxOffset});
                source.EnsureSplitKey(rdb, leftKey, itBegin->MaxOffset);
            } else {
                YDB_LOG_WARN_CTX(ctx, "Source has no split key predecessor for range, skipping left split key, MaxOffset stays -1 (dedup reset for this range)",
                    {"tabletId", Self->TabletID()},
                    {"sourceName", EscapeC(sourceName)},
                    {"rangeFrom", EscapeC(range.From.GetBuffer())});
            }

            // Dump final split keys and offsets for debugging
            if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::TX_DATASHARD)) {
                for (const auto* state : source.Offsets) {
                    YDB_LOG_TRACE_CTX(ctx, "Source split key",
                        {"tabletId", Self->TabletID()},
                        {"sourceName", EscapeC(sourceName)},
                        {"splitKey", EscapeC(state->SplitKey.GetBuffer())},
                        {"offset", state->MaxOffset});
                }
            }
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }
};

void TDataShard::Handle(TEvDataShard::TEvInitSplitMergeDestination::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxInitSplitMergeDestination(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvSplitTransferSnapshot::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    const ui64 srcTabletId = msg->Record.GetSrcTabletId();
    const ui64 srcTabletGen = msg->Record.GetSrcTabletGeneration();

    // We delay snapshot processing until all pending source offsets are received and processed
    if (msg->Record.GetReplicationSourceOffsetsBytes() > 0 &&
        ReceiveSnapshotsFrom.contains(srcTabletId) &&
        srcTabletGen != 0)
    {
        auto& entry = ReceiveReplicationSourceOffsetsFrom[srcTabletId];

        if (entry.Pending.empty()) {
            for (const auto& kv : GetUserTables()) {
                TPathId pathId(GetPathOwnerId(), kv.first);
                if (!entry.Received.contains(pathId)) {
                    auto actorId = RegisterWithSameMailbox(CreateReplicationSourceOffsetsClient(SelfId(), srcTabletId, pathId));
                    Actors.insert(actorId);
                    entry.Pending.insert(pathId);
                }
            }
        }

        if (!entry.Pending.empty()) {
            // We only need to keep the latest message from the latest generation
            const ui64 currentTabletGen = entry.Snapshot ? entry.Snapshot->Get()->Record.GetSrcTabletGeneration() : 0;
            if (srcTabletGen >= currentTabletGen) {
                entry.Snapshot = std::move(ev);
            }
            return;
        }
    }

    Execute(new TTxSplitTransferSnapshot(this, ev), ctx);
}

void TDataShard::Handle(TEvPrivate::TEvReplicationSourceOffsets::TPtr& ev, const TActorContext& ctx) {
    Actors.erase(ev->Sender);
    Execute(new TTxSplitReplicationSourceOffsets(this, ev), ctx);
}

}}


#undef YDB_LOG_THIS_FILE_COMPONENT
