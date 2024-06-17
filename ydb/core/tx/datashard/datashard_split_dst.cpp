#include "datashard_impl.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/string/escape.h>

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
                Y_ABORT_UNLESS(Self->GetPathOwnerId() == createTable.GetPathId().GetOwnerId() || Self->GetPathOwnerId() == INVALID_TABLET_ID);
                tableId = PathIdFromPathId(createTable.GetPathId());
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
        Y_ABORT_UNLESS(serilaizeOk, "Failed to serialize split/merge description");
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

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ack init split/merge destination OpId " << opId);

        ctx.Send(ackTo, new TEvDataShard::TEvInitSplitMergeDestinationAck(opId, Self->TabletID()));
        Self->SendRegistrationRequestTimeCast(ctx);
    }
};


class TDataShard::TTxSplitTransferSnapshot : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvSplitTransferSnapshot::TPtr Ev;

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

            Self->CreateUserTable(txc, newTableScheme);
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Ev->Get()->Record;

        ui64 srcTabletId = Ev->Get()->Record.GetSrcTabletId();
        ui64 opId = Ev->Get()->Record.GetOperationCookie();

        if (Self->State != TShardState::SplitDstReceivingSnapshot || !Self->ReceiveSnapshotsFrom.contains(srcTabletId)) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Ignoring received snapshot for split/merge TxId " << opId
                    << " from tabeltId " << srcTabletId);
            return true;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Received snapshot for split/merge TxId " << opId
                    << " from tabeltId " << srcTabletId);
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Received snapshot: " << record.DebugString());

        if (!Self->DstSplitSchemaInitialized) {
            LegacyInitSchema(txc);
        }

        for (ui32 i = 0 ; i < Ev->Get()->Record.TableSnapshotSize(); ++i) {
            ui32 localTableId = Ev->Get()->Record.GetTableSnapshot(i).GetTableId();
            TString compressedBody = Ev->Get()->Record.GetTableSnapshot(i).GetSnapshotData();
            TString snapBody = NBlockCodecs::Codec("lz4fast")->Decode(compressedBody);
            txc.Env.LoanTable(localTableId, snapBody);
        }

        NIceDb::TNiceDb db(txc.DB);

        // Choose the highest write version, so we won't overwrite any important data
        TRowVersion minWriteVersion(record.GetMinWriteVersionStep(), record.GetMinWriteVersionTxId());

        if (Self->GetSnapshotManager().GetMinWriteVersion() < minWriteVersion)
            Self->GetSnapshotManager().SetMinWriteVersion(db, minWriteVersion);

        const bool mvcc = Self->IsMvccEnabled();
        if (mvcc) {
            TRowVersion completeEdge(record.GetMvccCompleteEdgeStep(), record.GetMvccCompleteEdgeTxId());
            if (Self->GetSnapshotManager().GetCompleteEdge() < completeEdge)
                Self->GetSnapshotManager().SetCompleteEdge(db, completeEdge);
            TRowVersion incompleteEdge(record.GetMvccIncompleteEdgeStep(), record.GetMvccIncompleteEdgeTxId());
            if (Self->GetSnapshotManager().GetIncompleteEdge() < incompleteEdge)
                Self->GetSnapshotManager().SetIncompleteEdge(db, incompleteEdge);
            TRowVersion immediateWriteEdge(record.GetMvccImmediateWriteEdgeStep(), record.GetMvccImmediateWriteEdgeTxId());
            if (Self->GetSnapshotManager().GetImmediateWriteEdge() < immediateWriteEdge)
                Self->GetSnapshotManager().SetImmediateWriteEdge(immediateWriteEdge, txc);
            TRowVersion lowWatermark(record.GetMvccLowWatermarkStep(), record.GetMvccLowWatermarkTxId());
            if (Self->GetSnapshotManager().GetLowWatermark() < lowWatermark)
                Self->GetSnapshotManager().SetLowWatermark(db, lowWatermark);
            bool performedUnprotectedReads = record.GetMvccPerformedUnprotectedReads();
            if (!Self->GetSnapshotManager().GetPerformedUnprotectedReads() && performedUnprotectedReads)
                Self->GetSnapshotManager().SetPerformedUnprotectedReads(true, txc);
        }

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

        // Persist the fact that the snapshot has been received, so that duplicate event can be ignored
        db.Table<Schema::SplitDstReceivedSnapshots>().Key(srcTabletId).Update();
        Self->ReceiveSnapshotsFrom.erase(srcTabletId);

        if (record.GetWaitForActivation()) {
            Self->ReceiveActivationsFrom.insert(srcTabletId);
            db.Table<Schema::DstChangeSenderActivations>().Key(srcTabletId).Update();
        }

        if (Self->ReceiveSnapshotsFrom.empty()) {
            const auto minVersion = mvcc ? Self->GetSnapshotManager().GetLowWatermark()
                                         : Self->GetSnapshotManager().GetMinWriteVersion();

            // Mark versions not accessible via snapshots as deleted
            for (const auto& kv : Self->GetUserTables()) {
                // FIXME: tables need to always have owner id
                ui64 ownerId = Self->GetPathOwnerId();
                ui64 tableId = kv.first;
                ui32 localTableId = kv.second->LocalTid;
                TRowVersion vlower = TRowVersion::Min();

                for (const auto& kv : Self->GetSnapshotManager().GetSnapshots(TSnapshotTableKey(ownerId, tableId))) {
                    TRowVersion vupper(kv.first.Step, kv.first.TxId);
                    if (vlower < vupper) {
                        txc.DB.RemoveRowVersions(localTableId, vlower, vupper);
                    }
                    vlower = vupper.Next();
                }

                if (vlower < minVersion) {
                    txc.DB.RemoveRowVersions(localTableId, vlower, minVersion);
                }
            }

            for (auto& kv : Self->ReplicatedTables) {
                TReplicationSourceOffsetsDb rdb(txc);
                kv.second.OptimizeSplitKeys(rdb);
            }

            if (mvcc) {
                Self->PromoteFollowerReadEdge(txc);
            }

            // Note: we persist Ready, but keep current state in memory until Complete
            Self->SetPersistState(TShardState::Ready, txc);
            Self->State = TShardState::SplitDstReceivingSnapshot;

            // Schedule a new transaction that will move shard to the Ready state
            // and finish initialization. This new transaction is guaranteed to
            // wait until async LoanTable above is complete and new parts are
            // fully merged into the table.
            Self->Execute(new TTxLastSnapshotReceived(Self));
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        TActorId ackTo = Ev->Sender;
        ui64 opId = Ev->Get()->Record.GetOperationCookie();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ack snapshot OpId " << opId);

        ctx.Send(ackTo, new TEvDataShard::TEvSplitTransferSnapshotAck(opId, Self->TabletID()));
    }

    class TTxLastSnapshotReceived : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    public:
        TTxLastSnapshotReceived(TDataShard* self)
            : TTransactionBase(self)
        {}

        bool Execute(TTransactionContext&, const TActorContext&) override {
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            // Note: we skip init in an unlikely event of state resetting before reaching Complete
            if (Self->State == TShardState::SplitDstReceivingSnapshot) {
                // We have received all the data, finish shard initialization
                // Note: previously we used TxInit, however received system tables
                // have been empty for years now, and since pipes are still open we
                // may receive requests between TxInit loading the Ready state and
                // its Complete method initializing everything properly. Instead
                // necessary steps are repeated here.
                Self->State = TShardState::Ready;

                // We are already in StateWork, but we need to repeat many steps now that we are Ready
                Self->SwitchToWork(ctx);

                // We can send the registration request now that we are ready
                Self->SendRegistrationRequestTimeCast(ctx);

                // Initialize snapshot expiration queue with current context time
                Self->GetSnapshotManager().InitExpireQueue(ctx.Now());
                if (Self->GetSnapshotManager().HasExpiringSnapshots()) {
                    Self->PlanCleanup(ctx);
                }

                // Initialize change senders
                Self->KillChangeSender(ctx);
                Self->CreateChangeSender(ctx);
                Self->MaybeActivateChangeSender(ctx);
                Self->EmitHeartbeats();

                // Switch mvcc state if needed
                Self->CheckMvccStateChangeCanStart(ctx);
            }
        }
    };
};

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

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
            << " Received ReplicationSourceOffsets from tablet " << msg->SrcTabletId
            << " for table " << msg->PathId);

        auto itSrcTablets = Self->ReceiveReplicationSourceOffsetsFrom.find(msg->SrcTabletId);
        if (itSrcTablets == Self->ReceiveReplicationSourceOffsetsFrom.end() ||
            !itSrcTablets->second.Pending.contains(msg->PathId))
        {
            // Shouldn't really happen, but just ignore
            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                << " Ignoring unexpected ReplicationSourceOffsets from tablet " << msg->SrcTabletId
                << " for table " << msg->PathId);
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
            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                << " Ignoring valid ReplicationSourceOffsets from tablet " << msg->SrcTabletId
                << " for table " << msg->PathId << " due to unexpected state, possible protocol violation");
            return true;
        }

        const auto& userTables = Self->GetUserTables();
        Y_ABORT_UNLESS(msg->PathId.OwnerId == Self->GetPathOwnerId());
        auto itUserTables = userTables.find(msg->PathId.LocalPathId);
        Y_ABORT_UNLESS(itUserTables != userTables.end());
        TUserTable::TCPtr tableInfo = itUserTables->second;
        TConstArrayRef<NScheme::TTypeInfo> keyColumnTypes = tableInfo->KeyColumnTypes;

        auto* replTable = Self->EnsureReplicatedTable(msg->PathId);
        Y_ABORT_UNLESS(replTable);

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
            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                << " Ignoring valid ReplicationSourceOffsets from tablet " << msg->SrcTabletId
                << " for table " << msg->PathId << " without a known range description");
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

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
            << " Calculated ReplicationSourceOffsets range"
            << " from '" << EscapeC(range.From.GetBuffer()) << "'" << (leftFull ? " (full)" : "")
            << " to '" << EscapeC(range.To.GetBuffer()) << "'" << (rightFull ? " (full)" : ""));

        // Sanity check that range.From < range.To, otherwise we won't compute split points correctly
        // This shouldn't happen in practice though
        if (range.To.GetCells() &&
            ComparePrefixBorders(
                keyColumnTypes,
                range.From.GetCells(), PrefixModeLeftBorderInclusive,
                range.To.GetCells(), PrefixModeRightBorderNonInclusive) >= 0)
        {
            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                << " Ignoring ReplicationSourceOffsets from tablet " << msg->SrcTabletId
                << " for table " << msg->PathId << " with an incorrect range");
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
            Y_ABORT_UNLESS(itBegin != kvSource.second.begin());

            // Add the shard right border first
            if (!range.To.GetCells().empty() && !rightFull) {
                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                    << " Source " << EscapeC(sourceName)
                    << " adding right split at key '" << EscapeC(range.To.GetBuffer()) << "'");
                source.EnsureSplitKey(rdb, range.To);
            }

            for (auto it = itEnd; it != itBegin;) {
                --it;
                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                    << " Source " << EscapeC(sourceName)
                    << " adding middle split at key '" << EscapeC(it->SplitKey.GetBuffer()) << "' offset " << it->MaxOffset);
                source.EnsureSplitKey(rdb, it->SplitKey, it->MaxOffset);
            }

            --itBegin;
            const TSerializedCellVec& leftKey = leftFull ? TSerializedCellVec() : range.From;
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                << " Source " << EscapeC(sourceName)
                << " adding left split at key '" << EscapeC(leftKey.GetBuffer()) << "' offset " << itBegin->MaxOffset);
            source.EnsureSplitKey(rdb, leftKey, itBegin->MaxOffset);

            // Dump final split keys and offsets for debugging
            if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::TX_DATASHARD)) {
                for (const auto* state : source.Offsets) {
                    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID()
                        << " Source " << EscapeC(sourceName)
                        << " split key '" << EscapeC(state->SplitKey.GetBuffer()) << "' offset " << state->MaxOffset);
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
