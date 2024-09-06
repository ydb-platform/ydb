#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "setup_sys_locks.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/util/pb.h>

namespace NKikimr {
namespace NDataShard {


class TDataShard::TTxSplit : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvSplit::TPtr Ev;
    bool SplitAlreadyFinished;

    std::vector<std::unique_ptr<IEventHandle>> Replies;

public:
    TTxSplit(TDataShard* ds, TEvDataShard::TEvSplit::TPtr& ev)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , Ev(ev)
        , SplitAlreadyFinished(false)
    {}

    TTxType GetTxType() const override { return TXTYPE_SPLIT; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        ui64 opId = Ev->Get()->Record.GetOperationCookie();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " received split OpId " << opId
                    << " at state " << DatashardStateName(Self->State));

        NIceDb::TNiceDb db(txc.DB);

        if (Self->State == TShardState::Ready) {
            Self->SrcAckSplitTo.insert(Ev->Sender);
            Self->SrcSplitOpId = opId;
            Self->SrcSplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>(Ev->Get()->Record.GetSplitDescription());

            // Persist split description
            TString splitDescr;
            bool serilaizeOk = Self->SrcSplitDescription->SerializeToString(&splitDescr);
            Y_ABORT_UNLESS(serilaizeOk, "Failed to serialize split/merge description");
            db.Table<Schema::Sys>().Key(Schema::Sys_SrcSplitDescription).Update(NIceDb::TUpdate<Schema::Sys::Bytes>(splitDescr));

            Self->PersistSys(db, Schema::Sys_SrcSplitOpId, Self->SrcSplitOpId);

            Self->State = TShardState::SplitSrcWaitForNoTxInFlight;
            Self->PersistSys(db, Schema::Sys_State, Self->State);
            Self->NotifyAllOverloadSubscribers();

            // Wake up immediate ops, so they abort as soon as possible
            for (const auto& kv : Self->Pipeline.GetImmediateOps()) {
                const auto& op = kv.second;
                Self->Pipeline.AddCandidateOp(op);
                Self->PlanQueue.Progress(ctx);
            }

            Self->Pipeline.CleanupWaitingVolatile(ctx, Replies);
        } else {
            // Check that this is the same split request
            Y_ABORT_UNLESS(opId == Self->SrcSplitOpId,
                "Datashard %" PRIu64 " got unexpected split request opId %" PRIu64 " while already executing split request opId %" PRIu64,
                Self->TabletID(), opId, Self->SrcSplitOpId);

            Self->SrcAckSplitTo.insert(Ev->Sender);

            // Already waiting?
            if (Self->State == TShardState::SplitSrcWaitForNoTxInFlight ||
                Self->State == TShardState::SplitSrcMakeSnapshot) {
                SplitAlreadyFinished = false;
                return true;
            } else if (Self->State == TShardState::SplitSrcSendingSnapshot) {
                Y_ABORT_UNLESS(!Self->SplitSrcSnapshotSender.AllAcked(), "State should have changed at the moment when last ack was recevied");
                // Do nothing because we are still waiting for acks from DSTs
            } else {
                Y_ABORT_UNLESS(
                    Self->State == TShardState::SplitSrcWaitForPartitioningChanged ||
                    Self->State == TShardState::PreOffline ||
                    Self->State == TShardState::Offline);

                SplitAlreadyFinished = true;
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        Self->SendCommittedReplies(std::move(Replies));

        if (SplitAlreadyFinished) {
            // Send the Ack
            for (const TActorId& ackTo : Self->SrcAckSplitTo) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ack split to schemeshard " << Self->SrcSplitOpId);
                ctx.Send(ackTo, new TEvDataShard::TEvSplitAck(Self->SrcSplitOpId, Self->TabletID()));
            }
        } else {
            Self->CheckSplitCanStart(ctx);
        }
    }
};

void TDataShard::CheckSplitCanStart(const TActorContext& ctx) {
    if (State == TShardState::SplitSrcWaitForNoTxInFlight) {
        ui64 txInFly = TxInFly() + VolatileTxManager.GetTxInFlight();
        ui64 immediateTxInFly = ImmediateInFly();
        SetCounter(COUNTER_SPLIT_SRC_WAIT_TX_IN_FLY, txInFly);
        SetCounter(COUNTER_SPLIT_SRC_WAIT_IMMEDIATE_TX_IN_FLY, immediateTxInFly);
        if (txInFly == 0 && immediateTxInFly == 0 && !Pipeline.HasWaitingSchemeOps()) {
            Execute(CreateTxStartSplit(), ctx);
        }
    }
}


class TDataShard::TTxStartSplit : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    explicit TTxStartSplit(TDataShard* ds)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
    {}

    TTxType GetTxType() const override { return TXTYPE_START_SPLIT; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (Self->State != TShardState::SplitSrcWaitForNoTxInFlight &&
            Self->State != TShardState::SplitSrcMakeSnapshot) {
            // Already initiated
            return true;
        }

        if (Self->State == TShardState::SplitSrcMakeSnapshot && Self->SplitSnapshotStarted) {
            // Already making snapshot
            return true;
        }

        Y_ABORT_UNLESS(Self->TxInFly() == 0, "Currently split operation shouldn't start while there are in-flight transactions");

        // We need to remove all locks first, making sure persistent uncommitted
        // changes are not borrowed by new shards. Otherwise those will become
        // unaccounted for.
        if (!Self->SysLocksTable().GetLocks().empty()) {
            auto countBefore = Self->SysLocksTable().GetLocks().size();
            TDataShardLocksDb locksDb(*Self, txc);
            TSetupSysLocks guardLocks(*Self, &locksDb);
            for (auto& pr : Self->SysLocksTable().GetLocks()) {
                Self->SysLocksTable().EraseLock(pr.first);
                if (pr.second->IsPersistent()) {
                    // Don't erase more than one persistent lock at a time
                    break;
                }
            }
            Self->SysLocksTable().ApplyLocks();
            auto countAfter = Self->SysLocksTable().GetLocks().size();
            Y_ABORT_UNLESS(countAfter < countBefore, "Expected to erase at least one lock");
            Self->Execute(Self->CreateTxStartSplit(), ctx);
            return true;
        }

        ui64 opId = Self->SrcSplitOpId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " starting snapshot for split OpId " << opId);

        NIceDb::TNiceDb db(txc.DB);

#define VERIFY_TABLE_IS_EMPTY(table, isStrictCheck) \
        { \
            auto rowset = db.Table<Schema::table>().Range().Select(); \
            if (!rowset.IsReady()) \
                return false; \
            TStringStream str; \
            THolder<NScheme::TTypeRegistry> tr; \
            while (!rowset.EndOfSet()) { \
                if (!tr) \
                    tr.Reset(new NScheme::TTypeRegistry()); \
                str << rowset.DbgPrint(*tr) << "\n"; \
                if (!rowset.Next()) \
                    return false; \
            } \
            if (isStrictCheck) { \
                Y_ABORT_UNLESS(str.empty(), #table " table is not empty when starting Split at tablet %" PRIu64 " : \n%s", Self->TabletID(), str.Str().data()); \
            } else if (!str.empty()) { \
                LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, \
                     #table " table is not empty when starting Split at tablet " << Self->TabletID() << " : " << str.Str()); \
            } \
        }

        VERIFY_TABLE_IS_EMPTY(TxMain, true);
        VERIFY_TABLE_IS_EMPTY(TxDetails, true);
        VERIFY_TABLE_IS_EMPTY(PlanQueue, true);
        VERIFY_TABLE_IS_EMPTY(DeadlineQueue, true);

        // InReadSets could contain data for already completed Tx's in case of lost Ack's and retries on readset source tablets
        // Just ignore if for now but we should only persist readsets for known TxId's
        VERIFY_TABLE_IS_EMPTY(InReadSets, false);

        TVector<ui32> tablesToSnapshot(Self->SysTablesToTransferAtSplit,
            Self->SysTablesToTransferAtSplit + Y_ARRAY_SIZE(Self->SysTablesToTransferAtSplit));

        for (const auto& ti : Self->TableInfos) {
            tablesToSnapshot.push_back(ti.second->LocalTid);
            if (ti.second->ShadowTid) {
                tablesToSnapshot.push_back(ti.second->ShadowTid);
            }
        }

        TIntrusivePtr<NTabletFlatExecutor::TTableSnapshotContext> snapContext;
        if (Self->IsMvccEnabled()) {
            snapContext = new TSplitSnapshotContext(opId, std::move(tablesToSnapshot),
                                                    Self->GetSnapshotManager().GetCompleteEdge(),
                                                    Self->GetSnapshotManager().GetIncompleteEdge(),
                                                    Self->GetSnapshotManager().GetImmediateWriteEdge(),
                                                    Self->GetSnapshotManager().GetLowWatermark(),
                                                    Self->GetSnapshotManager().GetPerformedUnprotectedReads());
        } else {
            snapContext = new TSplitSnapshotContext(opId, std::move(tablesToSnapshot));
        }

        txc.Env.MakeSnapshot(snapContext);

        Self->SplitSnapshotStarted = true;
        Self->State = TShardState::SplitSrcMakeSnapshot;
        Self->PersistSys(db, Schema::Sys_State, Self->State);

        for (ui32 i = 0; i < Self->SrcSplitDescription->DestinationRangesSize(); ++i) {
            ui64 dstTablet = Self->SrcSplitDescription->GetDestinationRanges(i).GetTabletID();
            Self->SplitSrcSnapshotSender.AddDst(dstTablet);
        }

        Self->CancelReadIterators(Ydb::StatusIds::OVERLOADED, "Shard splitted", ctx);

        return true;
    }

    void Complete(const TActorContext &) override {
    }
};


NTabletFlatExecutor::ITransaction* TDataShard::CreateTxStartSplit() {
    return new TTxStartSplit(this);
}

class TDataShard::TTxSplitSnapshotComplete : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TIntrusivePtr<TSplitSnapshotContext> SnapContext;
    bool ChangeExchangeSplit;
    THashSet<ui64> ActivationList;
    THashSet<ui64> SplitList;

public:
    TTxSplitSnapshotComplete(TDataShard* ds, TIntrusivePtr<TSplitSnapshotContext> snapContext)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , SnapContext(snapContext)
        , ChangeExchangeSplit(false)
    {}

    TTxType GetTxType() const override { return TXTYPE_SPLIT_SNASHOT_COMPLETE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        ui64 opId = Self->SrcSplitOpId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " snapshot complete for split OpId " << opId);

        Y_ABORT_UNLESS(Self->State == TShardState::SplitSrcMakeSnapshot, "Datashard in unexpected state %s", DatashardStateName(Self->State).data());

        txc.Env.ClearSnapshot(*SnapContext);

        NIceDb::TNiceDb db(txc.DB);

        ui64 sourceOffsetsBytes = 0;
        for (const auto& kv : Self->ReplicatedTables) {
            for (const auto& kvSource : kv.second.SourceById) {
                sourceOffsetsBytes += kvSource.second.StatBytes;
            }
        }

        bool needToReadPages = false;
        ui64 totalSnapshotSize = 0;
        // Build snapshot data of all tables for each destination shard
        for (ui32 i = 0; i < Self->SrcSplitDescription->DestinationRangesSize(); ++i) {
            const auto& dstRangeDescr = Self->SrcSplitDescription->GetDestinationRanges(i);
            const ui64 dstTablet = dstRangeDescr.GetTabletID();

            TAutoPtr<NKikimrTxDataShard::TEvSplitTransferSnapshot> snapshot = new NKikimrTxDataShard::TEvSplitTransferSnapshot;
            snapshot->SetSrcTabletId(Self->TabletID());
            snapshot->SetOperationCookie(opId);

            // Fill user table scheme
            Y_ABORT_UNLESS(Self->TableInfos.size() == 1, "Support for more than 1 user table in a datashard is not implemented here");
            const TUserTable& tableInfo = *Self->TableInfos.begin()->second;
            tableInfo.GetSchema(*snapshot->MutableUserTableScheme());

            for (ui32 localTableId : SnapContext->TablesToSnapshot()) {
                TString snapBody;
                if (localTableId > Schema::MinLocalTid) {
                    // Extract dst range from split/merge description to pass it to BorrowSnapshot
                    TSerializedCellVec fromCells(dstRangeDescr.GetKeyRangeBegin());
                    TSerializedCellVec toCells(dstRangeDescr.GetKeyRangeEnd());

                    auto cellsToRawValues = [&tableInfo] (const TSerializedCellVec& cells) {
                        TVector<TRawTypeValue> rawVals;
                        ui32 ki = 0;
                        for (; ki < cells.GetCells().size(); ++ki) {
                            rawVals.push_back(
                                        cells.GetCells()[ki].IsNull() ?
                                            TRawTypeValue() :
                                            TRawTypeValue(cells.GetCells()[ki].Data(), cells.GetCells()[ki].Size(), tableInfo.KeyColumnTypes[ki])
                                            );
                        }
                        // Extend with NULLs if needed
                        for (; ki < tableInfo.KeyColumnTypes.size(); ++ki) {
                            rawVals.push_back(TRawTypeValue());
                        }
                        return rawVals;
                    };

                    TVector<TRawTypeValue> from = cellsToRawValues(fromCells);
                    TVector<TRawTypeValue> to;
                    if (!toCells.GetCells().empty()) { // special case: empty vec means +INF
                       to = cellsToRawValues(toCells);
                    }

                    // Apply dst range to user table
                    snapBody = Self->Executor()->BorrowSnapshot(localTableId, *SnapContext, from, to, dstTablet);
                } else {
                    // Transfer full contents of system table
                    snapBody = Self->Executor()->BorrowSnapshot(localTableId, *SnapContext, {}, {}, dstTablet);
                }

                if (snapBody.empty()) {
                    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " BorrowSnapshot needs to load pages for table "
                                << localTableId << " for split OpId " << opId);
                    needToReadPages = true;
                } else {
                    totalSnapshotSize += snapBody.size();
                    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " BorrowSnapshot: table "
                                << localTableId << " snapshot size is " << snapBody.size() << " total snapshot size is "
                                << totalSnapshotSize << " for split OpId " << opId);
                }

                if (!needToReadPages) {
                    auto* tableSnapshot = snapshot->AddTableSnapshot();
                    tableSnapshot->SetTableId(localTableId);
                    TString compressedBody = NBlockCodecs::Codec("lz4fast")->Encode(snapBody);
                    tableSnapshot->SetSnapshotData(compressedBody);
                }
            }

            if (!needToReadPages) {
                // Send version at which data is not protected by persistent snapshots
                if (auto minVersion = Self->GetSnapshotManager().GetMinWriteVersion()) {
                    snapshot->SetMinWriteVersionStep(minVersion.Step);
                    snapshot->SetMinWriteVersionTxId(minVersion.TxId);
                }

                if (Self->IsMvccEnabled()) {
                    snapshot->SetMvccLowWatermarkStep(SnapContext->LowWatermark.Step);
                    snapshot->SetMvccLowWatermarkTxId(SnapContext->LowWatermark.TxId);
                    snapshot->SetMvccCompleteEdgeStep(SnapContext->CompleteEdge.Step);
                    snapshot->SetMvccCompleteEdgeTxId(SnapContext->CompleteEdge.TxId);
                    snapshot->SetMvccIncompleteEdgeStep(SnapContext->IncompleteEdge.Step);
                    snapshot->SetMvccIncompleteEdgeTxId(SnapContext->IncompleteEdge.TxId);
                    snapshot->SetMvccImmediateWriteEdgeStep(SnapContext->ImmediateWriteEdge.Step);
                    snapshot->SetMvccImmediateWriteEdgeTxId(SnapContext->ImmediateWriteEdge.TxId);
                    snapshot->SetMvccPerformedUnprotectedReads(SnapContext->PerformedUnprotectedReads);
                }

                // Send info about existing persistent snapshots
                for (const auto& kv : Self->GetSnapshotManager().GetSnapshots()) {
                    if (kv.second.HasFlags(TSnapshot::FlagRemoved)) {
                        // Ignore removed snapshots
                        continue;
                    }
                    auto* proto = snapshot->AddPersistentSnapshots();
                    proto->SetOwnerId(kv.first.OwnerId);
                    proto->SetPathId(kv.first.PathId);
                    proto->SetStep(kv.first.Step);
                    proto->SetTxId(kv.first.TxId);
                    proto->SetName(kv.second.Name);
                    proto->SetFlags(kv.second.Flags);
                    proto->SetTimeoutMs(kv.second.Timeout.MilliSeconds());
                }

                if (tableInfo.HasAsyncIndexes() || tableInfo.HasCdcStreams()) {
                    snapshot->SetWaitForActivation(true);
                    ActivationList.insert(dstTablet);
                    if (tableInfo.HasCdcStreams()) {
                        SplitList.insert(dstTablet);
                    }
                }

                if (sourceOffsetsBytes > 0) {
                    snapshot->SetReplicationSourceOffsetsBytes(sourceOffsetsBytes);
                }

                // Persist snapshot data so that it can be sent if this datashard restarts
                TString snapshotMeta;
                Y_PROTOBUF_SUPPRESS_NODISCARD snapshot->SerializeToString(&snapshotMeta);
                db.Table<Schema::SplitSrcSnapshots>()
                        .Key(dstTablet)
                        .Update(NIceDb::TUpdate<Schema::SplitSrcSnapshots::SnapshotMeta>(snapshotMeta));

                Self->SplitSrcSnapshotSender.SaveSnapshotForSending(dstTablet, snapshot);
            }
        }

        if (needToReadPages) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " BorrowSnapshot is restarting for split OpId " << opId);
            return false;
        } else {
            txc.Env.DropSnapshot(SnapContext);

            for (ui64 dstTabletId : ActivationList) {
                Self->ChangeSenderActivator.AddDst(dstTabletId);
                db.Table<Schema::SrcChangeSenderActivations>().Key(dstTabletId).Update();
            }

            for (ui64 dstTabletId : SplitList) {
                Self->ChangeExchangeSplitter.AddDst(dstTabletId);
            }

            ChangeExchangeSplit = !Self->ChangesQueue && !Self->ChangeExchangeSplitter.Done();

            Self->State = TShardState::SplitSrcSendingSnapshot;
            Self->PersistSys(db, Schema::Sys_State, Self->State);

            return true;
        }
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Sending snapshots from src for split OpId " << Self->SrcSplitOpId);
        Self->SplitSrcSnapshotSender.DoSend(ctx);
        if (ChangeExchangeSplit) {
            Self->KillChangeSender(ctx);
            Self->ChangeExchangeSplitter.DoSplit(ctx);
        }
    }
};


NTabletFlatExecutor::ITransaction* TDataShard::CreateTxSplitSnapshotComplete(TIntrusivePtr<TSplitSnapshotContext> snapContext) {
    return new TTxSplitSnapshotComplete(this, snapContext);
}


class TDataShard::TTxSplitTransferSnapshotAck : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvSplitTransferSnapshotAck::TPtr Ev;
    bool AllDstAcksReceived;
    ui64 ActivateTabletId;

public:
    TTxSplitTransferSnapshotAck(TDataShard* ds, TEvDataShard::TEvSplitTransferSnapshotAck::TPtr& ev)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , Ev(ev)
        , AllDstAcksReceived(false)
        , ActivateTabletId(0)
    {}

    TTxType GetTxType() const override { return TXTYPE_SPLIT_TRANSFER_SNAPSHOT_ACK; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        ui64 opId = Ev->Get()->Record.GetOperationCookie();
        ui64 dstTabletId = Ev->Get()->Record.GetTabletId();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    Self->TabletID() << " Received snapshot Ack from dst " << dstTabletId << " for split OpId " << opId);

        Self->SplitSrcSnapshotSender.AckSnapshot(dstTabletId, ctx);

        if (Self->SplitSrcSnapshotSender.AllAcked()) {
            AllDstAcksReceived = true;
            Self->State = TShardState::SplitSrcWaitForPartitioningChanged;
            Self->PersistSys(db, Schema::Sys_State, Self->State);
        }

        // Remove the row for acked snapshot
        db.Table<Schema::SplitSrcSnapshots>().Key(dstTabletId).Delete();

        if (!Self->ChangesQueue && Self->ChangeExchangeSplitter.Done() && !Self->ChangeSenderActivator.Acked(dstTabletId)) {
            ActivateTabletId = dstTabletId;
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        if (AllDstAcksReceived) {
            for (const TActorId& ackTo : Self->SrcAckSplitTo) {
                ui64 opId = Self->SrcSplitOpId;
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ack split to schemeshard " << opId);
                ctx.Send(ackTo, new TEvDataShard::TEvSplitAck(opId, Self->TabletID()));
            }
        }

        if (ActivateTabletId) {
            Self->ChangeSenderActivator.DoSend(ActivateTabletId, ctx);
        }
    }
};


class TDataShard::TTxSplitPartitioningChanged : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    THashMap<TActorId, THashSet<ui64>> Waiters;

public:
    TTxSplitPartitioningChanged(TDataShard* ds, THashMap<TActorId, THashSet<ui64>>&& waiters)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , Waiters(std::move(waiters))
    {}

    TTxType GetTxType() const override { return TXTYPE_SPLIT_PARTITIONING_CHANGED; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Y_ABORT_UNLESS(!Self->ChangesQueue && Self->ChangeSenderActivator.AllAcked());

        // TODO: At this point Src should start rejecting all new Tx with SchemaChanged status
        if (Self->State != TShardState::SplitSrcWaitForPartitioningChanged) {
            Y_ABORT_UNLESS(Self->State == TShardState::PreOffline || Self->State == TShardState::Offline,
                "Unexpected TEvSplitPartitioningChanged at datashard %" PRIu64 " state %s",
                Self->TabletID(), DatashardStateName(Self->State).data());

            return true;
        }

        Self->DropAllUserTables(txc);

        NIceDb::TNiceDb db(txc.DB);
        Self->State = TShardState::PreOffline;
        Self->PersistSys(db, Schema::Sys_State, Self->State);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        for (const auto& [ackTo, opIds] : Waiters) {
            for (const ui64 opId : opIds) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ack split partitioning changed to schemeshard " << opId);
                ctx.Send(ackTo, new TEvDataShard::TEvSplitPartitioningChangedAck(opId, Self->TabletID()));
            }
        }

        if (!Self->MediatorDelayedReplies.empty()) {
            // We have some pending mediator replies, which must not be replied.
            // Unfortunately we may linger around for a long time, and clients
            // would keep awaiting replies for all that time. We have to make
            // sure those clients receive an appropriate disconnection error
            // instead.
            ctx.Send(Self->SelfId(), new TEvents::TEvPoison);
        }

        // TODO: properly check if there are no loans
        Self->CheckStateChange(ctx);
    }
};

void TDataShard::Handle(TEvDataShard::TEvSplit::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSplit(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvSplitTransferSnapshotAck::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSplitTransferSnapshotAck(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvSplitPartitioningChanged::TPtr& ev, const TActorContext& ctx) {
    const auto opId = ev->Get()->Record.GetOperationCookie();

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Got TEvSplitPartitioningChanged"
        << ": opId: " << opId
        << ", at datashard: " << TabletID()
        << ", state: " << DatashardStateName(State).data());

    SrcAckPartitioningChangedTo[ev->Sender].insert(opId);

    if (ChangesQueue || !ChangeSenderActivator.AllAcked()) {
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " delay partitioning changed ack"
            << ", ChangesQueue size: " << ChangesQueue.size()
            << ", siblings to be activated: " << ChangeSenderActivator.Dump());
    } else {
        Execute(CreateTxSplitPartitioningChanged(std::move(SrcAckPartitioningChangedTo)), ctx);
        SrcAckPartitioningChangedTo.clear(); // to be sure
    }
}

NTabletFlatExecutor::ITransaction* TDataShard::CreateTxSplitPartitioningChanged(THashMap<TActorId, THashSet<ui64>>&& waiters) {
    return new TTxSplitPartitioningChanged(this, std::move(waiters));
}

}}
