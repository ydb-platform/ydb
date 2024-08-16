#include "datashard_txs.h"
#include "datashard_locks_db.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/util/pb.h>


namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

TDataShard::TTxInit::TTxInit(TDataShard* ds)
    : TBase(ds)
{}

bool TDataShard::TTxInit::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "TDataShard::TTxInit::Execute");

    try {
        Self->State = TShardState::Unknown;
        Self->LastLocalTid = Schema::MinLocalTid;
        Self->LastLoanTableTid = 0;
        Self->NextSeqno = 1;
        Self->NextChangeRecordOrder = 1;
        Self->LastChangeRecordGroup = 1;
        Self->TransQueue.Reset();
        Self->SnapshotManager.Reset();
        Self->SchemaSnapshotManager.Reset();
        Self->S3Uploads.Reset();
        Self->S3Downloads.Reset();
        Self->CdcStreamScanManager.Reset();
        Self->CdcStreamHeartbeatManager.Reset();

        Self->KillChangeSender(ctx);
        Self->ChangesQueue.clear();
        Self->LockChangeRecords.clear();
        Self->CommittedLockChangeRecords.clear();
        ChangeRecords.clear();

        bool done = ReadEverything(txc);

        if (done && Self->State != TShardState::Offline) {
            Self->SnapshotManager.Fix_KIKIMR_12289(txc.DB);
            Self->SnapshotManager.Fix_KIKIMR_14259(txc.DB);
            for (const auto& pr : Self->TableInfos) {
                pr.second->Fix_KIKIMR_17222(txc.DB);
            }
        }

        return done;
    } catch (const TNotReadyTabletException &) {
        return false;
    } catch (const TSchemeErrorTabletException &ex) {
        Y_UNUSED(ex);
        Y_ABORT();
    } catch (...) {
        Y_ABORT("there must be no leaked exceptions");
    }
}

void TDataShard::TTxInit::Complete(const TActorContext &ctx) {
    LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "TDataShard::TTxInit::Complete");

    // Start MakeSnapshot() if we started in SplitSrcMakeSnapshot state
    if (Self->State == TShardState::SplitSrcMakeSnapshot) {
        Self->Execute(Self->CreateTxStartSplit(), ctx);
    } else if (Self->State == TShardState::SplitSrcSendingSnapshot) {
        if (!Self->SplitSrcSnapshotSender.AllAcked()) {
            Self->SplitSrcSnapshotSender.DoSend(ctx);
        }
    } else if (Self->State == TShardState::Offline) {
        // Remind the schemeshard that this shard is in Offline state and can be deleted
        Self->ReportState(ctx, Self->State);
    } else if (Self->State == TShardState::Ready) {
        // Make sure schema defaults are updated when changed
        Self->Execute(Self->CreateTxInitSchemaDefaults(), ctx);
    }

    Self->SwitchToWork(ctx);
    Self->SendRegistrationRequestTimeCast(ctx);

    // InReadSets table might have a lot of garbage due to old bug.
    // Run transaction to collect if shard is not going offline.
    if (Self->State != TShardState::Offline && Self->State != TShardState::PreOffline)
        Self->Execute(Self->CreateTxCheckInReadSets(), ctx);

    // Tables created with older SchemeShard versions don't have
    // path filled for user tables. Resolve path for them.
    Self->ResolveTablePath(ctx);

    // Plan cleanup if needed
    if (Self->State == TShardState::Ready ||
        Self->State == TShardState::SplitSrcWaitForNoTxInFlight ||
        Self->State == TShardState::SplitSrcMakeSnapshot)
    {
        // Initialize snapshot expiration queue with current context time
        Self->GetSnapshotManager().InitExpireQueue(ctx.Now());

        if (Self->GetSnapshotManager().HasExpiringSnapshots())
            Self->PlanCleanup(ctx);
    }

    // Find subdomain path id if needed
    if (Self->State == TShardState::Ready) {
        if (Self->SubDomainPathId) {
            Self->StartWatchingSubDomainPathId();
        } else {
            Self->StartFindSubDomainPathId();
        }
    }

    Self->CreateChangeSender(ctx);
    Self->EnqueueChangeRecords(std::move(ChangeRecords));
    Self->MaybeActivateChangeSender(ctx);
    Self->EmitHeartbeats();

    if (!Self->ChangesQueue) {
        if (!Self->ChangeExchangeSplitter.Done()) {
            Self->KillChangeSender(ctx);
            Self->ChangeExchangeSplitter.DoSplit(ctx);
        } else {
            for (const auto dstTabletId : Self->ChangeSenderActivator.GetDstSet()) {
                if (Self->SplitSrcSnapshotSender.Acked(dstTabletId)) {
                    Self->ChangeSenderActivator.DoSend(dstTabletId, ctx);
                }
            }
        }
    }

    // Switch mvcc state if needed
    Self->CheckMvccStateChangeCanStart(ctx);
}

#define LOAD_SYS_UI64(db, row, value) if (!TDataShard::SysGetUi64(db, row, value)) return false;
#define LOAD_SYS_BYTES(db, row, value) if (!TDataShard::SysGetBytes(db, row, value)) return false;
#define LOAD_SYS_BOOL(db, row, value) if (!TDataShard::SysGetBool(db, row, value)) return false;

bool TDataShard::TTxInit::ReadEverything(TTransactionContext &txc) {
    // Note that we should not store any data directly into Self until NoMoreReads() is called
    // But it is ok for initialization, as long as ALL FOLLOWING ACTIONS ARE IDEMPOTENT

    NIceDb::TNiceDb db(txc.DB);

    {
        bool ready = true;

#define PRECHARGE_SYS_TABLE(table) \
        { \
            if (txc.DB.GetScheme().GetTableInfo(table::TableId)) { \
                ready &= db.Table<table>().Precharge(); \
            } \
        }

        PRECHARGE_SYS_TABLE(Schema::Sys);
        PRECHARGE_SYS_TABLE(Schema::UserTables);
        PRECHARGE_SYS_TABLE(Schema::TxMain);
        PRECHARGE_SYS_TABLE(Schema::OutReadSets);
        PRECHARGE_SYS_TABLE(Schema::PlanQueue);
        PRECHARGE_SYS_TABLE(Schema::DeadlineQueue);
        PRECHARGE_SYS_TABLE(Schema::SchemaOperations);
        PRECHARGE_SYS_TABLE(Schema::SplitSrcSnapshots);
        PRECHARGE_SYS_TABLE(Schema::SplitDstReceivedSnapshots);
        PRECHARGE_SYS_TABLE(Schema::Snapshots);
        PRECHARGE_SYS_TABLE(Schema::S3Uploads);
        PRECHARGE_SYS_TABLE(Schema::S3UploadedParts);
        PRECHARGE_SYS_TABLE(Schema::S3Downloads);
        PRECHARGE_SYS_TABLE(Schema::ChangeRecords);
        PRECHARGE_SYS_TABLE(Schema::SrcChangeSenderActivations);
        PRECHARGE_SYS_TABLE(Schema::DstChangeSenderActivations);
        PRECHARGE_SYS_TABLE(Schema::ReplicationSources);
        PRECHARGE_SYS_TABLE(Schema::ReplicationSourceOffsets);
        PRECHARGE_SYS_TABLE(Schema::DstReplicationSourceOffsetsReceived);
        PRECHARGE_SYS_TABLE(Schema::UserTablesStats);
        PRECHARGE_SYS_TABLE(Schema::SchemaSnapshots);
        PRECHARGE_SYS_TABLE(Schema::Locks);
        PRECHARGE_SYS_TABLE(Schema::LockRanges);
        PRECHARGE_SYS_TABLE(Schema::LockConflicts);
        PRECHARGE_SYS_TABLE(Schema::TxVolatileDetails);
        PRECHARGE_SYS_TABLE(Schema::TxVolatileParticipants);
        PRECHARGE_SYS_TABLE(Schema::CdcStreamScans);
        PRECHARGE_SYS_TABLE(Schema::CdcStreamHeartbeats);

        if (!ready)
            return false;

#undef PRECHARGE_SYS_TABLE
    }

    // Reads from Sys table
    LOAD_SYS_UI64(db, Schema::Sys_State, Self->State);
    LOAD_SYS_UI64(db, Schema::Sys_LastLocalTid, Self->LastLocalTid);
    LOAD_SYS_UI64(db, Schema::Sys_LastLoanTableTid, Self->LastLoanTableTid);
    LOAD_SYS_UI64(db, Schema::Sys_NextSeqno, Self->NextSeqno);
    LOAD_SYS_UI64(db, Schema::Sys_NextChangeRecordOrder, Self->NextChangeRecordOrder);
    LOAD_SYS_UI64(db, Schema::Sys_LastChangeRecordGroup, Self->LastChangeRecordGroup);
    LOAD_SYS_UI64(db, Schema::Sys_TxReadSizeLimit, Self->TxReadSizeLimit);
    LOAD_SYS_UI64(db, Schema::Sys_PathOwnerId, Self->PathOwnerId);
    LOAD_SYS_UI64(db, Schema::Sys_CurrentSchemeShardId, Self->CurrentSchemeShardId);
    LOAD_SYS_UI64(db, Schema::Sys_LastSchemeShardGeneration, Self->LastSchemeOpSeqNo.Generation);
    LOAD_SYS_UI64(db, Schema::Sys_LastSchemeShardRound, Self->LastSchemeOpSeqNo.Round);
    LOAD_SYS_UI64(db, Schema::Sys_StatisticsDisabled, Self->StatisticsDisabled);

    ui64 subDomainOwnerId = 0;
    ui64 subDomainLocalPathId = 0;
    LOAD_SYS_UI64(db, Schema::Sys_SubDomainOwnerId, subDomainOwnerId);
    LOAD_SYS_UI64(db, Schema::Sys_SubDomainLocalPathId, subDomainLocalPathId);
    if (subDomainOwnerId && subDomainLocalPathId && subDomainOwnerId == Self->CurrentSchemeShardId) {
        // We only recognize subdomain path id when it's non-zero and matches
        // current schemeshard id. This protects against migrations that used
        // the older code version that did not support updating this path id.
        Self->SubDomainPathId.emplace(subDomainOwnerId, subDomainLocalPathId);
    }
    LOAD_SYS_BOOL(db, Schema::Sys_SubDomainOutOfSpace, Self->SubDomainOutOfSpace);

    {
        TString rawProcessingParams;
        LOAD_SYS_BYTES(db, Schema::Sys_SubDomainInfo, rawProcessingParams);
        if (!rawProcessingParams.empty()) {
            Self->ProcessingParams.reset(new NKikimrSubDomains::TProcessingParams());
            Y_ABORT_UNLESS(Self->ProcessingParams->ParseFromString(rawProcessingParams));
        }
    }

    if (!Self->Pipeline.Load(db))
        return false;

    { // Reads user tables metadata
        Self->TableInfos.clear(); // For idempotency
        auto rowset = db.Table<Schema::UserTables>().GreaterOrEqual(0).Select();  // TODO[serxa]: this should be Range() but it is not working right now
        if (!rowset.IsReady())
            return false;
        while (!rowset.EndOfSet()) {
            ui64 tableId = rowset.GetValue<Schema::UserTables::Tid>();
            ui32 localTid = rowset.GetValue<Schema::UserTables::LocalTid>();
            ui32 shadowTid = rowset.GetValueOrDefault<Schema::UserTables::ShadowTid>();
            TString schema = rowset.GetValue<Schema::UserTables::Schema>();
            NKikimrSchemeOp::TTableDescription descr;
            bool parseOk = ParseFromStringNoSizeLimit(descr, schema);
            Y_ABORT_UNLESS(parseOk);
            Self->AddUserTable(TPathId(Self->GetPathOwnerId(), tableId), new TUserTable(localTid, descr, shadowTid));
            if (!rowset.Next())
                return false;
        }
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::UserTablesStats::TableId)) {
        // Reads user tables persistent stats
        auto rowset = db.Table<Schema::UserTablesStats>().GreaterOrEqual(0).Select();
        if (!rowset.IsReady())
            return false;
        while (!rowset.EndOfSet()) {
            ui64 tableId = rowset.GetValue<Schema::UserTablesStats::Tid>();
            ui64 ts = rowset.GetValueOrDefault<Schema::UserTablesStats::FullCompactionTs>();
            if (ts != 0) {
                auto it = Self->TableInfos.find(tableId);
                if (it != Self->TableInfos.end()) {
                    it->second->Stats.LastFullCompaction = TInstant::Seconds(ts);
                }
            }
            if (!rowset.Next())
                return false;
        }
    }

    { // Read split snapshots on src tablet
        auto rowset = db.Table<Schema::SplitSrcSnapshots>().GreaterOrEqual(0).Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            ui64 dstTablet = rowset.GetValue<Schema::SplitSrcSnapshots::DstTabletId>();
            TString snapBody = rowset.GetValue<Schema::SplitSrcSnapshots::SnapshotMeta>();

            TAutoPtr<NKikimrTxDataShard::TEvSplitTransferSnapshot> snapshot = new NKikimrTxDataShard::TEvSplitTransferSnapshot;
            bool parseOk = ParseFromStringNoSizeLimit(*snapshot, snapBody);
            Y_ABORT_UNLESS(parseOk);
            Self->SplitSrcSnapshotSender.AddDst(dstTablet);
            Self->SplitSrcSnapshotSender.SaveSnapshotForSending(dstTablet, snapshot);

            if (!rowset.Next())
                return false;
        }
        Self->SplitSnapshotStarted = false;
    }

    if (db.HaveTable<Schema::SrcChangeSenderActivations>()) {
        // Read change sender activations on src tablet
        auto rowset = db.Table<Schema::SrcChangeSenderActivations>().GreaterOrEqual(0).Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            ui64 dstTabletId = rowset.GetValue<Schema::SrcChangeSenderActivations::DstTabletId>();
            Self->ChangeSenderActivator.AddDst(dstTabletId);

            if (!rowset.Next())
                return false;
        }
    }

    // Split/Merge description on DST
    LOAD_SYS_UI64(db, Schema::Sys_DstSplitOpId, Self->DstSplitOpId);
    {
        TString splitDescr;
        LOAD_SYS_BYTES(db, Schema::Sys_DstSplitDescription, splitDescr);
        if (!splitDescr.empty()) {
            Self->DstSplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>();
            bool parseOk = ParseFromStringNoSizeLimit(*Self->DstSplitDescription, splitDescr);
            Y_ABORT_UNLESS(parseOk);
        }

        LOAD_SYS_BOOL(db, Schema::Sys_DstSplitSchemaInitialized, Self->DstSplitSchemaInitialized);

        // Add all SRC datashards to the list
        Self->ReceiveSnapshotsFrom.clear();
        Self->ReceiveActivationsFrom.clear();

        if (Self->DstSplitDescription) {
            for (ui32 i = 0; i < Self->DstSplitDescription->SourceRangesSize(); ++i) {
                ui64 srcTabletId = Self->DstSplitDescription->GetSourceRanges(i).GetTabletID();
                Self->ReceiveSnapshotsFrom.insert(srcTabletId);
            }
        }

        {
            auto rowset = db.Table<Schema::SplitDstReceivedSnapshots>().GreaterOrEqual(0).Select();
            if (!rowset.IsReady())
                return false;

            // Exclude SRC datashards from which the snapshots have already been received
            while (!rowset.EndOfSet()) {
                ui64 srcTabletId = rowset.GetValue<Schema::SplitDstReceivedSnapshots::SrcTabletId>();
                Self->ReceiveSnapshotsFrom.erase(srcTabletId);

                if (!rowset.Next())
                    return false;
            }
        }

        if (db.HaveTable<Schema::DstChangeSenderActivations>()) {
            auto rowset = db.Table<Schema::DstChangeSenderActivations>().GreaterOrEqual(0).Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                ui64 srcTabletId = rowset.GetValue<Schema::DstChangeSenderActivations::SrcTabletId>();
                Self->ReceiveActivationsFrom.insert(srcTabletId);

                if (!rowset.Next())
                    return false;
            }
        }
    }

    // Split/Merge description on SRC
    LOAD_SYS_UI64(db, Schema::Sys_SrcSplitOpId, Self->SrcSplitOpId);
    {
        TString splitDescr;
        LOAD_SYS_BYTES(db, Schema::Sys_SrcSplitDescription, splitDescr);
        if (!splitDescr.empty()) {
            Self->SrcSplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>();
            bool parseOk = ParseFromStringNoSizeLimit(*Self->SrcSplitDescription, splitDescr);
            Y_ABORT_UNLESS(parseOk);

            switch (Self->State) {
            case TShardState::SplitSrcWaitForNoTxInFlight:
            case TShardState::SplitSrcMakeSnapshot:
                // split just started, there might be in-flight transactions
                break;
            default:
                for (ui32 i = 0; i < Self->SrcSplitDescription->DestinationRangesSize(); ++i) {
                    ui64 dstTablet = Self->SrcSplitDescription->GetDestinationRanges(i).GetTabletID();
                    Self->ChangeExchangeSplitter.AddDst(dstTablet);
                }
                break;
            }
        }
    }

    Y_ABORT_UNLESS(Self->State != TShardState::Unknown);

    Y_ABORT_UNLESS(Self->SplitSrcSnapshotSender.AllAcked() || Self->State == TShardState::SplitSrcSendingSnapshot,
             "Unexpected state %s while having unsent split snapshots at datashard %" PRIu64,
             DatashardStateName(Self->State).data(), Self->TabletID());

    Y_ABORT_UNLESS(Self->ReceiveSnapshotsFrom.empty() || Self->State == TShardState::SplitDstReceivingSnapshot,
             "Unexpected state %s while having non-received split snapshots at datashard %" PRIu64,
             DatashardStateName(Self->State).data(), Self->TabletID());

    // Load unsent ReadSets
    if (!Self->OutReadSets.LoadReadSets(db))
        return false;

    // TODO: properly check shard state
    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::TxMain::TableId)) {
        if (!Self->TransQueue.Load(db))
            return false;

        for (auto &pr : Self->TransQueue.GetTxsInFly()) {
            pr.second->BuildExecutionPlan(true);
            if (!pr.second->IsExecutionPlanFinished())
                Self->Pipeline.GetExecutionUnit(pr.second->GetCurrentUnit()).AddOperation(pr.second);
        }

        if (Self->TransQueue.GetPlan().size())
            Self->Pipeline.AddCandidateUnit(EExecutionUnitKind::PlanQueue);
        // TODO: add propose blockers to blockers list
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::Snapshots::TableId)) {
        if (!Self->SnapshotManager.Reload(db))
            return false;
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::S3Uploads::TableId)) {
        if (!Self->S3Uploads.Load(db))
            return false;
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::S3Downloads::TableId)) {
        if (!Self->S3Downloads.Load(db))
            return false;
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::SchemaSnapshots::TableId)) {
        if (!Self->SchemaSnapshotManager.Load(db)) {
            return false;
        }
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::ChangeRecords::TableId)) {
        if (!Self->LoadChangeRecords(db, ChangeRecords)) {
            return false;
        }
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::LockChangeRecords::TableId)) {
        if (!Self->LoadLockChangeRecords(db)) {
            return false;
        }
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::ChangeRecordCommits::TableId)) {
        if (!Self->LoadChangeRecordCommits(db, ChangeRecords)) {
            return false;
        }
    }

    Self->ReplicatedTables.clear();
    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::ReplicationSources::TableId)) {
        auto rowset = db.Table<Schema::ReplicationSources>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TPathId pathId;
            pathId.OwnerId = rowset.GetValue<Schema::ReplicationSources::PathOwnerId>();
            pathId.LocalPathId = rowset.GetValue<Schema::ReplicationSources::TablePathId>();
            ui64 sourceId = rowset.GetValue<Schema::ReplicationSources::SourceId>();
            TString sourceName = rowset.GetValue<Schema::ReplicationSources::SourceName>();

            if (auto* table = Self->EnsureReplicatedTable(pathId)) {
                table->LoadSource(sourceId, sourceName);
            }

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::ReplicationSourceOffsets::TableId)) {
        auto rowset = db.Table<Schema::ReplicationSourceOffsets>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TPathId pathId;
            pathId.OwnerId = rowset.GetValue<Schema::ReplicationSourceOffsets::PathOwnerId>();
            pathId.LocalPathId = rowset.GetValue<Schema::ReplicationSourceOffsets::TablePathId>();
            ui64 sourceId = rowset.GetValue<Schema::ReplicationSourceOffsets::SourceId>();

            if (auto* table = Self->FindReplicatedTable(pathId)) {
                if (auto* source = table->FindSource(sourceId)) {
                    ui64 splitKeyId = rowset.GetValue<Schema::ReplicationSourceOffsets::SplitKeyId>();
                    TSerializedCellVec splitKey = TSerializedCellVec(rowset.GetValue<Schema::ReplicationSourceOffsets::SplitKey>());
                    i64 maxOffset = rowset.GetValue<Schema::ReplicationSourceOffsets::MaxOffset>();
                    source->LoadSplitKey(splitKeyId, std::move(splitKey), maxOffset);
                }
            }

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    Self->ReceiveReplicationSourceOffsetsFrom.clear();
    if (Self->State == TShardState::SplitDstReceivingSnapshot && txc.DB.GetScheme().GetTableInfo(Schema::DstReplicationSourceOffsetsReceived::TableId)) {
        auto rowset = db.Table<Schema::DstReplicationSourceOffsetsReceived>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TPathId pathId;
            ui64 srcTabletId = rowset.GetValue<Schema::DstReplicationSourceOffsetsReceived::SrcTabletId>();
            pathId.OwnerId = rowset.GetValue<Schema::DstReplicationSourceOffsetsReceived::PathOwnerId>();
            pathId.LocalPathId = rowset.GetValue<Schema::DstReplicationSourceOffsetsReceived::TablePathId>();

            Self->ReceiveReplicationSourceOffsetsFrom[srcTabletId].Received.insert(pathId);

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::Locks::TableId)) {
        TDataShardLocksDb locksDb(*Self, txc);
        if (!Self->SysLocks.Load(locksDb)) {
            return false;
        }
    }

    if (Self->State != TShardState::Offline) {
        if (!Self->VolatileTxManager.Load(db)) {
            return false;
        }
        Self->OutReadSets.HoldArbiterReadSets();
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::CdcStreamScans::TableId)) {
        if (!Self->CdcStreamScanManager.Load(db)) {
            return false;
        }
    }

    if (Self->State != TShardState::Offline && txc.DB.GetScheme().GetTableInfo(Schema::CdcStreamHeartbeats::TableId)) {
        if (!Self->CdcStreamHeartbeatManager.Load(db)) {
            return false;
        }
    }

    Self->SubscribeNewLocks();

    Self->ScheduleRemoveAbandonedLockChanges();
    Self->ScheduleRemoveAbandonedSchemaSnapshots();

    return true;
}

/// Creates and updates schema at tablet boot time
class TDataShard::TTxInitSchema : public TTransactionBase<TDataShard> {
public:
    TTxInitSchema(TDataShard* self)
        : TBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        Y_UNUSED(txc);
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "TxInitSchema.Execute");

        NIceDb::TNiceDb db(txc.DB);

        bool isCreate = txc.DB.GetScheme().IsEmpty();

        if (isCreate) {
            Self->State = TShardState::WaitScheme;
        } else {
            LOAD_SYS_UI64(db, Schema::Sys_State, Self->State);
        }

        // Skip full schema migration (and dropped system table recreation)
        // if the datashard is in the process of drop.
        if (Self->State == TShardState::PreOffline || Self->State == TShardState::Offline) {
            db.MaterializeExisting<Schema>();
            Schema::SchemaTables<Schema::SchemaOperations>::Materialize(txc.DB, NIceDb::EMaterializationMode::NonExisting);
            Schema::SchemaTables<Schema::ScanProgress>::Materialize(txc.DB, NIceDb::EMaterializationMode::NonExisting);
        } else {
            db.Materialize<Schema>();
        }

        if (isCreate) {
            txc.DB.Alter().SetExecutorAllowLogBatching(gAllowLogBatchingDefaultValue);
            txc.DB.Alter().SetExecutorLogFlushPeriod(TDuration::MicroSeconds(500));

            Self->PersistSys(db, Schema::Sys_State, Self->State);

            auto state = EMvccState::MvccEnabled;
            Self->PersistSys(db, Schema::SysMvcc_State, (ui32)state);

            LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, TStringBuilder() << "TxInitSchema.Execute"
                << " MVCC state switched to  enabled state");

            Self->MvccSwitchState = TSwitchState::DONE;
        }

        //remove this code after all datashards upgrade Sys_SubDomainInfo row in Sys
        if (Self->State == TShardState::Ready ||
            Self->State == TShardState::SplitSrcWaitForNoTxInFlight) {
            TString rawProcessingParams;
            LOAD_SYS_BYTES(db, Schema::Sys_SubDomainInfo, rawProcessingParams)

            if (rawProcessingParams.empty()) {
                auto appdata = AppData(ctx);
                const ui32 selfDomain = appdata->DomainsInfo->GetDomainUidByTabletId(Self->TabletID());
                Y_ABORT_UNLESS(selfDomain != appdata->DomainsInfo->BadDomainId);
                const auto& domain = appdata->DomainsInfo->GetDomain(selfDomain);

                NKikimrSubDomains::TProcessingParams params = ExtractProcessingParams(domain);
                LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "TxInitSchema.Execute Persist Sys_SubDomainInfo");
                Self->PersistSys(db, Schema::Sys_SubDomainInfo, params.SerializeAsString());
            }
        }

        if (!isCreate) {
            ui64 currentSchemeShardId = INVALID_TABLET_ID;
            LOAD_SYS_UI64(db, Schema::Sys_CurrentSchemeShardId, currentSchemeShardId)

            ui64 pathOwnerId = INVALID_TABLET_ID;
            LOAD_SYS_UI64(db, Schema::Sys_PathOwnerId, pathOwnerId)

            if (pathOwnerId == INVALID_TABLET_ID && currentSchemeShardId != INVALID_TABLET_ID) {
                pathOwnerId = currentSchemeShardId;
                LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "TxInitSchema.Execute Persist Sys_PathOwnerId");
                Self->PersistSys(db, TDataShard::Schema::Sys_PathOwnerId, pathOwnerId);
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "TxInitSchema.Complete");
        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

/// Initializes schema defaults changes
class TDataShard::TTxInitSchemaDefaults : public TTransactionBase<TDataShard> {
public:
    TTxInitSchemaDefaults(TDataShard* self)
        : TBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA_DEFAULTS; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "TxInitSchemaDefaults.Execute");

        if (Self->State == TShardState::Ready) {
            for (const auto& pr : Self->TableInfos) {
                pr.second->ApplyDefaults(txc);
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "TxInitSchemaDefaults.Complete");
    }
};

ITransaction* TDataShard::CreateTxInit() {
    return new TTxInit(this);
}

ITransaction* TDataShard::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

ITransaction* TDataShard::CreateTxInitSchemaDefaults() {
    return new TTxInitSchemaDefaults(this);
}

bool TDataShard::SyncSchemeOnFollower(TTransactionContext &txc, const TActorContext &ctx,
                                          NKikimrTxDataShard::TError::EKind & status, TString& errMessage)
{
    status = NKikimrTxDataShard::TError::OK;
    errMessage.clear();

    const auto& scheme = txc.DB.GetScheme();

    // Check that TxInit from leader has been already replicated to the follower
    // and all internal tables have already been created
    bool isInitialized = scheme.GetTableInfo(Schema::Sys::TableId);
    if (!isInitialized) {
        status = NKikimrTxDataShard::TError::WRONG_SHARD_STATE;
        errMessage = Sprintf("Follower has not been initialized yet: tablet id: %" PRIu64, TabletID());
        return true;
    }

    auto* userTablesSchema = scheme.GetTableInfo(Schema::UserTables::TableId);
    Y_ABORT_UNLESS(userTablesSchema, "UserTables");

    // Check if tables changed since last time we synchronized them
    NTable::TDatabase::TChangeCounter lastSysUpdate = txc.DB.Head(Schema::Sys::TableId);
    NTable::TDatabase::TChangeCounter lastSchemeUpdate = txc.DB.Head(Schema::UserTables::TableId);
    NTable::TDatabase::TChangeCounter lastSnapshotsUpdate;
    if (scheme.GetTableInfo(Schema::Snapshots::TableId)) {
        lastSnapshotsUpdate = txc.DB.Head(Schema::Snapshots::TableId);
    }

    NIceDb::TNiceDb db(txc.DB);

    bool precharged = true;
    bool updated = false;
    if (FollowerState.LastSysUpdate < lastSysUpdate) {
        if (!db.Table<Schema::Sys>().Precharge()) {
            precharged = false;
        }
        updated = true;
    }
    if (FollowerState.LastSchemeUpdate < lastSchemeUpdate) {
        if (!db.Table<Schema::UserTables>().Precharge()) {
            precharged = false;
        }
        updated = true;
    }
    if (FollowerState.LastSnapshotsUpdate < lastSnapshotsUpdate) {
        if (!db.Table<Schema::Snapshots>().Precharge()) {
            precharged = false;
        }
        updated = true;
    }

    if (!updated) {
        return true;
    }

    if (!precharged) {
        return false;
    }

    if (FollowerState.LastSysUpdate < lastSysUpdate) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Updating sys metadata on follower, tabletId " << TabletID()
                << " prev " << FollowerState.LastSysUpdate
                << " current " << lastSysUpdate);

        bool ready = true;
        ready &= SysGetUi64(db, Schema::Sys_PathOwnerId, PathOwnerId);
        ready &= SysGetUi64(db, Schema::Sys_CurrentSchemeShardId, CurrentSchemeShardId);
        ready &= SnapshotManager.ReloadSys(db);
        if (!ready) {
            return false;
        }

        FollowerState.LastSysUpdate = lastSysUpdate;
    }

    if (FollowerState.LastSchemeUpdate < lastSchemeUpdate) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Updating tables metadata on follower, tabletId " << TabletID()
                << " prev " << FollowerState.LastSchemeUpdate
                << " current " << lastSchemeUpdate);

        struct TRow {
            TPathId TableId;
            TUserTable::TPtr Table;
        };

        std::vector<TRow> tables;

        if (userTablesSchema->Columns.contains(Schema::UserTables::ShadowTid::ColumnId)) {
            // New schema with ShadowTid column
            auto rowset = db.Table<Schema::UserTables>().Select<
                Schema::UserTables::Tid,
                Schema::UserTables::LocalTid,
                Schema::UserTables::Schema,
                Schema::UserTables::ShadowTid>();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                ui64 tableId = rowset.GetValue<Schema::UserTables::Tid>();
                ui32 localTid = rowset.GetValue<Schema::UserTables::LocalTid>();
                ui32 shadowTid = rowset.GetValueOrDefault<Schema::UserTables::ShadowTid>();
                TString schema = rowset.GetValue<Schema::UserTables::Schema>();
                NKikimrSchemeOp::TTableDescription descr;
                bool parseOk = ParseFromStringNoSizeLimit(descr, schema);
                Y_ABORT_UNLESS(parseOk);
                tables.push_back(TRow{
                    TPathId(GetPathOwnerId(), tableId),
                    new TUserTable(localTid, descr, shadowTid),
                });
                if (!rowset.Next())
                    return false;
            }
        } else {
            // Older schema without ShadowTid column
            auto rowset = db.Table<Schema::UserTables>().Select<
                Schema::UserTables::Tid,
                Schema::UserTables::LocalTid,
                Schema::UserTables::Schema>();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                ui64 tableId = rowset.GetValue<Schema::UserTables::Tid>();
                ui32 localTid = rowset.GetValue<Schema::UserTables::LocalTid>();
                ui32 shadowTid = 0;
                TString schema = rowset.GetValue<Schema::UserTables::Schema>();
                NKikimrSchemeOp::TTableDescription descr;
                bool parseOk = ParseFromStringNoSizeLimit(descr, schema);
                Y_ABORT_UNLESS(parseOk);
                tables.push_back(TRow{
                    TPathId(GetPathOwnerId(), tableId),
                    new TUserTable(localTid, descr, shadowTid),
                });
                if (!rowset.Next())
                    return false;
            }
        }

        TableInfos.clear();
        for (auto& table : tables) {
            AddUserTable(table.TableId, std::move(table.Table));
        }

        FollowerState.LastSchemeUpdate = lastSchemeUpdate;
    }

    // N.B. follower with snapshots support may be loaded in datashard without a snapshots table
    if (FollowerState.LastSnapshotsUpdate < lastSnapshotsUpdate) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Updating snapshots metadata on follower, tabletId " << TabletID()
                << " prev " << FollowerState.LastSnapshotsUpdate
                << " current " << lastSnapshotsUpdate);

        NIceDb::TNiceDb db(txc.DB);
        if (!SnapshotManager.ReloadSnapshots(db)) {
            return false;
        }

        FollowerState.LastSnapshotsUpdate = lastSnapshotsUpdate;
    }

    return true;
}

}}
