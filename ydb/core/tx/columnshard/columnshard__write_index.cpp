#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"
#include "blob_cache.h"

#include <ydb/core/tx/columnshard/engines/writer/compacted_blob_constructor.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

/// Common transaction for WriteIndex and GranuleCompaction.
/// For WriteIndex it writes new portion from InsertTable into index.
/// For GranuleCompaction it writes new portion of indexed data and mark old data with "switching" snapshot.
class TTxWriteIndex : public TTransactionBase<TColumnShard> {
public:
    TTxWriteIndex(TColumnShard* self, TEvPrivate::TEvWriteIndex::TPtr& ev)
        : TBase(self)
        , Ev(ev)
        , TabletTxNo(++Self->TabletTxCounter)
    {
        Y_VERIFY(Ev && Ev->Get()->IndexChanges);
    }

    ~TTxWriteIndex() {
        if (Ev) {
            auto changes = Ev->Get()->IndexChanges;
            if (!CompleteReady && changes) {
                changes->AbortEmergency();
            }
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE_INDEX; }

private:

    TEvPrivate::TEvWriteIndex::TPtr Ev;
    const ui32 TabletTxNo;
    TBackgroundActivity TriggerActivity = TBackgroundActivity::All();
    bool ApplySuccess = false;
    bool CompleteReady = false;

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxWriteIndex[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
};


bool TTxWriteIndex::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID()));
    Y_VERIFY(Self->InsertTable);
    Y_VERIFY(Self->TablesManager.HasPrimaryIndex());

    txc.DB.NoMoreReadsForTx();

    auto changes = Ev->Get()->IndexChanges;
    LOG_S_DEBUG(TxPrefix() << "execute(" << changes->TypeString() << ") changes: " << *changes << TxSuffix());
    if (Ev->Get()->GetPutStatus() == NKikimrProto::OK) {
        NOlap::TSnapshot snapshot(Self->LastPlannedStep, Self->LastPlannedTxId);
        Y_VERIFY(Ev->Get()->IndexInfo.GetLastSchema()->GetSnapshot() <= snapshot);

        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper dbWrap(txc.DB, &dsGroupSelector);
        ApplySuccess = Self->TablesManager.MutablePrimaryIndex().ApplyChanges(dbWrap, changes, snapshot);
        if (ApplySuccess) {
            LOG_S_DEBUG(TxPrefix() << "(" << changes->TypeString() << ") apply" << TxSuffix());
            NOlap::TWriteIndexContext context(txc, dbWrap);
            changes->WriteIndex(*Self, context);

            if (Ev->Get()->PutResult->GetBlobBatch().GetBlobCount()) {
                Self->BlobManager->SaveBlobBatch(std::move(Ev->Get()->PutResult->ReleaseBlobBatch()), *context.BlobManagerDb);
            }

            Self->UpdateIndexCounters();
        } else {
            NOlap::TChangesFinishContext context("cannot apply changes");
            changes->Abort(*Self, context);
            LOG_S_NOTICE(TxPrefix() << "(" << changes->TypeString() << ") cannot apply changes: "
                << *changes << TxSuffix());
        }
    } else {
        NOlap::TChangesFinishContext context("cannot write index blobs");
        changes->Abort(*Self, context);
        LOG_S_ERROR(TxPrefix() << " (" << changes->TypeString() << ") cannot write index blobs" << TxSuffix());
    }

    Self->EnqueueProgressTx(ctx);
    return true;
}

void TTxWriteIndex::Complete(const TActorContext& ctx) {
    CompleteReady = true;
    LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());

    const ui64 blobsWritten = Ev->Get()->PutResult->GetBlobBatch().GetBlobCount();
    const ui64 bytesWritten = Ev->Get()->PutResult->GetBlobBatch().GetTotalSize();

    if (!Ev->Get()->IndexChanges->IsAborted()) {
        NOlap::TWriteIndexCompleteContext context(ctx, blobsWritten, bytesWritten, Ev->Get()->Duration, TriggerActivity);
        Ev->Get()->IndexChanges->WriteIndexComplete(*Self, context);
    }

    if (Ev->Get()->GetPutStatus() == NKikimrProto::TRYLATER) {
        ctx.Schedule(Self->FailActivationDelay, new TEvPrivate::TEvPeriodicWakeup(true));
    } else {
        Self->EnqueueBackgroundActivities(false, TriggerActivity);
    }

    Self->UpdateResourceMetrics(ctx, Ev->Get()->PutResult->GetResourceUsage());
}

void TColumnShard::Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx) {
    auto putStatus = ev->Get()->GetPutStatus();

    if (putStatus == NKikimrProto::UNKNOWN) {
        if (IsAnyChannelYellowStop()) {
            LOG_S_ERROR("WriteIndex (out of disk space) at tablet " << TabletID());

            IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->SetPutStatus(NKikimrProto::TRYLATER);
            NOlap::TChangesFinishContext context("out of disk space");
            ev->Get()->IndexChanges->Abort(*this, context);
            ctx.Schedule(FailActivationDelay, new TEvPrivate::TEvPeriodicWakeup(true));
        } else {
            auto& blobs = ev->Get()->Blobs;
            LOG_S_DEBUG("WriteIndex (" << blobs.size() << " blobs) at tablet " << TabletID());

            Y_VERIFY(!blobs.empty());
            auto writeController = std::make_shared<NOlap::TCompactedWriteController>(ctx.SelfID, ev->Release(),  Settings.BlobWriteGrouppingEnabled);
            ctx.Register(CreateWriteActor(TabletID(), writeController, BlobManager->StartBlobBatch(), TInstant::Max(), Settings.MaxSmallBlobSize));
        }
    } else {
        if (putStatus == NKikimrProto::OK) {
            LOG_S_DEBUG("WriteIndex at tablet " << TabletID());
        } else {
            LOG_S_INFO("WriteIndex error at tablet " << TabletID());
        }

        OnYellowChannels(*ev->Get()->PutResult);
        Execute(new TTxWriteIndex(this, ev), ctx);
    }
}

}
