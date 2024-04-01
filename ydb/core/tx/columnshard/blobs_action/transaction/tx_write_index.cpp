#include "tx_write_index.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NColumnShard {

bool TTxWriteIndex::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    auto changes = Ev->Get()->IndexChanges;
    TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("external_task_id", changes->GetTaskIdentifier());
    Y_ABORT_UNLESS(Self->InsertTable);
    Y_ABORT_UNLESS(Self->TablesManager.HasPrimaryIndex());
    txc.DB.NoMoreReadsForTx();

    ACFL_DEBUG("event", "TTxWriteIndex::Execute")("change_type", changes->TypeString())("details", changes->DebugString());
    if (Ev->Get()->GetPutStatus() == NKikimrProto::OK) {
        NOlap::TSnapshot snapshot(Self->LastPlannedStep, Self->LastPlannedTxId);
        Y_ABORT_UNLESS(Ev->Get()->IndexInfo->GetLastSchema()->GetSnapshot() <= snapshot);

        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper dbWrap(txc.DB, &dsGroupSelector);
        AFL_VERIFY(Self->TablesManager.MutablePrimaryIndex().ApplyChanges(dbWrap, changes, snapshot));
        LOG_S_DEBUG(TxPrefix() << "(" << changes->TypeString() << ") apply" << TxSuffix());
        NOlap::TWriteIndexContext context(&txc.DB, dbWrap, Self->MutableIndexAs<NOlap::TColumnEngineForLogs>());
        changes->WriteIndexOnExecute(Self, context);

        NOlap::TBlobManagerDb blobManagerDb(txc.DB);
        changes->MutableBlobsAction().OnExecuteTxAfterAction(*Self, blobManagerDb, true);

        Self->UpdateIndexCounters();
    } else {
        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TBlobManagerDb blobsDb(txc.DB);
        changes->MutableBlobsAction().OnExecuteTxAfterAction(*Self, blobsDb, false);
        for (ui32 i = 0; i < changes->GetWritePortionsCount(); ++i) {
            for (auto&& i : changes->GetWritePortionInfo(i)->GetPortionInfo().Records) {
                LOG_S_WARN(TxPrefix() << "(" << changes->TypeString() << ":" << i.BlobRange << ") blob cannot apply changes: " << TxSuffix());
            }
        }
        NOlap::TChangesFinishContext context("cannot write index blobs");
        changes->Abort(*Self, context);
        LOG_S_ERROR(TxPrefix() << " (" << changes->TypeString() << ") cannot write index blobs" << TxSuffix());
    }

    Self->EnqueueProgressTx(ctx);
    return true;
}

void TTxWriteIndex::Complete(const TActorContext& ctx) {
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID()));
    CompleteReady = true;
    auto changes = Ev->Get()->IndexChanges;
    ACFL_DEBUG("event", "TTxWriteIndex::Complete")("change_type", changes->TypeString())("details", changes->DebugString());

    const ui64 blobsWritten = changes->GetBlobsAction().GetWritingBlobsCount();
    const ui64 bytesWritten = changes->GetBlobsAction().GetWritingTotalSize();

    if (!Ev->Get()->IndexChanges->IsAborted()) {
        NOlap::TWriteIndexCompleteContext context(ctx, blobsWritten, bytesWritten, Ev->Get()->Duration, TriggerActivity, Self->MutableIndexAs<NOlap::TColumnEngineForLogs>());
        Ev->Get()->IndexChanges->WriteIndexOnComplete(Self, context);
    }

    Self->EnqueueBackgroundActivities(false, TriggerActivity);
    changes->MutableBlobsAction().OnCompleteTxAfterAction(*Self, Ev->Get()->GetPutStatus() == NKikimrProto::OK);
    NYDBTest::TControllers::GetColumnShardController()->OnWriteIndexComplete(*changes, *Self);
}

TTxWriteIndex::~TTxWriteIndex() {
    if (Ev) {
        auto changes = Ev->Get()->IndexChanges;
        if (!CompleteReady && changes) {
            changes->AbortEmergency();
        }
    }
}

TTxWriteIndex::TTxWriteIndex(TColumnShard* self, TEvPrivate::TEvWriteIndex::TPtr& ev)
    : TBase(self)
    , Ev(ev)
    , TabletTxNo(++Self->TabletTxCounter)
{
    Y_ABORT_UNLESS(Ev && Ev->Get()->IndexChanges);
}

void TTxWriteIndex::Describe(IOutputStream& out) const noexcept {
    out << TypeName(*this);
    if (Ev->Get()->IndexChanges) {
        out << ": " << Ev->Get()->IndexChanges->DebugString();
    }
}

}
