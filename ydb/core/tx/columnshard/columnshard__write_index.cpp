#include "columnshard_impl.h"

#include "blobs_action/transaction/tx_draft.h"
#include "blobs_action/transaction/tx_write_index.h"
#include "columnshard_private_events.h"
#include "engines/changes/abstract/abstract.h"
#include "engines/writer/compacted_blob_constructor.h"

#include <library/cpp/actors/core/log.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx) {
    auto putStatus = ev->Get()->GetPutStatus();

    if (putStatus == NKikimrProto::UNKNOWN) {
        if (IsAnyChannelYellowStop()) {
            ACFL_ERROR("event", "TEvWriteIndex failed")("reason", "channel yellow stop");

            IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->SetPutStatus(NKikimrProto::TRYLATER);
            NOlap::TChangesFinishContext context("out of disk space");
            ev->Get()->IndexChanges->Abort(*this, context);
            ctx.Schedule(FailActivationDelay, new TEvPrivate::TEvPeriodicWakeup(true));
        } else {
            ACFL_DEBUG("event", "TEvWriteIndex")("count", ev->Get()->IndexChanges->GetWritePortionsCount());
            AFL_VERIFY(ev->Get()->IndexChanges->GetWritePortionsCount());

            const bool needDraftTransaction = ev->Get()->IndexChanges->GetBlobsAction().NeedDraftWritingTransaction();
            auto writeController = std::make_shared<NOlap::TCompactedWriteController>(ctx.SelfID, ev->Release());
            if (needDraftTransaction) {
                Execute(new TTxWriteDraft(this, writeController));
            } else {
                ctx.Register(CreateWriteActor(TabletID(), writeController, TInstant::Max()));
            }
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
