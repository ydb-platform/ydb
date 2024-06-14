#include "columnshard_impl.h"

#include "blobs_action/transaction/tx_draft.h"
#include "blobs_action/transaction/tx_write_index.h"
#include "columnshard_private_events.h"
#include "engines/changes/abstract/abstract.h"
#include "engines/writer/compacted_blob_constructor.h"

#include <ydb/core/tx/limiter/usage/abstract.h>
#include <ydb/core/tx/limiter/usage/service.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

class TDiskResourcesRequest: public NLimiter::IResourceRequest {
private:
    using TBase = NLimiter::IResourceRequest;
    std::shared_ptr<NOlap::TCompactedWriteController> WriteController;
    const ui64 TabletId;

private:
    virtual void DoOnResourceAllocated() override {
        NActors::TActivationContext::AsActorContext().Register(CreateWriteActor(TabletId, WriteController, TInstant::Max()));
    }

public:
    TDiskResourcesRequest(const std::shared_ptr<NOlap::TCompactedWriteController>& writeController, const ui64 tabletId)
        : TBase(writeController->GetWriteVolume())
        , WriteController(writeController)
        , TabletId(tabletId)
    {

    }
};

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
            const bool needDiskLimiter = ev->Get()->IndexChanges->NeedDiskWriteLimiter();
            auto writeController = std::make_shared<NOlap::TCompactedWriteController>(ctx.SelfID, ev->Release());
            const TConclusion<bool> needDraftTransaction = writeController->GetBlobsAction().NeedDraftWritingTransaction();
            AFL_VERIFY(needDraftTransaction.IsSuccess())("error", needDraftTransaction.GetErrorMessage());
            if (*needDraftTransaction) {
                Execute(new TTxWriteDraft(this, writeController));
            } else if (needDiskLimiter) {
                NLimiter::TCompDiskOperator::AskResource(std::make_shared<TDiskResourcesRequest>(writeController, TabletID()));
            } else {
                Register(CreateWriteActor(TabletID(), writeController, TInstant::Max()));
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
