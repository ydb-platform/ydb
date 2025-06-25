#include "columnshard_impl.h"
#include "columnshard_private_events.h"

#include "blobs_action/transaction/tx_draft.h"
#include "blobs_action/transaction/tx_write_index.h"
#include "engines/changes/abstract/abstract.h"
#include "engines/writer/compacted_blob_constructor.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/slide_limiter/usage/abstract.h>
#include <ydb/library/slide_limiter/usage/service.h>

namespace NKikimr::NColumnShard {

class TDiskResourcesRequest: public NLimiter::IResourceRequest {
private:
    using TBase = NLimiter::IResourceRequest;
    std::shared_ptr<NOlap::TCompactedWriteController> WriteController;
    const std::shared_ptr<NOlap::TColumnEngineChanges> Changes;
    const ui64 TabletId;

private:
    virtual void DoOnResourceAllocated() override {
        Changes->SetStage(NOlap::NChanges::EStage::Writing);
        NActors::TActivationContext::AsActorContext().Register(CreateWriteActor(TabletId, WriteController, TInstant::Max()));
    }

public:
    TDiskResourcesRequest(const std::shared_ptr<NOlap::TCompactedWriteController>& writeController, const ui64 tabletId,
        const std::shared_ptr<NOlap::TColumnEngineChanges>& changes)
        : TBase(writeController->GetWriteVolume())
        , WriteController(writeController)
        , Changes(changes)
        , TabletId(tabletId) {
    }
};

void TColumnShard::Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx) {
    auto putStatus = ev->Get()->GetPutStatus();

    if (putStatus == NKikimrProto::UNKNOWN) {
        const auto change = ev->Get()->IndexChanges;
        if (IsAnyChannelYellowStop()) {
            ACFL_ERROR("event", "TEvWriteIndex failed")("reason", "channel yellow stop");

            Counters.GetTabletCounters()->IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->SetPutStatus(NKikimrProto::TRYLATER);
            NOlap::TChangesFinishContext context("out of disk space");
            change->Abort(*this, context);
            ctx.Schedule(FailActivationDelay, new TEvPrivate::TEvPeriodicWakeup(true));
        } else {
            ACFL_DEBUG("event", "TEvWriteIndex")("count", change->GetWritePortionsCount());
            AFL_VERIFY(change->GetWritePortionsCount());
            const bool needDiskLimiter = change->NeedDiskWriteLimiter();
            auto writeController = std::make_shared<NOlap::TCompactedWriteController>(ctx.SelfID, ev->Release());
            const TConclusion<bool> needDraftTransaction = writeController->GetBlobsAction().NeedDraftWritingTransaction();
            AFL_VERIFY(needDraftTransaction.IsSuccess())("error", needDraftTransaction.GetErrorMessage());
            if (*needDraftTransaction) {
                ACFL_DEBUG("event", "TTxWriteDraft");
                change->SetStage(NOlap::NChanges::EStage::WriteDraft);
                Execute(new TTxWriteDraft(this, writeController));
            } else if (needDiskLimiter) {
                ACFL_DEBUG("event", "Limiter");
                change->SetStage(NOlap::NChanges::EStage::AskDiskQuota);
                NLimiter::TCompDiskOperator::AskResource(std::make_shared<TDiskResourcesRequest>(writeController, TabletID(), change));
            } else {
                ACFL_DEBUG("event", "WriteActor");
                change->SetStage(NOlap::NChanges::EStage::Writing);
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

}   // namespace NKikimr::NColumnShard
