#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

class TCompactedWriteController : public NColumnShard::IWriteController {
private:
    ui64 CurrentPortion = 0;
    ui64 CurrentBlobIndex = 0;
    TPortionInfoWithBlobs::TBlobInfo* CurrentBlobInfo = nullptr;

    TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> WriteIndexEv;
    std::shared_ptr<IBlobsAction> Action;
    TActorId DstActor;
protected:
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;
    virtual bool IsBlobActionsReady() const override {
        return Action->IsReady();
    }
public:
    TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv, bool blobGrouppingEnabled);
    ~TCompactedWriteController() {
        if (WriteIndexEv && WriteIndexEv->IndexChanges) {
            WriteIndexEv->IndexChanges->AbortEmergency();
        }
    }

    virtual std::vector<std::shared_ptr<IBlobsAction>> GetBlobActions() const override {
        return {Action};
    }

    virtual std::optional<TBlobWriteInfo> Next() override;

    virtual void OnBlobWriteResult(const TEvBlobStorage::TEvPutResult& result) override {
        Action->OnBlobWriteResult(result.Id, result.Status);
    }
};

}
