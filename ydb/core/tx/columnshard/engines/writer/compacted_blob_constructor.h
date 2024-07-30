#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/portions/with_blobs.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>

namespace NKikimr::NOlap {

class TCompactedWriteController : public NColumnShard::IWriteController {
private:
    TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> WriteIndexEv;
    TActorId DstActor;
    ui64 WriteVolume = 0;

protected:
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;
    virtual void DoAbort(const TString& reason) override;

public:
    const TBlobsAction& GetBlobsAction();
    ui64 GetWriteVolume() const {
        return WriteVolume;
    }

    TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv);
    ~TCompactedWriteController();
};

}
