#pragma once
#include "write.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/tx/columnshard/export/controller/abstract/abstract.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/export/common/identifier.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/export/events/events.h>

namespace NKikimr::NOlap::NExport {

class TActor: public NActors::TActorBootstrapped<TActor> {
private:
    using TBase = NActors::TActorBootstrapped<TActor>;
    const TIdentifier ExportId;
    const TActorId ShardActorId;
    const TTabletId ShardTabletId;
    const std::shared_ptr<IController> ExternalControl;
    std::shared_ptr<IBlobsStorageOperator> BlobsOperator;
    std::optional<TActorId> ScanActorId;

    bool LastMessageReceived = false;
    std::shared_ptr<arrow::RecordBatch> CurrentData;
    std::vector<TString> CurrentDataBlobs;
    std::optional<TOwnedCellVec> LastKey;
    static inline const ui64 FreeSpace = ((ui64)8) << 20;
protected:
    void HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        AFL_VERIFY(!ScanActorId);
        auto& msg = ev->Get()->Record;
        ScanActorId = ActorIdFromProto(msg.GetScanActorId());
        ExternalControl->OnScanStarted();
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)ShardTabletId, 1));
    }

    void HandleExecute(NEvents::TEvExportCursorSaved::TPtr& /*ev*/) {
        AFL_VERIFY(ScanActorId);
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)ShardTabletId, 1));
    }

    void HandleExecute(NEvents::TEvExportWritingFinished::TPtr& /*ev*/) {
        AFL_VERIFY(LastKey);
        Send(ShardActorId, new NEvents::TEvExportSaveCursor(ExportId, TCursor(*LastKey, LastMessageReceived)));
        if (LastMessageReceived) {
            PassAway();
        }
    }

    void HandleExecute(NEvents::TEvExportWritingFailed::TPtr& /*ev*/);

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev);

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& /*ev*/) {
        AFL_VERIFY(false);
    }
public:

    TActor(const TActorId& shardActorId, const TTabletId shardTabletId, std::shared_ptr<IController> externalControl,
        const std::shared_ptr<IBlobsStorageOperator>& bOperator, const TIdentifier& id)
        : ExportId(id)
        , ShardActorId(shardActorId)
        , ShardTabletId(shardTabletId)
        , ExternalControl(externalControl)
        , BlobsOperator(bOperator)
    {

    }

    void Bootstrap() {
        auto evStart = ExternalControl->BuildRequestInitiator();
        Send(ShardActorId, evStart.release());
    }

    STATEFN(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanData, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanError, HandleExecute);
                hFunc(NEvents::TEvExportCursorSaved, HandleExecute);
                hFunc(NEvents::TEvExportWritingFinished, HandleExecute);
                hFunc(NEvents::TEvExportWritingFailed, HandleExecute);
                default:
                    AFL_VERIFY(false)("event_type", ev->GetTypeName());
            }
        } catch (...) {
            AFL_VERIFY(false);
        }
    }


};

}