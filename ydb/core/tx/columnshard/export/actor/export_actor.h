#pragma once
#include "write.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/export/common/identifier.h>
#include <ydb/core/tx/columnshard/export/events/events.h>
#include <ydb/core/tx/columnshard/export/session/selector/abstract/selector.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NExport {

class TActor: public NActors::TActorBootstrapped<TActor> {
private:
    enum class EStage {
        Initialization,
        Initialized,
        WaitData,
        WaitWriting,
        WaitSaveCursor,
        Finished
    };

    using TBase = NActors::TActorBootstrapped<TActor>;
    const TIdentifier ExportId;
    const TActorId ShardActorId;
    const TTabletId ShardTabletId;
    NArrow::NSerialization::TSerializerContainer Serializer;
    TSelectorContainer Selector;
    std::shared_ptr<IBlobsStorageOperator> BlobsOperator;
    std::optional<TActorId> ScanActorId;

    TCursor Cursor;
    EStage Stage = EStage::Initialization;
    std::shared_ptr<arrow::RecordBatch> CurrentData;
    TString CurrentDataBlob;
    static inline const ui64 FreeSpace = ((ui64)8) << 20;
    void SwitchStage(const EStage from, const EStage to) {
        AFL_VERIFY(Stage == from);
        Stage = to;
    }

protected:
    void HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        SwitchStage(EStage::Initialization, EStage::Initialized);
        AFL_VERIFY(!ScanActorId);
        auto& msg = ev->Get()->Record;
        ScanActorId = ActorIdFromProto(msg.GetScanActorId());
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)ShardTabletId, 1));
    }

    void HandleExecute(NEvents::TEvExportCursorSaved::TPtr& /*ev*/) {
        SwitchStage(EStage::WaitSaveCursor, EStage::WaitData);
        AFL_VERIFY(ScanActorId);
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)ShardTabletId, 1));
    }

    void HandleExecute(NEvents::TEvExportWritingFinished::TPtr& /*ev*/) {
        SwitchStage(EStage::WaitWriting, EStage::WaitSaveCursor);
        AFL_VERIFY(Cursor.HasLastKey());
        Send(ShardActorId, new NEvents::TEvExportSaveCursor(ExportId, Cursor));
        if (Cursor.IsFinished()) {
            PassAway();
        }
    }

    void HandleExecute(NEvents::TEvExportWritingFailed::TPtr& ev);

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev);

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& /*ev*/) {
        AFL_VERIFY(false);
    }
public:

    TActor(const TActorId& shardActorId, const TTabletId shardTabletId, const NArrow::NSerialization::TSerializerContainer& serializer, const TSelectorContainer& selector,
        const std::shared_ptr<IBlobsStorageOperator>& bOperator, const TIdentifier& id, const TCursor& startCursor)
        : ExportId(id)
        , ShardActorId(shardActorId)
        , ShardTabletId(shardTabletId)
        , Serializer(serializer)
        , Selector(selector)
        , BlobsOperator(bOperator)
        , Cursor(startCursor)
    {
        AFL_VERIFY(serializer);
        AFL_VERIFY(selector);
    }

    void Bootstrap() {
        auto evStart = Selector->BuildRequestInitiator(Cursor);
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