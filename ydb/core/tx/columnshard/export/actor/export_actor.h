#pragma once
#include "write.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/actor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/export/common/identifier.h>
#include <ydb/core/tx/columnshard/export/events/events.h>
#include <ydb/core/tx/columnshard/export/session/selector/abstract/selector.h>
#include <ydb/core/tx/columnshard/export/session/session.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NExport {

class TActor: public NBackground::TSessionActor {
private:
    enum class EStage {
        Initialization,
        WaitData,
        WaitWriting,
        WaitSaveCursor,
        Finished
    };

    using TBase = NBackground::TSessionActor;
    std::shared_ptr<IBlobsStorageOperator> BlobsOperator;
    std::optional<TActorId> ScanActorId;

    EStage Stage = EStage::Initialization;
    std::shared_ptr<arrow::RecordBatch> CurrentData;
    TString CurrentDataBlob;
    std::shared_ptr<NExport::TSession> ExportSession;
    static inline const ui64 FreeSpace = ((ui64)8) << 20;
    void SwitchStage(const EStage from, const EStage to) {
        AFL_VERIFY(Stage == from)("from", (ui32)from)("real", (ui32)Stage)("to", (ui32)to);
        Stage = to;
    }

protected:
    void HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        SwitchStage(EStage::Initialization, EStage::WaitData);
        AFL_VERIFY(!ScanActorId);
        auto& msg = ev->Get()->Record;
        ScanActorId = ActorIdFromProto(msg.GetScanActorId());
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)TabletId, 1));
    }

    virtual void OnSessionStateSaved() override;

    virtual void OnTxCompleted(const ui64 /*txId*/) override {
        Session->FinishActor();
    }

    virtual void OnSessionProgressSaved() override {
        SwitchStage(EStage::WaitSaveCursor, EStage::WaitData);
        if (ExportSession->GetCursor().IsFinished()) {
            ExportSession->Finish();
            SaveSessionState();
        } else {
            AFL_VERIFY(ScanActorId);
            TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)TabletId, 1));
        }
    }

    void HandleExecute(NEvents::TEvExportWritingFinished::TPtr& /*ev*/) {
        SwitchStage(EStage::WaitWriting, EStage::WaitSaveCursor);
        AFL_VERIFY(ExportSession->GetCursor().HasLastKey());
        SaveSessionProgress();
    }

    void HandleExecute(NEvents::TEvExportWritingFailed::TPtr& ev);

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev);

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& /*ev*/) {
        AFL_VERIFY(false);
    }

    virtual void OnBootstrap(const TActorContext& /*ctx*/) override {
        auto evStart = ExportSession->GetTask().GetSelector()->BuildRequestInitiator(ExportSession->GetCursor());
        evStart->Record.SetGeneration((ui64)TabletId);
        Send(TabletActorId, evStart.release());
        Become(&TActor::StateFunc);
    }

public:
    TActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter>& adapter,
        const std::shared_ptr<IBlobsStorageOperator>& blobsOperator)
        : TBase(bgSession, adapter)
        , BlobsOperator(blobsOperator) {
        ExportSession = bgSession->GetLogicAsVerifiedPtr<NExport::TSession>();
    }

    STATEFN(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanData, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanError, HandleExecute);
                hFunc(NEvents::TEvExportWritingFinished, HandleExecute);
                hFunc(NEvents::TEvExportWritingFailed, HandleExecute);
                default:
                    TBase::StateInProgress(ev);
            }
        } catch (...) {
            AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NExport
