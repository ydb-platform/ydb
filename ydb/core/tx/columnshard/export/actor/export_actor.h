#pragma once

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/actor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/export/common/identifier.h>
#include <ydb/core/tx/columnshard/export/session/selector/abstract/selector.h>
#include <ydb/core/tx/columnshard/export/session/session.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

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
    std::shared_ptr<NExport::TSession> ExportSession;
    TActorId Exporter;
    static inline const ui64 FreeSpace = ((ui64)8) << 20;
    void SwitchStage(const EStage from, const EStage to);

protected:
    void HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev);

    virtual void OnSessionStateSaved() override;

    virtual void OnTxCompleted(const ui64 /*txId*/) override;

    virtual void OnSessionProgressSaved() override;

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev);

    void HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& /*ev*/) {
        AFL_VERIFY(false);
    }
    
    void HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult::TPtr& /*ev*/);
    
    void HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportError::TPtr& /*ev*/);

    virtual void OnBootstrap(const TActorContext& /*ctx*/) override;

public:
    TActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter>& adapter,
        const std::shared_ptr<IBlobsStorageOperator>& blobsOperator);

    STATEFN(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanData, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanError, HandleExecute);
                hFunc(NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult, HandleExecute);
                hFunc(NColumnShard::TEvPrivate::TEvBackupExportError, HandleExecute);
                default:
                    TBase::StateInProgress(ev);
            }
        } catch (...) {
            AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NExport
