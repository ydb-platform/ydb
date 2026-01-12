#pragma once

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/backup/import/session.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/actor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NImport {

class TImportActor: public NBackground::TSessionActor {
private:
    enum class EStage {
        Initialization,
        WaitData,
        WaitWriting,
        WaitSaveCursor,
        Finished
    };

    using TBase = NBackground::TSessionActor;

    EStage Stage = EStage::Initialization;
    std::shared_ptr<NImport::TSession> ImportSession;
    TActorId ImportActorId;
    void SwitchStage(const EStage from, const EStage to);

  protected:

    virtual void OnSessionStateSaved() override;

    virtual void OnTxCompleted(const ui64 /*txId*/) override;

    virtual void OnSessionProgressSaved() override;

    virtual void OnBootstrap(const TActorContext & /*ctx*/) override;
    
    void Handle(NColumnShard::TEvPrivate::TEvBackupImportRecordBatch::TPtr& ev);
    
    void Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev);

  public:
    TImportActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter> &adapter);

    STATEFN(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NColumnShard::TEvPrivate::TEvBackupImportRecordBatch, Handle);
                hFunc(NEvents::TDataEvents::TEvWriteResult, Handle);
                default:
                    TBase::StateInProgress(ev);
            }
        } catch (...) {
            AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NImport
