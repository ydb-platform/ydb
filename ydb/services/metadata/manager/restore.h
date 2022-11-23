#pragma once
#include "table_record.h"

#include <ydb/services/metadata/manager/restore_controller.h>
#include <ydb/services/metadata/request/request_actor.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadataManager {

template <class TObject>
class TRestoreObjectsActor: public NActors::TActorBootstrapped<TRestoreObjectsActor<TObject>> {
private:
    using TBase = NActors::TActorBootstrapped<TRestoreObjectsActor<TObject>>;
    using IRestoreObjectsController = IRestoreObjectsController<TObject>;
    typename IRestoreObjectsController::TPtr Controller;
    const TTableRecords ObjectIds;
    TString SessionId;

    void Handle(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogSelect>::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        const NInternal::NRequest::TDialogSelect::TResponse& result = ev->Get()->GetResult();
        Ydb::Table::ExecuteQueryResult qResult;
        result.operation().result().UnpackTo(&qResult);
        Y_VERIFY((size_t)qResult.result_sets().size() == 1);

        typename TObject::TDecoder decoder(qResult.result_sets()[0]);
        std::vector<TObject> objects;
        for (auto&& row : qResult.result_sets()[0].rows()) {
            TObject object;
            if (!object.DeserializeFromRecord(decoder, row)) {
                Controller->RestoreProblem("cannot parse exists object");
                return;
            }
            objects.emplace_back(std::move(object));
        }
        Controller->RestoreFinished(std::move(objects), qResult.tx_meta().id());
    }

    void Handle(NInternal::NRequest::TEvRequestFailed::TPtr& /*ev*/) {
        auto g = TBase::PassAwayGuard();
        Controller->RestoreProblem("cannot execute yql request");
    }

public:
    TRestoreObjectsActor(const TTableRecords& objectIds, typename IRestoreObjectsController::TPtr controller, const TString& sessionId)
        : Controller(controller)
        , ObjectIds(objectIds)
        , SessionId(sessionId)
    {
        Y_VERIFY(SessionId);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogSelect>, Handle);
            hFunc(NInternal::NRequest::TEvRequestFailed, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        if (ObjectIds.empty()) {
            Controller->RestoreProblem("no objects for restore");
            TBase::PassAway();
        }
        auto request = ObjectIds.BuildSelectQuery(TObject::GetStorageTablePath());
        request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        request.set_session_id(SessionId);
        TBase::Become(&TRestoreObjectsActor::StateMain);
        TBase::Register(new NInternal::NRequest::TYDBRequest<NInternal::NRequest::TDialogSelect>(request, TBase::SelfId()));
    }
};

}
