#pragma once
#include "table_record.h"

#include <ydb/services/metadata/manager/restore_controller.h>
#include <ydb/services/metadata/request/request_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadata::NModifications {

template <class TObject>
class TRestoreObjectsActor: public NActors::TActorBootstrapped<TRestoreObjectsActor<TObject>> {
private:
    using TBase = NActors::TActorBootstrapped<TRestoreObjectsActor<TObject>>;
    using IRestoreObjectsController = IRestoreObjectsController<TObject>;
    typename IRestoreObjectsController::TPtr Controller;
    const NInternal::TTableRecords ObjectIds;
    TString SessionId;
    const NACLib::TUserToken UserToken;

    void Handle(NRequest::TEvRequestResult<NRequest::TDialogSelect>::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        const NRequest::TDialogSelect::TResponse& result = ev->Get()->GetResult();
        Ydb::Table::ExecuteQueryResult qResult;
        result.operation().result().UnpackTo(&qResult);
        Y_ABORT_UNLESS((size_t)qResult.result_sets().size() == 1);

        typename TObject::TDecoder decoder(qResult.result_sets()[0]);
        std::vector<TObject> objects;
        for (auto&& row : qResult.result_sets()[0].rows()) {
            TObject object;
            if (!object.DeserializeFromRecord(decoder, row)) {
                Controller->OnRestoringProblem("cannot parse exists object");
                return;
            }
            objects.emplace_back(std::move(object));
        }
        Controller->OnRestoringFinished(std::move(objects), qResult.tx_meta().id());
    }

    void Handle(NRequest::TEvRequestFailed::TPtr& /*ev*/) {
        auto g = TBase::PassAwayGuard();
        Controller->OnRestoringProblem("cannot execute yql request");
    }

public:
    TRestoreObjectsActor(const NInternal::TTableRecords& objectIds, const NACLib::TUserToken& uToken, typename IRestoreObjectsController::TPtr controller, const TString& sessionId)
        : Controller(controller)
        , ObjectIds(objectIds)
        , SessionId(sessionId)
        , UserToken(uToken)
    {
        Y_ABORT_UNLESS(SessionId);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NRequest::TEvRequestResult<NRequest::TDialogSelect>, Handle);
            hFunc(NRequest::TEvRequestFailed, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        if (ObjectIds.empty()) {
            Controller->OnRestoringProblem("no objects for restore");
            TBase::PassAway();
        }
        auto request = ObjectIds.BuildSelectQuery(TObject::GetBehaviour()->GetStorageTablePath());
        request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        request.set_session_id(SessionId);
        TBase::Become(&TRestoreObjectsActor::StateMain);
        TBase::Register(new NRequest::TYDBCallbackRequest<NRequest::TDialogSelect>(request, UserToken, TBase::SelfId()));
    }
};

}
