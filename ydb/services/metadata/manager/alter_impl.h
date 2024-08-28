#pragma once
#include "abstract.h"
#include "modification_controller.h"
#include "preparation_controller.h"
#include "restore.h"
#include "modification.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadata::NModifications {

template <class TObject>
class TProcessingController:
    public IRestoreObjectsController<TObject>,
    public IModificationObjectsController,
    public IAlterPreparationController<TObject> {
private:
    const TActorIdentity ActorId;
public:
    using TPtr = std::shared_ptr<TProcessingController>;
    TProcessingController(const TActorIdentity actorId)
        : ActorId(actorId)
    {

    }

    virtual void OnRestoringFinished(std::vector<TObject>&& objects, const TString& transactionId) override {
        ActorId.Send(ActorId, new TEvRestoreFinished<TObject>(std::move(objects), transactionId));
    }
    virtual void OnRestoringProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvRestoreProblem(errorMessage));
    }
    virtual void OnModificationFinished() override {
        ActorId.Send(ActorId, new TEvModificationFinished());
    }
    virtual void OnModificationProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvModificationProblem(errorMessage));
    }
    virtual void OnPreparationProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvAlterPreparationProblem(errorMessage));
    }
    virtual void OnPreparationFinished(std::vector<TObject>&& objects)  override {
        ActorId.Send(ActorId, new TEvAlterPreparationFinished<TObject>(std::move(objects)));
    }

};

template <class TObject>
class TModificationActorImpl: public NActors::TActorBootstrapped<TModificationActorImpl<TObject>> {
private:
    using TBase = NActors::TActorBootstrapped<TModificationActorImpl<TObject>>;
protected:
    TString SessionId;
    TString TransactionId;
    typename TProcessingController<TObject>::TPtr InternalController;
    IAlterController::TPtr ExternalController;
    typename IObjectOperationsManager<TObject>::TPtr Manager;
    const IOperationsManager::TInternalModificationContext Context;
    std::vector<NInternal::TTableRecord> Patches;
    NInternal::TTableRecords RestoreObjectIds;
    const NACLib::TUserToken UserToken = NACLib::TSystemUsers::Metadata();
    virtual bool PrepareRestoredObjects(std::vector<TObject>& objects) const = 0;
    virtual bool ProcessPreparedObjects(NInternal::TTableRecords&& records) const = 0;
    virtual void InitState() = 0;
    virtual bool BuildRestoreObjectIds() = 0;
public:
    TModificationActorImpl(NInternal::TTableRecord&& patch,
        IAlterController::TPtr controller,
        const typename IObjectOperationsManager<TObject>::TPtr& manager,
        const IOperationsManager::TInternalModificationContext& context)
        : ExternalController(controller)
        , Manager(manager)
        , Context(context) {
        Patches.emplace_back(std::move(patch));
    }

    TModificationActorImpl(const NInternal::TTableRecord& patch, IAlterController::TPtr controller,
        const typename IObjectOperationsManager<TObject>::TPtr& manager,
        const IOperationsManager::TInternalModificationContext& context)
        : ExternalController(controller)
        , Manager(manager)
        , Context(context) {
        Patches.emplace_back(patch);
    }

    TModificationActorImpl(std::vector<NInternal::TTableRecord>&& patches, IAlterController::TPtr controller,
        const typename IObjectOperationsManager<TObject>::TPtr& manager,
        const IOperationsManager::TInternalModificationContext& context)
        : ExternalController(controller)
        , Manager(manager)
        , Context(context)
        , Patches(std::move(patches)) {

    }

    TModificationActorImpl(const std::vector<NInternal::TTableRecord>& patches, IAlterController::TPtr controller,
        const typename IObjectOperationsManager<TObject>::TPtr& manager,
        const IOperationsManager::TInternalModificationContext& context)
        : ExternalController(controller)
        , Manager(manager)
        , Context(context)
        , Patches(patches) {

    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NRequest::TEvRequestResult<NRequest::TDialogCreateSession>, Handle);
            hFunc(TEvRestoreFinished<TObject>, Handle);
            hFunc(TEvAlterPreparationFinished<TObject>, Handle);
            hFunc(NRequest::TEvRequestFailed, Handle);
            hFunc(TEvRestoreProblem, Handle);
            hFunc(TEvAlterPreparationProblem, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        InitState();
        if (!Patches.size()) {
            ExternalController->OnAlteringProblem("no patches");
            return this->PassAway();
        }
        if (!BuildRestoreObjectIds()) {
            return this->PassAway();
        }

        TBase::Register(new NRequest::TYDBCallbackRequest<NRequest::TDialogCreateSession>(
            NRequest::TDialogCreateSession::TRequest(), UserToken, TBase::SelfId()));
    }

    void Handle(typename NRequest::TEvRequestResult<NRequest::TDialogCreateSession>::TPtr& ev) {
        Ydb::Table::CreateSessionResponse currentFullReply = ev->Get()->GetResult();
        Ydb::Table::CreateSessionResult session;
        currentFullReply.operation().result().UnpackTo(&session);
        SessionId = session.session_id();
        Y_ABORT_UNLESS(SessionId);

        InternalController = std::make_shared<TProcessingController<TObject>>(TBase::SelfId());
        TBase::Register(new TRestoreObjectsActor<TObject>(RestoreObjectIds, UserToken, InternalController, SessionId));
    }

    void Handle(typename TEvRestoreFinished<TObject>::TPtr& ev) {
        TransactionId = ev->Get()->GetTransactionId();
        Y_ABORT_UNLESS(TransactionId);
        std::vector<TObject> objects = std::move(ev->Get()->MutableObjects());
        if (!PrepareRestoredObjects(objects)) {
            this->PassAway();
        } else {
            Manager->PrepareObjectsBeforeModification(std::move(objects), InternalController, Context);
        }
    }

    void Handle(typename TEvAlterPreparationFinished<TObject>::TPtr& ev) {
        NInternal::TTableRecords records;
        records.InitColumns(Manager->GetSchema().GetYDBColumns());
        records.ReserveRows(ev->Get()->GetObjects().size());
        for (auto&& i : ev->Get()->GetObjects()) {
            if (!records.AddRecordNativeValues(i.SerializeToRecord())) {
                ExternalController->OnAlteringProblem("unexpected serialization inconsistency");
                return this->PassAway();
            }
        }
        if (!ProcessPreparedObjects(std::move(records))) {
            ExternalController->OnAlteringProblem("cannot process prepared objects");
            return this->PassAway();
        }
    }

    void Handle(typename NRequest::TEvRequestFailed::TPtr& /*ev*/) {
        auto g = TBase::PassAwayGuard();
        ExternalController->OnAlteringProblem("cannot initialize session");
    }

    void Handle(TEvAlterPreparationProblem::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        ExternalController->OnAlteringProblem("preparation problem: " + ev->Get()->GetErrorMessage());
    }

    void Handle(TEvRestoreProblem::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        ExternalController->OnAlteringProblem("cannot restore objects: " + ev->Get()->GetErrorMessage());
    }

    void PassAway() override {
        if (SessionId) {
            NMetadata::NRequest::TDialogDeleteSession::TRequest deleteRequest;
            deleteRequest.set_session_id(SessionId);
            TBase::Register(new NRequest::TYDBCallbackRequest<NRequest::TDialogDeleteSession>(deleteRequest, UserToken, TBase::SelfId()));
        }

        TBase::PassAway();
    }
};

template <class TObject>
class TModificationActor: public TModificationActorImpl<TObject> {
private:
    using TBase = TModificationActorImpl<TObject>;
protected:
    using TBase::Manager;
    virtual void InitState() override {
        TBase::UnsafeBecome(&TModificationActor<TObject>::StateMain);
    }

    virtual bool BuildRestoreObjectIds() override {
        TBase::RestoreObjectIds.InitColumns(Manager->GetSchema().GetPKColumns());
        for (auto&& i : TBase::Patches) {
            if (!TBase::RestoreObjectIds.AddRecordNativeValues(i)) {
                TBase::ExternalController->OnAlteringProblem("no pk columns in patch");
                return false;
            }
        }
        return true;
    }

    virtual TString GetModificationType() const = 0;

public:
    using TBase::TBase;
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvModificationFinished, Handle);
            hFunc(TEvModificationProblem, Handle);
            default:
                TBase::StateMain(ev);
        }
    }

    virtual bool PrepareRestoredObjects(std::vector<TObject>& objects) const override {
        std::vector<bool> realPatches;
        realPatches.resize(TBase::Patches.size(), false);
        for (auto&& i : objects) {
            const NInternal::TTableRecord* trPatch = nullptr;
            NInternal::TTableRecord trObject = i.SerializeToRecord();
            for (auto&& p : TBase::Patches) {
                if (p.CompareColumns(trObject, Manager->GetSchema().GetPKColumnIds())) {
                    trPatch = &p;
                    break;
                }
            }
            TObject objectPatched;
            if (!trPatch) {
                TBase::ExternalController->OnAlteringProblem("cannot found patch for object");
                return false;
            } else if (!trObject.TakeValuesFrom(*trPatch)) {
                TBase::ExternalController->OnAlteringProblem("cannot patch object");
                return false;
            } else if (!TObject::TDecoder::DeserializeFromRecord(objectPatched, trObject)) {
                TBase::ExternalController->OnAlteringProblem("cannot parse object after patch");
                return false;
            } else {
                i = std::move(objectPatched);
            }
        }
        for (auto&& p : TBase::Patches) {
            bool found = false;
            for (auto&& i : objects) {
                if (i.SerializeToRecord().CompareColumns(p, Manager->GetSchema().GetPKColumnIds())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                TObject object;
                if (!TObject::TDecoder::DeserializeFromRecord(object, p)) {
                    TBase::ExternalController->OnAlteringProblem("cannot parse new object");
                    return false;
                }
                objects.emplace_back(std::move(object));
            }
        }
        return true;
    }

    void Handle(TEvModificationFinished::TPtr& /*ev*/) {
        auto g = TBase::PassAwayGuard();
        TBase::ExternalController->OnAlteringFinished();
    }

    void Handle(TEvModificationProblem::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        TBase::ExternalController->OnAlteringProblem("cannot " + GetModificationType() + " objects: " + ev->Get()->GetErrorMessage());
    }

};

}
