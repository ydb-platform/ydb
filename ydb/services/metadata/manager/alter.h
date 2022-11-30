#pragma once
#include "modification_controller.h"
#include "preparation_controller.h"
#include "restore.h"
#include "modification.h"

#include <ydb/services/metadata/abstract/manager.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadataManager {

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

    virtual void RestoreFinished(std::vector<TObject>&& objects, const TString& transactionId) override {
        ActorId.Send(ActorId, new TEvRestoreFinished<TObject>(std::move(objects), transactionId));
    }
    virtual void RestoreProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvRestoreProblem(errorMessage));
    }
    virtual void ModificationFinished() override {
        ActorId.Send(ActorId, new TEvModificationFinished());
    }
    virtual void ModificationProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvModificationProblem(errorMessage));
    }
    virtual void PreparationProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvAlterPreparationProblem(errorMessage));
    }
    virtual void PreparationFinished(std::vector<TObject>&& objects)  override {
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
    const NMetadata::IOperationsManager::TModificationContext Context;
    std::vector<TTableRecord> Patches;
    TTableRecords RestoreObjectIds;
    const NACLib::TUserToken UserToken = NACLib::TSystemUsers::Metadata();
    virtual bool PrepareRestoredObjects(std::vector<TObject>& objects) const = 0;
    virtual bool ProcessPreparedObjects(TTableRecords&& records) const = 0;
    virtual void InitState() = 0;
    virtual bool BuildRestoreObjectIds() = 0;
public:
    TModificationActorImpl(TTableRecord&& patch, IAlterController::TPtr controller, const NMetadata::IOperationsManager::TModificationContext& context)
        : ExternalController(controller)
        , Context(context) {
        Patches.emplace_back(std::move(patch));
    }

    TModificationActorImpl(const TTableRecord& patch, IAlterController::TPtr controller, const NMetadata::IOperationsManager::TModificationContext& context)
        : ExternalController(controller)
        , Context(context) {
        Patches.emplace_back(patch);
    }

    TModificationActorImpl(std::vector<TTableRecord>&& patches, IAlterController::TPtr controller, const NMetadata::IOperationsManager::TModificationContext& context)
        : ExternalController(controller)
        , Context(context)
        , Patches(std::move(patches)) {

    }

    TModificationActorImpl(const std::vector<TTableRecord>& patches, IAlterController::TPtr controller, const NMetadata::IOperationsManager::TModificationContext& context)
        : ExternalController(controller)
        , Context(context)
        , Patches(patches) {

    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogCreateSession>, Handle);
            hFunc(TEvRestoreFinished<TObject>, Handle);
            hFunc(TEvAlterPreparationFinished<TObject>, Handle);
            hFunc(NInternal::NRequest::TEvRequestFailed, Handle);
            hFunc(TEvRestoreProblem, Handle);
            hFunc(TEvAlterPreparationProblem, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        InitState();
        if (!Patches.size()) {
            ExternalController->AlterProblem("no patches");
            return TBase::PassAway();
        }
        if (!BuildRestoreObjectIds()) {
            return TBase::PassAway();
        }

        TBase::Register(new NInternal::NRequest::TYDBRequest<NInternal::NRequest::TDialogCreateSession>(
            NInternal::NRequest::TDialogCreateSession::TRequest(), UserToken, TBase::SelfId()));
    }

    void Handle(typename NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogCreateSession>::TPtr& ev) {
        Ydb::Table::CreateSessionResponse currentFullReply = ev->Get()->GetResult();
        Ydb::Table::CreateSessionResult session;
        currentFullReply.operation().result().UnpackTo(&session);
        SessionId = session.session_id();
        Y_VERIFY(SessionId);

        InternalController = std::make_shared<TProcessingController<TObject>>(TBase::SelfId());
        TBase::Register(new TRestoreObjectsActor<TObject>(RestoreObjectIds, UserToken, InternalController, SessionId));
    }

    void Handle(typename TEvRestoreFinished<TObject>::TPtr& ev) {
        TransactionId = ev->Get()->GetTransactionId();
        Y_VERIFY(TransactionId);
        std::vector<TObject> objects = std::move(ev->Get()->MutableObjects());
        if (!PrepareRestoredObjects(objects)) {
            TBase::PassAway();
        } else {
            TObject::AlteringPreparation(std::move(objects), InternalController, Context);
        }
    }

    void Handle(typename TEvAlterPreparationFinished<TObject>::TPtr& ev) {
        TTableRecords records;
        records.InitColumns(TObject::TDecoder::GetColumns());
        records.ReserveRows(ev->Get()->GetObjects().size());
        for (auto&& i : ev->Get()->GetObjects()) {
            if (!records.AddRecordNativeValues(i.SerializeToRecord())) {
                ExternalController->AlterProblem("unexpected serialization inconsistency");
                return TBase::PassAway();
            }
        }
        if (!ProcessPreparedObjects(std::move(records))) {
            ExternalController->AlterProblem("cannot process prepared objects");
            return TBase::PassAway();
        }
    }

    void Handle(typename NInternal::NRequest::TEvRequestFailed::TPtr& /*ev*/) {
        auto g = TBase::PassAwayGuard();
        ExternalController->AlterProblem("cannot initialize session");
    }

    void Handle(TEvAlterPreparationProblem::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        ExternalController->AlterProblem("preparation problem: " + ev->Get()->GetErrorMessage());
    }

    void Handle(TEvRestoreProblem::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        ExternalController->AlterProblem("cannot restore objects: " + ev->Get()->GetErrorMessage());
    }

};

template <class TObject>
class TModificationActor: public TModificationActorImpl<TObject> {
private:
    using TBase = TModificationActorImpl<TObject>;
protected:
    virtual void InitState() override {
        TBase::Become(&TModificationActor<TObject>::StateMain);
    }

    virtual bool BuildRestoreObjectIds() override {
        TBase::RestoreObjectIds.InitColumns(TObject::TDecoder::GetPKColumns());
        for (auto&& i : TBase::Patches) {
            if (!TBase::RestoreObjectIds.AddRecordNativeValues(i)) {
                TBase::ExternalController->AlterProblem("no pk columns in patch");
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
                TBase::StateMain(ev, ctx);
        }
    }

    virtual bool PrepareRestoredObjects(std::vector<TObject>& objects) const override {
        std::vector<bool> realPatches;
        realPatches.resize(TBase::Patches.size(), false);
        for (auto&& i : objects) {
            const TTableRecord* trPatch = nullptr;
            TTableRecord trObject = i.SerializeToRecord();
            for (auto&& p : TBase::Patches) {
                if (p.CompareColumns(trObject, TObject::TDecoder::GetPKColumnIds())) {
                    trPatch = &p;
                    break;
                }
            }
            TObject objectPatched;
            if (!trPatch) {
                TBase::ExternalController->AlterProblem("cannot found patch for object");
                return false;
            } else if (!trObject.TakeValuesFrom(*trPatch)) {
                TBase::ExternalController->AlterProblem("cannot patch object");
                return false;
            } else if (!TObject::TDecoder::DeserializeFromRecord(objectPatched, trObject)) {
                TBase::ExternalController->AlterProblem("cannot parse object after patch");
                return false;
            } else {
                i = std::move(objectPatched);
            }
        }
        for (auto&& p : TBase::Patches) {
            bool found = false;
            for (auto&& i : objects) {
                if (i.SerializeToRecord().CompareColumns(p, TObject::TDecoder::GetPKColumnIds())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                TObject object;
                if (!TObject::TDecoder::DeserializeFromRecord(object, p)) {
                    TBase::ExternalController->AlterProblem("cannot parse new object");
                    return false;
                }
                objects.emplace_back(std::move(object));
            }
        }
        return true;
    }

    void Handle(TEvModificationFinished::TPtr& /*ev*/) {
        auto g = TBase::PassAwayGuard();
        TBase::ExternalController->AlterFinished();
    }

    void Handle(TEvModificationProblem::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        TBase::ExternalController->AlterProblem("cannot " + GetModificationType() + " objects: " + ev->Get()->GetErrorMessage());
    }

};

template <class TObject>
class TAlterActor: public TModificationActor<TObject> {
private:
    using TBase = TModificationActor<TObject>;
protected:
    virtual bool ProcessPreparedObjects(TTableRecords&& records) const override {
        TBase::Register(new TUpdateObjectsActor<TObject>(std::move(records), TBase::UserToken,
            TBase::InternalController, TBase::SessionId, TBase::TransactionId, TBase::Context.GetUserToken()));
        return true;
    }

    virtual TString GetModificationType() const override {
        return "ALTER";
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TCreateActor: public TModificationActor<TObject> {
private:
    using TBase = TModificationActor<TObject>;
protected:
    virtual bool ProcessPreparedObjects(TTableRecords&& records) const override {
        TBase::Register(new TInsertObjectsActor<TObject>(std::move(records), TBase::UserToken,
            TBase::InternalController, TBase::SessionId, TBase::TransactionId, TBase::Context.GetUserToken()));
        return true;
    }

    virtual TString GetModificationType() const override {
        return "CREATE";
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TDropActor: public TModificationActor<TObject> {
private:
    using TBase = TModificationActor<TObject>;
protected:
    virtual void InitState() override {
        TBase::Become(&TDropActor<TObject>::StateMain);
    }

    virtual bool BuildRestoreObjectIds() override {
        auto& first = TBase::Patches.front();
        std::vector<Ydb::Column> columns = first.SelectOwnedColumns(TObject::TDecoder::GetPKColumns());
        if (!columns.size()) {
            TBase::ExternalController->AlterProblem("no pk columns in patch");
            return false;
        }
        if (!columns.size()) {
            TBase::ExternalController->AlterProblem("no columns for pk detection");
            return false;
        }
        TBase::RestoreObjectIds.InitColumns(columns);
        for (auto&& i : TBase::Patches) {
            if (!TBase::RestoreObjectIds.AddRecordNativeValues(i)) {
                TBase::ExternalController->AlterProblem("incorrect pk columns");
                return false;
            }
        }
        return true;
    }
    virtual TString GetModificationType() const override {
        return "DROP";
    }
public:
    using TBase::TBase;

    virtual bool ProcessPreparedObjects(TTableRecords&& records) const override {
        TBase::Register(new TDeleteObjectsActor<TObject>(std::move(records), TBase::UserToken,
            TBase::InternalController, TBase::SessionId, TBase::TransactionId, TBase::Context.GetUserToken()));
        return true;
    }

    virtual bool PrepareRestoredObjects(std::vector<TObject>& /*objects*/) const override {
        return true;
    }

};

template <class TObject>
class TCreateCommand: public NMetadata::IAlterCommand {
private:
    using TBase = NMetadata::IAlterCommand;
protected:
    virtual void DoExecute() const override {
        Context.SetActivityType(NMetadata::IOperationsManager::EActivityType::Create);
        TActivationContext::AsActorContext().Register(new NMetadataManager::TCreateActor<TObject>(GetRecords(), GetController(), GetContext()));
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TAlterCommand: public NMetadata::IAlterCommand {
private:
    using TBase = NMetadata::IAlterCommand;
protected:
    virtual void DoExecute() const override {
        Context.SetActivityType(NMetadata::IOperationsManager::EActivityType::Alter);
        TActivationContext::AsActorContext().Register(new NMetadataManager::TAlterActor<TObject>(GetRecords(), GetController(), GetContext()));
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TDropCommand: public NMetadata::IAlterCommand {
private:
    using TBase = NMetadata::IAlterCommand;
protected:
    virtual void DoExecute() const override {
        Context.SetActivityType(NMetadata::IOperationsManager::EActivityType::Drop);
        TActivationContext::AsActorContext().Register(new NMetadataManager::TDropActor<TObject>(GetRecords(), GetController(), GetContext()));
    }
public:
    using TBase::TBase;
};

}
