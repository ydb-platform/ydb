#pragma once
#include "alter_impl.h"
#include "modification_controller.h"
#include "preparation_controller.h"
#include "restore.h"
#include "modification.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadata::NModifications {

template <class TObject>
class TUpdateObjectActor: public TModificationActor<TObject> {
private:
    using TBase = TModificationActor<TObject>;
protected:
    virtual bool ProcessPreparedObjects(NInternal::TTableRecords&& records) const override {
        TBase::Register(new TUpdateObjectsActor<TObject>(std::move(records), TBase::UserToken,
            TBase::InternalController, TBase::SessionId, TBase::TransactionId, TBase::Context.GetExternalData().GetUserToken()));
        return true;
    }

    virtual TString GetModificationType() const override {
        return "ALTER";
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TUpsertObjectActor: public TModificationActor<TObject> {
private:
    using TBase = TModificationActor<TObject>;
protected:
    virtual bool ProcessPreparedObjects(NInternal::TTableRecords&& records) const override {
        TBase::Register(new TUpsertObjectsActor<TObject>(std::move(records), TBase::UserToken,
            TBase::InternalController, TBase::SessionId, TBase::TransactionId,
            TBase::Context.GetExternalData().GetUserToken()));
        return true;
    }

    virtual TString GetModificationType() const override {
        return "UPSERT";
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TCreateObjectActor: public TModificationActor<TObject> {
private:
    using TBase = TModificationActor<TObject>;
    bool ExistingOk = false;
protected:
    virtual bool ProcessPreparedObjects(NInternal::TTableRecords&& records) const override {
        TBase::Register(new TInsertObjectsActor<TObject>(std::move(records), TBase::UserToken,
            TBase::InternalController, TBase::SessionId, TBase::TransactionId,
            TBase::Context.GetExternalData().GetUserToken(), ExistingOk));
        return true;
    }

    virtual TString GetModificationType() const override {
        return "CREATE";
    }
public:
    using TBase::TBase;

    void SetExistingOk(bool existingOk) {
        ExistingOk = existingOk;
    }
};

template <class TObject>
class TDeleteObjectActor: public TModificationActor<TObject> {
private:
    using TBase = TModificationActor<TObject>;
protected:
    using TBase::Manager;
    virtual bool BuildRestoreObjectIds() override {
        auto& first = TBase::Patches.front();
        std::vector<Ydb::Column> columns = first.SelectOwnedColumns(Manager->GetSchema().GetPKColumns());
        if (!columns.size()) {
            TBase::ExternalController->OnAlteringProblem("no pk columns in patch");
            return false;
        }
        if (columns.size() != Manager->GetSchema().GetPKColumns().size()) {
            TBase::ExternalController->OnAlteringProblem("no columns for pk detection");
            return false;
        }
        TBase::RestoreObjectIds.InitColumns(columns);
        for (auto&& i : TBase::Patches) {
            if (!TBase::RestoreObjectIds.AddRecordNativeValues(i)) {
                TBase::ExternalController->OnAlteringProblem("incorrect pk columns");
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

    virtual bool ProcessPreparedObjects(NInternal::TTableRecords&& records) const override {
        TBase::Register(new TDeleteObjectsActor<TObject>(std::move(records), TBase::UserToken,
            TBase::InternalController, TBase::SessionId, TBase::TransactionId, TBase::Context.GetExternalData().GetUserToken()));
        return true;
    }

    virtual bool PrepareRestoredObjects(std::vector<TObject>& /*objects*/) const override {
        return true;
    }

};

template <class TObject>
class TUpsertObjectCommand: public IObjectModificationCommand {
private:
    using TBase = IObjectModificationCommand;
protected:
    virtual void DoExecute() const override {
        typename IObjectOperationsManager<TObject>::TPtr manager = TBase::GetOperationsManagerFor<TObject>();
        TActivationContext::AsActorContext().Register(new TUpsertObjectActor<TObject>(GetRecords(), GetController(), manager, GetContext()));
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TCreateObjectCommand: public IObjectModificationCommand {
private:
    using TBase = IObjectModificationCommand;
    bool ExistingOk = false;
protected:
    virtual void DoExecute() const override {
        typename IObjectOperationsManager<TObject>::TPtr manager = TBase::GetOperationsManagerFor<TObject>();
        auto* actor = new TCreateObjectActor<TObject>(GetRecords(), GetController(), manager, GetContext());
        actor->SetExistingOk(ExistingOk);
        TActivationContext::AsActorContext().Register(actor);
    }
public:
    TCreateObjectCommand(const NInternal::TTableRecord& record,
        IClassBehaviour::TPtr behaviour,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TInternalModificationContext& context,
        bool existingOk)
        : TBase(record, behaviour, controller, context)
        , ExistingOk(existingOk)
    {
    }
};

template <class TObject>
class TUpdateObjectCommand: public IObjectModificationCommand {
private:
    using TBase = IObjectModificationCommand;
protected:
    virtual void DoExecute() const override {
        typename IObjectOperationsManager<TObject>::TPtr manager = TBase::GetOperationsManagerFor<TObject>();
        TActivationContext::AsActorContext().Register(new TUpdateObjectActor<TObject>(GetRecords(), GetController(), manager, GetContext()));
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TDeleteObjectCommand: public IObjectModificationCommand {
private:
    using TBase = IObjectModificationCommand;
    bool MissingOk = false;
protected:
    virtual void DoExecute() const override {
        typename IObjectOperationsManager<TObject>::TPtr manager = TBase::GetOperationsManagerFor<TObject>();
        TActivationContext::AsActorContext().Register(new TDeleteObjectActor<TObject>(GetRecords(), GetController(), manager, GetContext()));
    }
public:
    TDeleteObjectCommand(const NInternal::TTableRecord& record,
        IClassBehaviour::TPtr behaviour,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TInternalModificationContext& context,
        bool missingOk)
        : TBase(record, behaviour, controller, context)
        , MissingOk(missingOk)
    {
    }
};

}
