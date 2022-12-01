#pragma once
#include "alter_impl.h"
#include "modification_controller.h"
#include "preparation_controller.h"
#include "restore.h"
#include "modification.h"

#include <ydb/services/metadata/abstract/manager.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadataManager {

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
        if (columns.size() != TObject::TDecoder::GetPKColumns().size()) {
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
