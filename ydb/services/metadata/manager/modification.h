#pragma once
#include "table_record.h"
#include "modification_controller.h"
#include "ydb_value_operator.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/services/metadata/request/request_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadata::NModifications {

template <class TObject>
class TModifyObjectsActor: public NActors::TActorBootstrapped<TModifyObjectsActor<TObject>> {
private:
    using TBase = NActors::TActorBootstrapped<TModifyObjectsActor<TObject>>;
    IModificationObjectsController::TPtr Controller;
    const TString SessionId;
    const TString TransactionId;
    const NACLib::TUserToken SystemUserToken;
    const std::optional<NACLib::TUserToken> UserToken;
protected:
    std::deque<NRequest::TDialogYQLRequest::TRequest> Requests;
    NInternal::TTableRecords Objects;
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const = 0;
    virtual TString GetModifyType() const = 0;

    void BuildRequestDirect() {
        Ydb::Table::ExecuteDataQueryRequest request = BuildModifyQuery();
        request.set_session_id(SessionId);
        request.mutable_tx_control()->set_tx_id(TransactionId);
        Requests.emplace_back(std::move(request));
    }

    void BuildRequestHistory() {
        if (!TObject::GetBehaviour()->GetStorageHistoryTablePath()) {
            return;
        }
        if (UserToken) {
            Objects.AddColumn(NInternal::TYDBColumn::Utf8("historyUserId"), NInternal::TYDBValue::Utf8(UserToken->GetUserSID()));
        }
        Objects.AddColumn(NInternal::TYDBColumn::UInt64("historyInstant"), NInternal::TYDBValue::UInt64(TActivationContext::Now().MicroSeconds()));
        Objects.AddColumn(NInternal::TYDBColumn::Utf8("historyAction"), NInternal::TYDBValue::Utf8(GetModifyType()));
        Ydb::Table::ExecuteDataQueryRequest request = Objects.BuildInsertQuery(TObject::GetBehaviour()->GetStorageHistoryTablePath());
        request.set_session_id(SessionId);
        request.mutable_tx_control()->set_tx_id(TransactionId);
        Requests.emplace_back(std::move(request));
    }

    void Handle(NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>::TPtr& /*ev*/) {
        if (Requests.size()) {
            TBase::Register(new NRequest::TYDBCallbackRequest<NRequest::TDialogYQLRequest>(
                Requests.front(), SystemUserToken, TBase::SelfId()));
            Requests.pop_front();
        } else {
            Controller->OnModificationFinished();
            TBase::PassAway();
        }
    }

    virtual void Handle(NRequest::TEvRequestFailed::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        Controller->OnModificationProblem("cannot execute yql request for " + GetModifyType() +
            " objects: " + ev->Get()->GetErrorMessage());
    }

public:
    TModifyObjectsActor(NInternal::TTableRecords&& objects, const NACLib::TUserToken& systemUserToken, IModificationObjectsController::TPtr controller, const TString& sessionId,
        const TString& transactionId, const std::optional<NACLib::TUserToken>& userToken)
        : Controller(controller)
        , SessionId(sessionId)
        , TransactionId(transactionId)
        , SystemUserToken(systemUserToken)
        , UserToken(userToken)
        , Objects(std::move(objects))

    {
        Y_ABORT_UNLESS(SessionId);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>, Handle);
            hFunc(NRequest::TEvRequestFailed, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        TBase::Become(&TModifyObjectsActor::StateMain);
        BuildRequestDirect();
        BuildRequestHistory();
        Y_ABORT_UNLESS(Requests.size());
        Requests.back().mutable_tx_control()->set_commit_tx(true);

        TBase::Register(new NRequest::TYDBCallbackRequest<NRequest::TDialogYQLRequest>(
            Requests.front(), SystemUserToken, TBase::SelfId()));
        Requests.pop_front();
    }
};

template <class TObject>
class TUpsertObjectsActor: public TModifyObjectsActor<TObject> {
private:
    using TBase = TModifyObjectsActor<TObject>;
protected:
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const override {
        return TBase::Objects.BuildUpsertQuery(TObject::GetBehaviour()->GetStorageTablePath());
    }
    virtual TString GetModifyType() const override {
        return "upsert";
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TUpdateObjectsActor: public TModifyObjectsActor<TObject> {
private:
    using TBase = TModifyObjectsActor<TObject>;
protected:
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const override {
        return TBase::Objects.BuildUpdateQuery(TObject::GetBehaviour()->GetStorageTablePath());
    }
    virtual TString GetModifyType() const override {
        return "update";
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TDeleteObjectsActor: public TModifyObjectsActor<TObject> {
private:
    using TBase = TModifyObjectsActor<TObject>;
protected:
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const override {
        auto manager = TObject::GetBehaviour()->GetOperationsManager();
        Y_ABORT_UNLESS(manager);
        auto objectIds = TBase::Objects.SelectColumns(manager->GetSchema().GetPKColumnIds());
        return objectIds.BuildDeleteQuery(TObject::GetBehaviour()->GetStorageTablePath());
    }
    virtual TString GetModifyType() const override {
        return "delete";
    }
public:
    using TBase::TBase;
};

template <class TObject>
class TInsertObjectsActor: public TModifyObjectsActor<TObject> {
private:
    using TBase = TModifyObjectsActor<TObject>;
    bool ExistingOk = false;
protected:
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const override {
        return TBase::Objects.BuildInsertQuery(TObject::GetBehaviour()->GetStorageTablePath());
    }
    virtual TString GetModifyType() const override {
        return "insert";
    }

    void Handle(NRequest::TEvRequestFailed::TPtr& ev) override {
        if (ev->Get()->GetStatus() == Ydb::StatusIds::PRECONDITION_FAILED && ExistingOk) {
            NRequest::TDialogYQLRequest::TResponse resp;
            this->Send(this->SelfId(), new NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>(std::move(resp)));
            this->Requests.clear(); // Remove history request
            return;
        }
        TBase::Handle(ev);
    }
public:
    TInsertObjectsActor(NInternal::TTableRecords&& objects, const NACLib::TUserToken& systemUserToken, IModificationObjectsController::TPtr controller, const TString& sessionId,
        const TString& transactionId, const std::optional<NACLib::TUserToken>& userToken, bool existingOk)
        : TBase(std::move(objects), systemUserToken, std::move(controller), sessionId, transactionId, userToken)
        , ExistingOk(existingOk)
    {
    }
};

}
