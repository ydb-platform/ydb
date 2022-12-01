#pragma once
#include "table_record.h"
#include "modification_controller.h"
#include "ydb_value_operator.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/services/metadata/request/request_actor.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadataManager {

template <class TObject>
class TModifyObjectsActor: public NActors::TActorBootstrapped<TModifyObjectsActor<TObject>> {
private:
    using TBase = NActors::TActorBootstrapped<TModifyObjectsActor<TObject>>;
    IModificationObjectsController::TPtr Controller;
    const TString SessionId;
    const TString TransactionId;
    const NACLib::TUserToken SystemUserToken;
    const std::optional<NACLib::TUserToken> UserToken;
    std::deque<NInternal::NRequest::TDialogYQLRequest::TRequest> Requests;
protected:
    TTableRecords Objects;
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const = 0;
    virtual TString GetModifyType() const = 0;

    void BuildRequestDirect() {
        Ydb::Table::ExecuteDataQueryRequest request = BuildModifyQuery();
        request.set_session_id(SessionId);
        request.mutable_tx_control()->set_tx_id(TransactionId);
        Requests.emplace_back(std::move(request));
    }

    void BuildRequestHistory() {
        if (!TObject::GetStorageHistoryTablePath()) {
            return;
        }
        if (UserToken) {
            Objects.AddColumn(TYDBColumn::Bytes("historyUserId"), TYDBValue::Bytes(UserToken->GetUserSID()));
        }
        Objects.AddColumn(TYDBColumn::UInt64("historyInstant"), TYDBValue::UInt64(TActivationContext::Now().MicroSeconds()));
        Objects.AddColumn(TYDBColumn::Bytes("historyAction"), TYDBValue::Bytes(GetModifyType()));
        Ydb::Table::ExecuteDataQueryRequest request = Objects.BuildInsertQuery(TObject::GetStorageHistoryTablePath());
        request.set_session_id(SessionId);
        request.mutable_tx_control()->set_tx_id(TransactionId);
        Requests.emplace_back(std::move(request));
    }

    void Handle(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogYQLRequest>::TPtr& /*ev*/) {
        if (Requests.size()) {
            TBase::Register(new NInternal::NRequest::TYDBRequest<NInternal::NRequest::TDialogYQLRequest>(
                Requests.front(), SystemUserToken, TBase::SelfId()));
            Requests.pop_front();
        } else {
            Controller->ModificationFinished();
            TBase::PassAway();
        }
    }

    void Handle(NInternal::NRequest::TEvRequestFailed::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        Controller->ModificationProblem("cannot execute yql request for " + GetModifyType() +
            " objects: " + ev->Get()->GetErrorMessage());
    }

public:
    TModifyObjectsActor(TTableRecords&& objects, const NACLib::TUserToken& systemUserToken, IModificationObjectsController::TPtr controller, const TString& sessionId,
        const TString& transactionId, const std::optional<NACLib::TUserToken>& userToken)
        : Controller(controller)
        , SessionId(sessionId)
        , TransactionId(transactionId)
        , SystemUserToken(systemUserToken)
        , UserToken(userToken)
        , Objects(std::move(objects))

    {
        Y_VERIFY(SessionId);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogYQLRequest>, Handle);
            hFunc(NInternal::NRequest::TEvRequestFailed, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        TBase::Become(&TModifyObjectsActor::StateMain);
        BuildRequestDirect();
        BuildRequestHistory();
        Y_VERIFY(Requests.size());
        Requests.back().mutable_tx_control()->set_commit_tx(true);

        TBase::Register(new NInternal::NRequest::TYDBRequest<NInternal::NRequest::TDialogYQLRequest>(
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
        return TBase::Objects.BuildUpsertQuery(TObject::GetStorageTablePath());
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
        return TBase::Objects.BuildUpdateQuery(TObject::GetStorageTablePath());
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
        auto objectIds = TBase::Objects.SelectColumns(TObject::TDecoder::GetPKColumnIds());
        return objectIds.BuildDeleteQuery(TObject::GetStorageTablePath());
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
protected:
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const override {
        return TBase::Objects.BuildInsertQuery(TObject::GetStorageTablePath());
    }
    virtual TString GetModifyType() const override {
        return "insert";
    }
public:
    using TBase::TBase;
};

}
