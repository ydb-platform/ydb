#pragma once
#include "table_record.h"
#include "modification_controller.h"
#include "ydb_value_operator.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/services/metadata/request/request_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadata::NModifications {

class TModificationStage {
private:
    YDB_ACCESSOR_DEF(Ydb::Table::ExecuteDataQueryRequest, Request);

public:
    using TPtr = std::shared_ptr<TModificationStage>;

    virtual TConclusionStatus HandleResult(const Ydb::Table::ExecuteQueryResult& /*result*/) const {
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus HandleError(const NRequest::TEvRequestFailed& ev) const {
        return TConclusionStatus::Fail(ev.GetErrorMessage());
    }

    void SetCommit() {
        Request.mutable_tx_control()->set_commit_tx(true);
    }

    TModificationStage(Ydb::Table::ExecuteDataQueryRequest request)
        : Request(std::move(request)) {
    }

    virtual ~TModificationStage() = default;
};

template <class TObject>
class TModifyObjectsActor: public NActors::TActorBootstrapped<TModifyObjectsActor<TObject>> {
private:
    using TBase = NActors::TActorBootstrapped<TModifyObjectsActor<TObject>>;
    IModificationObjectsController::TPtr Controller;
    const TString SessionId;
    const TString TransactionId;
    const NACLib::TUserToken SystemUserToken;
    const std::optional<NACLib::TUserToken> UserToken;

    void FillRequestSettings(Ydb::Table::ExecuteDataQueryRequest& request) {
        request.set_session_id(SessionId);
        request.mutable_tx_control()->set_tx_id(TransactionId);
    }

    void AdvanceStage() {
        AFL_VERIFY(!Stages.empty());
        Stages.pop_front();
        if (Stages.size()) {
            TBase::Register(
                new NRequest::TYDBCallbackRequest<NRequest::TDialogYQLRequest>(Stages.front()->GetRequest(), SystemUserToken, TBase::SelfId()));
        } else {
            Controller->OnModificationFinished();
            TBase::PassAway();
        }
    }

protected:
    std::deque<TModificationStage::TPtr> Stages;
    NInternal::TTableRecords Objects;
    virtual Ydb::Table::ExecuteDataQueryRequest BuildModifyQuery() const = 0;
    virtual TString GetModifyType() const = 0;
    virtual TModificationStage::TPtr DoBuildRequestDirect(Ydb::Table::ExecuteDataQueryRequest query) const {
        return std::make_shared<TModificationStage>(std::move(query));
    }

    void BuildPreconditionStages(const std::vector<TModificationStage::TPtr>& stages) {
        for (auto&& stage : stages) {
            FillRequestSettings(stage->MutableRequest());
            Stages.emplace_back(std::move(stage));
        }
    }

    void BuildRequestDirect() {
        Ydb::Table::ExecuteDataQueryRequest request = BuildModifyQuery();
        FillRequestSettings(request);
        Stages.emplace_back(DoBuildRequestDirect(request));
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
        FillRequestSettings(request);
        Stages.emplace_back(std::make_shared<TModificationStage>(std::move(request)));
    }

    void Handle(NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>::TPtr& ev) {
        const auto& operation = ev->Get()->GetResult().operation();
        AFL_VERIFY(operation.ready());

        Ydb::Table::ExecuteQueryResult result;
        operation.result().UnpackTo(&result);

        if (auto status = Stages.front()->HandleResult(result); status.IsFail()) {
            Controller->OnModificationProblem(status.GetErrorMessage());
            TBase::PassAway();
            return;
        }

        AdvanceStage();
    }

    void Handle(NRequest::TEvRequestFailed::TPtr& ev) {
        auto g = TBase::PassAwayGuard();
        if (auto status = Stages.front()->HandleError(*ev->Get()); status.IsFail()) {
            Controller->OnModificationProblem(status.GetErrorMessage());
        } else {
            Controller->OnModificationFinished();
        }
    }

public:
    TModifyObjectsActor(NInternal::TTableRecords&& objects, const NACLib::TUserToken& systemUserToken,
        IModificationObjectsController::TPtr controller, const TString& sessionId, const TString& transactionId,
        const std::optional<NACLib::TUserToken>& userToken, const std::vector<TModificationStage::TPtr>& preconditions)
        : Controller(controller)
        , SessionId(sessionId)
        , TransactionId(transactionId)
        , SystemUserToken(systemUserToken)
        , UserToken(userToken)
        , Objects(std::move(objects))

    {
        BuildPreconditionStages(preconditions);
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
        Y_ABORT_UNLESS(Stages.size());
        Stages.back()->SetCommit();

        TBase::Register(new NRequest::TYDBCallbackRequest<NRequest::TDialogYQLRequest>(Stages.front()->GetRequest(), SystemUserToken, TBase::SelfId()));
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

class TStageInsertObjects: public NModifications::TModificationStage {
private:
    const bool ExistingOk;

public:
    TConclusionStatus HandleError(const NRequest::TEvRequestFailed& ev) const override {
        if (ExistingOk && ev.GetStatus() == Ydb::StatusIds::PRECONDITION_FAILED) {
            return TConclusionStatus::Success();
        }
        return TConclusionStatus::Fail(ev.GetErrorMessage());
    }

    TStageInsertObjects(Ydb::Table::ExecuteDataQueryRequest request, const bool existingOk)
        : TModificationStage(std::move(request)), ExistingOk(existingOk) {
    }
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

    TModificationStage::TPtr DoBuildRequestDirect(Ydb::Table::ExecuteDataQueryRequest query) const override {
        return std::make_shared<TStageInsertObjects>(std::move(query), ExistingOk);
    }
public:
    TInsertObjectsActor(NInternal::TTableRecords&& objects, const NACLib::TUserToken& systemUserToken, IModificationObjectsController::TPtr controller, const TString& sessionId,
        const TString& transactionId, const std::optional<NACLib::TUserToken>& userToken, const std::vector<TModificationStage::TPtr>& preconditions, bool existingOk)
        : TBase(std::move(objects), systemUserToken, std::move(controller), sessionId, transactionId, userToken, preconditions)
        , ExistingOk(existingOk)
    {
    }
};

}
