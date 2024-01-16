#pragma once
#include "common.h"
#include "config.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NMetadata::NRequest {

template <class TDialogPolicy>
class TYDBOneRequestSender {
private:
    using TRequest = typename TDialogPolicy::TRequest;
    using TResponse = typename TDialogPolicy::TResponse;
    TRequest ProtoRequest;
    const NACLib::TUserToken UserToken;
    typename IExternalController<TDialogPolicy>::TPtr ExternalController;

    static void OnInternalResult(const NThreading::TFuture<TResponse>& f, typename IExternalController<TDialogPolicy>::TPtr externalController) {
        if (!f.HasValue() || f.HasException()) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot receive result on initialization";
            externalController->OnRequestFailed(Ydb::StatusIds::INTERNAL_ERROR, "cannot receive result from future");
            return;
        }
        TResponse response = f.GetValue();
        if (!TOperatorChecker<TResponse>::IsSuccess(response)) {
            AFL_ERROR(NKikimrServices::METADATA_PROVIDER)("event", "unexpected reply")("response", response.DebugString());
            NYql::TIssues issues;
            NYql::IssuesFromMessage(response.operation().issues(), issues);
            externalController->OnRequestFailed(response.operation().status(), issues.ToString());
            return;
        }
        externalController->OnRequestResult(std::move(response));
    }
public:
    void Start() const {
        auto request = ProtoRequest;
        using TRpcRequest = NGRpcService::TGrpcRequestOperationCall<TRequest, TResponse>;
        auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), AppData()->TenantName, UserToken.SerializeAsString(), TActivationContext::ActorSystem());
        auto extController = ExternalController;
        const auto replyCallback = [extController](const NThreading::TFuture<TResponse>& f) {
            TYDBOneRequestSender<TDialogPolicy>::OnInternalResult(f, extController);
        };
        result.Subscribe(replyCallback);
    }
    TYDBOneRequestSender(const TRequest& request, const NACLib::TUserToken& uToken, typename IExternalController<TDialogPolicy>::TPtr externalController)
        : ProtoRequest(request)
        , UserToken(uToken)
        , ExternalController(externalController) {

    }
};

template <class TCurrentDialogPolicy, class TNextController>
class IChainController: public IExternalController<TCurrentDialogPolicy> {
private:
    std::shared_ptr<TNextController> NextController;
    const NACLib::TUserToken UserToken;
protected:
    TConclusion<typename TNextController::TDialogPolicy::TRequest> BuildNextRequest(typename TCurrentDialogPolicy::TResponse&& result) const {
        return DoBuildNextRequest(std::move(result));
    }

    virtual TConclusion<typename TNextController::TDialogPolicy::TRequest> DoBuildNextRequest(typename TCurrentDialogPolicy::TResponse&& result) const = 0;
public:
    using TDialogPolicy = TCurrentDialogPolicy;
    virtual void OnRequestResult(typename TCurrentDialogPolicy::TResponse&& result) override {
        TConclusion<typename TNextController::TDialogPolicy::TRequest> nextRequest = BuildNextRequest(std::move(result));
        if (!nextRequest) {
            OnRequestFailed(nextRequest.GetStatus(), nextRequest.GetErrorMessage());
        } else {
            TYDBOneRequestSender<typename TNextController::TDialogPolicy> req(*nextRequest, UserToken, NextController);
            req.Start();
        }
    }
    virtual void OnRequestFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage) override final {
        NextController->OnRequestFailed(status, errorMessage);
    }
    IChainController(const NACLib::TUserToken& userToken, std::shared_ptr<TNextController> nextController)
        : NextController(nextController)
        , UserToken(userToken)
    {

    }
};

class TSessionContext {
    TString SessionId;
public:
    using TPtr = std::shared_ptr<TSessionContext>;

    void SetSessionId(const TString& sessionId) {
        SessionId = sessionId;
    }

    TString GetSessionId() const {
        return SessionId;
    }
};

template <class TDialogPolicy>
class TSessionedChainController: public IChainController<TDialogCreateSession, IExternalController<TDialogPolicy>> {
private:
    using TRequest = typename TDialogPolicy::TRequest;
    using TBase = IChainController<TDialogCreateSession, IExternalController<TDialogPolicy>>;
    TRequest ProtoRequest;
    TSessionContext::TPtr SessionContext;
protected:
    virtual TConclusion<typename TDialogPolicy::TRequest> DoBuildNextRequest(TDialogCreateSession::TResponse&& response) const override {
        auto result = ProtoRequest;
        Ydb::Table::CreateSessionResponse currentFullReply = std::move(response);
        Ydb::Table::CreateSessionResult session;
        currentFullReply.operation().result().UnpackTo(&session);
        const TString sessionId = session.session_id();
        if (!sessionId) {
            return TConclusionStatus::Fail("cannot build session for request");
        }
        result.set_session_id(sessionId);
        SessionContext->SetSessionId(sessionId);
        return result;
    }
public:
    TSessionedChainController(const TRequest& request, const NACLib::TUserToken& uToken, typename IExternalController<TDialogPolicy>::TPtr externalController, TSessionContext::TPtr sessionContext)
        : TBase(uToken, externalController)
        , ProtoRequest(request)
        , SessionContext(sessionContext) {
        Y_ABORT_UNLESS(SessionContext);
    }
};

template <class TDialogPolicy>
class TNaiveExternalController: public IExternalController<TDialogPolicy> {
private:
    const NActors::TActorIdentity ActorId;
public:
    TNaiveExternalController(const NActors::TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void OnRequestResult(typename TDialogPolicy::TResponse&& result) override {
        ActorId.Send(ActorId, new TEvRequestResult<TDialogPolicy>(std::move(result)));
        ActorId.Send(ActorId, new TEvRequestFinished);
    }
    virtual void OnRequestFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvRequestFailed(status, errorMessage));
    }
};

class TSessionDeleteResponseController: public IExternalController<NMetadata::NRequest::TDialogDeleteSession> {
private:
    TSessionContext::TPtr SessionContext;
public:
    TSessionDeleteResponseController(TSessionContext::TPtr sessionContext)
        : SessionContext(sessionContext) {
    }

    virtual void OnRequestResult(typename TDialogPolicy::TResponse&&) override {
    }

    virtual void OnRequestFailed(Ydb::StatusIds::StatusCode /*status*/, const TString& errorMessage) override {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot close session with id: " << SessionContext->GetSessionId() << ", reason: " << errorMessage;
    }
};

template <class TDialogPolicy>
class TSessionDeleteController: public IExternalController<TDialogPolicy> {
private:
    TSessionContext::TPtr SessionContext;
    typename IExternalController<TDialogPolicy>::TPtr ExternalController;
    const NACLib::TUserToken UserToken;

    void CloseSession() const {
        auto sessionId = SessionContext->GetSessionId();
        if (sessionId) {
            auto deleteRequest = NMetadata::NRequest::TDialogDeleteSession::TRequest();
            deleteRequest.set_session_id(sessionId);

            auto sessionDeleteResponseController = std::make_shared<NMetadata::NRequest::TSessionDeleteResponseController>(SessionContext);
            TYDBOneRequestSender<NMetadata::NRequest::TDialogDeleteSession> request(deleteRequest, UserToken, sessionDeleteResponseController);
            request.Start();
        }
    }

public:
    TSessionDeleteController(TSessionContext::TPtr sessionContext, const NACLib::TUserToken& userToken, typename IExternalController<TDialogPolicy>::TPtr externalController)
        : SessionContext(sessionContext)
        , ExternalController(externalController)
        , UserToken(userToken) {
    }

    virtual void OnRequestResult(typename TDialogPolicy::TResponse&& result) override {
        ExternalController->OnRequestResult(std::move(result));
        CloseSession();
    }

    virtual void OnRequestFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage) override {
        ExternalController->OnRequestFailed(status, errorMessage);
        CloseSession();
    }
};

class TYQLRequestExecutor {
private:
    static TDialogYQLRequest::TRequest BuildRequest(const TString& request, const bool readOnly) {
        TDialogYQLRequest::TRequest pRequest;
        pRequest.mutable_query()->set_yql_text(request);
        if (readOnly) {
            pRequest.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
        } else {
            pRequest.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            pRequest.mutable_tx_control()->set_commit_tx(true);
        }
        return pRequest;
    }
public:
    static void Execute(TDialogYQLRequest::TRequest&& request, const NACLib::TUserToken& uToken, IExternalController<TDialogYQLRequest>::TPtr controller) {
        auto sessionContext = std::make_shared<TSessionContext>();
        auto sessionDeleteController = std::make_shared<NMetadata::NRequest::TSessionDeleteController<NMetadata::NRequest::TDialogYQLRequest>>(sessionContext, uToken, controller);
        auto sessionController = std::make_shared<NMetadata::NRequest::TSessionedChainController<NMetadata::NRequest::TDialogYQLRequest>>
            (std::move(request), uToken, sessionDeleteController, sessionContext);
        NMetadata::NRequest::TYDBOneRequestSender<NMetadata::NRequest::TDialogCreateSession> ydbReq(NMetadata::NRequest::TDialogCreateSession::TRequest(),
            uToken, sessionController);
        ydbReq.Start();
    }
    static void Execute(const TString& request, const NACLib::TUserToken& uToken, IExternalController<TDialogYQLRequest>::TPtr controller, const bool readOnly) {
        Execute(BuildRequest(request, readOnly), uToken, controller);
    }
};

}
