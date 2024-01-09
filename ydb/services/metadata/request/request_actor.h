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

namespace NKikimr::NMetadata::NRequest {

class TEvRequestStart: public NActors::TEventLocal<TEvRequestStart, EEvents::EvRequestStart> {
public:
};

template <class TDialogPolicy>
class TYDBOneRequest: public NActors::TActorBootstrapped<TYDBOneRequest<TDialogPolicy>> {
private:
    using TBase = NActors::TActorBootstrapped<TYDBOneRequest<TDialogPolicy>>;
    using TRequest = typename TDialogPolicy::TRequest;
    using TResponse = typename TDialogPolicy::TResponse;
    TRequest ProtoRequest;
    const NACLib::TUserToken UserToken;
protected:
    class TEvRequestInternalResult: public NActors::TEventLocal<TEvRequestInternalResult, TDialogPolicy::EvResultInternal> {
    private:
        YDB_READONLY_DEF(NThreading::TFuture<typename TDialogPolicy::TResponse>, Future);
    public:
        TEvRequestInternalResult(const NThreading::TFuture<typename TDialogPolicy::TResponse>& f)
            : Future(f) {

        }
    };

    virtual void OnInternalResultError(Ydb::StatusIds::StatusCode status, const TString& errorMessage) = 0;
    virtual void OnInternalResultSuccess(TResponse&& response) = 0;
public:
    void Bootstrap(const TActorContext& /*ctx*/) {
        TBase::Become(&TBase::TThis::StateMain);
        TBase::template Sender<TEvRequestStart>().SendTo(TBase::SelfId());
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestInternalResult, Handle);
            hFunc(TEvRequestStart, Handle);
            default:
                break;
        }
    }
    void Handle(typename TEvRequestInternalResult::TPtr& ev) {
        if (!ev->Get()->GetFuture().HasValue() || ev->Get()->GetFuture().HasException()) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot receive result on initialization";
            OnInternalResultError(Ydb::StatusIds::INTERNAL_ERROR, "cannot receive result from future");
            return;
        }
        auto f = ev->Get()->GetFuture();
        TResponse response = f.ExtractValue();
        if (!TOperatorChecker<TResponse>::IsSuccess(response)) {
            AFL_ERROR(NKikimrServices::METADATA_PROVIDER)("event", "unexpected reply")("error_message", response.DebugString())("request", ProtoRequest.DebugString());
            NYql::TIssues issue;
            NYql::IssuesFromMessage(response.operation().issues(), issue);
            OnInternalResultError(response.operation().status(), issue.ToString());
            return;
        }
        OnInternalResultSuccess(std::move(response));
        TBase::PassAway();
    }

    void Handle(typename TEvRequestStart::TPtr& /*ev*/) {
        auto aSystem = TActivationContext::ActorSystem();
        using TRpcRequest = NGRpcService::TGrpcRequestOperationCall<TRequest, TResponse>;
        auto request = ProtoRequest;
        NACLib::TUserToken uToken("metadata@system", {});
        auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), AppData()->TenantName, uToken.SerializeAsString(), aSystem);
        const NActors::TActorId selfId = TBase::SelfId();
        const auto replyCallback = [aSystem, selfId](const NThreading::TFuture<TResponse>& f) {
            aSystem->Send(selfId, new TEvRequestInternalResult(f));
        };
        result.Subscribe(replyCallback);
    }

    TYDBOneRequest(const TRequest& request, const NACLib::TUserToken& uToken)
        : ProtoRequest(request)
        , UserToken(uToken) {

    }
};

template <class TDialogPolicy>
class TYDBCallbackRequest: public TYDBOneRequest<TDialogPolicy> {
private:
    using TBase = TYDBOneRequest<TDialogPolicy>;
    using TRequest = typename TDialogPolicy::TRequest;
    using TResponse = typename TDialogPolicy::TResponse;
    const NActors::TActorId CallbackActorId;
    const TConfig Config;
    ui32 Retry = 0;
protected:
    virtual void OnInternalResultError(Ydb::StatusIds::StatusCode status, const TString& errorMessage) override {
        TBase::template Sender<TEvRequestFailed>(status, errorMessage).SendTo(CallbackActorId);
        TBase::PassAway();
    }
    virtual void OnInternalResultSuccess(TResponse&& response) override {
        TBase::template Sender<TEvRequestResult<TDialogPolicy>>(std::move(response)).SendTo(CallbackActorId);
        TBase::template Sender<TEvRequestFinished>().SendTo(CallbackActorId);
    }
public:

    TYDBCallbackRequest(const TRequest& request, const NACLib::TUserToken& uToken, const NActors::TActorId actorCallbackId, const TConfig& config = Default<TConfig>())
        : TBase(request, uToken)
        , CallbackActorId(actorCallbackId)
        , Config(config) {

    }
};

template <class TDialogPolicy>
class TSessionedActorImpl: public NActors::TActorBootstrapped<TSessionedActorImpl<TDialogPolicy>> {
private:
    static_assert(!std::is_same<TDialogPolicy, TDialogCreateSession>());
    using TBase = NActors::TActorBootstrapped<TSessionedActorImpl<TDialogPolicy>>;

    ui32 Retry = 0;
    void Handle(TEvRequestResult<TDialogCreateSession>::TPtr& ev) {
        Ydb::Table::CreateSessionResponse currentFullReply = ev->Get()->GetResult();
        Ydb::Table::CreateSessionResult session;
        currentFullReply.operation().result().UnpackTo(&session);
        const TString sessionId = session.session_id();
        Y_ABORT_UNLESS(sessionId);
        std::optional<typename TDialogPolicy::TRequest> nextRequest = OnSessionId(sessionId);
        Y_ABORT_UNLESS(nextRequest);
        TBase::Register(new TYDBCallbackRequest<TDialogPolicy>(*nextRequest, UserToken, TBase::SelfId(), Config));
    }
protected:
    const TConfig Config;
    const NACLib::TUserToken UserToken;
    virtual std::optional<typename TDialogPolicy::TRequest> OnSessionId(const TString& sessionId) = 0;
    virtual void OnResult(const typename TDialogPolicy::TResponse& response) = 0;
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestResult<TDialogCreateSession>, Handle);
            hFunc(TEvRequestFailed, Handle);
            hFunc(TEvRequestStart, Handle);
            hFunc(TEvRequestResult<TDialogPolicy>, Handle);
            default:
                break;
        }
    }

    TSessionedActorImpl(const TConfig& config, const NACLib::TUserToken& uToken)
        : Config(config)
        , UserToken(uToken)
    {

    }

    void Handle(typename TEvRequestResult<TDialogPolicy>::TPtr& ev) {
        OnResult(ev->Get()->GetResult());
        TBase::PassAway();
    }

    void Handle(typename TEvRequestFailed::TPtr& /*ev*/) {
        TBase::Schedule(Config.GetRetryPeriod(++Retry), new TEvRequestStart);
    }

    void Handle(typename TEvRequestFinished::TPtr& /*ev*/) {
        Retry = 0;
    }

    void Handle(typename TEvRequestStart::TPtr& /*ev*/) {
        TBase::Register(new TYDBCallbackRequest<TDialogCreateSession>(TDialogCreateSession::TRequest(), UserToken, TBase::SelfId(), Config));
    }

    void Bootstrap() {
        TBase::Become(&TSessionedActorImpl::StateMain);
        TBase::template Sender<TEvRequestStart>().SendTo(TBase::SelfId());
    }
};

using TSessionedActor = TSessionedActorImpl<TDialogYQLRequest>;

}
