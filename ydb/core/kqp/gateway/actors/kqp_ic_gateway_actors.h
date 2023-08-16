#pragma once

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>


namespace NKikimr::NKqp {

template<typename TDerived, typename TRequest, typename TResponse, typename TResult>
class TRequestHandlerBase: public TActorBootstrapped<TDerived> {
public:
    using TCallbackFunc = std::function<void(NThreading::TPromise<TResult>, TResponse&&)>;
    using TBase = TRequestHandlerBase<TDerived, TRequest, TResponse, TResult>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_REQUEST_HANDLER;
    }

    TRequestHandlerBase(TRequest* request, NThreading::TPromise<TResult> promise, TCallbackFunc callback)
        : Request(request)
        , Promise(promise)
        , Callback(callback) {}

    void HandleError(const TString &error, const TActorContext &ctx) {
        Promise.SetValue(NYql::NCommon::ResultFromError<TResult>(error));
        this->Die(ctx);
    }

    virtual void HandleResponse(typename TResponse::TPtr &ev, const TActorContext &ctx) {
        Callback(Promise, std::move(*ev->Get()));
        this->Die(ctx);
    }

    void HandleUnexpectedEvent(const TString& requestType, ui32 eventType) {
        ALOG_CRIT(NKikimrServices::KQP_GATEWAY, "TRequestHandlerBase, unexpected event, request type: "
            << requestType << ", event type: " << eventType);

        Promise.SetValue(NYql::NCommon::ResultFromError<TResult>(YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, TStringBuilder()
            << "Unexpected event in " << requestType << ": " << eventType)));
        this->PassAway();
    }

    void Handle(NKikimr::TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Tablet not available, status: " << (ui32)ev->Get()->Status, {}));
            this->Die(ctx);
        }
    }

    void Handle(NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            "Connection to tablet was lost.", {}));
        this->Die(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            "Failed to deliver request to destination.", {}));
        this->Die(ctx);
    }

protected:
    THolder<TRequest> Request;
    NThreading::TPromise<TResult> Promise;
    TCallbackFunc Callback;
};

template<typename TRequest, typename TResponse, typename TResult>
class TActorRequestHandler: public TRequestHandlerBase<
    TActorRequestHandler<TRequest, TResponse, TResult>,
    TRequest,
    TResponse,
    TResult>
{
public:
    using TBase = typename TActorRequestHandler::TBase;
    using TCallbackFunc = typename TBase::TCallbackFunc;

    TActorRequestHandler(TActorId actorId, TRequest* request, NThreading::TPromise<TResult> promise, TCallbackFunc callback)
        : TBase(request, promise, callback)
        , ActorId(actorId) {}

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(ActorId, this->Request.Release(), IEventHandle::FlagTrackDelivery);

        this->Become(&TActorRequestHandler::AwaitState);
    }

    using TBase::Handle;
    using TBase::HandleResponse;

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvents::TEvUndelivered, Handle);
        default:
            TBase::HandleUnexpectedEvent("TActorRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    TActorId ActorId;
};

struct TDescribeSecretsResponse {
    TDescribeSecretsResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    TDescribeSecretsResponse(const TVector<TString>& secretValues)
        : SecretValues(secretValues)
        , Status(Ydb::StatusIds::SUCCESS)
    {}

    TVector<TString> SecretValues;
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

class TDescribeSecretsActor: public NActors::TActorBootstrapped<TDescribeSecretsActor> {
    STRICT_STFUNC(StateFunc,
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
    )

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvUnsubscribeExternal(GetSecretsSnapshotParser()));
        auto snapshot = ev->Get()->GetSnapshotAs<NMetadata::NSecret::TSnapshot>();

        TVector<TString> secretValues;
        secretValues.reserve(SecretIds.size());
        for (const auto& secretId: SecretIds) {
            TString secretValue;
            const bool isFound = snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(secretId), secretValue);
            if (!isFound) {
                Promise.SetValue(TDescribeSecretsResponse(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret with name '" + secretId.GetSecretId() + "' not found") }));
                PassAway();
                return;
            }
            secretValues.push_back(secretValue);
        }
        Promise.SetValue(TDescribeSecretsResponse(secretValues));
        PassAway();
    }

    NMetadata::NFetcher::ISnapshotsFetcher::TPtr GetSecretsSnapshotParser() {
        return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
    }

public:
    TDescribeSecretsActor(const TString& ownerUserId, const TVector<TString>& secretIds, NThreading::TPromise<TDescribeSecretsResponse> promise) 
        : SecretIds(CreateSecretIds(ownerUserId, secretIds))
        , Promise(promise)
    {}

    void Bootstrap() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            Promise.SetValue(TDescribeSecretsResponse(Ydb::StatusIds::INTERNAL_ERROR, { NYql::TIssue("metadata service is not active") }));
            PassAway();
            return;
        }
        
        this->Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvSubscribeExternal(GetSecretsSnapshotParser()));
        Become(&TDescribeSecretsActor::StateFunc);
    }

private:
    static TVector<NMetadata::NSecret::TSecretId> CreateSecretIds(const TString& ownerUserId, const TVector<TString>& secretIds) {
        TVector<NMetadata::NSecret::TSecretId> result;
        for (const auto& secretId: secretIds) {
            result.emplace_back(ownerUserId, secretId);
        }
        return result;
    }

private:
    const TVector<NMetadata::NSecret::TSecretId> SecretIds;
    NThreading::TPromise<TDescribeSecretsResponse> Promise;
};

}
