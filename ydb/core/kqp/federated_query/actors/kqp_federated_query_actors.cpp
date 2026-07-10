#include "kqp_federated_query_actors.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/services/scheme_secret/resolver.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/impl/access_service.h>

#include <util/stream/file.h>

namespace NKikimr::NKqp {

void RegisterDescribeSecretsActor(
    const NActors::TActorId& replyActorId,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    const std::vector<TString>& secretIds,
    NActors::TActorSystem* actorSystem
) {
    TVector<TString> secretNames{secretIds.begin(), secretIds.end()};
    auto future = NSecret::DescribeSecret(secretNames, userToken, database, actorSystem);
    future.Subscribe([actorSystem, replyActorId](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& result){
        actorSystem->Send(replyActorId, new TEvDescribeSecretsResponse(result.GetValue()));
    });
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(
    const NKikimrSchemeOp::TAuth& authDescription,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    NActors::TActorSystem* actorSystem
) {
    switch (authDescription.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount: {
            const TString& saSecretId = authDescription.GetServiceAccount().GetSecretName();
            return NSecret::DescribeSecret({saSecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kNone:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription({}));

        case NKikimrSchemeOp::TAuth::kBasic: {
            const TString& passwordSecretId = authDescription.GetBasic().GetPasswordSecretName();
            return NSecret::DescribeSecret({passwordSecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kMdbBasic: {
            const TString& saSecretId = authDescription.GetMdbBasic().GetServiceAccountSecretName();
            const TString& passwordSecreId = authDescription.GetMdbBasic().GetPasswordSecretName();
            return NSecret::DescribeSecret({saSecretId, passwordSecreId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kAws: {
            const TString& awsAccessKeyIdSecretId = authDescription.GetAws().GetAwsAccessKeyIdSecretName();
            const TString& awsAccessKeyKeySecretId = authDescription.GetAws().GetAwsSecretAccessKeySecretName();
            return NSecret::DescribeSecret({awsAccessKeyIdSecretId, awsAccessKeyKeySecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kToken: {
            const TString& tokenSecretId = authDescription.GetToken().GetTokenSecretName();
            return NSecret::DescribeSecret({tokenSecretId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::kIam: {
            if (authDescription.GetIam().HasResourceId()) {
                return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription({}));
            }
            const TString& initialTokenId = authDescription.GetIam().GetInitialTokenSecretName();
            return NSecret::DescribeSecret({initialTokenId}, userToken, database, actorSystem);
        }

        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("identity case is not specified") }));
    }
}

namespace {
// XXX begin duplicated code from replication/util.h
inline auto DefaultRetryableErrors() {
    using EStatus = NYdb::EStatus;
    return TVector<EStatus>{
        EStatus::ABORTED,
        EStatus::UNAVAILABLE,
        EStatus::OVERLOADED,
        EStatus::TIMEOUT,
        EStatus::BAD_SESSION,
        EStatus::SESSION_EXPIRED,
        EStatus::CANCELLED,
        EStatus::UNDETERMINED,
        EStatus::SESSION_BUSY,
        EStatus::CLIENT_DISCOVERY_FAILED,
        EStatus::CLIENT_LIMITS_REACHED,
    };
}

inline bool IsRetryableError(const NYdb::TStatus status, const TVector<NYdb::EStatus>& retryable) {
    switch (status.GetStatus()) {
    case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
    case NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED:
        return false;
    case NYdb::EStatus::TRANSPORT_UNAVAILABLE:
        for (const auto& issue : status.GetIssues()) {
            if (issue.GetMessage().contains("Misformatted domain name") || issue.GetMessage().contains("Domain name not found")) {
                return false;
            }
        }
        return true;
    default:
        return status.IsTransportError() || Find(retryable, status.GetStatus()) != retryable.end();
    }
}

inline bool IsRetryableError(const NYdb::TStatus status) {
    static auto defaultRetryableErrors = DefaultRetryableErrors();
    return IsRetryableError(status, defaultRetryableErrors);
}
// XXX end duplicated code

class TDescribeResourceIdService : public NActors::TActor<TDescribeResourceIdService> {
public:
    using TBase = NActors::TActor<TDescribeResourceIdService>;

    enum EDescribeResourceEvents {
        EvDescribeResourceId = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvEnd,
    };

    struct TState {
        TString Endpoint;
        TString Database;
        bool Ssl;
        TString CaCert;
        TString Token;
        NThreading::TPromise<TEvDescribeResourceIdResponse::TDescription> Promise;
        TBackoff Backoff = TBackoff(/*maxRetries=*/10, /*initialDelay=*/TDuration::MilliSeconds(100), /*maxDelay=*/TDuration::Seconds(10));
    };

    struct TEvDescribeResourceId : public NActors::TEventLocal<TEvDescribeResourceId, EvDescribeResourceId> {
    public:
        TEvDescribeResourceId(
            const TString& endpoint,
            const TString& database,
            bool ssl,
            const TString& caCert,
            const TString& token,
            NThreading::TPromise<TEvDescribeResourceIdResponse::TDescription> promise)
            : State(std::make_shared<TState>(
                TState {
                    .Endpoint = endpoint,
                    .Database = database,
                    .Ssl = ssl,
                    .CaCert = caCert,
                    .Token = token,
                    .Promise = std::move(promise)
                }))
        {
        }

        explicit TEvDescribeResourceId(std::shared_ptr<TState> state)
            : State(std::move(state))
        {
        }

    public:
        std::shared_ptr<TState> State;
    };

    explicit TDescribeResourceIdService(std::shared_ptr<NYdb::TDriver> driver)
        : TBase(&TDescribeResourceIdService::StateFunc)
        , Driver(std::move(driver))
    {
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvDescribeResourceId, Handle)
        sFunc(NActors::TEvents::TEvPoison, PassAway)
    )

    void PassAway() override {
        Driver.reset();
        TBase::PassAway();
    }

    void Handle(TEvDescribeResourceId::TPtr& ev) {
        const auto state = std::move(ev->Get()->State);
        const auto actorSystem = TlsActivationContext->ActorSystem();
        const auto selfId = SelfId();
        try {
            Y_ENSURE(Driver);
            NYdb::NTable::TClientSettings settings;
            settings
                .DiscoveryEndpoint(state->Endpoint)
                .DiscoveryMode(NYdb::EDiscoveryMode::Async)
                .Database(state->Database)
                .SslCredentials(NYdb::TSslCredentials(state->Ssl, state->CaCert))
                .AuthToken(state->Token);
            LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_GATEWAY,
                    "DescribeResourceId: SelfId=" << selfId << " DescribeTable " << state->Database << " at " << state->Endpoint << (state->Ssl ? " (Ssl)" : ""));
            NYdb::NTable::TTableClient tableClient(*Driver, settings);
            tableClient.GetSession().Subscribe([actorSystem, selfId, state](const NYdb::NTable::TAsyncCreateSessionResult& future) mutable {
                try {
                    auto& result = future.GetValue();
                    if (!result.IsSuccess()) {
                        LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " GetSession failed"
                                << ", status# " << result.GetStatus()
                                << ", issues# " << result.GetIssues().ToOneLineString()
                                << ", iteration# " << state->Backoff.GetIteration());
                        if (IsRetryableError(result) && state->Backoff.HasMore()) {
                            auto delay = state->Backoff.Next();
                            actorSystem->Schedule(delay,
                                    new NActors::IEventHandle(selfId, TActorId(), new TEvDescribeResourceId(std::move(state))));
                        } else {
                            state->Promise.SetValue(
                                    TEvDescribeResourceIdResponse::TDescription(static_cast<Ydb::StatusIds_StatusCode>(result.GetStatus()), NYql::TIssues({NYql::TIssue(result.GetIssues().ToString())})));
                        }
                        return;
                    }
                    state->Backoff.Reset();
                    result.GetSession()
                          .DescribeTable(state->Database, {})
                          .Subscribe([state, actorSystem, selfId](const NYdb::NTable::TAsyncDescribeTableResult& future) {
                              try {
                                  const auto& result = future.GetValue();
                                  if (!result.IsSuccess()) {
                                      LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " DescribeTable failed"
                                          << ", status# " << result.GetStatus()
                                          << ", issues# " << result.GetIssues().ToOneLineString()
                                          << ", iteration# " << state->Backoff.GetIteration());

                                      if (IsRetryableError(result) && state->Backoff.HasMore()) {
                                          auto delay = state->Backoff.Next();
                                          actorSystem->Schedule(delay,
                                                  new NActors::IEventHandle(selfId, TActorId(), new TEvDescribeResourceId(std::move(state))));
                                      } else {
                                          state->Promise.SetValue(
                                                  TEvDescribeResourceIdResponse::TDescription(static_cast<Ydb::StatusIds_StatusCode>(result.GetStatus()), NYql::TIssues({NYql::TIssue(result.GetIssues().ToString())})));
                                      }
                                      return;
                                  }
                                  LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_GATEWAY,
                                          "DescribeResourceId: SelfId=" << selfId << " Succeed");

                                  for (const auto& [k, v] : result.GetTableDescription().GetAttributes()) {
                                      LOG_TRACE_S(*actorSystem, NKikimrServices::KQP_GATEWAY,
                                              "DescribeResourceId: SelfId=" << selfId << " key=" << k << " value=" << v);
                                      if (k == "cloud_id") {
                                          LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " Resolved ResourceId=" << v);
                                          state->Promise.SetValue(TEvDescribeResourceIdResponse::TDescription(TString(v)));
                                          return;
                                      }
                                  }
                                  LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " cloud_id not found");
                                  state->Promise.SetValue(TEvDescribeResourceIdResponse::TDescription(""));
                              } catch(const std::exception& ex) {
                                  LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " got exception: " << ex.what());
                                  state->Promise.SetException(std::current_exception());
                              }
                          });
                  } catch(const std::exception& ex) {
                    LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " got exception: " << ex.what());
                    state->Promise.SetException(std::current_exception());
                  }
            });
        } catch(const std::exception& ex) {
            LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " got exception: " << ex.what());
            state->Promise.SetException(std::current_exception());
        }
    }
private:
    std::shared_ptr<NYdb::TDriver> Driver;
};
} // namespace

NThreading::TFuture<TEvDescribeResourceIdResponse::TDescription> DescribeExternalDataSourceResourceId(
    const TString& endpoint,
    const TString& database,
    bool ssl,
    const TString& caCert,
    const TString& token,
    NActors::TActorSystem* actorSystem
) {
    auto promise = NThreading::NewPromise<TEvDescribeResourceIdResponse::TDescription>();
    actorSystem->Send(MakeKqpDescribeResourceIdServiceId(), new TDescribeResourceIdService::TEvDescribeResourceId(endpoint, database, ssl, caCert, token, promise));
    return promise.GetFuture();
}

NActors::IActor* CreateDescribeResourceIdServiceActor(const std::shared_ptr<NYdb::TDriver>& driver) {
    return new TDescribeResourceIdService(driver);
}

namespace {

// XXX duplicated
inline bool IsRetryableGrpcError(const NYdbGrpc::TGrpcStatus& status) {
    switch (status.GRpcStatusCode) {
    case grpc::StatusCode::UNAUTHENTICATED:
    case grpc::StatusCode::PERMISSION_DENIED:
    case grpc::StatusCode::INVALID_ARGUMENT:
    case grpc::StatusCode::NOT_FOUND:
        return false;
    }
    return true;
}

class TAuthorizeServiceAccountUseActor : public NActors::TActorBootstrapped<TAuthorizeServiceAccountUseActor> {
public:
    using TBase = NActors::TActorBootstrapped<TAuthorizeServiceAccountUseActor>;
    TAuthorizeServiceAccountUseActor(const TString& serviceAccountId, const TString& token, NThreading::TPromise<NYdbGrpc::TGrpcStatus> promise)
        : Promise(std::move(promise))
        , ServiceAccountId(serviceAccountId)
        , Token(token)
    {
    }

    void Bootstrap() {
        Become(&TAuthorizeServiceAccountUseActor::StateFunc);
        EnableAccessServiceV2Interface = AppData()->FeatureFlags.GetEnableAccessServiceV2Interface();
        SendRequest();
    }

    void SendRequest() {
        const auto setupRequest = [&](auto& request) {
            request->Request.set_permission("iam.serviceAccounts.use");
            auto& resourcePath = *request->Request.add_resource_path();
            resourcePath.set_type("iam.serviceAccount");
            resourcePath.set_id(ServiceAccountId);
            *request->Request.mutable_iam_token() = Token;
        };

        if (EnableAccessServiceV2Interface) {
            auto request = MakeHolder<NCloud::TEvAccessService::TEvAuthorizeRequestV2>();
            setupRequest(request);
            Send(MakeKqpAccessServiceId(), std::move(request), NActors::IEventHandle::FlagTrackDelivery);
        } else {
            auto request = MakeHolder<NCloud::TEvAccessService::TEvAuthorizeRequest>();
            setupRequest(request);
            Send(MakeKqpAccessServiceId(), std::move(request), NActors::IEventHandle::FlagTrackDelivery);
        }
    }

    template <typename TEvResponse>
    void HandleAuthorizeResultImpl(typename TEvResponse::TPtr& ev) {
        if (ev->Get()->Status.Ok()) {
            ALOG_DEBUG(NKikimrServices::KQP_GATEWAY, "Authorize success, response# " << ev->Get()->Response.DebugString());
        } else {
            ALOG_WARN(NKikimrServices::KQP_GATEWAY, "Authorize failure"
                    << ", status# " << ev->Get()->Status.ToDebugString()
                    << ", iteration# " << Backoff.GetIteration());
            if (IsRetryableGrpcError(ev->Get()->Status) && Backoff.HasMore()) {
                auto delay = Backoff.Next();
                Schedule(delay, new NActors::TEvents::TEvWakeup());
                return;
            }
        }
        Promise.SetValue(std::move(ev->Get()->Status));
        PassAway();
    }

    void HandleAuthorizeResult(NCloud::TEvAccessService::TEvAuthorizeResponse::TPtr& ev) {
        HandleAuthorizeResultImpl<NCloud::TEvAccessService::TEvAuthorizeResponse>(ev);
    }

    void HandleAuthorizeResult(NCloud::TEvAccessService::TEvAuthorizeResponseV2::TPtr& ev) {
        HandleAuthorizeResultImpl<NCloud::TEvAccessService::TEvAuthorizeResponseV2>(ev);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        try {
            throw yexception() << "AccessService: "
                << "Undelivered Event " << ev->Get()->SourceType
                << " from " << SelfId() << " (Self) to " << ev->Sender
                << " Reason: " << ev->Get()->Reason << " Cookie: " << ev->Cookie;
        } catch(...) {
            Promise.SetException(std::current_exception());
        }
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NCloud::TEvAccessService::TEvAuthorizeResponse, HandleAuthorizeResult)
        hFunc(NCloud::TEvAccessService::TEvAuthorizeResponseV2, HandleAuthorizeResult)
        sFunc(NActors::TEvents::TEvWakeup, SendRequest)
        hFunc(NActors::TEvents::TEvUndelivered, Handle)
    )

    NThreading::TPromise<NYdbGrpc::TGrpcStatus> Promise;
    const TString ServiceAccountId;
    const TString Token;
    bool EnableAccessServiceV2Interface;
    TBackoff Backoff = TBackoff(/*maxRetries=*/10, /*initialDelay=*/TDuration::MilliSeconds(100), /*maxDelay=*/TDuration::Seconds(10));
};
}

NThreading::TFuture<NYdbGrpc::TGrpcStatus> AuthorizeServiceAccountUse(
    const TString& serviceAccount,
    const TString& token,
    NActors::TActorSystem* actorSystem
) {
    auto promise = NThreading::NewPromise<NYdbGrpc::TGrpcStatus>();
    auto actor = new TAuthorizeServiceAccountUseActor(serviceAccount, token, promise);
    actorSystem->Register(actor);
    return promise.GetFuture();
}

NActors::IActor* CreateAccessServiceActor() {
    // XXX duplicated: ticket_parser, http_proxy
    auto enableV2Interface = AppData()->FeatureFlags.GetEnableAccessServiceV2Interface();
    auto& authConfig = AppData()->AuthConfig;

    NCloud::TAccessServiceSettings asSettings;
    asSettings.Endpoint = authConfig.GetAccessServiceEndpoint();
    asSettings.EnableSsl = authConfig.GetUseAccessServiceTLS();
    asSettings.GrpcKeepAliveTimeMs = authConfig.GetAccessServiceGrpcKeepAliveTimeMs();
    asSettings.GrpcKeepAliveTimeoutMs = authConfig.GetAccessServiceGrpcKeepAliveTimeoutMs();

    if (asSettings.EnableSsl && authConfig.GetPathToRootCA()) {
        TString certificate = TFileInput(authConfig.GetPathToRootCA()).ReadAll();
        asSettings.CertificateRootCA = certificate;
    }
    return NCloud::CreateAccessServiceWithCache(asSettings, enableV2Interface);
}

}  // namespace NKikimr::NKqp
