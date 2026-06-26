#include "kqp_federated_query_actors.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/services/scheme_secret/resolver.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NKqp {

void RegisterDescribeSecretsActor(
    const NActors::TActorId& replyActorId,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    const std::vector<TString>& secretIds,
    TActorSystem* actorSystem
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
    TActorSystem* actorSystem
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
                                          state->Promise.SetValue(TString{v});
                                          return;
                                      }
                                  }
                                  LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " cloud_id not found");
                                  state->Promise.SetValue(TString(""));
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
    TActorSystem* actorSystem
) {
    auto promise = NThreading::NewPromise<TEvDescribeResourceIdResponse::TDescription>();
    actorSystem->Send(MakeKqpDescribeResourceIdServiceId(), new TDescribeResourceIdService::TEvDescribeResourceId(endpoint, database, ssl, caCert, token, promise));
    return promise.GetFuture();
}

IActor* CreateDescribeResourceIdServiceActor(const std::shared_ptr<NYdb::TDriver>& driver) {
    return new TDescribeResourceIdService(driver);
}

}  // namespace NKikimr::NKqp
