#include "rate_limiter_control_plane_service.h"

#include <ydb/core/protos/services.pb.h>
#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/ydb/create_schema.h>
#include <ydb/core/yq/libs/ydb/util.h>
#include <ydb/core/yq/libs/ydb/ydb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#define LOG_D(stream) LOG_DEBUG_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_I(stream) LOG_INFO_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_W(stream) LOG_WARN_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_E(stream) LOG_ERROR_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)

namespace NYq {

class TRateLimiterControlPlaneService : public NActors::TActorBootstrapped<TRateLimiterControlPlaneService> {
public:
    static constexpr char ActorName[] = "YQ_RATE_LIMITER_CONTROL_PLANE";

    TRateLimiterControlPlaneService(
        const NYq::NConfig::TRateLimiterConfig& rateLimiterConfig,
        const NYq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
        : Config(rateLimiterConfig)
        , YqSharedResources(yqSharedResources)
        , CredProviderFactory(credentialsProviderFactory)
    {
    }

    void Bootstrap() {
        Y_VERIFY(Config.GetControlPlaneEnabled());
        if (!Config.GetEnabled()) {
            Become(&TRateLimiterControlPlaneService::RateLimiterOffStateFunc);
            return;
        }

        StartInit();
    }

    static ERetryErrorClass RetryFunc(const NYdb::TStatus& status) {
        return status.GetStatus() == NYdb::EStatus::OVERLOADED ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;
    }

    TYdbSdkRetryPolicy::TPtr MakeCreateSchemaRetryPolicy() {
        return TYdbSdkRetryPolicy::GetExponentialBackoffPolicy(RetryFunc, TDuration::MilliSeconds(10), TDuration::Seconds(1), TDuration::Seconds(5));
    }

    void RunCreateCoordinationNodeActor(const TString& path) {
        Register(MakeCreateCoordinationNodeActor(SelfId(), NKikimrServices::YQ_RATE_LIMITER, YdbConnection, path, MakeCreateSchemaRetryPolicy()));
        ++CreatingCoordinationNodes;
    }

    void StartInit() {
        Become(&TRateLimiterControlPlaneService::InitStateFunc);

        YdbConnection = NewYdbConnection(Config.GetDatabase(), CredProviderFactory, YqSharedResources->CoreYdbDriver);

        for (const auto& limiterConfig : Config.GetLimiters()) {
            auto coordinationNodePath = JoinPath(YdbConnection->TablePathPrefix, limiterConfig.GetCoordinationNodePath());
            RunCreateCoordinationNodeActor(coordinationNodePath);
        }

        TryStartWorking();
    }

    void TryStartWorking() {
        Y_VERIFY(CreatingCoordinationNodes >= 0);
        if (CreatingCoordinationNodes > 0) {
            return;
        }

        Become(&TRateLimiterControlPlaneService::WorkingStateFunc);

        // Start processing deferred queries
    }

    void HandleInit(TEvents::TEvSchemaCreated::TPtr&) {
        --CreatingCoordinationNodes;

        TryStartWorking();
    }

    // State func that does nothing. Rate limiting is turned off.
    // Answers "OK" responses and does nothing.
    STRICT_STFUNC(
        RateLimiterOffStateFunc,
    )

    // State func that inits limiters.
    // Puts all incoming requests to queue.
    STRICT_STFUNC(
        InitStateFunc,
        hFunc(TEvents::TEvSchemaCreated, HandleInit);
    )

    // Working
    STRICT_STFUNC(
        WorkingStateFunc,
    )

private:
    const NYq::NConfig::TRateLimiterConfig Config;
    const NYq::TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TYdbConnectionPtr YdbConnection;

    // Init
    size_t CreatingCoordinationNodes = 0;
};

NActors::TActorId RateLimiterControlPlaneServiceId() {
    constexpr TStringBuf name = "RATE_LIM_CP";
    return NActors::TActorId(0, name);
}

NActors::IActor* CreateRateLimiterControlPlaneService(
    const NYq::NConfig::TRateLimiterConfig& rateLimiterConfig,
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
{
    return new TRateLimiterControlPlaneService(rateLimiterConfig, yqSharedResources, credentialsProviderFactory);
}

} // namespace NYq
