#include "rate_limiter_control_plane_service.h"
#include "update_limit_actor.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/core/fq/libs/rate_limiter/utils/path.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <deque>
#include <variant>

#define LOG_D(stream) LOG_DEBUG_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_I(stream) LOG_INFO_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_W(stream) LOG_WARN_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_E(stream) LOG_ERROR_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)

namespace NFq {

namespace {

constexpr size_t MaxRequestsInflight = 10;

ERetryErrorClass RetryFunc(const NYdb::TStatus& status) {
    if (status.IsSuccess()) {
        return ERetryErrorClass::NoRetry;
    }

    if (status.IsTransportError()) {
        return ERetryErrorClass::ShortRetry;
    }

    const NYdb::EStatus st = status.GetStatus();
    if (st == NYdb::EStatus::INTERNAL_ERROR || st == NYdb::EStatus::UNAVAILABLE ||
        st == NYdb::EStatus::TIMEOUT || st == NYdb::EStatus::BAD_SESSION ||
        st == NYdb::EStatus::SESSION_EXPIRED ||
        st == NYdb::EStatus::SESSION_BUSY) {
        return ERetryErrorClass::ShortRetry;
    }

    if (st == NYdb::EStatus::OVERLOADED) {
        return ERetryErrorClass::LongRetry;
    }

    return ERetryErrorClass::NoRetry;
}

TYdbSdkRetryPolicy::TPtr MakeSchemaRetryPolicy() {
    static auto policy = TYdbSdkRetryPolicy::GetExponentialBackoffPolicy(RetryFunc, TDuration::MilliSeconds(10), TDuration::Seconds(1), TDuration::Seconds(5));
    return policy;
}

struct TRateLimiterRequestsQueue {
    explicit TRateLimiterRequestsQueue(const TString& rateLimiterPath, const TYdbConnectionPtr& connection, ui64 cookieStep, ui64 index)
        : RateLimiterPath(rateLimiterPath)
        , Connection(connection)
        , CookieStep(cookieStep)
        , Index(index)
        , NextCookie(Index + CookieStep)
    {
    }

    TRateLimiterRequestsQueue& AddRequest(TEvRateLimiter::TEvCreateResource::TPtr& ev) {
        Queue.emplace_back(std::move(ev));
        return *this;
    }

    TRateLimiterRequestsQueue& AddRequest(TEvRateLimiter::TEvDeleteResource::TPtr& ev) {
        Queue.emplace_back(std::move(ev));
        return *this;
    }

    TRateLimiterRequestsQueue& AddRequest(TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev) {
        Queue.emplace_back(std::move(ev));
        return *this;
    }

    void ProcessRequests() {
        for (; Inflight.size() < MaxRequestsInflight && !Queue.empty(); Queue.pop_front(), NextCookie += CookieStep) {
            ProcessRequest(NextCookie, Inflight.emplace(NextCookie, std::move(Queue.front())).first->second);
        }
    }

    template <class TEventPtr>
    void OnResponse(TEventPtr& ev) {
        const auto it = Inflight.find(ev->Cookie);
        Y_ABORT_UNLESS(it != Inflight.end());
        ProcessResponse(it->second, ev);
        Inflight.erase(it);
        ProcessRequests();
    }

private:
    struct TRequest {
        explicit TRequest(TEvRateLimiter::TEvCreateResource::TPtr&& ev)
            : OriginalRequest(std::move(ev))
        {
        }

        explicit TRequest(TEvRateLimiter::TEvDeleteResource::TPtr&& ev)
            : OriginalRequest(std::move(ev))
        {
        }

        explicit TRequest(TEvQuotaService::TQuotaLimitChangeRequest::TPtr&& ev)
            : OriginalRequest(std::move(ev))
        {
        }

        using TOriginalRequestType = std::variant<TEvRateLimiter::TEvCreateResource::TPtr, TEvRateLimiter::TEvDeleteResource::TPtr, TEvQuotaService::TQuotaLimitChangeRequest::TPtr>;
        TOriginalRequestType OriginalRequest;
    };

private:
    void ProcessRequest(ui64 cookie, TRequest& req) {
        std::visit([cookie, &req, this](auto& ev) { ProcessRequest(cookie, req, ev); }, req.OriginalRequest);
    }

    void ProcessRequest(ui64 cookie, TRequest& req, TEvRateLimiter::TEvCreateResource::TPtr& ev) {
        Y_UNUSED(req);
        TString path;
        std::vector<TMaybe<double>> limits;
        if (!ev->Get()->Scope && !ev->Get()->QueryId && !ev->Get()->QueryLimit) { // cloud
            path = GetRateLimiterResourcePath(ev->Get()->CloudId);
            limits = {ev->Get()->CloudLimit};
        } else { // query
            path = GetRateLimiterResourcePath(ev->Get()->CloudId, ev->Get()->Scope, ev->Get()->QueryId);
            limits = {Nothing(), Nothing(), ev->Get()->QueryLimit}; // imply that cloud resource must already have been created
        }
        NActors::TActivationContext::AsActorContext().Register(
            MakeCreateRateLimiterResourceActor(
                NActors::TActivationContext::AsActorContext().SelfID,
                NKikimrServices::YQ_RATE_LIMITER,
                Connection,
                RateLimiterPath,
                path,
                limits,
                MakeSchemaRetryPolicy(),
                cookie
            )
        );
    }

    void ProcessRequest(ui64 cookie, TRequest& req, TEvRateLimiter::TEvDeleteResource::TPtr& ev) {
        Y_UNUSED(req);
        NActors::TActivationContext::AsActorContext().Register(
            MakeDeleteRateLimiterResourceActor(
                NActors::TActivationContext::AsActorContext().SelfID,
                NKikimrServices::YQ_RATE_LIMITER,
                Connection,
                RateLimiterPath,
                GetRateLimiterResourcePath(ev->Get()->CloudId, ev->Get()->Scope, ev->Get()->QueryId),
                MakeSchemaRetryPolicy(),
                cookie
            )
        );
    }

    void ProcessRequest(ui64 cookie, TRequest& req, TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev) {
        Y_UNUSED(req);
        NActors::TActivationContext::AsActorContext().Register(
            MakeUpdateCloudRateLimitActor(
                NActors::TActivationContext::AsActorContext().SelfID,
                Connection,
                RateLimiterPath,
                ev->Get()->SubjectId,
                ev->Get()->LimitRequested,
                MakeSchemaRetryPolicy(),
                cookie
            )
        );
    }

    void ProcessResponse(TRequest& req, TEvents::TEvSchemaCreated::TPtr& ev) {
        TEvRateLimiter::TEvCreateResource::TPtr& originalRequest = std::get<TEvRateLimiter::TEvCreateResource::TPtr>(req.OriginalRequest);
        if (IsResourceCreationSuccess(ev)) {
            NActors::TActivationContext::AsActorContext().Send(
                originalRequest->Sender,
                new TEvRateLimiter::TEvCreateResourceResponse(
                    RateLimiterPath,
                    ev->Get()->Result.GetIssues()
                ),
                0, // flags
                originalRequest->Cookie
            );
        } else {
            NActors::TActivationContext::AsActorContext().Send(
                originalRequest->Sender,
                new TEvRateLimiter::TEvCreateResourceResponse(
                    ev->Get()->Result.GetIssues()
                ),
                0, // flags
                originalRequest->Cookie
            );
        }
    }

    void ProcessResponse(TRequest& req, TEvents::TEvSchemaDeleted::TPtr& ev) {
        TEvRateLimiter::TEvDeleteResource::TPtr& originalRequest = std::get<TEvRateLimiter::TEvDeleteResource::TPtr>(req.OriginalRequest);
        NActors::TActivationContext::AsActorContext().Send(
            originalRequest->Sender,
            new TEvRateLimiter::TEvDeleteResourceResponse(
                ev->Get()->Result.IsSuccess(),
                ev->Get()->Result.GetIssues()
            ),
            0, // flags
            originalRequest->Cookie
        );
    }

    void ProcessResponse(TRequest& req, TEvents::TEvSchemaUpdated::TPtr& ev) {
        TEvQuotaService::TQuotaLimitChangeRequest::TPtr& originalRequest = std::get<TEvQuotaService::TQuotaLimitChangeRequest::TPtr>(req.OriginalRequest);
        auto& record = *originalRequest->Get();
        NActors::TActivationContext::AsActorContext().Send(
            originalRequest->Sender,
            new TEvQuotaService::TQuotaLimitChangeResponse(
                record.SubjectType,
                record.SubjectId,
                record.MetricName,
                ev->Get()->Result.IsSuccess() ? record.LimitRequested : record.Limit,
                record.LimitRequested
            ),
            0, // flags
            originalRequest->Cookie
        );
    }

    static bool IsResourceCreationSuccess(TEvents::TEvSchemaCreated::TPtr& ev) {
        if (ev->Get()->Result.IsSuccess() || ev->Get()->Result.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
            return true;
        }
        // Special case: when resource os already created, but have its quota altered manually
        if (ev->Get()->Result.GetStatus() == NYdb::EStatus::BAD_REQUEST) {
            for (const NYql::TIssue& i : ev->Get()->Result.GetIssues()) {
                if (i.GetMessage().find("Resource already exists") != TString::npos) {
                    return true;
                }
            }
        }
        return false;
    }

private:
    const TString RateLimiterPath;
    const TYdbConnectionPtr Connection;
    const ui64 CookieStep;
    const ui64 Index;

    std::deque<TRequest> Queue;
    std::unordered_map<ui64, TRequest> Inflight;
    ui64 NextCookie;
};

} // namespace

class TRateLimiterControlPlaneService : public NActors::TActorBootstrapped<TRateLimiterControlPlaneService> {
public:
    static constexpr char ActorName[] = "YQ_RATE_LIMITER_CONTROL_PLANE";

    TRateLimiterControlPlaneService(
        const NFq::NConfig::TRateLimiterConfig& rateLimiterConfig,
        const NFq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
        : Config(rateLimiterConfig)
        , YqSharedResources(yqSharedResources)
        , CredProviderFactory(credentialsProviderFactory)
    {
    }

    void Bootstrap() {
        Y_ABORT_UNLESS(Config.GetControlPlaneEnabled());
        if (!Config.GetEnabled()) {
            Become(&TRateLimiterControlPlaneService::RateLimiterOffStateFunc);
            return;
        }

        StartInit();
    }

    void RunCreateCoordinationNodeActor(const TString& path) {
        Register(MakeCreateCoordinationNodeActor(SelfId(), NKikimrServices::YQ_RATE_LIMITER, YdbConnection, path, MakeSchemaRetryPolicy()));
        ++CreatingCoordinationNodes;
    }

    void StartInit() {
        Become(&TRateLimiterControlPlaneService::InitStateFunc);

        YdbConnection = NewYdbConnection(Config.GetDatabase(), CredProviderFactory, YqSharedResources->CoreYdbDriver);

        Y_ABORT_UNLESS(Config.LimitersSize() > 0);
        RateLimiters.reserve(Config.LimitersSize());
        for (ui64 index = 0; index < Config.LimitersSize(); ++index) {
            const auto& limiterConfig = Config.GetLimiters(index);
            auto coordinationNodePath = JoinPath(YdbConnection->TablePathPrefix, limiterConfig.GetCoordinationNodePath());
            RunCreateCoordinationNodeActor(coordinationNodePath);
            RateLimiters.emplace_back(coordinationNodePath, YdbConnection, Config.LimitersSize(), index);
        }

        TryStartWorking();
    }

    void TryStartWorking() {
        Y_ABORT_UNLESS(CreatingCoordinationNodes >= 0);
        if (CreatingCoordinationNodes > 0) {
            return;
        }

        Become(&TRateLimiterControlPlaneService::WorkingStateFunc);

        // Start processing deferred queries
        for (auto& rateLimiter : RateLimiters) {
            rateLimiter.ProcessRequests();
        }
    }

    void HandleInit(TEvents::TEvSchemaCreated::TPtr&) {
        --CreatingCoordinationNodes;

        TryStartWorking();
    }

    void HandleInit(TEvRateLimiter::TEvCreateResource::TPtr& ev) {
        GetRateLimiter(ev->Get()->CloudId).AddRequest(ev);
    }

    void HandleInit(TEvRateLimiter::TEvDeleteResource::TPtr& ev) {
        GetRateLimiter(ev->Get()->CloudId).AddRequest(ev);
    }

    void HandleInit(TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev) {
        GetRateLimiter(ev->Get()->SubjectId).AddRequest(ev);
    }

    void HandleWorking(TEvRateLimiter::TEvCreateResource::TPtr& ev) {
        GetRateLimiter(ev->Get()->CloudId).AddRequest(ev).ProcessRequests();
    }

    void HandleWorking(TEvRateLimiter::TEvDeleteResource::TPtr& ev) {
        GetRateLimiter(ev->Get()->CloudId).AddRequest(ev).ProcessRequests();
    }

    void HandleWorking(TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev) {
        GetRateLimiter(ev->Get()->SubjectId).AddRequest(ev).ProcessRequests();
    }

    void HandleWorking(TEvents::TEvSchemaCreated::TPtr& ev) {
        GetRateLimiterByCookie(ev->Cookie).OnResponse(ev);
    }

    void HandleWorking(TEvents::TEvSchemaDeleted::TPtr& ev) {
        GetRateLimiterByCookie(ev->Cookie).OnResponse(ev);
    }

    void HandleWorking(TEvents::TEvSchemaUpdated::TPtr& ev) {
        GetRateLimiterByCookie(ev->Cookie).OnResponse(ev);
    }

    void HandleOff(TEvRateLimiter::TEvCreateResource::TPtr& ev) {
        Send(ev->Sender, new TEvRateLimiter::TEvCreateResourceResponse(""));
    }

    void HandleOff(TEvRateLimiter::TEvDeleteResource::TPtr& ev) {
        Send(ev->Sender, new TEvRateLimiter::TEvDeleteResourceResponse(true));
    }

    void HandleOff(TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev) {
        auto& record = *ev->Get();

        Send(ev->Sender, new TEvQuotaService::TQuotaLimitChangeResponse(record.SubjectType, record.SubjectId, record.MetricName,
            record.LimitRequested, record.LimitRequested));
    }

    void HandleQuotaUsage(TEvQuotaService::TQuotaUsageRequest::TPtr& ev) {
        Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(ev->Get()->SubjectType, ev->Get()->SubjectId, ev->Get()->MetricName, 0));
    }

    // State func that does nothing. Rate limiting is turned off.
    // Answers "OK" responses and does nothing.
    STRICT_STFUNC(
        RateLimiterOffStateFunc,
        hFunc(TEvRateLimiter::TEvCreateResource, HandleOff);
        hFunc(TEvRateLimiter::TEvDeleteResource, HandleOff);
        hFunc(TEvQuotaService::TQuotaLimitChangeRequest, HandleOff);
        hFunc(TEvQuotaService::TQuotaUsageRequest, HandleQuotaUsage);
    )

    // State func that inits limiters.
    // Puts all incoming requests to queue.
    STRICT_STFUNC(
        InitStateFunc,
        hFunc(TEvents::TEvSchemaCreated, HandleInit);
        hFunc(TEvRateLimiter::TEvCreateResource, HandleInit);
        hFunc(TEvRateLimiter::TEvDeleteResource, HandleInit);
        hFunc(TEvQuotaService::TQuotaLimitChangeRequest, HandleInit);
        hFunc(TEvQuotaService::TQuotaUsageRequest, HandleQuotaUsage);
    )

    // Working
    STRICT_STFUNC(
        WorkingStateFunc,
        hFunc(TEvRateLimiter::TEvCreateResource, HandleWorking);
        hFunc(TEvRateLimiter::TEvDeleteResource, HandleWorking);
        hFunc(TEvents::TEvSchemaCreated, HandleWorking);
        hFunc(TEvents::TEvSchemaDeleted, HandleWorking);
        hFunc(TEvents::TEvSchemaUpdated, HandleWorking);
        hFunc(TEvQuotaService::TQuotaLimitChangeRequest, HandleWorking);
        hFunc(TEvQuotaService::TQuotaUsageRequest, HandleQuotaUsage);
    )

    TRateLimiterRequestsQueue& GetRateLimiter(const TString& cloudId) {
        // Choose rate limiter: for now is hardcoded.
        Y_UNUSED(cloudId);
        return RateLimiters[0];
    }

    TRateLimiterRequestsQueue& GetRateLimiterByCookie(ui64 cookie) {
        return RateLimiters[cookie % RateLimiters.size()];
    }

private:
    const NFq::NConfig::TRateLimiterConfig Config;
    const NFq::TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TYdbConnectionPtr YdbConnection;

    // Init
    size_t CreatingCoordinationNodes = 0;

    std::vector<TRateLimiterRequestsQueue> RateLimiters;
};

NActors::IActor* CreateRateLimiterControlPlaneService(
    const NConfig::TRateLimiterConfig& rateLimiterConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
{
    return new TRateLimiterControlPlaneService(rateLimiterConfig, yqSharedResources, credentialsProviderFactory);
}

} // namespace NFq
