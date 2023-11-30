#include "quoter_service.h"

#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/base/path.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/retry/retry_policy.h>

#include <algorithm>
#include <deque>
#include <unordered_map>

#define LOG_T(stream) LOG_TRACE_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_D(stream) LOG_DEBUG_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_I(stream) LOG_INFO_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_W(stream) LOG_WARN_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)
#define LOG_E(stream) LOG_ERROR_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::YQ_RATE_LIMITER, stream)

namespace NFq {
namespace {

constexpr TDuration CLEANUP_PERIOD = TDuration::Minutes(10);
constexpr ui64 MaxInflight = 2;

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvQuotaReceived = EvBegin,
        EvRetryQuotaRequest,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvQuotaReceived : public NActors::TEventLocal<TEvQuotaReceived, EvQuotaReceived> {
        TEvQuotaReceived(const NYdb::TStatus& status, const TString& rateLimiter, const TString& resource, ui64 amount)
            : RateLimiter(rateLimiter)
            , Resource(resource)
            , Amount(amount)
            , Status(status)
        {
        }

        const TString RateLimiter;
        const TString Resource;
        const ui64 Amount;
        NYdb::TStatus Status;
    };

    struct TEvRetryQuotaRequest : public NActors::TEventLocal<TEvRetryQuotaRequest, EvRetryQuotaRequest> {
        TEvRetryQuotaRequest(const TString& rateLimiter, const TString& resource, ui64 amount, ui64 cookie)
            : RateLimiter(rateLimiter)
            , Resource(resource)
            , Amount(amount)
            , Cookie(cookie)
        {
        }

        const TString RateLimiter;
        const TString Resource;
        const ui64 Amount;
        const ui64 Cookie;
    };
};

class TYqQuoterService : public NActors::TActorBootstrapped<TYqQuoterService> {
    using TResourceKey = std::pair<TString, TString>;

    struct TResourceProcessor {
        std::deque<NKikimr::TEvQuota::TEvRequest::TPtr> Requests;
        ui64 AvailableAmount = 0;
        ui64 RequiredAmount = 0;
        ui64 RequestedAmount = 0;
        THashMap<ui64, IRetryPolicy<const NYdb::TStatus&>::IRetryState::TPtr> RateLimiterRequests;
    };

public:
    TYqQuoterService(
        const NFq::NConfig::TRateLimiterConfig& config,
        const NFq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
        : Config(config)
        , YqSharedResources(yqSharedResources)
        , CredProviderFactory(credentialsProviderFactory)
    {
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NKikimr::TEvQuota::TEvRequest, Handle);
        hFunc(NKikimr::TEvQuota::TEvCancelRequest, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(TEvPrivate::TEvQuotaReceived, Handle);
        hFunc(TEvPrivate::TEvRetryQuotaRequest, Handle);
    )

    void Bootstrap() {
        Become(&TYqQuoterService::StateFunc);
        LOG_I("Bootstraping with config: " << Config);
        Config.MutableDatabase()->SetDatabase(NKikimr::CanonizePath(Config.GetDatabase().GetDatabase())); // Crutch for rate limiter grpc
        YdbConnection = NewYdbConnection(Config.GetDatabase(), CredProviderFactory, YqSharedResources->CoreYdbDriver);

        Schedule(CLEANUP_PERIOD, new NActors::TEvents::TEvWakeup());
    }

    void Handle(NKikimr::TEvQuota::TEvRequest::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Get()->Deadline == TDuration::Max(), "Unimplemented");
        Y_ABORT_UNLESS(ev->Get()->Operator == NKikimr::TEvQuota::EResourceOperator::And, "Unimplemented");
        Y_ABORT_UNLESS(ev->Get()->Reqs.size() == 1, "Unimplemented");
        Y_ABORT_UNLESS(ev->Get()->Reqs[0].IsUsedAmount, "Unimplemented");
        Y_ABORT_UNLESS(!ev->Get()->Reqs[0].QuoterId, "Unimplemented");
        Y_ABORT_UNLESS(!ev->Get()->Reqs[0].ResourceId, "Unimplemented");

        const TString quoter = NKikimr::CanonizePath(ev->Get()->Reqs[0].Quoter);
        const TString& resource = ev->Get()->Reqs[0].Resource;
        Y_ABORT_UNLESS(quoter, "Quoter is unspecified");
        Y_ABORT_UNLESS(resource, "Resource is unspecified");

        const TResourceKey key(quoter, resource);
        if (const auto senderToResourceIt = SenderToResource.find(ev->Sender); senderToResourceIt == SenderToResource.end()) {
            SenderToResource.emplace(ev->Sender, key);
        }

        const ui64 amount = ev->Get()->Reqs[0].Amount;
        LOG_T("Quota request {\"" << quoter << "\", \"" << resource << "\"}. Amount: " << amount << ". Sender: " << ev->Sender);

        TResourceProcessor& proc = Resources[key];
        proc.RequiredAmount += amount;
        proc.Requests.emplace_back(std::move(ev));
        ProcessRequests(proc, key);
    }

    void AcquireResource(const TResourceKey& key, ui64 amount, ui64 cookie) {
        LOG_T("Send acquire resource request to {\"" << key.first << "\", \"" << key.second << "\"}. Amount: " << amount << ". Cookie: " << cookie);
        auto asyncStatus = YdbConnection->RateLimiterClient.AcquireResource(key.first, key.second, NYdb::NRateLimiter::TAcquireResourceSettings().Amount(amount));
        asyncStatus.Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(), selfId = SelfId(), cookie, key, amount](const NYdb::TAsyncStatus& status) {
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, new TEvPrivate::TEvQuotaReceived(status.GetValueSync(), key.first, key.second, amount), 0, cookie));
        });
    }

    void ProcessRequests(TResourceProcessor& proc, const TResourceKey& key) {
        LOG_T("Process requests for {\"" << key.first << "\", \"" << key.second << "\"}. Requests: " << proc.Requests.size()
            << ". RateLimiterInflight: " << proc.RateLimiterRequests.size() << ". AvailableAmount: " << proc.AvailableAmount
            << ". RequiredAmount: " << proc.RequiredAmount << ". RequestedAmount: " << proc.RequestedAmount);
        while (!proc.Requests.empty() && proc.AvailableAmount >= proc.Requests.front()->Get()->Reqs[0].Amount) {
            Send(proc.Requests.front()->Sender, new NKikimr::TEvQuota::TEvClearance(NKikimr::TEvQuota::TEvClearance::EResult::Success));
            proc.AvailableAmount -= proc.Requests.front()->Get()->Reqs[0].Amount;
            proc.RequiredAmount -= proc.Requests.front()->Get()->Reqs[0].Amount;
            proc.Requests.pop_front();
        }
        Y_ABORT_UNLESS(!proc.Requests.empty() || proc.RequiredAmount == 0); // Requests.empty() => RequiredAmount == 0
        if (proc.RateLimiterRequests.size() < MaxInflight && !proc.Requests.empty() && proc.RequestedAmount + proc.AvailableAmount < proc.RequiredAmount) {
            const ui64 rateLimiterRequestCookie = RateLimiterNextRequestCookie++;
            const ui64 amountToRequest = proc.RequiredAmount - proc.AvailableAmount - proc.RequestedAmount;
            AcquireResource(key, amountToRequest, rateLimiterRequestCookie);
            proc.RequestedAmount += amountToRequest;
            proc.RateLimiterRequests[rateLimiterRequestCookie];
        }
    }

    void FailRequests(TResourceProcessor& proc, ui64 amount) {
        while (!proc.Requests.empty() && amount >= proc.Requests.front()->Get()->Reqs[0].Amount) {
            Send(proc.Requests.front()->Sender, new NKikimr::TEvQuota::TEvClearance(NKikimr::TEvQuota::TEvClearance::EResult::GenericError));
            proc.RequiredAmount -= proc.Requests.front()->Get()->Reqs[0].Amount;
            proc.Requests.pop_front();
        }
        Y_ABORT_UNLESS(!proc.Requests.empty() || proc.RequiredAmount == 0); // Requests.empty() => RequiredAmount == 0
    }

    static ERetryErrorClass RetryClass(const NYdb::TStatus& status) {
        if (status.IsTransportError()) {
            return ERetryErrorClass::ShortRetry;
        }
        const NYdb::EStatus st = status.GetStatus();
        if (st == NYdb::EStatus::INTERNAL_ERROR
            || st == NYdb::EStatus::UNAVAILABLE
            || st == NYdb::EStatus::TIMEOUT
            || st == NYdb::EStatus::BAD_SESSION
            || st == NYdb::EStatus::SESSION_EXPIRED
            || st == NYdb::EStatus::UNDETERMINED
            || st == NYdb::EStatus::TRANSPORT_UNAVAILABLE
            || st == NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED
            || st == NYdb::EStatus::CLIENT_INTERNAL_ERROR
            || st == NYdb::EStatus::CLIENT_OUT_OF_RANGE
            || st == NYdb::EStatus::CLIENT_DISCOVERY_FAILED)
        {
            return ERetryErrorClass::ShortRetry;
        }

        if (st == NYdb::EStatus::OVERLOADED
            || st == NYdb::EStatus::SESSION_BUSY
            || st == NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED
            || st == NYdb::EStatus::CLIENT_LIMITS_REACHED)
        {
            return ERetryErrorClass::LongRetry;
        }
        return ERetryErrorClass::NoRetry;
    }

    bool ScheduleRetry(TResourceProcessor& proc, const TResourceKey& key, TEvPrivate::TEvQuotaReceived::TPtr& ev) { // true if error is retriable
        auto& retryState = proc.RateLimiterRequests[ev->Cookie];
        if (!retryState) {
            retryState = RetryPolicy->CreateRetryState();
        }
        if (const TMaybe<TDuration> delay = retryState->GetNextRetryDelay(ev->Get()->Status)) {
            LOG_D("Scheduled retry in " << *delay << " for resource {\"" << key.first << "\", \"" << key.second << "\"}. Status: " << ev->Get()->Status.GetStatus() << " " << ev->Get()->Status.GetIssues().ToOneLineString());
            Schedule(*delay, new TEvPrivate::TEvRetryQuotaRequest(key.first, key.second, ev->Get()->Amount, ev->Cookie));
            return true;
        } else {
            proc.RateLimiterRequests.erase(ev->Cookie);
            return false;
        }
    }

    void Handle(TEvPrivate::TEvQuotaReceived::TPtr& ev) {
        const TResourceKey key(ev->Get()->RateLimiter, ev->Get()->Resource);
        TResourceProcessor& proc = Resources[key];
        LOG_T("Received quota for resource {\"" << key.first << "\", \"" << key.second << "\"}. Amount: " << ev->Get()->Amount << ". Cookie: " << ev->Cookie << ". Status: " << ev->Get()->Status.GetStatus() << " " << ev->Get()->Status.GetIssues().ToOneLineString());
        if (ev->Get()->Status.IsSuccess()) {
            proc.RateLimiterRequests.erase(ev->Cookie);
            proc.RequestedAmount -= ev->Get()->Amount;
            proc.AvailableAmount += ev->Get()->Amount;
            ProcessRequests(proc, key);
        } else if (!ScheduleRetry(proc, key, ev)) {
            proc.RequestedAmount -= ev->Get()->Amount;
            FailRequests(proc, ev->Get()->Amount);
        }
    }

    void Handle(TEvPrivate::TEvRetryQuotaRequest::TPtr& ev) {
        const TResourceKey key(ev->Get()->RateLimiter, ev->Get()->Resource);
        const ui64 cookie = ev->Get()->Cookie;
        LOG_T("Retry acquire quota for resource {\"" << key.first << "\", \"" << key.second << "\"}. Amount: " << ev->Get()->Amount << ". Cookie: " << cookie);
        AcquireResource(key, ev->Get()->Amount, cookie);
    }

    void Handle(NKikimr::TEvQuota::TEvCancelRequest::TPtr& ev) {
        Y_ABORT_UNLESS(!ev->Cookie, "Unimplemented");
        if (const auto resIt = SenderToResource.find(ev->Sender); resIt != SenderToResource.end()) {
            if (const auto procIt = Resources.find(resIt->second); procIt != Resources.end()) {
                auto& proc = procIt->second;
                proc.Requests.erase(
                    std::remove_if(
                        proc.Requests.begin(),
                        proc.Requests.end(),
                        [&](const NKikimr::TEvQuota::TEvRequest::TPtr& req) { return req->Sender == ev->Sender; }
                    ),
                    proc.Requests.end());
            }
            SenderToResource.erase(resIt);
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        Y_UNUSED(ev);
        CleanupEmpty();
        Schedule(CLEANUP_PERIOD, new NActors::TEvents::TEvWakeup());
    }

    void CleanupEmpty() {
        int deletedRes = 0, deletedSenders = 0;
        for (auto i = Resources.begin(); i != Resources.end();) {
            const TResourceProcessor& res = i->second;
            if (res.Requests.empty() && res.RateLimiterRequests.empty()) {
                i = Resources.erase(i);
                ++deletedRes;
            } else {
                ++i;
            }
        }
        for (auto i = SenderToResource.begin(); i != SenderToResource.end();) {
            const TResourceKey& key = i->second;
            const auto resIt = Resources.find(key);
            if (resIt == Resources.end()) {
                i = SenderToResource.erase(i);
                ++deletedSenders;
            } else {
                bool found = false;
                for (auto& req : resIt->second.Requests) {
                    if (req->Sender == i->first) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    ++i;
                } else {
                    i = SenderToResource.erase(i);
                    ++deletedSenders;
                }
            }
        }
        if (deletedRes || deletedSenders) {
            LOG_I("CleanupEmpty. Deleted " << deletedRes << " resources and " << deletedSenders << " senders");
        }
    }

private:
    NFq::NConfig::TRateLimiterConfig Config;
    const NFq::TYqSharedResources::TPtr YqSharedResources;
    const NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TYdbConnectionPtr YdbConnection;
    ui64 RateLimiterNextRequestCookie = 1;
    std::unordered_map<TResourceKey, TResourceProcessor, THash<TResourceKey>> Resources;
    std::unordered_map<NActors::TActorId, TResourceKey, THash<NActors::TActorId>> SenderToResource;
    IRetryPolicy<const NYdb::TStatus&>::TPtr RetryPolicy = IRetryPolicy<const NYdb::TStatus&>::GetExponentialBackoffPolicy(RetryClass);
};

} // namespace

NActors::IActor* CreateQuoterService(
    const NFq::NConfig::TRateLimiterConfig& rateLimiterConfig,
    const NFq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
{
    return new TYqQuoterService(rateLimiterConfig, yqSharedResources, credentialsProviderFactory);
}

} // namespace NFq
