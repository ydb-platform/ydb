#include "meta_cache.h"

#include <ydb/mvp/core/core_ydb.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/http/http_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/digest/multi.h>
#include <util/generic/queue.h>
#include <util/string/cast.h>

#define YDB_LOG_THIS_FILE_COMPONENT HttpLog

namespace NMeta {

using namespace NHttp;

struct TEvPrivate {
    enum EEv {
        EvUpdateUrlState = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvUpdateUrlState : NActors::TEventLocal<TEvUpdateUrlState, EvUpdateUrlState> {
        TString Id;
        TCacheOwnership CacheOwnership;

        TEvUpdateUrlState(const TString& id, const TCacheOwnership& cacheOwnership)
            : Id(id)
            , CacheOwnership(cacheOwnership)
        {}
    };
};

class THttpIncomingDistributedCacheActor : public NActors::TActorBootstrapped<THttpIncomingDistributedCacheActor>, THttpConfig {
public:
    using TBase = NActors::TActorBootstrapped<THttpIncomingDistributedCacheActor>;
    NActors::TActorId HttpProxyId;
    TGetCachePolicy GetCachePolicy;
    TGetCacheOwnership GetCacheOwnership;
    static constexpr TDuration RefreshPeriod = TDuration::Seconds(1);
    THashMap<TString, TActorId> Handlers;

    struct TCacheRecord {
        TCachePolicy CachePolicy;
        TCacheOwnership CacheOwnership;
        TDuration Timeout = TDuration::Seconds(30);
        std::deque<TEvHttpProxy::TEvHttpIncomingRequest::TPtr> Waiters;

        TCacheRecord(const TCachePolicy cachePolicy)
            : CachePolicy(cachePolicy)
        {}

        TString GetForwardUrlForDebug() const {
            return CacheOwnership.GetForwardUrlForDebug();
        }

        TInstant GetRefreshTime(TInstant now) const {
            if (now < CacheOwnership.Deadline) {
                if (CacheOwnership.ForwardUrl.empty()) {
                    return now + ((CacheOwnership.Deadline - now) / 2);
                } else {
                    return CacheOwnership.Deadline;
                }
            } else {
                return now + CachePolicy.TimeToRefresh;
            }
        }
    };

    struct TRefreshRecord {
        TString Key;
        TInstant RefreshTime;

        bool operator <(const TRefreshRecord& b) const {
            return RefreshTime > b.RefreshTime;
        }
    };

    std::unordered_map<TString, TCacheRecord> Cache;
    std::unordered_map<THttpOutgoingRequest*, TEvHttpProxy::TEvHttpIncomingRequest::TPtr> OutgoingRequests;
    std::priority_queue<TRefreshRecord> RefreshQueue;

    THttpIncomingDistributedCacheActor(const NActors::TActorId& httpProxyId, TGetCachePolicy getCachePolicy, TGetCacheOwnership getCacheOwnership)
        : HttpProxyId(httpProxyId)
        , GetCachePolicy(std::move(getCachePolicy))
        , GetCacheOwnership(std::move(getCacheOwnership))
    {}

    static constexpr char ActorName[] = "HTTP_IN_DISTRIBUTED_CACHE_ACTOR";

    void Bootstrap() {
        Become(&THttpIncomingDistributedCacheActor::StateWork, RefreshPeriod, new NActors::TEvents::TEvWakeup());
    }

    static TString GetCacheKey(const THttpIncomingRequest* request, const TCachePolicy& policy) {
        TStringBuilder key;
        key << request->URL.Before('?');
        if (!policy.HeadersToCacheKey.empty()) {
            THeaders headers(request->Headers);
            bool wasHeader = false;
            for (const TString& header : policy.HeadersToCacheKey) {
                if (headers.Has(header)) {
                    if (wasHeader) {
                        key << "&";
                    } else {
                        key << "?";
                    }
                    key << header << "=" << headers[header];
                }
            }
        }
        return key;
    }

    TActorId GetRequestHandler(THttpIncomingRequestPtr request) {
        TStringBuf url = request->URL.Before('?');
        THashMap<TString, TActorId>::iterator it;
        while (!url.empty()) {
            it = Handlers.find(url);
            if (it != Handlers.end()) {
                return it->second;
            } else {
                if (url.EndsWith('/')) {
                    url.Trunc(url.size() - 1);
                }
                size_t pos = url.rfind('/');
                if (pos == TStringBuf::npos) {
                    break;
                } else {
                    url = url.substr(0, pos + 1);
                }
            }
        }
        return {};
    }

    void Handle(TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        auto itRequest = OutgoingRequests.find(event->Get()->Request.Get());
        if (itRequest == OutgoingRequests.end()) {
            YDB_LOG_ERROR("Cache received response to unknown request",
                {"host", event->Get()->Request->Host},
                {"URL", event->Get()->Request->URL});
            return;
        }
        if (event->Get()->Error.empty() && event->Get()->Response) {
            YDB_LOG_DEBUG("Cache received successfull response",
                {"status", event->Get()->Response->Status},
                {"URL", event->Get()->Request->URL});
            TEvHttpProxy::TEvHttpIncomingRequest::TPtr requestEvent = std::move(itRequest->second);
            THttpOutgoingResponsePtr response = event->Get()->Response->Reverse(requestEvent->Get()->Request);
            Send(requestEvent->Sender, new TEvHttpProxy::TEvHttpOutgoingResponse(response), 0, requestEvent->Cookie);
        } else {
            YDB_LOG_WARN("Cache received failed response with error - retrying locally",
                {"error", event->Get()->Error},
                {"URL", event->Get()->Request->URL});
            TActorId handler = GetRequestHandler(itRequest->second->Get()->Request);
            if (handler) {
                Send(itRequest->second->Forward(handler));
            }
        }
        OutgoingRequests.erase(itRequest);
    }

    void Handle(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(TEvHttpProxy::TEvAddListeningPort::TPtr event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(TEvHttpProxy::TEvRegisterHandler::TPtr event) {
        Handlers[event->Get()->Path] = event->Get()->Handler;
        Send(HttpProxyId, new TEvHttpProxy::TEvRegisterHandler(event->Get()->Path, SelfId()));
    }

    void Handle(TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        const THttpIncomingRequest* request = event->Get()->Request.Get();
        TCachePolicy policy = GetCachePolicy(request);
        if (policy.TimeToExpire == TDuration() && policy.RetriesCount == 0) {
            TActorId handler = GetRequestHandler(event->Get()->Request);
            if (handler) {
                Send(event->Forward(handler));
            }
            return;
        }
        auto key = GetCacheKey(request, policy);
        auto it = Cache.find(key);
        if (it != Cache.end()) {
            if (!it->second.Waiters.empty()) {
                YDB_LOG_DEBUG("IncomingForward keep waiting",
                    {"URL", request->URL},
                    {"waitersCount", it->second.Waiters.size()});
                it->second.Waiters.emplace_back(std::move(event));
                return;
            }
            if (it->second.CacheOwnership.ForwardUrl.empty()) {
                YDB_LOG_DEBUG("IncomingForward locally",
                    {"URL", request->URL});
                TActorId handler = GetRequestHandler(event->Get()->Request);
                if (handler) {
                    Send(event->Forward(handler));
                }
            } else {
                YDB_LOG_DEBUG("IncomingForward",
                    {"URL", request->URL},
                    {"forwardURLForDebug", it->second.GetForwardUrlForDebug()},
                    {"timeout", it->second.Timeout});
                THttpOutgoingRequestPtr newRequest = request->Forward(it->second.CacheOwnership.ForwardUrl);
                OutgoingRequests[newRequest.Get()] = std::move(event);
                Send(HttpProxyId, new TEvHttpProxy::TEvHttpOutgoingRequest(newRequest, it->second.Timeout));
            }
        } else {
            auto it = Cache.emplace(key, policy).first;
            it->second.Waiters.emplace_back(std::move(event));
            NActors::TActorSystem* actorSystem = NActors::TlsActivationContext->ActorSystem();
            NActors::TActorIdentity actorId = SelfId();
            auto callback = [actorSystem, actorId, key](TCacheOwnership ownership) {
                actorSystem->Send(actorId, new TEvPrivate::TEvUpdateUrlState(key, ownership));
            };
            if (!GetCacheOwnership(key, std::move(callback))) {
                YDB_LOG_WARN("RefreshGetCacheOwnership failed");
            }
        }
    }

    void Handle(TEvPrivate::TEvUpdateUrlState::TPtr ev) {
        TInstant now = NActors::TActivationContext::Now();
        auto id(ev->Get()->Id);
        const auto& ownership(ev->Get()->CacheOwnership);
        auto it = Cache.find(id);
        if (it == Cache.end()) {
            YDB_LOG_WARN("Cache record not found");
            return;
        }
        if (!ownership.ForwardUrl.empty() || ownership.Deadline > it->second.CacheOwnership.Deadline || it->second.CacheOwnership.Deadline < now) {
            it->second.CacheOwnership = ownership;
            if (it->second.CacheOwnership.ForwardUrl.empty() && it->second.CacheOwnership.Deadline == TInstant()) {
                it->second.CacheOwnership.Deadline = now + TDuration::Seconds(60);
            }
            YDB_LOG_DEBUG("Updating ownership with deadline",
                {"forwardURLForDebug", it->second.CacheOwnership.GetForwardUrlForDebug()},
                {"deadline", it->second.CacheOwnership.Deadline});
        } else {
            YDB_LOG_DEBUG("Keeping ownership with deadline",
                {"forwardURLForDebug", it->second.CacheOwnership.GetForwardUrlForDebug()},
                {"deadline", it->second.CacheOwnership.Deadline});

        }
        auto refreshTime = std::max(now + TDuration::Seconds(10), it->second.GetRefreshTime(now));
        YDB_LOG_DEBUG("SetRefreshTime",
            {"id", id},
            {"refreshTime", refreshTime},
            {"timePeriod", refreshTime - now});
        RefreshQueue.push({
            .Key = id,
            .RefreshTime = refreshTime,
        });
        for (auto& event : it->second.Waiters) {
            Send(event->Forward(SelfId()));
        }
        it->second.Waiters.clear();
    }

    void HandleRefresh() {
        TInstant now = NActors::TActivationContext::Now();
        while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= now) {
            TRefreshRecord rrec = RefreshQueue.top();
            auto key = rrec.Key;
            RefreshQueue.pop();
            auto it = Cache.find(key);
            if (it != Cache.end()) {
                YDB_LOG_DEBUG("Refresh with deadline",
                    {"deadline", it->second.CacheOwnership.Deadline});
                NActors::TActorSystem* actorSystem = NActors::TlsActivationContext->ActorSystem();
                NActors::TActorIdentity actorId = SelfId();
                auto callback = [actorSystem, actorId, key](TCacheOwnership ownership) {
                    actorSystem->Send(actorId, new TEvPrivate::TEvUpdateUrlState(key, ownership));
                };
                if (!GetCacheOwnership(key, std::move(callback))) {
                    YDB_LOG_WARN("RefreshGetCacheOwnership failed");
                }
            } else {
                YDB_LOG_WARN("Refresh key not found",
                    {"key", key});
            }
        }
        Schedule(RefreshPeriod, new NActors::TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
            hFunc(TEvHttpProxy::TEvAddListeningPort, Handle);
            hFunc(TEvHttpProxy::TEvRegisterHandler, Handle);
            hFunc(TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(TEvPrivate::TEvUpdateUrlState, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleRefresh);
        }
    }
};

NActors::IActor* CreateHttpMetaCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy, TGetCacheOwnership getCacheOwnership) {
    return new THttpIncomingDistributedCacheActor(httpProxyId, std::move(cachePolicy), std::move(getCacheOwnership));
}

} // namespace NHttp
