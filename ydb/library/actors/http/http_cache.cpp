#include "http.h"
#include "http_proxy.h"
#include "http_cache.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/http/http.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/digest/multi.h>
#include <util/generic/queue.h>
#include <util/string/cast.h>

namespace NHttp {

static bool StatusSuccess(const TStringBuf& status) {
    return status.StartsWith("2");
}

class THttpOutgoingCacheActor : public NActors::TActorBootstrapped<THttpOutgoingCacheActor>, THttpConfig {
public:
    using TBase = NActors::TActorBootstrapped<THttpOutgoingCacheActor>;
    NActors::TActorId HttpProxyId;
    TGetCachePolicy GetCachePolicy;
    static constexpr TDuration RefreshTimeout = TDuration::Seconds(1);

    struct TCacheKey {
        TString Host;
        TString URL;
        TString Headers;

        operator size_t() const {
            return MultiHash(Host, URL, Headers);
        }

        TString GetId() const {
            return MD5::Calc(Host + ':' + URL + ':' + Headers);
        }
    };

    struct TCacheRecord {
        TInstant RefreshTime;
        TInstant DeathTime;
        TCachePolicy CachePolicy;
        NHttp::THttpOutgoingRequestPtr Request;
        NHttp::THttpOutgoingRequestPtr OutgoingRequest;
        TDuration Timeout;
        NHttp::THttpIncomingResponsePtr Response;
        TString Error;
        TVector<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr> Waiters;

        TCacheRecord(const TCachePolicy cachePolicy)
            : CachePolicy(cachePolicy)
        {}

        bool IsValid() const {
            return Response != nullptr || !Error.empty();
        }

        void UpdateResponse(NHttp::THttpIncomingResponsePtr response, const TString& error, TInstant now) {
            if (error.empty() || Response == nullptr || !CachePolicy.KeepOnError) {
                Response = response;
                Error = error;
            }
            RefreshTime = now + CachePolicy.TimeToRefresh;
            if (CachePolicy.PaceToRefresh) {
                RefreshTime += TDuration::MilliSeconds(RandomNumber<ui64>() % CachePolicy.PaceToRefresh.MilliSeconds());
            }
        }

        TString GetName() const {
            return TStringBuilder() << (Request->Secure ? "https://" : "http://") << Request->Host << Request->URL;
        }
    };

    struct TRefreshRecord {
        TCacheKey Key;
        TInstant RefreshTime;

        bool operator <(const TRefreshRecord& b) const {
            return RefreshTime > b.RefreshTime;
        }
    };

    THashMap<TCacheKey, TCacheRecord> Cache;
    TPriorityQueue<TRefreshRecord> RefreshQueue;
    THashMap<THttpOutgoingRequest*, TCacheKey> OutgoingRequests;

    THttpOutgoingCacheActor(const NActors::TActorId& httpProxyId, TGetCachePolicy getCachePolicy)
        : HttpProxyId(httpProxyId)
        , GetCachePolicy(std::move(getCachePolicy))
    {}

    static constexpr char ActorName[] = "HTTP_OUT_CACHE_ACTOR";

    void Bootstrap() {
        //
        Become(&THttpOutgoingCacheActor::StateWork, RefreshTimeout, new NActors::TEvents::TEvWakeup());
    }

    static TString GetCacheHeadersKey(const NHttp::THttpOutgoingRequest* request, const TCachePolicy& policy) {
        TStringBuilder key;
        if (!policy.HeadersToCacheKey.empty()) {
            NHttp::THeaders headers(request->Headers);
            for (const TString& header : policy.HeadersToCacheKey) {
                key << headers[header];
            }
        }
        return key;
    }

    static TCacheKey GetCacheKey(const NHttp::THttpOutgoingRequest* request, const TCachePolicy& policy) {
        return { ToString(request->Host), ToString(request->URL), GetCacheHeadersKey(request, policy) };
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse::TPtr& event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(NHttp::TEvHttpProxy::TEvAddListeningPort::TPtr& event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(NHttp::TEvHttpProxy::TEvRegisterHandler::TPtr& event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        NHttp::THttpOutgoingRequestPtr request(event->Get()->Request);
        NHttp::THttpIncomingResponsePtr response(event->Get()->Response);
        auto itRequests = OutgoingRequests.find(request.Get());
        if (itRequests == OutgoingRequests.end()) {
            ALOG_ERROR(HttpLog, "Cache received response to unknown request " << request->Host << request->URL);
            return;
        }
        auto key = itRequests->second;
        OutgoingRequests.erase(itRequests);
        auto it = Cache.find(key);
        if (it == Cache.end()) {
            ALOG_ERROR(HttpLog, "Cache received response to unknown cache key " << request->Host << request->URL);
            return;
        }
        TCacheRecord& cacheRecord = it->second;
        cacheRecord.OutgoingRequest.Reset();
        for (auto& waiter : cacheRecord.Waiters) {
            NHttp::THttpIncomingResponsePtr response2;
            TString error2;
            if (response != nullptr) {
                response2 = response->Duplicate(waiter->Get()->Request);
            }
            if (!event->Get()->Error.empty()) {
                error2 = event->Get()->Error;
            }
            Send(waiter->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(waiter->Get()->Request, response2, error2));
        }
        cacheRecord.Waiters.clear();
        TString error;
        if (event->Get()->Error.empty()) {
            if (event->Get()->Response != nullptr && !StatusSuccess(event->Get()->Response->Status)) {
                error = event->Get()->Response->Message;
            }
        } else {
            error = event->Get()->Error;
        }
        if (!error.empty()) {
            ALOG_WARN(HttpLog, "Error from " << cacheRecord.GetName() << ": " << error);
        }
        ALOG_DEBUG(HttpLog, "OutgoingUpdate " << cacheRecord.GetName());
        cacheRecord.UpdateResponse(response, event->Get()->Error, NActors::TActivationContext::Now());
        RefreshQueue.push({it->first, it->second.RefreshTime});
        ALOG_DEBUG(HttpLog, "OutgoingSchedule " << cacheRecord.GetName() << " at " << cacheRecord.RefreshTime << " until " << cacheRecord.DeathTime);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const NHttp::THttpOutgoingRequest* request = event->Get()->Request.Get();
        auto policy = GetCachePolicy(request);
        if (policy.TimeToExpire == TDuration()) {
            Send(event->Forward(HttpProxyId));
            return;
        }
        auto key = GetCacheKey(request, policy);
        auto it = Cache.find(key);
        if (it != Cache.end()) {
            if (it->second.IsValid()) {
                ALOG_DEBUG(HttpLog, "OutgoingRespond "
                            << it->second.GetName()
                            << " ("
                            << ((it->second.Response != nullptr) ? ToString(it->second.Response->Size()) : TString("error"))
                            << ")");
                NHttp::THttpIncomingResponsePtr response = it->second.Response;
                if (response != nullptr) {
                    response = response->Duplicate(event->Get()->Request);
                }
                Send(event->Sender,
                         new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(event->Get()->Request,
                                                                          response,
                                                                          it->second.Error));
                it->second.DeathTime = NActors::TActivationContext::Now() + it->second.CachePolicy.TimeToExpire; // prolong active cache items
                return;
            }
        } else {
            it = Cache.emplace(key, policy).first;
            it->second.Request = event->Get()->Request;
            it->second.Timeout = event->Get()->Timeout;
            it->second.OutgoingRequest = it->second.Request->Duplicate();
            OutgoingRequests[it->second.OutgoingRequest.Get()] = key;
            ALOG_DEBUG(HttpLog, "OutgoingInitiate " << it->second.GetName());
            Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(it->second.OutgoingRequest, it->second.Timeout));
        }
        it->second.DeathTime = NActors::TActivationContext::Now() + it->second.CachePolicy.TimeToExpire;
        it->second.Waiters.emplace_back(std::move(event));
    }

    void HandleRefresh() {
        while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= NActors::TActivationContext::Now()) {
            TRefreshRecord rrec = RefreshQueue.top();
            RefreshQueue.pop();
            auto it = Cache.find(rrec.Key);
            if (it != Cache.end()) {
                if (it->second.DeathTime > NActors::TActivationContext::Now()) {
                    ALOG_DEBUG(HttpLog, "OutgoingRefresh " << it->second.GetName());
                    it->second.OutgoingRequest = it->second.Request->Duplicate();
                    OutgoingRequests[it->second.OutgoingRequest.Get()] = it->first;
                    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(it->second.OutgoingRequest, it->second.Timeout));
                } else {
                    ALOG_DEBUG(HttpLog, "OutgoingForget " << it->second.GetName());
                    if (it->second.OutgoingRequest) {
                        OutgoingRequests.erase(it->second.OutgoingRequest.Get());
                    }
                    Cache.erase(it);
                }
            }
        }
        Schedule(RefreshTimeout, new NActors::TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvAddListeningPort, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvRegisterHandler, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleRefresh);
        }
    }
};

const TDuration THttpOutgoingCacheActor::RefreshTimeout;

class THttpIncomingCacheActor : public NActors::TActorBootstrapped<THttpIncomingCacheActor>, THttpConfig {
public:
    using TBase = NActors::TActorBootstrapped<THttpIncomingCacheActor>;
    NActors::TActorId HttpProxyId;
    TGetCachePolicy GetCachePolicy;
    static constexpr TDuration RefreshTimeout = TDuration::Seconds(1);
    THashMap<TString, TActorId> Handlers;

    struct TCacheKey {
        TString URL;
        TString Headers;

        operator size_t() const {
            return MultiHash(URL, Headers);
        }

        TString GetId() const {
            return MD5::Calc(URL + ':' + Headers);
        }
    };

    struct TCacheRecord {
        TInstant RefreshTime;
        TInstant DeathTime;
        TCachePolicy CachePolicy;
        TString CacheId;
        NHttp::THttpIncomingRequestPtr Request;
        TDuration Timeout;
        NHttp::THttpOutgoingResponsePtr Response;
        TVector<NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr> Waiters;
        ui32 Retries = 0;
        bool Enqueued = false;

        TCacheRecord(const TCachePolicy cachePolicy)
            : CachePolicy(cachePolicy)
        {}

        bool IsValid() const {
            return Response != nullptr;
        }

        void InitRequest(NHttp::THttpIncomingRequestPtr request) {
            Request = request;
            if (CachePolicy.TimeToExpire) {
                DeathTime = NActors::TlsActivationContext->Now() + CachePolicy.TimeToExpire;
            }
        }

        void UpdateResponse(NHttp::THttpOutgoingResponsePtr response, const TString& error, TInstant now) {
            if (error.empty() || !CachePolicy.KeepOnError) {
                Response = response;
            }
            Retries = 0;
            if (CachePolicy.TimeToRefresh) {
                RefreshTime = now + CachePolicy.TimeToRefresh;
                if (CachePolicy.PaceToRefresh) {
                    RefreshTime += TDuration::MilliSeconds(RandomNumber<ui64>() % CachePolicy.PaceToRefresh.MilliSeconds());
                }
            }
        }

        void UpdateExpireTime() {
            if (CachePolicy.TimeToExpire) {
                DeathTime = NActors::TlsActivationContext->Now() + CachePolicy.TimeToExpire;
            }
        }

        TString GetName() const {
            return TStringBuilder() << (Request->Endpoint->Secure ? "https://" : "http://") << Request->Host << Request->URL
                << " (" << CacheId << ")";
        }
    };

    struct TRefreshRecord {
        TCacheKey Key;
        TInstant RefreshTime;

        bool operator <(const TRefreshRecord& b) const {
            return RefreshTime > b.RefreshTime;
        }
    };

    THashMap<TCacheKey, TCacheRecord> Cache;
    TPriorityQueue<TRefreshRecord> RefreshQueue;
    THashMap<THttpIncomingRequest*, TCacheKey> IncomingRequests;

    THttpIncomingCacheActor(const NActors::TActorId& httpProxyId, TGetCachePolicy getCachePolicy)
        : HttpProxyId(httpProxyId)
        , GetCachePolicy(std::move(getCachePolicy))
    {}

    static constexpr char ActorName[] = "HTTP_IN_CACHE_ACTOR";

    void Bootstrap() {
        //
        Become(&THttpIncomingCacheActor::StateWork, RefreshTimeout, new NActors::TEvents::TEvWakeup());
    }

    static TString GetCacheHeadersKey(const NHttp::THttpIncomingRequest* request, const TCachePolicy& policy) {
        TStringBuilder key;
        if (!policy.HeadersToCacheKey.empty()) {
            NHttp::THeaders headers(request->Headers);
            for (const TString& header : policy.HeadersToCacheKey) {
                key << headers[header];
            }
        }
        return key;
    }

    static TCacheKey GetCacheKey(const NHttp::THttpIncomingRequest* request, const TCachePolicy& policy) {
        return {ToString(request->URL), GetCacheHeadersKey(request, policy)};
    }

    TActorId GetRequestHandler(NHttp::THttpIncomingRequestPtr request) {
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

    void SendCacheRequest(const TCacheKey& cacheKey, TCacheRecord& cacheRecord) {
        cacheRecord.Request = cacheRecord.Request->Duplicate();
        cacheRecord.Request->AcceptEncoding.Clear(); // disable compression
        IncomingRequests[cacheRecord.Request.Get()] = cacheKey;
        TActorId handler = GetRequestHandler(cacheRecord.Request);
        if (handler) {
            Send(handler, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(cacheRecord.Request));
        } else {
            ALOG_ERROR(HttpLog, "Can't find cache handler for " << cacheRecord.GetName());
        }
    }

    void DropCacheRecord(THashMap<TCacheKey, TCacheRecord>::iterator it) {
        if (it->second.Request) {
            IncomingRequests.erase(it->second.Request.Get());
        }
        for (auto& waiter : it->second.Waiters) {
            NHttp::THttpOutgoingResponsePtr response;
            response = waiter->Get()->Request->CreateResponseGatewayTimeout("Timeout", "text/plain");
            Send(waiter->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        }
        Cache.erase(it);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(NHttp::TEvHttpProxy::TEvAddListeningPort::TPtr& event) {
        Send(event->Forward(HttpProxyId));
    }

    void Handle(NHttp::TEvHttpProxy::TEvRegisterHandler::TPtr& event) {
        Handlers[event->Get()->Path] = event->Get()->Handler;
        Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(event->Get()->Path, SelfId()));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse::TPtr& event) {
        NHttp::THttpIncomingRequestPtr request(event->Get()->Response->GetRequest());
        NHttp::THttpOutgoingResponsePtr response(event->Get()->Response);
        auto itRequests = IncomingRequests.find(request.Get());
        if (itRequests == IncomingRequests.end()) {
            ALOG_ERROR(HttpLog, "Cache received response to unknown request " << request->Host << request->URL);
            return;
        }

        TCacheKey key = itRequests->second;
        auto it = Cache.find(key);
        if (it == Cache.end()) {
            ALOG_ERROR(HttpLog, "Cache received response to unknown cache key " << request->Host << request->URL);
            return;
        }

        IncomingRequests.erase(itRequests);
        TCacheRecord& cacheRecord = it->second;
        TStringBuf status;
        TString error;

        if (event->Get()->Response != nullptr) {
            status = event->Get()->Response->Status;
            if (!StatusSuccess(status)) {
                error = event->Get()->Response->Message;
            }
        }
        if (cacheRecord.CachePolicy.RetriesCount > 0) {
            auto itStatusToRetry = std::find(cacheRecord.CachePolicy.StatusesToRetry.begin(), cacheRecord.CachePolicy.StatusesToRetry.end(), status);
            if (itStatusToRetry != cacheRecord.CachePolicy.StatusesToRetry.end()) {
                if (cacheRecord.Retries < cacheRecord.CachePolicy.RetriesCount) {
                    ++cacheRecord.Retries;
                    ALOG_WARN(HttpLog, "IncomingRetry " << cacheRecord.GetName() << ": " << status << " " << error);
                    SendCacheRequest(key, cacheRecord);
                    return;
                }
            }
        }
        for (auto& waiter : cacheRecord.Waiters) {
            NHttp::THttpOutgoingResponsePtr response2;
            response2 = response->Duplicate(waiter->Get()->Request);
            Send(waiter->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response2));
        }
        cacheRecord.Waiters.clear();
        if (!error.empty()) {
            ALOG_WARN(HttpLog, "Error from " << cacheRecord.GetName() << ": " << error);
            if (!cacheRecord.Response) {
                ALOG_DEBUG(HttpLog, "IncomingDiscard " << cacheRecord.GetName());
                DropCacheRecord(it);
                return;
            }
        }
        if (cacheRecord.CachePolicy.TimeToRefresh) {
            ALOG_DEBUG(HttpLog, "IncomingUpdate " << cacheRecord.GetName());
            cacheRecord.UpdateResponse(response, error, NActors::TActivationContext::Now());
            if (!cacheRecord.Enqueued) {
                RefreshQueue.push({it->first, it->second.RefreshTime});
                cacheRecord.Enqueued = true;
            }
            ALOG_DEBUG(HttpLog, "IncomingSchedule " << cacheRecord.GetName() << " at " << cacheRecord.RefreshTime << " until " << cacheRecord.DeathTime);
        } else {
            ALOG_DEBUG(HttpLog, "IncomingDrop " << cacheRecord.GetName());
            DropCacheRecord(it);
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event) {
        const NHttp::THttpIncomingRequest* request = event->Get()->Request.Get();
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
        if (it != Cache.end() && !policy.DiscardCache) {
            it->second.UpdateExpireTime();
            if (it->second.IsValid()) {
                ALOG_DEBUG(HttpLog, "IncomingRespond "
                            << it->second.GetName()
                            << " ("
                            << ((it->second.Response != nullptr) ? ToString(it->second.Response->Size()) : TString("error"))
                            << ")");
                NHttp::THttpOutgoingResponsePtr response = it->second.Response;
                if (response != nullptr) {
                    response = response->Duplicate(event->Get()->Request);
                }
                Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
                return;
            }
        } else {
            it = Cache.emplace(key, policy).first;
            it->second.CacheId = key.GetId(); // for debugging
            it->second.InitRequest(event->Get()->Request);
            if (policy.DiscardCache) {
                ALOG_DEBUG(HttpLog, "IncomingDiscardCache " << it->second.GetName());
            }
            ALOG_DEBUG(HttpLog, "IncomingInitiate " << it->second.GetName());
            SendCacheRequest(key, it->second);
        }
        it->second.Waiters.emplace_back(std::move(event));
    }

    void HandleRefresh() {
        while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= NActors::TActivationContext::Now()) {
            TRefreshRecord rrec = RefreshQueue.top();
            RefreshQueue.pop();
            auto it = Cache.find(rrec.Key);
            if (it != Cache.end()) {
                it->second.Enqueued = false;
                if (it->second.DeathTime > NActors::TActivationContext::Now()) {
                    ALOG_DEBUG(HttpLog, "IncomingRefresh " << it->second.GetName());
                    SendCacheRequest(it->first, it->second);
                } else {
                    ALOG_DEBUG(HttpLog, "IncomingForget " << it->second.GetName());
                    DropCacheRecord(it);
                }
            }
        }
        Schedule(RefreshTimeout, new NActors::TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvAddListeningPort, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvRegisterHandler, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleRefresh);
        }
    }
};

TCachePolicy GetDefaultCachePolicy(const THttpRequest* request, const TCachePolicy& defaultPolicy) {
    TCachePolicy policy = defaultPolicy;
    THeaders headers(request->Headers);
    TStringBuf cacheControl(headers["Cache-Control"]);
    while (TStringBuf cacheItem = cacheControl.NextTok(',')) {
        cacheItem = Trim(cacheItem, ' ');
        if (cacheItem == "no-store" || cacheItem == "no-cache") {
            policy.DiscardCache = true;
        }
        TStringBuf itemName = cacheItem.NextTok('=');
        itemName = TrimEnd(itemName, ' ');
        cacheItem = TrimBegin(cacheItem, ' ');
        if (itemName == "max-age") {
            policy.TimeToRefresh = policy.TimeToExpire = TDuration::Seconds(FromString(cacheItem));
        }
        if (itemName == "min-fresh") {
            policy.TimeToRefresh = policy.TimeToExpire = TDuration::Seconds(FromString(cacheItem));
        }
        if (itemName == "stale-if-error") {
            policy.KeepOnError = true;
        }
    }
    return policy;
}

NActors::IActor* CreateHttpCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy) {
    return new THttpOutgoingCacheActor(httpProxyId, std::move(cachePolicy));
}

NActors::IActor* CreateOutgoingHttpCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy) {
    return new THttpOutgoingCacheActor(httpProxyId, std::move(cachePolicy));
}

NActors::IActor* CreateIncomingHttpCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy) {
    return new THttpIncomingCacheActor(httpProxyId, std::move(cachePolicy));
}

}
