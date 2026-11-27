#include "remote_embedder_http.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/scheduler_cookie.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <util/generic/singleton.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

namespace NKnn {
namespace {

using namespace NActors;
using NHttp::TEvHttpProxy;

constexpr size_t MaxActiveRequests = 8;
constexpr size_t MaxQueuedRequests = 64;
constexpr size_t MaxHeaderBytes = 64 * 1024;
constexpr size_t MaxBodyBytes = 16 * 1024 * 1024;
constexpr size_t MaxChunkFramingBytes = 1024 * 1024;
constexpr ui32 ProxyPool = 1;

struct TEvEmbedderRequest final : TEventLocal<TEvEmbedderRequest, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    TEvEmbedderRequest(NHttp::THttpOutgoingRequestPtr request, TMonotonic deadline)
        : Request(std::move(request))
        , Deadline(deadline)
    {}

    NHttp::THttpOutgoingRequestPtr Request;
    TMonotonic Deadline;
};

class TEmbedderRequestDispatcher final : public TActorBootstrapped<TEmbedderRequestDispatcher> {
public:
    static constexpr char ActorName[] = "REMOTE_EMBEDDER_HTTP";

    void Bootstrap() {
        Become(&TEmbedderRequestDispatcher::StateWork);
    }

private:
    struct TActiveRequest {
        TActorId Proxy;
        TActorId Recipient;
        ui64 Cookie;
        TMonotonic Deadline;
        TSchedulerCookieHolder Timer;
    };

    STRICT_STFUNC(StateWork,
        hFunc(TEvEmbedderRequest, HandleRequest);
        hFunc(TEvHttpProxy::TEvHttpIncomingResponse, HandleResponse);
        hFunc(TEvents::TEvWakeup, HandleDeadline);
    )

    void HandleRequest(TEvEmbedderRequest::TPtr& event) {
        if (Active_.size() < MaxActiveRequests) {
            StartRequest(event);
        } else if (Queued_.size() < MaxQueuedRequests) {
            Queued_.push_back(std::move(event));
        } else {
            ReplyUnavailable(event, "HTTP proxy queue is full");
        }
    }

    void StartRequest(const TEvEmbedderRequest::TPtr& event) {
        auto* request = event->Get();
        const auto now = TActivationContext::Monotonic();
        if (now >= request->Deadline) {
            ReplyUnavailable(event, "Request deadline expired");
            return;
        }
        const auto cookie = ++NextCookie_;
        auto& active = Active_.try_emplace(cookie).first->second;
        // Each slot owns its proxy so expiring an attempt can close its
        // connection without interrupting the other requests.
        if (IdleProxies_.empty()) {
            active.Proxy = Register(NHttp::CreateHttpProxy({}), TMailboxType::HTSwap, ProxyPool);
        } else {
            active.Proxy = IdleProxies_.back();
            IdleProxies_.pop_back();
        }
        active.Recipient = event->Sender;
        active.Cookie = event->Cookie;
        active.Deadline = request->Deadline;
        active.Timer.Reset(ISchedulerCookie::Make2Way());
        Schedule(request->Deadline, new TEvents::TEvWakeup(cookie), active.Timer.Get());
        Send(active.Proxy, new TEvHttpProxy::TEvHttpOutgoingRequest(std::move(request->Request), request->Deadline - now), 0, cookie);
    }

    void HandleResponse(TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        const auto it = Active_.find(event->Cookie);
        if (it == Active_.end()) {
            return;
        }
        auto& active = it->second;
        IdleProxies_.push_back(active.Proxy);
        if (TActivationContext::Monotonic() >= active.Deadline) {
            ReplyExpired(active);
        } else {
            Send(active.Recipient, event->Release().Release(), 0, active.Cookie);
        }
        Active_.erase(it);
        StartQueuedRequests();
    }

    void HandleDeadline(TEvents::TEvWakeup::TPtr& event) {
        const auto it = Active_.find(event->Get()->Tag);
        if (it == Active_.end()) {
            return;
        }
        auto& active = it->second;
        Send(active.Proxy, new TEvents::TEvPoisonPill());
        ReplyExpired(active);
        Active_.erase(it);
        StartQueuedRequests();
    }

    void StartQueuedRequests() {
        while (!Queued_.empty() && Active_.size() < MaxActiveRequests) {
            auto request = std::move(Queued_.front());
            Queued_.pop_front();
            StartRequest(request);
        }
    }

    void ReplyUnavailable(const TEvEmbedderRequest::TPtr& event, const TString& error) {
        Send(event->Sender, new TEvHttpProxy::TEvHttpIncomingResponse(event->Get()->Request, nullptr, error), 0, event->Cookie);
    }

    void ReplyExpired(const TActiveRequest& active) {
        Send(active.Recipient, new TEvHttpProxy::TEvHttpIncomingResponse(nullptr, nullptr, "Request deadline expired"), 0, active.Cookie);
    }

    ui64 NextCookie_ = 0;
    THashMap<ui64, TActiveRequest> Active_;
    TVector<TActorId> IdleProxies_;
    TDeque<TEvEmbedderRequest::TPtr> Queued_;
};

TActorId MakeDispatcherId() {
    return TActorId(0, "embedderhttp");
}

// A private actor system keeps the proxy's request and response traces out of
// the node logger, including when HTTP tracing is enabled on the node.
class TEmbedderHttpRuntime {
public:
    TEmbedderHttpRuntime() {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 2;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "knn-http-control");
        // The stock proxy resolves uncached hosts synchronously.
        setup->Executors[ProxyPool] = new TBasicExecutorPool(ProxyPool, 2, 10, "knn-http-proxy");
        setup->Scheduler = new TBasicSchedulerThread;
        setup->LocalServices.emplace_back(MakePollerActorId(),
            TActorSetupCmd(CreatePollerActor(), TMailboxType::Simple, 0));
        setup->LocalServices.emplace_back(MakeDispatcherId(),
            TActorSetupCmd(new TEmbedderRequestDispatcher(), TMailboxType::HTSwap, 0));
        System = std::make_unique<TActorSystem>(setup);
        System->Start();
    }

    std::unique_ptr<TActorSystem> System;
};

TActorSystem& GetActorSystem() {
    return *Singleton<TEmbedderHttpRuntime>()->System;
}

class TDelayActor final : public TActorBootstrapped<TDelayActor> {
public:
    TDelayActor(TDuration delay, NThreading::TPromise<void> promise)
        : Delay_(delay)
        , Promise_(std::move(promise))
    {}

    static constexpr char ActorName[] = "REMOTE_EMBEDDER_DELAY";

    void Bootstrap() {
        Schedule(Delay_, new TEvents::TEvWakeup());
        Become(&TDelayActor::StateWork);
    }

private:
    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TEvWakeup::EventType, Complete);
    )

    void Complete() {
        Promise_.SetValue();
        PassAway();
    }

    const TDuration Delay_;
    NThreading::TPromise<void> Promise_;
};

} // namespace

NThreading::TFuture<TRemoteEmbedderHttpResponse> PostRemoteEmbedderRequest(
    TStringBuf authority, bool useTls, TStringBuf path, TStringBuf authorization, TStringBuf body, TDuration timeout)
{
    auto& system = GetActorSystem();
    const auto proxy = system.LookupLocalService(MakeDispatcherId());
    Y_ENSURE(proxy, "RemoteEmbedder: HTTP proxy is unavailable");

    auto request = MakeIntrusive<NHttp::THttpOutgoingRequest>();
    request->Secure = useTls;
    request->InitRequest("POST", path, "HTTP", "1.1");
    request->Set<&NHttp::THttpRequest::Host>(authority);
    request->Set<&NHttp::THttpRequest::Connection>("close");
    request->Set<&NHttp::THttpRequest::ContentType>("application/json");
    request->Set("Accept-Encoding", "identity");
    if (!authorization.empty()) {
        request->Set("Authorization", authorization);
    }
    request->SetBody(body);

    auto event = MakeHolder<TEvEmbedderRequest>(std::move(request), system.Monotonic() + timeout);
    return system.Ask<TEvHttpProxy::TEvHttpIncomingResponse>(proxy, std::move(event), timeout)
        .Apply([](const auto& future) -> TRemoteEmbedderHttpResponse {
            if (future.HasException()) {
                return {0, {}};
            }
            const auto& reply = *future.GetValue();
            if (!reply.Error.empty() || !reply.Response) {
                return {0, {}};
            }
            const auto& response = *reply.Response;
            if (!response.IsReady() || response.GetHeadersSize() > MaxHeaderBytes) {
                return {502, {}, true};
            }
            const auto status = FromStringWithDefault<ui32>(response.Status, 0);
            if (status < 200 || status >= 300) {
                return {status, {}};
            }
            const NHttp::THeaders headers(response.Headers);
            for (const auto& [name, value] : headers.Headers) {
                if ((AsciiEqualsIgnoreCase(name, "Content-Encoding") && !AsciiEqualsIgnoreCase(StripString(value), "identity")) ||
                    (AsciiEqualsIgnoreCase(name, "Transfer-Encoding") && !AsciiEqualsIgnoreCase(StripString(value), "chunked")) ||
                    (AsciiEqualsIgnoreCase(name, "Content-Length") && StripString(value) != response.ContentLength)) {
                    return {502, {}, true};
                }
            }
            if (response.Body.size() > MaxBodyBytes ||
                (response.IsChunkedEncoding() &&
                 response.Size() > response.GetHeadersSize() + response.Body.size() + MaxChunkFramingBytes)) {
                return {502, {}, true};
            }
            return {status, TString(response.Body)};
        });
}

NThreading::TFuture<void> WaitBeforeRemoteEmbedderRequest(TDuration delay) {
    auto promise = NThreading::NewPromise();
    GetActorSystem().Register(new TDelayActor(delay, promise));
    return promise.GetFuture();
}

} // namespace NKnn
