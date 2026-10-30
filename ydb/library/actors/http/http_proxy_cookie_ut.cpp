#include "http.h"
#include "http_proxy.h"
#include "http_cache.h"

#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace {

using namespace NActors;
using namespace NHttp;
using TEv = TEvHttpProxy;

constexpr ui64 CookieA = 0xFEDCBA9876543210ULL;
constexpr ui64 CookieB = 0x0123456789ABCDEFULL;
constexpr ui64 BackendCookie = 0xA55AA55A11223344ULL;
const TDuration WaitBudget = TDuration::Seconds(10);

enum class EProtocol { Http1, Http2 };
enum class EBackendResult { Success, Error };

template <class TEvent>
auto Receive(TTestActorRuntimeBase& runtime, TActorId edge) {
    auto event = runtime.GrabEdgeEvent<TEvent>(edge, WaitBudget);
    UNIT_ASSERT_C(event, "Missing event " << TEvent::EventType << " for " << edge);
    return event;
}

// Check the complete response family, so an unexpected event cannot be skipped.
template <class TEvent>
TEvent* ReceiveReply(TTestActorRuntimeBase& runtime, TActorId caller, TAutoPtr<IEventHandle>& handle) {
    runtime.GrabEdgeEvents<TEv::TEvHttpIncomingResponse, TEv::TEvHttpIncompleteIncomingResponse,
        TEv::TEvHttpIncomingDataChunk>(handle, WaitBudget);
    UNIT_ASSERT_C(handle, "Missing HTTP reply for " << caller);
    UNIT_ASSERT_VALUES_EQUAL(handle->GetRecipientRewrite(), caller);
    UNIT_ASSERT_VALUES_EQUAL(handle->GetTypeRewrite(), TEvent::EventType);
    return handle->Get<TEvent>();
}

void AssertResponse(const TEv::TEvHttpIncomingResponse& response, THttpOutgoingRequestPtr request, TStringBuf body) {
    UNIT_ASSERT_VALUES_EQUAL(response.Request.Get(), request.Get());
    UNIT_ASSERT_VALUES_EQUAL(response.Error, "");
    UNIT_ASSERT(response.Response);
    UNIT_ASSERT_VALUES_EQUAL(response.Response->Status, "200");
    UNIT_ASSERT_VALUES_EQUAL(response.Response->Body, body);
}

struct TSocketFixture {
    TTestActorRuntimeBase Runtime{1, true};
    TPortManager Ports;
    TIpPort Port = Ports.GetTcpPort();
    TActorId Proxy;
    TActorId Server;
    TActorId Caller;
    EProtocol Protocol;

    explicit TSocketFixture(EProtocol protocol)
        : Protocol(protocol)
    {
        Runtime.Initialize();
        Proxy = Runtime.Register(CreateHttpProxy());
        Server = Runtime.AllocateEdgeActor();
        Caller = Runtime.AllocateEdgeActor();
        auto* listen = new TEv::TEvAddListeningPort(Port);
        listen->AllowHttp2 = protocol == EProtocol::Http2;
        Runtime.Send(new IEventHandle(Proxy, Server, listen), 0, true);
        Receive<TEv::TEvConfirmListen>(Runtime, Server);
        Runtime.Send(new IEventHandle(Proxy, Server, new TEv::TEvRegisterHandler("/cookie", Server)), 0, true);
    }

    THttpOutgoingRequestPtr Request(TStringBuf tag) {
        auto request = THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(Port) + "/cookie");
        request->Set("X-Test-Request", tag);
        return request;
    }

    TEv::TEvHttpOutgoingRequest* RequestEvent(THttpOutgoingRequestPtr request) {
        auto* event = new TEv::TEvHttpOutgoingRequest(request);
        event->UseHttp2 = Protocol == EProtocol::Http2;
        event->AllowConnectionReuse = true;
        return event;
    }

    void SendRequest(TActorId destination, THttpOutgoingRequestPtr request, ui64 cookie) {
        auto* event = RequestEvent(request);
        // Exercise the legacy omitted-cookie constructor as well.
        Runtime.Send(cookie ? new IEventHandle(destination, Caller, event, 0, cookie)
                            : new IEventHandle(destination, Caller, event), 0, true);
    }

    void Reply(const TEv::TEvHttpIncomingRequest::TPtr& incoming, TStringBuf body) {
        Runtime.Send(new IEventHandle(incoming->Sender, Server,
            new TEv::TEvHttpOutgoingResponse(incoming->Get()->Request->CreateResponseOK(body, "text/plain"))), 0, true);
    }

    TActorId ExpectResponse(THttpOutgoingRequestPtr request, ui64 cookie, TStringBuf body) {
        TAutoPtr<IEventHandle> handle;
        const auto* response = ReceiveReply<TEv::TEvHttpIncomingResponse>(Runtime, Caller, handle);
        AssertResponse(*response, request, body);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, cookie);
        return handle->Sender;
    }
};

struct TEvRunRequest : TEventLocal<TEvRunRequest, TEvents::ES_PRIVATE + 1> {
    THttpOutgoingRequestPtr Request;
    TDuration LocalTimeout;

    TEvRunRequest(THttpOutgoingRequestPtr request, TDuration localTimeout)
        : Request(std::move(request))
        , LocalTimeout(localTimeout)
    {}
};

class TCoroutineClient : public TActorBootstrapped<TCoroutineClient> {
public:
    TCoroutineClient(TActorId proxy, TActorId lateReplies)
        : Proxy(proxy)
        , LateReplies(lateReplies)
    {}

    void Bootstrap() {
        Become(&TCoroutineClient::StateWork);
    }

private:
    void Handle(TEvRunRequest::TPtr event) {
        auto reply = co_await WithTimeout(event->Get()->LocalTimeout, RequestOnce(event->Get()->Request));
        if (reply) {
            Send((*reply)->Forward(event->Sender));
        } else {
            Send(event->Sender, new TEvents::TEvWakeup(), 0, event->Cookie);
        }
        co_return;
    }

    async<TEv::TEvHttpIncomingResponse::TPtr> RequestOnce(THttpOutgoingRequestPtr request) {
        co_return co_await ActorRequest<TEv::TEvHttpIncomingResponse>(Proxy,
            new TEv::TEvHttpOutgoingRequest(request, TDuration::Seconds(30)));
    }

    void HandleLateReply(TEv::TEvHttpIncomingResponse::TPtr& event) {
        Send(event->Forward(LateReplies));
    }

    void Stop() {
        Become(&TCoroutineClient::StateDying);
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRunRequest, Handle);
            hFunc(TEv::TEvHttpIncomingResponse, HandleLateReply);
            cFunc(TEvents::TEvPoison::EventType, Stop);
        }
    }

    STFUNC(StateDying) {
        Y_UNUSED(ev);
    }

    const TActorId Proxy;
    const TActorId LateReplies;
};

TEv::TEvHttpIncomingResponse::TPtr ReceiveCoroutineReply(TTestActorRuntimeBase& runtime, TActorId caller) {
    auto handle = runtime.GrabEdgeEvent<IEventHandle>(caller, WaitBudget);
    UNIT_ASSERT_C(handle, "Missing coroutine result");
    UNIT_ASSERT_C(handle->GetTypeRewrite() == TEv::TEvHttpIncomingResponse::EventType,
        "ActorRequest local wait timed out without a cookie-correlated HTTP response");
    return static_cast<TEventHandle<TEv::TEvHttpIncomingResponse>*>(handle.Release());
}

void CheckCacheCookies(EBackendResult result) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();
    const auto backend = runtime.AllocateEdgeActor();
    const auto caller = runtime.AllocateEdgeActor();
    TCachePolicy policy;
    policy.TimeToExpire = TDuration::Minutes(60);
    policy.TimeToRefresh = TDuration::Minutes(30);
    const auto cache = runtime.Register(CreateOutgoingHttpCache(backend, [policy](const THttpRequest*) { return policy; }));
    const std::array<ui64, 3> cookies{CookieA, CookieB, 0};
    std::array<THttpOutgoingRequestPtr, 3> requests;
    for (size_t i = 0; i < requests.size(); ++i) {
        requests[i] = THttpOutgoingRequest::CreateRequestGet("http://example.com/cookie");
        auto* event = new TEv::TEvHttpOutgoingRequest(requests[i]);
        runtime.Send(cookies[i] ? new IEventHandle(cache, caller, event, 0, cookies[i])
                               : new IEventHandle(cache, caller, event), 0, true);
    }
    auto fetch = Receive<TEv::TEvHttpOutgoingRequest>(runtime, backend);
    const auto fetchRequest = fetch->Get()->Request;
    THttpIncomingResponsePtr backendResponse;
    TString error;
    if (result == EBackendResult::Success) {
        backendResponse = new THttpIncomingResponse(fetchRequest);
        const TString bytes = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\ncache-cookie";
        backendResponse->EnsureEnoughSpaceAvailable(bytes.size());
        memcpy(backendResponse->Pos(), bytes.data(), bytes.size());
        backendResponse->Advance(bytes.size());
        UNIT_ASSERT(backendResponse->IsDone());
    } else {
        error = "cookie-test backend failure";
    }
    runtime.Send(new IEventHandle(fetch->Sender, backend,
        new TEv::TEvHttpIncomingResponse(fetchRequest, backendResponse, error), 0, BackendCookie), 0, true);
    auto assertReply = [&](THttpOutgoingRequestPtr request, ui64 cookie, const TEv::TEvHttpIncomingResponse* response, const IEventHandle& handle) {
        UNIT_ASSERT_VALUES_EQUAL(response->Request.Get(), request.Get());
        UNIT_ASSERT_VALUES_EQUAL(response->Error, error);
        if (result == EBackendResult::Success) {
            UNIT_ASSERT(response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->Response->GetRequest().Get(), request.Get());
            UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");
            UNIT_ASSERT_VALUES_EQUAL(response->Response->Body, "cache-cookie");
        } else {
            UNIT_ASSERT(!response->Response);
        }
        UNIT_ASSERT_VALUES_EQUAL(handle.Cookie, cookie);
    };
    std::array<bool, 3> seen{};
    for (size_t count = 0; count < requests.size(); ++count) {
        TAutoPtr<IEventHandle> handle;
        const auto* response = ReceiveReply<TEv::TEvHttpIncomingResponse>(runtime, caller, handle);
        size_t index = 0;
        while (index < requests.size() && requests[index].Get() != response->Request.Get()) {
            ++index;
        }
        UNIT_ASSERT(index < requests.size());
        UNIT_ASSERT(!seen[index]);
        seen[index] = true;
        assertReply(requests[index], cookies[index], response, *handle);
    }
    for (ui64 cookie : {CookieB, ui64(0)}) {
        auto request = THttpOutgoingRequest::CreateRequestGet("http://example.com/cookie");
        auto* event = new TEv::TEvHttpOutgoingRequest(request);
        runtime.Send(cookie ? new IEventHandle(cache, caller, event, 0, cookie)
                            : new IEventHandle(cache, caller, event), 0, true);
        TAutoPtr<IEventHandle> handle;
        const auto* response = ReceiveReply<TEv::TEvHttpIncomingResponse>(runtime, caller, handle);
        assertReply(request, cookie, response, *handle);
    }
}

}

Y_UNIT_TEST_SUITE(HttpProxyCookies) {
    Y_UNIT_TEST(ReusedConnectionEchoesCurrentRequestCookieIncludingZero) {
        TSocketFixture f(EProtocol::Http1);
        const std::array<ui64, 3> cookies{CookieA, CookieB, 0};
        const std::array<TStringBuf, 3> bodies{"first", "second", "third"};
        TActorId outgoingConnection, incomingConnection;
        for (size_t i = 0; i < cookies.size(); ++i) {
            auto request = f.Request("same");
            f.SendRequest(f.Proxy, request, cookies[i]);
            auto incoming = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
            if (i == 0) {
                incomingConnection = incoming->Sender;
            }
            UNIT_ASSERT_VALUES_EQUAL(incoming->Sender, incomingConnection);
            f.Reply(incoming, bodies[i]);
            const auto sender = f.ExpectResponse(request, cookies[i], bodies[i]);
            if (i == 0) {
                outgoingConnection = sender;
            }
            UNIT_ASSERT_VALUES_EQUAL(sender, outgoingConnection);
            if (i != 2) {
                // The actor's preceding turn enqueues connection availability before this barrier.
                const auto control = f.Runtime.AllocateEdgeActor();
                f.Runtime.Send(new IEventHandle(TEvents::TSystem::CheckActorLiveness,
                    TEvents::TEvCheckActorLiveness::RequestFlags, sender, control, nullptr, 0), 0, true);
                Receive<TEvents::TEvActorAlive>(f.Runtime, control);
            }
        }
    }

    Y_UNIT_TEST(ResolveFailureEchoesOutgoingRequestCookie) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto owner = runtime.AllocateEdgeActor();
        const auto caller = runtime.AllocateEdgeActor();
        auto request = THttpOutgoingRequest::CreateRequestGet("http://example.com/cookie");
        TEv::TEvHttpOutgoingRequest::TPtr envelope = static_cast<TEventHandle<TEv::TEvHttpOutgoingRequest>*>(
            new IEventHandle(owner, caller, new TEv::TEvHttpOutgoingRequest(request), 0, CookieB));
        const auto connection = runtime.Register(CreateOutgoingConnectionActor(owner, envelope));
        auto resolve = Receive<TEv::TEvResolveHostRequest>(runtime, owner);
        UNIT_ASSERT_VALUES_EQUAL(resolve->Sender, connection);
        runtime.Send(new IEventHandle(resolve->Sender, owner,
            new TEv::TEvResolveHostResponse("cookie-test resolver failure")), 0, true);
        TAutoPtr<IEventHandle> handle;
        const auto* response = ReceiveReply<TEv::TEvHttpIncomingResponse>(runtime, caller, handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Request.Get(), request.Get());
        UNIT_ASSERT(!response->Response);
        UNIT_ASSERT_VALUES_EQUAL(response->Error, "cookie-test resolver failure");
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, CookieB);
        auto closed = Receive<TEv::TEvHttpOutgoingConnectionClosed>(runtime, owner);
        UNIT_ASSERT_VALUES_EQUAL(closed->Get()->ConnectionID, connection);
    }

    Y_UNIT_TEST(MultiplexedResponsesEchoPerRequestCookies) {
        TSocketFixture f(EProtocol::Http2);
        auto warmup = f.Request("warmup");
        f.SendRequest(f.Proxy, warmup, CookieA);
        auto incomingWarmup = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        f.Reply(incomingWarmup, "warmup");
        const auto connection = f.ExpectResponse(warmup, CookieA, "warmup");
        auto requestB = f.Request("b");
        auto legacy = f.Request("legacy");
        f.SendRequest(connection, requestB, CookieB);
        f.SendRequest(connection, legacy, 0);
        auto incomingB = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        auto incomingLegacy = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        UNIT_ASSERT_VALUES_EQUAL(THeaders(incomingB->Get()->Request->Headers).Get("X-Test-Request"), "b");
        UNIT_ASSERT_VALUES_EQUAL(THeaders(incomingLegacy->Get()->Request->Headers).Get("X-Test-Request"), "legacy");
        UNIT_ASSERT_VALUES_EQUAL(incomingB->Sender, incomingWarmup->Sender);
        UNIT_ASSERT_VALUES_EQUAL(incomingLegacy->Sender, incomingWarmup->Sender);
        f.Reply(incomingLegacy, "h2-zero");
        UNIT_ASSERT_VALUES_EQUAL(f.ExpectResponse(legacy, 0, "h2-zero"), connection);
        f.Reply(incomingB, "h2-b");
        UNIT_ASSERT_VALUES_EQUAL(f.ExpectResponse(requestB, CookieB, "h2-b"), connection);
        f.Runtime.Send(new IEventHandle(connection, f.Caller, new TEvents::TEvPoison()), 0, true);
    }

    Y_UNIT_TEST(ClosingConnectionEchoesEachPendingRequestCookie) {
        TSocketFixture f(EProtocol::Http2);
        const std::array<ui64, 3> cookies{CookieA, CookieB, 0};
        std::array<THttpOutgoingRequestPtr, 3> requests{f.Request("a"), f.Request("b"), f.Request("legacy")};
        TEv::TEvHttpOutgoingRequest::TPtr envelope = static_cast<TEventHandle<TEv::TEvHttpOutgoingRequest>*>(
            new IEventHandle(f.Proxy, f.Caller, f.RequestEvent(requests[0]), 0, CookieA));
        const auto connection = f.Runtime.Register(CreateOutgoingConnectionActor(f.Proxy, envelope));
        auto first = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        f.SendRequest(connection, requests[1], CookieB);
        f.SendRequest(connection, requests[2], 0);
        for (size_t count = 0; count < 2; ++count) {
            auto incoming = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
            UNIT_ASSERT_VALUES_EQUAL(incoming->Sender, first->Sender);
        }
        f.Runtime.Send(new IEventHandle(connection, f.Caller, new TEvents::TEvPoison()), 0, true);
        std::array<bool, 3> seen{};
        for (size_t count = 0; count < requests.size(); ++count) {
            TAutoPtr<IEventHandle> handle;
            const auto* response = ReceiveReply<TEv::TEvHttpIncomingResponse>(f.Runtime, f.Caller, handle);
            UNIT_ASSERT(!response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->Error, "Connection closed");
            size_t index = 0;
            while (index < requests.size() && requests[index].Get() != response->Request.Get()) {
                ++index;
            }
            UNIT_ASSERT(index < requests.size());
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, cookies[index]);
        }
    }

    Y_UNIT_TEST(CoroutineActorRequestsResumeWithTheirOwnResponses) {
        TSocketFixture f(EProtocol::Http1);
        const auto client = f.Runtime.Register(new TCoroutineClient(f.Proxy, f.Runtime.AllocateEdgeActor()));
        auto requestA = f.Request("coroutine-a");
        auto requestB = f.Request("coroutine-b");
        for (const auto& request : {requestA, requestB}) {
            f.Runtime.Send(new IEventHandle(client, f.Caller, new TEvRunRequest(request, TDuration::Seconds(5))), 0, true);
        }
        auto incomingA = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        auto incomingB = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        if (THeaders(incomingA->Get()->Request->Headers).Get("X-Test-Request") != "coroutine-a") {
            incomingA.Swap(incomingB);
        }
        UNIT_ASSERT_VALUES_EQUAL(THeaders(incomingA->Get()->Request->Headers).Get("X-Test-Request"), "coroutine-a");
        UNIT_ASSERT_VALUES_EQUAL(THeaders(incomingB->Get()->Request->Headers).Get("X-Test-Request"), "coroutine-b");
        f.Reply(incomingB, "coroutine-b-body");
        auto replyB = ReceiveCoroutineReply(f.Runtime, f.Caller);
        AssertResponse(*replyB->Get(), requestB, "coroutine-b-body");
        f.Reply(incomingA, "coroutine-a-body");
        auto replyA = ReceiveCoroutineReply(f.Runtime, f.Caller);
        AssertResponse(*replyA->Get(), requestA, "coroutine-a-body");
        UNIT_ASSERT(replyA->Cookie != 0);
        UNIT_ASSERT(replyB->Cookie != 0);
        UNIT_ASSERT(replyA->Cookie != replyB->Cookie);
        f.Runtime.Send(new IEventHandle(client, f.Caller, new TEvents::TEvPoison()), 0, true);
    }

    Y_UNIT_TEST(CoroutineDeadlineRetryIgnoresLateFirstResponse) {
        TSocketFixture f(EProtocol::Http1);
        const auto lateReplies = f.Runtime.AllocateEdgeActor();
        const auto client = f.Runtime.Register(new TCoroutineClient(f.Proxy, lateReplies));
        auto firstRequest = f.Request("deadline-first");
        auto retryRequest = f.Request("deadline-retry");
        f.Runtime.Send(new IEventHandle(client, f.Caller,
            new TEvRunRequest(firstRequest, TDuration::Seconds(1)), 0, CookieA), 0, true);
        auto first = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        UNIT_ASSERT_VALUES_EQUAL(THeaders(first->Get()->Request->Headers).Get("X-Test-Request"), "deadline-first");
        UNIT_ASSERT_VALUES_EQUAL(Receive<TEvents::TEvWakeup>(f.Runtime, f.Caller)->Cookie, CookieA);

        f.Runtime.Send(new IEventHandle(client, f.Caller,
            new TEvRunRequest(retryRequest, TDuration::Seconds(5))), 0, true);
        // Actual server arrival proves ActorRequest sent the retry; the client
        // cannot process an old reply until that same handler turn suspends.
        auto retry = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        UNIT_ASSERT_VALUES_EQUAL(THeaders(retry->Get()->Request->Headers).Get("X-Test-Request"), "deadline-retry");
        f.Reply(first, "late-first-body");
        auto late = Receive<TEv::TEvHttpIncomingResponse>(f.Runtime, lateReplies);
        AssertResponse(*late->Get(), firstRequest, "late-first-body");
        f.Reply(retry, "retry-success-body");
        auto reply = ReceiveCoroutineReply(f.Runtime, f.Caller);
        AssertResponse(*reply->Get(), retryRequest, "retry-success-body");
        UNIT_ASSERT(late->Cookie != 0);
        UNIT_ASSERT(reply->Cookie != 0);
        UNIT_ASSERT(late->Cookie != reply->Cookie);
        f.Runtime.Send(new IEventHandle(client, f.Caller, new TEvents::TEvPoison()), 0, true);
    }

    Y_UNIT_TEST(CoalescedWaitersAndHitsEchoCallerCookies) {
        CheckCacheCookies(EBackendResult::Success);
    }

    Y_UNIT_TEST(ErrorWaitersAndHitsEchoCallerCookies) {
        CheckCacheCookies(EBackendResult::Error);
    }
}
