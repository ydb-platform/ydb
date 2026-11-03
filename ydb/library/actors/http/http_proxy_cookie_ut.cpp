#include "http.h"
#include "http_proxy.h"
#include "http_cache.h"

#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/task_group.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <vector>

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

void AssertNoReplies(TTestActorRuntimeBase& runtime, TActorId caller) {
    auto events = runtime.CaptureEvents();
    for (const auto& event : events) {
        if (event->GetRecipientRewrite() == caller) {
            UNIT_ASSERT_C(event->GetTypeRewrite() != TEv::TEvHttpIncomingResponse::EventType &&
                event->GetTypeRewrite() != TEv::TEvHttpIncompleteIncomingResponse::EventType &&
                event->GetTypeRewrite() != TEv::TEvHttpIncomingDataChunk::EventType,
                "Unexpected additional HTTP reply");
        }
    }
    runtime.PushEventsFront(events);
}

template <class TExpected>
void CheckLiveness(TTestActorRuntimeBase& runtime, TActorId actor) {
    const auto control = runtime.AllocateEdgeActor();
    runtime.Send(new IEventHandle(TEvents::TSystem::CheckActorLiveness,
        TEvents::TEvCheckActorLiveness::RequestFlags, actor, control, nullptr, 0), 0, true);
    Receive<TExpected>(runtime, control);
}

struct TSocketFixture {
    TTestActorRuntimeBase Runtime{1, true};
    TPortManager Ports;
    TIpPort Port = Ports.GetTcpPort();
    TActorId Proxy;
    TActorId Server;
    TActorId Caller;
    EProtocol Protocol;
    std::vector<THttpOutgoingRequestPtr> Requests;
    std::vector<THttpIncomingResponsePtr> Responses;

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
        Requests.push_back(request);
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
        UNIT_ASSERT_VALUES_EQUAL(response->Request.Get(), request.Get());
        UNIT_ASSERT_VALUES_EQUAL(response->Error, "");
        UNIT_ASSERT(response->Response);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Body, body);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, cookie);
        Responses.push_back(response->Response);
        return handle->Sender;
    }
};

struct TCoroutineObservation {
    THttpOutgoingRequestPtr Request;
    THttpIncomingResponsePtr Response;
    ui64 Cookie;
    TActorId Connection;
};

struct TEvCoroutineProgress : TEventLocal<TEvCoroutineProgress, TEvents::ES_PRIVATE + 1> {
    THttpOutgoingRequestPtr Request;

    explicit TEvCoroutineProgress(THttpOutgoingRequestPtr request)
        : Request(std::move(request))
    {}
};

struct TEvCoroutineComplete : TEventLocal<TEvCoroutineComplete, TEvents::ES_PRIVATE + 2> {
    std::vector<TCoroutineObservation> Observations;
    TString Error;
};

class TCoroutineClient : public TActorBootstrapped<TCoroutineClient> {
public:
    TCoroutineClient(TActorId proxy, TActorId owner, THttpOutgoingRequestPtr requestA, THttpOutgoingRequestPtr requestB)
        : Proxy(proxy)
        , Owner(owner)
        , RequestA(std::move(requestA))
        , RequestB(std::move(requestB))
    {}

    void Bootstrap() {
        Become(&TCoroutineClient::StateWork);
        auto completion = std::make_unique<TEvCoroutineComplete>();
        try {
            auto observations = co_await WithTimeout(TDuration::Seconds(5),
                RunConcurrent(std::move(RequestA), std::move(RequestB)));
            if (observations) {
                completion->Observations = std::move(*observations);
            } else {
                completion->Error = "ActorRequest timed out waiting for cookie-correlated HTTP responses";
            }
        } catch (...) {
            completion->Error = CurrentExceptionMessage();
        }
        // WithTaskGroup has consumed or cancelled both children before this point.
        Send(Owner, completion.release());
        Become(&TCoroutineClient::StateDying);
        PassAway();
        co_return;
    }

private:
    async<TCoroutineObservation> RoundTrip(THttpOutgoingRequestPtr request, TString expectedBody) {
        auto reply = co_await ActorRequest<TEv::TEvHttpIncomingResponse>(Proxy,
            new TEv::TEvHttpOutgoingRequest(request));
        if (reply->Get()->Request.Get() != request.Get() || !reply->Get()->Response ||
            reply->Get()->Error || reply->Get()->Response->Status != "200" ||
            reply->Get()->Response->Body != expectedBody || reply->Cookie == 0) {
            ythrow yexception() << "ActorRequest resumed with an incorrect HTTP response for " << expectedBody;
        }
        Send(Owner, new TEvCoroutineProgress(request));
        co_return TCoroutineObservation{request, reply->Get()->Response, reply->Cookie, reply->Sender};
    }

    async<std::vector<TCoroutineObservation>> RunConcurrent(THttpOutgoingRequestPtr requestA, THttpOutgoingRequestPtr requestB) {
        co_return co_await WithTaskGroup<TCoroutineObservation>(
            [this, requestA = std::move(requestA), requestB = std::move(requestB)]
            (TTaskGroup<TCoroutineObservation>& group) -> async<std::vector<TCoroutineObservation>> {
                group.Add([this, requestA]() -> async<TCoroutineObservation> {
                    co_return co_await RoundTrip(requestA, "coroutine-a-body");
                });
                group.Add([this, requestB]() -> async<TCoroutineObservation> {
                    co_return co_await RoundTrip(requestB, "coroutine-b-body");
                });
                std::vector<TCoroutineObservation> observations;
                while (group.Running() || group.Ready()) {
                    observations.push_back(co_await group.Next());
                }
                co_return observations;
            });
    }

    STFUNC(StateWork) {
        // A reply with a wrong/expired cookie must not be used to satisfy a waiter.
        Y_UNUSED(ev);
    }

    STFUNC(StateDying) {
        Y_UNUSED(ev);
    }

    const TActorId Proxy;
    const TActorId Owner;
    THttpOutgoingRequestPtr RequestA;
    THttpOutgoingRequestPtr RequestB;
};

enum class EDeadlineStage { FirstPending, RetryPending, Done };
enum class EDeadlineOutcome { Success, FirstCompletedEarly, RetryTimedOut, Error };

struct TEvFirstWaitTimedOut : TEventLocal<TEvFirstWaitTimedOut, TEvents::ES_PRIVATE + 3> {};
struct TEvRetryArmed : TEventLocal<TEvRetryArmed, TEvents::ES_PRIVATE + 4> {};

struct TEvLateFirstObserved : TEventLocal<TEvLateFirstObserved, TEvents::ES_PRIVATE + 5> {
    TEv::TEvHttpIncomingResponse::TPtr Reply;
    EDeadlineStage Stage;

    TEvLateFirstObserved(TEv::TEvHttpIncomingResponse::TPtr reply, EDeadlineStage stage)
        : Reply(std::move(reply))
        , Stage(stage)
    {}
};

struct TEvDeadlineComplete : TEventLocal<TEvDeadlineComplete, TEvents::ES_PRIVATE + 6> {
    EDeadlineOutcome Outcome = EDeadlineOutcome::Error;
    TEv::TEvHttpIncomingResponse::TPtr Reply;
    size_t LateCount = 0;
    size_t UnexpectedReplies = 0;
    TString Error;
};

class TDeadlineRetryClient : public TActorBootstrapped<TDeadlineRetryClient> {
public:
    TDeadlineRetryClient(TActorId proxy, TActorId owner, THttpOutgoingRequestPtr first, THttpOutgoingRequestPtr retry)
        : Proxy(proxy)
        , Owner(owner)
        , FirstRequest(std::move(first))
        , RetryRequest(std::move(retry))
    {}

    void Bootstrap() {
        Become(&TDeadlineRetryClient::StateWork);
        auto completion = std::make_unique<TEvDeadlineComplete>();
        try {
            auto first = co_await WithTimeout(TDuration::Seconds(1), RequestOnce(FirstRequest));
            if (first) {
                completion->Outcome = EDeadlineOutcome::FirstCompletedEarly;
            } else {
                Send(Owner, new TEvFirstWaitTimedOut());
                Stage = EDeadlineStage::RetryPending;
                auto retry = co_await WithTimeout(TDuration::Seconds(5), RequestRetry(std::move(RetryRequest)));
                if (retry) {
                    completion->Outcome = EDeadlineOutcome::Success;
                    completion->Reply = std::move(*retry);
                } else {
                    completion->Outcome = EDeadlineOutcome::RetryTimedOut;
                }
            }
        } catch (...) {
            completion->Error = CurrentExceptionMessage();
        }
        completion->LateCount = LateCount;
        completion->UnexpectedReplies = UnexpectedReplies;
        Stage = EDeadlineStage::Done;
        Send(Owner, completion.release());
        Become(&TDeadlineRetryClient::StateDying);
        PassAway();
        co_return;
    }

private:
    async<TEv::TEvHttpIncomingResponse::TPtr> RequestOnce(THttpOutgoingRequestPtr request) {
        co_return co_await ActorRequest<TEv::TEvHttpIncomingResponse>(Proxy,
            new TEv::TEvHttpOutgoingRequest(request, TDuration::Seconds(30)));
    }

    async<TEv::TEvHttpIncomingResponse::TPtr> RequestRetry(THttpOutgoingRequestPtr request) {
        // The test also waits for the actual retry server request. This actor
        // cannot process an old reply until this same turn suspends below.
        Send(Owner, new TEvRetryArmed());
        co_return co_await ActorRequest<TEv::TEvHttpIncomingResponse>(Proxy,
            new TEv::TEvHttpOutgoingRequest(request, TDuration::Seconds(30)));
    }

    void HandleLateReply(TEv::TEvHttpIncomingResponse::TPtr& reply) {
        if (Stage == EDeadlineStage::RetryPending && reply->Get()->Request.Get() == FirstRequest.Get()) {
            ++LateCount;
            Send(Owner, new TEvLateFirstObserved(std::move(reply), Stage));
        } else {
            ++UnexpectedReplies;
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEv::TEvHttpIncomingResponse, HandleLateReply);
        }
    }

    STFUNC(StateDying) {
        Y_UNUSED(ev);
    }

    const TActorId Proxy;
    const TActorId Owner;
    // Shared with the late-reply handler, which owns classification/accounting.
    const THttpOutgoingRequestPtr FirstRequest;
    THttpOutgoingRequestPtr RetryRequest;
    EDeadlineStage Stage = EDeadlineStage::FirstPending;
    size_t LateCount = 0;
    size_t UnexpectedReplies = 0;
};

NJson::TJsonValue DumpCache(TTestActorRuntimeBase& runtime, TActorId cache) {
    const auto control = runtime.AllocateEdgeActor();
    runtime.Send(new IEventHandle(cache, control, new TEv::TEvHttpDumpStateRequest()), 0, true);
    auto event = Receive<TEv::TEvHttpDumpStateResponse>(runtime, control);
    NJson::TJsonValue result;
    UNIT_ASSERT(NJson::ReadJsonTree(event->Get()->Body, &result, true));
    return result;
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
    for (const auto& request : requests) {
        UNIT_ASSERT(fetchRequest.Get() != request.Get());
    }
    auto dump = DumpCache(runtime, cache);
    UNIT_ASSERT_VALUES_EQUAL(dump["OutgoingRequestsCount"].GetInteger(), 1);
    UNIT_ASSERT_VALUES_EQUAL(dump["CacheRecordsCount"].GetInteger(), 1);
    UNIT_ASSERT_VALUES_EQUAL(dump["CacheRecords"][0]["WaitersCount"].GetInteger(), 3);
    UNIT_ASSERT(dump["CacheRecords"][0]["RefreshInFlight"].GetBoolean());
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
    std::vector<THttpIncomingResponsePtr> callerResponses;
    auto assertReply = [&](THttpOutgoingRequestPtr request, ui64 cookie, const TEv::TEvHttpIncomingResponse* response, const IEventHandle& handle) {
        UNIT_ASSERT_VALUES_EQUAL(response->Request.Get(), request.Get());
        UNIT_ASSERT_VALUES_EQUAL(response->Error, error);
        if (result == EBackendResult::Success) {
            UNIT_ASSERT(response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->Response->GetRequest().Get(), request.Get());
            UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");
            UNIT_ASSERT_VALUES_EQUAL(response->Response->Body, "cache-cookie");
            UNIT_ASSERT(response->Response.Get() != backendResponse.Get());
            for (const auto& previous : callerResponses) {
                UNIT_ASSERT(response->Response.Get() != previous.Get());
            }
            callerResponses.push_back(response->Response);
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
    dump = DumpCache(runtime, cache);
    UNIT_ASSERT_VALUES_EQUAL(dump["OutgoingRequestsCount"].GetInteger(), 0);
    UNIT_ASSERT_VALUES_EQUAL(dump["CacheRecordsCount"].GetInteger(), 1);
    UNIT_ASSERT_VALUES_EQUAL(dump["CacheHits"].GetInteger(), 2);
    UNIT_ASSERT_VALUES_EQUAL(dump["CacheMisses"].GetInteger(), 3);
    UNIT_ASSERT_VALUES_EQUAL(dump["CacheRecords"][0]["WaitersCount"].GetInteger(), 0);
    UNIT_ASSERT(!dump["CacheRecords"][0]["RefreshInFlight"].GetBoolean());
    AssertNoReplies(runtime, caller);
    auto events = runtime.CaptureEvents();
    for (const auto& event : events) {
        UNIT_ASSERT_C(event->GetRecipientRewrite() != backend ||
            event->GetTypeRewrite() != TEv::TEvHttpOutgoingRequest::EventType, "Unexpected extra cache fetch");
    }
    runtime.PushEventsFront(events);
}

}

Y_UNIT_TEST_SUITE(HttpProxyCookies) {
    Y_UNIT_TEST(ConcurrentOutgoingRequestsEchoDistinctCookies) {
        TSocketFixture f(EProtocol::Http1);
        const std::array<ui64, 3> cookies{CookieA, CookieB, 0};
        std::array<THttpOutgoingRequestPtr, 3> requests{f.Request("a"), f.Request("b"), f.Request("legacy")};
        std::array<TEv::TEvHttpIncomingRequest::TPtr, 3> incoming;
        for (size_t i = 0; i < requests.size(); ++i) {
            f.SendRequest(f.Proxy, requests[i], cookies[i]);
        }
        for (size_t count = 0; count < incoming.size(); ++count) {
            auto event = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
            THeaders headers(event->Get()->Request->Headers);
            const auto tag = headers.Get("X-Test-Request");
            size_t index = tag == "a" ? 0 : tag == "b" ? 1 : 2;
            UNIT_ASSERT_VALUES_EQUAL(tag, index == 0 ? "a" : index == 1 ? "b" : "legacy");
            UNIT_ASSERT(!incoming[index]);
            incoming[index] = event;
        }
        for (size_t index : {size_t(1), size_t(0), size_t(2)}) {
            auto response = incoming[index]->Get()->Request->CreateResponseString(
                "HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Length: 6\r\n\r\npassed");
            f.Runtime.Send(new IEventHandle(incoming[index]->Sender, f.Server, new TEv::TEvHttpOutgoingResponse(response)), 0, true);
            const auto connection = f.ExpectResponse(requests[index], cookies[index], "passed");
            CheckLiveness<TEvents::TEvActorDead>(f.Runtime, connection);
        }
        AssertNoReplies(f.Runtime, f.Caller);
    }

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
            if (i == 2) {
                auto response = incoming->Get()->Request->CreateResponseString(
                    "HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Length: 5\r\n\r\nthird");
                f.Runtime.Send(new IEventHandle(incoming->Sender, f.Server, new TEv::TEvHttpOutgoingResponse(response)), 0, true);
            } else {
                f.Reply(incoming, bodies[i]);
            }
            const auto sender = f.ExpectResponse(request, cookies[i], bodies[i]);
            if (i == 0) {
                outgoingConnection = sender;
            }
            UNIT_ASSERT_VALUES_EQUAL(sender, outgoingConnection);
            if (i != 2) {
                // The actor's preceding turn enqueues connection availability before this barrier.
                CheckLiveness<TEvents::TEvActorAlive>(f.Runtime, sender);
            } else {
                CheckLiveness<TEvents::TEvActorDead>(f.Runtime, sender);
            }
        }
        AssertNoReplies(f.Runtime, f.Caller);
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
        AssertNoReplies(runtime, caller);
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
        TEv::TEvHttpIncomingRequest::TPtr incomingB, incomingLegacy;
        for (size_t count = 0; count < 2; ++count) {
            auto incoming = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
            UNIT_ASSERT_VALUES_EQUAL(incoming->Sender, incomingWarmup->Sender);
            THeaders headers(incoming->Get()->Request->Headers);
            const auto tag = headers.Get("X-Test-Request");
            if (tag == "b") {
                UNIT_ASSERT(!incomingB);
                incomingB = incoming;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(tag, "legacy");
                UNIT_ASSERT(!incomingLegacy);
                incomingLegacy = incoming;
            }
        }
        f.Reply(incomingLegacy, "h2-zero");
        UNIT_ASSERT_VALUES_EQUAL(f.ExpectResponse(legacy, 0, "h2-zero"), connection);
        f.Reply(incomingB, "h2-b");
        UNIT_ASSERT_VALUES_EQUAL(f.ExpectResponse(requestB, CookieB, "h2-b"), connection);
        f.Runtime.Send(new IEventHandle(connection, f.Caller, new TEvents::TEvPoison()), 0, true);
        CheckLiveness<TEvents::TEvActorDead>(f.Runtime, connection);
        AssertNoReplies(f.Runtime, f.Caller);
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
        CheckLiveness<TEvents::TEvActorDead>(f.Runtime, connection);
        AssertNoReplies(f.Runtime, f.Caller);
    }

    Y_UNIT_TEST(CoroutineActorRequestsResumeWithTheirOwnResponses) {
        TSocketFixture f(EProtocol::Http1);
        auto requestA = f.Request("coroutine-a");
        auto requestB = f.Request("coroutine-b");
        const auto client = f.Runtime.Register(new TCoroutineClient(f.Proxy, f.Caller, requestA, requestB));
        TEv::TEvHttpIncomingRequest::TPtr incomingA, incomingB;
        for (size_t count = 0; count < 2; ++count) {
            auto incoming = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
            THeaders headers(incoming->Get()->Request->Headers);
            const auto tag = headers.Get("X-Test-Request");
            if (tag == "coroutine-a") {
                UNIT_ASSERT(!incomingA);
                incomingA = incoming;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(tag, "coroutine-b");
                UNIT_ASSERT(!incomingB);
                incomingB = incoming;
            }
        }
        f.Reply(incomingB, "coroutine-b-body");
        // Consume either progress or the explicit timeout completion, so a missing
        // cookie fails diagnostically rather than hanging on a progress-only wait.
        TAutoPtr<IEventHandle> handle;
        f.Runtime.GrabEdgeEvents<TEvCoroutineProgress, TEvCoroutineComplete>(handle, WaitBudget);
        UNIT_ASSERT(handle);
        UNIT_ASSERT_VALUES_EQUAL(handle->GetRecipientRewrite(), f.Caller);
        if (handle->GetTypeRewrite() == TEvCoroutineComplete::EventType) {
            UNIT_FAIL(handle->Get<TEvCoroutineComplete>()->Error);
        }
        UNIT_ASSERT_VALUES_EQUAL(handle->Get<TEvCoroutineProgress>()->Request.Get(), requestB.Get());
        f.Reply(incomingA, "coroutine-a-body");
        auto completion = Receive<TEvCoroutineComplete>(f.Runtime, f.Caller);
        UNIT_ASSERT_VALUES_EQUAL(completion->Get()->Error, "");
        const auto& observations = completion->Get()->Observations;
        UNIT_ASSERT_VALUES_EQUAL(observations.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(observations[0].Request.Get(), requestB.Get());
        UNIT_ASSERT_VALUES_EQUAL(observations[0].Response->Body, "coroutine-b-body");
        UNIT_ASSERT_VALUES_EQUAL(observations[1].Request.Get(), requestA.Get());
        UNIT_ASSERT_VALUES_EQUAL(observations[1].Response->Body, "coroutine-a-body");
        UNIT_ASSERT(observations[0].Cookie != 0);
        UNIT_ASSERT(observations[1].Cookie != 0);
        UNIT_ASSERT(observations[0].Cookie != observations[1].Cookie);
        CheckLiveness<TEvents::TEvActorDead>(f.Runtime, client);
        for (const auto& observation : observations) {
            CheckLiveness<TEvents::TEvActorDead>(f.Runtime, observation.Connection);
        }
    }

    Y_UNIT_TEST(CoroutineDeadlineRetryIgnoresLateFirstResponse) {
        TSocketFixture f(EProtocol::Http1);
        auto firstRequest = f.Request("deadline-first");
        auto retryRequest = f.Request("deadline-retry");
        const auto client = f.Runtime.Register(new TDeadlineRetryClient(f.Proxy, f.Caller, firstRequest, retryRequest));
        auto first = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        UNIT_ASSERT_VALUES_EQUAL(THeaders(first->Get()->Request->Headers).Get("X-Test-Request"), "deadline-first");
        Receive<TEvFirstWaitTimedOut>(f.Runtime, f.Caller);
        Receive<TEvRetryArmed>(f.Runtime, f.Caller);
        auto retry = Receive<TEv::TEvHttpIncomingRequest>(f.Runtime, f.Server);
        UNIT_ASSERT_VALUES_EQUAL(THeaders(retry->Get()->Request->Headers).Get("X-Test-Request"), "deadline-retry");

        f.Reply(first, "late-first-body");
        auto late = Receive<TEvLateFirstObserved>(f.Runtime, f.Caller);
        const auto& lateReply = late->Get()->Reply;
        UNIT_ASSERT(late->Get()->Stage == EDeadlineStage::RetryPending);
        UNIT_ASSERT_VALUES_EQUAL(lateReply->Get()->Request.Get(), firstRequest.Get());
        UNIT_ASSERT_VALUES_EQUAL(lateReply->Get()->Error, "");
        UNIT_ASSERT(lateReply->Get()->Response);
        UNIT_ASSERT_VALUES_EQUAL(lateReply->Get()->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(lateReply->Get()->Response->Body, "late-first-body");
        auto events = f.Runtime.CaptureEvents();
        for (const auto& event : events) {
            UNIT_ASSERT_C(event->GetRecipientRewrite() != f.Caller ||
                event->GetTypeRewrite() != TEvDeadlineComplete::EventType,
                "Retry completed before its response was sent");
        }
        f.Runtime.PushEventsFront(events);

        f.Reply(retry, "retry-success-body");
        auto completion = Receive<TEvDeadlineComplete>(f.Runtime, f.Caller);
        UNIT_ASSERT_VALUES_EQUAL(completion->Get()->Error, "");
        UNIT_ASSERT_C(completion->Get()->Outcome == EDeadlineOutcome::Success,
            "Expected Success, got outcome " << static_cast<int>(completion->Get()->Outcome)
            << "; unexpected replies=" << completion->Get()->UnexpectedReplies);
        UNIT_ASSERT_VALUES_EQUAL(completion->Get()->LateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(completion->Get()->UnexpectedReplies, 0);
        const auto& reply = completion->Get()->Reply;
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Request.Get(), retryRequest.Get());
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Error, "");
        UNIT_ASSERT(reply->Get()->Response);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Response->Body, "retry-success-body");
        UNIT_ASSERT(lateReply->Cookie != 0);
        UNIT_ASSERT(reply->Cookie != 0);
        UNIT_ASSERT(lateReply->Cookie != reply->Cookie);
        CheckLiveness<TEvents::TEvActorDead>(f.Runtime, client);
        CheckLiveness<TEvents::TEvActorDead>(f.Runtime, lateReply->Sender);
        CheckLiveness<TEvents::TEvActorDead>(f.Runtime, reply->Sender);
    }

    Y_UNIT_TEST(CoalescedWaitersAndHitsEchoCallerCookies) {
        CheckCacheCookies(EBackendResult::Success);
    }

    Y_UNIT_TEST(ErrorWaitersAndHitsEchoCallerCookies) {
        CheckCacheCookies(EBackendResult::Error);
    }
}
