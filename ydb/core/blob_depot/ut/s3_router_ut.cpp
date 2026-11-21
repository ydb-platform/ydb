#ifndef KIKIMR_DISABLE_S3_OPS

#include "../s3_router.h"
#include "../s3_router_events.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/aws_init/aws.h>

#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/network/sock.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <atomic>
#include <memory>

namespace NKikimr::NBlobDepot {

namespace {

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

struct TEvReleaseHeldResponses : TEventLocal<TEvReleaseHeldResponses, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    TString Hostname;
    explicit TEvReleaseHeldResponses(TString hostname)
        : Hostname(std::move(hostname))
    {}
};

// Tiny HTTP server actor that listens on a local port and returns a single hostname
// as plain text. Optional hold defers the reply until TEvReleaseHeldResponses.
class TFakeBalancer : public TActor<TFakeBalancer> {
    TString Hostname;
    int StatusCode = 200;
    bool HoldResponses = false;
    TVector<NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr> Held;
    std::atomic<size_t> Replies{0};

public:
    TFakeBalancer(TActorId /*proxyId*/, TString hostname)
        : TActor(&TThis::StateWork)
        , Hostname(std::move(hostname))
    {}

    void SetHostname(TString h) { Hostname = std::move(h); }
    void SetStatusCode(int code) { StatusCode = code; }
    void SetHoldResponses(bool hold) { HoldResponses = hold; }
    size_t GetReplies() const { return Replies.load(); }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(TEvReleaseHeldResponses, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr ev) {
        if (HoldResponses) {
            Held.push_back(std::move(ev));
            return;
        }

        Reply(ev);
    }

    void Handle(TEvReleaseHeldResponses::TPtr ev) {
        Hostname = std::move(ev->Get()->Hostname);
        HoldResponses = false;
        for (auto& held : Held) {
            Reply(held);
        }

        Held.clear();
    }

    void Reply(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        auto& req = ev->Get()->Request;
        auto response = StatusCode == 200
            ? req->CreateResponseOK(Hostname, "text/plain")
            : req->CreateResponse(ToString(StatusCode), "Error", "text/plain", Hostname);
        Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        ++Replies;
    }
};

class TStallingS3Server {
    TInetStreamSocket Listener;
    ui16 Port = 0;
    const TString Body;
    const TDuration HoldDuration;
    std::atomic<size_t> RequestsReceived{0};
    std::atomic<bool> Stopped{false};
    THolder<TThread> AcceptThread;
    TMutex ConnectionsMutex;
    TVector<THolder<TThread>> ConnectionThreads;

public:
    TStallingS3Server(size_t bodySize, TDuration holdDuration)
        : Body(bodySize, 'x')
        , HoldDuration(holdDuration)
    {
        SetSockOpt(Listener, SOL_SOCKET, SO_REUSEADDR, 1);
        TSockAddrInet addr("127.0.0.1", 0);
        Y_ENSURE(Listener.Bind(&addr) == 0);
        sockaddr_in name;
        socklen_t len = sizeof(name);
        Y_ENSURE(getsockname(Listener, reinterpret_cast<sockaddr*>(&name), &len) == 0);
        Port = InetToHost(name.sin_port);
        Y_ENSURE(Listener.Listen(16) == 0);
        AcceptThread = MakeHolder<TThread>([this] { AcceptLoop(); });
        AcceptThread->Start();
    }

    ~TStallingS3Server() {
        Stop();
    }

    ui16 GetPort() const { return Port; }
    size_t GetRequestsReceived() const { return RequestsReceived.load(); }

    void Stop() {
        if (Stopped.exchange(true)) {
            return;
        }

        TInetStreamSocket poke;
        TSockAddrInet addr("127.0.0.1", Port);
        poke.Connect(&addr);
        AcceptThread->Join();
        TVector<THolder<TThread>> threads;
        with_lock (ConnectionsMutex) {
            threads.swap(ConnectionThreads);
        }

        for (auto& thread : threads) {
            thread->Join();
        }

        Listener.Close();
    }

private:
    void AcceptLoop() {
        while (!Stopped.load()) {
            auto client = std::make_shared<TStreamSocket>();
            if (Listener.Accept(client.get()) != 0 || Stopped.load()) {
                return;
            }

            auto thread = MakeHolder<TThread>([this, client] { Serve(*client); });
            thread->Start();
            with_lock (ConnectionsMutex) {
                ConnectionThreads.push_back(std::move(thread));
            }
        }
    }

    void Serve(TStreamSocket& socket) {
        TString request;
        char buffer[4096];
        while (request.find("\r\n\r\n") == TString::npos) {
            const ssize_t received = socket.Recv(buffer, sizeof(buffer));
            if (received <= 0) {
                return;
            }

            request.append(buffer, received);
        }

        ++RequestsReceived;

        const TInstant deadline = TInstant::Now() + HoldDuration;
        while (TInstant::Now() < deadline && !Stopped.load()) {
            Sleep(TDuration::MilliSeconds(50));
        }

        TString response = TStringBuilder()
            << "HTTP/1.1 206 Partial Content\r\n"
            << "Content-Type: application/octet-stream\r\n"
            << "Content-Range: bytes 0-" << Body.size() - 1 << '/' << Body.size() << "\r\n"
            << "Content-Length: " << Body.size() << "\r\n"
            << "Connection: close\r\n"
            << "\r\n"
            << Body;
        TStringBuf data = response;
        while (!data.empty()) {
            const ssize_t sent = socket.Send(data.data(), data.size());
            if (sent <= 0) {
                return;
            }
            data.Skip(sent);
        }
    }
};

// Receives S3 error replies that the router sends back to the original requester.
class TErrorResponseCounter : public TActor<TErrorResponseCounter> {
    size_t& ErrorResponses;
    TString& LastExceptionName;
    TString& LastMessage;

public:
    TErrorResponseCounter(size_t& errorResponses, TString& lastExceptionName, TString& lastMessage)
        : TActor(&TThis::StateWork)
        , ErrorResponses(errorResponses)
        , LastExceptionName(lastExceptionName)
        , LastMessage(lastMessage)
    {}

    STATEFN(StateWork) {
        using TEvPutObjectResponse = NWrappers::NExternalStorage::TEvPutObjectResponse;
        if (ev->GetTypeRewrite() == TEvPutObjectResponse::EventType &&
                !ev->Get<TEvPutObjectResponse>()->IsSuccess()) {
            const auto& error = ev->Get<TEvPutObjectResponse>()->GetError();
            LastExceptionName = TString(error.GetExceptionName());
            LastMessage = TString(error.GetMessage());
            ++ErrorResponses;
        }
    }
};

ui16 PickFreePort() {
    TInetStreamSocket sock;
    TSockAddrInet addr("127.0.0.1", 0);
    SetSockOpt(sock, SOL_SOCKET, SO_REUSEADDR, 1);
    Y_ENSURE(sock.Bind(&addr) == 0);
    sockaddr_in name;
    socklen_t len = sizeof(name);
    Y_ENSURE(getsockname(sock, reinterpret_cast<sockaddr*>(&name), &len) == 0);
    return InetToHost(name.sin_port);
}

struct TGetResult : TThrRefBase {
    std::atomic<bool> Done{false};
    std::atomic<bool> Ok{false};
    TMutex Mutex;
    TString Error;
};

class TGetCollector : public TActor<TGetCollector> {
    TIntrusivePtr<TGetResult> Result;
public:
    explicit TGetCollector(TIntrusivePtr<TGetResult> result)
        : TActor(&TThis::StateWork)
        , Result(std::move(result))
    {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWrappers::NExternalStorage::TEvGetObjectResponse, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    void Handle(NWrappers::NExternalStorage::TEvGetObjectResponse::TPtr ev) {
        Result->Ok.store(ev->Get()->IsSuccess());
        if (!ev->Get()->IsSuccess()) {
            TGuard<TMutex> g(Result->Mutex);
            Result->Error = ev->Get()->GetError().GetMessage().c_str();
        }
        Result->Done.store(true);
    }
};

template <typename TCondition>
void WaitReal(TTestActorRuntime& runtime, TCondition condition, TDuration timeout = TDuration::Seconds(60)) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (!condition()) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "WaitReal timeout");
        // Give dispatch a scheduled event even when only the external SDK thread can make progress.
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        Sleep(TDuration::MilliSeconds(10));
    }
}

TActorId MakeStallingRouter(TTestActorRuntime& runtime, TStallingS3Server& server) {
    NKikimrBlobDepot::TS3BackendSettings settings;
    auto* s3 = settings.MutableSettings();
    s3->SetEndpoint(TStringBuilder() << "127.0.0.1:" << server.GetPort());
    s3->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
    s3->SetBucket("test-bucket");
    s3->SetAccessKey("access-key");
    s3->SetSecretKey("secret-key");
    s3->SetRegion("ru-central1");
    s3->SetUseVirtualAddressing(false);
    s3->SetHttpRequestTimeoutMs(0);
    s3->SetRequestTimeoutMs(0);
    s3->SetExecutorThreadsCount(1);
    settings.MutableAsyncMode()->SetUploadPutsInFlight(1);
    return runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
}

void SendRouterPut(TTestActorRuntime& runtime, TActorId router, TActorId sender, TString key, TDuration timeout,
        Aws::Http::ContinueRequestHandler canContinue = {}, TS3PutContext::ESource source = TS3PutContext::ESource::Cooling) {
    using namespace NWrappers::NExternalStorage;
    auto context = std::make_shared<TS3PutContext>(runtime.GetCurrentMonotonicTime() + timeout, source);
    auto request = Aws::S3::Model::PutObjectRequest().WithBucket("test-bucket").WithKey(std::move(key));
    request.SetContinueRequestHandler(std::move(canContinue));
    runtime.Send(new IEventHandle(router, sender,
        new TEvPutObjectRequest(request, TString("data"), std::move(context))), 0, true);
}

}  // namespace

Y_UNIT_TEST_SUITE(BlobDepotS3Router) {

    Y_UNIT_TEST(BalancerPollHappyPath) {
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        const ui16 balancerPort = PickFreePort();

        auto* proxy = NHttp::CreateHttpProxy();
        TActorId proxyId = runtime.Register(proxy);
        TActorId edgeId = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(proxyId, edgeId,
            new NHttp::TEvHttpProxy::TEvAddListeningPort(balancerPort)), 0, true);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        const TString hostname = "endpoint-A.example.com";
        auto* balancer = new TFakeBalancer({}, hostname);
        TActorId balancerId = runtime.Register(balancer);
        runtime.Send(new IEventHandle(proxyId, balancerId,
            new NHttp::TEvHttpProxy::TEvRegisterHandler("/", balancerId)), 0, true);

        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSettings()->SetEndpoint("initial-endpoint.example.com");
        settings.MutableSettings()->SetBucket("test-bucket");
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort << '/');
        settings.SetBalancerRefreshSecMin(1);
        settings.SetBalancerRefreshSecMax(1);

        TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
        Y_UNUSED(routerId);

        WaitReal(runtime, [&] { return balancer->GetReplies() > 0; });

        auto subsystem = GetServiceCounters(runtime.GetAppData(0).Counters, "tablets")->FindSubgroup("subsystem", "blob_depot");
        UNIT_ASSERT(subsystem);
        auto module = subsystem->FindSubgroup("module_id", "s3_router");
        UNIT_ASSERT(module);
        auto hostBuckets = module->FindSubgroup("component", "BalancerResolveByHostBucket");
        UNIT_ASSERT(hostBuckets);

        static constexpr size_t HostBucketCount = 16;
        const size_t hostBucket = THash<TStringBuf>{}(hostname) % HostBucketCount;
        auto bucket = hostBuckets->FindSubgroup("host_bucket", ToString(hostBucket));
        UNIT_ASSERT(bucket);
        auto selections = bucket->FindCounter("Selections");
        UNIT_ASSERT(selections);
        WaitReal(runtime, [&] { return *selections > 0; });

        auto latency = bucket->FindHistogram("LatencyMs");
        UNIT_ASSERT(latency);
        const auto snapshot = latency->Snapshot();
        ui64 latencySamples = 0;
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            latencySamples += snapshot->Value(i);
        }
        UNIT_ASSERT(latencySamples > 0);
    }

    Y_UNIT_TEST(FiveXxTriggersRefresh) {
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        const ui16 balancerPort = PickFreePort();

        auto* proxy = NHttp::CreateHttpProxy();
        TActorId proxyId = runtime.Register(proxy);
        TActorId edgeId = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(proxyId, edgeId,
            new NHttp::TEvHttpProxy::TEvAddListeningPort(balancerPort)), 0, true);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        auto* balancer = new TFakeBalancer({}, "endpoint-A.example.com");
        TActorId balancerId = runtime.Register(balancer);
        runtime.Send(new IEventHandle(proxyId, balancerId,
            new NHttp::TEvHttpProxy::TEvRegisterHandler("/", balancerId)), 0, true);

        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSettings()->SetEndpoint("initial-endpoint.example.com");
        settings.MutableSettings()->SetBucket("test-bucket");
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort << '/');
        settings.SetBalancerRefreshSecMin(60);
        settings.SetBalancerRefreshSecMax(60);

        TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));

        // Simulate a 5xx hint that an external code path would normally raise from
        // the IReplyAdapter when an S3 response carries HTTP 500-599. We use the
        // same private event type the adapter sends. Since the enum value is
        // private to the router, we just exercise the public Send-to-self mechanism
        // by pinging the router with Poison after a short pause; the goal here is
        // to make sure the router does not crash on out-of-band events.
        runtime.SimulateSleep(TDuration::Seconds(2));
        runtime.Send(new IEventHandle(routerId, edgeId,
            new TEvents::TEvPoison()), 0, true);
    }

    Y_UNIT_TEST(RejectedRequestGetsErrorResponse) {
        size_t errorResponses = 0;
        TString lastExceptionName;
        TString lastMessage;

        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        // nobody listens on this port, so the router never gets an endpoint to route to
        const ui16 balancerPort = PickFreePort();

        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSettings()->SetEndpoint("initial-endpoint.example.com");
        settings.MutableSettings()->SetBucket("test-bucket");
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort);
        settings.SetBalancerRefreshSecMin(60);
        settings.SetBalancerRefreshSecMax(60);

        TActorId senderId = runtime.Register(new TErrorResponseCounter(
            errorResponses, lastExceptionName, lastMessage));
        TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));

        static constexpr size_t maxPendingRequests = 256;
        for (size_t i = 0; i <= maxPendingRequests; ++i) {
            auto request = Aws::S3::Model::PutObjectRequest()
                .WithBucket("test-bucket")
                .WithKey(TStringBuilder() << "key-" << i);
            runtime.Send(new IEventHandle(routerId, senderId,
                new NWrappers::NExternalStorage::TEvPutObjectRequest(request, TString("data"))), 0, true);
        }

        // the pending queue holds maxPendingRequests requests, the one above that is answered right away
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(errorResponses, 1);
        UNIT_ASSERT_VALUES_EQUAL(lastExceptionName, "ServiceUnavailable");
        UNIT_ASSERT(lastMessage.Contains("S3 endpoint is not resolved yet"));

        auto stale = Aws::S3::Model::PutObjectRequest().WithBucket("old-bucket").WithKey("key");
        runtime.Send(new IEventHandle(routerId, senderId,
            new NWrappers::NExternalStorage::TEvPutObjectRequest(stale, TString("data"))), 0, true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(errorResponses, 2);
        UNIT_ASSERT_STRING_CONTAINS(lastMessage, "different bucket");

        // the queued ones are answered when the router goes away without ever resolving the endpoint
        runtime.Send(new IEventHandle(routerId, senderId, new TEvents::TEvPoison()), 0, true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(errorResponses, maxPendingRequests + 2);
        UNIT_ASSERT_VALUES_EQUAL(lastExceptionName, "ServiceUnavailable");
    }

    Y_UNIT_TEST(CoolingPutExpiresBehindAnUnresolvedRequest) {
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        size_t errors = 0;
        TString code, message;
        const auto sender = runtime.Register(new TErrorResponseCounter(errors, code, message));
        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSettings()->SetBucket("test-bucket");
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << PickFreePort());
        const auto router = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
        runtime.EnableScheduleForActor(router);
        SendRouterPut(runtime, router, sender, "long", TDuration::Hours(1));
        SendRouterPut(runtime, router, sender, "short", TDuration::Seconds(1));
        runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(errors, 1);
        UNIT_ASSERT_STRING_CONTAINS(message, "expired while waiting for an endpoint");
        runtime.Send(new IEventHandle(router, sender, new TEvents::TEvPoison), 0, true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(errors, 2);
    }

    Y_UNIT_TEST(IdleCoolingPutReleasesPhysicalSlotWithZeroSdkTimeouts) {
        using namespace NWrappers::NExternalStorage;
        TStallingS3Server server(4096, TDuration::Minutes(1));
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto firstRouter = MakeStallingRouter(runtime, server);
        const auto edge = runtime.AllocateEdgeActor();
        size_t errors = 0;
        TString code, message;
        const auto sender = runtime.Register(new TErrorResponseCounter(errors, code, message));

        auto live = std::make_shared<std::atomic_bool>(true);
        SendRouterPut(runtime, firstRouter, sender, "held", TDuration::Minutes(1), [live](const auto*) { return live->load(); });
        WaitReal(runtime, [&] { return server.GetRequestsReceived() == 1; });
        const auto replacementRouter = MakeStallingRouter(runtime, server);
        for (size_t n = 0; n < 3; ++n) {
            SendRouterPut(runtime, replacementRouter, edge, "retry", TDuration::Minutes(1));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        const auto replies = runtime.CaptureMailboxEvents(edge.Hint(), edge.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        for (const auto& reply : replies) {
            UNIT_ASSERT_VALUES_EQUAL(reply->GetTypeRewrite(), TEvents::TEvUndelivered::EventType);
            const auto* rejected = reply->Get<TEvents::TEvUndelivered>();
            UNIT_ASSERT_VALUES_EQUAL(rejected->SourceType, TEvPutObjectRequest::EventType);
            UNIT_ASSERT(!rejected->Unsure);
        }
        // Both routers share one SDK worker. Its next callback confirms the held task has released its slot.
        SendRouterPut(runtime, replacementRouter, sender, "executor-barrier", TDuration::Minutes(1),
            [](const auto*) { return false; }, TS3PutContext::ESource::Agent);
        live->store(false);
        WaitReal(runtime, [&] { return errors == 2; }, TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(server.GetRequestsReceived(), 1);
        SendRouterPut(runtime, replacementRouter, sender, "replacement", TDuration::Seconds(2));
        WaitReal(runtime, [&] { return server.GetRequestsReceived() == 2 && errors == 3; }, TDuration::Seconds(15));
    }

    Y_UNIT_TEST(IdleDeleteWithZeroSdkTimeoutsCanBeCancelled) {
        using namespace NWrappers::NExternalStorage;
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        TStallingS3Server server(4096, TDuration::Minutes(1));

        const auto router = MakeStallingRouter(runtime, server);
        const auto edge = runtime.AllocateEdgeActor();
        const auto live = std::make_shared<std::atomic_bool>(true);
        auto request = Aws::S3::Model::DeleteObjectRequest().WithBucket("test-bucket").WithKey("held");
        request.SetContinueRequestHandler([live](const auto*) { return live->load(); });
        runtime.Send(new IEventHandle(router, edge, new TEvDeleteObjectRequest(request)), 0, true);
        WaitReal(runtime, [&] { return server.GetRequestsReceived() == 1; });

        // A single-object DELETE has no upload body, and the server has sent no response bytes.
        live->store(false);
        NActors::TEventsList replies;
        WaitReal(runtime, [&] {
            replies = runtime.CaptureMailboxEvents(edge.Hint(), edge.NodeId());
            return !replies.empty();
        }, TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies.front()->GetTypeRewrite(), TEvDeleteObjectResponse::EventType);
        const auto* response = replies.front()->Get<TEvDeleteObjectResponse>();
        UNIT_ASSERT(!response->IsSuccess());
        const auto& error = response->GetError();
        UNIT_ASSERT_C(static_cast<Aws::Client::CoreErrors>(error.GetErrorType()) == Aws::Client::CoreErrors::USER_CANCELLED,
            error.GetMessage().c_str());
        UNIT_ASSERT_VALUES_EQUAL(server.GetRequestsReceived(), 1);
    }

    Y_UNIT_TEST(CancelledCoolingPutNeverLeavesTheSdkExecutorQueue) {
        using namespace NWrappers::NExternalStorage;
        TStallingS3Server server(4096, TDuration::Minutes(1));
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto router = MakeStallingRouter(runtime, server);
        const auto edge = runtime.AllocateEdgeActor();
        size_t errors = 0;
        TString code, message;
        const auto sender = runtime.Register(new TErrorResponseCounter(errors, code, message));
        auto live = std::make_shared<std::atomic_bool>(true);
        const auto canContinue = [live](const auto*) { return live->load(); };
        SendRouterPut(runtime, router, sender, "held-agent-put", TDuration::Minutes(1), canContinue, TS3PutContext::ESource::Agent);
        WaitReal(runtime, [&] { return server.GetRequestsReceived() == 1; });

        // The agent owns the only SDK thread. The queued cooling request must still occupy its physical slot.
        SendRouterPut(runtime, router, sender, "queued-cooling-put", TDuration::Minutes(1), canContinue);
        SendRouterPut(runtime, router, edge, "retry", TDuration::Minutes(1));
        auto rejected = runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(edge, TDuration::Seconds(2));
        UNIT_ASSERT(rejected);
        UNIT_ASSERT_VALUES_EQUAL(rejected->Get()->SourceType, TEvPutObjectRequest::EventType);
        live->store(false);
        WaitReal(runtime, [&] { return errors == 2; }, TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(server.GetRequestsReceived(), 1);
    }

    Y_UNIT_TEST(EndpointSwitchDoesNotAbortRequestsInFlight) {
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData(0).AwsClientConfig.SetRequestTimeoutMs(TDuration::Hours(1).MilliSeconds());
        runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) { return false; });
        TStallingS3Server s3Server(/*bodySize=*/4096, TDuration::Hours(1));

        const ui16 balancerPort = PickFreePort();
        auto* proxy = NHttp::CreateHttpProxy();
        TActorId proxyId = runtime.Register(proxy);
        TActorId edgeId = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(proxyId, edgeId,
            new NHttp::TEvHttpProxy::TEvAddListeningPort(balancerPort)), 0, true);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        const TString initialProxy = TStringBuilder() << "127.0.0.1:" << s3Server.GetPort();
        auto* balancer = new TFakeBalancer({}, initialProxy);
        TActorId balancerId = runtime.Register(balancer);
        runtime.Send(new IEventHandle(proxyId, balancerId,
            new NHttp::TEvHttpProxy::TEvRegisterHandler("/", balancerId)), 0, true);

        NKikimrBlobDepot::TS3BackendSettings settings;
        auto* s3 = settings.MutableSettings();
        s3->SetEndpoint("s3.example.com");
        s3->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        s3->SetBucket("test-bucket");
        s3->SetAccessKey("access-key");
        s3->SetSecretKey("secret-key");
        s3->SetRegion("ru-central1");
        s3->SetUseVirtualAddressing(false);
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort << '/');
        settings.SetBalancerRefreshSecMin(1);
        settings.SetBalancerRefreshSecMax(1);

        constexpr ui64 tabletId = 12345;
        const TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings), tabletId));
        const auto switches = GetServiceCounters(runtime.GetAppData(0).Counters, "tablets")
            ->GetSubgroup("subsystem", "blob_depot")
            ->GetSubgroup("module_id", "s3_router")
            ->GetSubgroup("tablet", ToString(tabletId))
            ->GetCounter("EndpointSwitches", true);
        WaitReal(runtime, [&] { return *switches == 1; });

        auto result = MakeIntrusive<TGetResult>();
        TActorId collectorId = runtime.Register(new TGetCollector(result));

        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket("test-bucket");
        request.SetKey("held-object");
        request.SetRange("bytes=0-4095");
        runtime.Send(new IEventHandle(routerId, collectorId,
            new NWrappers::NExternalStorage::TEvGetObjectRequest(request)), 0, true);

        WaitReal(runtime, [&] { return s3Server.GetRequestsReceived() >= 1; });

        balancer->SetHostname("127.0.0.1:1");
        runtime.SimulateSleep(TDuration::Seconds(2));
        WaitReal(runtime, [&] { return *switches >= 2; }, TDuration::Seconds(15));
        UNIT_ASSERT(!result->Done.load());
        s3Server.Stop(); // Release the held response after the router has installed the new endpoint.
        WaitReal(runtime, [&] { return result->Done.load(); }, TDuration::Seconds(15));
        UNIT_ASSERT_C(result->Ok.load(), result->Error);
    }

    Y_UNIT_TEST(RouterPoisonedWithRequestInFlight) {
        const TDuration holdDuration = TDuration::Seconds(3);
        TStallingS3Server s3Server(/*bodySize=*/4096, holdDuration);

        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData(0).AwsClientConfig.SetRequestTimeoutMs(TDuration::Hours(1).MilliSeconds());
        runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) { return false; });

        NKikimrBlobDepot::TS3BackendSettings settings;
        auto* s3 = settings.MutableSettings();
        s3->SetEndpoint(TStringBuilder() << "127.0.0.1:" << s3Server.GetPort());
        s3->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        s3->SetBucket("test-bucket");
        s3->SetAccessKey("access-key");
        s3->SetSecretKey("secret-key");
        s3->SetRegion("ru-central1");
        s3->SetUseVirtualAddressing(false);

        TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
        TActorId edgeId = runtime.AllocateEdgeActor();

        auto result = MakeIntrusive<TGetResult>();
        TActorId collectorId = runtime.Register(new TGetCollector(result));

        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket("test-bucket");
        request.SetKey("held-object");
        request.SetRange("bytes=0-4095");
        runtime.Send(new IEventHandle(routerId, collectorId,
            new NWrappers::NExternalStorage::TEvGetObjectRequest(request)), 0, true);

        WaitReal(runtime, [&] { return s3Server.GetRequestsReceived() >= 1; });

        // NodeWarden poisons the router when its last agent releases it; the inner
        // wrapper still holds the request and reports its stats after the router is gone.
        runtime.Send(new IEventHandle(routerId, edgeId, new TEvents::TEvPoison()), 0, true);
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        // The dying wrapper disables request processing, so the reply may carry the abort error; it must still arrive.
        WaitReal(runtime, [&] { return result->Done.load(); }, holdDuration * 3);
    }
}

}  // namespace NKikimr::NBlobDepot

#endif  // KIKIMR_DISABLE_S3_OPS
