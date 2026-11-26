#ifndef KIKIMR_DISABLE_S3_OPS

#include "../config.h"
#include "../s3_router.h"
#include "../s3_router_events.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
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
#include <type_traits>

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

void SetStallingEndpoint(NKikimrSchemeOp::TS3Settings& settings, ui16 port) {
    settings.SetEndpoint(TStringBuilder() << "127.0.0.1:" << port);
    settings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
    settings.SetBucket("test-bucket");
    settings.SetAccessKey("access-key");
    settings.SetSecretKey("secret-key");
    settings.SetRegion("ru-central1");
    settings.SetUseVirtualAddressing(false);
    settings.SetHttpRequestTimeoutMs(0);
    settings.SetRequestTimeoutMs(0);
}

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
    TString LastRequestHeaders;

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
    TString GetLastRequestHeaders() {
        TGuard guard(ConnectionsMutex);
        return LastRequestHeaders;
    }

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

        with_lock (ConnectionsMutex) {
            LastRequestHeaders = std::move(request);
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

TActorId RegisterBalancer(TTestActorRuntime& runtime, ui16 port, TFakeBalancer* balancer) {
    const auto proxy = runtime.Register(NHttp::CreateHttpProxy());
    const auto edge = runtime.AllocateEdgeActor();
    runtime.Send(new IEventHandle(proxy, edge, new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
    TAutoPtr<IEventHandle> confirmation;
    runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(confirmation);
    const auto id = runtime.Register(balancer);
    runtime.Send(new IEventHandle(proxy, id, new NHttp::TEvHttpProxy::TEvRegisterHandler("/", id)), 0, true);
    return id;
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

TActorId MakeStallingRouter(TTestActorRuntime& runtime, TStallingS3Server& server, ui16 balancerPort = 0) {
    NKikimrBlobDepot::TS3BackendSettings settings;
    auto* s3 = settings.MutableSettings();
    SetStallingEndpoint(*s3, server.GetPort());
    s3->SetExecutorThreadsCount(1);
    if (balancerPort) {
        s3->SetEndpoint("s3.example.com");
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort << '/');
    }
    settings.MutableAsyncMode()->SetUploadPutsInFlight(1);
    return runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
}

void SendRouterGet(TTestActorRuntime& runtime, TActorId router, TActorId sender, TString key, TDuration timeout) {
    auto context = std::make_shared<TS3GetContext>(runtime.GetCurrentMonotonicTime() + timeout);
    auto request = Aws::S3::Model::GetObjectRequest().WithBucket("test-bucket").WithKey(std::move(key)).WithRange("bytes=0-3");
    runtime.Send(new IEventHandle(router, sender,
        new NWrappers::NExternalStorage::TEvGetObjectRequest(request, std::move(context))), 0, true);
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

    Y_UNIT_TEST(ReplacementUsesConfirmedBalancerEndpointAndNewCredentials) {
        TStallingS3Server storage(4, TDuration::Zero());
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        const ui16 balancerPort = PickFreePort();
        auto* balancer = new TFakeBalancer({}, TStringBuilder() << "127.0.0.1:" << storage.GetPort());
        RegisterBalancer(runtime, balancerPort, balancer);

        NKikimrBlobDepot::TS3BackendSettings settings;
        auto* s3 = settings.MutableSettings();
        s3->SetEndpoint("s3.example.com");
        s3->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        s3->SetBucket("test-bucket");
        s3->SetAccessKey("initial-access");
        s3->SetSecretKey("secret");
        s3->SetRegion("ru-central1");
        s3->SetUseVirtualAddressing(false);
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort << '/');
        const auto endpoint = std::make_shared<TS3BalancerEndpoint>();
        const auto old = runtime.Register(CreateBlobDepotS3Router(settings, 12345, endpoint));
        WaitReal(runtime, [&] { return !endpoint->Get().first.empty(); });
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(old, edge, new TEvents::TEvPoison));

        balancer->SetStatusCode(503);
        const auto replies = balancer->GetReplies();
        settings.SetMetricsPushIntervalMs(100);
        s3->SetAccessKey("rotated-access");
        const auto router = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345, endpoint));
        WaitReal(runtime, [&] { return balancer->GetReplies() > replies; });
        const auto result = MakeIntrusive<TGetResult>();
        const auto collector = runtime.Register(new TGetCollector(result));
        auto request = Aws::S3::Model::GetObjectRequest().WithBucket("test-bucket").WithKey("value").WithRange("bytes=0-3");
        runtime.Send(new IEventHandle(router, collector, new NWrappers::NExternalStorage::TEvGetObjectRequest(request)), 0, true);
        WaitReal(runtime, [&] { return result->Done.load(); });
        UNIT_ASSERT_C(result->Ok.load(), result->Error);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetRequestsReceived(), 1);
        UNIT_ASSERT_STRING_CONTAINS(storage.GetLastRequestHeaders(), "Credential=rotated-access/");
    }

    Y_UNIT_TEST(BalancerPollHappyPath) {
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        const ui16 balancerPort = PickFreePort();

        const TString hostname = "endpoint-A.example.com";
        auto* balancer = new TFakeBalancer({}, hostname);
        RegisterBalancer(runtime, balancerPort, balancer);

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

    Y_UNIT_TEST(StoppedScannerReleasesOnlyItsQueuedLists) {
        using namespace NWrappers::NExternalStorage;
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSettings()->SetBucket("test-bucket");
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << PickFreePort());
        const auto router = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
        const auto stopped = runtime.AllocateEdgeActor();
        const auto live = runtime.AllocateEdgeActor();
        auto list = [&](TActorId sender) {
            runtime.Send(new IEventHandle(router, sender, new TEvListObjectsRequest(Aws::S3::Model::ListObjectsRequest()
                .WithBucket("test-bucket").WithPrefix("vg/"))), 0, true);
        };

        list(live);
        for (size_t i = 0; i < 255; ++i) {
            list(stopped);
        }
        runtime.Send(new IEventHandle(router, stopped, new TEvents::TEvGone), 0, true);
        for (size_t i = 0; i < 256; ++i) {
            list(live);
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        auto replies = runtime.CaptureMailboxEvents(live.Hint(), live.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies.front()->GetTypeRewrite(), TEvListObjectsResponse::EventType);
        UNIT_ASSERT(!replies.front()->Get<TEvListObjectsResponse>()->IsSuccess());
    }

    Y_UNIT_TEST(FiveXxTriggersRefresh) {
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        const ui16 balancerPort = PickFreePort();

        RegisterBalancer(runtime, balancerPort, new TFakeBalancer({}, "endpoint-A.example.com"));

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
        runtime.Send(new IEventHandle(routerId, runtime.AllocateEdgeActor(),
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
        using namespace NWrappers::NExternalStorage;
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto sender = runtime.AllocateEdgeActor();
        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSettings()->SetBucket("test-bucket");
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << PickFreePort());
        const auto router = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
        runtime.EnableScheduleForActor(router);
        SendRouterPut(runtime, router, sender, "long", TDuration::Hours(1));
        SendRouterPut(runtime, router, sender, "short", TDuration::Seconds(1));
        const auto expired = runtime.GrabEdgeEvent<TEvPutObjectResponse>(sender, TDuration::Seconds(2));
        UNIT_ASSERT(expired && !expired->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(expired->Get()->Key.value(), "short");
        UNIT_ASSERT_STRING_CONTAINS(expired->Get()->GetError().GetMessage().c_str(), "expired while waiting for an endpoint");
        runtime.Send(new IEventHandle(router, sender, new TEvents::TEvPoison), 0, true);
        const auto remaining = runtime.GrabEdgeEvent<TEvPutObjectResponse>(sender, TDuration::Seconds(1));
        UNIT_ASSERT(remaining && !remaining->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(remaining->Get()->Key.value(), "long");
    }

    Y_UNIT_TEST(ExpiredGetIsSkippedWhenBalancerRecovers) {
        using namespace NWrappers::NExternalStorage;
        TStallingS3Server storage(4, TDuration::Zero());
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto port = PickFreePort();
        const TString endpoint = TStringBuilder() << "127.0.0.1:" << storage.GetPort();
        auto* balancer = new TFakeBalancer({}, endpoint);
        balancer->SetHoldResponses(true);
        const auto balancerId = RegisterBalancer(runtime, port, balancer);
        const auto router = MakeStallingRouter(runtime, storage, port);
        runtime.EnableScheduleForActor(router);

        const auto live = MakeIntrusive<TGetResult>();
        const auto expired = runtime.AllocateEdgeActor();
        SendRouterGet(runtime, router, runtime.Register(new TGetCollector(live)), "live", TDuration::Minutes(1));
        SendRouterGet(runtime, router, expired, "expired", TDuration::Seconds(1));
        auto deletion = Aws::S3::Model::DeleteObjectsRequest().WithBucket("test-bucket");
        auto deadline = std::make_shared<TS3RequestDeadline>(runtime.GetCurrentMonotonicTime() + TDuration::Seconds(1));
        runtime.Send(new IEventHandle(router, expired, new TEvDeleteObjectsRequest(deletion, deadline)), 0, true);
        const auto failure = runtime.GrabEdgeEvent<TEvGetObjectResponse>(expired, TDuration::Seconds(3));
        UNIT_ASSERT(failure && !failure->Get()->IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(failure->Get()->GetError().GetMessage().c_str(), "expired while waiting for an endpoint");
        const auto deleteFailure = runtime.GrabEdgeEvent<TEvDeleteObjectsResponse>(expired, TDuration::Seconds(3));
        UNIT_ASSERT(deleteFailure && !deleteFailure->Get()->IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(deleteFailure->Get()->GetError().GetMessage().c_str(), "expired while waiting for an endpoint");
        UNIT_ASSERT(!live->Done.load());
        UNIT_ASSERT_VALUES_EQUAL(storage.GetRequestsReceived(), 0);

        runtime.Send(new IEventHandle(balancerId, expired, new TEvReleaseHeldResponses(endpoint)), 0, true);
        WaitReal(runtime, [&] { return live->Done.load(); });
        UNIT_ASSERT_C(live->Ok.load(), live->Error);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetRequestsReceived(), 1);
    }

    Y_UNIT_TEST(UnresolvedOperationsHonorCallerAndTransportDeadlines) {
        using namespace NWrappers::NExternalStorage;
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSettings()->SetBucket("test-bucket");
        settings.MutableSettings()->SetHttpRequestTimeoutMs(0);
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << PickFreePort());
        settings.SetBalancerRefreshSecMin(24 * 60 * 60);
        settings.SetBalancerRefreshSecMax(24 * 60 * 60);
        const auto router = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
        runtime.EnableScheduleForActor(router);
        const auto edge = runtime.AllocateEdgeActor();
        auto deletion = Aws::S3::Model::DeleteObjectsRequest().WithBucket("test-bucket");
        auto deadline = std::make_shared<TS3RequestDeadline>(runtime.GetCurrentMonotonicTime() + TDuration::Seconds(1));
        runtime.Send(new IEventHandle(router, edge, new TEvDeleteObjectsRequest(deletion, deadline)), 0, true);
        const auto expired = runtime.GrabEdgeEvent<TEvDeleteObjectsResponse>(edge, TDuration::Seconds(2));
        UNIT_ASSERT(expired && !expired->Get()->IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(expired->Get()->GetError().GetMessage().c_str(), "expired while waiting for an endpoint");
        // Send every wrapper operation with its default deadline. The unresolved router must answer each once.
        auto check = [&]<typename TRequest, typename TResponse>() {
            typename TRequest::TRequest request;
            request.SetBucket("test-bucket");
            if constexpr (std::is_same_v<TRequest, TEvGetObjectRequest>) {
                request.SetRange("bytes=0-3");
            }
            if constexpr (std::is_constructible_v<TRequest, const typename TRequest::TRequest&, TString&&>) {
                runtime.Send(new IEventHandle(router, edge, new TRequest(request, "body")), 0, true);
            } else {
                runtime.Send(new IEventHandle(router, edge, new TRequest(request)), 0, true);
            }
            const auto reply = runtime.GrabEdgeEvent<TResponse>(edge, DefaultS3PutTimeout + TDuration::Seconds(1));
            UNIT_ASSERT(reply && !reply->Get()->IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(reply->Get()->GetError().GetMessage().c_str(), "expired while waiting for an endpoint");
        };
#define CHECK_REQUEST(name) check.operator()<TEv##name##Request, TEv##name##Response>();
        Y_FOR_EACH_S3_WRAPPER_OP(CHECK_REQUEST)
#undef CHECK_REQUEST
    }

    Y_UNIT_TEST(IdleAgentGetExpiresAndReleasesPhysicalSlot) {
        using namespace NWrappers::NExternalStorage;
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        TStallingS3Server storage(4, TDuration::Minutes(1));
        runtime.GetAppData(0).Icb->CreateConfigControls(true);
        TControlBoard::SetValue(1, runtime.GetAppData(0).Icb->BlobDepotControls.S3MaxGetsInFlight);
        const auto router = MakeStallingRouter(runtime, storage);
        const auto held = MakeIntrusive<TGetResult>();
        SendRouterGet(runtime, router, runtime.Register(new TGetCollector(held)), "held", TDuration::Seconds(3));
        WaitReal(runtime, [&] { return storage.GetRequestsReceived() == 1; });
        const auto replacement = MakeStallingRouter(runtime, storage);
        const auto edge = runtime.AllocateEdgeActor();
        SendRouterGet(runtime, replacement, edge, "over-limit", TDuration::Minutes(1));
        const auto rejected = runtime.GrabEdgeEvent<TEvGetObjectResponse>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(rejected && !rejected->Get()->IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(rejected->Get()->GetError().GetMessage().c_str(), "too many outstanding S3 agent reads");

        // The server has sent no bytes and both SDK timeout settings are zero.
        WaitReal(runtime, [&] { return held->Done.load(); }, TDuration::Seconds(15));
        UNIT_ASSERT(!held->Ok.load());
        const auto live = MakeIntrusive<TGetResult>();
        SendRouterGet(runtime, replacement, runtime.Register(new TGetCollector(live)), "live", TDuration::Minutes(1));
        WaitReal(runtime, [&] { return storage.GetRequestsReceived() == 2; });
        storage.Stop();
        WaitReal(runtime, [&] { return live->Done.load(); });
        UNIT_ASSERT_C(live->Ok.load(), live->Error);
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
        runtime.GetAppData(0).Icb->CreateConfigControls(true);
        TControlBoard::SetValue(1, runtime.GetAppData(0).Icb->BlobDepotControls.S3MaxWritesInFlight);

        const auto router = MakeStallingRouter(runtime, server);
        const auto edge = runtime.AllocateEdgeActor();
        size_t errors = 0;
        TString code, message;
        const auto sender = runtime.Register(new TErrorResponseCounter(errors, code, message));
        auto live = std::make_shared<std::atomic_bool>(true);
        const auto canContinue = [live](const auto*) { return live->load(); };
        SendRouterPut(runtime, router, sender, "held-agent-put", TDuration::Minutes(1), canContinue, TS3PutContext::ESource::Agent);
        WaitReal(runtime, [&] { return server.GetRequestsReceived() == 1; });
        SendRouterPut(runtime, router, edge, "excess-agent-put", TDuration::Minutes(1), {}, TS3PutContext::ESource::Agent);
        const auto excess = runtime.GrabEdgeEvent<TEvPutObjectResponse>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(excess && !excess->Get()->IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(excess->Get()->GetError().GetMessage().c_str(), "too many outstanding S3 agent writes");

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

    Y_UNIT_TEST(PhysicalDeletesHonorConfiguredConcurrencyBeyondTheEndpointQueueLimit) {
        using namespace NWrappers::NExternalStorage;
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        TStallingS3Server storage(4, TDuration::Minutes(1));
        constexpr size_t limit = 272;
        runtime.GetAppData(0).Icb->CreateConfigControls(true);
        TControlBoard::SetValue(limit, runtime.GetAppData(0).Icb->BlobDepotControls.S3MaxDeletesInFlight);
        const auto router = MakeStallingRouter(runtime, storage);
        const auto edge = runtime.AllocateEdgeActor();
        for (size_t index = 0; index <= limit; ++index) {
            auto request = Aws::S3::Model::DeleteObjectRequest().WithBucket("test-bucket").WithKey(ToString(index));
            runtime.Send(new IEventHandle(router, edge, new TEvDeleteObjectRequest(request)), 0, true);
        }
        const auto excess = runtime.GrabEdgeEvent<TEvDeleteObjectResponse>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(excess && !excess->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(excess->Get()->Key.value(), ToString(limit));
        UNIT_ASSERT_STRING_CONTAINS(excess->Get()->GetError().GetMessage().c_str(), "too many outstanding S3 deletes");
        runtime.Send(new IEventHandle(router, edge, new TEvents::TEvPoison()), 0, true);
    }

    Y_UNIT_TEST(EndpointSwitchDoesNotAbortRequestsInFlight) {
        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData(0).AwsClientConfig.SetRequestTimeoutMs(TDuration::Hours(1).MilliSeconds());
        runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) { return false; });
        TStallingS3Server s3Server(/*bodySize=*/4096, TDuration::Hours(1));

        const ui16 balancerPort = PickFreePort();
        const TString initialProxy = TStringBuilder() << "127.0.0.1:" << s3Server.GetPort();
        auto* balancer = new TFakeBalancer({}, initialProxy);
        RegisterBalancer(runtime, balancerPort, balancer);

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
        const TDuration holdDuration = TDuration::Minutes(1);
        TStallingS3Server s3Server(/*bodySize=*/4096, holdDuration);

        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
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
        s3->SetHttpRequestTimeoutMs(0);
        s3->SetRequestTimeoutMs(0);

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

        WaitReal(runtime, [&] { return result->Done.load(); }, TDuration::Seconds(10));
        UNIT_ASSERT(!result->Ok.load());
        UNIT_ASSERT_VALUES_EQUAL(s3Server.GetRequestsReceived(), 1);
    }
}

}  // namespace NKikimr::NBlobDepot

#endif  // KIKIMR_DISABLE_S3_OPS
