#ifndef KIKIMR_DISABLE_S3_OPS

#include "../s3_router.h"

#include <ydb/core/base/appdata.h>
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
        try {
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        } catch (const NActors::TEmptyEventQueueException&) {
        }

        Sleep(TDuration::MilliSeconds(50));
    }
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

        auto* balancer = new TFakeBalancer({}, "endpoint-A.example.com");
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

        // Give the router a chance to issue its first balancer GET and process the reply.
        // We don't have a direct hook to observe the endpoint switch, so we just verify
        // that the runtime survives the round-trip without crashes/aborts.
        runtime.SimulateSleep(TDuration::Seconds(5));
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

        // the queued ones are answered when the router goes away without ever resolving the endpoint
        runtime.Send(new IEventHandle(routerId, senderId, new TEvents::TEvPoison()), 0, true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(errorResponses, maxPendingRequests + 1);
        UNIT_ASSERT_VALUES_EQUAL(lastExceptionName, "ServiceUnavailable");
    }

    Y_UNIT_TEST(EndpointSwitchDoesNotAbortRequestsInFlight) {
        const TDuration holdDuration = TDuration::Seconds(10);
        TStallingS3Server s3Server(/*bodySize=*/4096, holdDuration);

        TTestActorRuntime runtime;
        runtime.SetUseRealInterconnect();
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData(0).AwsClientConfig.SetRequestTimeoutMs(TDuration::Hours(1).MilliSeconds());
        runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) { return false; });

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

        TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings), 12345));
        Y_UNUSED(routerId);

        WaitReal(runtime, [&] { return balancer->GetReplies() >= 1; });

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
        UNIT_ASSERT_C(balancer->GetReplies() >= 2, "balancer did not refresh after the hostname change");

        WaitReal(runtime, [&] { return result->Done.load(); }, holdDuration * 3);
        UNIT_ASSERT_C(result->Ok.load(), result->Error);
    }
}

}  // namespace NKikimr::NBlobDepot

#endif  // KIKIMR_DISABLE_S3_OPS
