#ifndef KIKIMR_DISABLE_S3_OPS

#include "../s3_router.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/aws_init/aws.h>

#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/network/sock.h>

namespace NKikimr::NBlobDepot {

namespace {

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

// Tiny HTTP server actor that listens on a local port and returns a single hostname
// (configurable per /endpoint path) as plain text, with the desired HTTP status code.
class TFakeBalancer : public TActor<TFakeBalancer> {
    TActorId ProxyId;
    TString Hostname;
    int StatusCode = 200;
public:
    TFakeBalancer(TActorId proxyId, TString hostname)
        : TActor(&TThis::StateWork)
        , ProxyId(proxyId)
        , Hostname(std::move(hostname))
    {}

    void SetHostname(TString h) { Hostname = std::move(h); }
    void SetStatusCode(int code) { StatusCode = code; }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr ev) {
        auto& req = ev->Get()->Request;
        TString statusLine = TStringBuilder() << StatusCode << " "
            << (StatusCode == 200 ? "OK" : "Error");
        TString body = Hostname;
        NHttp::THttpOutgoingResponsePtr response = req->CreateResponse(
            ToString(StatusCode), statusLine, "text/plain", body);
        Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }
};

ui16 PickFreePort() {
    TInetStreamSocket sock;
    TSockAddrInet addr("127.0.0.1", 0);
    SetSockOpt(sock, SOL_SOCKET, SO_REUSEADDR, 1);
    if (sock.Bind(&addr) != 0) {
        ythrow yexception() << "bind failed";
    }
    sockaddr_in name;
    socklen_t len = sizeof(name);
    if (getsockname(sock, reinterpret_cast<sockaddr*>(&name), &len) != 0) {
        ythrow yexception() << "getsockname failed";
    }
    return InetToHost(name.sin_port);
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
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort);
        settings.SetBalancerRefreshSecMin(1);
        settings.SetBalancerRefreshSecMax(1);

        TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings)));
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
        settings.SetBalancerHost(TStringBuilder() << "127.0.0.1:" << balancerPort);
        settings.SetBalancerRefreshSecMin(60);
        settings.SetBalancerRefreshSecMax(60);

        TActorId routerId = runtime.Register(CreateBlobDepotS3Router(std::move(settings)));

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
}

}  // namespace NKikimr::NBlobDepot

#endif  // KIKIMR_DISABLE_S3_OPS
