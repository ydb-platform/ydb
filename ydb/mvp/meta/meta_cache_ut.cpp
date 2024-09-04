#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <util/system/tempfile.h>
#include <util/stream/null.h>
#include "meta_cache.h"

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

Y_UNIT_TEST_SUITE(MetaCache) {
    class TTestActorRuntime : public NActors::TTestActorRuntimeBase {
    public:
        TTestActorRuntime() {
            Initialize();
            SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
        }

        void SimulateSleep(TDuration duration) {
            auto sender = AllocateEdgeActor();
            Schedule(new IEventHandle(sender, sender, new NActors::TEvents::TEvWakeup()), duration);
            GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(sender);
        }
    };

    static NHttp::TCachePolicy GetCachePolicy(const NHttp::THttpRequest*) {
        NHttp::TCachePolicy policy;

        policy.TimeToExpire = TDuration::Days(7);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;

        return policy;
    }

    static const TString LocalAddress = "127.0.0.1";
    static const TString LocalEndpoint = "http://" + LocalAddress;

    Y_UNIT_TEST(BasicForwarding) {
        TTestActorRuntime actorSystem;
        TPortManager portManager;

        TAutoPtr<NActors::IEventHandle> handle;

        TIpPort port1 = portManager.GetTcpPort();
        TIpPort port2 = portManager.GetTcpPort();

        // first http server (original)
        NActors::IActor* proxy1 = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId1 = actorSystem.Register(proxy1);

        actorSystem.Send(new NActors::IEventHandle(proxyId1, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port1)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        TActorId cacheProxyId1 = actorSystem.Register(NMeta::CreateHttpMetaCache(proxyId1, GetCachePolicy, [=](const TString& id, NMeta::TGetCacheOwnershipCallback cb) {
            Y_UNUSED(id);
            cb({
                .ForwardUrl = TStringBuilder() << LocalEndpoint << ":" << port2,
                .Deadline = TInstant::Now() + TDuration::Seconds(60)
            });
            return true;
        }));

        NActors::TActorId serverId1 = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(cacheProxyId1, serverId1, new NHttp::TEvHttpProxy::TEvRegisterHandler("/server", serverId1)), 0, true);

        // second http server (forwarded)
        NActors::IActor* proxy2 = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId2 = actorSystem.Register(proxy2);

        actorSystem.Send(new NActors::IEventHandle(proxyId2, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port2)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);
        NActors::TActorId serverId2 = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId2, serverId2, new NHttp::TEvHttpProxy::TEvRegisterHandler("/server", serverId2)), 0, true);

        // http client
        NActors::IActor* proxyC = NHttp::CreateHttpProxy();
        NActors::TActorId proxyIdC = actorSystem.Register(proxyC);
        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(LocalEndpoint + ":" + ToString(port1) + "/server");
        actorSystem.Send(new NActors::IEventHandle(proxyIdC, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        // receiving response on server2
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request->Request->URL, "/server");
        UNIT_ASSERT_EQUAL(request->Request->Host, TStringBuilder() << LocalAddress << ":" << port2);

        // constructing response
        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n6\r\npassed\r\n0\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId2, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        // receiving response on client
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Response->Body, "passed");
    }

    Y_UNIT_TEST(TimeoutFallback) {
        TTestActorRuntime actorSystem;
        TPortManager portManager;

        actorSystem.SetScheduledEventFilter([num = 0](NActors::TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration, TInstant&) mutable {
            if (runtime.GetActorName(event->GetRecipientRewrite()) == "NHttp::TOutgoingConnectionActor<NHttp::TPlainSocketImpl>") {
                if (++num == 2) {
                    return false;
                }
            }
            return true;
        });

        TAutoPtr<NActors::IEventHandle> handle;

        TIpPort port1 = portManager.GetTcpPort();
        TIpPort port2 = portManager.GetTcpPort();

        // first http server (original)
        NActors::IActor* proxy1 = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId1 = actorSystem.Register(proxy1);

        actorSystem.Send(new NActors::IEventHandle(proxyId1, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port1)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        TActorId cacheProxyId1 = actorSystem.Register(NMeta::CreateHttpMetaCache(proxyId1, GetCachePolicy, [=](const TString& id, NMeta::TGetCacheOwnershipCallback cb) {
            Y_UNUSED(id);
            cb({
                .ForwardUrl = TStringBuilder() << LocalEndpoint << ":" << port2,
                .Deadline = TInstant::Now() + TDuration::Seconds(600)
            });
            return true;
        }));

        NActors::TActorId serverId1 = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(cacheProxyId1, serverId1, new NHttp::TEvHttpProxy::TEvRegisterHandler("/server", serverId1)), 0, true);

        // second http server (forwarded)
        NActors::IActor* proxy2 = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId2 = actorSystem.Register(proxy2);

        actorSystem.Send(new NActors::IEventHandle(proxyId2, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port2)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);
        NActors::TActorId serverId2 = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId2, serverId2, new NHttp::TEvHttpProxy::TEvRegisterHandler("/server", serverId2)), 0, true);

        // http client
        NActors::IActor* proxyC = NHttp::CreateHttpProxy();
        NActors::TActorId proxyIdC = actorSystem.Register(proxyC);
        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(LocalEndpoint + ":" + ToString(port1) + "/server");
        actorSystem.Send(new NActors::IEventHandle(proxyIdC, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request1 = nullptr;
        TAutoPtr<NActors::IEventHandle> handle1;
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request2 = nullptr;
        TAutoPtr<NActors::IEventHandle> handle2;

        while (!request1 || !request2) {
            // receiving response on server2 and server1
            NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
            if (request->Request->Host == TStringBuilder() << LocalAddress << ":" << port1) {
                request1 = request;
                handle1 = handle;
            } else if (request->Request->Host == TStringBuilder() << LocalAddress << ":" << port2) {
                request2 = request;
                handle2 = handle;
            } else {
                UNIT_ASSERT(false);
            }
            if (!request1 || !request2) {
                // waiting for the timeout - it seems that it doesn't actually work
                actorSystem.SimulateSleep(TDuration::Seconds(120));
            }
        }
        // constructing response
        NHttp::THttpOutgoingResponsePtr httpResponse = request1->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n6\r\npassed\r\n0\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle1->Sender, serverId2, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        // receiving response on client
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Response->Body, "passed");
    }
}
