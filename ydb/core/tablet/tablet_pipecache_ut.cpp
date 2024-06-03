#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPipeCacheTest) {

    struct TEvCustomTablet {
        enum EEv {
            EvHelloRequest = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvHelloResponse,
            EvEnd
        };

        struct TEvHelloRequest : public TEventLocal<TEvHelloRequest, EvHelloRequest> {};
        struct TEvHelloResponse : public TEventLocal<TEvHelloResponse, EvHelloResponse> {
            TActorId ServerId;
            TActorId ClientId;

            TEvHelloResponse(const TActorId& serverId, const TActorId& clientId)
                : ServerId(serverId)
                , ClientId(clientId)
            {}
        };
    };

    class TCustomTablet : public TActor<TCustomTablet>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        TCustomTablet(const TActorId& tablet, TTabletStorageInfo* info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
        { }

    private:
        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTabletPipe::TEvServerConnected, Handle);
                hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
                HFunc(TEvCustomTablet::TEvHelloRequest, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev) {
            auto* msg = ev->Get();
            Y_ABORT_UNLESS(!ServerToClient.contains(msg->ServerId));
            ServerToClient[msg->ServerId] = msg->ClientId;
        }

        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
            auto* msg = ev->Get();
            Y_ABORT_UNLESS(ServerToClient.contains(msg->ServerId));
            ServerToClient.erase(msg->ServerId);
        }

        void Handle(TEvCustomTablet::TEvHelloRequest::TPtr& ev, const TActorContext& ctx) {
            auto serverId = ev->Recipient;
            Y_ABORT_UNLESS(ServerToClient.contains(serverId));
            auto clientId = ServerToClient.at(serverId);
            ctx.Send(ev->Sender, new TEvCustomTablet::TEvHelloResponse(serverId, clientId));
        }

        void OnDetach(const TActorContext& ctx) override {
            return Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override {
            Y_UNUSED(ev);
            return Die(ctx);
        }

        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext& ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
        }

    private:
        THashMap<TActorId, TActorId> ServerToClient;
    };


    Y_UNIT_TEST(TestIdleRefresh) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TCustomTablet(tablet, info);
            });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }

        size_t observedConnects = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Type == TEvTabletPipe::EvConnect) {
                ++observedConnects;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto config = MakeIntrusive<TPipePeNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        auto cacheActor = runtime.Register(CreatePipePeNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();
        for (size_t i = 0; i < 20; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        UNIT_ASSERT_C(observedConnects > 10, "Observed only " << observedConnects << " tablet reconnects");
    }

    Y_UNIT_TEST(TestTabletNode) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        auto config = MakeIntrusive<TPipePeNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        auto cacheActor = runtime.Register(CreatePipePeNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvGetTabletNode(TTestTxConfig::TxTablet0)));
        auto ev1 = runtime.GrabEdgeEventRethrow<TEvPipeCache::TEvGetTabletNodeResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->TabletId, TTestTxConfig::TxTablet0);
        UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->NodeId, 0);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TCustomTablet(tablet, info);
            });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }

        runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvGetTabletNode(TTestTxConfig::TxTablet0)));
        auto ev2 = runtime.GrabEdgeEventRethrow<TEvPipeCache::TEvGetTabletNodeResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->TabletId, TTestTxConfig::TxTablet0);
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->NodeId, runtime.GetNodeId(0));
    }

    Y_UNIT_TEST(TestAutoConnect) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TCustomTablet(tablet, info);
            });

        auto config = MakeIntrusive<TPipePeNodeCacheConfig>();
        auto cacheActor = runtime.Register(CreatePipePeNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        size_t observedRequests = 0;
        auto observeRequests = runtime.AddObserver<TEvCustomTablet::TEvHelloRequest>(
            [&](TEvCustomTablet::TEvHelloRequest::TPtr&) {
                ++observedRequests;
            });

        TActorId client1;
        {
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0, {
                    .Subscribe = true,
                    .SubscribeCookie = 1,
                })), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
            client1 = ev->Get()->ClientId;
            UNIT_ASSERT_VALUES_EQUAL(observedRequests, 1u);
        }

        // Resubscribe which will also change cookie
        {
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0, {
                    .Subscribe = true,
                    .SubscribeCookie = 2,
                })), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ClientId, client1);
            UNIT_ASSERT_VALUES_EQUAL(observedRequests, 2u);
        }

        // Test forward without auto-connect or subscribe will use pipe that is currently subscribed
        {
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0, {
                    .AutoConnect = false,
                    .Subscribe = false,
                })), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender, TDuration::Seconds(1));
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ClientId, client1);
            UNIT_ASSERT_VALUES_EQUAL(observedRequests, 3u);
        }

        // Test that killing the client triggers TEvDeliveryProblem with Cookie==2
        {
            runtime.Send(new IEventHandle(client1, sender, new TEvents::TEvPoison), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvPipeCache::TEvDeliveryProblem>(sender, TDuration::Seconds(1));
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 2u);
        }

        // Test forward without auto-connect or subscribe will not create a new client
        {
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0, {
                    .AutoConnect = false,
                    .Subscribe = false,
                })), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender, TDuration::Seconds(1));
            UNIT_ASSERT(!ev);
            UNIT_ASSERT_VALUES_EQUAL(observedRequests, 3u);
        }

        // Test forward with auto-connect but without subscribe will create and use a new client
        TActorId client2;
        {
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0, {
                    .Subscribe = false,
                })), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender, TDuration::Seconds(1));
            UNIT_ASSERT(ev);
            client2 = ev->Get()->ClientId;
            UNIT_ASSERT(client2 != client1);
            UNIT_ASSERT_VALUES_EQUAL(observedRequests, 4u);
        }

        // Test forward without auto-connect or subscribe will not use pipe that is active but not subscribed
        {
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0, {
                    .AutoConnect = false,
                    .Subscribe = false,
                })), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender, TDuration::Seconds(1));
            UNIT_ASSERT(!ev);
            UNIT_ASSERT_VALUES_EQUAL(observedRequests, 4u);
        }

        // Test that killing the new client does not trigger TEvDeliveryProblem
        {
            runtime.Send(new IEventHandle(client2, sender, new TEvents::TEvPoison), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvPipeCache::TEvDeliveryProblem>(sender, TDuration::Seconds(1));
            UNIT_ASSERT(!ev);
        }
    }
}

} // namespace NKikimr
