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

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

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

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

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

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

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

    Y_UNIT_TEST(TestMultipleActiveClients) {
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

        // Configure pipe cache with multiple active clients
        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        config->MaxActiveClients = 3;
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        // Send 3 requests with time advances to create up to 3 clients
        for (size_t i = 0; i < 3; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                true)), 0, true);  // subscribe = true to keep client alive
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        // With MaxActiveClients=3, we should have created 3 connections
        UNIT_ASSERT_C(observedConnects == 3,
            "Expected 3 connections with MaxActiveClients=3, got " << observedConnects);
    }

    Y_UNIT_TEST(TestRoundRobinDistribution) {
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

        size_t totalConnects = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Type == TEvTabletPipe::EvConnect) {
                ++totalConnects;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        // Configure pipe cache with multiple active clients
        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        config->MaxActiveClients = 3;
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        // First, create all 3 clients by sending requests with time advances
        for (size_t i = 0; i < 3; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                true)), 0, true);  // subscribe = true to keep client alive
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        size_t connectsAfterSetup = totalConnects;
        UNIT_ASSERT_C(connectsAfterSetup == 3,
            "Expected 3 connections during setup, got " << connectsAfterSetup);

        // Now send many requests without time advance - should reuse existing clients via round-robin
        for (size_t i = 0; i < 30; ++i) {
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);  // subscribe = false for round-robin test
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        // No new connections should be created (round-robin uses existing)
        UNIT_ASSERT_C(totalConnects == connectsAfterSetup,
            "Expected no new connections during round-robin, but got " << (totalConnects - connectsAfterSetup) << " new ones");
    }

    Y_UNIT_TEST(TestForceReconnect) {
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

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->MaxActiveClients = 1;  // Single client to test force reconnect
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        // First request creates a connection
        runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0,
            false)), 0, true);
        runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 1u);

        // Second request without force reconnect reuses the connection
        runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0,
            false)), 0, true);
        runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 1u);

        // Force reconnect
        runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForcePipeReconnect(TTestTxConfig::TxTablet0)), 0, true);
        // Ensure the force reconnect event is processed
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() { return true; };
            runtime.DispatchEvents(options, TDuration::MilliSeconds(10));
        }

        // Next request should create a new connection
        runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0,
            false)), 0, true);
        runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 2u);
    }

    Y_UNIT_TEST(TestIdleClientCleanup) {
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
        size_t observedCloses = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Type == TEvTabletPipe::EvConnect) {
                ++observedConnects;
            } else if (ev->Type == TEvents::TEvPoisonPill::EventType) {
                ++observedCloses;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        config->MaxActiveClients = 3;
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        // Create 3 clients with time advances
        for (size_t i = 0; i < 3; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }
        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 3u);

        // Now advance time and send requests - idle clients should be cleaned up and rotated
        size_t initialCloses = observedCloses;
        for (size_t i = 0; i < 5; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        // Some idle clients should have been closed and new ones created
        UNIT_ASSERT_C(observedConnects > 3u,
            "Expected more connections due to rotation, got " << observedConnects);
        UNIT_ASSERT_C(observedCloses > initialCloses,
            "Expected some client closes due to idle cleanup, got " << (observedCloses - initialCloses));
    }

    Y_UNIT_TEST(TestClientDisconnectWithMultipleActive) {
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

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        config->MaxActiveClients = 2;
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        // Create first client with subscription
        runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0, {
                .Subscribe = true,
                .SubscribeCookie = 1,
            })), 0, true);
        auto ev1 = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        TActorId client1 = ev1->Get()->ClientId;

        // Create second client with time advance
        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        TActorId sender2 = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(cacheActor, sender2, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0, {
                .Subscribe = true,
                .SubscribeCookie = 2,
            })), 0, true);
        auto ev2 = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender2);
        TActorId client2 = ev2->Get()->ClientId;

        UNIT_ASSERT(client1 != client2);

        // Kill client1 - sender should get delivery problem
        runtime.Send(new IEventHandle(client1, sender, new TEvents::TEvPoison), 0, true);
        auto problem = runtime.GrabEdgeEventRethrow<TEvPipeCache::TEvDeliveryProblem>(sender, TDuration::Seconds(1));
        UNIT_ASSERT(problem);
        UNIT_ASSERT_VALUES_EQUAL(problem->Cookie, 1u);

        // sender2 should still work with client2
        runtime.Send(new IEventHandle(cacheActor, sender2, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0, {
                .AutoConnect = false,
                .Subscribe = false,
            })), 0, true);
        auto ev3 = runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender2, TDuration::Seconds(1));
        UNIT_ASSERT(ev3);
        UNIT_ASSERT_VALUES_EQUAL(ev3->Get()->ClientId, client2);
    }

    Y_UNIT_TEST(TestMaxActiveClientsLimit) {
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

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        config->MaxActiveClients = 2;  // Limit to 2
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        // Use 2 senders that subscribe to keep clients alive
        TActorId sender1 = runtime.AllocateEdgeActor();
        TActorId sender2 = runtime.AllocateEdgeActor();

        // Create first client
        runtime.Send(new IEventHandle(cacheActor, sender1, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0, {
                .Subscribe = true,
            })), 0, true);
        runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender1);
        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 1u);

        // Create second client with time advance
        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        runtime.Send(new IEventHandle(cacheActor, sender2, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0, {
                .Subscribe = true,
            })), 0, true);
        runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender2);
        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 2u);

        // Try to create a third client with time advance - should not create new one
        // because both existing clients have subscribers
        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        TActorId sender3 = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(cacheActor, sender3, new TEvPipeCache::TEvForward(
            new TEvCustomTablet::TEvHelloRequest,
            TTestTxConfig::TxTablet0, {
                .Subscribe = true,
            })), 0, true);
        runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender3);
        // No new connection because we're at limit and no idle clients to remove
        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 2u);
    }

    Y_UNIT_TEST(TestBackwardCompatibilitySingleClient) {
        // Test that with MaxActiveClients=1 (default), behavior is similar to before
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

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        // MaxActiveClients defaults to 1
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        // Send multiple requests without subscription
        for (size_t i = 0; i < 5; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        // With MaxActiveClients=1 and PipeRefreshTime, idle clients get rotated
        // Each time the refresh expires, the idle client is replaced
        UNIT_ASSERT_C(observedConnects >= 4,
            "Expected at least 4 connections due to idle rotation with MaxActiveClients=1, got " << observedConnects);
    }

    Y_UNIT_TEST(TestDuplicateFollowerDetection) {
        // Test that duplicate connections to the same follower node are detected and removed
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
        size_t observedCloses = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Type == TEvTabletPipe::EvConnect) {
                ++observedConnects;
            } else if (ev->Type == TEvents::TEvPoisonPill::EventType) {
                ++observedCloses;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        // Configure pipe cache with multiple active clients
        // All clients will connect to the same node (single-node test), triggering duplicate detection
        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        config->MaxActiveClients = 3;
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        TActorId sender = runtime.AllocateEdgeActor();

        // Create 3 idle clients (no subscribers) with time advances
        // All will connect to the same node, so duplicates will be detected
        for (size_t i = 0; i < 3; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);  // subscribe = false means clients are idle
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        // Initially we should have created 3 connections
        UNIT_ASSERT_C(observedConnects >= 3,
            "Expected at least 3 connections, got " << observedConnects);

        // When duplicate detection triggers on the 2nd and 3rd client connecting to the same node,
        // it should close idle duplicates and set ForceReconnect.
        // The next request after ForceReconnect should create additional connections.
        size_t connectsAfterInitial = observedConnects;

        // Send more requests with time advances to trigger ForceReconnect processing
        for (size_t i = 0; i < 5; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, sender, new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(sender);
        }

        // With duplicate detection, we should see more connections as duplicates are replaced
        UNIT_ASSERT_C(observedConnects > connectsAfterInitial,
            "Expected more connections due to duplicate detection and ForceReconnect, "
            "got " << observedConnects << " (was " << connectsAfterInitial << ")");

        // We should also see some closes due to duplicate removal
        UNIT_ASSERT_C(observedCloses > 0,
            "Expected some client closes due to duplicate detection, got " << observedCloses);
    }

    Y_UNIT_TEST(TestDuplicateDetectionWithSubscribers) {
        // Test that duplicate detection does NOT remove clients that have subscribers
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
        size_t observedCloses = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Type == TEvTabletPipe::EvConnect) {
                ++observedConnects;
            } else if (ev->Type == TEvents::TEvPoisonPill::EventType) {
                ++observedCloses;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto config = MakeIntrusive<TPipePerNodeCacheConfig>();
        config->PipeRefreshTime = TDuration::Seconds(1);
        config->MaxActiveClients = 3;
        auto cacheActor = runtime.Register(CreatePipePerNodeCache(config));

        // Create 3 clients WITH subscribers - they should not be removed by duplicate detection
        TVector<TActorId> senders;
        for (size_t i = 0; i < 3; ++i) {
            senders.push_back(runtime.AllocateEdgeActor());
        }

        for (size_t i = 0; i < 3; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, senders[i], new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0, {
                    .Subscribe = true,  // Subscribe keeps client alive
                })), 0, true);
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(senders[i]);
        }

        UNIT_ASSERT_VALUES_EQUAL(observedConnects, 3u);
        size_t closesAfterInitial = observedCloses;

        // Send more requests - since all clients have subscribers, no duplicates should be removed
        for (size_t i = 0; i < 5; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            runtime.Send(new IEventHandle(cacheActor, senders[0], new TEvPipeCache::TEvForward(
                new TEvCustomTablet::TEvHelloRequest,
                TTestTxConfig::TxTablet0,
                false)), 0, true);
            runtime.GrabEdgeEventRethrow<TEvCustomTablet::TEvHelloResponse>(senders[0]);
        }

        // No new closes because all clients have subscribers (non-idle)
        UNIT_ASSERT_C(observedCloses == closesAfterInitial,
            "Expected no additional closes when all clients have subscribers, "
            "but got " << (observedCloses - closesAfterInitial) << " more closes");
    }
}

} // namespace NKikimr
