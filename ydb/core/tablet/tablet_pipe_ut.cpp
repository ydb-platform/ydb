#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tx/tx.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
    struct TEvProducerTablet {
        enum EEv {
            EvConnect = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSend,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvConnect : public TEventLocal<TEvConnect, EvConnect> {
            TEvConnect(bool useBadTabletId = false, bool connectToUserTablet = true, bool withRetryPolicy = false)
                : UseBadTabletId(useBadTabletId)
                , ConnectToUserTablet(connectToUserTablet)
                , WithRetryPolicy(withRetryPolicy)
            {
            }

            const bool UseBadTabletId;
            const bool ConnectToUserTablet;
            const bool WithRetryPolicy;
        };

        struct TEvSend : public TEventLocal<TEvSend, EvSend> {
            TEvSend(bool useShutdown = false)
                : UseShutdown(useShutdown)
            {
            }

            const bool UseShutdown;
        };
    };

    struct TEvConsumerTablet {
        enum EEv {
            EvConnect = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvReject,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvReject : public TEventLocal<TEvReject, EvReject> {};
    };

    class TProducerTablet : public TActor<TProducerTablet>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        TProducerTablet(const TActorId &tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
            , HasData(false)
            , IsOpened(false)
            , IsShutdown(false)
        {
            Config.ConnectToUserTablet = true;
        }

    private:
        using IActor::Send; // name is used by IActor API

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
                HFunc(TEvProducerTablet::TEvConnect, Handle);
                HFunc(TEvProducerTablet::TEvSend, Handle);
                HFunc(TEvTabletPipe::TEvClientConnected, Handle);
                HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                HFunc(TEvents::TEvPong, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

        void Handle(TEvProducerTablet::TEvConnect::TPtr &ev, const TActorContext &ctx) {
            Cout << "Connect to another tablet\n";

            IsOpened = false;
            if (!!ClientId) {
                NTabletPipe::CloseClient(ctx, ClientId);
            }

            Config.ConnectToUserTablet = ev->Get()->ConnectToUserTablet;
            if (ev->Get()->WithRetryPolicy) {
                Config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            }

            auto client = NTabletPipe::CreateClient(ctx.SelfID, ev->Get()->UseBadTabletId ?
                TTestTxConfig::TxTablet2 : TTestTxConfig::TxTablet1, Config);
            ClientId = ctx.ExecutorThread.RegisterActor(client, TMailboxType::Simple, Max<ui32>(), ctx.SelfID);
        }

        void Handle(TEvProducerTablet::TEvSend::TPtr &ev, const TActorContext &ctx) {
            if (!ClientId)
                return;

            HasData = true;
            Send(ctx);
            if (ev->Get()->UseShutdown) {
                HasData = false;
                IsShutdown = true;
                NTabletPipe::ShutdownClient(ctx, ClientId);
            }
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            Cout << "Open " << ((ev->Get()->Status == NKikimrProto::OK) ? "ok" : "ERROR") << "\n";
            if (ev->Get()->Status == NKikimrProto::OK) {
                IsOpened = true;
                if (HasData) {
                    Cout << "Send data again\n";
                    Send(ctx);
                }
            } else if (ClientId == ev->Get()->ClientId) {
                ClientId = TActorId();
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
            Cout << "Pipe reset on client\n";
            if (IsOpened && ClientId == ev->Get()->ClientId && !IsShutdown) {
                Cout << "Recreate client\n";
                auto client = NTabletPipe::CreateClient(ctx.SelfID, TTestTxConfig::TxTablet1, Config);
                ClientId = ctx.ExecutorThread.RegisterActor(client);
            }
        }

        void Handle(TEvents::TEvPong::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
            Cout << "Got pong\n";
        }

        void Send(const TActorContext& ctx) {
            Cout << "Send data to another tablet\n";
            NTabletPipe::SendData(ctx, ClientId, new TEvents::TEvPing());
        }

        void OnDetach(const TActorContext &ctx) override {
            Cout << "Producer dead\n";
            NTabletPipe::CloseClient(ctx, ClientId);
            return Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
            Y_UNUSED(ev);
            Cout << "Producer dead\n";
            NTabletPipe::CloseClient(ctx, ClientId);
            return Die(ctx);
        }

        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext &ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
            Cout << "Producer loaded\n";
        }

    private:
        TActorId ClientId;
        bool HasData;
        bool IsOpened;
        bool IsShutdown;
        NTabletPipe::TClientConfig Config;
    };

    struct TEvPrivate {
        enum EEv {
            EvGetServerPipeInfo = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvServerPipeInfo,

            EvEnd
        };

        struct TEvGetServerPipeInfo : public TEventLocal<TEvGetServerPipeInfo, EvGetServerPipeInfo> {
        };

        struct TEvServerPipeInfo : public TEventLocal<TEvServerPipeInfo, EvServerPipeInfo> {
            TEvServerPipeInfo(ui32 opened, ui32 closed)
                : ServerPipesOpened(opened)
                , ServerPipesClosed(closed)
            {}

            ui32 ServerPipesOpened;
            ui32 ServerPipesClosed;
        };
    };

    class TConsumerTablet : public TActor<TConsumerTablet>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        TConsumerTablet(const TActorId &tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
            , PipeConnectAcceptor(NTabletPipe::CreateConnectAcceptor(TabletID()))
            , RejectAll(false)
            , ServerPipesOpened(0)
            , ServerPipesClosed(0)
        {
        }

    private:
        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext& ctx) override {
            Become(&TThis::StateWork);
            Cout << "Consumer loaded\n";
            SignalTabletActive(ctx);
            PipeConnectAcceptor->Activate(SelfId(), SelfId());
        }

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
                HFunc(TEvTabletPipe::TEvConnect, Handle);
                HFunc(TEvTabletPipe::TEvServerConnected, Handle);
                HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
                HFunc(TEvTabletPipe::TEvServerDestroyed, Handle);
                HFunc(TEvents::TEvPing, Handle);
                HFunc(TEvConsumerTablet::TEvReject, Handle);
                HFunc(TEvPrivate::TEvGetServerPipeInfo, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

        void Handle(TEvTabletPipe::TEvConnect::TPtr &ev, const TActorContext &) {
            Cout << "Get connect request from another tablet\n";
            if (RejectAll) {
                PipeConnectAcceptor->Reject(ev, SelfId(), NKikimrProto::BLOCKED);
            } else {
                LastServerId = PipeConnectAcceptor->Accept(ev, SelfId(), SelfId());
            }
        }

        void HandleQueued(TEvTabletPipe::TEvConnect::TPtr &ev, const TActorContext &) {
            Cout << "Enqueue connect request from another tablet\n";
            if (RejectAll) {
                PipeConnectAcceptor->Reject(ev, SelfId(), NKikimrProto::BLOCKED);
            } else {
                LastServerId = PipeConnectAcceptor->Enqueue(ev, SelfId());
            }
        }

        void Handle(TEvents::TEvPing::TPtr &ev, const TActorContext &ctx) {
            Cout << "Got ping\n";
            UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), ctx.SelfID);
            UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, LastServerId);
            ctx.Send(ev->Sender, new TEvents::TEvPong());
        }

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
            Cout << "Server pipe is opened\n";
            ++ServerPipesOpened;
        }

        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
            Cout << "Pipe reset on server\n";
            ++ServerPipesClosed;
        }

        void Handle(TEvTabletPipe::TEvServerDestroyed::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            PipeConnectAcceptor->Erase(ev);
            Cout << "Cleanup of pipe reset on server\n";
        }

        void Handle(TEvConsumerTablet::TEvReject::TPtr &ev, const TActorContext &) {
            Y_UNUSED(ev);
            Cout << "Drop & reject all connects\n";
            PipeConnectAcceptor->Detach(SelfId());
            RejectAll = true;
        }

        void Handle(TEvPrivate::TEvGetServerPipeInfo::TPtr &ev, const TActorContext &ctx) {
            Cout << "Server pipes opened: " << ServerPipesOpened << ", closed: " << ServerPipesClosed << "\n";
            ctx.Send(ev->Sender, new TEvPrivate::TEvServerPipeInfo(ServerPipesOpened, ServerPipesClosed));
        }

        void OnDetach(const TActorContext &ctx) override {
            Cout << "Consumer dead\n";
            PipeConnectAcceptor->Detach(SelfId());
            return Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
            Cout << "Consumer dead\n";
            Y_UNUSED(ev);
            PipeConnectAcceptor->Detach(SelfId());
            return Die(ctx);
        }

        void Enqueue(STFUNC_SIG) override {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTabletPipe::TEvConnect, HandleQueued);
            }
        }

        THolder<NTabletPipe::IConnectAcceptor> PipeConnectAcceptor;
        bool RejectAll;
        TActorId LastServerId;
        ui32 ServerPipesOpened;
        ui32 ServerPipesClosed;
    };

    class TConsumerTabletWithoutAcceptor : public TActor<TConsumerTabletWithoutAcceptor>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        TConsumerTabletWithoutAcceptor(const TActorId &tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
        {
        }

    private:
        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext& ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
            Cout << "Consumer loaded\n";
        }

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
                HFunc(TEvTabletPipe::TEvServerConnected, Handle);
                HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
                HFunc(TEvents::TEvPing, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

        void Handle(TEvents::TEvPing::TPtr &ev, const TActorContext &ctx) {
            Cout << "Got ping\n";
            UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), ctx.SelfID);
            ctx.Send(ev->Sender, new TEvents::TEvPong());
        }

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
            Cout << "Server pipe is opened\n";
        }

        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
            Cout << "Pipe reset on server\n";
        }

        void OnDetach(const TActorContext &ctx) override {
            Cout << "Consumer dead\n";
            return Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
            Cout << "Consumer dead\n";
            Y_UNUSED(ev);
            return Die(ctx);
        }
    };

Y_UNIT_TEST_SUITE(TTabletPipeTest) {
    Y_UNIT_TEST(TestOpen) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(true));
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestSendWithoutWaitOpen) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestKillClientBeforServerIdKnown) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        runtime.SetLogPriority(NKikimrServices::PIPE_SERVER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }

        NTabletPipe::TClientConfig config;
        config.ConnectToUserTablet = false;

        i32 i = 3;
        while (i --> 0) {
            auto client = NTabletPipe::CreateClient(sender, TTestTxConfig::TxTablet1, config);
            TActorId clientId = runtime.Register(client);

            // We want to close the client right after it has sent EvConnect to the target tablet but before
            // the client received the EvConnectResult
            runtime.SetObserverFunc([clientId, sender, &runtime](TAutoPtr<IEventHandle>& event) {
                if (event->Type == TEvTabletPipe::EvConnect) {
                    runtime.Send(new IEventHandle(clientId, sender, new TEvents::TEvPoisonPill()), 0);
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
            runtime.DispatchEvents();
        }

        {
            ForwardToTablet(runtime, TTestTxConfig::TxTablet1, sender, new TEvPrivate::TEvGetServerPipeInfo());
            TAutoPtr<IEventHandle> handle;
            const TEvPrivate::TEvServerPipeInfo* ev = runtime.GrabEdgeEvent<TEvPrivate::TEvServerPipeInfo>(handle);
            UNIT_ASSERT_VALUES_EQUAL(ev->ServerPipesOpened, ev->ServerPipesClosed);
        }
    }

    Y_UNIT_TEST(TestSendWithoutWaitOpenToWrongTablet) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(true));
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestSendAfterOpen) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestSendAfterReboot) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        TVector<ui64> tabletIds;
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
        auto guard = CreateTabletScheduledEventsGuard(tabletIds, runtime, sender);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(false, true, true));
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        Cout << "Reboot consumer\n";
        RebootTablet(runtime, TTestTxConfig::TxTablet1, sender);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }

        Cout << "Reboot producer\n";
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(false, true, true));
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestConsumerSidePipeReset) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }


        ForwardToTablet(runtime, TTestTxConfig::TxTablet1, sender, new TEvConsumerTablet::TEvReject());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientDestroyed, 1));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestConnectReject) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet1, sender, new TEvConsumerTablet::TEvReject());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestSendAfterOpenUsingTabletWithoutAcceptor) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTabletWithoutAcceptor(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(false, false));
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestRebootUsingTabletWithoutAcceptor) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();

        TVector<ui64> tabletIds;
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
        auto guard = CreateTabletScheduledEventsGuard(tabletIds, runtime, sender);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTabletWithoutAcceptor(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(false, false, true));
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        Cout << "Reboot consumer\n";
        RebootTablet(runtime, TTestTxConfig::TxTablet1, sender);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }

        Cout << "Reboot producer\n";
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(false, false, true));
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestShutdown) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(false));
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend(true));
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestTwoNodes) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);

        TActorId sender1 = runtime.AllocateEdgeActor(0);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        }, 0);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        }, 1);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvConnect(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvSend(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestClientDisconnectAfterPipeOpen) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);

        TActorId sender1 = runtime.AllocateEdgeActor(0);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        }, 0);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        }, 1);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvConnect(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        TActorId proxy = runtime.GetInterconnectProxy(0, 1);
        Y_ABORT_UNLESS(proxy);
        runtime.Send(new IEventHandle(proxy, sender1, new TEvInterconnect::TEvConnectNode), 0);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
        auto sessionId = handle->Sender;
        Cout << "SessionId: " << sessionId << "\n";

        runtime.Send(new IEventHandle(sessionId, sender1, new TEvents::TEvPoisonPill), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvSend(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestSendBeforeBootTarget) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        TActorId sender = runtime.AllocateEdgeActor();
        TVector<ui64> tabletIds;
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
        auto scheduledEventsGuard = CreateTabletScheduledEventsGuard(tabletIds, runtime, sender);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvConnect(false, true, true));

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            options.NonEmptyMailboxes.push_back(TEventMailboxId(sender.NodeId(), sender.Hint()));
            runtime.DispatchEvents(options);
            auto events = runtime.CaptureEvents();
            TAutoPtr<IEventHandle> handle;
            UNIT_ASSERT(GrabEvent<TEvTabletResolver::TEvForwardResult>(events, handle));
            runtime.PushEventsFront(events);
        }

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletResolver::EvForward, 15));
            runtime.DispatchEvents(options);
        }

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            options.NonEmptyMailboxes.push_back(TEventMailboxId(sender.NodeId(), sender.Hint()));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvProducerTablet::TEvSend());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestTwoNodesAndRebootOfProducer) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);

        TActorId sender1 = runtime.AllocateEdgeActor(0);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        }, 0);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        }, 1);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvConnect(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvSend(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }

        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender1);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvConnect(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvSend(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestTwoNodesAndRebootOfConsumer) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);
        runtime.SetLogPriority(NActorsServices::INTERCONNECT, NActors::NLog::PRI_DEBUG);

        TActorId sender1 = runtime.AllocateEdgeActor(0);
        TActorId sender2 = runtime.AllocateEdgeActor(1);

        TVector<ui64> tabletIds;
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
        auto guard = CreateTabletScheduledEventsGuard(tabletIds, runtime, sender1);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TProducerTablet(tablet, info);
        }, 0);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet1, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TConsumerTablet(tablet, info);
        }, 1);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 2));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvConnect(false, true, true), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvSend(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }

        RebootTablet(runtime, TTestTxConfig::TxTablet1, sender2, 1);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvConnect(false, true, true), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvClientConnected, 1));
            runtime.DispatchEvents(options);
        }

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvProducerTablet::TEvSend(), 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvents::THelloWorld::Pong));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestRewriteSameNode) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);

        TActorId edge1 = runtime.AllocateEdgeActor(0);
        TActorId edge2 = runtime.AllocateEdgeActor(1);

        // Make an event that is from edge1 to edge2, but rewritten for edge1
        auto event = new IEventHandle(edge2, edge1, new TEvents::TEvWakeup());
        event->Rewrite(TEvents::TSystem::Wakeup, edge1);

        // We send this event from node 1, which should be delivered to edge1
        runtime.Send(event, 0, /* viaActorSystem */ true);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
        UNIT_ASSERT(handle);
        UNIT_ASSERT_C(handle->Recipient == edge2, "Event recipient " << handle->Recipient << " != " << edge2);
        UNIT_ASSERT_C(handle->GetRecipientRewrite() == edge1, "Event rewrite recipient " << handle->GetRecipientRewrite() << " != " << edge1);
    }

    class TEchoTablet : public TActor<TEchoTablet>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        TEchoTablet(const TActorId &tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
        {
        }

    private:
        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext& ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
        }

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
                IgnoreFunc(TEvTabletPipe::TEvServerConnected);
                IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
                HFunc(TEvents::TEvPing, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

        void SendViaSession(const TActorId &sessionId, const TActorContext &ctx, const TActorId &recipient, TAutoPtr<IEventBase> msg, ui32 flags = 0, ui64 cookie = 0) {
            TAutoPtr<IEventHandle> ev = new IEventHandle(recipient, ctx.SelfID, msg.Release(), flags, cookie);

            if (sessionId) {
                ev->Rewrite(TEvInterconnect::EvForward, sessionId);
            }

            ctx.ExecutorThread.Send(ev);
        }

        void Handle(TEvents::TEvPing::TPtr &ev, const TActorContext &ctx) {
            UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), ctx.SelfID);

            if (ev->Sender.NodeId() != ctx.SelfID.NodeId()) {
                UNIT_ASSERT(ev->InterconnectSession);
            } else {
                UNIT_ASSERT(!ev->InterconnectSession);
            }

            SendViaSession(ev->InterconnectSession, ctx, ev->Sender, new TEvents::TEvPong());
        }

        void OnDetach(const TActorContext &ctx) override {
            Cout << "Tablet dead\n";
            return Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
            Cout << "Tablet dead\n";
            Y_UNUSED(ev);
            return Die(ctx);
        }
    };

    Y_UNIT_TEST(TestInterconnectSession) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);
        runtime.SetLogPriority(NActorsServices::INTERCONNECT, NActors::NLog::PRI_DEBUG);

        TActorId sender1 = runtime.AllocateEdgeActor(0);
        TActorId sender2 = runtime.AllocateEdgeActor(1);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy), [](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TEchoTablet(tablet, info);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }

        TAutoPtr<IEventHandle> handle;

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender1, new TEvents::TEvPing(), /* node index */ 0);
        runtime.GrabEdgeEvent<TEvents::TEvPong>(handle);
        UNIT_ASSERT(handle);
        UNIT_ASSERT_C(handle->Recipient == sender1, "Event recipient " << handle->Recipient << " != " << sender1);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender2, new TEvents::TEvPing(), /* node index */ 1);
        runtime.GrabEdgeEvent<TEvents::TEvPong>(handle);
        UNIT_ASSERT(handle);
        UNIT_ASSERT_C(handle->Recipient == sender2, "Event recipient " << handle->Recipient << " != " << sender2);
    }

}

}
