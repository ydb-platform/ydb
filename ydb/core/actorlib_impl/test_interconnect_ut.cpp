#include <ydb/library/actors/interconnect/interconnect_channel.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/driver_lib/version/version.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/protos/unittests.pb.h>
#include <library/cpp/http/io/headers.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/sanitizers.h>
#include <util/thread/factory.h>

namespace NKikimr {
    using namespace NActors;

Y_UNIT_TEST_SUITE(TInterconnectTest) {
    class TWall : public NActors::TActor<TWall> {
    public:
        TWall() noexcept
            : TActor(&TThis::StateFunc)
        {}

    private:
        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                fFunc(TEvents::TEvPing::EventType, OnPing);
            }
        }

        void OnPing(STFUNC_SIG) noexcept
        {
            Send(ev->Sender, new TEvents::TEvPong, (ev->GetChannel() << IEventHandle::ChannelShift) | (ev->GetSubChannel() ? IEventHandle::FlagUseSubChannel : 0) , ev->Cookie);
        }
    };

    class TKiller : public NActors::TActor<TKiller> {
    public:
        TKiller() noexcept
            : TActor(&TThis::StateFunc)
        {}

    private:
        STFUNC(StateFunc) {
            Send(ev->InterconnectSession, new TEvents::TEvPoisonPill);
        }
    };

    class TFlooder : public NActors::TActor<TFlooder> {
    public:
        TFlooder(const TActorId& peer, const TActorId& edge, unsigned count) noexcept
            : TActor(&TThis::InitFunc), Peer(peer), Edge(edge), Counter(count), Responses(0ULL)
        {}

    private:
        STFUNC(InitFunc) {
            switch (ev->GetTypeRewrite()) {
                CFunc(TEvents::TSystem::Bootstrap, OnStart);
            }
        }

        STFUNC(WaitFunc) {
            switch (ev->GetTypeRewrite()) {
                CFunc(TEvInterconnect::EvNodeConnected, OnConnect);
            }
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                fFunc(TEvents::TEvPong::EventType, OnPong);
            }
        }

        TAutoPtr<IEventHandle> AfterRegister(const TActorId &self, const TActorId& parentId) noexcept override {
            return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
        }

        void OnStart(const NActors::TActorContext &ctx) noexcept {
            Become(&TThis::WaitFunc);
            ctx.Send(TActivationContext::InterconnectProxy(Peer.NodeId()), new TEvInterconnect::TEvConnectNode, true);
        }

        void OnConnect(const NActors::TActorContext &ctx) noexcept {
            LOG_NOTICE(ctx, NActorsServices::TEST, "Node connected.");
            Become(&TThis::StateFunc);
            for (ui64 i = 0; i < Counter; ++i) {
                ctx.Send(Peer, new TEvents::TEvPing, 0, i);
            }
        }

        void OnPong(STFUNC_SIG) noexcept
        {
            UNIT_ASSERT_EQUAL(Responses, ev->Cookie);

            if (++Responses == Counter) {
               Send(Edge, new TEvents::TEvWakeup);
               ALOG_NOTICE(NActorsServices::TEST, "Done.");
            }
        }

        const TActorId Peer, Edge;
        const unsigned Counter;
        unsigned Responses;
    };

    TAutoPtr<IEventHandle> GetSerialized(const TAutoPtr<IEventHandle>& ev) {
        NActors::TAllocChunkSerializer chunker;
        ev->GetBase()->SerializeToArcadiaStream(&chunker);
        auto Data = chunker.Release(ev->GetBase()->CreateSerializationInfo());
        TAutoPtr<IEventHandle> serev =
            new IEventHandle(ev->GetBase()->Type(), ev->Flags,
                             ev->Recipient, ev->Sender,
                             std::move(Data), ev->Cookie);
        return serev;
    }

    Y_UNIT_TEST(TestSimplePingPong) {
        TTestBasicRuntime runtime(2);
        runtime.SetLogPriority(NActorsServices::INTERCONNECT,
                               NActors::NLog::PRI_DEBUG);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);
        const auto wall = runtime.Register(new TWall, 1);

        runtime.Send(new IEventHandle(wall, edge, new TEvents::TEvPing, 7 << IEventHandle::ChannelShift, 13), 0);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvPong>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 13);
        UNIT_ASSERT_EQUAL(handle->GetChannel(), 7);
    }

    Y_UNIT_TEST(TestManyEvents) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);
        const auto wall = runtime.Register(new TWall, 1);
        runtime.Register(new TFlooder(wall, edge, 100000), 0);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
    }

    Y_UNIT_TEST(TestCrossConnect) {
        TInstant time = TInstant::Now();
        constexpr ui64 iterations = NSan::PlainOrUnderSanitizer(200, 50);
        for (ui64 i = 0; i < iterations; ++i) {
            Cerr << "Starting iteration " << i << Endl;
            TTestBasicRuntime runtime(2);
            runtime.SetLogPriority(NActorsServices::INTERCONNECT, NActors::NLog::PRI_DEBUG);
            runtime.SetDispatcherRandomSeed(time, i);
            runtime.Initialize(TAppPrepare().Unwrap());
            const auto edge0 = runtime.AllocateEdgeActor(0);
            const auto edge1 = runtime.AllocateEdgeActor(1);
            const auto wall0 = runtime.Register(new TWall, 0);
            const auto wall1 = runtime.Register(new TWall, 1);

            runtime.Register(new TFlooder(wall1, edge0, 10), 0);
            runtime.Register(new TFlooder(wall0, edge1, 10), 1);

            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
        }
    }

    Y_UNIT_TEST(TestConnectAndDisconnect) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);
        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), edge, new TEvInterconnect::TEvConnectNode), 0, true);

        TAutoPtr<IEventHandle> handle;
        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(1));
        }

        runtime.Send(new IEventHandle(handle->Sender, edge, new TEvents::TEvPoisonPill), 0, true);

        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeDisconnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(1));
        }

    }

    Y_UNIT_TEST(TestBlobEvent) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        TString blob(100000, '@');
        for (auto i = 0U; i < blob.size(); ++i)
             const_cast<TString::value_type*>(blob.data())[i] = TString::value_type(i % 256);

        runtime.Send(new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, 13), 0);

        TAutoPtr<IEventHandle> handle;
        const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 13);
        UNIT_ASSERT_EQUAL(event->Blob, blob);
    }

    Y_UNIT_TEST(TestBlobEventPreSerialized) {
        TTestBasicRuntime runtime(2);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        TString blob(100000, '@');
        for (auto i = 0U; i < blob.size(); ++i)
             const_cast<TString::value_type*>(blob.data())[i] =
                 TString::value_type(i % 256);

        for (int count = 0; count < 100; ++count) {
            NActors::TAllocChunkSerializer chunker;
            TAutoPtr<IEventHandle> raw =
                new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, 13);
            TAutoPtr<IEventHandle> serev = GetSerialized(raw);

            runtime.Send(serev.Release(), 0);

            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, 13);
            UNIT_ASSERT_EQUAL(event->Blob, blob);
        }
    }

    Y_UNIT_TEST(TestNotifyUndelivered) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge0 = runtime.AllocateEdgeActor(0);
        const auto edge1 = runtime.AllocateEdgeActor(1);

        const TString blob(10000000, '#');

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), edge0, new TEvInterconnect::TEvConnectNode), 0, true);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);

        runtime.Send(new IEventHandle(edge1, edge0, new TEvents::TEvBlob(blob), IEventHandle::FlagTrackDelivery, 777), 0, true);
        runtime.Send(new IEventHandle(handle->Sender, edge0, new TEvents::TEvPoisonPill), 0, true);
        const auto event = runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 777);
        UNIT_ASSERT_EQUAL(event->SourceType, TEvents::TEvBlob::EventType);
        UNIT_ASSERT_EQUAL(event->Reason, TEvents::TEvUndelivered::Disconnected);
    }

    Y_UNIT_TEST(TestSubscribeByFlag) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);
        const auto wall = runtime.Register(new TKiller, 1);

        runtime.Send(new IEventHandle(wall, edge, new TEvents::TEvPing, IEventHandle::FlagSubscribeOnSession), 0);
        {
            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(1));
        }
        {
            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeDisconnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(1));
        }
    }

    Y_UNIT_TEST(TestReconnect) {
        TTestBasicRuntime runtime(2);
        runtime.SetUseRealInterconnect();
        runtime.SetLogPriority(NActorsServices::INTERCONNECT, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NActorsServices::INTERCONNECT_SESSION, NActors::NLog::PRI_DEBUG);
        SOCKET s = INVALID_SOCKET;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (auto *p = ev->CastAsLocal<TEvHandshakeDone>()) {
                s = *p->Socket;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto edge = runtime.AllocateEdgeActor(0);
        const auto wall = runtime.Register(new TWall, 1);

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), edge, new TEvInterconnect::TEvConnectNode), 0, true);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
        const auto session = handle->Sender;

        runtime.Send(new IEventHandle(wall, edge, new TEvents::TEvPing), 0, true);
        runtime.GrabEdgeEvent<TEvents::TEvPong>(handle);

        ShutDown(s, SHUT_RDWR);

        runtime.Send(new IEventHandle(wall, edge, new TEvents::TEvPing), 0, true);
        runtime.GrabEdgeEvent<TEvents::TEvPong>(handle);

        runtime.Send(new IEventHandle(session, edge, new TEvents::TEvPoisonPill));
        runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeDisconnected>(handle);
        UNIT_ASSERT_EQUAL(handle->Sender, session);
    }

    Y_UNIT_TEST(TestSubscribeAndUnsubsribeByEvent) {
        TTestBasicRuntime runtime(3);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge0 = runtime.AllocateEdgeActor(0);
        const auto edge1 = runtime.AllocateEdgeActor(1);

        TAutoPtr<IEventHandle> handle;

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(1, 0), edge1, new TEvInterconnect::TEvConnectNode), 1, true);

        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(0));
        }

        runtime.Send(new IEventHandle(handle->Sender, edge0, new TEvents::TEvUnsubscribe), 1, true);

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(1, 2), edge1, new TEvents::TEvSubscribe), 1, true);

        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(2));
        }

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), edge0, new TEvInterconnect::TEvConnectNode), 0, true);

        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(1));
        }

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 2), edge0, new TEvents::TEvSubscribe), 0, true);

        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(2));
        }


        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), edge0, new TEvents::TEvUnsubscribe), 0, true);
        runtime.Send(new IEventHandle(handle->Sender, edge0, new TEvents::TEvPoisonPill));

        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeDisconnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(2));
        }
    }

    Y_UNIT_TEST(TestManyEventsWithReconnect) {
        TTestBasicRuntime runtime(2);
        runtime.SetLogPriority(NActorsServices::INTERCONNECT, NActors::NLog::PRI_DEBUG);
        SOCKET s = INVALID_SOCKET;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (auto *p = ev->CastAsLocal<TEvHandshakeDone>()) {
                s = *p->Socket;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto edge = runtime.AllocateEdgeActor(0);
        const auto wall = runtime.Register(new TWall, 1);

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), edge, new TEvInterconnect::TEvConnectNode), 0, true);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);

        runtime.Send(new IEventHandle(wall, edge, new TEvents::TEvPing), 0, true);
        runtime.GrabEdgeEvent<TEvents::TEvPong>(handle);

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev){
            if (s != INVALID_SOCKET && TEvents::TEvPing::EventType == ev->Type && 666ULL == ev->Cookie) {
                ShutDown(s, SHUT_RDWR);
                s = INVALID_SOCKET;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.Register(new TFlooder(wall, edge, 10000), 0);

        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
    }

    Y_UNIT_TEST(TestBlobEvent220Bytes) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        TString blob(220, '!');
        for (auto i = 0U; i < blob.size(); ++i)
             const_cast<TString::value_type*>(blob.data())[i] =
                 TString::value_type(i);

        TAutoPtr<IEventHandle> ev =
            new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, 69);
        runtime.Send(ev.Release(), 0);

        TAutoPtr<IEventHandle> handle;
        const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 69);
        UNIT_ASSERT_EQUAL(event->Blob, blob);
    }

    Y_UNIT_TEST(TestBlobEvent220BytesPreSerialized) {
        TTestBasicRuntime runtime(2);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        TString blob(220, '!');
        for (auto i = 0U; i < blob.size(); ++i)
             const_cast<TString::value_type*>(blob.data())[i] =
                 TString::value_type(i);

        TAutoPtr<IEventHandle> ev =
            new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, 69);
        TAutoPtr<IEventHandle> serev = GetSerialized(ev);
        runtime.Send(serev.Release(), 0);

        TAutoPtr<IEventHandle> handle;
        const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 69);
        UNIT_ASSERT_EQUAL(event->Blob, blob);
    }

    Y_UNIT_TEST(TestBlobEventDifferentSizes) {
        TTestBasicRuntime runtime(2);
        runtime.SetLogPriority(NActorsServices::INTERCONNECT, NActors::NLog::PRI_DEBUG);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        for (size_t s = 1; s <= 1024; ++s) {
            TString blob(s, '!');
            for (auto i = 0U; i < blob.size(); ++i)
                 const_cast<TString::value_type*>(blob.data())[i] =
                     '0' + (i % 10);
            TAutoPtr<IEventHandle> ev =
                new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, s);
            runtime.Send(ev.Release(), 0);
            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, s);
            UNIT_ASSERT_EQUAL(event->Blob, blob);
        }
    }

    Y_UNIT_TEST(TestBlobEventDifferentSizesPreSerialized) {
        TTestBasicRuntime runtime(2);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        for (size_t s = 1; s <= 1024; ++s) {
            TString blob(s, '!');
            for (auto i = 0U; i < blob.size(); ++i)
                 const_cast<TString::value_type*>(blob.data())[i] =
                     '0' + (i % 10);
            TAutoPtr<IEventHandle> ev =
                new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, s);
            TAutoPtr<IEventHandle> serev = GetSerialized(ev);
            runtime.Send(serev.Release(), 0);
            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, s);
            UNIT_ASSERT_EQUAL(event->Blob, blob);
        }
    }

    Y_UNIT_TEST(TestBlobEventDifferentSizesPreSerializedAndRaw) {
        TTestBasicRuntime runtime(2);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        for (size_t s = 1; s <= 1024; ++s) {
            TString blob(s, '!');
            for (auto i = 0U; i < blob.size(); ++i)
                 const_cast<TString::value_type*>(blob.data())[i] =
                     '0' + (i % 10);
            TAutoPtr<IEventHandle> ev =
                new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, s);
            TAutoPtr<IEventHandle> serev = GetSerialized(ev);
            TAutoPtr<IEventHandle> handle;

            runtime.Send(serev.Release(), 0);
            const auto aevent = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, s);
            UNIT_ASSERT_EQUAL(aevent->Blob, blob);

            runtime.Send(ev.Release(), 0);
            const auto bevent = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, s);
            UNIT_ASSERT_EQUAL(bevent->Blob, blob);
        }
    }

    Y_UNIT_TEST(TestNotifyUndeliveredOnMissedActor) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);

        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1),
                                      edge,
                                      new TEvInterconnect::TEvConnectNode),
                     0, true);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);

        runtime.Send(new IEventHandle(TActorId(runtime.GetNodeId(1), "null"),
                                      edge,
                                      new TEvents::TEvPing,
                                      IEventHandle::FlagTrackDelivery, 13),
                     0, true);

        const auto event = runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 13);
        UNIT_ASSERT_EQUAL(event->SourceType, TEvents::TEvPing::EventType);
        UNIT_ASSERT_EQUAL(event->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
    }

    Y_UNIT_TEST(TestBlobEventUpToMebibytes) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        for (size_t s = 3; s <= 23; ++s) {
            TString blob(1 << s, '!');
            for (ui64 i = 0U; i < blob.size() / sizeof(ui64); ++i)
                 reinterpret_cast<ui64*>(const_cast<TString::value_type*>(blob.data()))[i] = i;

            runtime.Send(new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, s), 0);
            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, s);
            UNIT_ASSERT_EQUAL(event->Blob, blob);
        }
    }

    Y_UNIT_TEST(TestPreSerializedBlobEventUpToMebibytes) {
        TTestBasicRuntime runtime(2);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        for (size_t s = 3; s <= 23; ++s) {
            TString blob(1 << s, '!');
            for (ui64 i = 0U; i < blob.size() / sizeof(ui64); ++i)
                 reinterpret_cast<ui64*>(
                     const_cast<TString::value_type*>(blob.data()))[i] = i;

            TAutoPtr<IEventHandle> ev =
                new IEventHandle(edge, edge, new TEvents::TEvBlob(blob), 0, s);
            TAutoPtr<IEventHandle> serev = GetSerialized(ev);
            runtime.Send(serev.Release(), 0, true);
            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, s);
            UNIT_ASSERT_EQUAL(event->Blob, blob);
        }
    }

    Y_UNIT_TEST(TestPingPongThroughSubChannel) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);
        const auto wall = runtime.Register(new TWall, 1);

        runtime.Send(new IEventHandle(wall, edge, new TEvents::TEvPing, IEventHandle::FlagUseSubChannel, 113), 0);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvPong>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 113);
        UNIT_ASSERT_EQUAL(handle->GetChannel(), 0);
        UNIT_ASSERT_EQUAL(handle->GetSubChannel(), wall.LocalId());
    }

    Y_UNIT_TEST(TestBlobEventsThroughSubChannels) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto edge = runtime.AllocateEdgeActor(1);

        TString blob(100000, '@');
        for (auto i = 0U; i < blob.size(); ++i)
             const_cast<TString::value_type*>(blob.data())[i] = TString::value_type(i % 256);

        for (auto i  = 0U; i < 100U; ++i) {

            const auto src = runtime.AllocateEdgeActor(0);

            runtime.Send(new IEventHandle(edge, src, new TEvents::TEvBlob(blob), IEventHandle::FlagUseSubChannel, i), 0);

            TAutoPtr<IEventHandle> handle;
            const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
            UNIT_ASSERT_EQUAL(handle->Cookie, i);
            UNIT_ASSERT_EQUAL(event->Blob, blob);
            UNIT_ASSERT_EQUAL(handle->GetSubChannel(), src.LocalId());
        }
    }

    Y_UNIT_TEST(TestTraceIdPassThrough) {
        TTestBasicRuntime runtime(2);
        runtime.SetLogPriority(NActorsServices::INTERCONNECT,
                               NActors::NLog::PRI_DEBUG);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        TString blob(100000, '@');
        for (auto i = 0U; i < blob.size(); ++i)
            const_cast<TString::value_type*>(blob.data())[i] =
                TString::value_type(i % 256);

        auto sentTraceId = NWilson::TTraceId::NewTraceId(15, 4095);

        runtime.Send(new IEventHandle(edge, edge,
                                      new TEvents::TEvBlob(blob),
                                      0, 13, nullptr, sentTraceId.Clone()),
                     0);

        TAutoPtr<IEventHandle> handle;
        const auto event = runtime.GrabEdgeEvent<TEvents::TEvBlob>(handle);
        UNIT_ASSERT_EQUAL(handle->Cookie, 13);
        UNIT_ASSERT_EQUAL(event->Blob, blob);
        UNIT_ASSERT_EQUAL((bool)handle->TraceId, true);
    }

    Y_UNIT_TEST(TestAddressResolve) {
        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);
        auto event = new TEvResolveAddress;
        event->Address = "localhost";
        event->Port = 80;
        runtime.Send(new IEventHandle(GetNameserviceActorId(), edge, event), 0);
        TAutoPtr<IEventHandle> handle;
        const auto reply = runtime.GrabEdgeEvent<TEvAddressInfo>(handle);
        UNIT_ASSERT_VALUES_EQUAL(NAddr::PrintHostAndPort(*reply->Address),
                          "[::1]:80");
    }

    Y_UNIT_TEST(TestEventWithPayloadSerialization) {
        struct TEvMessage : TEventPB<TEvMessage, TMessageWithPayload, 12345> {};

        TTestBasicRuntime runtime(2);
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(1);

        for (size_t s1 = 0; s1 <= 100000; s1 = s1 * 2 + 1) {
            for (size_t s2 = 0; s2 <= 100000; s2 = s2 * 2 + 1) {
                Cerr << s1 << " " << s2 << Endl;

                auto makeRope = [](size_t size) {
                    TRope res;

                    while (size) {
                        size_t chunkLen = RandomNumber(size) + 1;
                        TString s = TString::Uninitialized(chunkLen);
                        char *p = s.Detach();
                        for (size_t k = 0; k < s.size(); ++k) {
                            *p++ = RandomNumber<unsigned char>();
                        }
                        res.Insert(res.End(), TRope(std::move(s)));
                        size -= chunkLen;
                    }

                    return res;
                };

                TRope rope1 = makeRope(s1);
                UNIT_ASSERT_VALUES_EQUAL(rope1.GetSize(), s1);

                TRope rope2 = makeRope(s2);
                UNIT_ASSERT_VALUES_EQUAL(rope2.GetSize(), s2);

                TString meta = Sprintf("%zu/%zu %s %s", s1, s2, rope1.DebugString().data(), rope2.DebugString().data());

                {
                    auto msg = MakeHolder<TEvMessage>();
                    msg->Record.SetMeta(meta);
                    msg->Record.AddPayloadId(msg->AddPayload(TRope(rope1)));
                    msg->Record.AddPayloadId(msg->AddPayload(TRope(rope2)));
                    runtime.Send(new IEventHandle(edge, TActorId(), msg.Release()), 0);
                }

                TAutoPtr<IEventHandle> handle;
                const auto event = runtime.GrabEdgeEvent<TEvMessage>(handle);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetMeta(), meta);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.PayloadIdSize(), 2);
                UNIT_ASSERT_EQUAL(event->GetPayload(event->Record.GetPayloadId(0)), rope1);
                UNIT_ASSERT_EQUAL(event->GetPayload(event->Record.GetPayloadId(1)), rope2);
            }
        }
    }

    void TestConnectionWithDifferentVersions(
            std::shared_ptr<NKikimrConfig::TCurrentCompatibilityInfo> node0,
            std::shared_ptr<NKikimrConfig::TCurrentCompatibilityInfo> node1) {
        TTestBasicRuntime runtime(2);
        runtime.SetUseRealInterconnect();
        runtime.SetICCommonSetupper([&](ui32 nodeNum, TIntrusivePtr<TInterconnectProxyCommon> common) {
            common->CompatibilityInfo = TString();
            NKikimrConfig::TCurrentCompatibilityInfo* current = nullptr;
            if (nodeNum % 2 == 0) {
                current = node0.get();
            } else if (nodeNum == 1) {
                current = node1.get();
            }
            Y_ABORT_UNLESS(current);
            Y_ABORT_UNLESS(CompatibilityInfo.MakeStored(NKikimrConfig::TCompatibilityRule::Interconnect, current)
                    .SerializeToString(&*common->CompatibilityInfo));

            common->ValidateCompatibilityInfo =
                [=](const TString& peer, TString& errorReason) {
                    NKikimrConfig::TStoredCompatibilityInfo peerPB;
                    if (!peerPB.ParseFromString(peer)) {
                        errorReason = "Cannot parse given CompatibilityInfo";
                        return false;
                    }

                    return CompatibilityInfo.CheckCompatibility(current, &peerPB,
                        NKikimrConfig::TCompatibilityRule::Interconnect, errorReason);
                };
        });
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto edge = runtime.AllocateEdgeActor(0);
        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), edge, new TEvInterconnect::TEvConnectNode), 0, true);

        TAutoPtr<IEventHandle> handle;
        {
            const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
            UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(1));
        }
    }

    Y_UNIT_TEST(OldNbs) {
        std::shared_ptr<NKikimrConfig::TCurrentCompatibilityInfo> node0 =
            std::make_shared<NKikimrConfig::TCurrentCompatibilityInfo>();
        {
            node0->SetApplication("nbs");
            auto* version = node0->MutableVersion();
            version->SetYear(22);
            version->SetMajor(4);
            version->SetMinor(1);
            version->SetHotfix(0);
        }

        std::shared_ptr<NKikimrConfig::TCurrentCompatibilityInfo> node1 =
            std::make_shared<NKikimrConfig::TCurrentCompatibilityInfo>();
        {
            node1->SetApplication("ydb");
            auto* version = node1->MutableVersion();
            version->SetYear(23);
            version->SetMajor(1);
            version->SetMinor(1);
            version->SetHotfix(0);

            {
                auto* nbsRule = node1->AddCanConnectTo();
                nbsRule->SetApplication("nbs");
                nbsRule->SetComponentId((ui32)NKikimrConfig::TCompatibilityRule::Interconnect);

                auto* lowerLimit = nbsRule->MutableLowerLimit();
                lowerLimit->SetYear(22);
                lowerLimit->SetMajor(4);

                nbsRule->MutableUpperLimit()->CopyFrom(*version);

                node1->AddStoresReadableBy()->CopyFrom(*nbsRule);
            }
        }

        TestConnectionWithDifferentVersions(node0, node1);
        TestConnectionWithDifferentVersions(node1, node0);
    }

    void TestOldFormat(TString oldTag, bool suppressOnNew, bool suppressOnOld) {
        std::shared_ptr<NKikimrConfig::TCurrentCompatibilityInfo> node0 =
            std::make_shared<NKikimrConfig::TCurrentCompatibilityInfo>();
        {
            node0->SetApplication("ydb");
            auto* version = node0->MutableVersion();
            version->SetYear(23);
            version->SetMajor(1);
            version->SetMinor(1);
            version->SetHotfix(0);

            {
                auto* rule = node0->AddCanConnectTo();
                rule->SetComponentId((ui32)NKikimrConfig::TCompatibilityRule::Interconnect);

                auto* lowerLimit = rule->MutableLowerLimit();
                lowerLimit->SetYear(22);
                lowerLimit->SetMajor(5);

                rule->MutableUpperLimit()->CopyFrom(*version);

                node0->AddStoresReadableBy()->CopyFrom(*rule);
            }
        }

        TTestBasicRuntime runtime(2);
        runtime.SetUseRealInterconnect();
        runtime.SetICCommonSetupper([=](ui32 nodeNum, TIntrusivePtr<TInterconnectProxyCommon> common) {
            if (nodeNum % 2 == 0) {
                if (!suppressOnNew) {
                    common->CompatibilityInfo.emplace();

                    common->ValidateCompatibilityInfo =
                        [=](const TString& peer, TString& errorReason) {
                            NKikimrConfig::TStoredCompatibilityInfo peerPB;
                            if (!peerPB.ParseFromString(peer)) {
                                errorReason = "Cannot parse given CompatibilityInfo";
                                return false;
                            }

                            return CompatibilityInfo.CheckCompatibility(node0.get(), &peerPB,
                                NKikimrConfig::TCompatibilityRule::Interconnect, errorReason);
                        };

                    common->ValidateCompatibilityOldFormat =
                        [=](const TMaybe<TInterconnectProxyCommon::TVersionInfo>& peer, TString& errorReason) {
                            if (!peer) {
                                return true;
                            }
                            return CompatibilityInfo.CheckCompatibility(node0.get(), *peer,
                                NKikimrConfig::TCompatibilityRule::Interconnect, errorReason);
                        };

                    common->VersionInfo = TInterconnectProxyCommon::TVersionInfo{
                        .Tag = "stable-23-1",
                        .AcceptedTags = { "stable-23-1", "stable-22-5" },
                    };
                }
            } else {
                if (!suppressOnOld) {
                    common->VersionInfo = TInterconnectProxyCommon::TVersionInfo{
                        .Tag = oldTag,
                        .AcceptedTags = { oldTag }
                    };
                }
            }
        });

        runtime.Initialize(TAppPrepare().Unwrap());

        using TPair = std::pair<ui32, ui32>;
        for (auto [node1, node2] : {TPair{0, 1}, TPair{1, 0}}) {
            const auto edge = runtime.AllocateEdgeActor(node1);
            runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(node1, node2), edge, new TEvInterconnect::TEvConnectNode), node1, true);

            TAutoPtr<IEventHandle> handle;
            {
                const auto event = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(handle);
                UNIT_ASSERT_EQUAL(event->NodeId, runtime.GetNodeId(node2));
            }
        }
    }

    Y_UNIT_TEST(OldFormat) {
        TestOldFormat("stable-22-5", false, false);
    }

    Y_UNIT_TEST(OldFormatSuppressVersionCheckOnNew) {
        TestOldFormat("trunk", true, false);
    }

    Y_UNIT_TEST(OldFormatSuppressVersionCheckOnOld) {
        TestOldFormat("trunk", false, true);
    }

    Y_UNIT_TEST(OldFormatSuppressVersionCheck) {
        TestOldFormat("trunk", true, true);
    }
}

}
