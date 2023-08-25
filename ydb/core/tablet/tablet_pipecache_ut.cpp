#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <library/cpp/actors/core/hfunc.h>

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
        struct TEvHelloResponse : public TEventLocal<TEvHelloResponse, EvHelloResponse> {};
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
                HFunc(TEvCustomTablet::TEvHelloRequest, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

        void Handle(TEvCustomTablet::TEvHelloRequest::TPtr& ev, const TActorContext& ctx) {
            ctx.Send(ev->Sender, new TEvCustomTablet::TEvHelloResponse);
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
        auto observerFunc = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
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
}

} // namespace NKikimr
