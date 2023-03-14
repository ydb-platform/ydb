#include <library/cpp/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <library/cpp/actors/interconnect/ut/lib/test_events.h>
#include <library/cpp/actors/interconnect/ut/lib/test_actors.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

Y_UNIT_TEST_SUITE(HugeCluster) {
    using namespace NActors;

    class TPoller: public TActor<TPoller> {
        const std::vector<TActorId>& Pollers;
        std::vector<TManualEvent>& Connected;

    public:
        TPoller(const std::vector<TActorId>& pollers, std::vector<TManualEvent>& events)
            : TActor(&TPoller::StateFunc)
            , Pollers(pollers)
            , Connected(events)
        {}

        void Handle(TEvTestStartPolling::TPtr /*ev*/, const TActorContext& ctx) {
            for (ui32 i = 0; i < Pollers.size(); ++i) {
                ctx.Send(Pollers[i], new TEvTest(), IEventHandle::FlagTrackDelivery, i);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev, const TActorContext& ctx) {
            const ui32 cookie = ev->Cookie;
            // Cerr << "TEvUndelivered ping from node# " << SelfId().NodeId() << " to node# " << cookie + 1 << Endl;
            ctx.Send(Pollers[cookie], new TEvTest(), IEventHandle::FlagTrackDelivery, cookie);
        }

        void Handle(TEvTest::TPtr ev, const TActorContext& /*ctx*/) {
            Connected[ev->Cookie].Signal();
        }

        void Handle(TEvents::TEvPoisonPill::TPtr& /*ev*/, const TActorContext& ctx) {
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvUndelivered, Handle)
            HFunc(TEvTestStartPolling, Handle)
            HFunc(TEvTest, Handle)
            HFunc(TEvents::TEvPoisonPill, Handle)
        )
    };

    class TStartPollers : public TActorBootstrapped<TStartPollers> {
        const std::vector<TActorId>& Pollers;

    public:
        TStartPollers(const std::vector<TActorId>& pollers)
            : Pollers(pollers)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateFunc);
            for (ui32 i = 0; i < Pollers.size(); ++i) {
                ctx.Send(Pollers[i], new TEvTestStartPolling(), IEventHandle::FlagTrackDelivery, i);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev, const TActorContext& ctx) {
            const ui32 cookie = ev->Cookie;
            // Cerr << "TEvUndelivered start poller message to node# " << cookie + 1 << Endl;
            ctx.Send(Pollers[cookie], new TEvTestStartPolling(), IEventHandle::FlagTrackDelivery, cookie);
        }

        void Handle(TEvents::TEvPoisonPill::TPtr& /*ev*/, const TActorContext& ctx) {
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvUndelivered, Handle)
            HFunc(TEvents::TEvPoisonPill, Handle)
        )
    };

    Y_UNIT_TEST(AllToAll) {
        ui32 nodesNum = 1000;

        // custom logger settings
        auto loggerSettings = MakeIntrusive<NLog::TSettings>(
                TActorId(0, "logger"),
                (NLog::EComponent)410,
                NLog::PRI_EMERG,
                NLog::PRI_EMERG,
                0U);

        loggerSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );

        constexpr ui32 WilsonComponentId = 430; // NKikimrServices::WILSON
        static const TString WilsonComponentName = "WILSON";

        loggerSettings->Append(
            (NLog::EComponent)WilsonComponentId,
            (NLog::EComponent)WilsonComponentId + 1,
            [](NLog::EComponent) -> const TString & { return WilsonComponentName; });

        TTestICCluster testCluster(nodesNum, NActors::TChannelsConfig(), nullptr, loggerSettings);

        std::vector<TActorId> pollers(nodesNum);
        std::vector<std::vector<TManualEvent>> events(nodesNum, std::vector<TManualEvent>(nodesNum));

        for (ui32 i = 0; i < nodesNum; ++i) {
            pollers[i] = testCluster.RegisterActor(new TPoller(pollers, events[i]), i + 1);
        }

        const TActorId startPollers = testCluster.RegisterActor(new TStartPollers(pollers), 1);

        for (ui32 i = 0; i < nodesNum; ++i) {
            for (ui32 j = 0; j < nodesNum; ++j) {
                events[i][j].WaitI();
            }
        }

        // kill actors to avoid use-after-free
        for (ui32 i = 0; i < nodesNum; ++i) {
            testCluster.KillActor(i + 1, pollers[i]);
        }
        testCluster.KillActor(1, startPollers);
    }

}
