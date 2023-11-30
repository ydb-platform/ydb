#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/ut/lib/test_events.h>
#include <ydb/library/actors/interconnect/ut/lib/test_actors.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

Y_UNIT_TEST_SUITE(HugeCluster) {
    using namespace NActors;

    class TPoller: public TActor<TPoller> {
        const std::vector<TActorId>& Targets;
        std::unordered_map<TActorId, TManualEvent>& Connected;

    public:
        TPoller(const std::vector<TActorId>& targets, std::unordered_map<TActorId, TManualEvent>& events)
            : TActor(&TPoller::StateFunc)
            , Targets(targets)
            , Connected(events)
        {}

        void Handle(TEvTestStartPolling::TPtr /*ev*/, const TActorContext& ctx) {
            for (ui32 i = 0; i < Targets.size(); ++i) {
                ctx.Send(Targets[i], new TEvTest(), IEventHandle::FlagTrackDelivery, i);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev, const TActorContext& ctx) {
            const ui32 cookie = ev->Cookie;
            // Cerr << "TEvUndelivered ping from node# " << SelfId().NodeId() << " to node# " << cookie + 1 << Endl;
            ctx.Send(Targets[cookie], new TEvTest(), IEventHandle::FlagTrackDelivery, cookie);
        }

        void Handle(TEvTest::TPtr ev, const TActorContext& /*ctx*/) {
            // Cerr << "Polled from " << ev->Sender.ToString() << Endl;
            Connected[ev->Sender].Signal();
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

    TIntrusivePtr<NLog::TSettings> MakeLogConfigs(NLog::EPriority priority) {
        // custom logger settings
        auto loggerSettings = MakeIntrusive<NLog::TSettings>(
                TActorId(0, "logger"),
                NActorsServices::LOGGER,
                priority,
                priority,
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

        return loggerSettings;
    }

    Y_UNIT_TEST(AllToAll) {
        ui32 nodesNum = 120;
        std::vector<TActorId> pollers(nodesNum);
        std::vector<std::unordered_map<TActorId, TManualEvent>> events(nodesNum);

        // Must destroy actor system before shared arrays
        {
            TTestICCluster testCluster(nodesNum, NActors::TChannelsConfig(), nullptr, MakeLogConfigs(NLog::PRI_EMERG));

            for (ui32 i = 0; i < nodesNum; ++i) {
                pollers[i] = testCluster.RegisterActor(new TPoller(pollers, events[i]), i + 1);
            }

            for (ui32 i = 0; i < nodesNum; ++i) {
                for (const auto& actor : pollers) {
                    events[i][actor] = TManualEvent();
                }
            }

            testCluster.RegisterActor(new TStartPollers(pollers), 1);

            for (ui32 i = 0; i < nodesNum; ++i) {
                for (auto& [_, ev] : events[i]) {
                    ev.WaitI();
                }
            }
        }
    }


    Y_UNIT_TEST(AllToOne) {
        ui32 nodesNum = 500;
        std::vector<TActorId> listeners;
        std::vector<TActorId> pollers(nodesNum - 1);
        std::unordered_map<TActorId, TManualEvent> events;
        std::unordered_map<TActorId, TManualEvent> emptyEventList;

        // Must destroy actor system before shared arrays
        {
            TTestICCluster testCluster(nodesNum, NActors::TChannelsConfig(), nullptr, MakeLogConfigs(NLog::PRI_EMERG));

            const TActorId listener = testCluster.RegisterActor(new TPoller({}, events), nodesNum);
            listeners = { listener };
            for (ui32 i = 0; i < nodesNum - 1; ++i) {
                pollers[i] = testCluster.RegisterActor(new TPoller(listeners, emptyEventList), i + 1);
            }

            for (const auto& actor : pollers) {
                events[actor] = TManualEvent();
            }

            testCluster.RegisterActor(new TStartPollers(pollers), 1);

            for (auto& [_, ev] : events) {
                ev.WaitI();
            }
        }
    }
}
