#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/mind/local.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace {

// What the tablet answers when the executor asks for its memory, and what the executor delivered back
struct TMemoryProbeState : public TThrRefBase {
    NMemory::TConsumerReport Report;
    std::optional<NMemory::EMemoryZone> LastZone;
    std::optional<ui64> LastShare;
};

class TMemoryProbeTablet : public TActor<TMemoryProbeTablet>, public TTabletExecutedFlat {
public:
    using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;

    TMemoryProbeTablet(const TActorId &tablet, TTabletStorageInfo *info, const TActorId &owner,
                       TIntrusivePtr<TMemoryProbeState> state)
        : TActor(&TMemoryProbeTablet::Inbox, NKikimrServices::TActivity::FAKE_ENV_A)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Owner(owner)
        , State(std::move(state))
    {
    }

private:
    void Inbox(TEventHandlePtr &eh)
    {
        if (eh->CastAsLocal<TEvents::TEvPoison>()) {
            if (!std::exchange(Stopping, true)) {
                auto ctx(this->ActorContext());
                Executor()->DetachTablet();
                Detach(ctx);
            }
        } else if (!Booted) {
            TTabletExecutedFlat::StateInitImpl(eh, SelfId());
        } else if (!TTabletExecutedFlat::HandleDefaultEvents(eh, SelfId())) {
            Y_TABLET_ERROR("Unexpected event " << eh->GetTypeName());
        }
    }

    void DefaultSignalTabletActive(const TActorContext&) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext&) override
    {
        Booted = true;
        SignalTabletActive(SelfId());
        Send(Owner, new NFake::TEvReady(TabletID(), SelfId()));
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext &ctx) override
    {
        OnDetach(ctx);
    }

    void OnDetach(const TActorContext&) override
    {
        PassAway();
    }

    NMemory::TConsumerReport GetMemoryReport() const override
    {
        return State->Report;
    }

    void OnMemoryZone(NMemory::EMemoryZone zone) override
    {
        State->LastZone = zone;
    }

    void OnMemoryLimit(ui64 shareBytes) override
    {
        State->LastShare = shareBytes;
    }

private:
    const TActorId Owner;
    const TIntrusivePtr<TMemoryProbeState> State;
    bool Booted = false;
    bool Stopping = false;
};

// Boots a flat tablet whose memory report the test controls and records what it sends to its launcher
struct TMemoryProbeEnv : public TMyEnvBase {
    TMemoryProbeEnv(bool hostEnabled, NMemory::TConsumerReport report)
    {
        Probe->Report = report;
        Env.GetAppData().FeatureFlags.SetEnableTabletMemoryHost(hostEnabled);

        Observer = Env.AddObserver<TEvLocal::TEvTabletMetrics>([this](TEvLocal::TEvTabletMetrics::TPtr& ev) {
            Reported.emplace_back(ev->Sender, ev->Get()->ResourceValues);
        });

        FireDummyTablet();

        // The executor refreshes its counters and metrics on a 15 second tick
        Env.SimulateSleep(TDuration::Seconds(30));
    }

    void FireDummyTablet(ui32 flags = 0) override
    {
        Y_UNUSED(flags);

        FireTablet(Edge, Tablet, [this](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TMemoryProbeTablet(tablet, info, Edge, Probe);
        });

        WaitFor<NFake::TEvReady>();
    }

    // The sender of the metrics is the executor, which is where a Local sends the zone back to
    TActorId ExecutorActor() const
    {
        UNIT_ASSERT_C(!Reported.empty(), "the tablet sent no metrics to its launcher");

        return Reported.back().first;
    }

    const NKikimrTabletBase::TMetrics* LastWithMemory() const
    {
        for (auto it = Reported.rbegin(); it != Reported.rend(); ++it) {
            if (it->second.HasMemory()) {
                return &it->second;
            }
        }

        return nullptr;
    }

    // The three fields travel together, but a later tick may resend only the one whose level moved
    const NKikimrTabletBase::TMetrics* LastWholeReport() const
    {
        for (auto it = Reported.rbegin(); it != Reported.rend(); ++it) {
            const auto& metrics = it->second;
            if (metrics.HasMemory() && metrics.HasMemoryDemand() && metrics.HasMemoryReclaimable()) {
                return &metrics;
            }
        }

        return nullptr;
    }

    TIntrusivePtr<TMemoryProbeState> Probe = MakeIntrusive<TMemoryProbeState>();
    TVector<std::pair<TActorId, NKikimrTabletBase::TMetrics>> Reported;
    TTestActorRuntime::TEventObserverHolder Observer;
};

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorTabletMemory) {

Y_UNIT_TEST(ReportReachesTabletMetrics)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});

    const auto *metrics = env.LastWholeReport();
    UNIT_ASSERT_C(metrics, "no memory report in the metrics the tablet sent");

    // The honest report, not the 50 KB estimate the executor used to add
    UNIT_ASSERT_GE(metrics->GetMemory(), 40_MB);
    UNIT_ASSERT_LT(metrics->GetMemory(), 45_MB);
    UNIT_ASSERT_VALUES_EQUAL(metrics->GetMemoryDemand(), metrics->GetMemory() + 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(metrics->GetMemoryReclaimable(), 10_MB);
}

Y_UNIT_TEST(ReportIsOffWithoutTheFlag)
{
    TMemoryProbeEnv env(false, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});

    const auto *metrics = env.LastWithMemory();
    UNIT_ASSERT_C(metrics, "no Memory in the metrics the tablet sent");

    // The old estimate, with nothing of the tablet's 40 MB in it
    UNIT_ASSERT_LT(metrics->GetMemory(), 40_MB);

    for (const auto& reported : env.Reported) {
        UNIT_ASSERT(!reported.second.HasMemoryDemand());
        UNIT_ASSERT(!reported.second.HasMemoryReclaimable());
    }
}

Y_UNIT_TEST(ZoneAndShareReachTheTablet)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});

    const TActorId executor = env.ExecutorActor();
    UNIT_ASSERT(!env.Probe->LastZone);
    UNIT_ASSERT(!env.Probe->LastShare);

    env.SendEv(executor, new NMemory::TEvMemoryZone(NMemory::EMemoryZone::Yellow));
    env.Env.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT(env.Probe->LastZone);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(*env.Probe->LastZone),
        static_cast<ui32>(NMemory::EMemoryZone::Yellow));
    UNIT_ASSERT(!env.Probe->LastShare);

    env.SendEv(executor, new NMemory::TEvMemoryZone(NMemory::EMemoryZone::Red, 4_MB));
    env.Env.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(*env.Probe->LastZone),
        static_cast<ui32>(NMemory::EMemoryZone::Red));
    UNIT_ASSERT(env.Probe->LastShare);
    UNIT_ASSERT_VALUES_EQUAL(*env.Probe->LastShare, 4_MB);
}

}

}
}
