#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/base/resource_profile.h>
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
    TAutoPtr<TMemoryToken> HeldMemory;
};

class TTxHoldStaticMemory : public ITransaction {
public:
    TTxHoldStaticMemory(TIntrusivePtr<TMemoryProbeState> state, const TActorId& replyTo)
        : State(std::move(state))
        , ReplyTo(replyTo)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (txc.GetMemoryLimit() < 50_MB) {
            txc.RequestMemory(50_MB - txc.GetMemoryLimit());
            return false;
        }
        UNIT_ASSERT_VALUES_EQUAL(txc.GetTaskId(), 0); // Static allocation, outside ResourceBroker.
        State->HeldMemory = txc.HoldMemory(50_MB);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ReplyTo, new TEvents::TEvWakeup);
    }

private:
    const TIntrusivePtr<TMemoryProbeState> State;
    const TActorId ReplyTo;
};

class TTxReleaseStaticMemory : public ITransaction {
public:
    TTxReleaseStaticMemory(TAutoPtr<TMemoryToken> token, const TActorId& replyTo)
        : Token(std::move(token))
        , ReplyTo(replyTo)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        txc.UseMemoryToken(std::move(Token));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ReplyTo, new TEvents::TEvWakeup);
    }

private:
    TAutoPtr<TMemoryToken> Token;
    const TActorId ReplyTo;
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
        } else if (auto* ev = eh->CastAsLocal<NFake::TEvCall>()) {
            ev->Callback(Executor(), this->ActorContext());
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

    void OnMemoryLimitCleared() override {
        State->LastShare.reset();
    }

private:
    const TActorId Owner;
    const TIntrusivePtr<TMemoryProbeState> State;
    bool Booted = false;
    bool Stopping = false;
};

// Boots a flat tablet whose memory report the test controls and records what it sends to its launcher
struct TMemoryProbeEnv : public TMyEnvBase {
    TMemoryProbeEnv(bool hostEnabled, NMemory::TConsumerReport report, bool allowStaticMemory = false)
    {
        Probe->Report = report;
        Env.GetAppData().FeatureFlags.SetEnableTabletMemoryHost(hostEnabled);
        if (allowStaticMemory) {
            auto& profiles = Env.GetAppData().ResourceProfiles;
            profiles = new TResourceProfiles;
            TResourceProfiles::TResourceProfile profile;
            profile.SetTabletType(NKikimrTabletBase::TTabletTypes::Unknown);
            profile.SetName("default");
            profile.SetStaticTabletTxMemoryLimit(0);
            profile.SetStaticTxMemoryLimit(100_MB);
            profile.SetTxMemoryLimit(100_MB);
            profiles->AddProfile(profile);
        }

        Observer = Env.AddObserver<TEvLocal::TEvTabletMetrics>([this](TEvLocal::TEvTabletMetrics::TPtr& ev) {
            Reported.emplace_back(ev->Sender, ev->Get()->ResourceValues);
            if (ev->Get()->TabletMemoryReport) {
                ControllerReports.push_back({ev->Sender, ev->Get()->Executor, *ev->Get()->TabletMemoryReport, ev->Get()->FollowerId, ev->Get()->SystemTablet});
            }
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

        TabletActor = GrabEdgeEvent<NFake::TEvReady>()->Get()->ActorId;
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

    // A memory report always carries all three fields from the same snapshot.
    const NKikimrTabletBase::TMetrics* LastWholeReport() const
    {
        for (auto it = Reported.rbegin(); it != Reported.rend(); ++it) {
            const auto& metrics = it->second;
            if (metrics.HasTabletMemoryUsed() && metrics.HasMemoryDemand() && metrics.HasMemoryReclaimable()) {
                return &metrics;
            }
        }

        return nullptr;
    }

    TIntrusivePtr<TMemoryProbeState> Probe = MakeIntrusive<TMemoryProbeState>();
    TActorId TabletActor;
    TVector<std::pair<TActorId, NKikimrTabletBase::TMetrics>> Reported;
    struct TControllerReport {
        TActorId Sender;
        TActorId Executor;
        NMemory::TConsumerReport Report;
        ui32 FollowerId;
        TActorId SystemTablet;
    };
    TVector<TControllerReport> ControllerReports;
    TTestActorRuntime::TEventObserverHolder Observer;
};

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorTabletMemory) {

Y_UNIT_TEST(ReportReachesTabletMetrics)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});

    const auto *metrics = env.LastWholeReport();
    UNIT_ASSERT_C(metrics, "no memory report in the metrics the tablet sent");

    UNIT_ASSERT_VALUES_EQUAL(metrics->GetTabletMemoryUsed(), 40_MB);
    UNIT_ASSERT_LT(metrics->GetMemory(), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(metrics->GetMemoryDemand(), 60_MB);
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

Y_UNIT_TEST(ReportClearsReclaimableMemory)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});
    env.Probe->Report.Reclaimable = 0;
    env.Reported.clear();
    env.Env.SimulateSleep(TDuration::Seconds(30));

    const auto* metrics = env.LastWholeReport();
    UNIT_ASSERT_C(metrics, "no report clearing reclaimable memory");
    UNIT_ASSERT_VALUES_EQUAL(metrics->GetMemoryReclaimable(), 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics->GetMemoryDemand(), 60_MB);
}

Y_UNIT_TEST(HiveMemoryIncludesStaticTransactionMemory)
{
    for (bool hostEnabled : {false, true}) {
        TMemoryProbeEnv env(hostEnabled, {.Used = 40_MB, .Demand = 60_MB}, true);
        const auto* initial = env.LastWithMemory();
        UNIT_ASSERT(initial);
        const ui64 initialMemory = initial->GetMemory();

        env.SendEv(env.TabletActor, new NFake::TEvCall([&](auto* executor, const auto& ctx) {
            executor->Execute(new TTxHoldStaticMemory(env.Probe, env.Edge), ctx);
        }));
        env.WaitForWakeUp();
        UNIT_ASSERT(env.Probe->HeldMemory);
        env.Env.SimulateSleep(TDuration::Seconds(30));

        const auto* held = env.LastWithMemory();
        UNIT_ASSERT(held);
        UNIT_ASSERT_VALUES_EQUAL(held->GetMemory(), initialMemory + 50_MB);
        if (hostEnabled) {
            UNIT_ASSERT_VALUES_EQUAL(env.ControllerReports.back().Report.Demand, 60_MB);
            UNIT_ASSERT_VALUES_EQUAL(env.ControllerReports.back().Report.Reclaimable, 0);
        } else {
            UNIT_ASSERT(!held->HasMemoryDemand());
            UNIT_ASSERT(!held->HasMemoryReclaimable());
        }

        env.SendEv(env.TabletActor, new NFake::TEvCall([&](auto* executor, const auto& ctx) {
            executor->Execute(new TTxReleaseStaticMemory(std::move(env.Probe->HeldMemory), env.Edge), ctx);
        }));
        env.WaitForWakeUp();
        UNIT_ASSERT(!env.Probe->HeldMemory);
        env.Reported.clear();
        env.Env.SimulateSleep(TDuration::Seconds(30));
        const auto* released = env.LastWithMemory();
        UNIT_ASSERT(released);
        UNIT_ASSERT_VALUES_EQUAL(released->GetMemory(), initialMemory);
        if (hostEnabled) {
            UNIT_ASSERT_VALUES_EQUAL(env.ControllerReports.back().Report.Demand, 60_MB);
        }
    }
}

Y_UNIT_TEST(ControllerReportExcludesExecutorMemory)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB}, true);
    UNIT_ASSERT(!env.ControllerReports.empty());
    const auto& initial = env.ControllerReports.back();
    UNIT_ASSERT_VALUES_EQUAL(initial.Report.Used, 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(initial.Report.Demand, 60_MB);
    UNIT_ASSERT_VALUES_EQUAL(initial.Report.Reclaimable, 10_MB);
    UNIT_ASSERT(env.LastWithMemory()->GetMemory() < initial.Report.Used);

    env.SendEv(env.TabletActor, new NFake::TEvCall([&](auto* executor, const auto& ctx) {
        executor->Execute(new TTxHoldStaticMemory(env.Probe, env.Edge), ctx);
    }));
    env.WaitForWakeUp();
    env.Env.SimulateSleep(TDuration::Seconds(30));
    UNIT_ASSERT(env.LastWithMemory()->GetMemory() >= 50_MB);
    // Static executor reservations change the Hive metric, but not the owner attribution.
    UNIT_ASSERT_VALUES_EQUAL(env.ControllerReports.back().Report.Used, 40_MB);
}

Y_UNIT_TEST(TabletSenderKeepsStableExecutorIdentity)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB});
    const TActorId executorId = env.ExecutorActor();
    env.ControllerReports.clear();
    env.Env.SimulateSleep(TDuration::Seconds(1));
    env.SendEv(env.TabletActor, new NFake::TEvCall([&](auto* executor, const auto& ctx) {
        auto* metrics = executor->GetResourceMetrics();
        metrics->TabletMemoryReport = NMemory::TConsumerReport{.Used = 41_MB, .Demand = 61_MB};
        // The total Hive report need not change for a change of MC attribution to be sent.
        UNIT_ASSERT(metrics->TryUpdate(ctx));
        ctx.Send(env.Edge, new TEvents::TEvWakeup);
    }));
    env.WaitForWakeUp();
    env.Env.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(env.ControllerReports.size(), 1u);
    const auto& report = env.ControllerReports.back();
    UNIT_ASSERT_VALUES_EQUAL(report.Sender, env.TabletActor);
    UNIT_ASSERT_VALUES_EQUAL(report.Executor, executorId);
    UNIT_ASSERT_VALUES_EQUAL(report.Report.Used, 41_MB);
    UNIT_ASSERT_VALUES_EQUAL(report.Report.Demand, 61_MB);

    env.Env.SimulateSleep(TDuration::Seconds(1));
    env.SendEv(env.TabletActor, new NFake::TEvCall([&](auto* executor, const auto& ctx) {
        executor->GetResourceMetrics()->TabletMemoryReport = NMemory::TConsumerReport{};
        UNIT_ASSERT(executor->GetResourceMetrics()->TryUpdate(ctx));
        ctx.Send(env.Edge, new TEvents::TEvWakeup);
    }));
    env.WaitForWakeUp();
    env.Env.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(env.ControllerReports.back().Report.Used, 0u);
    UNIT_ASSERT_VALUES_EQUAL(env.ControllerReports.back().Executor, executorId);
}

Y_UNIT_TEST(OwnerReportTryUpdateHonorsSignificantChanges)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB});
    const auto marker = env.Env.AllocateEdgeActor();
    const auto systemTablet = env.ControllerReports.back().SystemTablet;
    UNIT_ASSERT(systemTablet);
    NMetrics::TResourceMetrics metrics(env.Tablet, 0, env.Edge, marker, systemTablet);
    ui64 cpu = 0;
    TVector<NMemory::TConsumerReport> reports;
    const auto observer = env.Env.AddObserver<TEvLocal::TEvTabletMetrics>([&](auto& event) {
        if (event->Get()->Executor == marker) {
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->SystemTablet, systemTablet);
            reports.push_back(*event->Get()->TabletMemoryReport);
            event.Reset();
        }
    });
    const auto update = [&](ui64 used, ui64 demand, ui64 reclaimable, bool expected) {
        env.Env.SimulateSleep(TDuration::Seconds(2));
        env.SendEv(env.TabletActor, new NFake::TEvCall([&](auto*, const auto& ctx) {
            metrics.CPU.Set(cpu, ctx.Now());
            metrics.Memory.Set(0, ctx.Now());
            metrics.Network.Set(0, ctx.Now());
            metrics.StorageSystem.Set(0, ctx.Now());
            metrics.SetMemoryReport(used, demand, reclaimable);
            UNIT_ASSERT_VALUES_EQUAL(metrics.TryUpdate(ctx), expected);
            ctx.Send(env.Edge, new TEvents::TEvWakeup);
        }));
        env.WaitForWakeUp();
    };
    update(10_MB + 100_KB, 12_MB + 100_KB, 1_MB + 100_KB, true);
    update(10_MB + 100_KB + 1, 12_MB + 100_KB + 1, 1_MB + 100_KB + 1, false);
    // A legacy metric send carries the complete current snapshot even below its own threshold.
    cpu = 200000;
    update(10_MB + 100_KB + 2, 12_MB + 100_KB + 2, 1_MB + 100_KB + 2, true);
    UNIT_ASSERT_VALUES_EQUAL(reports.back().Used, 10_MB + 100_KB + 2);
    update(100_KB, 200_KB, 1, true);
    update(100_KB, 200_KB, 2, false);
    update(100_KB, 200_KB, 0, true);
    update(100_KB, 200_KB, 1, true);
    update(0, 0, 0, true);
    UNIT_ASSERT_VALUES_EQUAL(reports.size(), 6u);
}

Y_UNIT_TEST(HostFlagDoesNotChangeHivePlacementMemory)
{
    TMemoryProbeEnv enabled(true, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});
    TMemoryProbeEnv disabled(false, {.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});
    UNIT_ASSERT_VALUES_EQUAL(enabled.LastWithMemory()->GetMemory(), disabled.LastWithMemory()->GetMemory());
}

Y_UNIT_TEST(FollowerPublishesItsOwnMemoryReport)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB});
    env.FireDummyFollower(1);
    env.Env.SimulateSleep(TDuration::Seconds(30));
    bool reported = false;
    for (const auto& report : env.ControllerReports) {
        if (report.FollowerId == 1) {
            UNIT_ASSERT(report.Executor);
            UNIT_ASSERT_VALUES_EQUAL(report.Report.Used, 50_KB);
            reported = true;
        }
    }
    UNIT_ASSERT(reported);
}

Y_UNIT_TEST(ElasticShareCanBeWithdrawn)
{
    TMemoryProbeEnv env(true, {.Used = 40_MB, .Demand = 60_MB});
    env.SendEv(env.ExecutorActor(), new NMemory::TEvMemoryZone(NMemory::EMemoryZone::Green, 10_MB));
    env.Env.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT(env.Probe->LastShare);
    env.SendEv(env.ExecutorActor(), new NMemory::TEvMemoryZone(NMemory::EMemoryZone::Green, std::nullopt, true));
    env.Env.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT(!env.Probe->LastShare);
}

Y_UNIT_TEST(ZoneAndShareAreIgnoredWithoutTheFlag)
{
    TMemoryProbeEnv env(false, {.Used = 40_MB, .Demand = 60_MB});
    env.SendEv(env.ExecutorActor(), new NMemory::TEvMemoryZone(NMemory::EMemoryZone::Red, 0));
    env.Env.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT(!env.Probe->LastZone);
    UNIT_ASSERT(!env.Probe->LastShare);
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
