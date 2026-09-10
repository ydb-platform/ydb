#include <ydb/library/actors/subsystems/cgroup/cgroup_events.h>
#include <ydb/library/actors/subsystems/cgroup/cgroup_oom.h>
#include <ydb/library/actors/subsystems/cgroup/cgroup_oom_trend.h>
#include <ydb/library/actors/subsystems/cgroup/cgroup_v1.h>
#include <ydb/library/actors/subsystems/cgroup/cgroup_v2.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/event.h>

#include <atomic>
#include <functional>

using namespace NActors;

namespace {

    constexpr ui32 SystemPoolId = 0;
    constexpr ui32 IoPoolId = 1;
    constexpr ui64 RequestCookie = 42;

    template<class TResult>
    class TResultWaiter {
    public:
        void Set(TResult result) {
            Result = std::move(result);
            Ready.Signal();
        }

        bool Wait(TDuration timeout) {
            return Ready.WaitT(timeout);
        }

        const TResult& GetValue() const {
            return Result;
        }

    private:
        TManualEvent Ready;
        TResult Result;
    };

    void WriteFile(const TFsPath& root, const TString& path, const TString& content) {
        const TFsPath fullPath = root / path;
        fullPath.Parent().MkDirs();
        TFileOutput(fullPath.GetPath()).Write(content);
    }

    TCGroupV2StatsConfig MakeV2Config(
            const TFsPath& root,
            TDuration refreshPeriod = TDuration::Zero()) {
        return {
            .ExecutorPoolId = IoPoolId,
            .RefreshPeriod = refreshPeriod,
            .CGroupRoot = (root / "sys/fs/cgroup").GetPath(),
            .ProcSelfCGroup = (root / "proc/self/cgroup").GetPath(),
        };
    }

    TCGroupV1StatsConfig MakeV1Config(
            const TFsPath& root,
            TDuration refreshPeriod = TDuration::Zero()) {
        return {
            .ExecutorPoolId = IoPoolId,
            .RefreshPeriod = refreshPeriod,
            .CpuCGroupRoot = (root / "sys/fs/cgroup/cpu,cpuacct").GetPath(),
            .CpuAcctCGroupRoot = (root / "sys/fs/cgroup/cpu,cpuacct").GetPath(),
            .MemoryCGroupRoot = (root / "sys/fs/cgroup/memory").GetPath(),
            .BlkIoCGroupRoot = (root / "sys/fs/cgroup/blkio").GetPath(),
            .PidsCGroupRoot = (root / "sys/fs/cgroup/pids").GetPath(),
            .ProcSelfCGroup = (root / "proc/self/cgroup").GetPath(),
        };
    }

    THolder<TActorSystem> MakeActorSystem(
            std::unique_ptr<TCGroupV2StatsSubSystem> v2 = {},
            std::unique_ptr<TCGroupV1StatsSubSystem> v1 = {},
            std::unique_ptr<TCGroupOomSubSystem> oom = {}) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 2;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[SystemPoolId] =
            new TBasicExecutorPool(SystemPoolId, 1, 10, "system");
        setup->Executors[IoPoolId] = new TIOExecutorPool(IoPoolId, 1, "io");
        setup->Scheduler = new TBasicSchedulerThread;
        if (v2) {
            setup->RegisterSubSystem(std::move(v2));
        }
        if (v1) {
            setup->RegisterSubSystem(std::move(v1));
        }
        if (oom) {
            setup->RegisterSubSystem(std::move(oom));
        }
        return MakeHolder<TActorSystem>(setup);
    }

    template<class TEvent, class TResult>
    class TRequestActor : public TActorBootstrapped<TRequestActor<TEvent, TResult>> {
    public:
        using TThis = TRequestActor<TEvent, TResult>;
        using TRequest = std::function<void(const TActorId&, ui64)>;

        TRequestActor(TRequest request, TResultWaiter<TResult>& result)
            : Request(std::move(request))
            , Result(result)
        {
        }

        void Bootstrap() {
            this->Become(&TThis::StateWork);
            Request(this->SelfId(), RequestCookie);
        }

        using TResultEvent = TEvent;

        STRICT_STFUNC(StateWork,
            hFunc(TResultEvent, Handle)
        )

    private:
        void Handle(typename TResultEvent::TPtr& ev) {
            Y_ABORT_UNLESS(ev->Cookie == RequestCookie);
            Result.Set(std::move(ev->Get()->Stats));
            this->PassAway();
        }

    private:
        const TRequest Request;
        TResultWaiter<TResult>& Result;
    };

    template<class TEvent, class TResult>
    TResult RequestStats(
            TActorSystem& actorSystem,
            typename TRequestActor<TEvent, TResult>::TRequest request) {
        TResultWaiter<TResult> result;
        actorSystem.Register(
            new TRequestActor<TEvent, TResult>(std::move(request), result),
            TMailboxType::Simple,
            SystemPoolId);
        UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
        return result.GetValue();
    }

    TCGroupV2StatsPtr RequestV2Stats(
            TActorSystem& actorSystem,
            const TCGroupV2StatsSubSystem& subsystem) {
        return RequestStats<TEvCGroupV2Stats, TCGroupV2StatsPtr>(
            actorSystem,
            [&subsystem](const TActorId& recipient, ui64 cookie) {
                subsystem.ReadStats(recipient, cookie);
            });
    }

    TCGroupV1StatsPtr RequestV1Stats(
            TActorSystem* actorSystem,
            const TCGroupV1StatsSubSystem& subsystem) {
        return RequestStats<TEvCGroupV1Stats, TCGroupV1StatsPtr>(
            *actorSystem,
            [&subsystem](const TActorId& recipient, ui64 cookie) {
                subsystem.ReadStats(recipient, cookie);
            });
    }

    TCGroupMemoryStatsPtr RequestMemoryStats(
            TActorSystem& actorSystem,
            const ICGroupMemoryStatsProvider& provider) {
        return RequestStats<TEvCGroupMemoryStats, TCGroupMemoryStatsPtr>(
            actorSystem,
            [&provider](const TActorId& recipient, ui64 cookie) {
                provider.ReadMemoryStats(recipient, cookie);
            });
    }

    class TTrendRequestActor : public TActorBootstrapped<TTrendRequestActor> {
    public:
        TTrendRequestActor(
                const TCGroupOomSubSystem& subsystem,
                ECGroupOomTrendWindow window,
                TResultWaiter<std::optional<TCGroupOomTrend>>& result)
            : Subsystem(subsystem)
            , Window(window)
            , Result(result)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateWork);
            Subsystem.ReadTrend(SelfId(), Window, RequestCookie);
        }

        STRICT_STFUNC(StateWork,
            hFunc(TEvCGroupOomTrend, Handle)
        )

    private:
        void Handle(TEvCGroupOomTrend::TPtr& ev) {
            Y_ABORT_UNLESS(ev->Cookie == RequestCookie);
            Y_ABORT_UNLESS(
                ev->Get()->State ==
                    (ev->Get()->Trend
                        ? ECGroupOomTrendState::Active
                        : ECGroupOomTrendState::Stopped));
            Y_ABORT_UNLESS(!ev->Get()->TimeToOomThreshold);
            Result.Set(std::move(ev->Get()->Trend));
            PassAway();
        }

    private:
        const TCGroupOomSubSystem& Subsystem;
        const ECGroupOomTrendWindow Window;
        TResultWaiter<std::optional<TCGroupOomTrend>>& Result;
    };

    void RequestOomTrend(
            TActorSystem& actorSystem,
            const TCGroupOomSubSystem& subsystem,
            ECGroupOomTrendWindow window,
            TResultWaiter<std::optional<TCGroupOomTrend>>& result) {
        actorSystem.Register(
            new TTrendRequestActor(subsystem, window, result),
            TMailboxType::Simple,
            SystemPoolId);
    }

    class TOomAlertSubscriberActor : public TActorBootstrapped<TOomAlertSubscriberActor> {
    public:
        explicit TOomAlertSubscriberActor(TResultWaiter<TCGroupOomAlert>& result)
            : Result(result)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateWork);
        }

        STRICT_STFUNC(StateWork,
            hFunc(TEvCGroupOomAlert, Handle)
        )

    private:
        void Handle(TEvCGroupOomAlert::TPtr& ev) {
            Result.Set(std::move(ev->Get()->Alert));
            PassAway();
        }

    private:
        TResultWaiter<TCGroupOomAlert>& Result;
    };

    class TTrendSubscriberActor : public TActorBootstrapped<TTrendSubscriberActor> {
    public:
        TTrendSubscriberActor(
                TResultWaiter<std::optional<TCGroupOomTrend>>& activeResult,
                TResultWaiter<std::optional<TCGroupOomTrend>>& stoppedResult,
                TDuration timeToOomThreshold,
                std::atomic<ui32>& activeNotifications,
                std::atomic<ui32>& stoppedNotifications)
            : ActiveResult(activeResult)
            , StoppedResult(stoppedResult)
            , TimeToOomThreshold(timeToOomThreshold)
            , ActiveNotifications(activeNotifications)
            , StoppedNotifications(stoppedNotifications)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateWork);
        }

        STRICT_STFUNC(StateWork,
            hFunc(TEvCGroupOomTrend, Handle)
        )

    private:
        void Handle(TEvCGroupOomTrend::TPtr& ev) {
            Y_ABORT_UNLESS(
                ev->Get()->TimeToOomThreshold == TimeToOomThreshold);
            if (ev->Get()->State == ECGroupOomTrendState::Active) {
                ActiveNotifications.fetch_add(1, std::memory_order_relaxed);
                if (!HasActiveResult) {
                    ActiveResult.Set(std::move(ev->Get()->Trend));
                    HasActiveResult = true;
                }
            } else {
                StoppedNotifications.fetch_add(1, std::memory_order_relaxed);
                StoppedResult.Set(std::move(ev->Get()->Trend));
                PassAway();
            }
        }

    private:
        TResultWaiter<std::optional<TCGroupOomTrend>>& ActiveResult;
        TResultWaiter<std::optional<TCGroupOomTrend>>& StoppedResult;
        const TDuration TimeToOomThreshold;
        std::atomic<ui32>& ActiveNotifications;
        std::atomic<ui32>& StoppedNotifications;
        bool HasActiveResult = false;
    };

    TCGroupMemoryStatsPtr MakeMemoryStats(
            ui64 currentBytes,
            ui64 maxBytes,
            ECGroupVersion version = ECGroupVersion::V2) {
        auto stats = std::make_shared<TCGroupMemoryStats>();
        stats->Version = version;
        stats->CGroupPath = "/trend";
        stats->CurrentBytes = currentBytes;
        stats->MaxBytes = TCGroupMemoryLimit{
            .Value = maxBytes,
        };
        return stats;
    }

    struct TMockCGroupV2State {
        TActorSystem* ActorSystem = nullptr;
        TCGroupMemoryStatsPtr InitialMemoryStats;
        TCGroupMemoryStatsPtr MemoryStats;
        std::atomic<bool> ReturnHighMemoryStats = false;
        std::atomic<ui32> ReadStatsCalls = 0;
        std::atomic<ui32> ReadMemoryStatsCalls = 0;
    };

    class TMockCGroupV2StatsSubSystem final : public TCGroupV2StatsSubSystem {
    public:
        explicit TMockCGroupV2StatsSubSystem(TMockCGroupV2State* state)
            : State(state)
        {
        }

        void ReadStats(const TActorId& recipient, ui64 cookie) const override {
            ++State->ReadStatsCalls;
            State->ActorSystem->Send(recipient, new TEvCGroupV2Stats(nullptr), 0, cookie);
        }

        void ReadMemoryStats(const TActorId& recipient, ui64 cookie) const override {
            ++State->ReadMemoryStatsCalls;
            const auto& stats = State->ReturnHighMemoryStats.load()
                ? State->MemoryStats
                : State->InitialMemoryStats;
            State->ActorSystem->Send(
                recipient,
                new TEvCGroupMemoryStats(stats),
                0,
                cookie);
        }

        void OnAfterStart(TActorSystem& actorSystem) override {
            State->ActorSystem = &actorSystem;
        }

    private:
        TMockCGroupV2State* State;
    };

    struct TMockCGroupV1State {
        TActorSystem* ActorSystem = nullptr;
        TCGroupMemoryStatsPtr InitialMemoryStats;
        TCGroupMemoryStatsPtr MemoryStats;
        std::atomic<bool> ReturnHighMemoryStats = false;
        std::atomic<ui32> ReadStatsCalls = 0;
        std::atomic<ui32> ReadMemoryStatsCalls = 0;
    };

    class TMockCGroupV1StatsSubSystem final : public TCGroupV1StatsSubSystem {
    public:
        explicit TMockCGroupV1StatsSubSystem(TMockCGroupV1State* state)
            : State(state)
        {
        }

        void ReadStats(const TActorId& recipient, ui64 cookie) const override {
            ++State->ReadStatsCalls;
            State->ActorSystem->Send(recipient, new TEvCGroupV1Stats(nullptr), 0, cookie);
        }

        void ReadMemoryStats(const TActorId& recipient, ui64 cookie) const override {
            ++State->ReadMemoryStatsCalls;
            const auto& stats = State->ReturnHighMemoryStats.load()
                ? State->MemoryStats
                : State->InitialMemoryStats;
            State->ActorSystem->Send(
                recipient,
                new TEvCGroupMemoryStats(stats),
                0,
                cookie);
        }

        void OnAfterStart(TActorSystem& actorSystem) override {
            State->ActorSystem = &actorSystem;
        }

    private:
        TMockCGroupV1State* State;
    };

    struct TMockCGroupOomState {
        TCGroupOomConfig Config;
        TActorId SubscribedActor;
        TActorId UnsubscribedActor;
        TActorId TrendRecipient;
        TActorId TrendSubscriber;
        TActorId TrendUnsubscriber;
        ECGroupOomTrendWindow TrendWindow = ECGroupOomTrendWindow::ShortWindow;
        ui64 TrendCookie = 0;
        TDuration TrendThreshold;
        ui32 SubscribeCalls = 0;
        ui32 UnsubscribeCalls = 0;
        ui32 ReadTrendCalls = 0;
        ui32 SubscribeToTrendCalls = 0;
        ui32 UnsubscribeFromTrendCalls = 0;
    };

    class TMockCGroupOomSubSystem final : public TCGroupOomSubSystem {
    public:
        explicit TMockCGroupOomSubSystem(TMockCGroupOomState* state)
            : State(state)
        {
        }

        const TCGroupOomConfig& GetConfig() const override {
            return State->Config;
        }

        void Subscribe(const TActorId& actorId) override {
            State->SubscribedActor = actorId;
            ++State->SubscribeCalls;
        }

        void Unsubscribe(const TActorId& actorId) override {
            State->UnsubscribedActor = actorId;
            ++State->UnsubscribeCalls;
        }

        void ReadTrend(
                const TActorId& recipient,
                ECGroupOomTrendWindow window,
                ui64 cookie) const override {
            State->TrendRecipient = recipient;
            State->TrendWindow = window;
            State->TrendCookie = cookie;
            ++State->ReadTrendCalls;
        }

        void SubscribeToTrend(
                const TActorId& actorId,
                ECGroupOomTrendWindow window,
                TDuration timeToOomThreshold) override {
            State->TrendSubscriber = actorId;
            State->TrendWindow = window;
            State->TrendThreshold = timeToOomThreshold;
            ++State->SubscribeToTrendCalls;
        }

        void UnsubscribeFromTrend(const TActorId& actorId) override {
            State->TrendUnsubscriber = actorId;
            ++State->UnsubscribeFromTrendCalls;
        }

    private:
        TMockCGroupOomState* State;
    };

} // namespace

Y_UNIT_TEST_SUITE(TCGroupStatsSubSystemTest) {

    Y_UNIT_TEST(V2SendsStatsToRecipientAndPreservesCookie) {
        TTempDir tempDir;
        const TFsPath root = tempDir.Path();
        WriteFile(root, "proc/self/cgroup", "0::/test\n");
        WriteFile(root, "sys/fs/cgroup/test/memory.current", "4096\n");
        WriteFile(root, "sys/fs/cgroup/test/memory.max", "8192\n");

        auto actorSystem = MakeActorSystem(MakeCGroupV2StatsSubSystem(MakeV2Config(root)));
        actorSystem->Start();
        auto stats = RequestV2Stats(
            *actorSystem,
            GetCGroupV2StatsSubSystem(*actorSystem));

        UNIT_ASSERT(stats);
        UNIT_ASSERT_VALUES_EQUAL(stats->CGroupPath, "/test");
        UNIT_ASSERT(stats->Memory);
        UNIT_ASSERT_VALUES_EQUAL(stats->Memory->CurrentBytes, 4096);
        actorSystem->Stop();
    }

    Y_UNIT_TEST(V2CachesImmutableSnapshot) {
        TTempDir tempDir;
        const TFsPath root = tempDir.Path();
        WriteFile(root, "proc/self/cgroup", "0::/\n");
        WriteFile(root, "sys/fs/cgroup/memory.current", "100\n");

        auto actorSystem = MakeActorSystem(MakeCGroupV2StatsSubSystem(
            MakeV2Config(root, TDuration::Hours(1))));
        actorSystem->Start();
        const auto& subsystem = GetCGroupV2StatsSubSystem(*actorSystem);
        auto first = RequestV2Stats(*actorSystem, subsystem);
        WriteFile(root, "sys/fs/cgroup/memory.current", "200\n");
        auto second = RequestV2Stats(*actorSystem, subsystem);

        UNIT_ASSERT(first);
        UNIT_ASSERT(first == second);
        UNIT_ASSERT_VALUES_EQUAL(second->Memory->CurrentBytes, 100);
        actorSystem->Stop();
    }

    Y_UNIT_TEST(V1SendsVersionNeutralMemoryStats) {
        TTempDir tempDir;
        const TFsPath root = tempDir.Path();
        WriteFile(root, "proc/self/cgroup", "4:memory:/legacy\n");
        WriteFile(root, "sys/fs/cgroup/memory/legacy/memory.usage_in_bytes", "800\n");
        WriteFile(root, "sys/fs/cgroup/memory/legacy/memory.limit_in_bytes", "1000\n");
        WriteFile(root, "sys/fs/cgroup/memory/legacy/memory.failcnt", "3\n");

        auto actorSystem = MakeActorSystem(
            {}, MakeCGroupV1StatsSubSystem(MakeV1Config(root)));
        actorSystem->Start();
        auto stats = RequestMemoryStats(
            *actorSystem,
            GetCGroupV1StatsSubSystem(*actorSystem));

        UNIT_ASSERT(stats);
        UNIT_ASSERT(stats->Version == ECGroupVersion::V1);
        UNIT_ASSERT_VALUES_EQUAL(stats->CGroupPath, "/legacy");
        UNIT_ASSERT_VALUES_EQUAL(stats->CurrentBytes, 800);
        UNIT_ASSERT_VALUES_EQUAL(stats->MaxEvents, 3);
        actorSystem->Stop();
    }

    Y_UNIT_TEST(OomControllerNotifiesActor) {
        TTempDir tempDir;
        const TFsPath root = tempDir.Path();
        WriteFile(root, "proc/self/cgroup", "0::/\n");
        WriteFile(root, "sys/fs/cgroup/memory.current", "500\n");
        WriteFile(root, "sys/fs/cgroup/memory.max", "1000\n");

        TCGroupOomConfig oomConfig;
        oomConfig.ExecutorPoolId = SystemPoolId;
        oomConfig.PollPeriod = TDuration::MilliSeconds(10);
        oomConfig.MemoryUsageThreshold = 0.9;

        auto actorSystem = MakeActorSystem(
            MakeCGroupV2StatsSubSystem(MakeV2Config(root)),
            {},
            MakeCGroupOomSubSystem(oomConfig));
        actorSystem->Start();

        TResultWaiter<TCGroupOomAlert> result;
        const TActorId subscriber = actorSystem->Register(
            new TOomAlertSubscriberActor(result),
            TMailboxType::Simple,
            SystemPoolId);
        auto& oom = GetCGroupOomSubSystem(*actorSystem);
        oom.Subscribe(subscriber);

        WriteFile(root, "sys/fs/cgroup/memory.current", "950\n");
        UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
        const auto& alert = result.GetValue();
        UNIT_ASSERT(alert.HasReason(ECGroupOomReason::MemoryUsageThreshold));
        UNIT_ASSERT_DOUBLES_EQUAL(*alert.MemoryUsage, 0.95, 1e-12);

        oom.Unsubscribe(subscriber);
        actorSystem->Stop();
    }

    Y_UNIT_TEST(OomControllerUsesMockedV2Provider) {
        TMockCGroupV2State state;
        TResultWaiter<TCGroupOomAlert> result;
        state.InitialMemoryStats = MakeMemoryStats(500, 1'000);
        state.MemoryStats = MakeMemoryStats(950, 1'000);

        TCGroupOomConfig oomConfig;
        oomConfig.ExecutorPoolId = SystemPoolId;
        oomConfig.PollPeriod = TDuration::MilliSeconds(10);
        oomConfig.MemoryUsageThreshold = 0.9;

        std::unique_ptr<TCGroupV2StatsSubSystem> provider =
            std::make_unique<TMockCGroupV2StatsSubSystem>(&state);
        auto actorSystem = MakeActorSystem(
            std::move(provider),
            {},
            MakeCGroupOomSubSystem(oomConfig));
        actorSystem->Start();

        const auto& v2 = GetCGroupV2StatsSubSystem(*actorSystem);
        UNIT_ASSERT(!RequestV2Stats(*actorSystem, v2));
        UNIT_ASSERT_VALUES_EQUAL(state.ReadStatsCalls.load(), 1);

        const TActorId subscriber = actorSystem->Register(
            new TOomAlertSubscriberActor(result),
            TMailboxType::Simple,
            SystemPoolId);
        GetCGroupOomSubSystem(*actorSystem).Subscribe(subscriber);
        // Any high-memory reply is enqueued after the subscription.
        state.ReturnHighMemoryStats.store(true);

        UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(state.ReadMemoryStatsCalls.load() >= 1);
        UNIT_ASSERT(result.GetValue().HasReason(ECGroupOomReason::MemoryUsageThreshold));
        actorSystem->Stop();
    }

    Y_UNIT_TEST(OomControllerUsesMockedV1Provider) {
        TMockCGroupV1State state;
        TResultWaiter<TCGroupOomAlert> result;
        state.InitialMemoryStats = MakeMemoryStats(500, 1'000, ECGroupVersion::V1);
        state.MemoryStats = MakeMemoryStats(950, 1'000, ECGroupVersion::V1);

        TCGroupOomConfig oomConfig;
        oomConfig.ExecutorPoolId = SystemPoolId;
        oomConfig.PollPeriod = TDuration::MilliSeconds(10);
        oomConfig.MemoryUsageThreshold = 0.9;

        std::unique_ptr<TCGroupV1StatsSubSystem> provider =
            std::make_unique<TMockCGroupV1StatsSubSystem>(&state);
        auto actorSystem = MakeActorSystem(
            {},
            std::move(provider),
            MakeCGroupOomSubSystem(oomConfig));
        actorSystem->Start();

        const auto& v1 = GetCGroupV1StatsSubSystem(*actorSystem);
        UNIT_ASSERT(!RequestV1Stats(actorSystem.Get(), v1));
        UNIT_ASSERT_VALUES_EQUAL(state.ReadStatsCalls.load(), 1);

        const TActorId subscriber = actorSystem->Register(
            new TOomAlertSubscriberActor(result),
            TMailboxType::Simple,
            SystemPoolId);
        GetCGroupOomSubSystem(*actorSystem).Subscribe(subscriber);
        // Any high-memory reply is enqueued after the subscription.
        state.ReturnHighMemoryStats.store(true);

        UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(state.ReadMemoryStatsCalls.load() >= 1);
        UNIT_ASSERT(result.GetValue().HasReason(ECGroupOomReason::MemoryUsageThreshold));
        actorSystem->Stop();
    }

    Y_UNIT_TEST(OomControllerFallsBackBetweenMockedProviders) {
        TMockCGroupV2State v2State; // Registered provider with no available snapshot.
        TMockCGroupV1State v1State;
        TResultWaiter<TCGroupOomAlert> result;
        v1State.InitialMemoryStats = MakeMemoryStats(500, 1'000, ECGroupVersion::V1);
        v1State.MemoryStats = MakeMemoryStats(950, 1'000, ECGroupVersion::V1);

        TCGroupOomConfig oomConfig;
        oomConfig.ExecutorPoolId = SystemPoolId;
        oomConfig.PollPeriod = TDuration::MilliSeconds(10);
        oomConfig.MemoryUsageThreshold = 0.9;

        auto actorSystem = MakeActorSystem({}, {}, MakeCGroupOomSubSystem(oomConfig));
        actorSystem->RegisterSubSystem<TCGroupV1StatsSubSystem>(
            std::make_unique<TMockCGroupV1StatsSubSystem>(&v1State));
        actorSystem->RegisterSubSystem<TCGroupV2StatsSubSystem>(
            std::make_unique<TMockCGroupV2StatsSubSystem>(&v2State));
        actorSystem->Start();

        const TActorId subscriber = actorSystem->Register(
            new TOomAlertSubscriberActor(result),
            TMailboxType::Simple,
            SystemPoolId);
        GetCGroupOomSubSystem(*actorSystem).Subscribe(subscriber);
        // The threshold crossing must follow subscription enqueue.
        v1State.ReturnHighMemoryStats.store(true);

        UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(v2State.ReadMemoryStatsCalls.load() > 0);
        UNIT_ASSERT(v1State.ReadMemoryStatsCalls.load() > 0);
        const auto& alert = result.GetValue();
        UNIT_ASSERT(alert.Stats == v1State.MemoryStats);
        UNIT_ASSERT(alert.HasReason(ECGroupOomReason::MemoryUsageThreshold));
        UNIT_ASSERT(alert.MemoryUsage);
        UNIT_ASSERT_DOUBLES_EQUAL(*alert.MemoryUsage, 0.95, 1e-12);
        actorSystem->Stop();
    }

    Y_UNIT_TEST(OomPublicContractDispatchesToMock) {
        TMockCGroupOomState state;
        std::unique_ptr<TCGroupOomSubSystem> mock =
            std::make_unique<TMockCGroupOomSubSystem>(&state);
        auto actorSystem = MakeActorSystem({}, {}, std::move(mock));
        actorSystem->Start();
        TCGroupOomSubSystem& subsystem = GetCGroupOomSubSystem(*actorSystem);
        const TActorId actorId(1, "mock");

        UNIT_ASSERT(&subsystem.GetConfig() == &state.Config);
        subsystem.Subscribe(actorId);
        subsystem.Unsubscribe(actorId);
        subsystem.ReadTrend(actorId, ECGroupOomTrendWindow::LongWindow, RequestCookie);
        subsystem.SubscribeToTrend(
            actorId,
            ECGroupOomTrendWindow::LongWindow,
            TDuration::Seconds(1));
        subsystem.UnsubscribeFromTrend(actorId);

        UNIT_ASSERT_VALUES_EQUAL(state.SubscribeCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.UnsubscribeCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.ReadTrendCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.SubscribeToTrendCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.UnsubscribeFromTrendCalls, 1);
        UNIT_ASSERT(state.TrendRecipient == actorId);
        UNIT_ASSERT(state.TrendWindow == ECGroupOomTrendWindow::LongWindow);
        UNIT_ASSERT_VALUES_EQUAL(state.TrendCookie, RequestCookie);
        UNIT_ASSERT_VALUES_EQUAL(state.TrendThreshold, TDuration::Seconds(1));
        actorSystem->Stop();
    }

    Y_UNIT_TEST(CalculatesOomTrendForMultipleWindows) {
        constexpr ui64 MaxBytes = 10'000;
        constexpr ui64 InitialBytes = 1'000;
        constexpr ui64 GrowthBytesPerSecond = 2;

        NActors::NDetail::TCGroupOomTrendCalculator calculator({
            .ShortWindow = TCGroupOomTrendWindowConfig{
                .Duration = TDuration::Minutes(1),
                .RecalculationPeriod = TDuration::Seconds(1),
                .MinimumRSquared = 0.99,
            },
            .LongWindow = TCGroupOomTrendWindowConfig{
                .Duration = TDuration::Hours(1),
                .RecalculationPeriod = TDuration::Minutes(1),
                .MinimumRSquared = 0.99,
            },
        });

        for (ui64 seconds = 0; seconds <= 3'600; seconds += 30) {
            const auto recalculated = calculator.AddSample(
                TMonotonic::Seconds(seconds),
                MakeMemoryStats(
                    InitialBytes + GrowthBytesPerSecond * seconds,
                    MaxBytes));
            UNIT_ASSERT(
                recalculated.Contains(ECGroupOomTrendWindow::ShortWindow));
            UNIT_ASSERT_VALUES_EQUAL(
                recalculated.Contains(ECGroupOomTrendWindow::LongWindow),
                seconds % 60 == 0);

            if (seconds == 60) {
                const auto* shortTrend =
                    calculator.FindTrend(ECGroupOomTrendWindow::ShortWindow);
                UNIT_ASSERT(shortTrend);
                UNIT_ASSERT_DOUBLES_EQUAL(
                    shortTrend->GrowthBytesPerSecond,
                    GrowthBytesPerSecond,
                    1e-12);
                UNIT_ASSERT_DOUBLES_EQUAL(shortTrend->RSquared, 1.0, 1e-12);
                UNIT_ASSERT_VALUES_EQUAL(shortTrend->TimeToOom.Seconds(), 4'440);
                UNIT_ASSERT_VALUES_EQUAL(shortTrend->SampleCount, 3);
                UNIT_ASSERT(
                    !calculator.FindTrend(ECGroupOomTrendWindow::LongWindow));
            }
        }

        const auto* shortTrend =
            calculator.FindTrend(ECGroupOomTrendWindow::ShortWindow);
        const auto* longTrend =
            calculator.FindTrend(ECGroupOomTrendWindow::LongWindow);
        UNIT_ASSERT(shortTrend);
        UNIT_ASSERT(longTrend);
        UNIT_ASSERT_VALUES_EQUAL(shortTrend->TimeToOom.Seconds(), 900);
        UNIT_ASSERT_VALUES_EQUAL(longTrend->TimeToOom.Seconds(), 900);
        UNIT_ASSERT_VALUES_EQUAL(shortTrend->SampleCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(longTrend->SampleCount, 121);

        auto recalculated = calculator.AddSample(
            TMonotonic::Seconds(3'630),
            MakeMemoryStats(
                InitialBytes + GrowthBytesPerSecond * 3'630,
                MaxBytes));
        UNIT_ASSERT(
            recalculated.Contains(ECGroupOomTrendWindow::ShortWindow));
        UNIT_ASSERT(
            !recalculated.Contains(ECGroupOomTrendWindow::LongWindow));
        shortTrend = calculator.FindTrend(ECGroupOomTrendWindow::ShortWindow);
        longTrend = calculator.FindTrend(ECGroupOomTrendWindow::LongWindow);
        UNIT_ASSERT_VALUES_EQUAL(shortTrend->Stats->CurrentBytes, 8'260);
        UNIT_ASSERT_VALUES_EQUAL(longTrend->Stats->CurrentBytes, 8'200);

        recalculated = calculator.AddSample(
            TMonotonic::Seconds(3'660),
            MakeMemoryStats(
                InitialBytes + GrowthBytesPerSecond * 3'660,
                MaxBytes));
        UNIT_ASSERT(
            recalculated.Contains(ECGroupOomTrendWindow::ShortWindow));
        UNIT_ASSERT(
            recalculated.Contains(ECGroupOomTrendWindow::LongWindow));
        longTrend = calculator.FindTrend(ECGroupOomTrendWindow::LongWindow);
        UNIT_ASSERT_VALUES_EQUAL(longTrend->Stats->CurrentBytes, 8'320);
        UNIT_ASSERT_VALUES_EQUAL(longTrend->TimeToOom.Seconds(), 840);
    }

    Y_UNIT_TEST(RejectsNonLinearOomTrend) {
        NActors::NDetail::TCGroupOomTrendCalculator calculator({
            .ShortWindow = TCGroupOomTrendWindowConfig{
                .Duration = TDuration::Minutes(1),
                .MinimumRSquared = 0.99,
            },
        });

        calculator.AddSample(TMonotonic::Seconds(0), MakeMemoryStats(1'000, 10'000));
        calculator.AddSample(TMonotonic::Seconds(30), MakeMemoryStats(1'600, 10'000));
        UNIT_ASSERT(!calculator.HasResult(ECGroupOomTrendWindow::ShortWindow));
        calculator.AddSample(TMonotonic::Seconds(60), MakeMemoryStats(1'100, 10'000));

        UNIT_ASSERT(calculator.HasResult(ECGroupOomTrendWindow::ShortWindow));
        UNIT_ASSERT(!calculator.FindTrend(ECGroupOomTrendWindow::ShortWindow));
    }

    Y_UNIT_TEST(OomControllerNotifiesAndReturnsTrend) {
        TTempDir tempDir;
        const TFsPath root = tempDir.Path();
        WriteFile(root, "proc/self/cgroup", "0::/\n");
        WriteFile(root, "sys/fs/cgroup/memory.current", "100\n");
        WriteFile(root, "sys/fs/cgroup/memory.max", "10000\n");

        constexpr auto trendWindow = ECGroupOomTrendWindow::ShortWindow;
        TCGroupOomConfig oomConfig;
        oomConfig.ExecutorPoolId = SystemPoolId;
        oomConfig.PollPeriod = TDuration::MilliSeconds(10);
        oomConfig.MemoryUsageThreshold.reset();
        oomConfig.TrendWindows.ShortWindow = TCGroupOomTrendWindowConfig{
            .Duration = TDuration::MilliSeconds(500),
            .RecalculationPeriod = TDuration::MilliSeconds(50),
            .MinimumRSquared = 0,
        };

        auto actorSystem = MakeActorSystem(
            MakeCGroupV2StatsSubSystem(MakeV2Config(root)),
            {},
            MakeCGroupOomSubSystem(oomConfig));
        actorSystem->Start();

        TResultWaiter<std::optional<TCGroupOomTrend>> activeResult;
        TResultWaiter<std::optional<TCGroupOomTrend>> stoppedResult;
        std::atomic<ui32> activeNotifications = 0;
        std::atomic<ui32> stoppedNotifications = 0;
        const TDuration timeToOomThreshold = TDuration::Seconds(10);
        const TActorId subscriber = actorSystem->Register(
            new TTrendSubscriberActor(
                activeResult,
                stoppedResult,
                timeToOomThreshold,
                activeNotifications,
                stoppedNotifications),
            TMailboxType::Simple,
            SystemPoolId);

        auto& oom = GetCGroupOomSubSystem(*actorSystem);
        oom.SubscribeToTrend(
            subscriber,
            trendWindow,
            timeToOomThreshold);

        TResultWaiter<std::optional<TCGroupOomTrend>> noTrendResult;
        RequestOomTrend(*actorSystem, oom, trendWindow, noTrendResult);
        UNIT_ASSERT(!noTrendResult.Wait(TDuration::MilliSeconds(20)));
        UNIT_ASSERT(noTrendResult.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!noTrendResult.GetValue());

        for (ui64 currentBytes = 150;
                currentBytes < 10'000 &&
                    activeNotifications.load(std::memory_order_relaxed) == 0;
                currentBytes += 50) {
            WriteFile(
                root,
                "sys/fs/cgroup/memory.current",
                ToString(currentBytes) + "\n");
            Sleep(TDuration::MilliSeconds(20));
        }

        UNIT_ASSERT(activeResult.Wait(TDuration::Seconds(5)));
        const auto& notification = activeResult.GetValue();
        UNIT_ASSERT(notification);
        UNIT_ASSERT(notification->Window == trendWindow);
        UNIT_ASSERT_VALUES_EQUAL(
            notification->WindowDuration,
            TDuration::MilliSeconds(500));
        UNIT_ASSERT(notification->GrowthBytesPerSecond > 0);
        UNIT_ASSERT(notification->TimeToOom <= TDuration::Seconds(10));

        for (size_t attempt = 0;
                attempt < 100 &&
                    activeNotifications.load(std::memory_order_relaxed) < 2;
                ++attempt) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(
            activeNotifications.load(std::memory_order_relaxed) >= 2);

        TResultWaiter<std::optional<TCGroupOomTrend>> latestResult;
        RequestOomTrend(*actorSystem, oom, trendWindow, latestResult);
        UNIT_ASSERT(latestResult.Wait(TDuration::Seconds(5)));
        const auto& latest = latestResult.GetValue();
        UNIT_ASSERT(latest);
        UNIT_ASSERT(latest->Window == trendWindow);
        UNIT_ASSERT(latest->TimeToOom <= TDuration::Seconds(10));

        WriteFile(root, "sys/fs/cgroup/memory.current", "100\n");
        UNIT_ASSERT(stoppedResult.Wait(TDuration::Seconds(5)));
        Sleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(
            stoppedNotifications.load(std::memory_order_relaxed),
            1);

        oom.UnsubscribeFromTrend(subscriber);
        actorSystem->Stop();
    }

} // Y_UNIT_TEST_SUITE
