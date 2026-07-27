#include <subsystems/cgroup/cgroup_events.h>
#include <subsystems/cgroup/cgroup_oom.h>
#include <subsystems/cgroup/cgroup_v1.h>
#include <subsystems/cgroup/cgroup_v2.h>

#include <actor_bootstrapped.h>
#include <actorsystem.h>
#include <executor_pool_basic.h>
#include <executor_pool_io.h>
#include <hfunc.h>
#include <scheduler_basic.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

#include <functional>

using namespace NActors;

namespace {

    constexpr ui32 SystemPoolId = 0;
    constexpr ui32 IoPoolId = 1;
    constexpr ui64 RequestCookie = 42;

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

        TRequestActor(TRequest request, NThreading::TPromise<TResult> promise)
            : Request(std::move(request))
            , Promise(std::move(promise))
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
            Promise.SetValue(std::move(ev->Get()->Stats));
            this->PassAway();
        }

    private:
        const TRequest Request;
        NThreading::TPromise<TResult> Promise;
    };

    template<class TEvent, class TResult>
    NThreading::TFuture<TResult> RequestStats(
            TActorSystem& actorSystem,
            typename TRequestActor<TEvent, TResult>::TRequest request) {
        auto promise = NThreading::NewPromise<TResult>();
        auto future = promise.GetFuture();
        actorSystem.Register(
            new TRequestActor<TEvent, TResult>(std::move(request), std::move(promise)),
            TMailboxType::Simple,
            SystemPoolId);
        return future;
    }

    NThreading::TFuture<TCGroupV2StatsPtr> RequestV2Stats(
            TActorSystem& actorSystem,
            const TCGroupV2StatsSubSystem& subsystem) {
        return RequestStats<TEvCGroupV2Stats, TCGroupV2StatsPtr>(
            actorSystem,
            [&subsystem](const TActorId& recipient, ui64 cookie) {
                subsystem.ReadStats(recipient, cookie);
            });
    }

    NThreading::TFuture<TCGroupMemoryStatsPtr> RequestMemoryStats(
            TActorSystem& actorSystem,
            const ICGroupMemoryStatsProvider& provider) {
        return RequestStats<TEvCGroupMemoryStats, TCGroupMemoryStatsPtr>(
            actorSystem,
            [&provider](const TActorId& recipient, ui64 cookie) {
                provider.ReadMemoryStats(recipient, cookie);
            });
    }

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
            GetCGroupV2StatsSubSystem(*actorSystem)).GetValueSync();

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
        auto first = RequestV2Stats(*actorSystem, subsystem).GetValueSync();
        WriteFile(root, "sys/fs/cgroup/memory.current", "200\n");
        auto second = RequestV2Stats(*actorSystem, subsystem).GetValueSync();

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
            GetCGroupV1StatsSubSystem(*actorSystem)).GetValueSync();

        UNIT_ASSERT(stats);
        UNIT_ASSERT(stats->Version == ECGroupVersion::V1);
        UNIT_ASSERT_VALUES_EQUAL(stats->CGroupPath, "/legacy");
        UNIT_ASSERT_VALUES_EQUAL(stats->CurrentBytes, 800);
        UNIT_ASSERT_VALUES_EQUAL(stats->MaxEvents, 3);
        actorSystem->Stop();
    }

    Y_UNIT_TEST(OomControllerNotifiesCallback) {
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

        auto promise = NThreading::NewPromise<TCGroupOomAlert>();
        auto future = promise.GetFuture();
        auto& oom = GetCGroupOomSubSystem(*actorSystem);
        const auto subscriptionId = oom.Subscribe(
            [promise](const TCGroupOomAlert& alert) mutable {
                promise.TrySetValue(alert);
            });

        WriteFile(root, "sys/fs/cgroup/memory.current", "950\n");
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        const auto& alert = future.GetValue();
        UNIT_ASSERT(alert.HasReason(ECGroupOomReason::MemoryUsageThreshold));
        UNIT_ASSERT_DOUBLES_EQUAL(*alert.MemoryUsage, 0.95, 1e-12);

        oom.Unsubscribe(subscriptionId);
        actorSystem->Stop();
    }

} // Y_UNIT_TEST_SUITE
