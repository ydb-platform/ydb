#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>

#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <util/system/platform.h>

#include <cerrno>
#include <csignal>
#include <memory>
#include <tuple>

#if defined(_unix_)
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace NYql::NDqs {
namespace {

using namespace NActors;

class TIdleTaskRunnerActorFactory
    : public NDq::NTaskRunnerActor::ITaskRunnerActorFactory
{
public:
    std::tuple<NDq::NTaskRunnerActor::ITaskRunnerActor*, IActor*> Create(
        NDq::NTaskRunnerActor::ITaskRunnerActor::ICallbacks* /*parent*/,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> /*alloc*/,
        const NDq::TTxId& /*txId*/,
        ui64 /*taskId*/,
        THolder<NDq::TDqMemoryQuota>&& /*memoryQuota*/) override
    {
        UNIT_FAIL("Idle worker should not run a task");
        return {};
    }
};

class TLocalWorkerManagerFixture
{
public:
    TTestActorRuntimeBase Runtime;
    TLocalWorkerManagerOptions Options;
    TActorId Manager;
    TActorId Allocator;

    TLocalWorkerManagerFixture()
    {
        Runtime.Initialize();
        const auto poller = Runtime.GetLocalServiceId(MakePollerActorId());
        UNIT_ASSERT(poller);
        Runtime.SetObserverFunc([poller] (TAutoPtr<IEventHandle>& event) {
            if (event->GetRecipientRewrite() == poller) {
                UNIT_ASSERT(event->GetTypeRewrite() == TEvents::TSystem::Bootstrap);
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        Options.MkqlInitialMemoryLimit = 1;
        Options.TaskRunnerActorFactory = std::make_shared<TIdleTaskRunnerActorFactory>();
        Allocator = Runtime.AllocateEdgeActor();
        Manager = Runtime.Register(CreateLocalWorkerManager(Options));
        Synchronize();
    }

    TEvAllocateWorkersResponse::TPtr Allocate(ui64 resourceId, ui64 freeAfterMs = 0)
    {
        auto request = MakeHolder<TEvAllocateWorkersRequest>(
            /*count*/ 1, /*user*/ "", TMaybe<ui64>(resourceId));
        request->Record.SetTraceId("deadline-test");
        request->Record.SetFreeWorkerAfterMs(freeAfterMs);
        Runtime.Send(new IEventHandle(Manager, Allocator, request.Release()));
        auto response = Runtime.GrabEdgeEvent<TEvAllocateWorkersResponse>(
            Allocator, TDuration::Seconds(1));
        UNIT_ASSERT(response && response->Get()->Record.HasWorkers());
        return response;
    }

    void Expire()
    {
        Sleep(TDuration::MilliSeconds(2));
        Runtime.Send(new IEventHandle(Manager, Allocator, new TEvents::TEvWakeup()));
        Synchronize();
    }

    void Synchronize()
    {
        Runtime.Send(new IEventHandle(Manager, Allocator, new TEvQueryStatus()));
        UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvQueryStatusResponse>(
            Allocator, TDuration::Seconds(1)));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(LocalWorkerManagerTest)
{
    Y_UNIT_TEST(FreeNotificationPreservesSenderCheckAndIsIdempotent)
    {
        for (bool matchingSender : {true, false}) {
            TLocalWorkerManagerFixture fixture;
            fixture.Allocate(101);
            const auto sender = matchingSender ? fixture.Allocator : fixture.Runtime.AllocateEdgeActor();
            for (int attempt = 0; attempt < 2; ++attempt) {
                fixture.Runtime.Send(new IEventHandle(
                    fixture.Manager, sender, new TEvFreeWorkersNotify(101)));
                fixture.Synchronize();
                UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.ActiveWorkers->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.FreeGroupError->Val(), matchingSender ? 0 : 1);
            }
        }
    }

#if defined(_unix_)
    Y_UNIT_TEST(ShutdownWithAllocatedGroupsExitsNormally)
    {
        const auto child = fork();
        UNIT_ASSERT_C(child >= 0, "fork failed, errno: " << errno);
        if (child == 0) {
            try {
                TLocalWorkerManagerFixture fixture;
                fixture.Allocate(101);
                fixture.Allocate((1ULL << 40) + 101);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.ActiveWorkers->Val(), 2);
                fixture.Runtime.Send(new IEventHandle(
                    fixture.Manager, fixture.Allocator, new TEvents::TEvPoison()));
                fixture.Synchronize();
            } catch (...) {
                _exit(2);
            }
            _exit(3);
        }

        int status = 0;
        pid_t waited = 0;
        const auto deadline = TMonotonic::Now() + TDuration::Seconds(10);
        do {
            waited = waitpid(child, &status, WNOHANG);
            if (waited == child || (waited < 0 && errno != EINTR)) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TMonotonic::Now() < deadline);

        UNIT_ASSERT_C(waited >= 0 || errno == EINTR, "waitpid failed, errno: " << errno);
        const bool completed = waited == child;
        if (!completed) {
            kill(child, SIGKILL);
            do {
                waited = waitpid(child, &status, 0);
            } while (waited < 0 && errno == EINTR);
        }
        UNIT_ASSERT_C(waited == child, "waitpid failed, errno: " << errno);
        UNIT_ASSERT_C(completed, "shutdown did not complete before the deadline");
        UNIT_ASSERT_C(WIFEXITED(status), "shutdown child status: " << status);
        UNIT_ASSERT_VALUES_EQUAL(WEXITSTATUS(status), 0);
    }
#endif

    Y_UNIT_TEST(DeadlineKeepsFullResourceId)
    {
        TLocalWorkerManagerFixture fixture;
        fixture.Allocate((1ULL << 40) + 101, /*freeAfterMs*/ 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.ActiveWorkers->Val(), 1);
        fixture.Expire();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.ActiveWorkers->Val(), 0);
    }

    Y_UNIT_TEST(DeadlinePreservesResourceWithMatchingLowBits)
    {
        TLocalWorkerManagerFixture fixture;
        const auto live = fixture.Allocate(101);
        fixture.Allocate((1ULL << 40) + 101, /*freeAfterMs*/ 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.ActiveWorkers->Val(), 2);
        fixture.Expire();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.ActiveWorkers->Val(), 1);
        const auto replay = fixture.Allocate(101);
        UNIT_ASSERT_VALUES_EQUAL(
            live->Get()->Record.GetWorkers().SerializeAsString(),
            replay->Get()->Record.GetWorkers().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Options.Counters.ActiveWorkers->Val(), 1);
    }
}

} // namespace NYql::NDqs
