#include "resource_broker_impl.h"

#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#ifndef NDEBUG
const bool ENABLE_DETAILED_RESOURCE_BROKER_LOG = true;
#else
const bool ENABLE_DETAILED_RESOURCE_BROKER_LOG = false;
#endif

namespace NKikimr {

using namespace NKikimrResourceBroker;
using namespace NResourceBroker;

static void SetupLogging(TTestActorRuntime& runtime) {
    NActors::NLog::EPriority priority = ENABLE_DETAILED_RESOURCE_BROKER_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
    runtime.SetLogPriority(NKikimrServices::RESOURCE_BROKER, priority);
}

static NKikimrResourceBroker::TResourceBrokerConfig
MakeTestConfig()
{
    NKikimrResourceBroker::TResourceBrokerConfig config;

    auto queue = config.AddQueues();
    queue->SetName("queue_default");
    queue->SetWeight(5);
    queue->MutableLimit()->AddResource(400);

    queue = config.AddQueues();
    queue->SetName("queue_compaction0");
    queue->SetWeight(10);
    queue->MutableLimit()->AddResource(400);

    queue = config.AddQueues();
    queue->SetName("queue_compaction1");
    queue->SetWeight(20);
    queue->MutableLimit()->AddResource(400);

    queue = config.AddQueues();
    queue->SetName("queue_scan");
    queue->SetWeight(20);
    queue->MutableLimit()->AddResource(400);

    auto task = config.AddTasks();
    task->SetName("unknown");
    task->SetQueueName("queue_default");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName("compaction0");
    task->SetQueueName("queue_compaction0");
    task->SetDefaultDuration(TDuration::Seconds(10).GetValue());

    task = config.AddTasks();
    task->SetName("compaction1");
    task->SetQueueName("queue_compaction1");
    task->SetDefaultDuration(TDuration::Seconds(20).GetValue());

    task = config.AddTasks();
    task->SetName("scan");
    task->SetQueueName("queue_scan");
    task->SetDefaultDuration(TDuration::Seconds(20).GetValue());

    config.MutableResourceLimit()->AddResource(500);
    config.MutableResourceLimit()->AddResource(500);

    return config;
}

static void
WaitForBootstrap(TTestActorRuntime &runtime)
{
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
    UNIT_ASSERT(runtime.DispatchEvents(options));
}

static void
SubmitTask(TTestActorRuntime &runtime, TActorId broker, TActorId sender,
           ui64 id, ui64 cpu, ui64 memory, const TString &type,
           ui32 priority, TIntrusivePtr<TThrRefBase> cookie = nullptr)
{
    TAutoPtr<TEvResourceBroker::TEvSubmitTask> event
        = new TEvResourceBroker::TEvSubmitTask(id,
                                               "task-" + ToString(id),
                                               {{cpu, memory}},
                                               type,
                                               priority,
                                               cookie);
    runtime.Send(new IEventHandle(broker, sender, event.Release()));
}

static void
UpdateTask(TTestActorRuntime &runtime, TActorId broker, TActorId sender,
           ui64 id, ui64 cpu, ui64 memory, ui32 priority, const TString &type,
           bool resubmit = false)
{
    TAutoPtr<TEvResourceBroker::TEvUpdateTask> event
        = new TEvResourceBroker::TEvUpdateTask(id,
                                               {{cpu, memory}},
                                               type, priority, resubmit);

    runtime.Send(new IEventHandle(broker, sender, event.Release()));
}

static void
UpdateTaskCookie(TTestActorRuntime &runtime, TActorId broker, TActorId sender,
                 ui64 id, TIntrusivePtr<TThrRefBase> cookie)
{
    TAutoPtr<TEvResourceBroker::TEvUpdateTaskCookie> event
        = new TEvResourceBroker::TEvUpdateTaskCookie(id, cookie);

    runtime.Send(new IEventHandle(broker, sender, event.Release()));
}

static void
RemoveTask(TTestActorRuntime &runtime, TActorId broker, TActorId sender, ui64 id)
{
    TAutoPtr<TEvResourceBroker::TEvRemoveTask> event
        = new TEvResourceBroker::TEvRemoveTask(id);

    runtime.Send(new IEventHandle(broker, sender, event.Release()));
}

static void
FinishTask(TTestActorRuntime &runtime, TActorId broker, TActorId sender, ui64 id)
{
    TAutoPtr<TEvResourceBroker::TEvFinishTask> event
        = new TEvResourceBroker::TEvFinishTask(id);

    runtime.Send(new IEventHandle(broker, sender, event.Release()));
}

static ui64
WaitForResourceAllocation(TTestActorRuntime &runtime, ui64 id,
                          TIntrusivePtr<TThrRefBase> cookie = nullptr)
{
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvResourceBroker::TEvResourceAllocated>(handle);
    if (id)
        UNIT_ASSERT_VALUES_EQUAL(reply->TaskId, id);
    UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, cookie);
    return reply->TaskId;
}

static void
WaitForError(TTestActorRuntime &runtime, ui64 id, TEvResourceBroker::TStatus::ECode code,
             TIntrusivePtr<TThrRefBase> cookie = nullptr)
{
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvResourceBroker::TEvTaskOperationError>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->TaskId, id);
    UNIT_ASSERT_VALUES_EQUAL(reply->Status.Code, code);
    UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, cookie);
}

static void
CheckCounters(::NMonitoring::TDynamicCounterPtr counters, const TString &group, const TString &name,
              ui64 cpu, ui64 memory, ui64 finished, ui64 enqueued, ui64 infly)
{
    auto g = counters->GetSubgroup(group, name);
    UNIT_ASSERT_VALUES_EQUAL(g->GetCounter("CPUConsumption")->Val(), cpu);
    UNIT_ASSERT_VALUES_EQUAL(g->GetCounter("MemoryConsumption")->Val(), memory);
    UNIT_ASSERT_VALUES_EQUAL(g->GetCounter("FinishedTasks")->Val(), finished);
    UNIT_ASSERT_VALUES_EQUAL(g->GetCounter("EnqueuedTasks")->Val(), enqueued);
    UNIT_ASSERT_VALUES_EQUAL(g->GetCounter("InFlyTasks")->Val(), infly);
}

static void CheckConfigure(TTestActorRuntime &runtime, TActorId broker, TActorId sender,
                           const NKikimrResourceBroker::TResourceBrokerConfig &config,
                           bool success)
{
    TAutoPtr<TEvResourceBroker::TEvConfigure> event = new TEvResourceBroker::TEvConfigure;
    event->Record.CopyFrom(config);
    runtime.Send(new IEventHandle(broker, sender, event.Release()));

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvResourceBroker::TEvConfigureResult>(handle);
    auto &rec = reply->Record;

    UNIT_ASSERT_VALUES_EQUAL((int)rec.GetSuccess(), (int)success);
}

static
TIntrusivePtr<IResourceBroker> GetInstantResourceBroker(TTestActorRuntime &runtime, TActorId broker, TActorId sender) {
    runtime.Send(new IEventHandle(broker, sender, new TEvResourceBroker::TEvResourceBrokerRequest));
    auto answer = runtime.GrabEdgeEvent<TEvResourceBroker::TEvResourceBrokerResponse>(sender);
    return answer->Get()->ResourceBroker;
}

Y_UNIT_TEST_SUITE(TResourceBroker) {
    Y_UNIT_TEST(TestErrors) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender1 = runtime.AllocateEdgeActor();
        TActorId sender2 = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        auto cookie1 = MakeIntrusive<TThrRefBase>();
        auto cookie2 = MakeIntrusive<TThrRefBase>();

        // Submit task-1.
        SubmitTask(runtime, brokerId, sender1, 1, 400, 400, "unknown", 5, cookie1);
        // Resources are allocated for task-1.
        WaitForResourceAllocation(runtime, 1, cookie1);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender1, 2, 500, 500, "compaction0", 5, cookie2);
        // Submit task-3.
        SubmitTask(runtime, brokerId, sender1, 3, 500, 500, "compaction0", 5, cookie2);
        // Submit task-4.
        SubmitTask(runtime, brokerId, sender1, 4, 500, 500, "compaction0", 5, nullptr);

        // Submit task-2 one more time and get ALREADY_EXISTS.
        SubmitTask(runtime, brokerId, sender1, 2, 500, 500, "compaction0", 5, cookie1);
        WaitForError(runtime, 2, TEvResourceBroker::TStatus::ALREADY_EXISTS, cookie1);

        // Try to remove task-1 and get TASK_IN_FLY.
        RemoveTask(runtime, brokerId, sender1, 1);
        WaitForError(runtime, 1, TEvResourceBroker::TStatus::TASK_IN_FLY, cookie1);
        // Try to remove task-5 and get UNKNOWN_TASK.
        RemoveTask(runtime, brokerId, sender1, 5);
        WaitForError(runtime, 5, TEvResourceBroker::TStatus::UNKNOWN_TASK, nullptr);
        // Try to remove task-2 by another client and get UNKNOWN_TASK.
        RemoveTask(runtime, brokerId, sender2, 2);
        WaitForError(runtime, 2, TEvResourceBroker::TStatus::UNKNOWN_TASK, nullptr);
        // Remove task-2.
        RemoveTask(runtime, brokerId, sender1, 2);

        // Try to update task-2 and get UNKNOWN_TASK.
        UpdateTask(runtime, brokerId, sender1, 2, 500, 500, 4, "unknown");
        WaitForError(runtime, 2, TEvResourceBroker::TStatus::UNKNOWN_TASK, nullptr);
        // Try to update task-4 by another client and get UNKNOWN_TASK.
        UpdateTask(runtime, brokerId, sender2, 4, 500, 500, 4, "unknown");
        WaitForError(runtime, 4, TEvResourceBroker::TStatus::UNKNOWN_TASK, nullptr);
        // Update resources and priority of task-4.
        UpdateTask(runtime, brokerId, sender1, 4, 250, 250, 4, "compaction0");
        // Update resources and priority of task-3.
        UpdateTask(runtime, brokerId, sender1, 3, 250, 250, 6, "compaction0");

        // Try to finish task-5 and get UNKNOWN_TASK.
        FinishTask(runtime, brokerId, sender1, 5);
        WaitForError(runtime, 5, TEvResourceBroker::TStatus::UNKNOWN_TASK, nullptr);
        // Try to finish task-2 by another client and get UNKNOWN_TASK.
        FinishTask(runtime, brokerId, sender2, 2);
        WaitForError(runtime, 2, TEvResourceBroker::TStatus::UNKNOWN_TASK, nullptr);
        // Try to finish task-3 and get TASK_IN_QUEUE.
        FinishTask(runtime, brokerId, sender1, 3);
        WaitForError(runtime, 3, TEvResourceBroker::TStatus::TASK_IN_QUEUE, cookie2);
        // Finish task-1.
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender1, 1);

        // Resources are allocated for task-4.
        WaitForResourceAllocation(runtime, 4, nullptr);
        // Finish task-4.
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender1, 4);
        // Resources are allocated for task-3.
        WaitForResourceAllocation(runtime, 3,  cookie2);
    };

    Y_UNIT_TEST(TestOverusage) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit task-1.
        SubmitTask(runtime, brokerId, sender, 1, 50, 50, "compaction0", 5);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender, 2, 410, 410, "compaction0", 5);
        // Submit task-3.
        SubmitTask(runtime, brokerId, sender, 3, 550, 550, "compaction1", 5);

        // Resources are allocated for task-1.
        WaitForResourceAllocation(runtime, 1);
        // Finish task-1.
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 1);
        // Resources are allocated for task-3.
        WaitForResourceAllocation(runtime, 3);
        // Finish task-3.
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 3);
        // Resources are allocated for task-2.
        WaitForResourceAllocation(runtime, 2);
    }

    Y_UNIT_TEST(TestExecutionStat) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit task-1 to block queues.
        SubmitTask(runtime, brokerId, sender, 1, 500, 500, "unknown", 5);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender, 2, 50, 50, "compaction1", 5);
        // Submit task-3.
        SubmitTask(runtime, brokerId, sender, 3, 50, 50, "compaction0", 5);
        // Submit task-4.
        SubmitTask(runtime, brokerId, sender, 4, 50, 50, "compaction1", 5);
        // Submit task-5.
        SubmitTask(runtime, brokerId, sender, 5, 50, 50, "compaction0", 5);
        // Submit task-6.
        SubmitTask(runtime, brokerId, sender, 6, 50, 50, "compaction1", 5);
        // Submit task-7.
        SubmitTask(runtime, brokerId, sender, 7, 50, 50, "compaction0", 5);

        // Finish task-1.
        WaitForResourceAllocation(runtime, 1);
        FinishTask(runtime, brokerId, sender, 1);
        // Now expect resource allocation in submission order (GEN1 task has
        // 2x more estimated execution time but its queue has 2x weight).
        WaitForResourceAllocation(runtime, 2);
        WaitForResourceAllocation(runtime, 3);
        WaitForResourceAllocation(runtime, 4);
        WaitForResourceAllocation(runtime, 5);
        WaitForResourceAllocation(runtime, 6);
        WaitForResourceAllocation(runtime, 7);
        // Finish all tasks keeping old execution stat.
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 3);
        FinishTask(runtime, brokerId, sender, 5);
        FinishTask(runtime, brokerId, sender, 7);
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 2);
        FinishTask(runtime, brokerId, sender, 4);
        FinishTask(runtime, brokerId, sender, 6);

        // Now submit and finish tasks to change statistics for GEN1.
        for (int i = 0; i < 20; ++i) {
            SubmitTask(runtime, brokerId, sender, 1, 50, 50, "compaction1", 5);
            WaitForResourceAllocation(runtime, 1);
            now += TDuration::Seconds(10);
            runtime.UpdateCurrentTime(now);
            FinishTask(runtime, brokerId, sender, 1);
        }

        // Submit task-1 to block queues.
        SubmitTask(runtime, brokerId, sender, 1, 500, 500, "unknown", 5);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender, 2, 50, 50, "compaction1", 5);
        // Submit task-3.
        SubmitTask(runtime, brokerId, sender, 3, 50, 50, "compaction0", 5);
        // Submit task-4.
        SubmitTask(runtime, brokerId, sender, 4, 50, 50, "compaction1", 5);
        // Submit task-5.
        SubmitTask(runtime, brokerId, sender, 5, 50, 50, "compaction0", 5);
        // Submit task-6.
        SubmitTask(runtime, brokerId, sender, 6, 50, 50, "compaction1", 5);
        // Submit task-7.
        SubmitTask(runtime, brokerId, sender, 7, 50, 50, "compaction0", 5);

        // Finish task-1.
        WaitForResourceAllocation(runtime, 1);
        FinishTask(runtime, brokerId, sender, 1);
        // Now expect resource allocation in modified order because GEN1
        // task became 2x cheaper.
        WaitForResourceAllocation(runtime, 2);
        WaitForResourceAllocation(runtime, 3);
        WaitForResourceAllocation(runtime, 4);
        WaitForResourceAllocation(runtime, 6);
        WaitForResourceAllocation(runtime, 5);
        WaitForResourceAllocation(runtime, 7);
    }

    Y_UNIT_TEST(TestRealUsage) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit task-1.
        SubmitTask(runtime, brokerId, sender, 1, 400, 400, "compaction0", 5);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender, 2, 400, 400, "compaction1", 5);
        // Submit task-3.
        SubmitTask(runtime, brokerId, sender, 3, 400, 400, "compaction0", 5);
        // Submit task-4.
        SubmitTask(runtime, brokerId, sender, 4, 400, 400, "compaction1", 5);
        // Submit task-5.
        SubmitTask(runtime, brokerId, sender, 5, 400, 400, "compaction0", 5);
        // Submit task-6.
        SubmitTask(runtime, brokerId, sender, 6, 400, 400, "compaction1", 5);

        // Finish task-1 after 10 seconds.
        WaitForResourceAllocation(runtime, 1);
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 1);

        // Now finish GEN1 tasks in 7 seconds and all GEN0 should wait due to real
        // resources usage.
        WaitForResourceAllocation(runtime, 2);
        now += TDuration::Seconds(7);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 2);

        WaitForResourceAllocation(runtime, 4);
        now += TDuration::Seconds(7);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 4);

        WaitForResourceAllocation(runtime, 6);
    }

    Y_UNIT_TEST(TestCounters) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit task-1.
        SubmitTask(runtime, brokerId, sender, 1, 200, 200, "compaction0", 5);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender, 2, 100, 100, "compaction1", 5);
        // Submit task-3.
        SubmitTask(runtime, brokerId, sender, 3, 100, 100, "compaction1", 5);
        // Submit task-4.
        SubmitTask(runtime, brokerId, sender, 4, 100, 100, "compaction1", 5);
        // Submit task-5.
        SubmitTask(runtime, brokerId, sender, 5, 250, 250, "compaction0", 5);
        // Submit task-6.
        SubmitTask(runtime, brokerId, sender, 6, 250, 250, "compaction1", 5);
        // Submit task-7.
        SubmitTask(runtime, brokerId, sender, 7, 150, 150, "compaction1", 5);

        WaitForResourceAllocation(runtime, 1);
        WaitForResourceAllocation(runtime, 2);
        WaitForResourceAllocation(runtime, 3);
        WaitForResourceAllocation(runtime, 4);

        CheckCounters(counters, "queue", "total", 500, 500, 0, 3, 4);
        CheckCounters(counters, "queue", "queue_compaction0", 200, 200, 0, 1, 1);
        CheckCounters(counters, "queue", "queue_compaction1", 300, 300, 0, 2, 3);
        CheckCounters(counters, "task", "compaction0", 200, 200, 0, 1, 1);
        CheckCounters(counters, "task", "compaction1", 300, 300, 0, 2, 3);

        FinishTask(runtime, brokerId, sender, 1);
        FinishTask(runtime, brokerId, sender, 2);

        WaitForResourceAllocation(runtime, 5);

        CheckCounters(counters, "queue", "total", 450, 450, 2, 2, 3);
        CheckCounters(counters, "queue", "queue_compaction0", 250, 250, 1, 0, 1);
        CheckCounters(counters, "queue", "queue_compaction1", 200, 200, 1, 2, 2);
        CheckCounters(counters, "task", "compaction0", 250, 250, 1, 0, 1);
        CheckCounters(counters, "task", "compaction1", 200, 200, 1, 2, 2);

        FinishTask(runtime, brokerId, sender, 3);
        FinishTask(runtime, brokerId, sender, 4);

        WaitForResourceAllocation(runtime, 6);

        CheckCounters(counters, "queue", "total", 500, 500, 4, 1, 2);
        CheckCounters(counters, "queue", "queue_compaction0", 250, 250, 1, 0, 1);
        CheckCounters(counters, "queue", "queue_compaction1", 250, 250, 3, 1, 1);
        CheckCounters(counters, "task", "compaction0", 250, 250, 1, 0, 1);
        CheckCounters(counters, "task", "compaction1", 250, 250, 3, 1, 1);

        FinishTask(runtime, brokerId, sender, 5);
        FinishTask(runtime, brokerId, sender, 6);

        WaitForResourceAllocation(runtime, 7);

        CheckCounters(counters, "queue", "total", 150, 150, 6, 0, 1);
        CheckCounters(counters, "queue", "queue_compaction0", 0, 0, 2, 0, 0);
        CheckCounters(counters, "queue", "queue_compaction1", 150, 150, 4, 0, 1);
        CheckCounters(counters, "task", "compaction0", 0, 0, 2, 0, 0);
        CheckCounters(counters, "task", "compaction1", 150, 150, 4, 0, 1);

        FinishTask(runtime, brokerId, sender, 7);
        SubmitTask(runtime, brokerId, sender, 1000, 500, 500, "compaction0", 5);
        WaitForResourceAllocation(runtime, 1000);

        for (ui64 i = 1; i <= 10; ++i)
            SubmitTask(runtime, brokerId, sender, i, 1, 1, "unknown", 5);

        now += TDuration::Seconds(15);
        runtime.UpdateCurrentTime(now);

        FinishTask(runtime, brokerId, sender, 1000);
        for (ui64 i = 1; i <= 10; ++i)
            WaitForResourceAllocation(runtime, i);

        now += TDuration::Seconds(5);
        runtime.UpdateCurrentTime(now);

        for (ui64 i = 1; i <= 10; ++i)
            FinishTask(runtime, brokerId, sender, i);
    }

    Y_UNIT_TEST(TestQueueWithConfigure) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        config.MutableTasks(2)->SetQueueName("default");

        SubmitTask(runtime, brokerId, sender, 1, 500, 500, "unknown", 5);
        SubmitTask(runtime, brokerId, sender, 2, 200, 200, "compaction0", 5);
        SubmitTask(runtime, brokerId, sender, 3, 200, 200, "compaction1", 5);

        WaitForResourceAllocation(runtime, 1);

        config = MakeTestConfig();
        config.MutableResourceLimit()->Clear();
        config.MutableResourceLimit()->AddResource(1000);
        config.MutableResourceLimit()->AddResource(1000);
        config.MutableQueues(1)->SetWeight(50);
        TAutoPtr<TEvResourceBroker::TEvConfigure> event1 = new TEvResourceBroker::TEvConfigure;
        event1->Record.CopyFrom(config);
        runtime.Send(new IEventHandle(brokerId, sender, event1.Release()));

        WaitForResourceAllocation(runtime, 2);
        WaitForResourceAllocation(runtime, 3);

        TAutoPtr<IEventHandle> handle;
        auto reply = runtime.GrabEdgeEventRethrow<TEvResourceBroker::TEvConfigureResult>(handle);
        UNIT_ASSERT(reply->Record.GetSuccess());

        // Try unknown queue.
        config.MutableTasks(2)->SetQueueName("queue_default1");
        CheckConfigure(runtime, brokerId, sender, config, false);

        // Try no unknown task.
        config.MutableTasks(2)->SetQueueName("queue_default");
        config.MutableTasks(0)->SetName("unknown1");
        CheckConfigure(runtime, brokerId, sender, config, false);

        // Try no default queue.
        config.MutableQueues(0)->SetName("queue_default1");
        config.MutableTasks(0)->SetName("unknown");
        CheckConfigure(runtime, brokerId, sender, config, false);

        // Correct config.
        config.MutableQueues(0)->SetName("queue_default");
        CheckConfigure(runtime, brokerId, sender, config, true);
    }

    Y_UNIT_TEST(TestRandomQueue) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        TSet<ui64> ids;
        const char *names[] = { "unknown", "compaction0", "compaction1", "wrong" };
        for (ui64 i = 1; i <= 1000; ++i) {
            SubmitTask(runtime, brokerId, sender, i, RandomNumber<ui32>(500), RandomNumber<ui32>(500),
                       names[RandomNumber<ui32>(4)], RandomNumber<ui32>(5));
            ids.insert(i);
        }

        while (!ids.empty()) {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvResourceBroker::TEvResourceAllocated>(handle);
            UNIT_ASSERT(ids.contains(reply->TaskId));
            ids.erase(reply->TaskId);
            now += TDuration::MilliSeconds(RandomNumber<ui32>(20000));
            runtime.UpdateCurrentTime(now);
            FinishTask(runtime, brokerId, sender, reply->TaskId);
        }

        CheckCounters(counters, "queue", "total", 0, 0, 1000, 0, 0);
    }

    Y_UNIT_TEST(TestNotifyActorDied) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender1 = runtime.AllocateEdgeActor();
        TActorId sender2 = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit task-1.
        SubmitTask(runtime, brokerId, sender1, 1, 500, 500, "compaction0", 5);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender1, 2, 200, 200, "compaction1", 5);
        // Submit task-3.
        SubmitTask(runtime, brokerId, sender2, 3, 200, 200, "compaction0", 5);
        // Submit task-4.
        SubmitTask(runtime, brokerId, sender2, 4, 200, 200, "compaction1", 5);

        // Get resources for task-1.
        WaitForResourceAllocation(runtime, 1);

        // Remove task-1 and task-2.
        now += TDuration::Seconds(1);
        runtime.UpdateCurrentTime(now);
        runtime.Send(new IEventHandle(brokerId, sender1, new TEvResourceBroker::TEvNotifyActorDied));

        // Get resources for task-4.
        WaitForResourceAllocation(runtime, 4);
        // Get resources for task-3.
        WaitForResourceAllocation(runtime, 3);
    }

    Y_UNIT_TEST(TestAutoTaskId) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit tasks task-1 .. task-100
        for (ui64 i = 1; i <= 100; ++i)
            SubmitTask(runtime, brokerId, sender, i, 400, 400, "compaction0", 5);

        // Get resources for task-1.
        WaitForResourceAllocation(runtime, 1);

        // Submit task with auto id and get resources for it
        SubmitTask(runtime, brokerId, sender, 0, 100, 100, "compaction1", 5);
        auto id = WaitForResourceAllocation(runtime, 0);
        UNIT_ASSERT(id);

        // Submit task to check auto task is finished correctly.
        SubmitTask(runtime, brokerId, sender, id + 1, 100, 100, "compaction1", 5);

        // Remove auto task and wait the next one in queue.
        now += TDuration::Seconds(1);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, id);
        WaitForResourceAllocation(runtime, id + 1);
    }

    Y_UNIT_TEST(TestChangeTaskType) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit tasks task-1.
        SubmitTask(runtime, brokerId, sender, 1, 400, 400, "compaction0", 5);
        // Submit tasks task-2.
        SubmitTask(runtime, brokerId, sender, 2, 400, 400, "compaction0", 5);
        // Submit tasks task-3.
        SubmitTask(runtime, brokerId, sender, 3, 400, 400, "compaction0", 5);

        // Get resources for task-1.
        WaitForResourceAllocation(runtime, 1);

        // Move task-3 to another queue.
        UpdateTask(runtime, brokerId, sender, 3, 400, 400, 5, "compaction1");

        // Finish task-1.
        now += TDuration::Seconds(1);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 1);

        // Get resources for task-3.
        WaitForResourceAllocation(runtime, 3);
    }

    Y_UNIT_TEST(TestResubmitTask) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit tasks task-1.
        SubmitTask(runtime, brokerId, sender, 1, 200, 200, "compaction0", 5);
        // Submit tasks task-2.
        SubmitTask(runtime, brokerId, sender, 2, 200, 200, "compaction0", 5);
        // Submit tasks task-3.
        SubmitTask(runtime, brokerId, sender, 3, 200, 200, "compaction0", 5);

        // Get resources for task-1.
        WaitForResourceAllocation(runtime, 1);
        WaitForResourceAllocation(runtime, 2);

        // Increase resource requirement for task-2 and return it to queue.
        UpdateTask(runtime, brokerId, sender, 2, 400, 400, 5, "compaction0", true);

        // Finish task-1.
        now += TDuration::Seconds(1);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 1);

        // Get resources for task-2.
        WaitForResourceAllocation(runtime, 2);

        // Decrease resource requirement for task-2 and return it to queue.
        UpdateTask(runtime, brokerId, sender, 2, 200, 200, 5, "compaction0", true);

        // Get resources for task-2.
        WaitForResourceAllocation(runtime, 2);
        // Get resources for task-3.
        WaitForResourceAllocation(runtime, 3);
    }

    Y_UNIT_TEST(TestUpdateCookie) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        TIntrusivePtr<TThrRefBase> cookie1 = new TThrRefBase;
        TIntrusivePtr<TThrRefBase> cookie2 = new TThrRefBase;

        // Submit task task-1.
        SubmitTask(runtime, brokerId, sender, 1, 400, 400, "compaction0", 5);
        WaitForResourceAllocation(runtime, 1);
        // Submit task task-2.
        SubmitTask(runtime, brokerId, sender, 2, 200, 200, "compaction0", 5, cookie1);
        // Update cookie for task-2.
        UpdateTaskCookie(runtime, brokerId, sender, 2, cookie2);
        // Finish task-1.
        FinishTask(runtime, brokerId, sender, 1);
        // Get resources for task-2.
        WaitForResourceAllocation(runtime, 2, cookie2);
        // Submit task task-3.
        SubmitTask(runtime, brokerId, sender, 3, 200, 200, "compaction0", 5);
        // Get resources for task-3.
        WaitForResourceAllocation(runtime, 3);
        // Update cookie for task-2.
        UpdateTaskCookie(runtime, brokerId, sender, 2, cookie1);
        // Finish task-3.
        FinishTask(runtime, brokerId, sender, 3);
        // Increase resource requirement for task-2 and return it to queue.
        UpdateTask(runtime, brokerId, sender, 2, 400, 400, 5, "compaction0", true);
        // Get resources for task-2.
        WaitForResourceAllocation(runtime, 2, cookie1);
    }

    Y_UNIT_TEST(TestOverusageDifferentResources) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        auto now = Now();
        runtime.UpdateCurrentTime(now);
        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto config = MakeTestConfig();
        auto broker = CreateResourceBrokerActor(config, counters);
        auto brokerId = runtime.Register(broker);
        WaitForBootstrap(runtime);

        // Submit task-1.
        SubmitTask(runtime, brokerId, sender, 1, 500, 0, "compaction0", 5);
        // Submit task-2.
        SubmitTask(runtime, brokerId, sender, 2, 500, 0, "compaction1", 5);

        // Resources are allocated for task-1.
        WaitForResourceAllocation(runtime, 1);
        // Finish task-1.
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 1);

        // Resources are allocated for task-2.
        WaitForResourceAllocation(runtime, 2);
        // Finish task-2.
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);
        FinishTask(runtime, brokerId, sender, 2);

        // Submit task-3.
        SubmitTask(runtime, brokerId, sender, 3, 250, 0, "compaction1", 5);
        WaitForResourceAllocation(runtime, 3);
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);

        // Submit task-4.
        SubmitTask(runtime, brokerId, sender, 4, 0, 800, "scan", 5);
        now += TDuration::Seconds(10);
        runtime.UpdateCurrentTime(now);

        // Submit task-5, then above task-4 must not block this allocation.
        SubmitTask(runtime, brokerId, sender, 5, 250, 0, "compaction0", 5);
        WaitForResourceAllocation(runtime, 5);
    }
};

Y_UNIT_TEST_SUITE(TResourceBrokerInstant) {
    Y_UNIT_TEST(Test) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto broker = runtime.Register(CreateResourceBrokerActor(MakeTestConfig(), counters));
        WaitForBootstrap(runtime);

        auto rb = GetInstantResourceBroker(runtime, broker, sender);

        bool ok = rb->SubmitTaskInstant({/* taskId */ 1, /* name */ "task-1", /* resources */ {100, 100},
                                         /* type */ "compaction0", /* priority */ 0, /* cookie */ nullptr}, sender);
        UNIT_ASSERT(ok);
        CheckCounters(counters, "queue", "total", 100, 100, 0, 0, 1);

        ok = rb->ReduceTaskResourcesInstant(/* taskId */ 1, /* reduceBy */ {20, 30}, sender);
        UNIT_ASSERT(ok);
        CheckCounters(counters, "queue", "total", 80, 70, 0, 0, 1);

        ok = rb->FinishTaskInstant({/* taskId */ 1, /* cancel */ false}, sender);
        UNIT_ASSERT(ok);
        CheckCounters(counters, "queue", "total", 0, 0, 1, 0, 0);
    }

    Y_UNIT_TEST(TestErrors) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto broker = runtime.Register(CreateResourceBrokerActor(MakeTestConfig(), counters));
        WaitForBootstrap(runtime);

        auto rb = GetInstantResourceBroker(runtime, broker, sender);

        bool ok = rb->SubmitTaskInstant({/* taskId */ 1, /* name */ "task-1", /* resources */ {100, 100},
                                         /* type */ "compaction0", /* priority */ 0, /* cookie */ nullptr}, sender);
        UNIT_ASSERT(ok);
        CheckCounters(counters, "queue", "total", 100, 100, 0, 0, 1);

        ok = rb->SubmitTaskInstant({/* taskId */ 1, /* name */ "task-1", /* resources */ {100500, 100500},
                                    /* type */ "compaction0", /* priority */ 0, /* cookie */ nullptr}, sender);
        UNIT_ASSERT(!ok);
        CheckCounters(counters, "queue", "total", 100, 100, 0, 0, 1);

        ok = rb->ReduceTaskResourcesInstant(/* taskId */ 2, /* reduceBy */ {20, 30}, sender);
        UNIT_ASSERT(!ok);
        CheckCounters(counters, "queue", "total", 100, 100, 0, 0, 1);

        ok = rb->FinishTaskInstant({/* taskId */ 2, /* cancel */ false}, sender);
        UNIT_ASSERT(!ok);
        CheckCounters(counters, "queue", "total", 100, 100, 0, 0, 1);
    }

    Y_UNIT_TEST(TestMerge) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        SetupLogging(runtime);

        TActorId sender = runtime.AllocateEdgeActor();

        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto broker = runtime.Register(CreateResourceBrokerActor(MakeTestConfig(), counters));
        WaitForBootstrap(runtime);

        auto rb = GetInstantResourceBroker(runtime, broker, sender);

        // recipient task
        {
            bool ok = rb->SubmitTaskInstant({/* taskId */ 1, /* name */ "task-1", /* resources */ {100, 200},
                                             /* type */ "compaction0", /* priority */ 0, /* cookie */ nullptr}, sender);
            UNIT_ASSERT(ok);
            CheckCounters(counters, "queue", "total", 100, 200, 0, 0, 1);
        }

        // donor task
        {
            bool ok = rb->SubmitTaskInstant({/* taskId */ 2, /* name */ "task-2", /* resources */ {100, 100},
                                             /* type */ "compaction0", /* priority */ 0, /* cookie */ nullptr}, sender);
            UNIT_ASSERT(ok);
            CheckCounters(counters, "queue", "total", 200, 300, 0, 0, 2);
        }

        // merge tasks
        {
            bool ok = rb->MergeTasksInstant(/* recipientTaskId */ 1, /* donorTaskId */ 2, sender);
            UNIT_ASSERT(ok);

            CheckCounters(counters, "queue", "total", 200, 300, 1, 0, 1);
        }

        // donor task of different type
        {
            bool ok = rb->SubmitTaskInstant({/* taskId */ 3, /* name */ "task-3", /* resources */ {10, 20},
                                             /* type */ "compaction1", /* priority */ 0, /* cookie */ nullptr}, sender);
            UNIT_ASSERT(ok);

            CheckCounters(counters, "queue", "total", 210, 320, 1, 0, 2);
        }

        // merge tasks
        {
            bool ok = rb->MergeTasksInstant(/* recipientTaskId */ 1, /* donorTaskId */ 3, sender);
            UNIT_ASSERT(!ok);

            CheckCounters(counters, "queue", "total", 210, 320, 1, 0, 2);
        }
    }
};

Y_UNIT_TEST_SUITE(TResourceBrokerConfig) {

    Y_UNIT_TEST(UpdateQueues) {
        NKikimrResourceBroker::TResourceBrokerConfig config;
        if (auto* q = config.AddQueues()) {
            q->SetName("queue0");
            q->SetWeight(10);
            q->MutableLimit()->AddResource(1);
            q->MutableLimit()->AddResource(128);
        }

        NKikimrResourceBroker::TResourceBrokerConfig updates;
        if (auto* q = updates.AddQueues()) {
            q->SetName("queue1");
            q->SetWeight(100);
            q->MutableLimit()->SetCpu(10);
            q->MutableLimit()->SetMemory(1024);
        }
        if (auto* q = updates.AddQueues()) {
            q->SetName("queue2");
            q->SetWeight(200);
            q->MutableLimit()->AddResource(20);
            q->MutableLimit()->AddResource(2048);
        }
        NResourceBroker::MergeConfigUpdates(config, updates);

        UNIT_ASSERT_VALUES_EQUAL(config.QueuesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(0).ShortDebugString(), "Name: \"queue0\" Weight: 10 Limit { Resource: 1 Resource: 128 }");
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(1).ShortDebugString(), "Name: \"queue1\" Weight: 100 Limit { Cpu: 10 Memory: 1024 }");
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(2).ShortDebugString(), "Name: \"queue2\" Weight: 200 Limit { Cpu: 20 Memory: 2048 }");

        updates.Clear();
        if (auto* q = updates.AddQueues()) {
            q->SetName("queue0");
            q->MutableLimit()->AddResource(2);
        }
        if (auto* q = updates.AddQueues()) {
            q->SetName("queue1");
            q->MutableLimit()->SetCpu(15);
        }
        NResourceBroker::MergeConfigUpdates(config, updates);

        UNIT_ASSERT_VALUES_EQUAL(config.QueuesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(0).ShortDebugString(), "Name: \"queue0\" Weight: 10 Limit { Resource: 2 Resource: 128 Cpu: 2 }");
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(1).ShortDebugString(), "Name: \"queue1\" Weight: 100 Limit { Cpu: 15 Memory: 1024 }");
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(2).ShortDebugString(), "Name: \"queue2\" Weight: 200 Limit { Cpu: 20 Memory: 2048 }");

        updates.Clear();
        if (auto* q = updates.AddQueues()) {
            q->SetName("queue0");
            q->MutableLimit()->SetCpu(3);
            q->MutableLimit()->SetMemory(256);
        }
        NResourceBroker::MergeConfigUpdates(config, updates);

        UNIT_ASSERT_VALUES_EQUAL(config.QueuesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(0).ShortDebugString(), "Name: \"queue0\" Weight: 10 Limit { Resource: 3 Resource: 256 Cpu: 3 Memory: 256 }");
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(1).ShortDebugString(), "Name: \"queue1\" Weight: 100 Limit { Cpu: 15 Memory: 1024 }");
        UNIT_ASSERT_VALUES_EQUAL(config.GetQueues(2).ShortDebugString(), "Name: \"queue2\" Weight: 200 Limit { Cpu: 20 Memory: 2048 }");
    }

    Y_UNIT_TEST(UpdateTasks) {
        NKikimrResourceBroker::TResourceBrokerConfig config;
        if (auto* t = config.AddTasks()) {
            t->SetName("task0");
            t->SetQueueName("queue0");
            t->SetDefaultDuration(TDuration::Seconds(1).GetValue());
        }

        NKikimrResourceBroker::TResourceBrokerConfig updates;
        if (auto* t = updates.AddTasks()) {
            t->SetName("task1");
            t->SetQueueName("queue1");
            t->SetDefaultDuration(TDuration::Seconds(2).GetValue());
        }
        if (auto* t = updates.AddTasks()) {
            t->SetName("task2");
            t->SetQueueName("queue2");
            t->SetDefaultDuration(TDuration::Seconds(3).GetValue());
        }
        NResourceBroker::MergeConfigUpdates(config, updates);

        UNIT_ASSERT_VALUES_EQUAL(config.TasksSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(config.GetTasks(0).ShortDebugString(), "Name: \"task0\" QueueName: \"queue0\" DefaultDuration: 1000000");
        UNIT_ASSERT_VALUES_EQUAL(config.GetTasks(1).ShortDebugString(), "Name: \"task1\" QueueName: \"queue1\" DefaultDuration: 2000000");
        UNIT_ASSERT_VALUES_EQUAL(config.GetTasks(2).ShortDebugString(), "Name: \"task2\" QueueName: \"queue2\" DefaultDuration: 3000000");

        updates.Clear();
        if (auto* t = updates.AddTasks()) {
            t->SetName("task0");
            t->SetQueueName("changed0");
        }
        if (auto* t = updates.AddTasks()) {
            t->SetName("task1");
            t->SetDefaultDuration(TDuration::Seconds(4).GetValue());
        }
        NResourceBroker::MergeConfigUpdates(config, updates);

        UNIT_ASSERT_VALUES_EQUAL(config.TasksSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(config.GetTasks(0).ShortDebugString(), "Name: \"task0\" QueueName: \"changed0\" DefaultDuration: 1000000");
        UNIT_ASSERT_VALUES_EQUAL(config.GetTasks(1).ShortDebugString(), "Name: \"task1\" QueueName: \"queue1\" DefaultDuration: 4000000");
        UNIT_ASSERT_VALUES_EQUAL(config.GetTasks(2).ShortDebugString(), "Name: \"task2\" QueueName: \"queue2\" DefaultDuration: 3000000");
    }

    Y_UNIT_TEST(UpdateResourceLimit) {
        NKikimrResourceBroker::TResourceBrokerConfig config;
        if (auto* r = config.MutableResourceLimit()) {
            r->SetCpu(10);
            r->SetMemory(1024);
        }

        NKikimrResourceBroker::TResourceBrokerConfig updates;
        if (auto* r = updates.MutableResourceLimit()) {
            r->AddResource(20);
            r->SetMemory(2048);
        }
        NResourceBroker::MergeConfigUpdates(config, updates);

        UNIT_ASSERT_VALUES_EQUAL(config.ShortDebugString(), "ResourceLimit { Cpu: 20 Memory: 2048 }");
    }

} // TResourceBrokerConfig

} // NKikimr
