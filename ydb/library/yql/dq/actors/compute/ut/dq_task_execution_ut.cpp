#include <ydb/library/yql/dq/actors/compute/dq_task_execution.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NDq;

namespace {
struct TQuotaState {
    ui32 Starts = 0, Stops = 0, Destroyed = 0;
};

struct TQuotaWork : IDqSchedulableWork {
    explicit TQuotaWork(std::shared_ptr<TQuotaState> state) : State(std::move(state)) {}
    ~TQuotaWork() override { ++State->Destroyed; }
    std::optional<TDuration> TryStartExecution(TMonotonic) override {
        ++State->Starts;
        return TDuration::MilliSeconds(5);
    }
    void StopExecution() override { ++State->Stops; }
    void NotifyResumed(bool) override {}
    void RegisterForResume(const NActors::TActorId&) override {}
    TWorkScope GetWorkScope() const override { return {}; }
    const std::shared_ptr<TQuotaState> State;
};

struct TQuotaFactory : IDqSchedulableWorkFactory {
    explicit TQuotaFactory(std::shared_ptr<TQuotaState> state) : State(std::move(state)) {}
    std::unique_ptr<IDqSchedulableWork> CreateSchedulableWork() override {
        return std::make_unique<TQuotaWork>(State);
    }
    TWorkScope GetWorkScope() const override { return {}; }
    const std::shared_ptr<TQuotaState> State;
};

struct TScheduler : IDqTaskScheduler {
    std::shared_ptr<const TQueryGraphRegistration> RegisterGraph(NActors::TActorId,
        std::shared_ptr<const TQueryTaskGraph>, TVector<ui64>) override { return {}; }
    std::shared_ptr<TTaskSchedulingHandle> Register(NActors::TActorId, NActors::TActorId, ui64) override { return {}; }
    void BindExecution(NActors::TActorId, NActors::TActorId, std::shared_ptr<IDqTaskExecution>) override {}
    void NotifyExecutionAvailable() override { ++AvailableNotifications; }
    void SetQueryWeight(NActors::TActorId, double) override {}
    void DescribeQuery(NActors::TActorId, NActors::TActorId) override {}
    const TTaskSchedulerSettings& GetSettings() const override { return Settings; }
    const TTaskSchedulerSettings Settings;
    ui32 AvailableNotifications = 0;
};
} // namespace

Y_UNIT_TEST_SUITE(DqDetachedTaskExecution) {
    Y_UNIT_TEST(CancellingAQuotaWaitBalancesThrottleExactlyOnce) {
        const auto state = std::make_shared<TQuotaState>();
        const auto scheduler = std::make_shared<TScheduler>();
        const auto task = std::make_shared<TTaskSchedulingHandle>(NActors::TActorId{}, NActors::TActorId{}, [](auto) {}, [] {});
        // A rejected grant must not touch the runner. Keeping it absent makes
        // accidental execution on a quota denial fail immediately.
        auto execution = std::make_shared<TDqTaskExecution>(TIntrusivePtr<IDqTaskRunner>{}, nullptr, scheduler, task,
            std::make_shared<TQuotaFactory>(state), nullptr, NActors::TActorId{});
        UNIT_ASSERT(execution->TryAcquire());
        for (ui32 i = 0; i < 2; ++i) {
            const auto result = execution->RunSlice({100, TMonotonic::Now() + TDuration::MilliSeconds(1)});
            UNIT_ASSERT(result.Progress.QuotaRejected);
            UNIT_ASSERT(!result.Progress.RunnerInvoked);
            UNIT_ASSERT(result.RetryAfter && *result.RetryAfter == TDuration::MilliSeconds(5));
        }
        UNIT_ASSERT_VALUES_EQUAL(state->Starts, 2);
        UNIT_ASSERT_VALUES_EQUAL(state->Stops, 0);
        execution->Retire(false);
        execution->Retire(false);
        UNIT_ASSERT_VALUES_EQUAL(state->Stops, 1);
        UNIT_ASSERT_VALUES_EQUAL(state->Destroyed, 1);
        execution->Release();
        UNIT_ASSERT(execution->IsAvailable());
        UNIT_ASSERT_VALUES_EQUAL(scheduler->AvailableNotifications, 1);
    }
}
