#include <ydb/library/yql/providers/common/message_stream/async_io/read_actor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NFq::NMessageStream::NInternal {
using namespace NYql::NDq;
namespace {

struct TState {
    bool Allow = false;
    bool Active = false;
    bool Waiting = false;
    ui32 Starts = 0;
    ui32 Stops = 0;
    std::optional<bool> Resumed;
    NActors::TActorId ActorId;
};

class TWork : public IDqSchedulableWork {
public:
    explicit TWork(TState& state) : State(state) {}

    std::optional<TDuration> TryStartExecution(TMonotonic) override {
        UNIT_ASSERT(!State.Active);
        ++State.Starts;
        State.Waiting = !State.Allow;
        State.Active = State.Allow;
        return State.Allow ? std::nullopt : std::make_optional(TDuration::MilliSeconds(10));
    }

    void StopExecution() override {
        UNIT_ASSERT(State.Active || State.Waiting);
        ++State.Stops;
        State.Active = State.Waiting = false;
    }

    void NotifyResumed(bool byScheduler) override { State.Resumed = byScheduler; }
    void RegisterForResume(const NActors::TActorId& actorId) override { State.ActorId = actorId; }
    TWorkScope GetWorkScope() const override { return {}; }

private:
    TState& State;
};

class TFactory : public IDqSchedulableWorkFactory {
public:
    TState State;
    std::unique_ptr<IDqSchedulableWork> CreateSchedulableWork() override {
        return std::make_unique<TWork>(State);
    }
    TWorkScope GetWorkScope() const override { return {}; }
};

} // namespace

Y_UNIT_TEST_SUITE(TPqCpuQuotaTest) {
    Y_UNIT_TEST(DefersCallbacksUntilQuotaIsAvailable) {
        auto factory = std::make_shared<TFactory>();
        TMessageStreamCpuQuota quota(factory);
        ui32 calls = 0;
        auto callback = [&] { UNIT_ASSERT(factory->State.Active); ++calls; };
        UNIT_ASSERT(quota.Execute(callback));
        UNIT_ASSERT(quota.Execute(callback));
        UNIT_ASSERT_VALUES_EQUAL(calls, 0);
        UNIT_ASSERT_VALUES_EQUAL(factory->State.Stops, 0);
        UNIT_ASSERT(quota.GetCpuTime() == TDuration::Zero());
        quota.NotifyResumed(false);
        UNIT_ASSERT(factory->State.Resumed == false);
        factory->State.Allow = true;
        UNIT_ASSERT(!quota.Execute(callback));
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        UNIT_ASSERT_VALUES_EQUAL(factory->State.Stops, 1);
        UNIT_ASSERT(!quota.IsWaiting());
    }

    Y_UNIT_TEST(SchedulerResume) {
        auto factory = std::make_shared<TFactory>();
        TMessageStreamCpuQuota quota(factory);
        const NActors::TActorId actorId(1, 42);
        quota.RegisterForResume(actorId);
        UNIT_ASSERT(factory->State.ActorId == actorId);
        UNIT_ASSERT(quota.Execute([] {}));
        quota.NotifyResumed(true);
        UNIT_ASSERT(factory->State.Resumed == true);
        factory->State.Allow = true;
        UNIT_ASSERT(!quota.Execute([] {}));
        quota.NotifyResumed(false); // A stale wakeup must not affect the next unit.
        UNIT_ASSERT(factory->State.Resumed == true);
    }

    Y_UNIT_TEST(CancelAndDestructionReleaseThrottle) {
        auto factory = std::make_shared<TFactory>();
        {
            TMessageStreamCpuQuota quota(factory);
            UNIT_ASSERT(quota.Execute([] {}));
            quota.Cancel();
            quota.Cancel();
            UNIT_ASSERT_VALUES_EQUAL(factory->State.Stops, 1);
            UNIT_ASSERT(quota.Execute([] {}));
        }
        UNIT_ASSERT_VALUES_EQUAL(factory->State.Stops, 2);
        UNIT_ASSERT(!factory->State.Waiting);
    }

    Y_UNIT_TEST(ExceptionReleasesQuota) {
        auto factory = std::make_shared<TFactory>();
        factory->State.Allow = true;
        TMessageStreamCpuQuota quota(factory);
        UNIT_ASSERT_EXCEPTION(quota.Execute([] { ythrow yexception() << "callback failed"; }), yexception);
        UNIT_ASSERT_VALUES_EQUAL(factory->State.Stops, 1);
        UNIT_ASSERT(!factory->State.Active);
        UNIT_ASSERT(!quota.Execute([] {}));
        UNIT_ASSERT_VALUES_EQUAL(factory->State.Stops, 2);
    }

    Y_UNIT_TEST(WithoutScheduler) {
        TMessageStreamCpuQuota quota(nullptr);
        bool called = false;
        UNIT_ASSERT(!quota.Execute([&] { called = true; }));
        UNIT_ASSERT(called);
        quota.Cancel();
    }
}
} // namespace NFq::NMessageStream::NInternal
