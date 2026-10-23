#include "dq_channel_service_impl.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NDq;

namespace {
class TPendingFinishBuffer : public IChannelBuffer {
public:
    explicit TPendingFinishBuffer(ui64 channel)
        : IChannelBuffer(TChannelFullInfo(channel, {}, {}, 1, 2, TCollectStatsLevel::None)) {}

    EDqFillLevel GetFillLevel() const override { return NoLimit; }
    void SetFillAggregator(std::shared_ptr<TDqFillAggregator>) override {}
    void Push(TDataChunk&&) override { Y_ABORT("Unexpected producer access"); }
    bool IsFinished() override { return Confirmed; }
    bool IsEarlyFinished() override { return false; }
    bool IsEmpty() override { return !Queued; }
    bool Pop(TDataChunk& data) override {
        if (!Queued) { return false; }
        Queued = false;
        data = TDataChunk(true);
        return true;
    }
    void EarlyFinish() override { Confirmed = true; }
    void ExportPushStats(TDqAsyncStats&) override {}
    void ExportPopStats(TDqAsyncStats&) override {}

    void ConfirmFinish() {
        Confirmed = true;
        GetInputTask()->Signal(InputDependency(Info.ChannelId));
    }

private:
    bool Queued = true;
    bool Confirmed = false;
};
}

Y_UNIT_TEST_SUITE(DqChannelTrace) {
    Y_UNIT_TEST(FinishMarkerWaitsForItsConfirmationAfterAnOlderInputProbe) {
        using namespace NKikimr::NMiniKQL;
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TMemoryUsageInfo memory("channel-trace-test");
        THolderFactory factory(alloc.Ref(), memory);
        auto* type = TDataType::Create(NYql::NUdf::TDataType<i32>::Id, env);
        for (bool confirmBeforeComplete : {false, true}) {
            ui32 enqueues = 0;
            auto task = std::make_shared<TTaskSchedulingHandle>(NActors::TActorId{}, NActors::TActorId{},
                [&enqueues](auto) { ++enqueues; }, [] {});
            task->RequestExecution();
            const auto generation = task->TryDispatch();
            UNIT_ASSERT(generation && task->Start(*generation));

            // A merge probed a now-finished channel before consuming EOF from
            // the next input. Its stale probe must not hide the new dependency.
            task->Await(InputDependency(42), task->Observe(InputDependency(42)));
            auto buffer = std::make_shared<TPendingFinishBuffer>(43);
            TDqChannelSettings settings;
            settings.ChannelId = 43;
            settings.RowType = type;
            settings.HolderFactory = &factory;
            auto channel = MakeIntrusive<TFastDqInputChannel>(std::weak_ptr<TDqChannelService>{}, settings, buffer);
            channel->BindTaskScheduling(task);
            TUnboxedValueBatch batch(type);
            TMaybe<TInstant> watermark;
            UNIT_ASSERT(!channel->Pop(batch, watermark));
            UNIT_ASSERT(!channel->IsFinished());
            if (confirmBeforeComplete) { buffer->ConfirmFinish(); }
            task->Complete({.RunnerInvoked = true});
            if (!confirmBeforeComplete) { buffer->ConfirmFinish(); }
            UNIT_ASSERT_VALUES_EQUAL(enqueues, 2);
            UNIT_ASSERT(task->GetSnapshot().State == ETaskSchedulingState::Ready);
            task->Cancel(true);
        }
    }

    Y_UNIT_TEST(LocalSnapshotDoesNotArmNotificationsOrObserveWaits) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto registry = std::make_shared<TLocalBufferRegistry>(nullptr, counters, 1024, 800, 128, false);
        const TChannelFullInfo info(42, {}, {}, 1, 2, TCollectStatsLevel::None);
        auto buffer = registry->GetOrCreateLocalBuffer(registry, info, nullptr);
        auto task = std::make_shared<TTaskSchedulingHandle>(NActors::TActorId{}, NActors::TActorId{}, [](auto) {}, [] {});
        buffer->BindTaskScheduling(task, true);
        task->RequestExecution();
        const auto generation = task->TryDispatch();
        UNIT_ASSERT(generation && task->Start(*generation));
        const auto empty = buffer->GetTraceSnapshot();
        UNIT_ASSERT(empty.Empty && *empty.Empty);
        UNIT_ASSERT(empty.Finished && !*empty.Finished);
        UNIT_ASSERT_VALUES_EQUAL(*empty.CapacityBytes, 1024);
        UNIT_ASSERT(!buffer->NeedToNotifyInput.load());
        UNIT_ASSERT(!buffer->NeedToNotifyOutput.load());
        UNIT_ASSERT(!task->HasObservedWaits());
        {
            std::lock_guard lock(buffer->Mutex);
            buffer->Queue.emplace(true);
            buffer->FillLevel = HardLimit;
            buffer->Finished.store(true);
        }
        const auto full = buffer->GetTraceSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(*full.BufferedChunks, 1);
        UNIT_ASSERT(full.Empty && !*full.Empty);
        UNIT_ASSERT(full.Finished && *full.Finished);
        UNIT_ASSERT_VALUES_EQUAL(*full.FillLevel, static_cast<ui32>(HardLimit));
        UNIT_ASSERT(!buffer->NeedToNotifyInput.load());
        UNIT_ASSERT(!buffer->NeedToNotifyOutput.load());
        UNIT_ASSERT(!task->HasObservedWaits());
        task->Cancel(true);
    }
}
