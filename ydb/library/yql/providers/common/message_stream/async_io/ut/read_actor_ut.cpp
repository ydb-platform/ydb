#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/providers/common/message_stream/async_io/testlib/read_actor_fixture.h>

namespace NFq::NMessageStream::NTest {
namespace {
class TCallbackQuotaFactory final : public IDqSchedulableWorkFactory {
    class TWork final : public IDqSchedulableWork {
    public:
        explicit TWork(TCallbackQuotaFactory& owner) : Owner(owner) {}
        std::optional<TDuration> TryStartExecution(TMonotonic) override {
            ++Owner.Attempts;
            if (Owner.Permits) {
                --Owner.Permits;
                return std::nullopt;
            }
            return TDuration::Hours(1);
        }
        void StopExecution() override { ++Owner.Stops; }
        void NotifyResumed(bool scheduler) override { Owner.Resumes.push_back(scheduler); }
        void RegisterForResume(const NActors::TActorId&) override {}
        TWorkScope GetWorkScope() const override { return {}; }
    private:
        TCallbackQuotaFactory& Owner;
    };
public:
    ui32 Permits = 0;
    ui32 Attempts = 0;
    ui32 Stops = 0;
    TVector<bool> Resumes;
    std::unique_ptr<IDqSchedulableWork> CreateSchedulableWork() override {
        return std::make_unique<TWork>(*this);
    }
    TWorkScope GetWorkScope() const override { return {}; }
};
} // namespace

Y_UNIT_TEST_SUITE(MessageStreamReadActor) {
    Y_UNIT_TEST(AllocatorIsRequired) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            CreateMessageStreamReadActor(TMessageStreamReadActorSettings{}, std::make_unique<TState>()),
            yexception,
            "Message stream read actor requires an allocator");
    }

    Y_UNIT_TEST(StaleQuotaTimerAfterSchedulerResume) {
        using namespace NActors;
        auto factory = std::make_shared<TCallbackQuotaFactory>();
        TVector<ui32> callbacks;
        TVector<TAutoPtr<IEventHandle>> timers;
        TTestActorRuntimeBase runtime(1, false);
        runtime.SetScheduledEventFilter([&](auto&, TAutoPtr<IEventHandle>& event, TDuration, TInstant&) {
            timers.emplace_back(event.Release());
            return true;
        });
        runtime.Initialize();
        TMessageStreamReadActorSettings settings;
        settings.WorkFactory = factory;
        settings.Alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__);
        auto [input, actor] = CreateMessageStreamReadActor(std::move(settings), std::make_unique<TState>());
        Y_UNUSED(input);
        const auto actorId = runtime.Register(actor);
        const auto dispatchUntil = [&](auto condition) {
            TDispatchOptions options;
            options.CustomFinalCondition = condition;
            if (!condition()) {
                runtime.DispatchEvents(options);
            }
            UNIT_ASSERT(condition());
        };
        runtime.Send(new IEventHandle(actorId, TActorId(), new TEvExecuteMessageStreamCallback([&] {
            callbacks.push_back(1);
        })));
        runtime.Send(new IEventHandle(actorId, TActorId(), new TEvExecuteMessageStreamCallback([&] {
            callbacks.push_back(2);
        })));
        dispatchUntil([&] { return timers.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(factory->Attempts, 1);
        UNIT_ASSERT(callbacks.empty());

        // Resume the first callback, then block on the second one with a new timer.
        factory->Permits = 1;
        runtime.Send(new IEventHandle(actorId, TActorId(), new TEvents::TEvWakeup(201)));
        dispatchUntil([&] { return timers.size() == 2; });
        UNIT_ASSERT_VALUES_EQUAL(callbacks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(callbacks[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(factory->Attempts, 3);
        UNIT_ASSERT_VALUES_EQUAL(factory->Stops, 1);
        UNIT_ASSERT_VALUES_EQUAL(factory->Resumes.size(), 1);
        UNIT_ASSERT(factory->Resumes[0]);

        // The old timer must not attempt to acquire the newly available quota.
        factory->Permits = 1;
        // Send delivers synchronously in this runtime.
        runtime.Send(timers[0].Release());
        UNIT_ASSERT_VALUES_EQUAL(factory->Attempts, 3);
        UNIT_ASSERT_VALUES_EQUAL(factory->Resumes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(callbacks.size(), 1);

        runtime.Send(timers[1].Release());
        dispatchUntil([&] { return callbacks.size() == 2; });
        UNIT_ASSERT_VALUES_EQUAL(callbacks[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(factory->Attempts, 4);
        UNIT_ASSERT_VALUES_EQUAL(factory->Stops, 2);
        UNIT_ASSERT_VALUES_EQUAL(factory->Resumes.size(), 2);
        UNIT_ASSERT(!factory->Resumes[1]);
    }


    Y_UNIT_TEST(StreamingDoesNotRequireWriteTimeOrEndOffset) {
        TFixture f;
        f.Client->SupportsWriteTime = false;
        f.Init(true);
        f.Start(std::nullopt);
        f.Data(0);
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT(!f.Finished);
        UNIT_ASSERT(!f.Client->Settings.RequireWriteTime);
        UNIT_ASSERT(!f.Client->Settings.ReadFromWriteTime);
        UNIT_ASSERT(!f.Control->Bound);
    }
    Y_UNIT_TEST(SnapshotDoesNotRequireWriteTime) {
        TFixture f;
        f.Client->SupportsWriteTime = false;
        f.Init();
        f.Start(1);
        f.Data(0);
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT(f.Finished);
        UNIT_ASSERT(!f.Client->Settings.ReadFromWriteTime);
        UNIT_ASSERT(!f.Client->Settings.RequireWriteTime);
    }
    Y_UNIT_TEST_TWIN(ExplicitReadTimeBoundRequiresTimestamp, Epoch) {
        TFixture f;
        f.BeginWriteTime = Epoch ? TInstant::Zero() : TInstant::Seconds(1);
        f.Init(true);
        f.Start(std::nullopt);
        f.Data(0);
        UNIT_ASSERT_EXCEPTION(f.Read(), TMessageStreamException);
        UNIT_ASSERT(f.Client->Settings.ReadFromWriteTime);
        UNIT_ASSERT_VALUES_EQUAL(*f.Client->Settings.ReadFromWriteTime, *f.BeginWriteTime);
        UNIT_ASSERT(f.Client->Settings.RequireWriteTime);
    }
    Y_UNIT_TEST(CheckpointReadTimeBoundRequiresTimestamp) {
        TFixture f;
        auto state = std::make_unique<TState>();
        state->GetReadState().StartingMessageTimestamp = TInstant::Seconds(5);
        f.Init(true, false, std::move(state));
        f.Start(std::nullopt);
        f.Data(0);
        UNIT_ASSERT_EXCEPTION(f.Read(), TMessageStreamException);
        UNIT_ASSERT(f.Client->Settings.ReadFromWriteTime);
        UNIT_ASSERT_VALUES_EQUAL(*f.Client->Settings.ReadFromWriteTime, TInstant::Seconds(5));
        UNIT_ASSERT(f.Client->Settings.RequireWriteTime);
    }
    Y_UNIT_TEST(UpperWriteTimeBoundRequiresTimestampWithoutAddingLowerBound) {
        TFixture f;
        f.EndWriteTime = TInstant::Seconds(5);
        f.Init();
        f.Start(1);
        f.Data(0);
        UNIT_ASSERT_EXCEPTION(f.Read(), TMessageStreamException);
        UNIT_ASSERT(!f.Client->Settings.ReadFromWriteTime);
        UNIT_ASSERT(f.Client->Settings.RequireWriteTime);
    }
    Y_UNIT_TEST(StreamingAutopartitioningWaitsForCheckpoint) {
        TFixture f;
        f.EnableStreamingAutopartitioning = true;
        f.Init(true);
        const auto error = f.Setup.AsyncInputPromises->FatalError.GetFuture();
        f.Start(1);
        f.Data(0);
        f.Session->Events.emplace_back(TMessageStreamPartitionExhaustedEvent{f.Control});
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT(!f.Finished);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Exhausted, 0);
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(1), saved);
        f.Commit(1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Exhausted, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Ranges.size(), 1);
        UNIT_ASSERT(!error.HasValue());
    }
    Y_UNIT_TEST(StreamingAutopartitioningRequiresOptIn) {
        TFixture f;
        f.Init(true);
        const auto error = f.Setup.AsyncInputPromises->FatalError.GetFuture();
        f.Start(0);
        f.Session->Events.emplace_back(TMessageStreamPartitionExhaustedEvent{f.Control});
        f.Read();
        UNIT_ASSERT(error.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_STRING_CONTAINS(error.GetValue().ToString(), "auto partitioning is not supported");
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Exhausted, 0);
    }
    Y_UNIT_TEST(WatermarksRequireWriteTime) {
        TFixture f;
        f.WatermarksEnabled = true;
        f.Init(true);
        f.Start(std::nullopt);
        f.Data(0);
        UNIT_ASSERT_EXCEPTION(f.Read(), TMessageStreamException);
        UNIT_ASSERT(f.Client->Settings.RequireWriteTime);
    }
    Y_UNIT_TEST(WatermarkIsDeliveredWithTimestampedRecord) {
        TFixture f;
        f.WatermarksEnabled = true;
        f.Init(true);
        f.Start(std::nullopt);
        f.Data(0, "payload", true);
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT(f.Client->Settings.RequireWriteTime);
        UNIT_ASSERT(f.LastWatermark.Defined());
        UNIT_ASSERT_VALUES_EQUAL(*f.LastWatermark, TInstant::Seconds(1));
    }
    Y_UNIT_TEST(RequiredWriteTimeIsValidated) {
        TFixture f;
        f.Init(true, true);
        f.Start(1);
        f.Data(0);
        UNIT_ASSERT_EXCEPTION(f.Read(), TMessageStreamException);
    }
    Y_UNIT_TEST(SnapshotRequiresKnownBound) {
        TFixture f;
        f.Init();
        f.Start(std::nullopt);
        UNIT_ASSERT_EXCEPTION(f.Read(), TMessageStreamException);
    }
    Y_UNIT_TEST(EmptySnapshotFinishes) {
        TFixture f;
        f.Init();
        f.Start(0);
        UNIT_ASSERT(f.Read().empty());
        UNIT_ASSERT(f.Finished);
    }
    Y_UNIT_TEST(NoPollingWithoutFreeSpace) {
        TFixture f;
        f.Init();
        f.Start(1);
        f.Data(0);
        UNIT_ASSERT(f.Read(0).empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Session->Polls, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT(f.Finished);
    }
    Y_UNIT_TEST(StopWaitsForDeliveredDataCheckpoint) {
        TFixture f;
        f.Init(true);
        f.Start(2);
        f.Data(0);
        f.Session->Events.emplace_back(TMessageStreamPartitionStopRequestedEvent{f.Control});
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Stopped, 0);
        UNIT_ASSERT(f.Control->Ranges.empty());
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(1), saved);
        f.Commit(1);
        f.Commit(1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Stopped, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Ranges.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Ranges[0].first, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Ranges[0].second, 1);
    }
    Y_UNIT_TEST_TWIN(ConfirmationIgnoresOtherPartitionCheckpoints, Exhausted) {
        TFixture f;
        f.PartitionIds = {0, 1};
        f.EnableStreamingAutopartitioning = true;
        auto other = std::make_shared<TControl>(1);
        f.Init(true);
        f.Start(3);
        f.Start(other, 3);
        f.Data(0);
        f.Data(other, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 2);
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(1), saved);

        f.Data(other, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        TSourceState later;
        f.Setup.SaveSourceState(CreateCheckpoint(2), later);
        f.Data(other, 2);
        if constexpr (Exhausted) {
            f.Session->Events.emplace_back(TMessageStreamPartitionExhaustedEvent{f.Control});
        } else {
            f.Session->Events.emplace_back(TMessageStreamPartitionStopRequestedEvent{f.Control});
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Stopped + f.Control->Exhausted, 0);
        f.Commit(1);
        f.Commit(1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Stopped, Exhausted ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Exhausted, Exhausted ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Ranges.size(), 1);
        // The other assignment still has both a checkpoint and current work.
        UNIT_ASSERT_VALUES_EQUAL(other->Ranges.size(), 1);
    }
    Y_UNIT_TEST_TWIN(ConfirmationIgnoresOtherPartitionBufferedData, Exhausted) {
        TFixture f;
        f.PartitionIds = {0, 1};
        f.WatermarksEnabled = true;
        f.EnableStreamingAutopartitioning = true;
        auto other = std::make_shared<TControl>(1);
        f.Init(true);
        f.Start(2);
        f.Start(other, 2);
        f.Data(0, "first", true);
        f.Data(other, 0, "second", true);
        // The preceding watermark splits the next record into another batch.
        f.Data(other, 1, "buffered", true);
        if constexpr (Exhausted) {
            f.Session->Events.emplace_back(TMessageStreamPartitionExhaustedEvent{f.Control});
        } else {
            f.Session->Events.emplace_back(TMessageStreamPartitionStopRequestedEvent{f.Control});
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 2);
        UNIT_ASSERT(f.LastWatermark.Defined());
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(1), saved);
        f.Commit(1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Stopped, Exhausted ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Exhausted, Exhausted ? 1 : 0);
        const auto rows = f.Read();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows.front(), "buffered");
    }
    Y_UNIT_TEST(EmptyCheckpointDoesNotDelayStop) {
        TFixture f;
        f.Init(true);
        f.Start(0);
        f.Read();
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(1), saved);
        f.Session->Events.emplace_back(TMessageStreamPartitionStopRequestedEvent{f.Control});
        f.Read();
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Stopped, 1);
    }
    Y_UNIT_TEST(NewAssignmentDoesNotDelayOldAssignmentStop) {
        TFixture f;
        f.Init(true);
        f.Start(1);
        f.Read();
        auto previous = f.Control;
        f.Control = std::make_shared<TControl>();
        f.Start(1);
        f.Data(0);
        f.Session->Events.emplace_back(TMessageStreamPartitionStopRequestedEvent{previous});
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(previous->Stopped, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Stopped, 0);
        UNIT_ASSERT(f.Control->Ranges.empty());
    }
    Y_UNIT_TEST(CommitDoesNotAcknowledgeNewAssignment) {
        TFixture f;
        f.Init(true);
        f.Start(2);
        f.Data(0);
        f.Read();
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(1), saved);
        auto previous = f.Control;
        f.Setup.Execute([&](TFakeActor&) {
            f.Control = std::make_shared<TControl>();
            f.Start(2);
            f.Session->Events.emplace_back(TMessageStreamPartitionClosedEvent{previous});
        });
        f.Read();
        f.Commit(1);
        UNIT_ASSERT_VALUES_EQUAL(previous->Ranges.size(), 1);
        UNIT_ASSERT(f.Control->Ranges.empty());
    }
    Y_UNIT_TEST(SnapshotSessionStaysOpenUntilCommitAndShutdown) {
        TFixture f;
        f.Init();
        f.Start(1);
        f.Data(0);
        f.Session->Events.emplace_back(TMessageStreamPartitionExhaustedEvent{f.Control});
        f.Read();
        UNIT_ASSERT(f.Finished);
        UNIT_ASSERT(!f.Session->Closed);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Exhausted, 0);
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(1), saved);
        f.Commit(1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Exhausted, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Control->Ranges.size(), 1);
        f.Setup.Terminate();
        UNIT_ASSERT(f.Session->Closed);
    }
    Y_UNIT_TEST(SessionCreatedAfterShutdownIsClosed) {
        TFixture f;
        auto promise = NThreading::NewPromise<std::shared_ptr<IMessageStreamReadSession>>();
        f.PendingSession = promise.GetFuture();
        f.Init(true);
        UNIT_ASSERT(f.Read().empty());
        f.Setup.Terminate();
        promise.SetValue(f.Session);
        UNIT_ASSERT(f.Session->Closed);
    }
    Y_UNIT_TEST_TWIN(ReconnectDiscardsPendingSession, Failed) {
        TFixture f;
        auto pending = NThreading::NewPromise<std::shared_ptr<IMessageStreamReadSession>>();
        auto abandoned = std::make_shared<TSession>();
        auto calls = std::make_shared<ui32>(0);
        f.ReconnectPeriod = TDuration::MilliSeconds(100);
        f.SessionFactory = [pending, abandoned, replacement = f.Session, calls]() mutable {
            if (++*calls == 1) {
                return pending.GetFuture();
            }
            if (*calls == 2) {
                if constexpr (Failed) {
                    pending.SetException("abandoned session factory failed");
                } else {
                    pending.SetValue(abandoned);
                    UNIT_ASSERT(abandoned->Closed);
                }
            }
            return NThreading::MakeFuture<std::shared_ptr<IMessageStreamReadSession>>(replacement);
        };
        f.Init(true);
        f.Start(1);
        f.Data(0, "replacement");
        UNIT_ASSERT(f.Read().empty());
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        TVector<TString> rows;
        while (rows.empty()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Reconnect did not start another session factory");
            UNIT_ASSERT(f.DataReady.Wait(TDuration::Seconds(1)));
            rows = f.Read();
        }
        f.Setup.Terminate();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows.front(), "replacement");
        UNIT_ASSERT(*calls >= 2);
        UNIT_ASSERT_VALUES_EQUAL(abandoned->Polls, 0);
        UNIT_ASSERT(f.Session->Closed);
    }
    Y_UNIT_TEST(SessionInitializationFailureIsReported) {
        TFixture f;
        auto promise = NThreading::NewPromise<std::shared_ptr<IMessageStreamReadSession>>();
        f.PendingSession = promise.GetFuture();
        f.Init(true);
        UNIT_ASSERT(f.Read().empty());
        promise.SetException("session initialization failed");
        UNIT_ASSERT_EXCEPTION(f.Read(), yexception);
    }
    Y_UNIT_TEST(CheckpointContainsOnlyDeliveredOffsets) {
        TFixture f;
        f.Init();
        f.Start(2);
        f.Data(0);
        f.Read(0);
        TSourceState empty;
        f.Setup.SaveSourceState(CreateCheckpoint(1), empty);
        UNIT_ASSERT(empty.Data.empty());
        f.Read();
        TSourceState saved;
        f.Setup.SaveSourceState(CreateCheckpoint(2), saved);
        UNIT_ASSERT_VALUES_EQUAL(saved.Data.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(saved.Data.front().Blob, "1");
    }
}
}
