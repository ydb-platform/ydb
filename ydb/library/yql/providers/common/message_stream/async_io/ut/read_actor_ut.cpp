#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/providers/common/message_stream/async_io/testlib/read_actor_fixture.h>

namespace NFq::NMessageStream::NTest {
Y_UNIT_TEST_SUITE(MessageStreamReadActor) {
    Y_UNIT_TEST(AllocatorIsRequired) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            CreateMessageStreamReadActor(TMessageStreamReadActorSettings{}, std::make_unique<TState>()),
            yexception,
            "Message stream read actor requires an allocator");
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
