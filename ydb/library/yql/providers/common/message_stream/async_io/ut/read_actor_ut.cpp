#include <ydb/library/yql/providers/common/message_stream/async_io/testlib/read_actor_fixture.h>

namespace NYql::NDq::NMessageStreamTest {
Y_UNIT_TEST_SUITE(MessageStreamReadActor) {
    Y_UNIT_TEST(StreamingDoesNotRequireWriteTimeOrEndOffset) {
        TFixture f;
        f.Init(true);
        f.Start(std::nullopt);
        f.Data(0);
        UNIT_ASSERT_VALUES_EQUAL(f.Read().size(), 1);
        UNIT_ASSERT(!f.Finished);
        UNIT_ASSERT(!f.Client->Settings.RequireWriteTime);
        UNIT_ASSERT(!f.Control->Bound);
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
