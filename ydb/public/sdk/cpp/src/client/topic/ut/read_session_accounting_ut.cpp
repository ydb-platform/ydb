#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.ipp>
#include <ydb/public/sdk/cpp/src/client/topic/common/executor_impl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic {

    Y_UNIT_TEST_SUITE(TReadSessionDecompressionAccounting) {
        Y_UNIT_TEST(CleanupAndDecompressionReleaseSameMessage) {
            TReadSessionSettings settings;
            auto counters = MakeIntrusive<TReaderCounters>();
            MakeCountersNotNull(*counters);
            settings.MaxMemoryUsageBytes(1_MB).Counters(counters);

            auto events = std::make_shared<TReadSessionEventsQueue<false>>(settings);
            auto context = MakeWithCallbackContext<TSingleClusterReadSessionImpl<false>>(
                settings, "", "read-session", "", TLog{}, nullptr, events, nullptr, 1, 1);
            auto session = context->TryGet();
            auto partition = MakeIntrusive<TPartitionStreamImpl<false>>(
                ui64{1}, "topic", "read-session", 0, 1, 0, std::nullopt, context);

            TPartitionData<false> data;
            auto* batch = data.add_batches();
            batch->set_codec(Ydb::Topic::CODEC_RAW);
            batch->add_message_data()->set_data(std::string(10, 'a'));
            batch->add_message_data()->set_data(std::string(20, 'b'));
            auto info = std::make_shared<TDataDecompressionInfo<false>>(
                std::move(data), context, true);

            {
                TDeferredActions<false> actions;
                UNIT_ASSERT(info->PlanDecompressionTasks(1.0, partition, actions));
            }

            auto queue = partition->ExtractQueue();
            UNIT_ASSERT_VALUES_EQUAL(queue.size(), 2);
            TRawPartitionStreamEvent<false> first(std::move(queue.front()));
            queue.pop_front();
            TRawPartitionStreamEvent<false> second(std::move(queue.front()));
            queue.pop_front();
            queue.emplace_back(std::move(first));

            {
                TDeferredActions<false> actions;
                const auto estimatedSize = info->StartDecompressionTasks(
                    std::make_shared<TSyncExecutor>(), 1_MB, actions);
                UNIT_ASSERT_VALUES_EQUAL(estimatedSize, 30);
                // StartDecompressionTasksImpl normally reserves this estimated size.
                session->OnDataDecompressed(0, 0, estimatedSize, 0);

                // Cleanup marks the first message abandoned while the task is pending.
                queue.Cleanup(actions);
            } // The task releases all 30 bytes claimed by cleanup.

            UNIT_ASSERT(second.IsReady());
            queue.emplace_back(std::move(second));
            {
                TDeferredActions<false> actions;
                // Cleanup must not release this message again after the worker released the task.
                queue.Cleanup(actions);
            }
        }

        // Regression test for YDBBUGS-1002. An unrecoverable read error takes
        // the OnReadDone -> AbortSession(closeEvent) path, which drains the
        // events queue and defers OnUserRetrievedEvent for undelivered ready
        // data events. When the same bytes were concurrently released by a
        // user-side retrieval, the session accounting must not underflow.
        Y_UNIT_TEST(AbortSessionDrainingReadyEventsDoesNotPanicAfterConcurrentRelease) {
            TReadSessionSettings settings;
            auto counters = MakeIntrusive<TReaderCounters>();
            MakeCountersNotNull(*counters);
            settings.MaxMemoryUsageBytes(1_MB).Counters(counters);

            auto events = std::make_shared<TReadSessionEventsQueue<false>>(settings);
            auto context = MakeWithCallbackContext<TSingleClusterReadSessionImpl<false>>(
                settings, "", "read-session", "", TLog{}, nullptr, events, nullptr, 1, 1);
            auto session = context->TryGet();
            auto partition = MakeIntrusive<TPartitionStreamImpl<false>>(
                ui64{1}, "topic", "read-session", 0, 1, 0, std::nullopt, context);

            TPartitionData<false> data;
            auto* batch = data.add_batches();
            batch->set_codec(Ydb::Topic::CODEC_RAW);
            batch->add_message_data()->set_data(std::string(10, 'a'));
            batch->add_message_data()->set_data(std::string(20, 'b'));
            auto info = std::make_shared<TDataDecompressionInfo<false>>(
                std::move(data), context, true);

            {
                TDeferredActions<false> actions;
                UNIT_ASSERT(info->PlanDecompressionTasks(1.0, partition, actions));
            }

            {
                TDeferredActions<false> actions;
                const auto estimatedSize = info->StartDecompressionTasks(
                    std::make_shared<TSyncExecutor>(), 1_MB, actions);
                UNIT_ASSERT_VALUES_EQUAL(estimatedSize, 30);
                // StartDecompressionTasksImpl normally reserves this estimated size.
                session->OnDataDecompressed(0, 0, estimatedSize, 2);
            }

            // Move the ready data events into the session events queue,
            // as the session does when decompression completes.
            events->SignalReadyEvents(partition);

            // The user concurrently retrieved and released the same 30 bytes
            // (the GetEvents path), so the session has nothing left to release.
            session->OnUserRetrievedEvent(30, 2);

            // Unrecoverable stream error on the gRPC thread:
            // OnReadDone -> AbortSession(closeEvent). The queue cleanup must
            // not release the same bytes a second time.
            session->AbortSession(EStatus::ABORTED, "Test abort");

            // The user still receives the session closed event.
            auto closeEvent = events->GetEvent();
            UNIT_ASSERT(closeEvent.has_value());
            UNIT_ASSERT(std::holds_alternative<TSessionClosedEvent>(*closeEvent));
        }
    } // Y_UNIT_TEST_SUITE(TReadSessionDecompressionAccounting)

} // namespace NYdb::inline Dev::NTopic
