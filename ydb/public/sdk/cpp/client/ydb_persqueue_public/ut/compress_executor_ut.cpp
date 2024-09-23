#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

namespace NYdb::NPersQueue::NTests {

Y_UNIT_TEST_SUITE(CompressExecutor) {
    Y_UNIT_TEST(TestReorderedExecutor) {
        auto queue = std::make_shared<TLockFreeQueue<ui64>>();
        TYdbPqWriterTestHelper helper(TEST_CASE_NAME, queue);

        auto f1 = helper.Write();
        auto f2 = helper.Write();
        f1.Wait(TDuration::Seconds(1));
        UNIT_ASSERT(!f1.HasValue());
        UNIT_ASSERT(!f2.HasValue());

        queue->Enqueue(2);
        f2.Wait(TDuration::Seconds(1));
        UNIT_ASSERT(!f1.HasValue());
        UNIT_ASSERT(!f2.HasValue());
        queue->Enqueue(1);
        Cerr << "Waiting for writes to complete.\n";
        f1.Wait();
        f2.Wait();

    }
    Y_UNIT_TEST(TestExecutorMemUsage) {
        auto queue = std::make_shared<TLockFreeQueue<ui64>>();
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto executor = MakeIntrusive<TYdbPqTestExecutor>(queue);
        auto config = setup->GetWriteSessionSettings();
        auto memUsageLimit = 1_KB;
        config.MaxMemoryUsage(memUsageLimit);
        config.CompressionExecutor(executor);
        auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

        config.RetryPolicy(retryPolicy);

        auto writer = setup->GetPersQueueClient().CreateWriteSession(config);

        TMaybe<TContinuationToken> continueToken;
        auto event = *writer->GetEvent(true);
        UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);

        auto waitEventFuture = writer->WaitEvent();
        if (waitEventFuture.HasValue()) {
            auto event = *writer->GetEvent(true);
            if(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
                Y_ABORT("ANother ready to accept!");
            }
            if(std::holds_alternative<TSessionClosedEvent>(event)) {
                Cerr << "Session closed: " << std::get<TSessionClosedEvent>(event).DebugString() << "\n";
                Y_ABORT("");
            }
        }
        UNIT_ASSERT(!waitEventFuture.HasValue());

        TStringBuilder msgBuilder;
        while (msgBuilder.size() < 100_KB) {
            msgBuilder << "0123456789abcdefghijk";
        }
        const ui64 COMPRESSED_SIZE = 305;
        TString message = TString(msgBuilder);
        ui64 seqNo = 1;
        auto doWrite = [&] {
            writer->Write(std::move(*continueToken), message, seqNo);
            ++seqNo;
        };
        doWrite();
        waitEventFuture = writer->WaitEvent();
        waitEventFuture.Wait(TDuration::Seconds(3));
        UNIT_ASSERT(!waitEventFuture.HasValue());
        queue->Enqueue(1);
        event = *writer->GetEvent(true);
        UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
        event = *writer->GetEvent(true);
        UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event));

        Cerr << "===Will now kick tablets\n";
        setup->KickTablets();
        retryPolicy->ExpectBreakDown();
        retryPolicy->WaitForRetriesSync(1);
        ui64 currentUsageEstimate = 0;
        while (currentUsageEstimate <= memUsageLimit) {
            doWrite();
            queue->Enqueue(seqNo - 1);
            currentUsageEstimate += COMPRESSED_SIZE;
            if (currentUsageEstimate <= memUsageLimit) {
                event = *writer->GetEvent(true);
                UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
                continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
            }
        }
        Cerr << "============ UT - Wait event...\n";
        waitEventFuture = writer->WaitEvent();
        waitEventFuture.Wait(TDuration::Seconds(3));
        UNIT_ASSERT(!waitEventFuture.HasValue());

        writer = nullptr;
        retryPolicy = nullptr;
    }
}
};
