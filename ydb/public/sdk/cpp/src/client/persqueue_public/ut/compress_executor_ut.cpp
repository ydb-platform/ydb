#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h>

#include <atomic>

namespace NYdb::NPersQueue::NTests {
namespace {

TContinuationToken WaitForWriteToken(IWriteSession& session) {
    while (true) {
        UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(30)), "timeout waiting for write token");
        for (auto& event : session.GetEvents()) {
            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                return std::move(ready->ContinuationToken);
            }
            if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                UNIT_FAIL("write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(CompressExecutor) {
    Y_UNIT_TEST(PreparedWritesUseCompressionExecutor) {
        for (const bool preEncoded : {false, true}) {
            TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
            auto executor = NTopic::NTests::CreateThreadPoolManagedExecutor(1);
            auto settings = setup.GetWriteSessionSettings();
            settings.Codec(preEncoded ? ECodec::GZIP : ECodec::RAW)
                .BatchFlushInterval(TDuration::Zero())
                .CompressionExecutor(executor);
            auto writer = setup.GetPersQueueClient().CreateWriteSession(settings);

            ui64 seqNo = 1;
            for (const std::string_view data : {"first", "second"}) {
                auto token = WaitForWriteToken(*writer);
                if (preEncoded) {
                    writer->WriteEncoded(std::move(token), data, ECodec::RAW, static_cast<ui32>(data.size()), seqNo);
                } else {
                    writer->Write(std::move(token), data, seqNo);
                }
                ++seqNo;
            }
            UNIT_ASSERT_VALUES_EQUAL(executor->GetPlannedCount(), 1);

            executor->RunAllTasks();
            ui64 nextSeqNo = 1;
            while (nextSeqNo <= 2) {
                UNIT_ASSERT_C(writer->WaitEvent().Wait(TDuration::Seconds(30)), "timeout waiting for write acknowledgements");
                for (auto& event : writer->GetEvents()) {
                    if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                        for (const auto& ack : acks->Acks) {
                            UNIT_ASSERT_VALUES_EQUAL(ack.SeqNo, nextSeqNo++);
                            UNIT_ASSERT_VALUES_EQUAL(ack.State, TWriteSessionEvent::TWriteAck::EES_WRITTEN);
                        }
                    } else if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                        UNIT_FAIL("write session closed unexpectedly: " << closed->GetIssues().ToString());
                    }
                }
            }
            UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
        }
    }

    Y_UNIT_TEST(CloseWithQueuedRawSend) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        auto executor = NTopic::NTests::CreateThreadPoolManagedExecutor(1);
        auto settings = setup.GetWriteSessionSettings();
        settings.Codec(ECodec::RAW)
            .BatchFlushInterval(TDuration::Zero())
            .CompressionExecutor(executor);
        auto writer = setup.GetPersQueueClient().CreateWriteSession(settings);
        writer->Write(WaitForWriteToken(*writer), "message");
        UNIT_ASSERT_VALUES_EQUAL(executor->GetPlannedCount(), 1);

        UNIT_ASSERT(!writer->Close(TDuration::Zero()));
        writer.reset();

        auto drained = NThreading::NewPromise<void>();
        executor->Post([drained]() mutable {
            drained.SetValue();
        });
        executor->RunAllTasks();
        UNIT_ASSERT_C(drained.GetFuture().Wait(TDuration::Seconds(30)), "executor did not drain after session destruction");
    }

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
        auto executor = std::make_shared<TYdbPqTestExecutor>(queue);
        auto config = setup->GetWriteSessionSettings();
        auto memUsageLimit = 1_KB;
        config.MaxMemoryUsage(memUsageLimit);
        config.CompressionExecutor(executor);
        auto counters = MakeIntrusive<TWriterCounters>(new ::NMonitoring::TDynamicCounters());
        config.Counters(counters);
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
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), message.size());
        waitEventFuture = writer->WaitEvent();
        waitEventFuture.Wait(TDuration::Seconds(3));
        UNIT_ASSERT(!waitEventFuture.HasValue());
        queue->Enqueue(1);
        event = *writer->GetEvent(true);
        auto ackEvent = *writer->GetEvent(true);
        // The ACK can arrive before the transport completion releases memory.
        if (std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event)) {
            std::swap(event, ackEvent);
        }
        UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
        UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TAcksEvent>(ackEvent));

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
    Y_UNIT_TEST(TestSyncCompressionReentrantWrite) {
        auto firstToken = NThreading::NewPromise<TContinuationToken>();
        auto writeDone = NThreading::NewPromise<void>();
        std::atomic<size_t> readyCount{0};
        std::shared_ptr<IWriteSession> writer;
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        auto config = setup.GetWriteSessionSettings();
        config.Codec(ECodec::GZIP).MaxMemoryUsage(10_KB).BatchFlushInterval(TDuration::Zero());
        config.CompressionExecutor(CreateSyncExecutor());
        config.EventHandlers_.HandlersExecutor(CreateSyncExecutor());
        config.EventHandlers_.ReadyToAcceptHandler([&](TWriteSessionEvent::TReadyToAcceptEvent& event) {
            const auto index = readyCount.fetch_add(1);
            if (index == 0) {
                firstToken.SetValue(std::move(event.ContinuationToken));
            } else if (index == 1) {
                writer->Write(std::move(event.ContinuationToken), "next");
                writeDone.SetValue();
            }
        });

        writer = setup.GetPersQueueClient().CreateWriteSession(config);
        UNIT_ASSERT(firstToken.GetFuture().Wait(TDuration::Seconds(10)));
        writer->Write(firstToken.GetFuture().ExtractValueSync(), TString(100_KB, 'x'));
        UNIT_ASSERT(writeDone.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(10)));
    }
}
};
