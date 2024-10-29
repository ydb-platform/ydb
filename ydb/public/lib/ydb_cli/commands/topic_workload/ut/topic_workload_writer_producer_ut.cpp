#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer_producer.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_stats_collector.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>

// handle ack event should save statistics
// WaitForContinuationToken should take Continuation token from event
// WaitForContinuationToken should throw exception if event is of wrong type
// Send should put message create ts to the map
// Send should call write method of the writeSession
// Send should set data from generated messages

using namespace NYdb;
using namespace NYdb::NConsoleClient;
using namespace NYdb::NTopic;

namespace NTests {
    Y_UNIT_TEST_SUITE(TTopicWorkloadWriterProducerTests) {
        class MockWriteSession : public IWriteSession {
            public:
                MOCK_METHOD(void, Write, (const TString& data, const TString& continuationToken));
        };
        class TFixture : public NUnitTest::TBaseFixture {
        protected: 
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector = std::make_shared<TTopicWorkloadStatsCollector>();
            std::shared_ptr<MockWriteSession> WriteSession = std::make_shared<MockWriteSession>();
            NUnifiedAgent::TClock Clock = {};

            TTopicWorkloadWriterProducer CreateProducer(NConsoleClient::TTopicWorkloadWriterParams&& params);
            TWriteSessionEvent::TAcksEvent CreateAckEvent(ui64 seqno);
            TTopicWorkloadWriterParams CreateParams();
        };

        TTopicWorkloadWriterProducer TFixture::CreateProducer(NConsoleClient::TTopicWorkloadWriterParams&& params) {
            auto producer = TTopicWorkloadWriterProducer(
                std::move(params),
                StatsCollector,
                "my-test-producer",
                1,
                Clock
            );

            producer.SetWriteSession(WriteSession);

            return producer;
        }

        TWriteSessionEvent::TAcksEvent TFixture::CreateAckEvent(ui64 seqno) {
            return TWriteSessionEvent::TAcksEvent{
                .Acks = TVector<TWriteSessionEvent::TWriteAck>{
                    TWriteSessionEvent::TWriteAck{
                        .SeqNo = seqno,
                        .State = TWriteSessionEvent::TWriteAck::EEventState::EES_WRITTEN,
                        .Details = {},
                        .Stat = {}
                    }}};
        }

        TTopicWorkloadWriterParams TFixture::CreateParams() {
            return NConsoleClient::TTopicWorkloadWriterParams{
                .TotalSec = 60,
                .WarmupSec = 0,
                .Driver = TDriver({}),
                .Log = {},
                .StatsCollector = {},
                .ErrorFlag = {},
                .StartedCount = 0,
                .GeneratedMessages = {},
                .Database = {},
                .TopicName = "my-test-topic",
                .BytesPerSec = 100'000,
                .MessageSize = 100'000,
                .ProducerThreadCount = 1,
                .ProducersPerThread = 1,
                .WriterIdx = 0,
                .PartitionCount = 100,
                .PartitionSeed = 123,
                .Direct = false,
                .Codec = {},
                .UseTransactions = true,
                .UseAutoPartitioning = false,
                .CommitPeriodMs = 100,
                .CommitMessages = 0
            };
        };

        Y_UNIT_TEST_F(HandleAckEvent_ShouldSaveStatistics, TFixture) {
            auto params = CreateParams();
            auto producer = CreateProducer(std::move(params));
            auto mockNow = TInstant::MilliSeconds(1730111051000);
            Clock.SetBase(mockNow);
            Clock.Configure();
            auto createTime = TInstant::MilliSeconds(1730111050000);
            producer.Send(createTime, {});

            auto ackEvent = CreateAckEvent(1);
            producer.HandleAckEvent(ackEvent);

            ::testing::internal::CaptureStdout();
            StatsCollector->PrintTotalStats();
            std::string output = ::testing::internal::GetCapturedStdout();

            UNIT_ASSERT_STRINGS_EQUAL(
                output,
                ""
            );
        }

        Y_UNIT_TEST_F(WaitForContinuationToken_ShouldExtractContinuationTokenFromEvent, TFixture) {
            auto params = CreateParams();
            auto producer = CreateProducer(std::move(params));
            UNIT_ASSERT(!producer.ContinuationTokenDefined());
            ON_CALL(WriteSession, WaitEvent(_)).WillByDefault(Return(NThreading::MakeFuture()));
            
            producer.WaitForContinuationToken(TDuration::Zero());

            UNIT_ASSERT(producer.ContinuationTokenDefined());
        }
    }
}
