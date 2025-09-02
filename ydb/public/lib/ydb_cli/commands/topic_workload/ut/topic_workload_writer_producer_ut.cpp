#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer_producer.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_stats_collector.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/logger/stream.h>

using namespace NYdb;
using namespace NYdb::NConsoleClient;
using namespace NYdb::NTopic;
using ::testing::_;

namespace NTests {
    Y_UNIT_TEST_SUITE(TTopicWorkloadWriterProducerTests) {
        class MockWriteSession : public IWriteSession {
            public:
                MOCK_METHOD(NThreading::TFuture<void>, WaitEvent, (), (override));

                MOCK_METHOD(std::optional<TWriteSessionEvent::TEvent>, GetEvent, (bool block), (override));

                MOCK_METHOD(std::vector<TWriteSessionEvent::TEvent>, GetEvents, (bool block, std::optional<size_t> maxEventsCount), (override));

                MOCK_METHOD(NThreading::TFuture<uint64_t>, GetInitSeqNo, (), (override));

                MOCK_METHOD(void, Write, (TContinuationToken&& continuationToken, TWriteMessage&& message, TTransactionBase* tx), (override));

                MOCK_METHOD(void, Write, (TContinuationToken&& continuationToken, std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp), (override));

                MOCK_METHOD(void, WriteEncoded, (TContinuationToken&& continuationToken, TWriteMessage&& params, TTransactionBase* tx), (override));

                MOCK_METHOD(void, WriteEncoded, (TContinuationToken&& continuationToken, std::string_view data, ECodec codec, uint32_t originalSize, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp), (override));

                MOCK_METHOD(bool, Close, (TDuration closeTimeout), (override));

                MOCK_METHOD(TWriterCounters::TPtr, GetCounters, (), (override));
        };

        class MockContinuationTokenIssuer : public TContinuationTokenIssuer {
            public:
                static TContinuationToken IssueContinuationToken() {
                    return TContinuationTokenIssuer::IssueContinuationToken();
                }
        };


        class TFixture : public NUnitTest::TBaseFixture {
        protected:
            TFixture() :
                WriteSession(std::make_shared<MockWriteSession>()),
                Clock(CreateClock()),
                Log(CreateLogger()),
                GeneratedMessages(TTopicWorkloadWriterWorker::GenerateMessages(100'000))
            {}
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector = std::make_shared<TTopicWorkloadStatsCollector>(
                // we need error flag = true, cause without it, stats collector will go into the infinite loop
                // during the call to PrintWindowStatsLoop. And this call is the only way to deque write events to
                // statisitcs.
                1, 1, false, false, 5, 60, 0, 99, std::make_shared<std::atomic_bool>(true), false
            );
            std::shared_ptr<MockWriteSession> WriteSession;
            NUnifiedAgent::TClock Clock;
            TStringStream LoggedData;
            std::shared_ptr<TLog> Log;
            std::vector<TString> GeneratedMessages;

            THolder<TTopicWorkloadWriterProducer> CreateProducer();
            TWriteSessionEvent::TAcksEvent CreateAckEvent(ui64 seqno);
            TTopicWorkloadWriterParams CreateParams();
            std::shared_ptr<TLog> CreateLogger();
            NUnifiedAgent::TClock CreateClock();
            void InitContinuationToken(TTopicWorkloadWriterProducer& producer);
        };

        THolder<TTopicWorkloadWriterProducer> TFixture::CreateProducer() {
            auto producer = MakeHolder<TTopicWorkloadWriterProducer>(
                std::move(CreateParams()),
                StatsCollector,
                "my-test-producer",
                1,
                Clock
            );

            producer->SetWriteSession(WriteSession);

            return producer;
        }

        TWriteSessionEvent::TAcksEvent TFixture::CreateAckEvent(ui64 seqno) {
            TWriteStat::TPtr writeStat = new TWriteStat{};

            writeStat->WriteTime = TDuration::Zero();
            writeStat->MinTimeInPartitionQueue = TDuration::Zero();
            writeStat->MaxTimeInPartitionQueue = TDuration::Zero();
            writeStat->PartitionQuotedTime = TDuration::Zero();
            writeStat->TopicQuotedTime = TDuration::Zero();

            return TWriteSessionEvent::TAcksEvent{
                .Acks = TVector<TWriteSessionEvent::TWriteAck>{
                    TWriteSessionEvent::TWriteAck{
                        .SeqNo = seqno,
                        .State = TWriteSessionEvent::TWriteAck::EEventState::EES_WRITTEN,
                        .Details = TWriteSessionEvent::TWriteAck::TWrittenMessageDetails{
                            .Offset = 1,
                            .PartitionId = 1
                        },
                        .Stat = writeStat
                    }}};
        }

        TTopicWorkloadWriterParams TFixture::CreateParams() {
            size_t messageSize = 100'000;

            return TTopicWorkloadWriterParams{
                .TotalSec = 60,
                .WarmupSec = 0,
                .Driver = TDriver({}),
                .Log = Log,
                .StatsCollector = {},
                .ErrorFlag = std::make_shared<std::atomic_bool>(false),
                .StartedCount = 0,
                .GeneratedMessages = GeneratedMessages,
                .Database = {},
                .TopicName = "my-test-topic",
                .BytesPerSec = 100'000,
                .MessageSize = messageSize,
                .ProducerThreadCount = 1,
                .WriterIdx = 0,
                .PartitionCount = 100,
                .PartitionSeed = 123,
                .Direct = false,
                .Codec = {},
                .UseTransactions = true,
                .UseAutoPartitioning = false,
                .CommitIntervalMs = 100,
                .CommitMessages = 0
            };
        }

        std::shared_ptr<TLog> TFixture::CreateLogger() {
            TLog log(THolder(new TStreamLogBackend(&LoggedData)));
            return std::make_shared<TLog>(log);
        }

        NUnifiedAgent::TClock TFixture::CreateClock() {
            NUnifiedAgent::TClock clock;
            if (!clock.Configured()) {
                clock.Configure();
            }
            return clock;
        }

        void TFixture::InitContinuationToken(TTopicWorkloadWriterProducer& producer) {
            auto continuationToken = MockContinuationTokenIssuer::IssueContinuationToken();
            std::optional<TWriteSessionEvent::TEvent> event = std::variant<
                    TWriteSessionEvent::TAcksEvent,
                    TWriteSessionEvent::TReadyToAcceptEvent,
                    TSessionClosedEvent
                    >(TWriteSessionEvent::TReadyToAcceptEvent(std::move(continuationToken)));

            ON_CALL(*WriteSession, GetEvent(_)).WillByDefault(testing::Return(event));
            ON_CALL(*WriteSession, WaitEvent()).WillByDefault(testing::Return(NThreading::MakeFuture()));

            producer.WaitForContinuationToken(TDuration::Zero());
        }

        Y_UNIT_TEST_F(HandleAckEvent_ShouldSaveStatistics, TFixture) {
            auto producer = CreateProducer();
            auto mockNow = TInstant::MilliSeconds(1730111051000);
            Clock.SetBase(mockNow);
            InitContinuationToken(*producer);
            auto createTime = TInstant::MilliSeconds(1730111050000);
            producer->Send(createTime, {});
            UNIT_ASSERT_EQUAL(0, StatsCollector->GetTotalWriteMessages());
            // We need this call, cause only this call initializes StatsCollector.WarmupTime.
            // If it is not initialized, no events will be accepted.
            // Both here and below it will not go into the loop, cause we provided errorFlag into the constructor above.
            StatsCollector->PrintWindowStatsLoop();

            auto ackEvent = CreateAckEvent(1);
            producer->HandleAckEvent(ackEvent);

            // We need this call, cause only this way collector will deque wrtie events to statisitcs.
            StatsCollector->PrintWindowStatsLoop();
            UNIT_ASSERT_VALUES_EQUAL(1, StatsCollector->GetTotalWriteMessages());
        }

        Y_UNIT_TEST_F(Send_ShouldCallWriteMethodOfTheWriteSession, TFixture) {
            // arrange
            auto producer = CreateProducer();
            uint64_t initSeqNo = 0;
            ON_CALL(*WriteSession, GetInitSeqNo()).WillByDefault(testing::Return(NThreading::MakeFuture(initSeqNo)));
            producer->WaitForInitSeqNo();
            InitContinuationToken(*producer);
            auto createTs = TInstant::MilliSeconds(1730111050000);

            // assert
            EXPECT_CALL(*WriteSession, Write(
                _,
                testing::AllOf(
                    // first message id is initSeqNo + 1
                    testing::Field(&TWriteMessage::Data, GeneratedMessages[initSeqNo + 1]),
                    testing::Field(&TWriteMessage::SeqNo_, initSeqNo + 1),
                    testing::Field(&TWriteMessage::CreateTimestamp_, createTs)
                ),
                _
                ));

            // act
            producer->Send(createTs, {});
        }

        Y_UNIT_TEST_F(WaitForContinuationToken_ShouldExtractContinuationTokenFromEvent, TFixture) {
            auto producer = CreateProducer();
            UNIT_ASSERT(!producer->ContinuationTokenDefined());

            InitContinuationToken(*producer);

            UNIT_ASSERT(producer->ContinuationTokenDefined());
        }

        Y_UNIT_TEST_F(WaitForContinuationToken_ShouldThrowExceptionIfEventOfTheWrongType, TFixture) {
            auto producer = CreateProducer();
            std::optional<TWriteSessionEvent::TEvent> event = std::variant<
                    TWriteSessionEvent::TAcksEvent,
                    TWriteSessionEvent::TReadyToAcceptEvent,
                    TSessionClosedEvent
                    >(CreateAckEvent(123));
            ON_CALL(*WriteSession, GetEvent(_)).WillByDefault(testing::Return(event));
            ON_CALL(*WriteSession, WaitEvent()).WillByDefault(testing::Return(NThreading::MakeFuture()));

            UNIT_ASSERT_EXCEPTION(producer->WaitForContinuationToken(TDuration::Zero()), yexception);
        }
    }
}
