#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_keyed_writer_producer.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer_worker_common.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/logger/stream.h>

using namespace NYdb;
using namespace NYdb::NConsoleClient;
using namespace NYdb::NTopic;

namespace NTests {

Y_UNIT_TEST_SUITE(TTopicWorkloadKeyedWriterProducerTests) {
    class MockProducer : public IProducer {
    public:
        MOCK_METHOD(TWriteResult, Write, (TWriteMessage&& message), (override));
        MOCK_METHOD(NThreading::TFuture<TFlushResult>, Flush, (), (override));
        MOCK_METHOD(TCloseResult, Close, (TDuration closeTimeout), (override));
        MOCK_METHOD(TWriteStats, GetWriteStats, (), (override));
    };

    class TFixture : public NUnitTest::TBaseFixture {
    protected:
        TFixture()
            : StatsCollector(std::make_shared<TTopicWorkloadStatsCollector>(
                1, 1, false, false, 5, 60, 0, 99, std::make_shared<std::atomic_bool>(true), false))
            , GeneratedMessages(TTopicWorkloadWriterWorker::GenerateMessages(128))
        {
            if (!Clock.Configured()) {
                Clock.Configure();
            }
        }

        TDriver Driver{TDriverConfig{}};
        std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;
        std::shared_ptr<std::atomic_bool> ErrorFlag = std::make_shared<std::atomic_bool>(false);
        std::vector<TString> GeneratedMessages;
        TStringStream LoggedData;
        NUnifiedAgent::TClock Clock;

        TTopicWorkloadKeyedWriterParams CreateParams() {
            auto log = std::make_shared<TLog>(THolder(new TStreamLogBackend(&LoggedData)));
            TTopicWorkloadWriterParams base{
                .TotalSec = 60,
                .WarmupSec = 0,
                .Driver = Driver,
                .Log = std::move(log),
                .StatsCollector = StatsCollector,
                .ErrorFlag = ErrorFlag,
                .StartedCount = {},
                .GeneratedMessages = GeneratedMessages,
                .Database = {},
                .TopicName = "my-test-topic",
                .BytesPerSec = 0,
                .MessageSize = 128,
                .ProducerThreadCount = 1,
                .WriterIdx = 0,
                .PartitionCount = 1,
                .PartitionSeed = 1,
                .Direct = false,
                .Codec = {},
                .UseTransactions = true,
                .CommitIntervalMs = 100,
                .CommitMessages = 100,
            };
            return TTopicWorkloadKeyedWriterParams{base};
        }

        std::shared_ptr<TTopicWorkloadKeyedWriterProducer> CreateProducer(const std::shared_ptr<IProducer>& producer) {
            auto params = CreateParams();
            auto keyed = std::make_shared<TTopicWorkloadKeyedWriterProducer>(
                params,
                StatsCollector,
                "keyed-producer",
                "session",
                Clock);
            keyed->SetProducer(producer);
            return keyed;
        }
    };

    Y_UNIT_TEST_F(Send_DropsInflightWhenWriteIsNotQueued, TFixture) {
        auto mock = std::make_shared<MockProducer>();
        EXPECT_CALL(*mock, Write(testing::_)).WillOnce(testing::Return(TWriteResult{
            .Status = EWriteStatus::Error,
        }));

        auto producer = CreateProducer(mock);
        producer->Send(TInstant::Now(), nullptr);

        UNIT_ASSERT_VALUES_EQUAL(0, producer->InflightMessagesCnt());
    }

    Y_UNIT_TEST_F(Ack_DrainsInflightCount, TFixture) {
        auto mock = std::make_shared<MockProducer>();
        EXPECT_CALL(*mock, Write(testing::_)).WillOnce(testing::Return(TWriteResult{
            .Status = EWriteStatus::Queued,
        }));

        auto producer = CreateProducer(mock);
        producer->Send(TInstant::Now(), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(1, producer->InflightMessagesCnt());

        TWriteSessionEvent::TAcksEvent ackEvent{
            .Acks = {
                TWriteSessionEvent::TWriteAck{
                    .SeqNo = 1,
                    .State = TWriteSessionEvent::TWriteAck::EES_WRITTEN,
                },
            },
        };
        producer->HandleAckEvent(ackEvent);

        UNIT_ASSERT_VALUES_EQUAL(0, producer->InflightMessagesCnt());
    }

    Y_UNIT_TEST_F(SessionClosed_StopsWriterWhenWriteWasQueued, TFixture) {
        auto mock = std::make_shared<MockProducer>();
        EXPECT_CALL(*mock, Write(testing::_)).WillOnce(testing::Return(TWriteResult{
            .Status = EWriteStatus::Queued,
        }));

        auto producer = CreateProducer(mock);
        producer->Send(TInstant::Now(), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(1, producer->InflightMessagesCnt());
        UNIT_ASSERT(!*ErrorFlag);

        TSessionClosedEvent event(EStatus::SESSION_EXPIRED, NYdb::NIssue::TIssues());
        producer->HandleSessionClosed(event);

        UNIT_ASSERT(*ErrorFlag);
        UNIT_ASSERT_VALUES_EQUAL(1, producer->InflightMessagesCnt());
    }

    Y_UNIT_TEST_F(TryCommitTx_WaitsUntilInflightDrains, TFixture) {
        auto params = CreateParams();
        std::optional<TTransactionSupport> txSupport;
        txSupport.emplace(Driver, "", "");
        txSupport->AppendRow("");

        TInstant commitTime = TInstant::Zero();
        bool waitForCommitTx = false;
        NTopicWorkloadWriterInternal::TryCommitTxCommon(
            params,
            txSupport,
            commitTime,
            waitForCommitTx,
            [](const TInstant&, const TInstant&, size_t) {},
            [] {
                return false;
            });

        UNIT_ASSERT(waitForCommitTx);
        UNIT_ASSERT_VALUES_EQUAL(commitTime, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(1, txSupport->Rows.size());
    }

    Y_UNIT_TEST_F(TryCommitTx_CommitsWhenReadyAndNoRows, TFixture) {
        auto params = CreateParams();
        std::optional<TTransactionSupport> txSupport;
        txSupport.emplace(Driver, "", "");

        TInstant commitTime = TInstant::Zero();
        bool waitForCommitTx = true;
        NTopicWorkloadWriterInternal::TryCommitTxCommon(
            params,
            txSupport,
            commitTime,
            waitForCommitTx,
            [](const TInstant&, const TInstant&, size_t) {},
            [] {
                return true;
            });

        UNIT_ASSERT(!waitForCommitTx);
        UNIT_ASSERT_VALUES_EQUAL(commitTime, TInstant::Zero() + TDuration::MilliSeconds(params.CommitIntervalMs));
        UNIT_ASSERT(txSupport->Rows.empty());
    }
}

} // namespace NTests
