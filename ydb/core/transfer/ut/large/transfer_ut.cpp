#include <thread>

#include <ydb/core/transfer/ut/common/utils.h>

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(TransferLarge)
{

    auto CreateWriter(MainTestCase& setup, const size_t partitionId) {
        Cerr << "CREATE PARTITION WRITER " << partitionId << Endl << Flush;

        TString producerId = TStringBuilder() << "producer-" << partitionId;

        TWriteSessionSettings writeSettings;
        writeSettings.Path(setup.TopicName);
        writeSettings.PartitionId(partitionId);
        writeSettings.DeduplicationEnabled(true);
        writeSettings.ProducerId(producerId);
        writeSettings.MessageGroupId(producerId);

        TTopicClient client(setup.Driver);
        return client.CreateSimpleBlockingWriteSession(writeSettings);
    }

    void Write(MainTestCase& setup, const size_t partitionId, const size_t messageCount, const size_t messageSize) {
        auto writer = CreateWriter(setup, partitionId);

        Cerr << "PARTITION " << partitionId << " START WRITE " << messageCount << " MESSAGES" << Endl << Flush;

        TString msg(messageSize, '*');
        for (size_t i = 0; i < messageCount;) {
            TWriteMessage m(msg);
            m.SeqNo(i + 1);
            if (writer->Write(std::move(m))) {
                ++i;
            } else {
                Sleep(TDuration::MilliSeconds(100));
                UNIT_ASSERT(false);
            }
        }

        writer->Close(TDuration::Minutes(1));
        Cerr << "PARTITION " << partitionId << " ALL MESSAGES HAVE BEEN WRITEN" << Endl << Flush;
    }

    void CheckAllMessagesHaveBeenWritten(MainTestCase& setup, const size_t expected)  {
        // Check all messages have been writen
        auto d = setup.DescribeTopic();
        for (auto& p : d.GetTopicDescription().GetPartitions()) {
            UNIT_ASSERT_VALUES_EQUAL_C(expected, p.GetPartitionStats()->GetEndOffset(), "Partition " << p.GetPartitionId());
        }
    }

    void WaitAllMessagesHaveBeenCommitted(MainTestCase& setup, const size_t expected, const TDuration timeout = TDuration::Seconds(10)) {
        TInstant endTime = TInstant::Now() + timeout;
        bool allPartitionsHaveBeenCommitted = false;

        while(TInstant::Now() < endTime) {
            auto d = setup.DescribeConsumer();
            auto& p = d.GetConsumerDescription().GetPartitions();
            allPartitionsHaveBeenCommitted = AllOf(p.begin(), p.end(), [&](auto& x) {
                Cerr << "WAIT COMMITTED expected=" << expected
                    << " read=" <<  x.GetPartitionConsumerStats()->GetLastReadOffset()
                    << " committed=" << x.GetPartitionConsumerStats()->GetCommittedOffset() << Endl << Flush;
                return x.GetPartitionConsumerStats()->GetCommittedOffset() == expected;
            });

            if (allPartitionsHaveBeenCommitted) {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(allPartitionsHaveBeenCommitted, "Partitions haven`t been commited to end");
    }

    void CheckSourceTableIsValid(MainTestCase& setup, const size_t partitionCount, size_t expectedOffset) {
        auto r = setup.ExecuteQuery(Sprintf(R"(
            SELECT a.Partition, a.Offset, b.Offset
            FROM %s AS a
                LEFT JOIN %s AS b ON b.Partition = a.Partition AND b.Offset = a.Offset + 1
            WHERE
                b.Offset IS NULL
            ORDER BY
                a.Partition,
                a.Offset
        )", setup.TableName.data(), setup.TableName.data()));

        const auto proto = NYdb::TProtoAccessor::GetProto(r.GetResultSet(0));
        UNIT_ASSERT_VALUES_EQUAL(partitionCount, proto.rows_size());
        for (size_t i = 0; i < (size_t)proto.rows_size(); ++i) {
            auto& row = proto.rows(i);
            auto partition = row.items(0).uint32_value();
            auto offset = row.items(1).uint64_value();

            Cerr << "RESULT PARTITION=" << partition << " OFFSET=" << offset << Endl << Flush;

            UNIT_ASSERT_VALUES_EQUAL(i, partition);
            UNIT_ASSERT_VALUES_EQUAL_C(expectedOffset - 1, offset, "Partition " << i);
        }
    }

    void BigTransfer(const std::string tableType, const size_t partitionCount, const size_t messageCount, const size_t messageSize) {
        MainTestCase testCase(std::nullopt, tableType);
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Partition Uint32 NOT NULL,
                    Offset Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Partition, Offset)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(partitionCount);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Partition:CAST($x._partition AS Uint32),
                            Offset:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Seconds(1), 8_MB));

        std::vector<std::thread> writerThreads;
        writerThreads.reserve(partitionCount);
        for (size_t i = 0; i < partitionCount; ++i) {
            writerThreads.emplace_back([&, i = i]() {
                Write(testCase, i, messageCount, messageSize);
            });
            Sleep(TDuration::MilliSeconds(25));
        }

        for (size_t i = 0; i < partitionCount; ++i) {
            Cerr << "WAIT THREAD " << i << Endl << Flush;
            writerThreads[i].join();
        }

        Cerr << "WAIT REPLICATION FINISHED" << Endl << Flush;

        CheckAllMessagesHaveBeenWritten(testCase, messageCount);
        testCase.CheckReplicationState(TReplicationDescription::EState::Running);
        WaitAllMessagesHaveBeenCommitted(testCase, messageCount);

        CheckSourceTableIsValid(testCase, partitionCount, messageCount);

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(Transfer1KM_1P_ColumnTable)
    {
        BigTransfer("COLUMN", 1, 1000, 64);
    }

    Y_UNIT_TEST(Transfer1KM_1KP_ColumnTable)
    {
        BigTransfer("COLUMN", 1000, 1000, 64);
    }

    Y_UNIT_TEST(Transfer100KM_10P_ColumnTable)
    {
        BigTransfer("COLUMN", 10, 100000, 64);
    }

    Y_UNIT_TEST(Transfer1KM_1P_RowTable)
    {
        BigTransfer("ROW", 1, 1000, 64);
    }

    Y_UNIT_TEST(Transfer1KM_1KP_RowTable)
    {
        BigTransfer("ROW", 1000, 1000, 64);
    }

    Y_UNIT_TEST(Transfer100KM_10P_RowTable)
    {
        BigTransfer("ROW", 10, 100000, 64);
    }

}

