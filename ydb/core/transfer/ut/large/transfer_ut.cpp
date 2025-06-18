#include <thread>

#include <util/generic/guid.h>
#include <ydb/core/transfer/ut/common/utils.h>

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(TransferLarge)
{

    auto CreateWriter(MainTestCase& setup, const size_t writerId) {
        Cerr << "CREATE PARTITION WRITER " << writerId << Endl << Flush;

        TString producerId = TStringBuilder() << "writer-" << writerId << "-" << CreateGuidAsString();

        TWriteSessionSettings writeSettings;
        writeSettings.Path(setup.TopicName);
        writeSettings.DeduplicationEnabled(true);
        writeSettings.ProducerId(producerId);
        writeSettings.MessageGroupId(producerId);

        TTopicClient client(setup.Driver);
        return client.CreateSimpleBlockingWriteSession(writeSettings);
    }

    void Write(MainTestCase& setup, const size_t writerId, const size_t messageCount, const size_t messageSize) {
        auto writer = CreateWriter(setup, writerId);

        Cerr << "PARTITION " << writerId << " START WRITE " << messageCount << " MESSAGES" << Endl << Flush;

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

        //writer->Close(TDuration::Minutes(1));
        writer->Close();
        Cerr << "PARTITION " << writerId << " ALL MESSAGES HAVE BEEN WRITTEN" << Endl << Flush;
    }

    void WaitAllMessagesHaveBeenCommitted(MainTestCase& setup, size_t expected, const TDuration timeout = TDuration::Seconds(10)) {
        TInstant endTime = TInstant::Now() + timeout;

        bool allPartitionsHaveBeenCommitted = false;

        while(TInstant::Now() < endTime) {
            std::map<size_t, size_t> offsets;
            {
                auto d = setup.DescribeTopic();
                for (auto& p : d.GetTopicDescription().GetPartitions()) {
                    offsets[p.GetPartitionId()] = p.GetPartitionStats()->GetEndOffset();
                }
            }

            size_t messages = 0;
            for (auto& [partitionId, count] : offsets) {
                Cerr << "PARTITION " << partitionId << " END OFFSET " << count << Endl << Flush;
                messages += count;
            }

            Cerr << "ALL MESSAGES " << messages << " EXPECTED " << expected << Endl << Flush;
            UNIT_ASSERT_VALUES_EQUAL(expected, messages);

            auto d = setup.DescribeConsumer();
            auto& p = d.GetConsumerDescription().GetPartitions();
            allPartitionsHaveBeenCommitted = AllOf(p.begin(), p.end(), [&](auto& x) {
                Cerr << "WAIT COMMITTED partition=" << x.GetPartitionId() 
                    << " expected=" << offsets[x.GetPartitionId()]
                    << " read=" <<  x.GetPartitionConsumerStats()->GetLastReadOffset()
                    << " committed=" << x.GetPartitionConsumerStats()->GetCommittedOffset() << Endl << Flush;
                return x.GetPartitionConsumerStats()->GetCommittedOffset() == offsets[x.GetPartitionId()];
            });

            if (allPartitionsHaveBeenCommitted) {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(allPartitionsHaveBeenCommitted, "Partitions haven`t been commited to end");
    }

    void CheckSourceTableIsValid(MainTestCase& setup) {
        std::map<size_t, size_t> offsets;
        auto d = setup.DescribeTopic();
        for (auto& p : d.GetTopicDescription().GetPartitions()) {
            offsets[p.GetPartitionId()] = p.GetPartitionStats()->GetEndOffset();
        }

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
        for (size_t i = 0; i < (size_t)proto.rows_size(); ++i) {
            auto& row = proto.rows(i);
            auto partition = row.items(0).uint32_value();
            auto offset = row.items(1).uint64_value();

            Cerr << "RESULT PARTITION=" << partition << " OFFSET=" << offset << Endl << Flush;
            UNIT_ASSERT_VALUES_EQUAL_C(offsets[partition] - 1, offset, "Partition " << i);
        }
    }

    void BigTransfer(const std::string tableType, const size_t threadsCount, const size_t messageCount, const size_t messageSize, bool autopartitioning, bool localTopic = false) {
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
        if (autopartitioning) {
            testCase.CreateTopic({
                .MinPartitionCount = std::max<ui64>(1, threadsCount >> 4),
                .MaxPartitionCount = threadsCount << 2,
                .AutoPartitioningEnabled = true
            });
        } else {
            testCase.CreateTopic(threadsCount);

        }

        auto settings = MainTestCase::CreateTransferSettings::WithBatching(TDuration::Seconds(1), 8_MB);
        settings.LocalTopic = localTopic;

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
            )", settings);

        std::vector<std::thread> writerThreads;
        writerThreads.reserve(threadsCount);
        for (size_t i = 0; i < threadsCount; ++i) {
            writerThreads.emplace_back([&, i = i]() {
                Write(testCase, i, messageCount, messageSize);
            });
            Sleep(TDuration::MilliSeconds(25));
        }

        for (size_t i = 0; i < threadsCount; ++i) {
            Cerr << "WAIT THREAD " << i << Endl << Flush;
            writerThreads[i].join();
        }

        Cerr << "WAIT REPLICATION FINISHED" << Endl << Flush;

        Sleep(TDuration::Seconds(3));

        testCase.CheckReplicationState(TReplicationDescription::EState::Running);
        Cerr << "WaitAllMessagesHaveBeenCommitted" << Endl << Flush;
        WaitAllMessagesHaveBeenCommitted(testCase, messageCount * threadsCount);

        CheckSourceTableIsValid(testCase);

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    //
    // Topic autopartitioning is disabled
    //

    Y_UNIT_TEST(Transfer1KM_1P_ColumnTable)
    {
        BigTransfer("COLUMN", 1, 1000, 64, false);
    }

    Y_UNIT_TEST(Transfer1KM_1KP_ColumnTable)
    {
        BigTransfer("COLUMN", 1000, 1000, 64, false);
    }

    Y_UNIT_TEST(Transfer100KM_10P_ColumnTable)
    {
        BigTransfer("COLUMN", 10, 100000, 64, false);
    }

    Y_UNIT_TEST(Transfer1KM_1P_RowTable)
    {
        BigTransfer("ROW", 1, 1000, 64, false);
    }

    Y_UNIT_TEST(Transfer1KM_1KP_RowTable)
    {
        BigTransfer("ROW", 1000, 1000, 64, false);
    }

    Y_UNIT_TEST(Transfer100KM_10P_RowTable)
    {
        BigTransfer("ROW", 10, 100000, 64, false);
    }

    //
    // Topic autopartitioning is enabled
    //

    Y_UNIT_TEST(Transfer1KM_1P_ColumnTable_TopicAutoPartitioning)
    {
        BigTransfer("COLUMN", 1, 1000, 64, true);
    }

    Y_UNIT_TEST(Transfer1KM_1KP_ColumnTable_TopicAutoPartitioning)
    {
        BigTransfer("COLUMN", 1000, 1000, 64, true);
    }

    Y_UNIT_TEST(Transfer100KM_10P_ColumnTable_TopicAutoPartitioning)
    {
        BigTransfer("COLUMN", 10, 100000, 64, true);
    }

    Y_UNIT_TEST(Transfer1KM_1P_RowTable_TopicAutoPartitioning)
    {
        BigTransfer("ROW", 1, 1000, 64, true);
    }

    Y_UNIT_TEST(Transfer1KM_1KP_RowTable_TopicAutoPartitioning)
    {
        BigTransfer("ROW", 1000, 1000, 64, true);
    }

    Y_UNIT_TEST(Transfer100KM_10P_RowTable_TopicAutoPartitioning)
    {
        BigTransfer("ROW", 10, 100000, 64, true);
    }

    //
    // LocalRead
    //

    Y_UNIT_TEST(Transfer100KM_10P_LocalRead)
    {
        BigTransfer("ROW", 10, 100000, 64, false, true);
    }

    Y_UNIT_TEST(Transfer100KM_10P_LocalRead_TopicAutoPartitioning)
    {
        BigTransfer("ROW", 10, 100000, 64, true, true);
    }

}

