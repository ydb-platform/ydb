#include <ydb/core/transfer/ut/common/transfer_common.h>

Y_UNIT_TEST_SUITE(Transfer_ColumnTable)
{
    const std::string TableType = "COLUMN";

    Y_UNIT_TEST(KeyColumnFirst)
    {
        KeyColumnFirst(TableType);
    }

    Y_UNIT_TEST(KeyColumnLast)
    {
        KeyColumnLast(TableType);
    }

    Y_UNIT_TEST(ComplexKey)
    {
        ComplexKey(TableType);
    }


    Y_UNIT_TEST(NullableColumn)
    {
        NullableColumn(TableType);
    }

    Y_UNIT_TEST(WriteNullToKeyColumn)
    {
        WriteNullToKeyColumn(TableType);
    }

    Y_UNIT_TEST(WriteNullToColumn)
    {
        WriteNullToColumn(TableType);
    }


    Y_UNIT_TEST(Upsert_DifferentBatch)
    {
        Upsert_DifferentBatch(TableType);
    }

    Y_UNIT_TEST(Upsert_OneBatch)
    {
        Upsert_OneBatch(TableType);
    }


    Y_UNIT_TEST(ColumnType_Date)
    {
        ColumnType_Date(TableType);
    }

    Y_UNIT_TEST(ColumnType_Double)
    {
        ColumnType_Double(TableType);
    }

    Y_UNIT_TEST(ColumnType_Utf8_LongValue)
    {
        ColumnType_Utf8_LongValue(TableType);
    }


    Y_UNIT_TEST(MessageField_Partition)
    {
        MessageField_Partition(TableType);
    }

    Y_UNIT_TEST(MessageField_SeqNo)
    {
        MessageField_SeqNo(TableType);
    }

    Y_UNIT_TEST(MessageField_ProducerId)
    {
        MessageField_ProducerId(TableType);
    }

    Y_UNIT_TEST(MessageField_MessageGroupId)
    {
        MessageField_ProducerId(TableType);
    }


    Y_UNIT_TEST(ProcessingJsonMessage)
    {
        ProcessingJsonMessage(TableType);
    }

    Y_UNIT_TEST(ProcessingCDCMessage)
    {
        ProcessingCDCMessage(TableType);
    }

    Y_UNIT_TEST(ProcessingTargetTable)
    {
        ProcessingTargetTable(TableType);
    }

    Y_UNIT_TEST(ProcessingTargetTableOtherType)
    {
        ProcessingTargetTableOtherType(TableType);
    }

    void BigBatchSize(bool local) {
        MainTestCase testCase(std::nullopt, TableType);

        testCase.CreateTable(R"(
                CREATE TABLE %s (
                    partition_id Uint32 NOT NULL,
                    offset Uint64 NOT NULL,
                    line Uint32 NOT NULL,
                    message String,
                    PRIMARY KEY (partition_id, offset, line)
                )
                PARTITION BY HASH(partition_id, offset)
                WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);

        TString big(512_KB, '-');
        auto settings = MainTestCase::CreateTransferSettings::WithLocalTopic(local);
        settings.BatchSizeBytes = 1_GB;
        testCase.CreateTransfer(Sprintf(R"(
                $l = ($x) -> {
                    $lines = ListEnumerate(String::SplitToList($x._data, "\n"));

                    $m = ($line) -> {
                        return <|
                            partition_id: $x._partition,
                            offset: $x._offset,
                            line: CAST($line.0 AS Uint32),
                            message: $line.1 || '%s'
                        |>;
                    };

                    return ListMap($lines, $m);
                };
            )", big.data()), settings);

        const size_t PartCont = 900;

        TStringBuilder msg;
        for (size_t i = 0; i < PartCont; ++i) {
            msg << i << '\n';
        }

        // the first message is less than the limit, and the second message is less than the limit, but both of them are more
        testCase.Write({msg});
        testCase.Write({msg});

        testCase.CheckTransferState(TTransferDescription::EState::Running);
        testCase.CheckCommittedOffset(0, 2, TDuration::Seconds(30));

        TExpectations expectations;
        for (size_t msg = 0; msg < 2; ++msg) {
            for (size_t i = 0; i <= PartCont; ++i) {
                expectations.push_back({
                    _C("offset", ui64{msg}),
                    _C("line", ui32(i)),
                });
            }
        }

        testCase.CheckResult(expectations);

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(BigBatchSize_Remote) {
        BigBatchSize(false);
    }

    Y_UNIT_TEST(BigBatchSize_Local) {
        BigBatchSize(true);
    }
}
