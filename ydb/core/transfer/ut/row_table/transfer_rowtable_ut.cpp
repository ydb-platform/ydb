#include <ydb/core/transfer/ut/common/transfer_common.h>

Y_UNIT_TEST_SUITE(Transfer_RowTable)
{
    const std::string TableType = "ROW";

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


    Y_UNIT_TEST(MessageField_CreateTimestamp)
    {
        MessageField_CreateTimestamp(TableType);
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

    Y_UNIT_TEST(MessageField_WriteTimestamp)
    {
        MessageField_WriteTimestamp(TableType);
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


    Y_UNIT_TEST(DropColumn)
    {
        DropColumn(TableType);
    }

    Y_UNIT_TEST(TableWithSyncIndex) {
        MainTestCase testCase(std::nullopt, TableType);

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    INDEX `title_index` GLOBAL SYNC ON (`Message`),
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:$x._offset,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.Write({"Message-1"});

        testCase.CheckTransferStateError("Only async-indexed tables are supported by BulkUpsert");

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(TableWithAsyncIndex) {
        MainTestCase testCase(std::nullopt, TableType);

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    INDEX `title_index` GLOBAL ASYNC ON (`Message`),
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:$x._offset,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Key", ui64(0)),
            _C("Message", TString("Message-1")),
        }});

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }
}
