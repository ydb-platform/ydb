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

    Y_UNIT_TEST(ColumnType_Int8)
    {
        ColumnType_Int8(TableType);
    }

    Y_UNIT_TEST(ColumnType_Int16)
    {
        ColumnType_Int8(TableType);
    }

    Y_UNIT_TEST(ColumnType_Int32)
    {
        ColumnType_Int32(TableType);
    }

    Y_UNIT_TEST(ColumnType_Int64)
    {
        ColumnType_Int64(TableType);
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
}
