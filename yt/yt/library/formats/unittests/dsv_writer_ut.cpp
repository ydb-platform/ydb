#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/formats/dsv_parser.h>
#include <yt/yt/library/formats/dsv_writer.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NFormats {
namespace {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TDsvWriterTest, StringScalar)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnStringScalar("0-2-xb-1234");
    EXPECT_EQ("0-2-xb-1234", outputStream.Str());
}

TEST(TDsvWriterTest, ListContainingDifferentTypes)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginList();
    consumer.OnListItem();
    consumer.OnInt64Scalar(100);
    consumer.OnListItem();
    consumer.OnStringScalar("foo");
    consumer.OnListItem();
    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("10");
        consumer.OnKeyedItem("b");
        consumer.OnStringScalar("c");
    consumer.OnEndMap();
    consumer.OnEndList();

    TString output =
        "100\n"
        "foo\n"
        "\n"
        "a=10\tb=c\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, ListInsideList)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginList();
    consumer.OnListItem();
    EXPECT_ANY_THROW(consumer.OnBeginList());
}

TEST(TDsvWriterTest, ListInsideMap)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginMap();
    consumer.OnKeyedItem("foo");
    EXPECT_ANY_THROW(consumer.OnBeginList());
}

TEST(TDsvWriterTest, MapInsideMap)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginMap();
    consumer.OnKeyedItem("foo");
    EXPECT_ANY_THROW(consumer.OnBeginMap());
}

TEST(TDsvWriterTest, WithoutEsacping)
{
    auto config = New<TDsvFormatConfig>();
    config->EnableEscaping = false;

    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream, config);

    consumer.OnStringScalar("string_with_\t_\\_=_and_\n");

    TString output = "string_with_\t_\\_=_and_\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, ListUsingOnRaw)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnRaw("[10; 20; 30]", EYsonType::Node);
    TString output =
        "10\n"
        "20\n"
        "30\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, MapUsingOnRaw)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnRaw("{a=b; c=d}", EYsonType::Node);
    TString output = "a=b\tc=d";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDsvWriterTest, SimpleTabular)
{
    auto nameTable = New<TNameTable>();
    auto integerId = nameTable->RegisterName("integer");
    auto stringId = nameTable->RegisterName("string");
    auto doubleId = nameTable->RegisterName("double");
    auto fooId = nameTable->RegisterName("foo");
    auto oneId = nameTable->RegisterName("one");
    auto tableIndexId = nameTable->RegisterName(TableIndexColumnName);
    auto rowIndexId = nameTable->RegisterName(RowIndexColumnName);
    auto rangeIndexId = nameTable->RegisterName(RangeIndexColumnName);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedInt64Value(42, integerId));
    row1.AddValue(MakeUnversionedStringValue("some", stringId));
    row1.AddValue(MakeUnversionedDoubleValue(10., doubleId));
    row1.AddValue(MakeUnversionedInt64Value(2, tableIndexId));
    row1.AddValue(MakeUnversionedInt64Value(42, rowIndexId));
    row1.AddValue(MakeUnversionedInt64Value(1, rangeIndexId));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("bar", fooId));
    row2.AddValue(MakeUnversionedSentinelValue(EValueType::Null, integerId));
    row2.AddValue(MakeUnversionedInt64Value(1, oneId));
    row2.AddValue(MakeUnversionedInt64Value(2, tableIndexId));
    row2.AddValue(MakeUnversionedInt64Value(43, rowIndexId));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow()};

    TStringStream outputStream;
    auto config = New<TDsvFormatConfig>();
    config->EnableTableIndex = true;

    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = true;
    auto writer = CreateSchemalessWriterForDsv(
        config,
        nameTable,
        CreateAsyncAdapter(static_cast<IOutputStream*>(&outputStream)),
        false,
        controlAttributes,
        0);

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();

    TString output =
        "integer=42\tstring=some\tdouble=10.\t@table_index=2\n"
        "foo=bar\tone=1\t@table_index=2\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, AnyTabular)
{
    auto nameTable = New<TNameTable>();
    auto anyId = nameTable->RegisterName("any");

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedAnyValue("[]", anyId));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    TStringStream outputStream;
    auto controlAttributes = New<TControlAttributesConfig>();
    auto writer = CreateSchemalessWriterForDsv(
        New<TDsvFormatConfig>(),
        nameTable,
        CreateAsyncAdapter(static_cast<IOutputStream*>(&outputStream)),
        false,
        controlAttributes,
        0);

    EXPECT_FALSE(writer->Write(rows));
    EXPECT_ANY_THROW(writer->GetReadyEvent().Get().ThrowOnError());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTskvWriterTest, SimpleTabular)
{
    auto nameTable = New<TNameTable>();
    auto id1 = nameTable->RegisterName("id");
    auto id2 = nameTable->RegisterName("guid");
    auto tableIndexId = nameTable->RegisterName(TableIndexColumnName);
    auto rowIndexId = nameTable->RegisterName(RowIndexColumnName);
    auto rangeIndexId = nameTable->RegisterName(RangeIndexColumnName);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedInt64Value(2, tableIndexId));
    row1.AddValue(MakeUnversionedInt64Value(42, rowIndexId));
    row1.AddValue(MakeUnversionedInt64Value(1, rangeIndexId));


    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("1", id1));
    row2.AddValue(MakeUnversionedInt64Value(100500, id2));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("2", id1));
    row3.AddValue(MakeUnversionedInt64Value(20025, id2));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow(), row3.GetRow() };

    TStringStream outputStream;
    auto config = New<TDsvFormatConfig>();
    config->LinePrefix = "tskv";

    auto controlAttributes = New<TControlAttributesConfig>();
    auto writer = CreateSchemalessWriterForDsv(
        config,
        nameTable,
        CreateAsyncAdapter(static_cast<IOutputStream*>(&outputStream)),
        false,
        controlAttributes,
        0);

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();

    TString output =
        "tskv\n"
        "tskv\tid=1\tguid=100500\n"
        "tskv\tid=2\tguid=20025\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TTskvWriterTest, Escaping)
{
    auto key1 = TString("\0 is escaped", 12);

    auto nameTable = New<TNameTable>();
    auto id1 = nameTable->RegisterName(key1);
    auto id2 = nameTable->RegisterName("Escaping in in key: \r \t \n \\ =");

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue(key1, id1));
    row.AddValue(MakeUnversionedStringValue("Escaping in value: \r \t \n \\ =", id2));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    TStringStream outputStream;
    auto config = New<TDsvFormatConfig>();
    config->LinePrefix = "tskv";

    auto controlAttributes = New<TControlAttributesConfig>();
    auto writer = CreateSchemalessWriterForDsv(
        config,
        nameTable,
        CreateAsyncAdapter(static_cast<IOutputStream*>(&outputStream)),
        false,
        controlAttributes,
        0);

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();

    TString output =
        "tskv"
        "\t"

        "\\0 is escaped"
        "="
        "\\0 is escaped"

        "\t"

        "Escaping in in key: \\r \\t \\n \\\\ \\="
        "="
        "Escaping in value: \\r \\t \\n \\\\ =" // Note: = is not escaped

        "\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
