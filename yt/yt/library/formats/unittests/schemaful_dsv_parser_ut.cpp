#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/library/formats/schemaful_dsv_parser.h>

#include <yt/yt/core/yson/null_consumer.h>

namespace NYT::NFormats {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulDsvParserTest, Simple)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("6"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("100"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("max\tignat"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "5\t6\n"
        "100\tmax\\tignat\n";

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a", "b"};

    ParseSchemafulDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulDsvParserTest, TableIndex)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("x"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(0));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("y"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("z"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "1\tx\n"
        "0\ty\n"
        "0\tz\n";

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a"};
    config->EnableTableIndex = true;

    ParseSchemafulDsv(input, &Mock, config);
}

TEST(TSchemafulDsvParserTest, TooManyRows)
{
    TString input = "5\t6\n";

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a"};

    EXPECT_THROW({ ParseSchemafulDsv(input, GetNullYsonConsumer(), config); }, std::exception);
}

TEST(TSchemafulDsvParserTest, SpecialSymbols)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    auto value = TString("6\0", 2);
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5\r"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar(value));
    EXPECT_CALL(Mock, OnEndMap());

    TString input("5\r\t6\0\n", 6);

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a", "b"};

    ParseSchemafulDsv(input, &Mock, config);
}

TEST(TSchemafulDsvParserTest, EnabledEscaping)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    auto value = TString("6\0", 2);
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5\r\r"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar(value));
    EXPECT_CALL(Mock, OnEndMap());

    TString input("5\r\\r\t6\0\n", 8);

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a", "b"};
    config->EnableEscaping = true;

    ParseSchemafulDsv(input, &Mock, config);
}

TEST(TSchemafulDsvParserTest, DisabledEscaping)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    auto value = TString("6\0", 2);
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5\r\\r"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar(value));
    EXPECT_CALL(Mock, OnEndMap());

    TString input("5\r\\r\t6\0\n", 8);

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a", "b"};
    config->EnableEscaping = false;

    ParseSchemafulDsv(input, &Mock, config);
}

TEST(TSchemafulDsvParserTest, ColumnsNamesHeader)
{
    TString input("a\tb\n1\t2\n");

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a", "b"};
    config->EnableColumnNamesHeader = true;

    EXPECT_THROW(ParseSchemafulDsv(input, GetNullYsonConsumer(), config), std::exception);
}

TEST(TSchemafulDsvParserTest, MissingValueModePrintSentinel)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    TString input = "x\t\tz\n";
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("x"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("c"));
        EXPECT_CALL(Mock, OnStringScalar("z"));
    EXPECT_CALL(Mock, OnEndMap());

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a", "b", "c"};
    // By default missing_value_mode = fail and no sentinel values are used,
    // i. e. there is no way to represent YSON entity with this format.

    ParseSchemafulDsv(input, &Mock, config);

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("x"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnEntity());
        EXPECT_CALL(Mock, OnKeyedItem("c"));
        EXPECT_CALL(Mock, OnStringScalar("z"));
    EXPECT_CALL(Mock, OnEndMap());

    config->MissingValueMode = EMissingSchemafulDsvValueMode::PrintSentinel;
    // By default missing_value_sentinel = "".

    ParseSchemafulDsv(input, &Mock, config);

    input = "null\tNULL\t\n";

    config->MissingValueSentinel = "NULL";

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("null"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnEntity());
        EXPECT_CALL(Mock, OnKeyedItem("c"));
        EXPECT_CALL(Mock, OnStringScalar(""));
    EXPECT_CALL(Mock, OnEndMap());

    ParseSchemafulDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
