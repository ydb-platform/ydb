#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/library/formats/yamred_dsv_parser.h>

namespace NYT::NFormats {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvParserTest, Simple)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key_a"));
        EXPECT_CALL(Mock, OnStringScalar("1"));
        EXPECT_CALL(Mock, OnKeyedItem("key_b"));
        EXPECT_CALL(Mock, OnStringScalar("2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey_x"));
        EXPECT_CALL(Mock, OnStringScalar("3"));
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("6"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key_a"));
        EXPECT_CALL(Mock, OnStringScalar("7"));
        EXPECT_CALL(Mock, OnKeyedItem("key_b"));
        EXPECT_CALL(Mock, OnStringScalar("8"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey_x"));
        EXPECT_CALL(Mock, OnStringScalar("9"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("max\tignat"));
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("100"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "1 2\t3\ta=5\tb=6\n"
        "7 8\t9\tb=max\\tignat\ta=100\n";

    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    config->SubkeyColumnNames.push_back("subkey_x");

    ParseYamredDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvParserTest, EmptyField)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("0 1"));
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("b"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "\t0 1\ta=b\n";

    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key");
    config->SubkeyColumnNames.push_back("subkey");

    ParseYamredDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvParserTest, Escaping)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("\t"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("0\n1"));
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("\tb\nc"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "\\t\t0\\n1\ta=\\tb\\nc\n";

    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = true;
    config->EnableEscaping = true;
    config->KeyColumnNames.push_back("key");
    config->SubkeyColumnNames.push_back("subkey");

    ParseYamredDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvParserTest, Lenval)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("a"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("bc"));
        EXPECT_CALL(Mock, OnKeyedItem("d"));
        EXPECT_CALL(Mock, OnStringScalar("e"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = TString(
        "\x01\x00\x00\x00" "a"
        "\x02\x00\x00\x00" "bc"
        "\x03\x00\x00\x00" "d=e",
        3 * 4 + 1 + 2 + 3);

    auto config = New<TYamredDsvFormatConfig>();
    config->Lenval = true;
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key");
    config->SubkeyColumnNames.push_back("subkey");

    ParseYamredDsv(input, &Mock, config);
}

TEST(TYamredDsvParserTest, EOM)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("a"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("bc"));
        EXPECT_CALL(Mock, OnKeyedItem("d"));
        EXPECT_CALL(Mock, OnStringScalar("e"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = TString(
        "\x01\x00\x00\x00" "a"
        "\x02\x00\x00\x00" "bc"
        "\x03\x00\x00\x00" "d=e"
        "\xfb\xff\xff\xff" "\x01\x00\x00\x00\x00\x00\x00\x00",
        3 * 4 + 1 + 2 + 3 + 12);

    auto config = New<TYamredDsvFormatConfig>();
    config->Lenval = true;
    config->EnableEom = true;
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key");
    config->SubkeyColumnNames.push_back("subkey");

    ParseYamredDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
