#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/library/formats/yamr_parser.h>

#include <yt/yt/core/yson/null_consumer.h>

namespace NYT::NFormats {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TYamrParserTest, Simple)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(2));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "key1\tvalue1\n"
        "2\n"
        "key2\tvalue2\n";

    ParseYamr(input, &Mock);
}

TEST(TYamrParserTest, ValueWithTabs)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar(TStringBuf("key1\0", 5)));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value with \t and some other"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar(TStringBuf("another\0 value with \t", 21)));
    EXPECT_CALL(Mock, OnEndMap());

    TString input(
        "key1\0\tvalue with \t and some other\n"
        "key2\tanother\0 value with \t\n",
        34 +
        27);

    ParseYamr(input, &Mock);
}

TEST(TYamrParserTest, SimpleWithSubkey)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "key1\tsubkey1\tvalue1\n"
        "key2\tsubkey2\tvalue2\n";

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrParserTest, IncompleteRows)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar(""));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "key1\tsubkey1\tvalue1\n"
        "key\tsubkey\n"
        "key2\tsubkey2\tvalue2\n";

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrParserTest, IncorrectIncompleteRows)
{
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = false;

    EXPECT_THROW(ParseYamr("\n", GetNullYsonConsumer(), config), std::exception);
    EXPECT_THROW(ParseYamr("key\n", GetNullYsonConsumer(), config), std::exception);
    EXPECT_THROW(ParseYamr("key\tvalue\nkey\n", GetNullYsonConsumer(), config), std::exception);
}

TEST(TYamrParserTest, TabsInValue)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("a\tb\\tc\t"));
    EXPECT_CALL(Mock, OnEndMap());

    auto config = New<TYamrFormatConfig>();
    TString input = "key\ta\tb\\tc\t";
    ParseYamr(input, &Mock, config);
}

TEST(TYamrParserTest, Escaping)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("\tkey\t"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("\n"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("a\tb\t\n"));
    EXPECT_CALL(Mock, OnEndMap());

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->EnableEscaping = true;

    TString input = "\\tkey\\t\t\\n\ta\tb\t\\n\n";
    ParseYamr(input, &Mock, config);
}

TEST(TYamrParserTest, CustomSeparators)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    auto config = New<TYamrFormatConfig>();
    config->RecordSeparator = 'Y';
    config->FieldSeparator = 'X';

    TString input = "keyXvalueYkey2Xvalue2Y";
    ParseYamr(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamrLenvalParserTest, Simple)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2",

        2 * (2 * 4 + 4 + 6) + 8); // all i32 + lengths of keys

    auto config = New<TYamrFormatConfig>();
    config->Lenval = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrLenvalParserTest, SimpleWithSubkey)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = TString(
        "\x04\x00\x00\x00" "key1"
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x07\x00\x00\x00" "subkey2"
        "\x06\x00\x00\x00" "value2",

        2 * (3 * 4 + 4 + 7 + 6)); // all i32 + lengths of keys

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrLenvalParserTest, EmptyFields)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar(""));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = TString(
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00",
        3 * 4);

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrLenvalParserTest, HugeLength)
{
    TString input = TString(
        "\xFF\xFF\xFF\xFF"
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00",
        3 * 4);

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    EXPECT_THROW(ParseYamr(input, GetNullYsonConsumer(), config), std::exception);
}

TEST(TYamrLenvalParserTest, SimpleEndOfMessage)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"

        "\xfb\xff\xff\xff" "\x02\x00\x00\x00\x00\x00\x00\x00",

        2 * (2 * 4 + 4 + 6) + 8 + 12); // all i32 + lengths of keys

    auto config = New<TYamrFormatConfig>();
    config->Lenval = true;
    config->EnableEom = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrLenvalParserTest, EmptyFieldsWithEOM)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar(""));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = TString(
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\xfb\xff\xff\xff" "\x01\x00\x00\x00\x00\x00\x00\x00",
        3 * 4 + 12);

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;
    config->EnableEom = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrParserTest, IncorrectPlaceOfEOM)
{
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = false;
    config->Lenval = true;
    config->EnableEom = true;

    TString input1 = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\xfb\xff\xff\xff" "\x02\x00\x00\x00\x00\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2",

        2 * (2 * 4 + 4 + 6) + 8 + 12); // all i32 + lengths of keys

    TString input2 = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"

        "\xfb\xff\xff\xff" "\x02\x00\x00\x00\x00\x00\x00\x00"

        "\x06\x00\x00\x00" "value2",

        2 * (2 * 4 + 4 + 6) + 8 + 12); // all i32 + lengths of keys

    EXPECT_THROW(ParseYamr(input1, GetNullYsonConsumer(), config), std::exception);
    EXPECT_THROW(ParseYamr(input2, GetNullYsonConsumer(), config), std::exception);
}

TEST(TYamrParserTest, IncorrectEOM)
{
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = false;
    config->Lenval = true;
    config->EnableEom = true;

    // Garbage after EOM marker
    TString input1 = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\xfb\xff\xff\xff" "\x01\x00\x00\x00\x00\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2",

        2 * (2 * 4 + 4 + 6) + 8 + 12); // all i32 + lengths of keys

    // Row count mismatch
    TString input2 = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"

        "\xfb\xff\xff\xff" "\x03\x00\x00\x00\x00\x00\x00\x00",

        2 * (2 * 4 + 4 + 6) + 8 + 12); // all i32 + lengths of keys

    // Missing EOM marker
    TString input3 = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2",

        2 * (2 * 4 + 4 + 6) + 8); // all i32 + lengths of keys

    // Missing EOM marker with empty fields
    TString input4 = TString(
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00",
        3 * 4);

    EXPECT_THROW(ParseYamr(input1, GetNullYsonConsumer(), config), std::exception);
    EXPECT_THROW(ParseYamr(input2, GetNullYsonConsumer(), config), std::exception);
    EXPECT_THROW(ParseYamr(input3, GetNullYsonConsumer(), config), std::exception);
    EXPECT_THROW(ParseYamr(input4, GetNullYsonConsumer(), config), std::exception);
}

TEST(TYamrParserTest, UnsupportedEOMInTextMode)
{
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = false;
    config->Lenval = false;
    config->EnableEom = true;

    TString input = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"


        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"

        "\xfb\xff\xff\xff" "\x02\x00\x00\x00\x00\x00\x00\x00",

        2 * (2 * 4 + 4 + 6) + 8 + 12); // all i32 + lengths of keys

    EXPECT_THROW(ParseYamr(input, GetNullYsonConsumer(), config), std::exception);
}

TEST(TYamrParserTest, UnexpectedEOM)
{
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = false;
    config->Lenval = true;
    config->EnableEom = false;

    TString input = TString(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"

        "\xfb\xff\xff\xff" "\x02\x00\x00\x00\x00\x00\x00\x00",

        2 * (2 * 4 + 4 + 6) + 8 + 12); // all i32 + lengths of keys

    EXPECT_THROW(ParseYamr(input, GetNullYsonConsumer(), config), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
