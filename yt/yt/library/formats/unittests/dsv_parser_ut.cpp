#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/library/formats/dsv_parser.h>

namespace NYT::NFormats {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TDsvParserTest, Simple)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("integer"));
        EXPECT_CALL(Mock, OnStringScalar("42"));
        EXPECT_CALL(Mock, OnKeyedItem("string"));
        EXPECT_CALL(Mock, OnStringScalar("some"));
        EXPECT_CALL(Mock, OnKeyedItem("double"));
        EXPECT_CALL(Mock, OnStringScalar("10"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
        EXPECT_CALL(Mock, OnKeyedItem("one"));
        EXPECT_CALL(Mock, OnStringScalar("1"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "integer=42\tstring=some\tdouble=10\n"
        "foo=bar\tone=1\n";
    ParseDsv(input, &Mock);
}

TEST(TDsvParserTest, EmptyInput)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    TString input = "";
    ParseDsv(input, &Mock);
}

TEST(TDsvParserTest, BinaryData)
{
    StrictMock<TMockYsonConsumer> Mock;

    auto a = TString("\0\0\0\0", 4);
    auto b = TString("\x80\0\x16\xC8", 4);

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("ntr"));
        EXPECT_CALL(Mock, OnStringScalar(a));
        EXPECT_CALL(Mock, OnKeyedItem("xrp"));
        EXPECT_CALL(Mock, OnStringScalar(b));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "ntr=\\0\\0\\0\\0\txrp=\x80\\0\x16\xC8\n";
    ParseDsv(input, &Mock);
}

TEST(TDsvParserTest, EmptyRecord)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "\n";
    ParseDsv(input, &Mock);
}

TEST(TDsvParserTest, EmptyRecords)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "\n\n";
    ParseDsv(input, &Mock);
}

TEST(TDsvParserTest, EmptyKeysAndValues)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem(""));
        EXPECT_CALL(Mock, OnStringScalar(""));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "=\n";
    ParseDsv(input, &Mock);
}

TEST(TDsvParserTest, UnescapedZeroInInput)
{
    StrictMock<TMockYsonConsumer> Mock;

    TString input = TString("a\0b=v", 5);
    EXPECT_ANY_THROW({
        ParseDsv(input, &Mock);
    });
}

TEST(TDsvParserTest, ZerosAreNotTerminals)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    TString key = TString("a\0b", 3);
    TString value = TString("c\0d", 3);

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem(key));
        EXPECT_CALL(Mock, OnStringScalar(value));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "a\\0b=c\\0d\n";
    ParseDsv(input, &Mock);
}

TEST(TDsvParserTest, UnterminatedRecord)
{
    NiceMock<TMockYsonConsumer> Mock;

    TString input = "a=b";
    EXPECT_ANY_THROW({
        ParseDsv(input, &Mock);
    });
}

////////////////////////////////////////////////////////////////////////////////

class TTskvParserTest: public ::testing::Test
{
public:
    StrictMock<TMockYsonConsumer> Mock;
    NiceMock<TMockYsonConsumer> ErrorMock;

    TDsvFormatConfigPtr Config;

    void SetUp() override {
        Config = New<TDsvFormatConfig>();
        Config->LinePrefix = "tskv";
    }
};

TEST_F(TTskvParserTest, Simple)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("id"));
        EXPECT_CALL(Mock, OnStringScalar("1"));
        EXPECT_CALL(Mock, OnKeyedItem("guid"));
        EXPECT_CALL(Mock, OnStringScalar("100500"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("id"));
        EXPECT_CALL(Mock, OnStringScalar("2"));
        EXPECT_CALL(Mock, OnKeyedItem("guid"));
        EXPECT_CALL(Mock, OnStringScalar("20025"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "tskv\n"
        "tskv\tid=1\tguid=100500\t\n"
        "tskv\tid=2\tguid=20025\n";
    ParseDsv(input, &Mock, Config);
}

TEST_F(TTskvParserTest, SimpleWithNewLine)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "tskv\tfoo=bar\n";
    ParseDsv(input, &Mock, Config);
}

TEST_F(TTskvParserTest, Escaping)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a=b"));
        EXPECT_CALL(Mock, OnStringScalar("c=d or e=f"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key_with_\t,\r_and_\n"));
        EXPECT_CALL(Mock, OnStringScalar("value_with_\t,\\_and_\r\n"));
        EXPECT_CALL(Mock, OnKeyedItem("another_key"));
        EXPECT_CALL(Mock, OnStringScalar("another_value"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "t\\s\\kv\n"
        "tskv" "\t" "a\\=b"  "="  "c\\=d or e=f" "\n" // Note: unescaping is less strict
        "tskv" "\t"
        "key_with_\\t,\r_and_\\n"
        "="
        "value_with_\\t,\\\\_and_\\r\\n"
        "\t"
        "an\\other_\\key=anoth\\er_v\\alue"
        "\n";

    ParseDsv(input, &Mock, Config);
}

TEST_F(TTskvParserTest, DisabledEscaping)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a\\"));
        EXPECT_CALL(Mock, OnStringScalar("b\\t=c\\=d or e=f\\0"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "tskv\t\\x\\y\n"
        "tskv" "\t" "a\\=b\\t"  "="  "c\\=d or e=f\\0" "\n";

    Config->EnableEscaping = false;

    ParseDsv(input, &Mock, Config);
}

TEST_F(TTskvParserTest, AllowedUnescapedSymbols)
{
    Config->LinePrefix = "prefix_with_=";

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("just_key"));
        EXPECT_CALL(Mock, OnStringScalar("value_with_="));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "prefix_with_=" "\t" "just_key" "=" "value_with_=" "\n";
    ParseDsv(input, &Mock, Config);
}

TEST_F(TTskvParserTest, UndefinedValues)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("b"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "tskv" "\t" "tskv" "\t" "tskv" "\n"
        "tskv\t" "some_key" "\t\t\t" "a=b" "\t" "another_key" "\n" // Note: consequent \t
        "tskv\n";
    ParseDsv(input, &Mock, Config);
}


TEST_F(TTskvParserTest, OnlyLinePrefix)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "tskv\n";
    ParseDsv(input, &Mock, Config);
}

TEST_F(TTskvParserTest, OnlyLinePrefixAndTab)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "tskv\t\n";
    ParseDsv(input, &Mock, Config);
}

TEST_F(TTskvParserTest, NotFinishedLinePrefix)
{
    TString input = "tsk";

    EXPECT_ANY_THROW({
        ParseDsv(input, &ErrorMock, Config);
    });
}

TEST_F(TTskvParserTest, WrongLinePrefix)
{
    TString input =
        "tskv\ta=b\n"
        "tZkv\tc=d\te=f\n"
        "tskv\ta=b\n";

    EXPECT_ANY_THROW({
        ParseDsv(input, &ErrorMock, Config);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDriver
