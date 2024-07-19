#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/core/json/json_parser.h>

namespace NYT::NJson {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

inline TString SurroundWithQuotes(const TString& s)
{
    TString quote = "\"";
    return quote + s + quote;
}

// Basic types:
TEST(TJsonParserTest, List)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(1));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("aaa"));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(3.5)));
    EXPECT_CALL(Mock, OnEndList());

    TString input = "[1,\"aaa\",3.5]";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Map)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("hello"));
        EXPECT_CALL(Mock, OnStringScalar("world"));
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "{\"hello\":\"world\",\"foo\":\"bar\"}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Integer1)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnInt64Scalar(1ll << 62));

    TString input = "4611686018427387904";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Integer2)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnInt64Scalar(std::numeric_limits<i64>::max()));

    TString input = "9223372036854775807";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, UnsignedInteger)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnUint64Scalar((1ull << 63) + (1ull << 62)));

    TString input = "13835058055282163712";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, UnsignedInteger2)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnUint64Scalar((1ull << 63)));

    TString input = "9223372036854775808";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Infinity)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnDoubleScalar(std::numeric_limits<double>::infinity()));

    TString input = "inf";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, MinusInfinity)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnDoubleScalar(-std::numeric_limits<double>::infinity()));

    TString input = "-inf";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, IncorrectInfinity)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    TString input = "[0, -in, 1.0]";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, UnsignedInteger3)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnUint64Scalar(std::numeric_limits<ui64>::max()));

    TString input = "18446744073709551615";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Entity)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnEntity());

    TString input = "null";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, EmptyString)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnStringScalar(""));

    TString input = SurroundWithQuotes("");

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}


TEST(TJsonParserTest, OutOfRangeUnicodeSymbols)
{
    StrictMock<TMockYsonConsumer> Mock;

    TString input = SurroundWithQuotes("\\u0100");
    TStringInput stream(input);

    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, EscapedUnicodeSymbols)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    TString s = TString("\x80\n\xFF", 3);
    EXPECT_CALL(Mock, OnStringScalar(s));

    TString input = SurroundWithQuotes("\\u0080\\u000A\\u00FF");

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Boolean)
{
    StrictMock<TMockYsonConsumer> Mock;
    TString input = "true";

    EXPECT_CALL(Mock, OnBooleanScalar(true));

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, InvalidJson)
{
    StrictMock<TMockYsonConsumer> Mock;
    TString input = "{\"hello\" = \"world\"}"; // YSon style instead of json

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, Embedded)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnKeyedItem("foo"));
            EXPECT_CALL(Mock, OnStringScalar("bar"));
        EXPECT_CALL(Mock, OnEndMap());
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnBeginList());
            EXPECT_CALL(Mock, OnListItem());
            EXPECT_CALL(Mock, OnInt64Scalar(1));
        EXPECT_CALL(Mock, OnEndList());
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "{"
            "\"a\":{\"foo\":\"bar\"}"
            ","
            "\"b\":[1]"
        "}";
    TStringInput stream(input);

    ParseJson(&stream, &Mock);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJsonParserPlainTest, Simple)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(1));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("aaa"));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(3.5)));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnEntity());
        EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnEndList());

    TString input = "[1,\"aaa\",3.5,{\"a\": null}]";

    TStringInput stream(input);

    auto config = New<TJsonFormatConfig>();
    config->Plain = true;

    ParseJson(&stream, &Mock, config);
}

TEST(TJsonParserPlainTest, Incorrect)
{
    InSequence dummy;

    StrictMock<TMockYsonConsumer> Mock;
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("$attributes"));
        EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnKeyedItem("foo"));
            EXPECT_CALL(Mock, OnStringScalar("bar"));
        EXPECT_CALL(Mock, OnEndMap());
        EXPECT_CALL(Mock, OnKeyedItem("$value"));
        EXPECT_CALL(Mock, OnBeginList());
            EXPECT_CALL(Mock, OnListItem());
            EXPECT_CALL(Mock, OnInt64Scalar(1));
        EXPECT_CALL(Mock, OnEndList());
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":[1]"
        "}";
    TStringInput stream(input);

    auto config = New<TJsonFormatConfig>();
    config->Plain = true;

    ParseJson(&stream, &Mock, config);
}

TEST(TJsonParserPlainTest, ListFragment)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("hello"));
        EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";

    auto config = New<TJsonFormatConfig>();
    config->Plain = true;

    TStringInput stream(input);
    ParseJson(&stream, &Mock, config, EYsonType::ListFragment);
}

////////////////////////////////////////////////////////////////////////////////

// Values with attributes:
TEST(TJsonParserTest, ListWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndList());

    TString input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":[1]"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, MapWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("spam"));
        EXPECT_CALL(Mock, OnStringScalar("bad"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":{\"spam\":\"bad\"}"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Int64WithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnInt64Scalar(42));

    TString input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":42"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, EntityWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnEntity());

    TString input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":null"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, StringWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnStringScalar("some_string"));

    TString input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":\"some_string\""
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, DoubleAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnBeginAttributes());
            EXPECT_CALL(Mock, OnKeyedItem("another_foo"));
            EXPECT_CALL(Mock, OnStringScalar("another_bar"));
        EXPECT_CALL(Mock, OnEndAttributes());
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnStringScalar("some_string"));

    TString input =
        "{"
            "\"$attributes\":{\"foo\":"
                "{"
                    "\"$attributes\":{\"another_foo\":\"another_bar\"}"
                    ","
                    "\"$value\":\"bar\"}"
                "}"
            ","
            "\"$value\":\"some_string\""
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, AnnotateWithTypes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(42));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnUint64Scalar(123));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(71.6)));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnBooleanScalar(true));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnBooleanScalar(false));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("aaa"));

        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(42));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnUint64Scalar(123));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(71.6)));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnBooleanScalar(true));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnBooleanScalar(false));

        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(89));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnUint64Scalar(89));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(89)));
    EXPECT_CALL(Mock, OnEndList());

    TString input =
        "["
            "{\"$value\":\"42\",\"$type\":\"int64\"},"
            "{\"$value\":\"123\",\"$type\":\"uint64\"},"
            "{\"$value\":\"71.6\",\"$type\":\"double\"},"
            "{\"$value\":\"true\",\"$type\":\"boolean\"},"
            "{\"$value\":\"false\",\"$type\":\"boolean\"},"
            "{\"$value\":\"aaa\",\"$type\":\"string\"},"

            "{\"$value\":42,\"$type\":\"int64\"},"
            "{\"$value\":123,\"$type\":\"uint64\"},"
            "{\"$value\":71.6,\"$type\":\"double\"},"
            "{\"$value\":true,\"$type\":\"boolean\"},"
            "{\"$value\":false,\"$type\":\"boolean\"},"

            "{\"$value\":89,\"$type\":\"int64\"},"
            "{\"$value\":89,\"$type\":\"uint64\"},"
            "{\"$value\":89,\"$type\":\"double\"}"
        "]";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, SomeHackyTest)
{
    TString input = "{\"$value\": \"yamr\", \"$attributes\": {\"lenval\": \"false\", \"has_subkey\": \"false\"}}";

    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("lenval"));
        EXPECT_CALL(Mock, OnStringScalar("false"));
        EXPECT_CALL(Mock, OnKeyedItem("has_subkey"));
        EXPECT_CALL(Mock, OnStringScalar("false"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnStringScalar("yamr"));

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, EmptyListFragment)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    TString empty;
    TStringInput stream(empty);
    ParseJson(&stream, &Mock, nullptr, EYsonType::ListFragment);
}

TEST(TJsonParserTest, ListFragment)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("hello"));
        EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";

    TStringInput stream(input);
    ParseJson(&stream, &Mock, nullptr, EYsonType::ListFragment);
}

TEST(TJsonParserTest, SpecialKeys)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("$$value"));
        EXPECT_CALL(Mock, OnStringScalar("10"));
        EXPECT_CALL(Mock, OnKeyedItem("$attributes"));
        EXPECT_CALL(Mock, OnStringScalar("20"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "{\"$$$value\":\"10\",\"$$attributes\":\"20\"}\n";

    TStringInput stream(input);
    ParseJson(&stream, &Mock, nullptr, EYsonType::ListFragment);
}

TEST(TJsonParserTest, AttributesWithoutValue)
{
    StrictMock<TMockYsonConsumer> Mock;

    TString input = "{\"$attributes\":\"20\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, Trash)
{
    StrictMock<TMockYsonConsumer> Mock;

    TString input = "fdslfsdhfkajsdhf";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, TrailingTrash)
{
    StrictMock<TMockYsonConsumer> Mock;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("a"));
    EXPECT_CALL(Mock, OnStringScalar("b"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "{\"a\":\"b\"} fdslfsdhfkajsdhf";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, MultipleValues)
{
    StrictMock<TMockYsonConsumer> Mock;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("a"));
    EXPECT_CALL(Mock, OnStringScalar("b"));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "{\"a\":\"b\"}{\"a\":\"b\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, ReservedKeyName)
{
    StrictMock<TMockYsonConsumer> Mock;

    EXPECT_CALL(Mock, OnBeginMap());

    TString input = "{\"$other\":\"20\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock));
}

TEST(TJsonParserTest, MemoryLimit1)
{
    StrictMock<TMockYsonConsumer> Mock;

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 10;

    TString input = "{\"my_string\":\"" + TString(100000, 'X') + "\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock, config));
}

TEST(TJsonParserTest, MemoryLimit2)
{
    StrictMock<TMockYsonConsumer> Mock;

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("my_string"));
        TString expectedString(100000, 'X');
        EXPECT_CALL(Mock, OnStringScalar(expectedString));
    EXPECT_CALL(Mock, OnEndMap());

    TString input = "{\"my_string\":\"" + TString(100000, 'X') + "\"}";

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 500000;

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, MemoryLimit3)
{
    StrictMock<TMockYsonConsumer> Mock;

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 1000;

    int keyCount = 100;
    TStringStream stream;
    stream << "{";
    for (int i = 0; i < keyCount; ++i) {
        stream << "\"key" << ToString(i) << "\": \"value\"";
        if (i + 1 < keyCount) {
            stream << ",";
        }
    }
    stream << "}";

    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock, config));
}

TEST(TJsonParserTest, MemoryLimit4)
{
    NiceMock<TMockYsonConsumer> Mock;

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 200000;
    config->BufferSize = 512;

    int rowCount = 1000;
    int keyCount = 100;

    TStringStream stream;
    for (int j = 0; j < rowCount; ++j) {
        stream << "{";
        for (int i = 0; i < keyCount; ++i) {
            stream << "\"key" << ToString(i) << "\": \"value\"";
            if (i + 1 < keyCount) {
                stream << ",";
            }
        }
        stream << "}\n";
    }

    // Not throw, because of total memory occupied by all rows is greater than MemoryLimit,
    // but memory occuied by individual row is much lower than MemoryLimit.
    ParseJson(&stream, &Mock, config, EYsonType::ListFragment);
}

////////////////////////////////////////////////////////////////////////////////

TString MakeDeepMapJson(int depth)
{
    TString result;
    for (int i = 0; i < depth; ++i) {
        result += "{\"k\":";
    }
    result += "0";
    for (int i = 0; i < depth; ++i) {
        result += "}";
    }
    return result;
}

TString MakeDeepListJson(int depth)
{
    TString result;
    for (int i = 0; i < depth; ++i) {
        result += "[";
    }
    result += "0";
    for (int i = 0; i < depth; ++i) {
        result += "]";
    }
    return result;
}

TEST(TJsonParserTest, ParseDeepMapNoExcept)
{
    TStringStream yson;
    NYT::NYson::TYsonWriter writer(&yson);
    auto config = NYT::New<TJsonFormatConfig>();
    config->NestingLevelLimit = 20;
    TJsonParser parser(&writer, config, EYsonType::Node);
    EXPECT_NO_THROW(parser.Read(MakeDeepMapJson(config->NestingLevelLimit)));
}

TEST(TJsonParserTest, ParseDeepMapExcept)
{
    TStringStream yson;
    NYT::NYson::TYsonWriter writer(&yson);
    auto config = NYT::New<TJsonFormatConfig>();
    config->NestingLevelLimit = 20;
    TJsonParser parser(&writer, config, EYsonType::Node);
    EXPECT_THROW(parser.Read(MakeDeepMapJson(config->NestingLevelLimit + 1)), NYT::TErrorException);
}

TEST(TJsonParserTest, ParseDeepListNoExcept)
{
    TStringStream yson;
    NYT::NYson::TYsonWriter writer(&yson);
    auto config = NYT::New<TJsonFormatConfig>();
    config->NestingLevelLimit = 20;
    TJsonParser parser(&writer, config, EYsonType::Node);
    EXPECT_NO_THROW(parser.Read(MakeDeepListJson(config->NestingLevelLimit)));
}

TEST(TJsonParserTest, ParseDeepListExcept)
{
    TStringStream yson;
    NYT::NYson::TYsonWriter writer(&yson);
    auto config = NYT::New<TJsonFormatConfig>();
    config->NestingLevelLimit = 20;
    TJsonParser parser(&writer, config, EYsonType::Node);
    EXPECT_THROW(parser.Read(MakeDeepListJson(config->NestingLevelLimit + 1)), NYT::TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJson
