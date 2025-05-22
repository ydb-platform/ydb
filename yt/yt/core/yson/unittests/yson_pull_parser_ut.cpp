#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/token_writer.h>
#include <yt/yt/core/yson/null_consumer.h>

#include <yt/yt/core/misc/error.h>

#include <util/stream/format.h>
#include <util/stream/mem.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TYsonItem& ysonItem, ::std::ostream* out)
{
    auto type = ysonItem.GetType();
    (*out) << ToString(type) << " {";
    switch (type) {
        case EYsonItemType::Int64Value:
            (*out) << ysonItem.UncheckedAsInt64();
            break;
        case EYsonItemType::Uint64Value:
            (*out) << ysonItem.UncheckedAsUint64();
            break;
        case EYsonItemType::DoubleValue:
            (*out) << ysonItem.UncheckedAsDouble();
            break;
        case EYsonItemType::BooleanValue:
            (*out) << ysonItem.UncheckedAsBoolean();
            break;
        case EYsonItemType::StringValue:
            (*out) << ysonItem.UncheckedAsString();
            break;
        default:
            (*out) << ' ';
            break;
    }
    (*out) << "}";
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetYsonPullSignature(TYsonPullParser* parser, std::optional<size_t> levelToMark=std::nullopt)
{
    TString result;
    {
        TStringOutput out(result);
        bool exhausted = false;
        while (!exhausted) {
            if (levelToMark && parser->IsOnValueBoundary(*levelToMark)) {
                out << "! ";
            }

            auto item = parser->Next();
            switch (item.GetType()) {
                case EYsonItemType::BeginMap:
                    out << "{";
                    break;
                case EYsonItemType::EndMap:
                    out << "}";
                    break;
                case EYsonItemType::BeginAttributes:
                    out << "<";
                    break;
                case EYsonItemType::EndAttributes:
                    out << ">";
                    break;
                case EYsonItemType::BeginList:
                    out << "[";
                    break;
                case EYsonItemType::EndList:
                    out << "]";
                    break;
                case EYsonItemType::EntityValue:
                    out << "#";
                    break;
                case EYsonItemType::BooleanValue:
                    out << (item.UncheckedAsBoolean() ? "%true" : "%false");
                    break;
                case EYsonItemType::Int64Value:
                    out << item.UncheckedAsInt64();
                    break;
                case EYsonItemType::Uint64Value:
                    out << item.UncheckedAsUint64() << 'u';
                    break;
                case EYsonItemType::DoubleValue: {
                    auto value = item.UncheckedAsDouble();
                    if (std::isnan(value)) {
                        out << "%nan";
                    } else if (std::isinf(value)) {
                        if (value > 0) {
                            out << "%+inf";
                        } else {
                            out << "%-inf";
                        }
                    } else {
                        out << Prec(value, PREC_POINT_DIGITS, 2);
                    }
                    break;
                }
                case EYsonItemType::StringValue:
                    out << '\'' << EscapeC(item.UncheckedAsString()) << '\'';
                    break;
                case EYsonItemType::EndOfStream:
                    exhausted = true;
                    continue;
            }
            out << ' ';
        }
    }
    if (result) {
        // Strip trailing space.
        result.resize(result.size() - 1);
    }
    return result;
}

TString GetYsonPullSignature(
    TStringBuf input,
    EYsonType type = EYsonType::Node,
    std::optional<size_t> levelToMark = std::nullopt)
{
    TMemoryInput in(input.data(), input.size());
    TYsonPullParser parser(&in, type);
    return GetYsonPullSignature(&parser, levelToMark);
}

////////////////////////////////////////////////////////////////////////////////

class TStringBufVectorReader
    : public IZeroCopyInput
{
public:
    TStringBufVectorReader(const std::vector<TStringBuf>& data)
        : Data_(data.rbegin(), data.rend())
    {
        NextBuffer();
    }

private:
    void NextBuffer()
    {
        if (Data_.empty()) {
            CurrentInput_.reset();
        } else {
            CurrentInput_.emplace(Data_.back());
            Data_.pop_back();
        }
    }

    size_t DoNext(const void** ptr, size_t len) override
    {
        if (!len) {
            return 0;
        }
        while (CurrentInput_) {
            auto result = CurrentInput_->Next(ptr, len);
            if (result) {
                return result;
            }
            NextBuffer();
        }
        return 0;
    }

private:
    std::vector<TStringBuf> Data_;
    std::optional<TMemoryInput> CurrentInput_;
};

struct TStringBufCursorHelper
{
    TMemoryInput MemoryInput;
    TYsonPullParser PullParser;

    TStringBufCursorHelper(TStringBuf input, EYsonType ysonType)
        : MemoryInput(input)
        , PullParser(&MemoryInput, ysonType)
    { }
};

class TStringBufCursor
    : private TStringBufCursorHelper
    , public TYsonPullParserCursor
{
public:
    explicit TStringBufCursor(TStringBuf input, EYsonType ysonType = EYsonType::Node)
        : TStringBufCursorHelper(input, ysonType)
        , TYsonPullParserCursor(&PullParser)
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonSyntaxCheckerTest, NestingLevel)
{
    NDetail::TYsonSyntaxChecker checker(EYsonType::Node, /*nestingLevelLimit*/ 10);

    EXPECT_EQ(checker.GetNestingLevel(), 0U);

    checker.OnBeginList();
    EXPECT_EQ(checker.GetNestingLevel(), 1U);

    checker.OnAttributesBegin();
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnString();
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnEquality();
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnSimpleNonstring(EYsonItemType::Int64Value);
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnAttributesEnd();
    EXPECT_EQ(checker.GetNestingLevel(), 1U);

    checker.OnString();
    EXPECT_EQ(checker.GetNestingLevel(), 1U);

    checker.OnSeparator();
    EXPECT_EQ(checker.GetNestingLevel(), 1U);

    checker.OnBeginMap();
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnString();
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnEquality();
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnString();
    EXPECT_EQ(checker.GetNestingLevel(), 2U);

    checker.OnEndMap();
    EXPECT_EQ(checker.GetNestingLevel(), 1U);

    checker.OnEndList();
    EXPECT_EQ(checker.GetNestingLevel(), 0U);

    checker.OnFinish();
    EXPECT_EQ(checker.GetNestingLevel(), 0U);
}

TEST(TYsonSyntaxCheckerTest, Boundaries)
{
    NDetail::TYsonSyntaxChecker checker(EYsonType::Node, /*nestingLevelLimit*/ 10);

    checker.OnBeginList();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), true);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnAttributesBegin();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnValueBoundary(2), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnString();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnValueBoundary(2), true);
    EXPECT_EQ(checker.IsOnKey(), true);

    checker.OnEquality();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnSimpleNonstring(EYsonItemType::Int64Value);
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnAttributesEnd();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnString();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), true);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnSeparator();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), true);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnBeginMap();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnValueBoundary(2), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnString();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnValueBoundary(2), true);
    EXPECT_EQ(checker.IsOnKey(), true);

    checker.OnEquality();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnValueBoundary(2), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnString();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnValueBoundary(2), true);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnEndMap();
    EXPECT_EQ(checker.IsOnValueBoundary(0), false);
    EXPECT_EQ(checker.IsOnValueBoundary(1), true);
    EXPECT_EQ(checker.IsOnValueBoundary(2), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnEndList();
    EXPECT_EQ(checker.IsOnValueBoundary(0), true);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnKey(), false);

    checker.OnFinish();
    EXPECT_EQ(checker.IsOnValueBoundary(0), true);
    EXPECT_EQ(checker.IsOnValueBoundary(1), false);
    EXPECT_EQ(checker.IsOnKey(), false);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonPullParserTest, Int)
{
    EXPECT_EQ(GetYsonPullSignature(" 100500 "), "100500");
    EXPECT_EQ(GetYsonPullSignature("\x02\xa7\xa2\x0c"), "-100500");
}

TEST(TYsonPullParserTest, Uint)
{
    EXPECT_EQ(GetYsonPullSignature(" 42u "), "42u");
    EXPECT_EQ(GetYsonPullSignature("\x06\x94\x91\x06"), "100500u");
}

TEST(TYsonPullParserTest, Double)
{
    EXPECT_EQ(GetYsonPullSignature(" 31415926e-7 "), "3.14");
    EXPECT_EQ(GetYsonPullSignature("\x03iW\x14\x8B\n\xBF\5@"), "2.72");
    EXPECT_EQ(GetYsonPullSignature(" %nan "), "%nan");
    EXPECT_ANY_THROW(GetYsonPullSignature(" %+nan "));
    EXPECT_ANY_THROW(GetYsonPullSignature(" %-nan "));
    EXPECT_ANY_THROW(GetYsonPullSignature(" %nany "));
    EXPECT_ANY_THROW(GetYsonPullSignature(" +nan "));
    EXPECT_EQ(GetYsonPullSignature(" %inf "), "%+inf");
    EXPECT_EQ(GetYsonPullSignature(" %+inf "), "%+inf");
    EXPECT_EQ(GetYsonPullSignature(" %-inf "), "%-inf");
    EXPECT_ANY_THROW(GetYsonPullSignature(" +inf "));
    EXPECT_ANY_THROW(GetYsonPullSignature(" %infinity "));
}

TEST(TYsonPullParserTest, String)
{
    EXPECT_EQ(GetYsonPullSignature(" nan "), "'nan'");
    EXPECT_EQ(GetYsonPullSignature("\x01\x06" "bar"), "'bar'");
    EXPECT_EQ(GetYsonPullSignature("\x01\x80\x01" + TString(64, 'a')), TString("'") + TString(64, 'a') + "'");
    EXPECT_EQ(GetYsonPullSignature(TStringBuf("\x01\x00"sv)), "''");
    EXPECT_EQ(GetYsonPullSignature(" Hello_789_World_123 "), "'Hello_789_World_123'");
    EXPECT_EQ(GetYsonPullSignature(" Hello_789_World_123 "), "'Hello_789_World_123'");
    EXPECT_EQ(
        GetYsonPullSignature("\" abcdeABCDE <1234567> + (10_000) - = 900   \""),
        "' abcdeABCDE <1234567> + (10_000) - = 900   '");
}

TEST(TYsonPullParserTest, StringEscaping)
{
    TString expected;
    for (int i = 0; i < 256; ++i) {
        expected.push_back(char(i));
    }

    TStringBuf inputData =
        "\"\\0\\1\\2\\3\\4\\5\\6\\7\\x08\\t\\n\\x0B\\x0C\\r\\x0E\\x0F"
        "\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1A\\x1B"
        "\\x1C\\x1D\\x1E\\x1F !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCD"
        "EFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        "\\x7F\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89\\x8A"
        "\\x8B\\x8C\\x8D\\x8E\\x8F\\x90\\x91\\x92\\x93\\x94\\x95\\x96"
        "\\x97\\x98\\x99\\x9A\\x9B\\x9C\\x9D\\x9E\\x9F\\xA0\\xA1\\xA2"
        "\\xA3\\xA4\\xA5\\xA6\\xA7\\xA8\\xA9\\xAA\\xAB\\xAC\\xAD\\xAE"
        "\\xAF\\xB0\\xB1\\xB2\\xB3\\xB4\\xB5\\xB6\\xB7\\xB8\\xB9\\xBA"
        "\\xBB\\xBC\\xBD\\xBE\\xBF\\xC0\\xC1\\xC2\\xC3\\xC4\\xC5\\xC6"
        "\\xC7\\xC8\\xC9\\xCA\\xCB\\xCC\\xCD\\xCE\\xCF\\xD0\\xD1\\xD2"
        "\\xD3\\xD4\\xD5\\xD6\\xD7\\xD8\\xD9\\xDA\\xDB\\xDC\\xDD\\xDE"
        "\\xDF\\xE0\\xE1\\xE2\\xE3\\xE4\\xE5\\xE6\\xE7\\xE8\\xE9\\xEA"
        "\\xEB\\xEC\\xED\\xEE\\xEF\\xF0\\xF1\\xF2\\xF3\\xF4\\xF5\\xF6"
        "\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\""sv;

    TMemoryInput inputStream(inputData);
    TYsonPullParser parser(&inputStream, EYsonType::Node);
    auto item1 = parser.Next();
    EXPECT_EQ(item1.GetType(), EYsonItemType::StringValue);
    EXPECT_EQ(item1.UncheckedAsString(), expected);

    auto item2 = parser.Next();
    EXPECT_TRUE(item2.IsEndOfStream());
}

TEST(TYsonPullParserTest, TrailingSlashes)
{
    char inputData[] = {'"', '\\', '"', '\\', '\\', '"'};
    TMemoryInput inputStream(inputData, sizeof(inputData));
    TYsonPullParser parser(&inputStream, EYsonType::Node);
    auto item1 = parser.Next();
    EXPECT_EQ(item1.GetType(), EYsonItemType::StringValue);
    EXPECT_EQ(item1.UncheckedAsString(), "\"\\");

    auto item2 = parser.Next();
    EXPECT_TRUE(item2.IsEndOfStream());
}

TEST(TYsonPullParserTest, Entity)
{
    EXPECT_EQ(GetYsonPullSignature(" # "), "#");
}

TEST(TYsonPullParserTest, Boolean)
{
    EXPECT_EQ(GetYsonPullSignature(" %true "), "%true");
    EXPECT_EQ(GetYsonPullSignature(" %false "), "%false");
    EXPECT_EQ(GetYsonPullSignature("\x04"), "%false");
    EXPECT_EQ(GetYsonPullSignature("\x05"), "%true");
    EXPECT_ANY_THROW(GetYsonPullSignature(" %falsee "));
}

TEST(TYsonPullParserTest, List)
{
    EXPECT_EQ(GetYsonPullSignature("[]"), "[ ]");
    EXPECT_EQ(GetYsonPullSignature("[[]]"), "[ [ ] ]");
    EXPECT_EQ(GetYsonPullSignature("[[] ; ]"), "[ [ ] ]");
    EXPECT_EQ(GetYsonPullSignature("[[] ; [[]] ]"), "[ [ ] [ [ ] ] ]");
}

TEST(TYsonPullParserTest, Map)
{
    EXPECT_EQ(GetYsonPullSignature("{}"), "{ }");
    EXPECT_EQ(GetYsonPullSignature("{k=v}"), "{ 'k' 'v' }");
    EXPECT_EQ(GetYsonPullSignature("{k=v;}"), "{ 'k' 'v' }");
    EXPECT_EQ(GetYsonPullSignature("{k1=v; k2={} }"), "{ 'k1' 'v' 'k2' { } }");
}

TEST(TYsonPullParserTest, Attributes)
{
    EXPECT_EQ(GetYsonPullSignature("<>#"), "< > #");
    EXPECT_EQ(GetYsonPullSignature("<a=v> #"), "< 'a' 'v' > #");
    EXPECT_EQ(GetYsonPullSignature("<a=v;> #"), "< 'a' 'v' > #");
    EXPECT_EQ(GetYsonPullSignature("<a1=v; a2={}; a3=<># > #"), "< 'a1' 'v' 'a2' { } 'a3' < > # > #");
}

TEST(TYsonPullParserTest, ListFragment)
{
    EXPECT_EQ(GetYsonPullSignature("", EYsonType::ListFragment), "");
    EXPECT_EQ(GetYsonPullSignature("#", EYsonType::ListFragment), "#");
    EXPECT_EQ(GetYsonPullSignature("#;", EYsonType::ListFragment), "#");
    EXPECT_EQ(GetYsonPullSignature("#; #", EYsonType::ListFragment), "# #");
    EXPECT_EQ(GetYsonPullSignature("[];{};<>#;", EYsonType::ListFragment), "[ ] { } < > #");
}

TEST(TYsonPullParserTest, StraySemicolon)
{
    EXPECT_THROW_THAT(
        GetYsonPullSignature("{x=y};", EYsonType::Node),
        testing::HasSubstr("yson_type = \"list_fragment\""));
}

TEST(TYsonPullParserTest, MapFragment)
{
    EXPECT_EQ(GetYsonPullSignature("", EYsonType::MapFragment), "");
    EXPECT_EQ(GetYsonPullSignature("k=v ", EYsonType::MapFragment), "'k' 'v'");
    EXPECT_EQ(GetYsonPullSignature("k=v ; ", EYsonType::MapFragment), "'k' 'v'");
    EXPECT_EQ(
        GetYsonPullSignature("k1=v; k2={}; k3=[]; k4=<>#", EYsonType::MapFragment),
        "'k1' 'v' 'k2' { } 'k3' [ ] 'k4' < > #");
}

TEST(TYsonPullParserTest, Complex1)
{
    EXPECT_EQ(
        GetYsonPullSignature(
            "<acl = { read = [ \"*\" ]; write = [ sandello ] } ;  \n"
            "  lock_scope = mytables> \n"
            "{ path = \"/home/sandello\" ; mode = 0755 }"),

            "< 'acl' { 'read' [ '*' ] 'write' [ 'sandello' ] }"
            " 'lock_scope' 'mytables' >"
            " { 'path' '/home/sandello' 'mode' 755 }");
}

TEST(TYsonPullParserTest, TestBadYson)
{
    EXPECT_ANY_THROW(GetYsonPullSignature("foo bar "));
    EXPECT_ANY_THROW(GetYsonPullSignature("foo bar ", EYsonType::ListFragment));
    EXPECT_ANY_THROW(GetYsonPullSignature("{foo=1;2};", EYsonType::ListFragment));
    EXPECT_ANY_THROW(GetYsonPullSignature("foo; bar"));
    EXPECT_ANY_THROW(GetYsonPullSignature("foo bar ", EYsonType::MapFragment));
    EXPECT_ANY_THROW(GetYsonPullSignature("key=[a=b;c=d]", EYsonType::MapFragment));
    EXPECT_ANY_THROW(GetYsonPullSignature("foo=bar "));
    EXPECT_ANY_THROW(GetYsonPullSignature("foo=bar ", EYsonType::ListFragment));
}

TEST(TYsonPullParserTest, Capture)
{
    EXPECT_EQ(GetYsonPullSignature(" foo ", EYsonType::Node, 0), "! 'foo' !");
    EXPECT_EQ(GetYsonPullSignature(" <foo=bar>foo ", EYsonType::Node, 0), "! < 'foo' 'bar' > 'foo' !");
    EXPECT_EQ(GetYsonPullSignature(" <foo=bar>[ 42 ] ", EYsonType::Node, 1), "< 'foo' ! 'bar' ! > [ ! 42 ! ]");
    EXPECT_EQ(GetYsonPullSignature(
        " <foo=[bar]; bar=2; baz=[[1;2;3]]>[ 1; []; {foo=[]}] ", EYsonType::Node, 2),
        "< 'foo' [ ! 'bar' ! ] 'bar' 2 'baz' [ ! [ 1 2 3 ] ! ] > [ 1 [ ! ] { 'foo' ! [ ] ! } ]");
}

TEST(TYsonPullParserTest, DepthLimitExceeded)
{
    constexpr auto DepthLimit = DefaultYsonParserNestingLevelLimit;
    EXPECT_NO_THROW(GetYsonPullSignature(TString(DepthLimit - 1, '[') + TString(DepthLimit - 1, ']')));
    EXPECT_ANY_THROW(GetYsonPullSignature(TString(DepthLimit, '[') + TString(DepthLimit, ']')));
}

TEST(TYsonPullParserTest, DepthLimitInTransfer)
{
    constexpr auto DepthLimit = DefaultYsonParserNestingLevelLimit;

    auto transfer = [] (TStringBuf yson) {
        TMemoryInput input(yson);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        cursor.TransferComplexValue(GetNullYsonConsumer());
    };

    EXPECT_NO_THROW(transfer(TString(DepthLimit - 1, '[') + TString(DepthLimit - 1, ']')));
    EXPECT_ANY_THROW(transfer(TString(DepthLimit, '[') + TString(DepthLimit, ']')));
}

TEST(TYsonPullParserTest, ContextInExceptions)
{
    try {
        GetYsonPullSignature("{foo bar = 580}");
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), testing::HasSubstr("bar = 580}"));
        return;
    }
    GTEST_FAIL() << "Expected exception to be thrown";
}

TEST(TYsonPullParserTest, ContextInExceptions_ManyBlocks)
{
    try {
        auto manyO = TString(100, 'o');
        TStringBufVectorReader input(
            {
                "{fo",
                manyO, // try to overflow 64 byte context
                "o bar = 580}",
            });
        TYsonPullParser parser(&input, EYsonType::Node);
        GetYsonPullSignature(&parser);
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), testing::HasSubstr("bar = 580}"));
        return;
    }
    GTEST_FAIL() << "Expected exception to be thrown";
}

TEST(TYsonPullParserTest, ContextInExceptions_ContextAtTheVeryBeginning)
{
    try {
        GetYsonPullSignature("! foo bar baz");
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), testing::HasSubstr("! foo bar"));
        return;
    }
    GTEST_FAIL() << "Expected exception to be thrown";
}

TEST(TYsonPullParserTest, ContextInExceptions_Margin)
{
    try {
        auto manyO = TString(100, 'o');
        TStringBufVectorReader input(
            {
                "{fo",
                manyO, // try to overflow 64 byte context
                "a",
                "b",
                "c",
                "d bar = 580}",
            });
        TYsonPullParser parser(&input, EYsonType::Node);
        GetYsonPullSignature(&parser);
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), testing::HasSubstr("oabcd bar = 580}"));
        return;
    }
}

std::optional<i64> ParseOptionalZigZagVarint(TYsonPullParser& parser)
{
    char buffer[MaxVarInt64Size];
    auto len = parser.ParseOptionalInt64AsZigzagVarint(buffer);
    if (len == 0) {
        return {};
    }
    i64 value;
    ReadVarInt64(buffer, buffer + len, &value);
    return value;
}

std::optional<ui64> ParseOptionalVarint(TYsonPullParser& parser)
{
    char buffer[MaxVarUint64Size];
    auto len = parser.ParseOptionalUint64AsVarint(buffer);
    if (len == 0) {
        return {};
    }
    ui64 value;
    ReadVarUint64(buffer, buffer + len, &value);
    return value;
};

i64 ParseZigZagVarint(TYsonPullParser& parser)
{
    char buffer[MaxVarInt64Size];
    auto len = parser.ParseOptionalInt64AsZigzagVarint(buffer);
    i64 value;
    ReadVarInt64(buffer, buffer + len, &value);
    return value;
}

ui64 ParseVarint(TYsonPullParser& parser)
{
    char buffer[MaxVarUint64Size];
    auto len = parser.ParseOptionalUint64AsVarint(buffer);
    ui64 value;
    ReadVarUint64(buffer, buffer + len, &value);
    return value;
};

TEST(TYsonPullParserTest, TypedParsingBasicCases)
{
    TStringBufVectorReader input(
        {
            "["
                "["
                    "-100500;\x02\xa7\xa2\x0c;" "-100500;\x02\xa7\xa2\x0c;#;"
                    "-100500;\x02\xa7\xa2\x0c;" "-100500;\x02\xa7\xa2\x0c;#;"
                    "100500u;\x06\x94\x91\x06;" "100500u;\x06\x94\x91\x06;#;"
                    "100500u;\x06\x94\x91\x06;" "100500u;\x06\x94\x91\x06;#;"
                    "2.72;\x03iW\x14\x8B\n\xBF\5@;" "2.72;\x03iW\x14\x8B\n\xBF\5@;#;"
                    "\"bar\";" "\x01\x06" "bar;" "\"bar\";" "\x01\x06" "bar;" "#;"
                    "%true;\x05;%false;\x04;" "%true;\x05;%false;\x04;#;"
                "];"
                "#;"
            "]"
        });

    TYsonPullParser parser(&input, EYsonType::Node);
    EXPECT_TRUE(parser.ParseOptionalBeginList());
    parser.ParseBeginList();

    EXPECT_FALSE(parser.IsEndList());

    EXPECT_EQ(parser.ParseInt64(), -100500);
    EXPECT_EQ(parser.ParseInt64(), -100500);
    EXPECT_EQ(parser.ParseOptionalInt64(), std::optional<i64>(-100500));
    EXPECT_EQ(parser.ParseOptionalInt64(), std::optional<i64>(-100500));
    EXPECT_EQ(parser.ParseOptionalInt64(), std::optional<i64>{});

    EXPECT_EQ(ParseZigZagVarint(parser), -100500);
    EXPECT_EQ(ParseZigZagVarint(parser), -100500);
    EXPECT_EQ(ParseOptionalZigZagVarint(parser), std::optional<i64>(-100500));
    EXPECT_EQ(ParseOptionalZigZagVarint(parser), std::optional<i64>(-100500));
    EXPECT_EQ(ParseOptionalZigZagVarint(parser), std::optional<i64>{});

    EXPECT_EQ(parser.ParseUint64(), 100500u);
    EXPECT_EQ(parser.ParseUint64(), 100500u);
    EXPECT_EQ(parser.ParseOptionalUint64(), std::optional<ui64>(100500));
    EXPECT_EQ(parser.ParseOptionalUint64(), std::optional<ui64>(100500));
    EXPECT_EQ(parser.ParseOptionalUint64(), std::optional<ui64>{});

    EXPECT_EQ(ParseVarint(parser), 100500u);
    EXPECT_EQ(ParseVarint(parser), 100500u);
    EXPECT_EQ(ParseOptionalVarint(parser), std::optional<ui64>(100500));
    EXPECT_EQ(ParseOptionalVarint(parser), std::optional<ui64>(100500));
    EXPECT_EQ(ParseOptionalVarint(parser), std::optional<ui64>{});

    EXPECT_DOUBLE_EQ(parser.ParseDouble(), 2.72);
    EXPECT_DOUBLE_EQ(parser.ParseDouble(), 2.7182818284590451);
    EXPECT_DOUBLE_EQ(*parser.ParseOptionalDouble(), 2.72);
    EXPECT_DOUBLE_EQ(*parser.ParseOptionalDouble(), 2.7182818284590451);
    EXPECT_EQ(parser.ParseOptionalDouble(), std::optional<double>{});

    EXPECT_EQ(parser.ParseString(), "bar");
    EXPECT_EQ(parser.ParseString(), "bar");
    EXPECT_EQ(parser.ParseOptionalString(), std::optional<TStringBuf>("bar"));
    EXPECT_EQ(parser.ParseOptionalString(), std::optional<TStringBuf>("bar"));
    EXPECT_EQ(parser.ParseOptionalString(), std::optional<TStringBuf>{});

    EXPECT_FALSE(parser.IsEndList());

    EXPECT_EQ(parser.ParseBoolean(), true);
    EXPECT_EQ(parser.ParseBoolean(), true);
    EXPECT_EQ(parser.ParseBoolean(), false);
    EXPECT_EQ(parser.ParseBoolean(), false);
    EXPECT_EQ(parser.ParseOptionalBoolean(), std::optional<bool>(true));
    EXPECT_EQ(parser.ParseOptionalBoolean(), std::optional<bool>(true));
    EXPECT_EQ(parser.ParseOptionalBoolean(), std::optional<bool>(false));
    EXPECT_EQ(parser.ParseOptionalBoolean(), std::optional<bool>(false));
    EXPECT_EQ(parser.ParseOptionalBoolean(), std::optional<bool>{});

    EXPECT_TRUE(parser.IsEndList());
    parser.ParseEndList();

    EXPECT_FALSE(parser.ParseOptionalBeginList());
    parser.ParseEndList();

    EXPECT_EQ(parser.Next().GetType(), EYsonItemType::EndOfStream);
}

TEST(TYsonPullParserTest, TypedParsingBasicErrors)
{
    {
        TStringBufVectorReader input({"100u"});
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_THROW_THAT(parser.ParseInt64(), ::testing::HasSubstr("expected \"int64_value\""));
    }
    {
        TStringBufVectorReader input({"\x06\x94\x91\x06"});
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_THROW_THAT(parser.ParseInt64(), ::testing::HasSubstr("expected \"int64_value\""));
    }
    {
        TStringBufVectorReader input({"-100"});
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_THROW_THAT(parser.ParseUint64(), ::testing::HasSubstr("expected \"uint64_value\""));
    }
    {
        TStringBufVectorReader input({"\x02\xa7\xa2\x0c"});
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_THROW_THAT(parser.ParseUint64(), ::testing::HasSubstr("expected \"uint64_value\""));
    }
    {
        TStringBufVectorReader input({"[1;;]"});
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_NO_THROW(parser.ParseBeginList());
        EXPECT_NO_THROW(parser.ParseInt64());
        EXPECT_THROW_THAT(parser.ParseEndList(), ::testing::HasSubstr("Unexpected \";\""));
    }
    {
        TStringBufVectorReader input({"[1;;]"});
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_NO_THROW(parser.ParseBeginList());
        EXPECT_NO_THROW(parser.ParseInt64());
        EXPECT_THROW_THAT(parser.IsEndList(), ::testing::HasSubstr("Unexpected \";\""));
    }
    {
        TStringBufVectorReader input({"[1;]]"});
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_NO_THROW(parser.ParseBeginList());
        EXPECT_NO_THROW(parser.ParseInt64());
        EXPECT_NO_THROW(parser.ParseEndList());
        EXPECT_THROW_THAT(parser.ParseEndList(), ::testing::HasSubstr("Unexpected \"]\""));
    }
}

TEST(TYsonPullParserTest, TestTransferValueViaTokenWriterBasicCases)
{
    TStringBuf inputString = "[ [ {foo=<attr=value;>bar; qux=[-1; 2u; %false; 3.14; lol; # ; ] ; }; ] ; 6; ]";

    auto output = TString();
    {
        TStringOutput outputStream(output);
        TCheckedInDebugYsonTokenWriter tokenWriter(&outputStream);
        TStringBufVectorReader input({inputString});
        TYsonPullParser parser(&input, EYsonType::Node);
        parser.TransferComplexValue(&tokenWriter);
        EXPECT_EQ(parser.Next().GetType(), EYsonItemType::EndOfStream);
    }
    auto outputWithPreviousItem = TString();
    {
        TStringOutput outputStream(outputWithPreviousItem);
        TCheckedInDebugYsonTokenWriter tokenWriter(&outputStream);
        TStringBufVectorReader input({inputString});
        TYsonPullParser parser(&input, EYsonType::Node);
        auto firstItem = parser.Next();
        parser.TransferComplexValue(&tokenWriter, firstItem);
        EXPECT_EQ(parser.Next().GetType(), EYsonItemType::EndOfStream);
    }
    auto expectedOutput = TString();
    {
        TStringOutput outputStream(expectedOutput);
        TCheckedInDebugYsonTokenWriter tokenWriter(&outputStream);
        tokenWriter.WriteBeginList();
        tokenWriter.WriteBeginList();
        tokenWriter.WriteBeginMap();
        tokenWriter.WriteBinaryString("foo");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteBeginAttributes();
        tokenWriter.WriteBinaryString("attr");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteBinaryString("value");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndAttributes();
        tokenWriter.WriteBinaryString("bar");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryString("qux");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteBeginList();
        tokenWriter.WriteBinaryInt64(-1);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryUint64(2);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryBoolean(false);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryDouble(3.14);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryString("lol");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEntity();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndList();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndMap();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndList();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryInt64(6);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndList();
    }
    EXPECT_EQ(expectedOutput, output);
    EXPECT_EQ(expectedOutput, outputWithPreviousItem);

    {
        TStringBufVectorReader input({
            "<a=b;c=d;>12"
        });

        TYsonPullParser parser(&input, EYsonType::Node);
        auto item = parser.Next();
        EXPECT_EQ(item.GetType(), EYsonItemType::BeginAttributes);

        auto output = TString();
        {
            TStringOutput outputStream(output);
            TCheckedInDebugYsonTokenWriter writer(&outputStream);
            EXPECT_NO_THROW(parser.TransferAttributes(&writer, item));
        }

        i64 x;
        EXPECT_NO_THROW(x = parser.ParseInt64());
        EXPECT_EQ(x, 12);

        {
            TStringStream expectedOutput;
            TCheckedInDebugYsonTokenWriter tokenWriter(&expectedOutput);
            tokenWriter.WriteBeginAttributes();
            tokenWriter.WriteBinaryString("a");
            tokenWriter.WriteKeyValueSeparator();
            tokenWriter.WriteBinaryString("b");
            tokenWriter.WriteItemSeparator();
            tokenWriter.WriteBinaryString("c");
            tokenWriter.WriteKeyValueSeparator();
            tokenWriter.WriteBinaryString("d");
            tokenWriter.WriteItemSeparator();
            tokenWriter.WriteEndAttributes();
            tokenWriter.Flush();
            EXPECT_EQ(expectedOutput.Str(), output);
        }
    }
}

TEST(TYsonPullParserTest, TestSkipValueBasicCases)
{
    {
        TStringBufVectorReader input({
            "[<a=b;c=d;>12; 13; qux;]"
        });
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_NO_THROW(parser.ParseBeginList());
        auto item = parser.Next();
        EXPECT_EQ(item, TYsonItem::Simple(EYsonItemType::BeginAttributes));
        EXPECT_NO_THROW(parser.SkipComplexValueOrAttributes(item));
        item = parser.Next();
        EXPECT_EQ(item, TYsonItem::Int64(12));
        EXPECT_NO_THROW(parser.SkipComplexValueOrAttributes(item));
        i64 x;
        EXPECT_NO_THROW(x = parser.ParseInt64());
        EXPECT_EQ(x, 13);
        TString s;
        EXPECT_NO_THROW(s = parser.ParseString());
        EXPECT_EQ(s, "qux");
        EXPECT_EQ(parser.Next(), TYsonItem::Simple(EYsonItemType::EndList));
        EXPECT_EQ(parser.Next(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
    {
        TStringBufVectorReader input({
            "[<a=b;c=d;>12; 13; qux;]"
        });
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_NO_THROW(parser.ParseBeginList());
        auto item = parser.Next();
        EXPECT_EQ(item.GetType(), EYsonItemType::BeginAttributes);
        EXPECT_NO_THROW(parser.SkipAttributes(item));
        item = parser.Next();
        EXPECT_THROW_THAT(parser.SkipAttributes(item), ::testing::HasSubstr("attributes"));
        EXPECT_EQ(item, TYsonItem::Int64(12));
        EXPECT_NO_THROW(parser.SkipComplexValue(item));
        EXPECT_NO_THROW(parser.SkipComplexValue());
        TString s;
        EXPECT_NO_THROW(s = parser.ParseString());
        EXPECT_EQ(s, "qux");
        EXPECT_EQ(parser.Next(), TYsonItem::Simple(EYsonItemType::EndList));
        EXPECT_EQ(parser.Next(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
    {
        TStringBufVectorReader input({
            "<a=b;c=d;>12"
        });
        TYsonPullParser parser(&input, EYsonType::Node);
        auto item = parser.Next();
        EXPECT_EQ(item.GetType(), EYsonItemType::BeginAttributes);
        EXPECT_NO_THROW(parser.SkipComplexValueOrAttributes(item));
        i64 x;
        EXPECT_NO_THROW(x = parser.ParseInt64());
        EXPECT_EQ(x, 12);
        EXPECT_EQ(parser.Next(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
    {
        TStringBufVectorReader input({
            "<a=b;c=d;>12"
        });
        TYsonPullParser parser(&input, EYsonType::Node);
        auto item = parser.Next();
        EXPECT_EQ(item.GetType(), EYsonItemType::BeginAttributes);
        EXPECT_NO_THROW(parser.SkipComplexValue(item));
        EXPECT_EQ(parser.Next(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
    {
        TStringBufVectorReader input({
            "<a=b;c=d;>12"
        });
        TYsonPullParser parser(&input, EYsonType::Node);
        EXPECT_NO_THROW(parser.SkipComplexValue());
        EXPECT_EQ(parser.Next(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
}

TEST(TYsonPullParserCursorTest, TestTransferValueBasicCases)
{
    TStringBuf input = "[ [ {foo=<attr=value>bar; qux=[-1; 2u; %false; 3.14; lol; # ]} ] ; 6 ]";
    auto cursor = TStringBufCursor(input);
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
    cursor.Next();
    {
        ::testing::StrictMock<TMockYsonConsumer> mock;
        {
            ::testing::InSequence g;
            EXPECT_CALL(mock, OnBeginList());
            EXPECT_CALL(mock, OnListItem());
            EXPECT_CALL(mock, OnBeginMap());
            EXPECT_CALL(mock, OnKeyedItem("foo"));
            EXPECT_CALL(mock, OnBeginAttributes());
            EXPECT_CALL(mock, OnKeyedItem("attr"));
            EXPECT_CALL(mock, OnStringScalar("value"));
            EXPECT_CALL(mock, OnEndAttributes());
            EXPECT_CALL(mock, OnStringScalar("bar"));
            EXPECT_CALL(mock, OnKeyedItem("qux"));
            EXPECT_CALL(mock, OnBeginList());
            EXPECT_CALL(mock, OnListItem());
            EXPECT_CALL(mock, OnInt64Scalar(-1));
            EXPECT_CALL(mock, OnListItem());
            EXPECT_CALL(mock, OnUint64Scalar(2));
            EXPECT_CALL(mock, OnListItem());
            EXPECT_CALL(mock, OnBooleanScalar(false));
            EXPECT_CALL(mock, OnListItem());
            EXPECT_CALL(mock, OnDoubleScalar(::testing::DoubleEq(3.14)));
            EXPECT_CALL(mock, OnListItem());
            EXPECT_CALL(mock, OnStringScalar("lol"));
            EXPECT_CALL(mock, OnListItem());
            EXPECT_CALL(mock, OnEntity());
            EXPECT_CALL(mock, OnEndList());
            EXPECT_CALL(mock, OnEndMap());
            EXPECT_CALL(mock, OnEndList());
        }
        cursor.TransferComplexValue(&mock);
    }
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Int64(6));
}


TEST(TYsonPullParserCursorTest, TestTransferAttributesBasicCases)
{
    TStringBuf input = "[<attr=value>bar; qux; 2]";
    auto cursor = TStringBufCursor(input);
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
    cursor.Next();
    {
        ::testing::StrictMock<TMockYsonConsumer> mock;
        {
            ::testing::InSequence g;
            EXPECT_CALL(mock, OnBeginAttributes());
            EXPECT_CALL(mock, OnKeyedItem("attr"));
            EXPECT_CALL(mock, OnStringScalar("value"));
            EXPECT_CALL(mock, OnEndAttributes());
        }
        cursor.TransferAttributes(&mock);
    }
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::String("bar"));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::String("qux"));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Int64(2));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndList));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndOfStream));
}

TEST(TYsonPullParserCursorTest, TestTransferValueViaTokenWriterBasicCases)
{
    TStringBuf input = "[ [ {foo=<attr=value;>bar; qux=[-1; 2u; %false; 3.14; lol; # ; ] ; }; ] ; 6; ]";
    auto output1 = TString();
    {
        TStringOutput outputStream(output1);
        TCheckedInDebugYsonTokenWriter tokenWriter(&outputStream);
        auto cursor = TStringBufCursor(input);
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
        cursor.TransferComplexValue(&tokenWriter);
    }
    auto output2 = TString();
    {
        TStringOutput outputStream(output2);
        TCheckedInDebugYsonTokenWriter tokenWriter(&outputStream);
        tokenWriter.WriteBeginList();
        tokenWriter.WriteBeginList();
        tokenWriter.WriteBeginMap();
        tokenWriter.WriteBinaryString("foo");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteBeginAttributes();
        tokenWriter.WriteBinaryString("attr");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteBinaryString("value");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndAttributes();
        tokenWriter.WriteBinaryString("bar");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryString("qux");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteBeginList();
        tokenWriter.WriteBinaryInt64(-1);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryUint64(2);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryBoolean(false);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryDouble(3.14);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryString("lol");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEntity();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndList();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndMap();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndList();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteBinaryInt64(6);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndList();
    }
    EXPECT_EQ(output1, output2);
}

TEST(TYsonPullParserCursorTest, TestTransferAttributesViaTokenWriterBasicCases)
{
    TStringBuf input = "[<attr=value;>bar; qux; 2;]";
    auto cursor = TStringBufCursor(input);
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
    cursor.Next();

    auto output1 = TString();
    {
        TStringOutput outputStream(output1);
        TCheckedInDebugYsonTokenWriter tokenWriter(&outputStream);
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginAttributes));
        cursor.TransferAttributes(&tokenWriter);
    }
    auto output2 = TString();
    {
        TStringOutput outputStream(output2);
        TCheckedInDebugYsonTokenWriter tokenWriter(&outputStream);
        tokenWriter.WriteBeginAttributes();
        tokenWriter.WriteBinaryString("attr");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteBinaryString("value");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndAttributes();
    }
    EXPECT_EQ(output1, output2);
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::String("bar"));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::String("qux"));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Int64(2));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndList));
    cursor.Next();
    EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndOfStream));
}

TEST(TYsonPullParserCursorTest, TestSkipValueBasicCases)
{
    {
        TStringBuf input = "[<a=b;c=d;>12; qux;]";
        auto cursor = TStringBufCursor(input);
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginAttributes));
        EXPECT_NO_THROW(cursor.SkipAttributes());
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Int64(12));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::String("qux"));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndList));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
    {
        TStringBuf input = "[<a=b;c=d;>12; 13; qux;]";
        auto cursor = TStringBufCursor(input);
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginAttributes));
        EXPECT_NO_THROW(cursor.SkipComplexValue());
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Int64(13));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::String("qux"));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndList));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
}

TEST(TYsonPullParserCursorTest, TestParseCompoundBasicCases)
{
    {
        TStringBuf input = "[[1;2]; <x=1;y=2>%true; #]";
        auto cursor = TStringBufCursor(input);
        int timesCalled = 0;
        auto listConsumer = [&] (TYsonPullParserCursor* cursor) {
            switch (timesCalled) {
                case 0:
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(1));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(2));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::EndList));
                    cursor->Next();
                    break;
                case 1:
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginAttributes));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::String("x"));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(1));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::String("y"));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(2));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::EndAttributes));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Boolean(true));
                    cursor->Next();
                    break;
                case 2:
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::EntityValue));
                    cursor->Next();
                    break;
                default:
                    GTEST_FAIL() << "Times called is not 0, 1 or 2: " << timesCalled;
            }
            ++timesCalled;
        };
        EXPECT_NO_THROW(cursor.ParseList(listConsumer));
        EXPECT_EQ(timesCalled, 3);
    }

    auto makeKeyValueConsumer = [] (int& timesCalled) {
        auto fun = [&] (TYsonPullParserCursor* cursor) {
            switch (timesCalled) {
                case 0:
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::String("a"));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(1));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(2));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::EndList));
                    cursor->Next();
                    break;
                case 1:
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::String("b"));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginAttributes));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::String("x"));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(1));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::String("y"));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Int64(2));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::EndAttributes));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Boolean(true));
                    cursor->Next();
                    break;
                case 2:
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::String("c"));
                    cursor->Next();
                    EXPECT_EQ(cursor->GetCurrent(), TYsonItem::Simple(EYsonItemType::EntityValue));
                    cursor->Next();
                    break;
                default:
                    GTEST_FAIL() << "Times called is not 0, 1 or 2: " << timesCalled;
            }
            ++timesCalled;
        };
        return fun;
    };

    {
        TStringBuf input = "{a=[1;2]; b=<x=1;y=2>%true; c=#}";
        auto cursor = TStringBufCursor(input);
        int timesCalled = 0;
        EXPECT_NO_THROW(cursor.ParseMap(makeKeyValueConsumer(timesCalled)));
        EXPECT_EQ(timesCalled, 3);
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
    {
        TStringBuf input = "<a=[1;2]; b=<x=1;y=2>%true; c=#>[x; 3]";
        auto cursor = TStringBufCursor(input);
        int timesCalled = 0;
        EXPECT_NO_THROW(cursor.ParseAttributes(makeKeyValueConsumer(timesCalled)));
        EXPECT_EQ(timesCalled, 3);
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::BeginList));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::String("x"));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Int64(3));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndList));
        cursor.Next();
        EXPECT_EQ(cursor.GetCurrent(), TYsonItem::Simple(EYsonItemType::EndOfStream));
    }
}

TEST(TYsonPullParserCursorTest, TestParseBoolAndNan)
{
    {
        TStringBuf input = "%nan";
        auto cursor = TStringBufCursor(input);
        EXPECT_EQ(cursor.GetCurrent().GetType(), EYsonItemType::DoubleValue);
        EXPECT_TRUE(std::isnan(cursor.GetCurrent().UncheckedAsDouble()));
    }
    {
        TStringBuf input = "%true";
        auto cursor = TStringBufCursor(input);
        EXPECT_EQ(cursor.GetCurrent().GetType(), EYsonItemType::BooleanValue);
        EXPECT_TRUE(cursor.GetCurrent().UncheckedAsBoolean());
    }
}

TEST(TYsonPullParserCursorTest, ListFragment)
{
    {
        TStringBuf input = "1;2;3";
        auto cursor = TStringBufCursor(input, EYsonType::ListFragment);
        EXPECT_TRUE(cursor.TryConsumeFragmentStart());
        EXPECT_FALSE(cursor.TryConsumeFragmentStart());
        cursor.Next();
        EXPECT_FALSE(cursor.TryConsumeFragmentStart());
    }
    {
        TStringBuf input = "1;2;3";
        auto cursor = TStringBufCursor(input, EYsonType::ListFragment);
        EXPECT_TRUE(cursor.TryConsumeFragmentStart());

        std::vector<int> expected = {1, 2, 3};
        for (auto expectedEl : expected) {
            EXPECT_EQ(cursor->UncheckedAsInt64(), expectedEl);
            cursor.Next();
        }
        EXPECT_EQ(cursor->GetType(), EYsonItemType::EndOfStream);
    }
}

TEST(TYsonPullParserCursorTest, MapFragment)
{
    {
        TStringBuf input = "a=1;b=2;c=3";
        auto cursor = TStringBufCursor(input, EYsonType::MapFragment);
        EXPECT_TRUE(cursor.TryConsumeFragmentStart());
        EXPECT_FALSE(cursor.TryConsumeFragmentStart());
        cursor.Next();
        EXPECT_FALSE(cursor.TryConsumeFragmentStart());
    }
    {
        TStringBuf input = "a=1;b=2;c=3";
        auto cursor = TStringBufCursor(input, EYsonType::MapFragment);
        EXPECT_TRUE(cursor.TryConsumeFragmentStart());

        std::vector<std::pair<TString, int>> expected = {{"a", 1}, {"b", 2}, {"c", 3}};
        for (const auto& [key, value] : expected) {
            EXPECT_EQ(cursor->UncheckedAsString(), key);
            cursor.Next();
            EXPECT_EQ(cursor->UncheckedAsInt64(), value);
            cursor.Next();
        }
        EXPECT_EQ(cursor->GetType(), EYsonItemType::EndOfStream);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
