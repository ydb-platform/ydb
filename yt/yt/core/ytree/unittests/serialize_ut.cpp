#include "serialize_ut.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/ytree/unittests/proto/test.pb.h>

#include <library/cpp/yt/misc/arcadia_enum.h>

#include <array>

namespace NYT::NYTree {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TTestMessage& l, const TTestMessage& r)
{
    return l.int32_field() == r.int32_field() && l.string_field() == r.string_field();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

YT_DEFINE_ARCADIA_ENUM_TRAITS(ESerializableArcadiaEnum)

namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestEnum,
    ((One)      (1) )
    ((FortyTwo) (42))
);

DEFINE_BIT_ENUM(ETestBitEnum,
    ((Red)    (0x0001))
    ((Yellow) (0x0002))
    ((Green)  (0x0004))
);

template <typename T>
T PullParserConvert(TYsonStringBuf s)
{
    TMemoryInput input(s.AsStringBuf());
    TYsonPullParser parser(&input, s.GetType());
    TYsonPullParserCursor cursor(&parser);
    T result;
    Deserialize(result, &cursor);
    EXPECT_EQ(cursor->GetType(), EYsonItemType::EndOfStream);
    return result;
}

template <typename TOriginal, typename TResult = TOriginal>
void TestSerializationDeserializationPullParser(const TOriginal& original, EYsonType ysonType = EYsonType::Node)
{
    auto yson = ConvertToYsonString(original);
    if (ysonType != EYsonType::Node) {
        auto buf = yson.AsStringBuf();
        yson = TYsonString(buf.SubString(1, buf.Size() - 2), ysonType);
    }
    auto deserialized = PullParserConvert<TResult>(yson);
    EXPECT_EQ(original, deserialized);
    auto node = ConvertTo<INodePtr>(original);
    node->MutableAttributes()->Set("some_attribute", 14);
    auto ysonWithAttribute = ConvertToYsonString(node);
    auto deserializedWithAttribute = PullParserConvert<TResult>(ysonWithAttribute);
    EXPECT_EQ(original, deserializedWithAttribute);
}

template <typename TOriginal, typename TResult = TOriginal>
void TestSerializationDeserializationNode(const TOriginal& original)
{
    auto node = ConvertToNode(original);
    auto deserialized = ConvertTo<TResult>(node);
    EXPECT_EQ(original, deserialized);
}

template <typename TOriginal, typename TResult = TOriginal>
void TestSerializationDeserialization(const TOriginal& original, EYsonType ysonType = EYsonType::Node)
{
    TestSerializationDeserializationPullParser<TOriginal, TResult>(original, ysonType);
    TestSerializationDeserializationNode<TOriginal, TResult>(original);
}

template <typename TResult, typename TSource>
void TestDeserialization(const TResult& expected, const TSource& source)
{
    auto yson = ConvertToYsonString(source);
    auto node = ConvertTo<INodePtr>(yson);
    EXPECT_EQ(expected, PullParserConvert<TResult>(yson));
    EXPECT_EQ(expected, ConvertTo<TResult>(node));
}

TString RemoveSpaces(const TString& str)
{
    TString res = str;
    while (true) {
        size_t pos = res.find(" ");
        if (pos == TString::npos) {
            break;
        }
        res.replace(pos, 1, "");
    }
    return res;
}

TEST(TYTreeSerializationTest, All)
{
    TYsonString canonicalYson(TStringBuf(
        "<\"acl\"={\"execute\"=[\"*\";];};>"
        "{\"mode\"=755;\"path\"=\"/home/sandello\";}"));
    auto root = ConvertToNode(canonicalYson);
    auto deserializedYson = ConvertToYsonString(root, NYson::EYsonFormat::Text);
    EXPECT_EQ(RemoveSpaces(canonicalYson.ToString()), deserializedYson.ToString());
}

TEST(TCustomTypeSerializationTest, TInstant)
{
    {
        TInstant value = TInstant::MilliSeconds(100500);
        TestSerializationDeserialization(value);
        TestDeserialization<TInstant, double>(value, 100500.);
        TestDeserialization<TInstant, i64>(value, 100500);
        TestDeserialization<TInstant, ui64>(value, 100500U);
        TestDeserialization<TInstant, TString>(value, "1970-01-01T00:01:40.500000");
    }
    {
        TDuration value = TDuration::MilliSeconds(100500);
        TestSerializationDeserialization(value);
        TestDeserialization<TDuration, double>(value, 100500.);
        TestDeserialization<TDuration, i64>(value, 100500);
        TestDeserialization<TDuration, ui64>(value, 100500U);
        TestDeserialization<TDuration, TString>(value, "100.5s");
    }
}

TEST(TCustomTypeSerializationTest, Optional)
{
    {
        std::optional<int> value(10);
        auto yson = ConvertToYsonString(value);
        EXPECT_EQ(10, ConvertTo<std::optional<int>>(yson));
        TestSerializationDeserialization(value);
    }
    {
        std::optional<int> value;
        auto yson = ConvertToYsonString(value);
        EXPECT_EQ(TString("#"), yson.ToString());
        EXPECT_EQ(value, ConvertTo<std::optional<int>>(yson));
        TestSerializationDeserialization(value);
    }
}

TEST(TSerializationTest, Simple)
{
    {
        signed char value = -127;
        TestSerializationDeserialization(value);
    }
    {
        unsigned char value = 255;
        TestSerializationDeserialization(value);
    }
    {
        short value = -30'000;
        TestSerializationDeserialization(value);
    }
    {
        unsigned short value = 65'535;
        TestSerializationDeserialization(value);
    }
    {
        int value = -2'000'000;
        TestSerializationDeserialization(value);
    }
    {
        unsigned value = 4'000'000;
        TestSerializationDeserialization(value);
    }
    {
        long value = -1'999'999;
        TestSerializationDeserialization(value);
    }
    {
        unsigned long value = 3'999'999;
        TestSerializationDeserialization(value);
    }
    {
        long long value = -8'000'000'000'000LL;
        TestSerializationDeserialization(value);
        TestDeserialization<long long int, unsigned long long int>(
            1000000,
            1000000U);
    }
    {
        unsigned long long value = 16'000'000'000'000uLL;
        TestSerializationDeserialization(value);
    }

    {
        double value = 2.7182818284590452353602874713527e12;
        TestSerializationDeserialization(value);
        TestDeserialization<double, i64>(1.0, 1);
        TestDeserialization<double, ui64>(1.0, 1U);
    }

    {
        TString value = "abacaba";
        TestSerializationDeserialization(value);
    }

    {
        bool value = true;
        TestSerializationDeserialization(value);
        value = false;
        TestSerializationDeserialization(value);

        TestDeserialization(true, TString("true"));
        TestDeserialization(false, TString("false"));

        TestDeserialization(false, i64(0));
        TestDeserialization(true, i64(1));
        TestDeserialization(false, ui64(0));
        TestDeserialization(true, ui64(1));

        EXPECT_THROW(ConvertTo<bool>(ConvertTo<INodePtr>(i64(-1))), std::exception);
        EXPECT_THROW(ConvertTo<bool>(ConvertTo<INodePtr>(ui64(12))), std::exception);
        EXPECT_THROW(PullParserConvert<bool>(ConvertToYsonString(i64(-1))), std::exception);
        EXPECT_THROW(PullParserConvert<bool>(ConvertToYsonString(ui64(12U))), std::exception);
    }

    {
        char value = 'a';
        TestSerializationDeserialization(value);
        value = 'Z';
        TestSerializationDeserialization(value);
    }
}

TEST(TSerializationTest, PackRefs)
{
    std::vector<TSharedRef> refs;
    refs.push_back(TSharedRef::FromString("abc"));
    refs.push_back(TSharedRef::FromString("12"));

    auto packed = PackRefs(refs);
    auto unpacked = UnpackRefs(packed);

    EXPECT_EQ(2u, unpacked.size());
    EXPECT_EQ("abc", ToString(unpacked[0]));
    EXPECT_EQ("12", ToString(unpacked[1]));
}

TEST(TSerializationTest, Map)
{
    std::map<TString, size_t> original{{"First", 12U}, {"Second", 7883U}, {"Third", 7U}};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::MapFragment);
}

TEST(TSerializationTest, Set)
{
    std::set<TString> original{"First", "Second", "Third"};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, MultiSet)
{
    std::multiset<TString> original{"First", "Second", "Third", "Second", "Third", "Third"};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, MultiMap)
{
    std::multimap<TString, size_t> original{{"First", 12U}, {"Second", 7883U}, {"Third", 7U}};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::MapFragment);
}

TEST(TSerializationTest, MultiMapErrorDuplicateKey)
{
    std::multimap<TString, size_t> original{{"First", 12U}, {"Second", 7883U}, {"First", 2U}, {"Second", 3U}};
    auto yson = ConvertToYsonString(original);
    EXPECT_THROW(ConvertTo<std::decay<decltype(original)>::type>(yson), std::exception);
}

TEST(TSerializationTest, UnorderedMap)
{
    std::unordered_map<TString, size_t> original{{"First", 12U}, {"Second", 7883U}, {"Third", 7U}};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::MapFragment);
}

TEST(TSerializationTest, UnorderedSet)
{
    const std::unordered_set<TString> original{"First", "Second", "Third"};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, UnorderedMultiSet)
{
    const std::unordered_multiset<TString> original{"First", "Second", "Third", "Second", "Third", "Third"};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, UnorderedMultiMap)
{
    const std::unordered_multimap<TString, size_t> original{{"First", 12U}, {"Second", 7883U}, {"Third", 7U}};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::MapFragment);
}

TEST(TSerializationTest, UnorderedMultiMapErrorDuplicateKey)
{
    const std::unordered_multimap<TString, size_t> original{{"Second", 7883U}, {"Third", 7U}, {"Second", 7U}};
    auto yson = ConvertToYsonString(original);
    EXPECT_THROW(ConvertTo<std::decay<decltype(original)>::type>(yson), std::exception);
}

TEST(TSerializationTest, Vector)
{
    const std::vector<TString> original{"First", "Second", "Third"};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, Deque)
{
    const std::deque<TString> original{"First", "Second", "Third"};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, Pair)
{
    auto original = std::pair<size_t, TString>(1U, "Second");
    TestSerializationDeserialization(original);
}

TEST(TSerializationTest, Atomic)
{
    std::atomic<size_t> original(42U);
    TestSerializationDeserialization<std::atomic<size_t>, size_t>(original);
}

TEST(TSerializationTest, Array)
{
    std::array<TString, 4> original{{"One", "Two", "3", "4"}};
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, Tuple)
{
    auto original = std::tuple<int, TString, size_t>(43, "TString", 343U);
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, VectorOfTuple)
{
    std::vector<std::tuple<int, TString, size_t>> original{
        std::tuple<int, TString, size_t>(43, "First", 343U),
        std::tuple<int, TString, size_t>(0, "Second", 7U),
        std::tuple<int, TString, size_t>(2323, "Third", 9U)
    };

    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::ListFragment);
}

TEST(TSerializationTest, MapOnArray)
{
    std::map<TString, std::array<size_t, 3>> original{
        {"1", {{2112U, 4343U, 5445U}}},
        {"22", {{54654U, 93U, 5U}}},
        {"333", {{7U, 93U, 9U}}},
        {"rel", {{233U, 9763U, 0U}}}
    };
    TestSerializationDeserialization(original);
    TestSerializationDeserialization(original, EYsonType::MapFragment);
}

TEST(TSerializationTest, Enum)
{
    for (const auto original : TEnumTraits<ETestEnum>::GetDomainValues()) {
        TestSerializationDeserialization(original);
    }
}

TEST(TSerializationTest, EnumUnknownValue)
{
    auto unknownValue = static_cast<ETestEnum>(ToUnderlying(TEnumTraits<ETestEnum>::GetMaxValue()) + 1);
    TestSerializationDeserialization(unknownValue);
}

TEST(TSerializationTest, BitEnum)
{
    for (const auto original : TEnumTraits<ETestBitEnum>::GetDomainValues()) {
        TestSerializationDeserialization(original);
    }
    TestSerializationDeserialization(ETestBitEnum::Green | ETestBitEnum::Red);
    TestSerializationDeserialization(ETestBitEnum::Green | ETestBitEnum::Red | ETestBitEnum::Yellow);
}

TEST(TSerializationTest, SerializableArcadiaEnum)
{
    for (const auto original : GetEnumAllValues<ESerializableArcadiaEnum>()) {
        TestSerializationDeserialization(original);
    }
}

TEST(TYTreeSerializationTest, Protobuf)
{
    NProto::TTestMessage message;
    message.set_int32_field(1);
    message.set_string_field("test");
    TestSerializationDeserializationNode(message);
}

TEST(TYTreeSerializationTest, ProtobufKeepUnknown)
{
    TYsonString canonicalYson(TStringBuf("{\"int32_field\"=1;\"string_field\"=\"test\";\"unknown_field\"=1;}"));
    {
        auto node = ConvertTo<NProto::TTestMessage>(canonicalYson);
        auto deserializedYson = ConvertToYsonString(node, NYson::EYsonFormat::Text);
        EXPECT_EQ(RemoveSpaces(canonicalYson.ToString()), deserializedYson.ToString());
    }
    {
        auto node = PullParserConvert<NProto::TTestMessage>(canonicalYson);
        auto deserializedYson = ConvertToYsonString(node, NYson::EYsonFormat::Text);
        EXPECT_EQ(RemoveSpaces(canonicalYson.ToString()), deserializedYson.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTestClass
{
public:
    INodePtr Attributes;
    int Field = 123;
};

class TTestClassRC
    : public TRefCounted
    , public TTestClass
{ };

DECLARE_REFCOUNTED_CLASS(TTestClassRC)
DEFINE_REFCOUNTED_TYPE(TTestClassRC)

void Deserialize(TTestClass& value, const INodePtr& node)
{
    value.Attributes = node->Attributes().ToMap();
    value.Field = node->GetValue<i64>();
}

void Deserialize(TTestClass& value, TYsonPullParserCursor* cursor)
{
    Deserialize(value, ExtractTo<INodePtr>(cursor));
}

TEST(TSerializationTest, Pointers)
{
    TYsonString yson(TStringBuf("<x=12;y=#>42"));

    auto expectedNode = BuildYsonNodeFluently().BeginMap()
        .Item("x").Value(12)
        .Item("y").Entity()
    .EndMap();

    auto check = [&] (TStringBuf message, const auto& ptr) {
        SCOPED_TRACE(message);
        ASSERT_TRUE(ptr);
        ASSERT_EQ(ptr->Field, 42);
        ASSERT_TRUE(AreNodesEqual(ptr->Attributes, expectedNode));
    };

    auto uniqueFromNode = ConvertTo<std::unique_ptr<TTestClass>>(ConvertToNode(yson));
    check("unique_ptr:FromNode", uniqueFromNode);

    auto uniqueFromPullParser = PullParserConvert<std::unique_ptr<TTestClass>>(yson);
    check("unique_ptr:FromNode", uniqueFromPullParser);

    auto intrusiveFromNode = ConvertTo<TTestClassRCPtr>(ConvertToNode(yson));
    check("intrusive:FromNode", intrusiveFromNode);

    auto intrusiveFromPullParser = PullParserConvert<TTestClassRCPtr>(yson);
    check("intrusive:FromNode", intrusiveFromPullParser);
}

TEST(TSerializationTest, PointersFromEntity)
{
    TYsonString yson(TStringBuf("<x=12;y=#>#"));

    auto check = [&] (TStringBuf message, const auto& ptr) {
        SCOPED_TRACE(message);
        ASSERT_TRUE(ptr);
        ASSERT_EQ(ptr->Field, 123);
        ASSERT_EQ(ptr->Attributes, nullptr);
    };

    auto uniqueFromNode = ConvertTo<std::unique_ptr<TTestClass>>(ConvertToNode(yson));
    check("unique_ptr:FromNode", uniqueFromNode);

    auto uniqueFromPullParser = PullParserConvert<std::unique_ptr<TTestClass>>(yson);
    check("unique_ptr:FromNode", uniqueFromPullParser);

    auto intrusiveFromNode = ConvertTo<TTestClassRCPtr>(ConvertToNode(yson));
    check("intrusive:FromNode", intrusiveFromNode);

    auto intrusiveFromPullParser = PullParserConvert<TTestClassRCPtr>(yson);
    check("intrusive:FromNode", intrusiveFromPullParser);
}

TEST(TDeserializeTest, Enums)
{
    {
        auto originalYson = BuildYsonNodeFluently()
            .BeginList()
                .Item().Value(1)
                .Item().Value("forty_two")
            .EndList();
        auto expectedEnums = std::vector<ETestEnum>{ETestEnum::One, ETestEnum::FortyTwo};

        std::vector<ETestEnum> deserialized;
        Deserialize(deserialized, originalYson);
        EXPECT_EQ(deserialized, expectedEnums);

        deserialized.clear();
        EXPECT_THROW(
            Deserialize(deserialized, BuildYsonNodeFluently().BeginList().Item().Value(1.42).EndList()),
            std::exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
