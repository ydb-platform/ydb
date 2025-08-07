#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/polymorphic_yson_struct.h>
#include <yt/yt/core/ytree/size.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/ytree/unittests/proto/test.pb.h>

#include <util/stream/buffer.h>

#include <util/ysaveload.h>

#include <array>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestEnum,
    (Value0)
    (Value1)
    (Value2)
);

////////////////////////////////////////////////////////////////////////////////

struct TTestSubconfig
    : public TYsonStruct
{
    int MyInt;
    unsigned int MyUint;
    bool MyBool;
    std::vector<TString> MyStringList;
    ETestEnum MyEnum;
    TDuration MyDuration;
    TSize MySize;

    REGISTER_YSON_STRUCT(TTestSubconfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_int", &TThis::MyInt)
            .Default(100)
            .InRange(95, 205);
        registrar.Parameter("my_uint", &TThis::MyUint)
            .Default(50)
            .InRange(31, 117);
        registrar.Parameter("my_bool", &TThis::MyBool)
            .Default(false);
        registrar.Parameter("my_string_list", &TThis::MyStringList)
            .Default();
        registrar.Parameter("my_enum", &TThis::MyEnum)
            .Default(ETestEnum::Value1);
        registrar.Parameter("my_duration", &TThis::MyDuration)
            .Default(TDuration::Seconds(1));
        registrar.Parameter("my_size", &TThis::MySize)
            .Default(TSize::FromString("8K"));
    }
};

using TTestSubconfigPtr = TIntrusivePtr<TTestSubconfig>;

////////////////////////////////////////////////////////////////////////////////

struct TTestConfig
    : public TYsonStruct
{
    TString MyString;
    TTestSubconfigPtr Subconfig;
    std::vector<TTestSubconfigPtr> SubconfigList;
    std::unordered_map<TString, TTestSubconfigPtr> SubconfigMap;
    std::optional<i64> NullableInt;

    REGISTER_YSON_STRUCT(TTestConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        registrar.Parameter("my_string", &TThis::MyString)
            .NonEmpty();
        registrar.Parameter("sub", &TThis::Subconfig)
            .DefaultNew();
        registrar.Parameter("sub_list", &TThis::SubconfigList)
            .Default();
        registrar.Parameter("sub_map", &TThis::SubconfigMap)
            .Default();
        registrar.Parameter("nullable_int", &TThis::NullableInt)
            .Default();

        registrar.Preprocessor([] (TTestConfig* config) {
            config->MyString = "x";
            config->Subconfig->MyInt = 200;
        });
    }
};

using TTestConfigPtr = TIntrusivePtr<TTestConfig>;

////////////////////////////////////////////////////////////////////////////////

class TSimpleYsonStruct
    : public TYsonStruct
{
public:
    int IntValue;

    REGISTER_YSON_STRUCT(TSimpleYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int_value", &TSimpleYsonStruct::IntValue)
            .Default(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

auto GetCompleteConfigNode(int offset = 0)
{
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString" + std::to_string(offset))
            .Item("sub").BeginMap()
                .Item("my_int").Value(99 + offset)
                .Item("my_uint").Value(101 + offset)
                .Item("my_bool").Value(true)
                .Item("my_enum").Value("value2")
                .Item("my_string_list").BeginList()
                    .Item().Value("ListItem0")
                    .Item().Value("ListItem1")
                    .Item().Value("ListItem2")
                .EndList()
                .Item("my_duration").Value("2h")
                .Item("my_size").Value("2M")
            .EndMap()
            .Item("sub_list").BeginList()
                .Item().BeginMap()
                    .Item("my_int").Value(99 + offset)
                    .Item("my_uint").Value(101 + offset)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                    .Item("my_duration").Value("2h")
                    .Item("my_size").Value(2'000'000)
                .EndMap()
                .Item().BeginMap()
                    .Item("my_int").Value(99 + offset)
                    .Item("my_uint").Value(101 + offset)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                    .Item("my_duration").Value("2h")
                    .Item("my_size").Value("2000K")
                .EndMap()
            .EndList()
            .Item("sub_map").BeginMap()
                .Item("sub1").BeginMap()
                    .Item("my_int").Value(99 + offset)
                    .Item("my_uint").Value(101 + offset)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                    .Item("my_duration").Value("2h")
                    .Item("my_size").Value(2'000'000)
                .EndMap()
                .Item("sub2").BeginMap()
                    .Item("my_int").Value(99 + offset)
                    .Item("my_uint").Value(101 + offset)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                    .Item("my_duration").Value(2 * 60 * 60 * 1000)
                    .Item("my_size").Value(2'000'000)
                .EndMap()
            .EndMap()
        .EndMap();
}

void TestCompleteSubconfig(TTestSubconfig* subconfig, int offset = 0)
{
    for (auto field : {"my_int", "my_uint", "my_bool", "my_enum", "my_string_list", "my_duration", "my_size"}) {
        EXPECT_TRUE(subconfig->IsSet(field));
    }

    EXPECT_EQ(99 + offset, subconfig->MyInt);
    EXPECT_EQ(101u + offset, subconfig->MyUint);
    EXPECT_TRUE(subconfig->MyBool);
    EXPECT_EQ(3u, subconfig->MyStringList.size());
    EXPECT_EQ("ListItem0", subconfig->MyStringList[0]);
    EXPECT_EQ("ListItem1", subconfig->MyStringList[1]);
    EXPECT_EQ("ListItem2", subconfig->MyStringList[2]);
    EXPECT_EQ(ETestEnum::Value2, subconfig->MyEnum);
    EXPECT_EQ(TDuration::Hours(2), subconfig->MyDuration);
    EXPECT_EQ(2'000'000, subconfig->MySize);
}

void TestCompleteConfig(TIntrusivePtr<TTestConfig> config, int offset = 0)
{
    EXPECT_EQ("TestString" + std::to_string(offset), config->MyString);
    EXPECT_TRUE(config->IsSet("my_string"));

    EXPECT_TRUE(config->IsSet("sub"));
    TestCompleteSubconfig(config->Subconfig.Get(), offset);

    EXPECT_EQ(2u, config->SubconfigList.size());
    EXPECT_TRUE(config->IsSet("sub_list"));
    TestCompleteSubconfig(config->SubconfigList[0].Get(), offset);
    TestCompleteSubconfig(config->SubconfigList[1].Get(), offset);

    EXPECT_EQ(2u, config->SubconfigMap.size());
    EXPECT_TRUE(config->IsSet("sub_map"));
    auto it1 = config->SubconfigMap.find("sub1");
    EXPECT_FALSE(it1 == config->SubconfigMap.end());
    TestCompleteSubconfig(it1->second.Get(), offset);
    auto it2 = config->SubconfigMap.find("sub2");
    EXPECT_FALSE(it2 == config->SubconfigMap.end());
    TestCompleteSubconfig(it2->second.Get(), offset);
}

// {LoadFromNode}
using TYsonStructParseTestParameter = std::tuple<bool>;

class TYsonStructParseTest
    : public ::testing::TestWithParam<TYsonStructParseTestParameter>
{
public:
    template <typename T>
    TIntrusivePtr<T> Load(
        const INodePtr& node,
        bool postprocess = true,
        bool setDefaults = true,
        const NYPath::TYPath& path = {})
    {
        auto [loadFromNode] = GetParam();
        auto config = New<T>();
        if (loadFromNode) {
            config->Load(node, postprocess, setDefaults, path);
        } else {
            auto ysonString = ConvertToYsonString(node);
            auto string = ysonString.ToString();
            TStringInput input(string);
            TYsonPullParser parser(&input, EYsonType::Node);
            auto cursor = TYsonPullParserCursor(&parser);
            config->Load(&cursor, postprocess, setDefaults, path);
        }
        return config;
    }
};

INSTANTIATE_TEST_SUITE_P(
    LoadFromNode,
    TYsonStructParseTest,
    ::testing::Values(TYsonStructParseTestParameter{
        true
    })
);

INSTANTIATE_TEST_SUITE_P(
    LoadFromCursor,
    TYsonStructParseTest,
    ::testing::Values(TYsonStructParseTestParameter{
        false
    })
);

TEST_P(TYsonStructParseTest, Complete)
{
    auto configNode = GetCompleteConfigNode();

    auto config = Load<TTestConfig>(configNode->AsMap());

    TestCompleteConfig(config);
}

TEST_P(TYsonStructParseTest, MissingParameter)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_bool").Value(true)
            .EndMap()
        .EndMap();

    auto config = Load<TTestConfig>(configNode->AsMap());

    EXPECT_EQ("TestString", config->MyString);
    EXPECT_TRUE(config->IsSet("my_string"));
    EXPECT_TRUE(config->IsSet("sub"));
    EXPECT_FALSE(config->IsSet("sub_list"));
    EXPECT_FALSE(config->IsSet("sub_map"));

    EXPECT_EQ(200, config->Subconfig->MyInt);
    EXPECT_TRUE(config->Subconfig->MyBool);
    EXPECT_EQ(0u, config->Subconfig->MyStringList.size());
    EXPECT_EQ(ETestEnum::Value1, config->Subconfig->MyEnum);
    EXPECT_EQ(0u, config->SubconfigList.size());
    EXPECT_EQ(0u, config->SubconfigMap.size());
    EXPECT_TRUE(config->Subconfig->IsSet("my_bool"));
    for (auto field : {"my_int", "my_uint", "my_enum", "my_string_list"}) {
        EXPECT_FALSE(config->Subconfig->IsSet(field));
    }
}

TEST_P(TYsonStructParseTest, MissingSubconfig)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
        .EndMap();

    auto config = Load<TTestConfig>(configNode->AsMap());

    EXPECT_EQ("TestString", config->MyString);
    EXPECT_EQ(200, config->Subconfig->MyInt);
    EXPECT_FALSE(config->Subconfig->MyBool);
    EXPECT_EQ(0u, config->Subconfig->MyStringList.size());
    EXPECT_EQ(ETestEnum::Value1, config->Subconfig->MyEnum);
    EXPECT_EQ(0u, config->SubconfigList.size());
    EXPECT_EQ(0u, config->SubconfigMap.size());
    for (auto field : {"my_int", "my_uint", "my_bool", "my_enum", "my_string_list"}) {
        EXPECT_FALSE(config->Subconfig->IsSet(field));
    }
}

TEST_P(TYsonStructParseTest, UnrecognizedSimple)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("option").Value(1)
        .EndMap();

    auto config = Load<TTestConfig>(configNode->AsMap());

    auto unrecognizedNode = config->GetLocalUnrecognized();
    auto unrecognizedRecursivelyNode = config->GetRecursiveUnrecognized();
    EXPECT_TRUE(AreNodesEqual(unrecognizedNode, unrecognizedRecursivelyNode));
    EXPECT_EQ(1, unrecognizedNode->GetChildCount());
    for (const auto& [key, child] : unrecognizedNode->GetChildren()) {
        EXPECT_EQ("option", key);
        EXPECT_EQ(1, child->AsInt64()->GetValue());
    }

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);
    auto deserializedConfig = ConvertTo<TTestConfigPtr>(output);
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(config), ConvertToNode(deserializedConfig)));
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleStructKeepUnrecognized
    : public TYsonStruct
{
public:
    int Value;

    REGISTER_YSON_STRUCT(TSimpleStructKeepUnrecognized);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        registrar.Parameter("value", &TSimpleStructKeepUnrecognized::Value)
            .Default(1);
    }
};

TEST_P(TYsonStructParseTest, UnrecognizedSorted)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("value_unrecognized").Value(42)
            .Item("unrecognized").Value("TestString")
            .Item("value").Value(1337)
            .Item("a_unrecognized").Value("TestString")
        .EndMap();

    auto config = Load<TSimpleStructKeepUnrecognized>(configNode->AsMap());

    auto unrecognizedNode = config->GetLocalUnrecognized();
    auto unrecognizedRecursivelyNode = config->GetRecursiveUnrecognized();
    EXPECT_TRUE(AreNodesEqual(unrecognizedNode, unrecognizedRecursivelyNode));
    EXPECT_EQ(3, unrecognizedNode->GetChildCount());

    TString expectedYson;
    expectedYson += "{\"a_unrecognized\"=\"TestString\";";
    expectedYson += "\"value\"=1337;";
    expectedYson += "\"value_unrecognized\"=42;";
    expectedYson += "\"unrecognized\"=\"TestString\";}";

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(TYsonString(expectedYson)),
        ConvertToNode(TYsonString(output.AsStringBuf()))))
        << "Expected: " << expectedYson
        << ", got: " << output.AsStringBuf();
}

////////////////////////////////////////////////////////////////////////////////

template <EUnrecognizedStrategy strategy>
class TThrowOnUnrecognized
    : public TYsonStruct
{
public:
    int IntValue;

    TIntrusivePtr<TSimpleYsonStruct> Nested;

    REGISTER_YSON_STRUCT(TThrowOnUnrecognized);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(strategy);

        registrar.Parameter("int_value", &TThrowOnUnrecognized::IntValue)
            .Default(1);

        registrar.Parameter("nested", &TThrowOnUnrecognized::Nested)
            .DefaultNew();
    }
};

TEST_P(TYsonStructParseTest, UnrecognizedThrow)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("unrecognized").Value(1)
        .EndMap();

    Load<TThrowOnUnrecognized<EUnrecognizedStrategy::Drop>>(configNode->AsMap());
    EXPECT_THROW_WITH_SUBSTRING(
        Load<TThrowOnUnrecognized<EUnrecognizedStrategy::Throw>>(configNode->AsMap()),
        "Unrecognized field \"/unrecognized\" has been encountered");
}

TEST_P(TYsonStructParseTest, UnrecognizedThrowRecursive)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("nested").BeginMap()
                .Item("unrecognized").Value(1)
            .EndMap()
        .EndMap();

    Load<TThrowOnUnrecognized<EUnrecognizedStrategy::Drop>>(configNode->AsMap());
    EXPECT_THROW_WITH_SUBSTRING(
        Load<TThrowOnUnrecognized<EUnrecognizedStrategy::ThrowRecursive>>(configNode->AsMap()),
        "Unrecognized field \"/nested/unrecognized\" has been encountered");
}

TEST_P(TYsonStructParseTest, UnrecognizedRecursive)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("option").Value(1)
            .Item("sub").BeginMap()
                .Item("sub_option").Value(42)
            .EndMap()
        .EndMap();

    auto config = Load<TTestConfig>(configNode->AsMap());

    auto unrecognizedRecursivelyNode = config->GetRecursiveUnrecognized();
    EXPECT_EQ(2, unrecognizedRecursivelyNode->GetChildCount());
    for (const auto& [key, child] : unrecognizedRecursivelyNode->GetChildren()) {
        if (key == "option") {
            EXPECT_EQ(1, child->AsInt64()->GetValue());
        } else {
            EXPECT_EQ("sub", key);
            EXPECT_EQ(42, child->AsMap()->GetChildOrThrow("sub_option")->AsInt64()->GetValue());
        }
    }

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);
    auto deserializedConfig = ConvertTo<TTestConfigPtr>(output);
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(config), ConvertToNode(deserializedConfig)));
}

TEST_P(TYsonStructParseTest, UnknownIsSet)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("foo").Value(1)
        .EndMap();

    auto config = Load<TTestConfig>(configNode->AsMap());

    EXPECT_THROW(config->IsSet("foo"), std::exception);
    EXPECT_THROW(config->IsSet("bar"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

class TConfigWithOneLevelNesting
    : public TYsonStruct
{
public:
    TTestSubconfigPtr Subconfig;

    REGISTER_YSON_STRUCT(TConfigWithOneLevelNesting);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub", &TThis::Subconfig)
            .DefaultNew();
    }
};

class TConfigWithTwoLevelNesting
    : public TYsonStruct
{
public:
    TIntrusivePtr<TConfigWithOneLevelNesting> Subconfig;

    REGISTER_YSON_STRUCT(TConfigWithTwoLevelNesting);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        registrar.Parameter("subconfig", &TThis::Subconfig)
            .DefaultNew();
    }
};

TEST_P(TYsonStructParseTest, UnrecognizedRecursiveTwoLevelNesting)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("subconfig").BeginMap()
                .Item("sub").BeginMap()
                    .Item("unrecognized_option").Value(42)
                .EndMap()
            .EndMap()
        .EndMap();

    auto config = Load<TConfigWithTwoLevelNesting>(configNode->AsMap());

    auto unrecognized = config->GetRecursiveUnrecognized();
    EXPECT_EQ(
        ConvertToYsonString(configNode, EYsonFormat::Text).AsStringBuf(),
        ConvertToYsonString(unrecognized, EYsonFormat::Text).AsStringBuf());
}

TEST_P(TYsonStructParseTest, MissingRequiredParameter)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub").BeginMap()
                .Item("my_int").Value(99)
                .Item("my_bool").Value(true)
            .EndMap()
        .EndMap();

    EXPECT_THROW(Load<TTestConfig>(configNode->AsMap()), std::exception);
}

TEST_P(TYsonStructParseTest, IncorrectNodeType)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value(1) // incorrect type
        .EndMap();
    auto configNode = builder->EndTree();

    EXPECT_THROW(Load<TTestConfig>(configNode->AsMap()), std::exception);
}

TEST_P(TYsonStructParseTest, ArithmeticOverflow)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Value(Max<i64>())
                .Item("my_bool").Value(true)
                .Item("my_enum").Value("Value2")
                .Item("my_string_list").BeginList()
                    .Item().Value("ListItem0")
                    .Item().Value("ListItem1")
                    .Item().Value("ListItem2")
                .EndList()
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    EXPECT_THROW(Load<TTestConfig>(configNode->AsMap()), std::exception);
}

TEST_P(TYsonStructParseTest, Postprocess)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("") // empty!
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfig>(configNode->AsMap(), false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST_P(TYsonStructParseTest, PostprocessSubconfig)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Value(210) // out of range
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfig>(configNode->AsMap(), false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST_P(TYsonStructParseTest, PostprocessSubconfigList)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub_list").BeginList()
                .Item().BeginMap()
                    .Item("my_int").Value(210) // out of range
                .EndMap()
            .EndList()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfig>(configNode->AsMap(), false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST_P(TYsonStructParseTest, PostprocessSubconfigMap)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub_map").BeginMap()
                .Item("sub").BeginMap()
                    .Item("my_int").Value(210) // out of range
                .EndMap()
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfig>(configNode->AsMap(), false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST(TYsonStructTest, SaveSingleParameter)
{
    auto config = New<TTestConfig>();
    config->MyString = "test";
    config->NullableInt = 10;

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    config->SaveParameter("my_string", builder.get());
    auto actual = ConvertTo<TString>(builder->EndTree());
    EXPECT_EQ("test", actual);
}

TEST(TYsonStructTest, LoadSingleParameter)
{
    auto config = New<TTestConfig>();
    config->NullableInt = 10;

    config->LoadParameter("my_string", ConvertToNode("test"));
    EXPECT_EQ("test", config->MyString);
    EXPECT_EQ(10, config->NullableInt);
    EXPECT_TRUE(config->IsSet("my_string"));
    EXPECT_FALSE(config->IsSet("sub"));
}

TEST(TYsonStructTest, LoadBadSingleParameter)
{
    auto config = New<TTestConfig>();
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        config->LoadParameter("my_string", ConvertToNode(42)),
        NYT::TErrorException,
        "has invalid type: expected \"string\", actual \"int64\"");
}

TEST(TYsonStructTest, LoadSingleParameterOverwriteDefaults)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
        .Item("my_int").Value(100)
        .EndMap();
    auto subConfig = builder->EndTree();

    auto config1 = New<TTestConfig>();
    config1->Subconfig->MyBool = true;
    config1->LoadParameter("sub", subConfig);
    EXPECT_EQ(100, config1->Subconfig->MyInt);
    EXPECT_FALSE(config1->Subconfig->MyBool);  // Subconfig is overwritten.
    EXPECT_TRUE(config1->IsSet("sub"));
    EXPECT_TRUE(config1->Subconfig->IsSet("my_int"));
    EXPECT_FALSE(config1->Subconfig->IsSet("my_bool"));
}

TEST(TYsonStructTest, ResetSingleParameter)
{
    auto config = New<TTestSubconfig>();
    config->MyInt = 10;
    config->MyUint = 10;

    config->ResetParameter("my_int");
    EXPECT_EQ(100, config->MyInt);  // Default value.
    EXPECT_EQ(10u, config->MyUint);
    EXPECT_FALSE(config->IsSet("my_int"));
}

TEST(TYsonStructTest, Save)
{
    auto config = New<TTestConfig>();

    // add non-default fields;
    config->MyString = "hello!";
    config->SubconfigList.push_back(New<TTestSubconfig>());
    config->SubconfigMap["item"] = New<TTestSubconfig>();
    config->NullableInt = 42;

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

    TString subconfigYson =
        "{\"my_bool\"=%false;"
        "\"my_enum\"=\"value1\";"
        "\"my_int\"=200;"
        "\"my_uint\"=50u;"
        "\"my_string_list\"=[];"
        "\"my_duration\"=1000;"
        "\"my_size\"=8000}";

    TString subconfigYsonOrigin =
        "{\"my_bool\"=%false;"
        "\"my_enum\"=\"value1\";"
        "\"my_int\"=100;"
        "\"my_uint\"=50u;"
        "\"my_string_list\"=[];"
        "\"my_duration\"=1000;"
        "\"my_size\"=8000}";

    TString expectedYson;
    expectedYson += "{\"my_string\"=\"hello!\";";
    expectedYson += "\"sub\"=" + subconfigYson + ";";
    expectedYson += "\"sub_list\"=[" + subconfigYsonOrigin + "];";
    expectedYson += "\"sub_map\"={\"item\"=" + subconfigYsonOrigin + "};";
    expectedYson += "\"nullable_int\"=42}";

    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(TYsonString(expectedYson)),
        ConvertToNode(TYsonString(output.AsStringBuf()))))
        << "Expected: " << expectedYson
        << ", got: " << output.AsStringBuf();
}

TEST(TYsonStructTest, TestConfigUpdate)
{
    auto config = New<TTestConfig>();
    {
        auto newConfig = UpdateYsonStruct(config, nullptr);
        EXPECT_EQ(newConfig->Subconfig->MyInt, 200);
    }

    {
        auto newConfig = UpdateYsonStruct(config, ConvertToNode(TYsonString(TStringBuf("{\"sub\"={\"my_int\"=150}}"))));
        EXPECT_EQ(newConfig->Subconfig->MyInt, 150);
        EXPECT_TRUE(newConfig->IsSet("sub"));
        EXPECT_TRUE(newConfig->Subconfig->IsSet("my_int"));

        // UpdateYsonStruct fully materializes the source node and loads is back.
        // After such process all fields (expect those that weren't saved) will be
        // explicitly loaded and thus marked as set.
        // Note that nullable_int is not saved thus is not loaded back during the update.
        EXPECT_TRUE(newConfig->Subconfig->IsSet("my_bool"));
        EXPECT_TRUE(newConfig->IsSet("sub_list"));
        EXPECT_FALSE(newConfig->IsSet("nullable_int"));
    }

    {
        auto newConfig = UpdateYsonStruct(config, ConvertToNode(TYsonString(TStringBuf("{\"sub\"={\"my_int_with_typo\"=150}}"))));
        EXPECT_EQ(newConfig->Subconfig->MyInt, 200);
        EXPECT_TRUE(newConfig->IsSet("sub"));

        // The comment above applies.
        EXPECT_TRUE(newConfig->Subconfig->IsSet("my_int"));
    }
}

TEST(TYsonStructTest, NoDefaultNewAliasing)
{
    auto config1 = New<TTestConfig>();
    auto config2 = New<TTestConfig>();
    EXPECT_NE(config1->Subconfig, config2->Subconfig);
}

TEST(TYsonStructTest, Reconfigure)
{
    auto config = New<TTestConfig>();
    auto subconfig = config->Subconfig;

    EXPECT_EQ("x", config->MyString);
    EXPECT_EQ(200, subconfig->MyInt);

    auto patch1 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("y")
        .EndMap();
    ReconfigureYsonStruct(config, patch1);

    EXPECT_EQ("y", config->MyString);
    EXPECT_EQ(subconfig, config->Subconfig);
    EXPECT_EQ(200, subconfig->MyInt);
    EXPECT_TRUE(config->IsSet("my_string"));
    EXPECT_FALSE(config->IsSet("sub"));

    auto patch2 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("z")
            .Item("sub").BeginMap()
                .Item("my_int").Value(95)
            .EndMap()
        .EndMap();
    ReconfigureYsonStruct(config, patch2);

    EXPECT_EQ("z", config->MyString);
    EXPECT_EQ(subconfig, config->Subconfig);
    EXPECT_EQ(95, subconfig->MyInt);
    EXPECT_TRUE(config->IsSet("my_string"));
    EXPECT_TRUE(config->IsSet("sub"));
    EXPECT_TRUE(config->Subconfig->IsSet("my_int"));
    EXPECT_FALSE(config->Subconfig->IsSet("my_bool"));
}

struct TTestYsonStructWithFieldInitializer
    : public TYsonStruct
{
    TTestSubconfigPtr Sub = New<TTestSubconfig>();

    REGISTER_YSON_STRUCT(TTestYsonStructWithFieldInitializer);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub", &TThis::Sub)
            .DefaultNew();
    }
};

TEST(TYsonStructTest, TestNestedWithFieldInitializer)
{
    using TConfig = TTestYsonStructWithFieldInitializer;
    using TPtr = TIntrusivePtr<TConfig>;

    auto yson = ConvertTo<TPtr>(TYsonString(TStringBuf("{}")));
}

////////////////////////////////////////////////////////////////////////////////

#define FOR_EACH_FIELD_1(XX) \
    XX(Field00, field00) \
    XX(Field01, field01) \
    XX(Field02, field02) \
    XX(Field03, field03) \
    XX(Field04, field04) \
    XX(Field05, field05) \
    XX(Field06, field06) \
    XX(Field07, field07) \
    XX(Field08, field08) \
    XX(Field09, field09) \
    XX(Field10, field10) \
    XX(Field11, field11) \
    XX(Field12, field12) \
    XX(Field13, field13) \
    XX(Field14, field14) \
    XX(Field15, field15) \
    XX(Field16, field16) \
    XX(Field17, field17) \
    XX(Field18, field18) \
    XX(Field19, field19) \
    XX(Field20, field20) \
    XX(Field21, field21) \
    XX(Field22, field22) \
    XX(Field23, field23) \
    XX(Field24, field24) \
    XX(Field25, field25) \
    XX(Field26, field26) \
    XX(Field27, field27) \
    XX(Field28, field28) \
    XX(Field29, field29) \

#define FOR_EACH_FIELD_2(XX) \
    XX(Field30, field30) \
    XX(Field31, field31) \
    XX(Field32, field32) \
    XX(Field33, field33) \
    XX(Field34, field34) \
    XX(Field35, field35) \
    XX(Field36, field36) \
    XX(Field37, field37) \
    XX(Field38, field38) \
    XX(Field39, field39) \
    XX(Field40, field40) \
    XX(Field41, field41) \
    XX(Field42, field42) \
    XX(Field43, field43) \
    XX(Field44, field44) \
    XX(Field45, field45) \
    XX(Field46, field46) \
    XX(Field47, field47) \
    XX(Field48, field48) \
    XX(Field49, field49) \
    XX(Field50, field50) \
    XX(Field51, field51) \
    XX(Field52, field52) \
    XX(Field53, field53) \
    XX(Field54, field54) \
    XX(Field55, field55) \
    XX(Field56, field56) \
    XX(Field57, field57) \
    XX(Field58, field58) \
    XX(Field59, field59) \

#define FOR_EACH_FIELD_3(XX) \
    XX(Field60, field60) \
    XX(Field61, field61) \
    XX(Field62, field62) \
    XX(Field63, field63) \
    XX(Field64, field64) \
    XX(Field65, field65) \
    XX(Field66, field66) \
    XX(Field67, field67) \
    XX(Field68, field68) \
    XX(Field69, field69) \
    XX(Field70, field70) \
    XX(Field71, field71) \
    XX(Field72, field72) \
    XX(Field73, field73) \
    XX(Field74, field74) \
    XX(Field75, field75) \
    XX(Field76, field76) \
    XX(Field77, field77) \
    XX(Field78, field78) \
    XX(Field79, field79) \
    XX(Field80, field80) \
    XX(Field81, field81) \
    XX(Field82, field82) \
    XX(Field83, field83) \
    XX(Field84, field84) \
    XX(Field85, field85) \
    XX(Field86, field86) \
    XX(Field87, field87) \
    XX(Field88, field88) \
    XX(Field89, field89) \

#define DECLARE_FIELD(CamelCase, snake_case) \
    int CamelCase;

#define REGISTER_FIELD(CamelCase, snake_case) \
    registrar.Parameter(#snake_case, &TThis::CamelCase) \
        .Optional();

struct TTestConfigWithManyFieldsBase
    : public virtual TYsonStruct
{
    FOR_EACH_FIELD_1(DECLARE_FIELD);

    REGISTER_YSON_STRUCT(TTestConfigWithManyFieldsBase);

    static void Register(TRegistrar registrar)
    {
        FOR_EACH_FIELD_1(REGISTER_FIELD);
    }
};

struct TTestConfigWithManyFieldsDerived1
    : public virtual TTestConfigWithManyFieldsBase
{
    FOR_EACH_FIELD_2(DECLARE_FIELD);

    REGISTER_YSON_STRUCT(TTestConfigWithManyFieldsDerived1);

    static void Register(TRegistrar registrar)
    {
        FOR_EACH_FIELD_2(REGISTER_FIELD);
    }
};

struct TTestConfigWithManyFieldsDerived2
    : public virtual TTestConfigWithManyFieldsBase
{
    FOR_EACH_FIELD_3(DECLARE_FIELD);

    REGISTER_YSON_STRUCT(TTestConfigWithManyFieldsDerived2);

    static void Register(TRegistrar registrar)
    {
        FOR_EACH_FIELD_3(REGISTER_FIELD);
    }
};

struct TTestConfigWithManyFieldsFinal
    : public TTestConfigWithManyFieldsDerived1
    , public TTestConfigWithManyFieldsDerived2
{
    TString String;

    REGISTER_YSON_STRUCT(TTestConfigWithManyFieldsFinal);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("string", &TThis::String)
            .Default();
    }
};

struct TTestConfigWithManyFieldsBaseLite
    : public virtual TYsonStructLiteWithFieldTracking
{
    FOR_EACH_FIELD_1(DECLARE_FIELD);

    REGISTER_YSON_STRUCT_LITE(TTestConfigWithManyFieldsBaseLite);

    static void Register(TRegistrar registrar)
    {
        FOR_EACH_FIELD_1(REGISTER_FIELD);
    }
};

struct TTestConfigWithManyFieldsDerivedLite1
    : public virtual TTestConfigWithManyFieldsBaseLite
{
    FOR_EACH_FIELD_2(DECLARE_FIELD);

    REGISTER_YSON_STRUCT_LITE(TTestConfigWithManyFieldsDerivedLite1);

    static void Register(TRegistrar registrar)
    {
        FOR_EACH_FIELD_2(REGISTER_FIELD);
    }
};

struct TTestConfigWithManyFieldsDerivedLite2
    : public virtual TTestConfigWithManyFieldsBaseLite
{
    FOR_EACH_FIELD_3(DECLARE_FIELD);

    REGISTER_YSON_STRUCT_LITE(TTestConfigWithManyFieldsDerivedLite2);

    static void Register(TRegistrar registrar)
    {
        FOR_EACH_FIELD_3(REGISTER_FIELD);
    }
};

struct TTestConfigWithManyFieldsFinalLite
    : public TTestConfigWithManyFieldsDerivedLite1
    , public TTestConfigWithManyFieldsDerivedLite2
{
    TString String;

    REGISTER_YSON_STRUCT_LITE(TTestConfigWithManyFieldsFinalLite);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("string", &TThis::String)
            .Default();
    }
};

#undef FOR_EACH_FIELD_1
#undef FOR_EACH_FIELD_2
#undef FOR_EACH_FIELD_3
#undef DECLARE_FIELD
#undef REGISTER_FIELD

TEST(TYsonStructTest, TestIsSetWithManyFields)
{
    // Test compact bitmap with >62 fields to require allocations.

    auto config = ConvertTo<TIntrusivePtr<TTestConfigWithManyFieldsFinal>>(
        TYsonString(TStringBuf(
            "{field04=4;field19=19;field55=55;string=foo;}"
        ))
    );

    EXPECT_TRUE(config->IsSet("field04"));
    EXPECT_TRUE(config->IsSet("field19"));
    EXPECT_TRUE(config->IsSet("field55"));
    EXPECT_TRUE(config->IsSet("string"));

    EXPECT_FALSE(config->IsSet("field14"));
    EXPECT_FALSE(config->IsSet("field49"));
    EXPECT_FALSE(config->IsSet("field76"));
}

TEST(TYsonStructTest, TestIsSetWithManyFieldsLite)
{
    // Test compact bitmap with >62 fields to require allocations.

    auto config = ConvertTo<TTestConfigWithManyFieldsFinalLite>(
        TYsonString(TStringBuf(
            "{field04=4;field19=19;field55=55;string=foo;}"
        ))
    );

    EXPECT_TRUE(config.IsSet("field04"));
    EXPECT_TRUE(config.IsSet("field19"));
    EXPECT_TRUE(config.IsSet("field55"));
    EXPECT_TRUE(config.IsSet("string"));

    EXPECT_FALSE(config.IsSet("field14"));
    EXPECT_FALSE(config.IsSet("field49"));
    EXPECT_FALSE(config.IsSet("field76"));

    auto copy = config;

    EXPECT_TRUE(copy.IsSet("field04"));
    EXPECT_TRUE(copy.IsSet("field19"));
    EXPECT_TRUE(copy.IsSet("field55"));
    EXPECT_TRUE(copy.IsSet("string"));

    EXPECT_FALSE(copy.IsSet("field14"));
    EXPECT_FALSE(copy.IsSet("field49"));
    EXPECT_FALSE(copy.IsSet("field76"));

    auto moved = std::move(copy);

    EXPECT_TRUE(moved.IsSet("field04"));
    EXPECT_TRUE(moved.IsSet("field19"));
    EXPECT_TRUE(moved.IsSet("field55"));
    EXPECT_TRUE(moved.IsSet("string"));

    EXPECT_FALSE(moved.IsSet("field14"));
    EXPECT_FALSE(moved.IsSet("field49"));
    EXPECT_FALSE(moved.IsSet("field76"));

    EXPECT_FALSE(copy.IsSet("field04"));
    EXPECT_FALSE(copy.IsSet("field19"));
    EXPECT_FALSE(copy.IsSet("field55"));
    EXPECT_FALSE(copy.IsSet("string"));

    copy.Load(ConvertToNode(TYsonString(TStringBuf("{field05=5;field77=7;}"))));
    EXPECT_TRUE(copy.IsSet("field05"));
    EXPECT_TRUE(copy.IsSet("field77"));
    EXPECT_FALSE(copy.IsSet("field10"));
    EXPECT_FALSE(copy.IsSet("field55"));
    EXPECT_FALSE(copy.IsSet("string"));
}

////////////////////////////////////////////////////////////////////////////////

class TTestConfigLite
    : public TYsonStructLite
{
public:
    TString MyString;
    std::optional<i64> NullableInt;

    REGISTER_YSON_STRUCT_LITE(TTestConfigLite);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_string", &TThis::MyString).NonEmpty();
        registrar.Parameter("nullable_int", &TThis::NullableInt).Default();
    }
};

TEST(TYsonStructTest, SaveLite)
{
    TTestConfigLite config;

    config.MyString = "hello!";
    config.NullableInt = 42;

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

    TString expectedYson;
    expectedYson += "{\"my_string\"=\"hello!\";";
    expectedYson += "\"nullable_int\"=42}";

    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(TYsonString(expectedYson)),
        ConvertToNode(TYsonString(output.AsStringBuf()))));
}

TEST(TYsonStructTest, NewRefCountedInitedWithDefaults)
{
    auto config = New<TTestConfig>();
    EXPECT_EQ(config->MyString, "x");
    EXPECT_TRUE(config->Subconfig != nullptr);
    EXPECT_EQ(config->Subconfig->MyInt, 200);
}

class TTestLiteWithDefaults
    : public TYsonStructLite
{
public:
    TString MyString;
    int MyInt;
    TTestSubconfigPtr Subconfig;

    REGISTER_YSON_STRUCT_LITE(TTestLiteWithDefaults);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_string", &TThis::MyString)
            .Default("y");
        registrar.Parameter("subconfig", &TThis::Subconfig)
            .DefaultNew();
        registrar.Preprocessor([] (TTestLiteWithDefaults* conf) {
            conf->MyInt = 10;
        });
    }
};

TEST(TYsonStructTest, NewLiteInitedWithDefaults)
{
    TTestLiteWithDefaults config;
    EXPECT_EQ(config.MyString, "y");
    EXPECT_EQ(config.MyInt, 10);
    EXPECT_TRUE(config.Subconfig != nullptr);
    EXPECT_EQ(config.Subconfig->MyInt, 100);
}

TEST(TYsonStructTest, TestConvertToLite)
{
    auto deserialized = ConvertTo<TTestLiteWithDefaults>(TYsonString(TStringBuf("{}")));
    EXPECT_EQ(deserialized.MyString, "y");
    EXPECT_EQ(deserialized.MyInt, 10);
    EXPECT_NE(deserialized.Subconfig, nullptr);
}

////////////////////////////////////////////////////////////////////////////////

class TTestConfigLiteWithFieldTracking
    : public TYsonStructLiteWithFieldTracking
{
public:
    TString MyString;
    std::optional<i64> NullableInt;

    REGISTER_YSON_STRUCT_LITE(TTestConfigLiteWithFieldTracking);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_string", &TThis::MyString).Default("foo");
        registrar.Parameter("nullable_int", &TThis::NullableInt).Default();
    }
};

TEST(TYsonStructTest, TestLiteFieldTracking)
{
    TTestConfigLiteWithFieldTracking config;
    EXPECT_FALSE(config.IsSet("my_string"));
    EXPECT_FALSE(config.IsSet("nullable_int"));

    config.Load(ConvertToNode(TYsonString(TStringBuf("{my_string=foo}"))));
    EXPECT_TRUE(config.IsSet("my_string"));
    EXPECT_FALSE(config.IsSet("nullable_int"));
}

TEST(TYsonStructTest, TestLiteFieldTrackingCopy)
{
    auto config1 = ConvertTo<TTestConfigLiteWithFieldTracking>(TYsonString(TStringBuf("{my_string=foo}")));

    auto config2(config1);
    EXPECT_TRUE(config2.IsSet("my_string"));
    EXPECT_FALSE(config2.IsSet("nullable_int"));

    auto config3 = ConvertTo<TTestConfigLiteWithFieldTracking>(TYsonString(TStringBuf("{nullable_int=1234}")));
    EXPECT_FALSE(config3.IsSet("my_string"));
    EXPECT_TRUE(config3.IsSet("nullable_int"));
    config3 = config1;
    EXPECT_TRUE(config3.IsSet("my_string"));
    EXPECT_FALSE(config3.IsSet("nullable_int"));
}

TEST(TYsonStructTest, TestLiteFieldTrackingMove)
{
    auto config1 = ConvertTo<TTestConfigLiteWithFieldTracking>(TYsonString(TStringBuf("{my_string=foo}")));

    auto config2(std::move(config1));
    EXPECT_TRUE(config2.IsSet("my_string"));
    EXPECT_FALSE(config2.IsSet("nullable_int"));
    EXPECT_EQ(config2.MyString, "foo");

    // IsSet() will always return false for a moved-from config.
    EXPECT_FALSE(config1.IsSet("my_string"));
    EXPECT_FALSE(config1.IsSet("nullable_int"));

    auto config3 = ConvertTo<TTestConfigLiteWithFieldTracking>(TYsonString(TStringBuf("{nullable_int=1234}")));
    EXPECT_FALSE(config3.IsSet("my_string"));
    EXPECT_TRUE(config3.IsSet("nullable_int"));
    config3 = std::move(config2);
    EXPECT_TRUE(config3.IsSet("my_string"));
    EXPECT_FALSE(config3.IsSet("nullable_int"));

    // IsSet() will always return false for a moved-from config.
    EXPECT_FALSE(config2.IsSet("my_string"));
    EXPECT_FALSE(config2.IsSet("nullable_int"));
}

TEST(TYsonStructTest, TestLiteLoadToMovedFrom)
{
    auto config1 = ConvertTo<TTestConfigLiteWithFieldTracking>(TYsonString(TStringBuf("{my_string=foo}")));

    auto config2(std::move(config1));
    EXPECT_TRUE(config2.IsSet("my_string"));
    EXPECT_FALSE(config2.IsSet("nullable_int"));
    EXPECT_EQ(config2.MyString, "foo");

    // IsSet() will always return false for a moved-from config.
    EXPECT_FALSE(config1.IsSet("my_string"));
    EXPECT_FALSE(config1.IsSet("nullable_int"));

    config1.Load(ConvertToNode(TYsonString(TStringBuf("{nullable_int=123}"))));
    EXPECT_FALSE(config1.IsSet("my_string"));
    EXPECT_TRUE(config1.IsSet("nullable_int"));
    EXPECT_EQ(config1.MyString, "foo");
    EXPECT_EQ(config1.NullableInt, 123);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestLiteFieldNormalYson
    : public virtual TYsonStruct
{
    TTestLiteWithDefaults SubLite;
    TTestConfigLiteWithFieldTracking SubTracked;

    REGISTER_YSON_STRUCT(TTestLiteFieldNormalYson);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub_lite", &TThis::SubLite)
            .Default();
        registrar.Parameter("sub_tracked", &TThis::SubTracked)
            .Default();
    }
};

struct TTestLiteFieldNormalYsonSecondBase
    : public virtual TYsonStruct
{
    TTestLiteWithDefaults SubLite2;
    TTestConfigLiteWithFieldTracking SubTracked2;

    REGISTER_YSON_STRUCT(TTestLiteFieldNormalYsonSecondBase);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub_lite_2", &TThis::SubLite2)
            .Default();
        registrar.Parameter("sub_tracked_2", &TThis::SubTracked2)
            .Default();
    }
};

struct TTestLiteFieldNormalYsonDoubleDerived
    : public TTestLiteFieldNormalYson
    , public TTestLiteFieldNormalYsonSecondBase
{
    TTestLiteWithDefaults SubLite3;
    TTestConfigLiteWithFieldTracking SubTracked3;

    REGISTER_YSON_STRUCT(TTestLiteFieldNormalYsonDoubleDerived);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub_lite_3", &TThis::SubLite3)
            .Default();
        registrar.Parameter("sub_tracked_3", &TThis::SubTracked3)
            .Default();
    }
};

TEST(TYsonStructTest, YsonStructWithLiteField)
{
    using TConfig = TTestLiteFieldNormalYson;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    {
        auto yson = ConvertTo<TConfigPtr>(TYsonString(TStringBuf("{}")));
        auto& sub = yson->SubLite;
        EXPECT_EQ(sub.MyString, "y");
        EXPECT_EQ(sub.MyInt, 10);
        EXPECT_TRUE(sub.Subconfig);
    }

    {
        auto yson = New<TConfig>();
        auto& sub = yson->SubLite;
        EXPECT_EQ(sub.MyString, "y");
        EXPECT_EQ(sub.MyInt, 10);
        EXPECT_TRUE(sub.Subconfig);
    }
}

TEST(TYsonStructTest, DoubleDerivedYsonStructWithLiteFields)
{
    using TConfig = TTestLiteFieldNormalYsonDoubleDerived;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    {
        auto yson = ConvertTo<TConfigPtr>(TYsonString(TStringBuf("{}")));

        EXPECT_FALSE(yson->IsSet("sub_lite"));
        EXPECT_FALSE(yson->IsSet("sub_tracked"));
        EXPECT_FALSE(yson->IsSet("sub_lite_2"));
        EXPECT_FALSE(yson->IsSet("sub_tracked_2"));

        {
            auto& sub = yson->SubLite;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson->SubTracked;
            EXPECT_EQ(sub.MyString, "foo");
            EXPECT_EQ(sub.NullableInt, std::nullopt);
            EXPECT_FALSE(sub.IsSet("my_string"));
            EXPECT_FALSE(sub.IsSet("nullable_int"));
        }
        {
            auto& sub = yson->SubLite2;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson->SubTracked2;
            EXPECT_EQ(sub.MyString, "foo");
            EXPECT_EQ(sub.NullableInt, std::nullopt);
            EXPECT_FALSE(sub.IsSet("my_string"));
            EXPECT_FALSE(sub.IsSet("nullable_int"));
        }
        {
            auto& sub = yson->SubLite3;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson->SubTracked3;
            EXPECT_EQ(sub.MyString, "foo");
            EXPECT_EQ(sub.NullableInt, std::nullopt);
            EXPECT_FALSE(sub.IsSet("my_string"));
            EXPECT_FALSE(sub.IsSet("nullable_int"));
        }
    }

    {
        auto yson = New<TConfig>();

        {
            auto& sub = yson->SubLite;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson->SubLite2;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson->SubLite3;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
    }

    {
        auto yson = ConvertTo<TConfigPtr>(TYsonString(TStringBuf(
            "{sub_lite={my_string=foo};sub_tracked_2={nullable_int=555}}")));

        EXPECT_TRUE(yson->IsSet("sub_lite"));
        EXPECT_TRUE(yson->IsSet("sub_tracked_2"));
        for (auto key : {"sub_lite_2", "sub_lite_3", "sub_tracked", "sub_tracked_3"}) {
            EXPECT_FALSE(yson->IsSet(key))
                << key;
        }

    {
            auto& sub = yson->SubTracked2;
            EXPECT_TRUE(sub.IsSet("nullable_int"));
            EXPECT_FALSE(sub.IsSet("my_string"));
            EXPECT_EQ(sub.NullableInt, 555);
            EXPECT_EQ(sub.MyString, "foo");
        }

        {
            auto& sub = yson->SubTracked;
            EXPECT_FALSE(sub.IsSet("nullable_int"));
            EXPECT_FALSE(sub.IsSet("my_string"));
            EXPECT_EQ(sub.NullableInt, std::nullopt);
            EXPECT_EQ(sub.MyString, "foo");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTestLiteFieldLite
    : public virtual TYsonStructLite
{
    TTestLiteWithDefaults SubLite;

    REGISTER_YSON_STRUCT_LITE(TTestLiteFieldLite);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub_lite", &TThis::SubLite)
            .Default();
    }
};

struct TTestLiteFieldLiteSecondBase
    : public virtual TYsonStructLite
{
    TTestLiteWithDefaults SubLite2;

    REGISTER_YSON_STRUCT_LITE(TTestLiteFieldLiteSecondBase);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub_lite_2", &TThis::SubLite2)
            .Default();
    }
};

struct TTestLiteFieldLiteDoubleDerived
    : public TTestLiteFieldLite
    , public TTestLiteFieldLiteSecondBase
{
    TTestLiteWithDefaults SubLite3;

    REGISTER_YSON_STRUCT_LITE(TTestLiteFieldLiteDoubleDerived);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub_lite_3", &TThis::SubLite3)
            .Default();
    }
};

TEST(TYsonStructTest, LiteWithLiteField)
{
    using TConfig = TTestLiteFieldLite;

    {
        auto yson = ConvertTo<TTestLiteFieldLite>(TYsonString(TStringBuf("{}")));
        auto& sub = yson.SubLite;
        EXPECT_EQ(sub.MyString, "y");
        EXPECT_EQ(sub.MyInt, 10);
        EXPECT_TRUE(sub.Subconfig);
    }

    {
        TConfig yson;
        auto& sub = yson.SubLite;
        EXPECT_EQ(sub.MyString, "y");
        EXPECT_EQ(sub.MyInt, 10);
        EXPECT_TRUE(sub.Subconfig);
    }
}

TEST(TYsonStructTest, DoubleDerivedLiteWithLiteFields)
{
    using TConfig = TTestLiteFieldLiteDoubleDerived;

    {
        auto yson = ConvertTo<TConfig>(TYsonString(TStringBuf("{}")));

        {
            auto& sub = yson.SubLite;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson.SubLite2;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson.SubLite3;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
    }

    {
        TConfig yson;

        {
            auto& sub = yson.SubLite;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson.SubLite2;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
        {
            auto& sub = yson.SubLite3;
            EXPECT_EQ(sub.MyString, "y");
            EXPECT_EQ(sub.MyInt, 10);
            EXPECT_TRUE(sub.Subconfig);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTestConfigWithAliases
    : public TYsonStruct
{
public:
    TString Value;

    REGISTER_YSON_STRUCT(TTestConfigWithAliases);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Value)
            .Alias("alias1")
            .Alias("alias2");
    }
};

TEST(TYsonStructTest, Aliases1)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();
    config->Load(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
    EXPECT_TRUE(config->IsSet("key"));
    EXPECT_TRUE(config->IsSet("alias1"));
}

TEST(TYsonStructTest, Aliases2)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();
    config->Load(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
    EXPECT_TRUE(config->IsSet("key"));
    EXPECT_TRUE(config->IsSet("alias1"));
}

TEST(TYsonStructTest, Aliases3)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value")
            .Item("alias2").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();
    config->Load(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
}

TEST(TYsonStructTest, Aliases4)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value1")
            .Item("alias2").Value("value2")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();

    EXPECT_THROW(config->Load(configNode->AsMap()), std::exception);
    EXPECT_FALSE(config->IsSet("key"));
}

TEST(TYsonStructTest, Aliases5)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();

    EXPECT_THROW(config->Load(configNode->AsMap()), std::exception);
    EXPECT_FALSE(config->IsSet("key"));
}

struct TTestConfigWithContainers
    : public NYTree::TYsonStructLite
{
    std::vector<TString> Vector;
    std::array<TString, 3> Array;
    std::pair<size_t, TString> Pair;
    std::set<TString> Set;
    std::map<TString, int> Map;
    std::multiset<int> MultiSet;
    std::unordered_set<TString> UnorderedSet;
    std::unordered_map<TString, int> UnorderedMap;
    std::unordered_multiset<size_t> UnorderedMultiSet;

    REGISTER_YSON_STRUCT_LITE(TTestConfigWithContainers);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("vector", &TThis::Vector)
            .Default();
        registrar.Parameter("array", &TThis::Array)
            .Default();
        registrar.Parameter("pair", &TThis::Pair)
            .Default();
        registrar.Parameter("set", &TThis::Set)
            .Default();
        registrar.Parameter("map", &TThis::Map)
            .Default();
        registrar.Parameter("multiset", &TThis::MultiSet)
            .Default();
        registrar.Parameter("unordered_set", &TThis::UnorderedSet)
            .Default();
        registrar.Parameter("unordered_map", &TThis::UnorderedMap)
            .Default();
        registrar.Parameter("unordered_multiset", &TThis::UnorderedMultiSet)
            .Default();
    }
};

TEST(TYsonStructTest, ParameterTuplesAndContainers)
{
    TTestConfigWithContainers original;
    TTestConfigWithContainers deserialized;

    original.Vector = { "fceswf", "sadfcesa" };
    original.Array = {{ "UYTUY", ":LL:a", "78678678" }};
    original.Pair = { 7U, "UYTUY" };
    original.Set = { "  q!", "12343e", "svvr", "0001" };
    original.Map = { {"!", 4398}, {"zzz", 0} };
    original.MultiSet = { 33, 33, 22, 22, 11 };
    original.UnorderedSet = { "41", "52", "001", "set" };
    original.UnorderedMap = { {"12345", 8}, {"XXX", 9}, {"XYZ", 42} };
    original.UnorderedMultiSet = { 1U, 2U, 1U, 0U, 0U };

    Deserialize(deserialized, ConvertToNode(ConvertToYsonString(original)));

    EXPECT_EQ(original.Vector, deserialized.Vector);
    EXPECT_EQ(original.Array, deserialized.Array);
    EXPECT_EQ(original.Pair, deserialized.Pair);
    EXPECT_EQ(original.Set, deserialized.Set);
    EXPECT_EQ(original.Map, deserialized.Map);
    EXPECT_EQ(original.MultiSet, deserialized.MultiSet);
    EXPECT_EQ(original.UnorderedSet, deserialized.UnorderedSet);
    EXPECT_EQ(original.UnorderedMap, deserialized.UnorderedMap);
    EXPECT_EQ(original.UnorderedMultiSet, deserialized.UnorderedMultiSet);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructTest, EnumAsKeyToYHash)
{
    THashMap<ETestEnum, TString> deserialized, original = {
        {ETestEnum::Value0, "abc"}
    };

    TString serialized = "{\"value0\"=\"abc\";}";
    EXPECT_EQ(serialized, ConvertToYsonString(original, EYsonFormat::Text).AsStringBuf());

    Deserialize(deserialized, ConvertToNode(TYsonString(serialized, EYsonType::Node)));

    EXPECT_EQ(original, deserialized);
}

////////////////////////////////////////////////////////////////////////////////

class TConfigWithOptional
    : public TYsonStruct
{
public:
    std::optional<int> Value;

    REGISTER_YSON_STRUCT(TConfigWithOptional);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default(123);
    }
};

TEST(TYsonStructTest, NullableWithNonNullDefault)
{
    {
        auto config = ConvertTo<TIntrusivePtr<TConfigWithOptional>>(TYsonString(TStringBuf("{}")));
        EXPECT_EQ(123, *config->Value);
        EXPECT_EQ(123, ConvertToNode(config)->AsMap()->GetChildOrThrow("value")->GetValue<i64>());
        EXPECT_FALSE(config->IsSet("value"));
    }

    {
        auto config = ConvertTo<TIntrusivePtr<TConfigWithOptional>>(TYsonString(TStringBuf("{value=#}")));
        EXPECT_FALSE(config->Value);
        EXPECT_EQ(ENodeType::Entity, ConvertToNode(config)->AsMap()->GetChildOrThrow("value")->GetType());
        EXPECT_TRUE(config->IsSet("value"));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TConfigWithDontSerializeDefault
    : public TYsonStruct
{
public:
    int Value;
    int OtherValue;

    REGISTER_YSON_STRUCT(TConfigWithDontSerializeDefault);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default(123);
        registrar.Parameter("other_value", &TThis::OtherValue)
            .Default(456)
            .DontSerializeDefault();
    }
};

TEST(TYsonStructTest, DontSerializeDefault)
{
    {
        auto config = New<TConfigWithDontSerializeDefault>();
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=123;}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));
    }

    {
        auto config = New<TConfigWithDontSerializeDefault>();
        config->OtherValue = 789;
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=123;\"other_value\"=789;}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TSimpleStruct
{
    int Value;
};

class TConfigWithUniversalParameterAccessor
    : public TYsonStruct
{
public:
    TSimpleStruct NestedStruct;

    REGISTER_YSON_STRUCT(TConfigWithUniversalParameterAccessor);

    static void Register(TRegistrar registrar)
    {
        registrar.ParameterWithUniversalAccessor<int>("value", [] (TThis* config) -> int& { return config->NestedStruct.Value; })
            .Default(123);
    }
};

TEST(TYsonStructTest, UniversalParameterAccessor)
{
    {
        auto config = New<TConfigWithUniversalParameterAccessor>();
        EXPECT_EQ(123, config->NestedStruct.Value);
        EXPECT_FALSE(config->IsSet("value"));

        config->NestedStruct.Value = 3;
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=3;}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));
    }

    {
        TString sourceYson = "{\"value\"=3;}";
        auto config = ConvertTo<TIntrusivePtr<TConfigWithUniversalParameterAccessor>>(TYsonString(TStringBuf(sourceYson)));

        EXPECT_EQ(3, config->NestedStruct.Value);
        EXPECT_TRUE(config->IsSet("value"));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TVirtualInheritanceConfig
    : public virtual TYsonStruct
{
    int Value;

    REGISTER_YSON_STRUCT(TVirtualInheritanceConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default(123);
    }
};

TEST(TYsonStructTest, VirtualInheritance)
{
    {
        auto config = New<TVirtualInheritanceConfig>();
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=123;}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));
    }
}

class TBase
    : public TYsonStruct
{
public:
    int Value;

    REGISTER_YSON_STRUCT(TBase);

    static void Register(TRegistrar)
    { }
};

class TDerived
    : public TBase
{
public:

    REGISTER_YSON_STRUCT(TDerived);

    static void Register(TRegistrar registrar)
    {
        registrar.BaseClassParameter("value", &TDerived::Value)
            .Default(123);
    }
};

TEST(TYsonStructTest, RegisterBaseFieldInDerived)
{
    {
        auto config = New<TDerived>();
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=123;}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TClassLevelPostprocessConfig
    : public TYsonStruct
{
    int Value;

    REGISTER_YSON_STRUCT(TClassLevelPostprocessConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
        registrar.Postprocessor([] (TClassLevelPostprocessConfig* config) {
            config->Value = 10;
        });
    }
};

TEST(TYsonStructTest, ClassLevelPostprocess)
{
    {
        auto config = New<TClassLevelPostprocessConfig>();
        config->Value = 1;
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=1}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));

        auto deserialized = ConvertTo<TIntrusivePtr<TClassLevelPostprocessConfig>>(output);
        EXPECT_EQ(deserialized->Value, 10);

    }
}

////////////////////////////////////////////////////////////////////////////////

struct TRecursiveConfig
    : public TYsonStruct
{
    TIntrusivePtr<TRecursiveConfig> Subconfig;

    int Value;

    REGISTER_YSON_STRUCT(TRecursiveConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("subconfig", &TThis::Subconfig)
            .Default();
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

TEST(TYsonStructTest, RecursiveConfig)
{
    {
        auto config = New<TRecursiveConfig>();
        config->Value = 1;
        config->Subconfig = New<TRecursiveConfig>();
        config->Subconfig->Value = 3;
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=1;\"subconfig\"={\"value\"=3}}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));

        auto deserialized = ConvertTo<TIntrusivePtr<TRecursiveConfig>>(output);
        EXPECT_EQ(deserialized->Value, 1);
        EXPECT_EQ(deserialized->Subconfig->Value, 3);
    }
}

////////////////////////////////////////////////////////////////////////////////


template <class T>
TIntrusivePtr<T> CreateCustomDefault()
{
    auto result = New<T>();
    result->IntValue = 10;
    return result;
}

class TYsonStructWithNestedStructsAndCustomDefaults
    : public TYsonStruct
{
public:
    TIntrusivePtr<TSimpleYsonStruct> YsonStruct;

    REGISTER_YSON_STRUCT(TYsonStructWithNestedStructsAndCustomDefaults);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("yson_struct", &TThis::YsonStruct)
            .DefaultCtor([] { return CreateCustomDefault<TSimpleYsonStruct>(); });
    }
};

TEST(TYsonStructTest, TestCustomDefaultsOfNestedStructsAreNotDiscardedOnDeserialize)
{
    auto deserialized = ConvertTo<TIntrusivePtr<TYsonStructWithNestedStructsAndCustomDefaults>>(TYsonString(TStringBuf("{}")));
    EXPECT_EQ(deserialized->YsonStruct->IntValue, 10);
}

////////////////////////////////////////////////////////////////////////////////

class TYsonStructWithNestedStructsAndPreprocessors
    : public TYsonStruct
{
public:
    TIntrusivePtr<TSimpleYsonStruct> YsonStruct;

    REGISTER_YSON_STRUCT(TYsonStructWithNestedStructsAndPreprocessors);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("yson_struct", &TThis::YsonStruct)
            .Default();
        registrar.Preprocessor([] (TThis* s) {
            s->YsonStruct = CreateCustomDefault<TSimpleYsonStruct>();
        });
    }
};

TEST(TYsonStructTest, TestPreprocessorsEffectsOnNestedStructsArePreservedOnDeserialize)
{
    auto deserialized = ConvertTo<TIntrusivePtr<TYsonStructWithNestedStructsAndPreprocessors>>(TYsonString(TStringBuf("{}")));
    EXPECT_EQ(deserialized->YsonStruct->IntValue, 10);
}

////////////////////////////////////////////////////////////////////////////////

class TBaseWithCustomConfigure
    : public TYsonStruct
{
public:
    int Int;
    double Double;

    REGISTER_YSON_STRUCT(TBaseWithCustomConfigure);

    static void Register(TRegistrar)
    { }

protected:
    static void CustomConfigure(TRegistrar registrar, int defaultInt, double defaultDouble)
    {
        registrar.Parameter("int", &TThis::Int)
            .Default(defaultInt);
        registrar.Postprocessor([defaultDouble] (TThis* s) {
            s->Double = defaultDouble;
        });
    }
};

class TDerivedWithCustomConfigure
    : public TBaseWithCustomConfigure
{
public:

    REGISTER_YSON_STRUCT(TDerivedWithCustomConfigure);

    static void Register(TRegistrar registrar)
    {
        CustomConfigure(registrar, 10, 2.2);
    }
};

TEST(TYsonStructTest, TestHierarchiesWithCustomInitializationOfBaseParameters)
{
    {
        auto deserialized = ConvertTo<TIntrusivePtr<TDerivedWithCustomConfigure>>(TYsonString(TStringBuf("{}")));
        EXPECT_EQ(deserialized->Int, 10);
        EXPECT_EQ(deserialized->Double, 2.2);
        EXPECT_FALSE(deserialized->IsSet("int"));
    }

    {
        auto deserialized = ConvertTo<TIntrusivePtr<TDerivedWithCustomConfigure>>(TYsonString(TStringBuf("{int=123}")));
        EXPECT_EQ(deserialized->Int, 123);
        EXPECT_TRUE(deserialized->IsSet("int"));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructTest, TestSimpleSerialization)
{
    TBufferStream stream;

    auto initialize = [] (auto& config) {
        config.MyString = "TestString";
        config.NullableInt.emplace(42);
    };

    TIntrusivePtr<TTestConfig> config;
    auto defaultConfig = New<TTestConfig>();

    ::Save(&stream, config);
    ::Load(&stream, config);
    EXPECT_FALSE(config);

    ::Save(&stream, defaultConfig);
    ::Load(&stream, config);
    EXPECT_TRUE(config);

    EXPECT_EQ(config->MyString, defaultConfig->MyString);
    EXPECT_FALSE(config->NullableInt);
    EXPECT_EQ(config->Subconfig->MyInt, defaultConfig->Subconfig->MyInt);

    config = New<TTestConfig>();
    initialize(*config);
    ::Save(&stream, config);

    config = nullptr;
    ::Load(&stream, config);
    EXPECT_EQ(config->MyString, "TestString");
    EXPECT_EQ(config->NullableInt, 42);

    TTestConfigLite liteConfig;
    initialize(liteConfig);
    ::Save(&stream, liteConfig);

    liteConfig.SetDefaults();
    ::Load(&stream, liteConfig);
    EXPECT_EQ(liteConfig.MyString, "TestString");
    EXPECT_EQ(liteConfig.NullableInt, 42);
}

TEST(TYsonStructTest, TestComplexSerialization)
{
    struct TComplexStruct
    {
        TTestConfigPtr Config1;
        TTestConfigPtr Config2;
        TTestConfigLite LiteConfig;
        TString StructName;

        Y_SAVELOAD_DEFINE(Config1, Config2, LiteConfig, StructName);
    };

    TComplexStruct toSerialize{
        .Config1 = New<TTestConfig>(),
        .Config2 = New<TTestConfig>(),
        .LiteConfig = TTestConfigLite(),
        .StructName = "tmp",
    };
    toSerialize.Config1->Load(GetCompleteConfigNode());
    toSerialize.Config2->Load(GetCompleteConfigNode(/*offset*/ 1));
    toSerialize.LiteConfig.MyString = "LiteConfig";
    toSerialize.LiteConfig.NullableInt.emplace(42);

    TBufferStream stream;

    ::Save(&stream, toSerialize);

    TComplexStruct deserialized;
    ::Load(&stream, deserialized);

    {
        SCOPED_TRACE("First deserialized config.");
        TestCompleteConfig(deserialized.Config1);
    }
    {
        SCOPED_TRACE("Second deserialized config.");
        TestCompleteConfig(deserialized.Config2, /*offset*/ 1);
    }
    EXPECT_EQ(deserialized.LiteConfig.MyString, "LiteConfig");
    EXPECT_EQ(deserialized.LiteConfig.NullableInt, 42);
    EXPECT_EQ(deserialized.StructName, "tmp");

    std::vector<TTestConfigPtr> configsList;
    configsList.reserve(5);
    for (int i = 0; i < 5; ++i) {
        auto config = New<TTestConfig>();
        config->Load(GetCompleteConfigNode(/*offset*/ i));
        configsList.push_back(std::move(config));
    }
    ::Save(&stream, configsList);
    configsList.clear();
    ::Load(&stream, configsList);

    for (int i = 0; i < 5; ++i) {
        SCOPED_TRACE(Format("%v-th config from configs list", i));
        TestCompleteConfig(configsList[i], /*offset*/ i);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTestOptionalNoInit
    : public NYT::NYTree::TYsonStructLite
{
public:
    int FieldWithInit = 1;
    int FieldNoInit = 1;

    REGISTER_YSON_STRUCT_LITE(TTestOptionalNoInit);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field1", &TThis::FieldWithInit)
            .Optional();
        registrar.Parameter("field2", &TThis::FieldNoInit)
            .Optional(/*init*/ false);
    }
};

TEST(TYsonStructTest, TestOptionalNoInit)
{
    TTestOptionalNoInit x;
    EXPECT_EQ(0, x.FieldWithInit);
    EXPECT_EQ(1, x.FieldNoInit);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestNonLiteralStruct
{
    TTestNonLiteralStruct()
    { }
};

class TTestNonLiteralStructSerializer
    : public virtual TExternalizedYsonStruct
{
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTestNonLiteralStruct, TTestNonLiteralStructSerializer);

    static void Register(TRegistrar)
    { }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestNonLiteralStruct, TTestNonLiteralStructSerializer);

static_assert(CExternallySerializable<TTestNonLiteralStruct>);

////////////////////////////////////////////////////////////////////////////////

struct TTestTraitConfig
{
    int Field1;
    double Field2;
};

class TTestTraitConfigSerializer
    : public virtual TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTestTraitConfig, TTestTraitConfigSerializer);

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalClassParameter("field1", &TThat::Field1);
        registrar.ExternalClassParameter("field2", &TThat::Field2);
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestTraitConfig, TTestTraitConfigSerializer);

class TFieldTester
    : public NYT::NYTree::TYsonStructLite
{
public:
    TTestTraitConfig Field;

    REGISTER_YSON_STRUCT_LITE(TFieldTester);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field", &TThis::Field);
    }
};

TEST(TYsonStructTest, ExternalizedYsonStructField)
{
    TFieldTester writer = {};
    writer.Field = {55, 34,};

    TBufferStream stream;

    ::Save(&stream, writer);

    TFieldTester reader = {};
    ::Load(&stream, reader);
    EXPECT_EQ(writer.Field.Field1, 55);
    EXPECT_EQ(writer.Field.Field2, 34);
    EXPECT_EQ(reader.Field.Field1, 55);
    EXPECT_EQ(reader.Field.Field2, 34);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestProcessorsTraitConfig
{
    int Field1 = 11;
    int Field2 = 33;

    static inline bool PostprocessorCalled = false;
    static inline bool PreprocessorCalled = false;
};

class TTestProcessorsTraitConfigSerializer
    : public TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTestProcessorsTraitConfig, TTestProcessorsTraitConfigSerializer);

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalClassParameter("field1", &TThat::Field1)
            .Default(42)
            .CheckThat([] (const int& field1) {
                return field1 % 2 == 0;
            });
        registrar.ExternalClassParameter("field2", &TThat::Field2)
            .Default(180);

        registrar.ExternalPreprocessor([] (TThat* podstruct) {
            //! NB(arkady-e1ppa): Preprocessor is called twice during deserialization.
            //! Same behavior is present for a normal YsonStructLite so I can't be
            //! bothered fixing this for my struct and introduce inconsistent behavior.
            // EXPECT_FALSE(TThat::PreprocessorCalled);
            EXPECT_FALSE(TThat::PostprocessorCalled);
            TThat::PreprocessorCalled = true;
            podstruct->Field2 = 88;
        });

        registrar.ExternalPostprocessor([] (TThat* podstruct) {
            EXPECT_TRUE(TThat::PreprocessorCalled);
            EXPECT_FALSE(TThat::PostprocessorCalled);
            TThat::PostprocessorCalled = true;
            podstruct->Field1 = 37;
        });
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestProcessorsTraitConfig, TTestProcessorsTraitConfigSerializer);

class TFieldTesterForProcessor
    : public NYT::NYTree::TYsonStructLite
{
public:
    TTestProcessorsTraitConfig Field;

    REGISTER_YSON_STRUCT_LITE(TFieldTesterForProcessor);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field", &TThis::Field);
    }
};

TEST(TYsonStructTest, ExternalizedYsonStructPostPreprocessors)
{
    TTestProcessorsTraitConfig::PreprocessorCalled = false;
    TTestProcessorsTraitConfig::PostprocessorCalled = false;

    TFieldTesterForProcessor writer = {};

    EXPECT_EQ(writer.Field.Field1, 11);
    EXPECT_EQ(writer.Field.Field2, 33);

    EXPECT_FALSE(TTestProcessorsTraitConfig::PreprocessorCalled);
    EXPECT_FALSE(TTestProcessorsTraitConfig::PostprocessorCalled);

    TBufferStream stream;

    ::Save(&stream, writer);

    EXPECT_TRUE(TTestProcessorsTraitConfig::PreprocessorCalled);
    EXPECT_FALSE(TTestProcessorsTraitConfig::PostprocessorCalled);
    EXPECT_EQ(writer.Field.Field1, 11);
    EXPECT_EQ(writer.Field.Field2, 33);

    TTestProcessorsTraitConfig::PreprocessorCalled = false;
    TTestProcessorsTraitConfig::PostprocessorCalled = false;

    TFieldTesterForProcessor reader = {};

    ::Load(&stream, reader);

    EXPECT_TRUE(TTestProcessorsTraitConfig::PreprocessorCalled);
    EXPECT_TRUE(TTestProcessorsTraitConfig::PostprocessorCalled);
    EXPECT_EQ(reader.Field.Field1, 37);
    EXPECT_EQ(reader.Field.Field2, 33);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestTraitConfigWithDefaults
{
    int Field1;
    double Field2;
};

class TTestTraitConfigWithDefaultsSerializer
    : public TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTestTraitConfigWithDefaults, TTestTraitConfigWithDefaultsSerializer);

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalClassParameter("field1", &TThat::Field1)
            .Default(42);
        registrar.ExternalClassParameter("field2", &TThat::Field2)
            .Default(34);
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestTraitConfigWithDefaults, TTestTraitConfigWithDefaultsSerializer);

class TFieldTesterWithCustomDefaults
    : public NYT::NYTree::TYsonStructLite
{
public:
    TTestTraitConfigWithDefaults Field;

    REGISTER_YSON_STRUCT_LITE(TFieldTesterWithCustomDefaults);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field", &TThis::Field)
            .Default({44, 12});
    }
};

TEST(TYsonStructTest, ExternalizedYsonStructCustomDefaults)
{
    TFieldTesterWithCustomDefaults tester;

    EXPECT_EQ(tester.Field.Field1, 44);
    EXPECT_EQ(tester.Field.Field2, 12);

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("field").BeginMap()
                .Item("field2").Value(77)
            .EndMap()
        .EndMap()->AsMap();

    tester.Load(node);

    EXPECT_EQ(tester.Field.Field1, 44);
    EXPECT_EQ(tester.Field.Field2, 77);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestDerivedPodConfig
    : public TTestTraitConfig
{
    int Field3;
};

class TTestDerivedPodConfigSerializer
    : public TTestTraitConfigSerializer
{
public:
    REGISTER_DERIVED_EXTERNALIZED_YSON_STRUCT(TTestDerivedPodConfig, TTestDerivedPodConfigSerializer, (TTestTraitConfigSerializer));

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalClassParameter("field_3", &TThat::Field3);
    }
};

class TDerivedFieldTester
    : public NYT::NYTree::TYsonStructLite
{
public:
    TTestDerivedPodConfig Field;

    REGISTER_YSON_STRUCT_LITE(TDerivedFieldTester);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field", &TThis::Field);
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestDerivedPodConfig, TTestDerivedPodConfigSerializer);

TEST(TYsonStructTest, ExternalizedYsonStructDerivedFromExternalized)
{
    TDerivedFieldTester writer = {};
    writer.Field = {{55, 34}, 37};

    TBufferStream stream;

    ::Save(&stream, writer);

    TDerivedFieldTester reader = {};
    ::Load(&stream, reader);
    EXPECT_EQ(writer.Field.Field1, 55);
    EXPECT_EQ(writer.Field.Field2, 34);
    EXPECT_EQ(writer.Field.Field3, 37);
    EXPECT_EQ(reader.Field.Field1, 55);
    EXPECT_EQ(reader.Field.Field2, 34);
    EXPECT_EQ(reader.Field.Field3, 37);
}

struct TTestDoubleDerivedPodConfig
    : public TTestDerivedPodConfig
{
    int Field4;
};

class TTestDoubleDerivedPodConfigSerializer
    : public TTestDerivedPodConfigSerializer
{
public:
    REGISTER_DERIVED_EXTERNALIZED_YSON_STRUCT(TTestDoubleDerivedPodConfig, TTestDoubleDerivedPodConfigSerializer, (TTestDerivedPodConfigSerializer));

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalClassParameter("field_4", &TThat::Field4);
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestDoubleDerivedPodConfig, TTestDoubleDerivedPodConfigSerializer);

class TDoubleDerivedFieldTester
    : public NYT::NYTree::TYsonStructLite
{
public:
    TTestDoubleDerivedPodConfig Field;

    REGISTER_YSON_STRUCT_LITE(TDoubleDerivedFieldTester);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field", &TThis::Field);
    }
};

TEST(TYsonStructTest, ExternalizedYsonStructDoubleDerivedFromExternalized)
{
    TDoubleDerivedFieldTester writer = {};
    writer.Field = {{{55, 34}, 37}, 77};

    TBufferStream stream;

    ::Save(&stream, writer);

    TDoubleDerivedFieldTester reader = {};
    ::Load(&stream, reader);
    EXPECT_EQ(writer.Field.Field1, 55);
    EXPECT_EQ(writer.Field.Field2, 34);
    EXPECT_EQ(writer.Field.Field3, 37);
    EXPECT_EQ(writer.Field.Field4, 77);
    EXPECT_EQ(reader.Field.Field1, 55);
    EXPECT_EQ(reader.Field.Field2, 34);
    EXPECT_EQ(reader.Field.Field3, 37);
    EXPECT_EQ(reader.Field.Field4, 77);
}

struct TTestDerivedSecondBase
{
    int Field5;
    int Field6;
};

class TTestDerivedSecondBaseSerializer
    : public virtual TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTestDerivedSecondBase, TTestDerivedSecondBaseSerializer);

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalClassParameter("field_5", &TThat::Field5);
        registrar.ExternalClassParameter("field_6", &TThat::Field6);
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestDerivedSecondBase, TTestDerivedSecondBaseSerializer);

struct TTestDerivedTwoBasesConfig
    : public TTestDoubleDerivedPodConfig
    , public TTestDerivedSecondBase
{ };

class TTestDerivedTwoBasesConfigSerializer
    : public TTestDoubleDerivedPodConfigSerializer
    , public TTestDerivedSecondBaseSerializer
{
public:
    REGISTER_DERIVED_EXTERNALIZED_YSON_STRUCT(
        TTestDerivedTwoBasesConfig,
        TTestDerivedTwoBasesConfigSerializer,
        (TTestDoubleDerivedPodConfigSerializer)
        (TTestDerivedSecondBaseSerializer));

    static void Register(TRegistrar)
    { }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestDerivedTwoBasesConfig, TTestDerivedTwoBasesConfigSerializer);

class TTwoBasesFieldTester
    : public NYT::NYTree::TYsonStructLite
{
public:
    TTestDerivedTwoBasesConfig Field;

    REGISTER_YSON_STRUCT_LITE(TTwoBasesFieldTester);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field", &TThis::Field);
    }
};

TEST(TYsonStructTest, ExternalizedYsonStructDerivedFromTwoExternalizedBases)
{
    TTwoBasesFieldTester writer = {};
    writer.Field = {{{{55, 34}, 37}, 77}, {7, 8}};

    TBufferStream stream;

    ::Save(&stream, writer);

    TTwoBasesFieldTester reader = {};
    ::Load(&stream, reader);
    EXPECT_EQ(writer.Field.Field1, 55);
    EXPECT_EQ(writer.Field.Field2, 34);
    EXPECT_EQ(writer.Field.Field3, 37);
    EXPECT_EQ(writer.Field.Field4, 77);
    EXPECT_EQ(writer.Field.Field5, 7);
    EXPECT_EQ(writer.Field.Field6, 8);
    EXPECT_EQ(reader.Field.Field1, 55);
    EXPECT_EQ(reader.Field.Field2, 34);
    EXPECT_EQ(reader.Field.Field3, 37);
    EXPECT_EQ(reader.Field.Field4, 77);
    EXPECT_EQ(reader.Field.Field5, 7);
    EXPECT_EQ(reader.Field.Field6, 8);
}

////////////////////////////////////////////////////////////////////////////////

class TYsonStructWithCustomSubDefault
    : public TYsonStruct
{
public:
    TIntrusivePtr<TSimpleYsonStruct> Sub;

    REGISTER_YSON_STRUCT(TYsonStructWithCustomSubDefault);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub", &TYsonStructWithCustomSubDefault::Sub)
            .DefaultCtor([] {
                auto sub = New<TSimpleYsonStruct>();
                sub->IntValue = 2;
                return sub;
            });
    }
};

TEST(TYsonStructTest, CustomSubStruct)
{
    auto testStruct = New<TYsonStructWithCustomSubDefault>();
    EXPECT_EQ(testStruct->Sub->IntValue, 2);

    auto testNode = BuildYsonNodeFluently()
        .BeginMap()
        .EndMap();
    testStruct->Load(testNode);
    EXPECT_EQ(testStruct->Sub->IntValue, 2);

    testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub")
                .BeginMap()
                .EndMap()
        .EndMap();
    testStruct->Load(testNode);
    EXPECT_EQ(testStruct->Sub->IntValue, 2);

    testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub")
                .BeginMap()
                    .Item("int_value").Value(3)
                .EndMap()
        .EndMap();
    testStruct->Load(testNode);
    EXPECT_EQ(testStruct->Sub->IntValue, 3);
}

////////////////////////////////////////////////////////////////////////////////

class TTestSubConfigLiteWithDefaults
    : public TYsonStructLite
{
public:
    int MyInt;
    TString MyString;

    REGISTER_YSON_STRUCT_LITE(TTestSubConfigLiteWithDefaults);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_int", &TThis::MyInt)
            .Default(42);
        registrar.Parameter("my_string", &TThis::MyString)
            .Default("y");
    }
};

class TTestConfigWithSubStructLite
    : public TYsonStructLite
{
public:
    TTestSubConfigLiteWithDefaults Sub;

    REGISTER_YSON_STRUCT_LITE(TTestConfigWithSubStructLite);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sub", &TThis::Sub)
            .DefaultCtor([] {
                TTestSubConfigLiteWithDefaults sub = {};
                sub.MyInt = 11;
                sub.MyString = "x";
                return sub;
            });
    }
};

TEST(TYsonStructTest, CustomSubStructLite)
{
    TTestConfigWithSubStructLite testStruct = {};

    auto testNode = BuildYsonNodeFluently()
        .BeginMap()
        .EndMap();
    testStruct.Load(testNode->AsMap());
    EXPECT_EQ(testStruct.Sub.MyInt, 11);
    EXPECT_EQ(testStruct.Sub.MyString, "x");

    testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub")
                .BeginMap()
                .EndMap()
        .EndMap();
    testStruct.Load(testNode->AsMap());
    EXPECT_EQ(testStruct.Sub.MyInt, 11);
    EXPECT_EQ(testStruct.Sub.MyString, "x");

    testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub")
                .BeginMap()
                    .Item("my_string").Value("C")
                .EndMap()
        .EndMap();
    testStruct.Load(testNode->AsMap());
    EXPECT_EQ(testStruct.Sub.MyInt, 11);
    EXPECT_EQ(testStruct.Sub.MyString, "C");
}

////////////////////////////////////////////////////////////////////////////////

struct TTestSupConfigWithCustomDefaults
{
    TTestTraitConfigWithDefaults Sub;
};

class TTestSupConfigWithCustomDefaultsSerializer
    : public TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTestSupConfigWithCustomDefaults, TTestSupConfigWithCustomDefaultsSerializer);

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalClassParameter("sub", &TThat::Sub)
            .Default(TTestTraitConfigWithDefaults{
                .Field1 = 16,
                .Field2 = 34,
            });
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTestSupConfigWithCustomDefaults, TTestSupConfigWithCustomDefaultsSerializer);

TEST(TYsonStructTest, CustomSubExternalizedStruct)
{
    TTestSupConfigWithCustomDefaults testStruct = {};

    auto testNode = BuildYsonNodeFluently()
        .BeginMap()
        .EndMap();
    Deserialize(testStruct, testNode->AsMap());
    EXPECT_EQ(testStruct.Sub.Field1, 16);
    EXPECT_EQ(testStruct.Sub.Field2, 34);

    testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub")
                .BeginMap()
                .EndMap()
        .EndMap();
    Deserialize(testStruct, testNode->AsMap());
    EXPECT_EQ(testStruct.Sub.Field1, 16);
    EXPECT_EQ(testStruct.Sub.Field2, 34);

    testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub")
                .BeginMap()
                    .Item("field2").Value(77)
                .EndMap()
        .EndMap();
    Deserialize(testStruct, testNode->AsMap());
    EXPECT_EQ(testStruct.Sub.Field1, 16);
    EXPECT_EQ(testStruct.Sub.Field2, 77);
}

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TSimpleYsonStruct> CreateSimpleYsonStruct(int value)
{
    auto result = New<TSimpleYsonStruct>();
    result->IntValue = value;
    return result;
}

class TTestingNestedListWithCustomDefault
    : public TYsonStruct
{
public:
    std::vector<TIntrusivePtr<TSimpleYsonStruct>> NestedList;

    REGISTER_YSON_STRUCT(TTestingNestedListWithCustomDefault);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("nested_list_1", &TThis::NestedList)
            .DefaultCtor([] {
                return std::vector{CreateSimpleYsonStruct(5)};
            });
    }
};

TEST(TYsonStructTest, NestedListWithCustomDefault)
{
    auto testInput = TYsonString(TStringBuf("{}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TTestingNestedListWithCustomDefault>>(testInput);

    EXPECT_EQ(deserialized->NestedList.size(), 1u);
    EXPECT_EQ(deserialized->NestedList[0]->IntValue, 5);
}

////////////////////////////////////////////////////////////////////////////////

class TTestingNestedMapWithCustomDefault
    : public TYsonStruct
{
public:
    THashMap<TString, TIntrusivePtr<TSimpleYsonStruct>> NestedMap;

    REGISTER_YSON_STRUCT(TTestingNestedMapWithCustomDefault);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("nested_map", &TThis::NestedMap)
            .DefaultCtor([] {
                return THashMap<TString, TIntrusivePtr<TSimpleYsonStruct>>{
                    {"foo", CreateSimpleYsonStruct(42)},
                    {"bar", CreateSimpleYsonStruct(7)},
                };
            });
    }
};

TEST(TYsonStructTest, NestedMapWithCustomDefault)
{
    auto testInput = TYsonString(TStringBuf("{}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TTestingNestedMapWithCustomDefault>>(testInput);

    EXPECT_EQ(deserialized->NestedMap.size(), 2u);
    EXPECT_EQ(deserialized->NestedMap["foo"]->IntValue, 42);
    EXPECT_EQ(deserialized->NestedMap["bar"]->IntValue, 7);

    auto testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("nested_map")
                .BeginMap()
                    .Item("baz")
                        .BeginMap()
                            .Item("int_value").Value(33)
                        .EndMap()
                    .Item("foo")
                        .BeginMap()
                            .Item("int_value").Value(88)
                        .EndMap()
                .EndMap()
        .EndMap();
    Deserialize(deserialized, testNode->AsMap());
    EXPECT_EQ(deserialized->NestedMap.size(), 3u);
    EXPECT_EQ(deserialized->NestedMap["baz"]->IntValue, 33);
    EXPECT_EQ(deserialized->NestedMap["foo"]->IntValue, 88);
    EXPECT_EQ(deserialized->NestedMap["bar"]->IntValue, 7);
}

////////////////////////////////////////////////////////////////////////////////

class TTestingNestedMapWithCustomDefaultResetOnLoad
    : public TYsonStruct
{
public:
    THashMap<TString, TIntrusivePtr<TSimpleYsonStruct>> NestedMap;

    REGISTER_YSON_STRUCT(TTestingNestedMapWithCustomDefaultResetOnLoad);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("nested_map", &TThis::NestedMap)
            .DefaultCtor([] {
                return THashMap<TString, TIntrusivePtr<TSimpleYsonStruct>>{
                    {"foo", CreateSimpleYsonStruct(42)},
                    {"bar", CreateSimpleYsonStruct(7)},
                };
            })
            .ResetOnLoad();
    }
};

TEST(TYsonStructTest, NestedMapWithCustomDefaultAndResetOnLoad)
{
    auto testInput = TYsonString(TStringBuf("{}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TTestingNestedMapWithCustomDefaultResetOnLoad>>(testInput);

    EXPECT_EQ(deserialized->NestedMap.size(), 2u);
    EXPECT_EQ(deserialized->NestedMap["foo"]->IntValue, 42);
    EXPECT_EQ(deserialized->NestedMap["bar"]->IntValue, 7);

    auto testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("nested_map")
                .BeginMap()
                    .Item("baz")
                        .BeginMap()
                            .Item("int_value").Value(33)
                        .EndMap()
                    .Item("foo")
                        .BeginMap()
                            .Item("int_value").Value(88)
                        .EndMap()
                .EndMap()
        .EndMap();
    Deserialize(deserialized, testNode->AsMap());
    EXPECT_EQ(deserialized->NestedMap.size(), 2u);
    EXPECT_EQ(deserialized->NestedMap["baz"]->IntValue, 33);
    EXPECT_EQ(deserialized->NestedMap["foo"]->IntValue, 88);
}

////////////////////////////////////////////////////////////////////////////////

struct TInnerYsonStruct
    : public TYsonStruct
{
    int MyInt;

    REGISTER_YSON_STRUCT(TInnerYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_int", &TThis::MyInt)
            .Default(1);

        registrar.Postprocessor([] (TThis* self) {
            self->MyInt = 42;
        });
    }
};

struct TOuterYsonStructWithNull
    : public TYsonStruct
{
    TIntrusivePtr<TInnerYsonStruct> Inner;

    REGISTER_YSON_STRUCT(TOuterYsonStructWithNull);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("inner", &TThis::Inner)
            // Make sure postprocessor doesn't cause sigsegv in case of nullptr
            .Default();
    }
};

struct TOuterYsonStructWithValidation
    : public TYsonStruct
{
    TIntrusivePtr<TInnerYsonStruct> Inner;

    REGISTER_YSON_STRUCT(TOuterYsonStructWithValidation);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("inner", &TThis::Inner)
            .DefaultNew()
            // Will be true only if postprocessor is called and is called before this check.
            .CheckThat([] (const TIntrusivePtr<TInnerYsonStruct>& inner) {
                EXPECT_EQ(inner->MyInt % 2, 0);
            });
    }
};

TEST(TYsonStructTest, OuterYsonStructWithNull)
{
    auto testInput = TYsonString(TStringBuf("{}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TOuterYsonStructWithNull>>(testInput);
}

TEST(TYsonStructTest, OuterYsonStructWithValidation)
{
    auto testInput = TYsonString(TStringBuf("{}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TOuterYsonStructWithValidation>>(testInput);

    EXPECT_TRUE(deserialized->Inner);
    EXPECT_EQ(deserialized->Inner->MyInt, 42);
}

struct TWithYsonString
    : public TYsonStructLite
{
    NYson::TYsonString MyString;

    REGISTER_YSON_STRUCT_LITE(TWithYsonString);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_string", &TThis::MyString)
            .Default();
    }
};

TEST(TYsonStructTest, TYsonStringFieldSimple)
{
    TWithYsonString value;

    {
        auto node = BuildYsonNodeFluently()
            .BeginMap()
                .Item("my_string").Value(ConvertToYsonString(42))
            .EndMap();

        Deserialize(value, node->AsMap());
        EXPECT_TRUE(value.MyString);
        EXPECT_EQ(ConvertTo<i32>(value.MyString), 42);
    }

    {
        std::string message{"Hi mom!"};

        auto node = BuildYsonNodeFluently()
            .BeginMap()
                .Item("my_string").Value(ConvertToYsonString(message))
            .EndMap();

        Deserialize(value, node->AsMap());
        EXPECT_TRUE(value.MyString);
        EXPECT_EQ(ConvertTo<std::string>(value.MyString), message);
    }

    {
        auto config = New<TTestConfig>();
        config->MyString = "Hello, world!";

        auto node = BuildYsonNodeFluently()
            .BeginMap()
                .Item("my_string").Value(ConvertToYsonString(config))
            .EndMap();

        Deserialize(value, node->AsMap());
        EXPECT_TRUE(value.MyString);

        auto extracted = ConvertTo<TTestConfigPtr>(value.MyString);
        EXPECT_EQ(extracted->NullableInt, config->NullableInt);
        EXPECT_EQ(extracted->MyString, extracted->MyString);
    }
}

TEST(TYsonStructTest, TYsonStringFieldCompound)
{
    TWithYsonString value;

    auto config = New<TTestConfig>();
    config->MyString = "Hello, world!";

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value(ConvertToYsonString(config))
        .EndMap();

    Deserialize(value, node->AsMap());
    EXPECT_TRUE(value.MyString);

    auto extracted = ConvertTo<TTestConfigPtr>(value.MyString);
    EXPECT_EQ(extracted->NullableInt, config->NullableInt);
    EXPECT_EQ(extracted->MyString, extracted->MyString);
}

////////////////////////////////////////////////////////////////////////////////

struct TPolyBase
    : public TYsonStruct
{
    int BaseField;

    REGISTER_YSON_STRUCT(TPolyBase);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("base_field", &TThis::BaseField)
            .Default(42);
    }
};

struct TPolyDerived1
    : public TPolyBase
{
    int Field1;

    REGISTER_YSON_STRUCT(TPolyDerived1);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field1", &TThis::Field1)
            .Default(33);
    }
};

struct TPolyDerived2
    : public TPolyBase
{
    int Field2;

    REGISTER_YSON_STRUCT(TPolyDerived2);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field2", &TThis::Field2)
            .Default(11);

        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

TEST(TYsonStructTest, TestSlicing1)
{
    TIntrusivePtr<TPolyBase> sliced = New<TPolyDerived1>();

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("base_field").Value(11)
            .Item("field1").Value(123)
        .EndMap();

    Deserialize(sliced, node->AsMap());

    auto concrete = DynamicPointerCast<TPolyDerived1>(sliced);

    EXPECT_TRUE(concrete.operator bool());
    EXPECT_EQ(concrete->BaseField, 11);
    EXPECT_EQ(concrete->Field1, 123);
}

TEST(TYsonStructTest, TestSlicing2)
{
    TIntrusivePtr<TPolyBase> sliced = New<TPolyBase>();

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("base_field").Value(11)
            .Item("field1").Value(123)
        .EndMap();

    Deserialize(sliced, node->AsMap());

    auto concrete = DynamicPointerCast<TPolyDerived1>(sliced);

    EXPECT_FALSE(concrete.operator bool());
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_POLYMORPHIC_YSON_STRUCT(MyPoly,
    ((Base) (TPolyBase))
    ((Drv1) (TPolyDerived1))
    ((Drv2) (TPolyDerived2))
);

TEST(TYsonStructTest, TestPolymorphicYsonStruct)
{
    TMyPoly poly;

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("base")
            .Item("base_field").Value(11)
            .Item("field1").Value(123)
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyType::Base);

    auto basePtr = poly.TryGetConcrete<TPolyBase>();
    EXPECT_TRUE(basePtr.operator bool());
    EXPECT_EQ(basePtr->BaseField, 11);

    node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("drv1")
            .Item("base_field").Value(14)
            .Item("field1").Value(111)
            .Item("field2").Value(1337) // Unrecognized but will be dropped.
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyType::Drv1);

    auto drv1Ptr = poly.TryGetConcrete<TPolyDerived1>();
    EXPECT_TRUE(drv1Ptr.operator bool());
    EXPECT_EQ(drv1Ptr->BaseField, 14);
    EXPECT_EQ(drv1Ptr->Field1, 111);

    node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("drv2")
            .Item("base_field").Value(7)
            .Item("field1").Value(188)
        .EndMap();

    // Now field1 is unrecognized.
    EXPECT_THROW(Deserialize(poly, node->AsMap()), std::exception);

    node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("drv2")
            .Item("base_field").Value(17)
            .Item("field2").Value(144)
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyType::Drv2);

    auto drv2Ptr = poly.TryGetConcrete<TPolyDerived2>();
    EXPECT_TRUE(drv2Ptr.operator bool());
    EXPECT_EQ(drv2Ptr->BaseField, 17);
    EXPECT_EQ(drv2Ptr->Field2, 144);
}

TEST(TYsonStructTest, TestPolymorphicYsonStructSaveLoad)
{
    auto drv = New<TPolyDerived2>();
    drv->Field2 = 5;
    drv->BaseField = 0;

    auto poly = TMyPoly{EMyPolyType::Drv2, std::move(drv)};

    auto serialized = ConvertToYsonString(poly);
    auto deserialized = ConvertTo<TMyPoly>(serialized);

    EXPECT_EQ(deserialized.GetCurrentType(), EMyPolyType::Drv2);

    drv = poly.TryGetConcrete<TPolyDerived2>();

    EXPECT_TRUE(drv.operator bool());
    EXPECT_EQ(drv->BaseField, 0);
    EXPECT_EQ(drv->Field2, 5);
}

TEST(TYsonStructTest, TestPolymorphicYsonStructMergeIfPossible)
{
    TMyPoly poly;

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("drv1")
            .Item("base_field").Value(14)
            // Field1 is missing -- default is 33
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyType::Drv1);

    auto drv1Ptr = poly.TryGetConcrete<TPolyDerived1>();
    EXPECT_TRUE(drv1Ptr.operator bool());
    EXPECT_EQ(drv1Ptr->BaseField, 14);
    EXPECT_EQ(drv1Ptr->Field1, 33);

    node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("drv1")
            .Item("field1").Value(18)
            // BaseField is missing -- default is 42
        .EndMap();

    poly.Load(node->AsMap(), /*postprocess*/false, /*setDefaults*/false);
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyType::Drv1);

    drv1Ptr = poly.TryGetConcrete<TPolyDerived1>();
    EXPECT_TRUE(drv1Ptr.operator bool());
    EXPECT_EQ(drv1Ptr->BaseField, 14); // <- Field must remain the same.
    EXPECT_EQ(drv1Ptr->Field1, 18);
}

DEFINE_POLYMORPHIC_YSON_STRUCT_WITH_DEFAULT(MyPolyDefault, Drv1,
    ((Base) (TPolyBase))
    ((Drv1) (TPolyDerived1))
    ((Drv2) (TPolyDerived2))
);

TEST(TYsonStructTest, TestPolymorphicYsonStructDefault)
{
    TMyPolyDefault poly;

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("base_field").Value(11)
            .Item("field1").Value(123)
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyDefaultType::Drv1);

    auto drv1Ptr = poly.TryGetConcrete<TPolyDerived1>();
    EXPECT_TRUE(drv1Ptr.operator bool());
    EXPECT_EQ(drv1Ptr->BaseField, 11);
    EXPECT_EQ(drv1Ptr->Field1, 123);

    node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("drv2")
            .Item("base_field").Value(14)
            .Item("field2").Value(111)
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyDefaultType::Drv2);

    node = BuildYsonNodeFluently()
    .BeginMap()
        .Item("base_field").Value(11)
        .Item("field1").Value(123)
    .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyDefaultType::Drv1);
}

DEFINE_ENUM(EMyPolyDefaultEnum,
    (Base)
    (Drv1)
    (Drv2)
);

DEFINE_POLYMORPHIC_YSON_STRUCT_FOR_ENUM_WITH_DEFAULT(MyPolyDefaultEnum, EMyPolyDefaultEnum, Base,
    ((Base) (TPolyBase))
    ((Drv1) (TPolyDerived1))
    ((Drv2) (TPolyDerived2))
);

TEST(TYsonStructTest, TestPolymorphicYsonStructDefaultEnum)
{
    TMyPolyDefaultEnum poly;

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("base_field").Value(11)
            .Item("field1").Value(123)
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyDefaultEnum::Base);

    auto basePtr = poly.TryGetConcrete<TPolyBase>();
    EXPECT_TRUE(basePtr.operator bool());
    EXPECT_EQ(basePtr->BaseField, 11);

    node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("drv1")
            .Item("base_field").Value(14)
            .Item("field1").Value(111)
        .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyDefaultEnum::Drv1);

    node = BuildYsonNodeFluently()
    .BeginMap()
        .Item("base_field").Value(11)
        .Item("field1").Value(123)
    .EndMap();

    Deserialize(poly, node->AsMap());
    EXPECT_EQ(poly.GetCurrentType(), EMyPolyDefaultEnum::Base);
}

////////////////////////////////////////////////////////////////////////////////

struct TPolyHolder
    : public TYsonStructLite
{
    TMyPoly PolyField;

    REGISTER_YSON_STRUCT_LITE(TPolyHolder);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("poly_field", &TThis::PolyField)
            .Default();

        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::ThrowRecursive);
    }
};

TEST(TYsonStructTest, TestPolymorphicYsonStructSerializeEmpty)
{
    TPolyHolder holder;

    auto node = ConvertToNode(holder);

    Deserialize(holder, node->AsMap());
}

TEST(TYsonStructTest, TestPolymorphicYsonStructAsField)
{
    TPolyHolder holder;

    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("poly_field").BeginMap()
                .Item("type").Value("drv2")
                .Item("base_field").Value(17)
                .Item("field2").Value(144)
            .EndMap()
        .EndMap();

    Deserialize(holder, node->AsMap());

    EXPECT_EQ(holder.PolyField.GetCurrentType(), EMyPolyType::Drv2);
    auto drv = holder.PolyField.TryGetConcrete<TPolyDerived2>();

    EXPECT_TRUE(drv);
    EXPECT_EQ(drv->BaseField, 17);
    EXPECT_EQ(drv->Field2, 144);

    node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("poly_field").BeginMap()
                .Item("type").Value("drv1")
                .Item("base_field").Value(17)
                .Item("field2").Value(144)
            .EndMap()
        .EndMap();

    // field2 is unrecognized for drv1. Its unrecognized strategy is
    // Drop by default but holder has recursive throw so it must
    // throw.
    EXPECT_THROW(Deserialize(holder, node->AsMap()), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

struct TNonComparable
{ };

static_assert(!std::equality_comparable<TNonComparable>);

void Deserialize(TNonComparable& /*value*/, auto /*node*/)
{ }

[[maybe_unused]] void Serialize(const TNonComparable& /*value*/, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    consumer->OnEndMap();
}

struct TComparableYsonStruct
    : public TYsonStructLite
{
    int Value;

    std::vector<int> Values;
    TIntrusivePtr<TSimpleYsonStruct> SimpleSubStruct;
    THashMap<int, TIntrusivePtr<TSimpleYsonStruct>> Mapping;
    IMapNodePtr MapNode;

    bool UnregisteredValue = false;

    REGISTER_YSON_STRUCT_LITE(TComparableYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default(0);
        registrar.Parameter("simple_sub_struct", &TThis::SimpleSubStruct)
            .DefaultCtor([] {
                auto ptr = New<TSimpleYsonStruct>();
                ptr->IntValue = 77;
                return ptr;
            });
        registrar.Parameter("mapping", &TThis::Mapping)
            .DefaultCtor([] {
                THashMap<int, TIntrusivePtr<TSimpleYsonStruct>> mapping = {};
                mapping[42] = New<TSimpleYsonStruct>();
                return mapping;
            });

        registrar.Parameter("values", &TThis::Values)
            .Default({1, 2, 3});

        registrar.Parameter("map_node", &TThis::MapNode)
            .Default();
    }
};

static_assert(std::equality_comparable<TComparableYsonStruct>);

struct TNonComparableYsonStruct
    : public TYsonStructLite
{
    TNonComparable Value;

    REGISTER_YSON_STRUCT_LITE(TNonComparableYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

static_assert(std::equality_comparable<TNonComparableYsonStruct>);

struct TComparableBase
{
    static inline int BaseComparisonCalls = 0;

    bool operator==(const TComparableBase&) const
    {
        ++BaseComparisonCalls;
        return true;
    }
};

inline int TemplatedComparisonCalls = 0;

template <class T>
concept CWithTemplatedComparison = requires {
    typename T::TEnableTemplatedComparison;
};

template <CWithTemplatedComparison T>
bool operator==(const T&, const T&)
{
    ++TemplatedComparisonCalls;
    return true;
}

struct TYsonStructWithComparableBase
    : public TYsonStructLite
    , public TComparableBase
{
    REGISTER_YSON_STRUCT_LITE(TYsonStructWithComparableBase);

    static void Register(TRegistrar /*registrar*/)
    { }
};

static_assert(std::equality_comparable<TYsonStructWithComparableBase>);

struct TYsonStructWithTemplatedComparison
    : public TYsonStructLite
{
    using TEnableTemplatedComparison = void;

    REGISTER_YSON_STRUCT_LITE(TYsonStructWithTemplatedComparison);

    static void Register(TRegistrar /*registrar*/)
    { }
};

static_assert(std::equality_comparable<TYsonStructWithTemplatedComparison>);

TEST(TYsonStructTest, CompareComparable)
{
    TComparableYsonStruct lhs;
    TComparableYsonStruct rhs;

    EXPECT_TRUE(lhs == lhs);
    EXPECT_TRUE(lhs == rhs);

    rhs.Value = 42;
    EXPECT_FALSE(lhs == rhs);
}

TEST(TYsonStructTest, CompareNonComparable)
{
    TNonComparableYsonStruct lhs;
    TNonComparableYsonStruct rhs;

    EXPECT_TRUE(lhs == lhs);
    EXPECT_FALSE(lhs == rhs);
}

TEST(TYsonStructTest, CompareRecursively)
{
    TComparableYsonStruct lhs;
    TComparableYsonStruct rhs;

    rhs.SimpleSubStruct->IntValue = 11;
    EXPECT_FALSE(lhs == rhs);
}

TEST(TYsonStructTest, CompareDifferentType)
{
    TComparableYsonStruct lhs;
    auto rhs = New<TSimpleYsonStruct>();

    EXPECT_FALSE(static_cast<const TYsonStructBase&>(lhs) == static_cast<const TYsonStructBase&>(*rhs));
}

TEST(TYsonStructTest, CompareWithDifferentUnregistered)
{
    TComparableYsonStruct lhs;
    TComparableYsonStruct rhs;

    EXPECT_TRUE(lhs == rhs);

    lhs.UnregisteredValue = false;
    rhs.UnregisteredValue = true;
    EXPECT_TRUE(lhs == rhs);
}

TEST(TYsonStructTest, DontOverrideWithComparisonFromBase)
{
    TYsonStructWithComparableBase::BaseComparisonCalls = 0;
    auto cleanup = Finally([] {
        TYsonStructWithComparableBase::BaseComparisonCalls = 0;
    });

    TYsonStructWithComparableBase value;
    EXPECT_TRUE(value == value);

    EXPECT_EQ(TYsonStructWithComparableBase::BaseComparisonCalls, 0);
}

TEST(TYsonStructTest, OverrideWithComparisonFromTemplatedFunction)
{
    TemplatedComparisonCalls = 0;
    auto cleanup = Finally([] {
        TemplatedComparisonCalls = 0;
    });

    TYsonStructWithTemplatedComparison value;
    EXPECT_TRUE(value == value);

    EXPECT_EQ(TemplatedComparisonCalls, 1);
}

////////////////////////////////////////////////////////////////////////////////

struct THasFieldWithDefaultedStrategy
    : public TYsonStructLite
{
    TIntrusivePtr<TConfigWithOneLevelNesting> Field;

    REGISTER_YSON_STRUCT_LITE(THasFieldWithDefaultedStrategy);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("field", &TThis::Field)
            .Default()
            .DefaultUnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

struct THasFieldWithDefaultedStrategyAndOwnRecursiveStrategy
    : public TYsonStructLite
{
    TIntrusivePtr<TConfigWithOneLevelNesting> Field;

    REGISTER_YSON_STRUCT_LITE(THasFieldWithDefaultedStrategyAndOwnRecursiveStrategy);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        registrar.Parameter("field", &TThis::Field)
            .Default()
            .DefaultUnrecognizedStrategy(EUnrecognizedStrategy::Throw)
            .EnforceDefaultUnrecognizedStrategy();
    }
};

TEST(TYsonStructTest, DefaultUnrecognizedStrategy1)
{
    auto source = BuildYsonNodeFluently().BeginMap()
        .Item("field").BeginMap()
            .Item("unrecognized").Value(42)
        .EndMap()
    .EndMap();

    THasFieldWithDefaultedStrategy yson = {};
    EXPECT_ANY_THROW(Deserialize(yson, source->AsMap()));
}

TEST(TYsonStructTest, DefaultUnrecognizedStrategy2)
{
    auto source = BuildYsonNodeFluently().BeginMap()
        .Item("field").BeginMap()
            .Item("sub").BeginMap()
            .EndMap()
        .EndMap()
    .EndMap();

    THasFieldWithDefaultedStrategy yson = {};
    Deserialize(yson, source->AsMap());
}

TEST(TYsonStructTest, DefaultUnrecognizedStrategy3)
{
    auto source = BuildYsonNodeFluently().BeginMap()
        .Item("field").BeginMap()
            .Item("unrecognized").Value(42)
        .EndMap()
    .EndMap();

    THasFieldWithDefaultedStrategyAndOwnRecursiveStrategy yson = {};
    EXPECT_ANY_THROW(Deserialize(yson, source->AsMap()));
}

////////////////////////////////////////////////////////////////////////////////

struct TTestYsonStructWithProto
    : public virtual TYsonStruct
{
    NProto::TTestMessage DefaultProto;
    TProtoSerializedAsYson<NProto::TTestMessage> YsonProto;
    TProtoSerializedAsString<NProto::TTestMessage> StringProto;

    REGISTER_YSON_STRUCT(TTestYsonStructWithProto);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("default_proto", &TThis::DefaultProto)
            .Default();
        registrar.Parameter("yson_proto", &TThis::YsonProto)
            .Default();
        registrar.Parameter("string_proto", &TThis::StringProto)
            .Default();
    }
};

using TTestYsonStructWithProtoPtr = TIntrusivePtr<TTestYsonStructWithProto>;

TEST(TYsonStructTest, ProtoSerialize)
{
    NProto::TTestMessage proto;
    proto.set_int32_field(532);
    proto.set_string_field("abcdef");
    auto serialized = proto.SerializeAsString();

    auto ysonStruct = New<TTestYsonStructWithProto>();
    ysonStruct->DefaultProto.CopyFrom(proto);
    ysonStruct->YsonProto.CopyFrom(proto);
    ysonStruct->StringProto.CopyFrom(proto);

    auto node = ConvertToNode(ysonStruct);
    const auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("default_proto")
                .BeginMap()
                    .Item("int32_field").Value(532)
                    .Item("string_field").Value("abcdef")
                .EndMap()
            .Item("yson_proto")
                .BeginMap()
                    .Item("int32_field").Value(532)
                    .Item("string_field").Value("abcdef")
                .EndMap()
            .Item("string_proto").Value(serialized)
        .EndMap();
    EXPECT_TRUE(AreNodesEqual(node, expectedNode));
    auto otherStruct = ConvertTo<TTestYsonStructWithProtoPtr>(node);
    EXPECT_EQ(*otherStruct, *ysonStruct);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
