#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <util/stream/buffer.h>

#include <util/ysaveload.h>

#include <array>

namespace NYT::NYTree {
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
    }
};

typedef TIntrusivePtr<TTestSubconfig> TTestSubconfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TTestConfig
    : public TYsonStruct
{
public:
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

typedef TIntrusivePtr<TTestConfig> TTestConfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TSimpleYsonSerializable
    : public TYsonSerializable
{
public:
    int IntValue;

    TSimpleYsonSerializable()
    {
        RegisterParameter("int_value", IntValue)
            .Default(1);
    }
};

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

class TYsonStructWithSimpleYsonSerializable
    : public TYsonStruct
{
public:
    TIntrusivePtr<TSimpleYsonSerializable> YsonSerializable;

    REGISTER_YSON_STRUCT(TYsonStructWithSimpleYsonSerializable);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        registrar.Parameter("yson_serializable", &TYsonStructWithSimpleYsonSerializable::YsonSerializable)
            .DefaultNew();
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
                .EndMap()
            .EndMap()
        .EndMap();
}

void TestCompleteSubconfig(TTestSubconfig* subconfig, int offset = 0)
{
    EXPECT_EQ(99 + offset, subconfig->MyInt);
    EXPECT_EQ(101u + offset, subconfig->MyUint);
    EXPECT_TRUE(subconfig->MyBool);
    EXPECT_EQ(3u, subconfig->MyStringList.size());
    EXPECT_EQ("ListItem0", subconfig->MyStringList[0]);
    EXPECT_EQ("ListItem1", subconfig->MyStringList[1]);
    EXPECT_EQ("ListItem2", subconfig->MyStringList[2]);
    EXPECT_EQ(ETestEnum::Value2, subconfig->MyEnum);
}

void TestCompleteConfig(TIntrusivePtr<TTestConfig> config, int offset = 0)
{
    EXPECT_EQ("TestString" + std::to_string(offset), config->MyString);
    TestCompleteSubconfig(config->Subconfig.Get(), offset);
    EXPECT_EQ(2u, config->SubconfigList.size());
    TestCompleteSubconfig(config->SubconfigList[0].Get(), offset);
    TestCompleteSubconfig(config->SubconfigList[1].Get(), offset);
    EXPECT_EQ(2u, config->SubconfigMap.size());
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
    EXPECT_EQ(200, config->Subconfig->MyInt);
    EXPECT_TRUE(config->Subconfig->MyBool);
    EXPECT_EQ(0u, config->Subconfig->MyStringList.size());
    EXPECT_EQ(ETestEnum::Value1, config->Subconfig->MyEnum);
    EXPECT_EQ(0u, config->SubconfigList.size());
    EXPECT_EQ(0u, config->SubconfigMap.size());
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

TEST_P(TYsonStructParseTest, UnrecognizedWithNestedYsonSerializable)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("yson_serializable").BeginMap()
                .Item("unrecognized").Value(1)
            .EndMap()
        .EndMap();

    auto config = Load<TYsonStructWithSimpleYsonSerializable>(configNode->AsMap());

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

    config->LoadParameter("my_string", ConvertToNode("test"), EMergeStrategy::Default);
    EXPECT_EQ("test", config->MyString);
    EXPECT_EQ(10, config->NullableInt);
}

TEST(TYsonStructTest, LoadSingleParameterWithMergeStrategy)
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
    config1->LoadParameter("sub", subConfig, EMergeStrategy::Default);
    EXPECT_EQ(100, config1->Subconfig->MyInt);
    EXPECT_TRUE(config1->Subconfig->MyBool);  // Subconfig merged by default.

    auto config2 = New<TTestConfig>();
    config2->Subconfig->MyBool = true;
    config2->LoadParameter("sub", subConfig, EMergeStrategy::Overwrite);
    EXPECT_EQ(100, config2->Subconfig->MyInt);
    EXPECT_FALSE(config2->Subconfig->MyBool);  // Overwrite destroyed previous values.
}

TEST(TYsonStructTest, ResetSingleParameter)
{
    auto config = New<TTestSubconfig>();
    config->MyInt = 10;
    config->MyUint = 10;

    config->ResetParameter("my_int");
    EXPECT_EQ(100, config->MyInt);  // Default value.
    EXPECT_EQ(10u, config->MyUint);
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
        "\"my_string_list\"=[]}";

    TString subconfigYsonOrigin =
        "{\"my_bool\"=%false;"
        "\"my_enum\"=\"value1\";"
        "\"my_int\"=100;"
        "\"my_uint\"=50u;"
        "\"my_string_list\"=[]}";

    TString expectedYson;
    expectedYson += "{\"my_string\"=\"hello!\";";
    expectedYson += "\"sub\"=" + subconfigYson + ";";
    expectedYson += "\"sub_list\"=[" + subconfigYsonOrigin + "];";
    expectedYson += "\"sub_map\"={\"item\"=" + subconfigYsonOrigin + "};";
    expectedYson += "\"nullable_int\"=42}";

    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(TYsonString(expectedYson)),
        ConvertToNode(TYsonString(output.AsStringBuf()))));
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
    }

    {
        auto newConfig = UpdateYsonStruct(config, ConvertToNode(TYsonString(TStringBuf("{\"sub\"={\"my_int_\"=150}}"))));
        EXPECT_EQ(newConfig->Subconfig->MyInt, 200);
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
    TTestConfigLite config = TTestConfigLite::Create();

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
    TTestLiteWithDefaults config = TTestLiteWithDefaults::Create();
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
}

class TTestConfigWithContainers
    : public NYTree::TYsonStructLite
{
public:
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
    TTestConfigWithContainers original = TTestConfigWithContainers::Create();
    TTestConfigWithContainers deserialized = TTestConfigWithContainers::Create();

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
    ASSERT_EQ(serialized, ConvertToYsonString(original, EYsonFormat::Text).AsStringBuf());

    Deserialize(deserialized, ConvertToNode(TYsonString(serialized, EYsonType::Node)));

    ASSERT_EQ(original, deserialized);
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
    }

    {
        auto config = ConvertTo<TIntrusivePtr<TConfigWithOptional>>(TYsonString(TStringBuf("{value=#}")));
        EXPECT_FALSE(config->Value);
        EXPECT_EQ(ENodeType::Entity, ConvertToNode(config)->AsMap()->GetChildOrThrow("value")->GetType());
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

class TVirtualInheritanceConfig
    : public virtual TYsonStruct
{
public:
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

class TYsonSerializableClass
    : public TYsonSerializable
{
public:
    TYsonSerializableClass()
    {
        RegisterParameter("int_value", IntValue)
            .Default(1);
    }

    int IntValue;
};

class TYsonStructClass
    : public TYsonStruct
{
public:
    THashMap<TString, TIntrusivePtr<TYsonSerializableClass>> YsonSerializableHashMap;

    TIntrusivePtr<TYsonSerializableClass> YsonSerializableValue;

    REGISTER_YSON_STRUCT(TYsonStructClass);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("yson_serializable_hash_map", &TThis::YsonSerializableHashMap)
            .Default();

        registrar.Parameter("yson_serializable_value", &TThis::YsonSerializableValue)
            .DefaultNew();

        registrar.Preprocessor([] (TYsonStructClass* klass) {
            klass->YsonSerializableValue->IntValue = 5;
        });
    }
};

TEST(TYsonStructTest, YsonSerializableNestedToYsonStructSimple)
{
    {
        auto config = New<TYsonStructClass>();
        EXPECT_EQ(config->YsonSerializableValue->IntValue, 5);

        config->YsonSerializableHashMap["x"] = New<TYsonSerializableClass>();
        config->YsonSerializableHashMap["x"]->IntValue = 10;
        config->YsonSerializableValue->IntValue = 2;

        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);
        TString expectedYson = "{yson_serializable_hash_map={x={int_value=10}};yson_serializable_value={int_value=2}}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));

        auto deserialized = ConvertTo<TIntrusivePtr<TYsonStructClass>>(output);
        EXPECT_EQ(deserialized->YsonSerializableHashMap["x"]->IntValue, 10);
        EXPECT_EQ(deserialized->YsonSerializableValue->IntValue, 2);

    }
}

TEST(TYsonStructTest, YsonSerializableNestedToYsonStructDeserializesFromEmpty)
{
    {
        auto testInput = TYsonString(TStringBuf("{yson_serializable_value={}}"));
        auto deserialized = ConvertTo<TIntrusivePtr<TYsonStructClass>>(testInput);
        EXPECT_EQ(deserialized->YsonSerializableValue->IntValue, 5);
    }
}
////////////////////////////////////////////////////////////////////////////////

class TClassLevelPostprocessConfig
    : public TYsonStruct
{
public:
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

class TRecursiveConfig
    : public TYsonStruct
{
public:
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

class TNestedYsonSerializableClass
    : public TYsonSerializable
{
public:
    TNestedYsonSerializableClass()
    {
        RegisterParameter("int_value", IntValue)
            .Default(1);
        RegisterPostprocessor([&] {
            IntValue = 10;
        });
    }

    int IntValue;
};

class TYsonStructClass2
    : public TYsonStruct
{
public:
    THashMap<TString, TIntrusivePtr<TNestedYsonSerializableClass>> YsonSerializableHashMap;

    REGISTER_YSON_STRUCT(TYsonStructClass2);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("yson_serializable_hash_map", &TYsonStructClass2::YsonSerializableHashMap)
            .Default();
    }
};

TEST(TYsonStructTest, PostprocessIsPropagatedFromYsonStructToYsonSerializable)
{
    auto testInput = TYsonString(TStringBuf("{yson_serializable_hash_map={x={int_value=2}}}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TYsonStructClass2>>(testInput);
    EXPECT_EQ(deserialized->YsonSerializableHashMap["x"]->IntValue, 10);
}

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
    TIntrusivePtr<TSimpleYsonSerializable> YsonSerializable;
    TIntrusivePtr<TSimpleYsonStruct> YsonStruct;

    REGISTER_YSON_STRUCT(TYsonStructWithNestedStructsAndCustomDefaults);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("yson_serializable", &TThis::YsonSerializable)
            .DefaultCtor([] () { return CreateCustomDefault<TSimpleYsonSerializable>(); });
        registrar.Parameter("yson_struct", &TThis::YsonStruct)
            .DefaultCtor([] () { return CreateCustomDefault<TSimpleYsonStruct>(); });
    }
};

TEST(TYsonStructTest, TestCustomDefaultsOfNestedStructsAreDiscardedOnDeserialize)
{
    auto deserialized = ConvertTo<TIntrusivePtr<TYsonStructWithNestedStructsAndCustomDefaults>>(TYsonString(TStringBuf("{}")));
    EXPECT_EQ(deserialized->YsonSerializable->IntValue, 1);
    EXPECT_EQ(deserialized->YsonStruct->IntValue, 1);
}

////////////////////////////////////////////////////////////////////////////////

class TYsonStructWithNestedStructsAndPreprocessors
    : public TYsonStruct
{
public:
    TIntrusivePtr<TSimpleYsonSerializable> YsonSerializable;
    TIntrusivePtr<TSimpleYsonStruct> YsonStruct;

    REGISTER_YSON_STRUCT(TYsonStructWithNestedStructsAndPreprocessors);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("yson_struct", &TThis::YsonStruct)
            .Default();
        registrar.Parameter("yson_serializable", &TThis::YsonSerializable)
            .Default();
        registrar.Preprocessor([] (TThis* s) {
            s->YsonSerializable = CreateCustomDefault<TSimpleYsonSerializable>();
            s->YsonStruct = CreateCustomDefault<TSimpleYsonStruct>();
        });
    }
};

TEST(TYsonStructTest, TestPreprocessorsEffectsOnNestedStructsArePreservedOnDeserialize)
{
    auto deserialized = ConvertTo<TIntrusivePtr<TYsonStructWithNestedStructsAndPreprocessors>>(TYsonString(TStringBuf("{}")));
    EXPECT_EQ(deserialized->YsonSerializable->IntValue, 10);
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
    auto deserialized = ConvertTo<TIntrusivePtr<TDerivedWithCustomConfigure>>(TYsonString(TStringBuf("{}")));
    EXPECT_EQ(deserialized->Int, 10);
    EXPECT_EQ(deserialized->Double, 2.2);
}

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

    auto liteConfig = TTestConfigLite::Create();
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
        TTestConfigLite LiteConfig = TTestConfigLite::Create();
        TString StructName;

        Y_SAVELOAD_DEFINE(Config1, Config2, LiteConfig, StructName);
    };

    TComplexStruct toSerialize{
        .Config1 = New<TTestConfig>(),
        .Config2 = New<TTestConfig>(),
        .LiteConfig = TTestConfigLite::Create(),
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

} // namespace
} // namespace NYT::NYTree
