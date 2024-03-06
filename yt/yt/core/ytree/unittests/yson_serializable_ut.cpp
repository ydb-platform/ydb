#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

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
    : public TYsonSerializable
{
    int MyInt;
    unsigned int MyUint;
    bool MyBool;
    std::vector<TString> MyStringList;
    ETestEnum MyEnum;

    TTestSubconfig()
    {
        RegisterParameter("my_int", MyInt).Default(100).InRange(95, 205);
        RegisterParameter("my_uint", MyUint).Default(50).InRange(31, 117);
        RegisterParameter("my_bool", MyBool).Default(false);
        RegisterParameter("my_string_list", MyStringList).Default();
        RegisterParameter("my_enum", MyEnum).Default(ETestEnum::Value1);
    }
};

using TTestSubconfigPtr = TIntrusivePtr<TTestSubconfig>;

////////////////////////////////////////////////////////////////////////////////

class TTestConfig
    : public TYsonSerializable
{
public:
    TString MyString;
    TTestSubconfigPtr Subconfig;
    std::vector<TTestSubconfigPtr> SubconfigList;
    std::unordered_map<TString, TTestSubconfigPtr> SubconfigMap;
    std::optional<i64> NullableInt;

    TTestConfig()
    {
        SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        RegisterParameter("my_string", MyString).NonEmpty();
        RegisterParameter("sub", Subconfig).DefaultNew();
        RegisterParameter("sub_list", SubconfigList).Default();
        RegisterParameter("sub_map", SubconfigMap).Default();
        RegisterParameter("nullable_int", NullableInt).Default();

        RegisterPreprocessor([&] () {
            MyString = "x";
            Subconfig->MyInt = 200;
        });
    }
};

using TTestConfigPtr = TIntrusivePtr<TTestConfig>;

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

class TYsonSerializableWithSimpleYsonStruct
    : public TYsonSerializable
{
public:
    TIntrusivePtr<TSimpleYsonStruct> YsonStruct;

    TYsonSerializableWithSimpleYsonStruct()
    {
        SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        RegisterParameter("yson_struct", YsonStruct)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

// {LoadFromNode}
using TYsonSerializableParseTestParameter = std::tuple<bool>;

class TYsonSerializableParseTest
    : public ::testing::TestWithParam<TYsonSerializableParseTestParameter>
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

    template <typename T>
    TIntrusivePtr<T> Load(const TYsonStringBuf& yson)
    {
        return Load<T>(ConvertTo<INodePtr>(yson));
    }
};

INSTANTIATE_TEST_SUITE_P(
    LoadFromNode,
    TYsonSerializableParseTest,
    ::testing::Values(TYsonSerializableParseTestParameter{
        true
    })
);

INSTANTIATE_TEST_SUITE_P(
    LoadFromCursor,
    TYsonSerializableParseTest,
    ::testing::Values(TYsonSerializableParseTestParameter{
        false
    })
);

////////////////////////////////////////////////////////////////////////////////

void TestCompleteSubconfig(TTestSubconfig* subconfig)
{
    EXPECT_EQ(99, subconfig->MyInt);
    EXPECT_EQ(101u, subconfig->MyUint);
    EXPECT_TRUE(subconfig->MyBool);
    EXPECT_EQ(3u, subconfig->MyStringList.size());
    EXPECT_EQ("ListItem0", subconfig->MyStringList[0]);
    EXPECT_EQ("ListItem1", subconfig->MyStringList[1]);
    EXPECT_EQ("ListItem2", subconfig->MyStringList[2]);
    EXPECT_EQ(ETestEnum::Value2, subconfig->MyEnum);
}

TEST_P(TYsonSerializableParseTest, Complete)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Value(99)
                .Item("my_uint").Value(101)
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
                    .Item("my_int").Value(99)
                    .Item("my_uint").Value(101)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("my_int").Value(99)
                    .Item("my_uint").Value(101)
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
                    .Item("my_int").Value(99)
                    .Item("my_uint").Value(101)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                .EndMap()
                .Item("sub2").BeginMap()
                    .Item("my_int").Value(99)
                    .Item("my_uint").Value(101)
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

    auto config = Load<TTestConfig>(configNode->AsMap());

    EXPECT_EQ("TestString", config->MyString);
    TestCompleteSubconfig(config->Subconfig.Get());
    EXPECT_EQ(2u, config->SubconfigList.size());
    TestCompleteSubconfig(config->SubconfigList[0].Get());
    TestCompleteSubconfig(config->SubconfigList[1].Get());
    EXPECT_EQ(2u, config->SubconfigMap.size());
    auto it1 = config->SubconfigMap.find("sub1");
    EXPECT_FALSE(it1 == config->SubconfigMap.end());
    TestCompleteSubconfig(it1->second.Get());
    auto it2 = config->SubconfigMap.find("sub2");
    EXPECT_FALSE(it2 == config->SubconfigMap.end());
    TestCompleteSubconfig(it2->second.Get());
}

TEST_P(TYsonSerializableParseTest, MissingParameter)
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

TEST_P(TYsonSerializableParseTest, MissingSubconfig)
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

TEST_P(TYsonSerializableParseTest, UnrecognizedSimple)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("option").Value(1)
        .EndMap();

    auto config = Load<TTestConfig>(configNode->AsMap());

    auto unrecognizedNode = config->GetUnrecognized();
    auto unrecognizedRecursivelyNode = config->GetUnrecognizedRecursively();
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

TEST_P(TYsonSerializableParseTest, UnrecognizedRecursive)
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

    auto unrecognizedRecursivelyNode = config->GetUnrecognizedRecursively();
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

TEST_P(TYsonSerializableParseTest, UnrecognizedWithNestedYsonStruct)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("yson_struct").BeginMap()
                .Item("unrecognized").Value(1)
            .EndMap()
        .EndMap();

    auto config = Load<TYsonSerializableWithSimpleYsonStruct>(configNode->AsMap());

    auto unrecognized = config->GetRecursiveUnrecognized();
    EXPECT_EQ(
        ConvertToYsonString(configNode, EYsonFormat::Text).AsStringBuf(),
        ConvertToYsonString(unrecognized, EYsonFormat::Text).AsStringBuf());
}

TEST_P(TYsonSerializableParseTest, MissingRequiredParameter)
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

TEST_P(TYsonSerializableParseTest, IncorrectNodeType)
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

TEST_P(TYsonSerializableParseTest, ArithmeticOverflow)
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

TEST_P(TYsonSerializableParseTest, Postprocess)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("") // empty!
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfig>(configNode, false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST_P(TYsonSerializableParseTest, PostprocessSubconfig)
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

    auto config = Load<TTestConfig>(configNode, false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST_P(TYsonSerializableParseTest, PostprocessSubconfigList)
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

    auto config = Load<TTestConfig>(configNode, false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST_P(TYsonSerializableParseTest, PostprocessSubconfigMap)
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

    auto config = Load<TTestConfig>(configNode, false);
    EXPECT_THROW(config->Postprocess(), std::exception);
}

TEST(TYsonSerializableTest, SaveSingleParameter)
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

TEST(TYsonSerializableTest, LoadSingleParameter)
{
    auto config = New<TTestConfig>();
    config->NullableInt = 10;

    config->LoadParameter("my_string", ConvertToNode("test"), EMergeStrategy::Default);
    EXPECT_EQ("test", config->MyString);
    EXPECT_EQ(10, config->NullableInt);
}

TEST(TYsonSerializableTest, LoadSingleParameterWithMergeStrategy)
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

TEST(TYsonSerializableTest, ResetSingleParameter)
{
    auto config = New<TTestSubconfig>();
    config->MyInt = 10;
    config->MyUint = 10;

    config->ResetParameter("my_int");
    EXPECT_EQ(100, config->MyInt);  // Default value.
    EXPECT_EQ(10u, config->MyUint);
}

TEST(TYsonSerializableTest, Save)
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
        ConvertToNode(output)));
}

TEST(TYsonSerializableTest, TestConfigUpdate)
{
    auto config = New<TTestConfig>();
    {
        auto newConfig = UpdateYsonSerializable(config, nullptr);
        EXPECT_EQ(newConfig->Subconfig->MyInt, 200);
    }

    {
        auto newConfig = UpdateYsonSerializable(config, ConvertToNode(TYsonString(TStringBuf("{\"sub\"={\"my_int\"=150}}"))));
        EXPECT_EQ(newConfig->Subconfig->MyInt, 150);
    }

    {
        auto newConfig = UpdateYsonSerializable(config, ConvertToNode(TYsonString(TStringBuf("{\"sub\"={\"my_int_\"=150}}"))));
        EXPECT_EQ(newConfig->Subconfig->MyInt, 200);
    }
}

TEST(TYsonSerializableTest, NoDefaultNewAliasing)
{
    auto config1 = New<TTestConfig>();
    auto config2 = New<TTestConfig>();
    EXPECT_NE(config1->Subconfig, config2->Subconfig);
}

TEST(TYsonSerializableTest, Reconfigure)
{
    auto config = New<TTestConfig>();
    auto subconfig = config->Subconfig;

    EXPECT_EQ("x", config->MyString);
    EXPECT_EQ(200, subconfig->MyInt);

    auto configNode1 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("y")
        .EndMap();
    ReconfigureYsonSerializable(config, configNode1);

    EXPECT_EQ("y", config->MyString);
    EXPECT_EQ(subconfig, config->Subconfig);
    EXPECT_EQ(200, subconfig->MyInt);

    auto configNode2 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("z")
            .Item("sub").BeginMap()
                .Item("my_int").Value(95)
            .EndMap()
        .EndMap();
    ReconfigureYsonSerializable(config, configNode2);

    EXPECT_EQ("z", config->MyString);
    EXPECT_EQ(subconfig, config->Subconfig);
    EXPECT_EQ(95, subconfig->MyInt);
}

////////////////////////////////////////////////////////////////////////////////

class TTestConfigLite
    : public TYsonSerializableLite
{
public:
    TString MyString;
    std::optional<i64> NullableInt;

    TTestConfigLite()
    {
        RegisterParameter("my_string", MyString).NonEmpty();
        RegisterParameter("nullable_int", NullableInt).Default();
    }
};

TEST(TYsonSerializableTest, SaveLite)
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
        ConvertToNode(output)));
}

////////////////////////////////////////////////////////////////////////////////

class TTestConfigWithAliases
    : public TYsonSerializable
{
public:
    TString Value;

    TTestConfigWithAliases()
    {
        RegisterParameter("key", Value)
            .Alias("alias1")
            .Alias("alias2");
    }
};

TEST_P(TYsonSerializableParseTest, Aliases1)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfigWithAliases>(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
}

TEST_P(TYsonSerializableParseTest, Aliases2)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfigWithAliases>(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
}

TEST_P(TYsonSerializableParseTest, Aliases3)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value")
            .Item("alias2").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = Load<TTestConfigWithAliases>(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
}

TEST_P(TYsonSerializableParseTest, Aliases4)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value1")
            .Item("alias2").Value("value2")
        .EndMap();
    auto configNode = builder->EndTree();

    EXPECT_THROW(Load<TTestConfigWithAliases>(configNode->AsMap()), std::exception);
}

TEST_P(TYsonSerializableParseTest, Aliases5)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
        .EndMap();
    auto configNode = builder->EndTree();

    EXPECT_THROW(Load<TTestConfigWithAliases>(configNode->AsMap()), std::exception);
}

TEST_P(TYsonSerializableParseTest, ParameterTuplesAndContainers)
{
    class TTestClass
        : public NYTree::TYsonSerializable
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

        TTestClass()
        {
            RegisterParameter("vector", Vector)
                .Default();
            RegisterParameter("array", Array)
                .Default();
            RegisterParameter("pair", Pair)
                .Default();
            RegisterParameter("set", Set)
                .Default();
            RegisterParameter("map", Map)
                .Default();
            RegisterParameter("multiset", MultiSet)
                .Default();
            RegisterParameter("unordered_set", UnorderedSet)
                .Default();
            RegisterParameter("unordered_map", UnorderedMap)
                .Default();
            RegisterParameter("unordered_multiset", UnorderedMultiSet)
                .Default();
        }
    };

    auto original = New<TTestClass>();
    original->Vector = { "fceswf", "sadfcesa" };
    original->Array = {{ "UYTUY", ":LL:a", "78678678" }};
    original->Pair = { 7U, "UYTUY" };
    original->Set = { "  q!", "12343e", "svvr", "0001" };
    original->Map = { {"!", 4398}, {"zzz", 0} };
    original->MultiSet = { 33, 33, 22, 22, 11 };
    original->UnorderedSet = { "41", "52", "001", "set" };
    original->UnorderedMap = { {"12345", 8}, {"XXX", 9}, {"XYZ", 42} };
    original->UnorderedMultiSet = { 1U, 2U, 1U, 0U, 0U };

    auto deserialized = Load<TTestClass>(ConvertToYsonString(*original));

    EXPECT_EQ(original->Vector, deserialized->Vector);
    EXPECT_EQ(original->Array, deserialized->Array);
    EXPECT_EQ(original->Pair, deserialized->Pair);
    EXPECT_EQ(original->Set, deserialized->Set);
    EXPECT_EQ(original->Map, deserialized->Map);
    EXPECT_EQ(original->MultiSet, deserialized->MultiSet);
    EXPECT_EQ(original->UnorderedSet, deserialized->UnorderedSet);
    EXPECT_EQ(original->UnorderedMap, deserialized->UnorderedMap);
    EXPECT_EQ(original->UnorderedMultiSet, deserialized->UnorderedMultiSet);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonSerializableTest, EnumAsKeyToYHash)
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

TEST_P(TYsonSerializableParseTest, NullableWithNonNullDefault)
{
    class TConfig
        : public TYsonSerializable
    {
    public:
        std::optional<int> Value;

        TConfig()
        {
            RegisterParameter("value", Value)
                .Default(123);
        }
    };

    {
        auto config = Load<TConfig>(TYsonStringBuf("{}"));
        EXPECT_EQ(123, *config->Value);
        EXPECT_EQ(123, ConvertToNode(config)->AsMap()->GetChildValueOrThrow<i64>("value"));
    }

    {
        auto config = Load<TConfig>(TYsonStringBuf("{value=#}"));
        EXPECT_FALSE(config->Value);
        EXPECT_EQ(ENodeType::Entity, ConvertToNode(config)->AsMap()->GetChildOrThrow("value")->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonSerializableTest, DontSerializeDefault)
{
    class TConfig
        : public TYsonSerializable
    {
    public:
        int Value;
        int OtherValue;

        TConfig()
        {
            RegisterParameter("value", Value)
                .Default(123);
            RegisterParameter("other_value", OtherValue)
                .Default(456)
                .DontSerializeDefault();
        }
    };

    {
        auto config = New<TConfig>();
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=123;}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(output)));
    }

    {
        auto config = New<TConfig>();
        config->OtherValue = 789;
        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

        TString expectedYson = "{\"value\"=123;\"other_value\"=789;}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(output)));
    }
}

class TYsonStructClass
    : public TYsonStruct
{
public:
    int IntValue;

    REGISTER_YSON_STRUCT(TYsonStructClass);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int_value", &TThis::IntValue)
            .Default(1);
    }
};

class TYsonSerializableClass
    : public TYsonSerializable
{
public:
    THashMap<TString, TIntrusivePtr<TYsonStructClass>> YsonStructHashMap;

    TIntrusivePtr<TYsonStructClass> YsonStructValue;

    TYsonSerializableClass()
    {
        RegisterParameter("yson_struct_hash_map", YsonStructHashMap)
            .Default();

        RegisterParameter("yson_struct_value", YsonStructValue)
            .DefaultNew();

        RegisterPreprocessor([&] () {
            YsonStructValue->IntValue = 5;
        });
    }
};

TEST_P(TYsonSerializableParseTest, YsonStructNestedToYsonSerializableSimple)
{
    {
        auto config = New<TYsonSerializableClass>();
        EXPECT_EQ(config->YsonStructValue->IntValue, 5);

        config->YsonStructHashMap["x"] = New<TYsonStructClass>();
        config->YsonStructHashMap["x"]->IntValue = 10;
        config->YsonStructValue->IntValue = 2;

        auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);
        TString expectedYson = "{yson_struct_hash_map={x={int_value=10}};yson_struct_value={int_value=2}}";
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(expectedYson)),
            ConvertToNode(TYsonString(output.AsStringBuf()))));

        auto deserialized = Load<TYsonSerializableClass>(output);
        EXPECT_EQ(deserialized->YsonStructHashMap["x"]->IntValue, 10);
        EXPECT_EQ(deserialized->YsonStructValue->IntValue, 2);

    }
}

TEST_P(TYsonSerializableParseTest, YsonStructNestedToYsonSerializableDeserializesFromEmpty)
{
    {
        auto testInput = TYsonString(TStringBuf("{yson_struct_value={}}"));
        auto deserialized = Load<TYsonSerializableClass>(testInput);
        EXPECT_EQ(deserialized->YsonStructValue->IntValue, 5);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNestedYsonStructClass
    : public TYsonStruct
{
public:
    int IntValue;

    REGISTER_YSON_STRUCT(TNestedYsonStructClass);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int_value", &TThis::IntValue)
            .Default(1);
        registrar.Postprocessor([&] (TNestedYsonStructClass* klass) {
            klass->IntValue = 10;
        });
    }
};

class TYsonSerializableClass2
    : public TYsonSerializable
{
public:
    THashMap<TString, TIntrusivePtr<TNestedYsonStructClass>> YsonStructHashMap;

    TYsonSerializableClass2()
    {
        RegisterParameter("yson_struct_hash_map", YsonStructHashMap)
            .Default();
    }
};

TEST_P(TYsonSerializableParseTest, PostprocessIsPropagatedFromYsonSerializableToYsonStruct)
{
    auto testInput = TYsonString(TStringBuf("{yson_struct_hash_map={x={int_value=2}}}"));
    auto deserialized = Load<TYsonSerializableClass2>(testInput);
    EXPECT_EQ(deserialized->YsonStructHashMap["x"]->IntValue, 10);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> CreateCustomDefault()
{
    auto result = New<T>();
    result->IntValue = 10;
    return result;
}

class TYsonSerializableWithNestedStructsAndCustomDefaults
    : public TYsonSerializable
{
public:
    TIntrusivePtr<TSimpleYsonSerializable> YsonSerializable;
    TIntrusivePtr<TSimpleYsonStruct> YsonStruct;

    TYsonSerializableWithNestedStructsAndCustomDefaults()
    {
        RegisterParameter("yson_serializable", YsonSerializable)
            .Default(CreateCustomDefault<TSimpleYsonSerializable>());
        RegisterParameter("yson_struct", YsonStruct)
            .Default(CreateCustomDefault<TSimpleYsonStruct>());
    }
};

TEST(TYsonSerializableTest, TestCustomDefaultsOfNestedStructsAreDiscardedOnDeserialize)
{
    auto testInput = TYsonString(TStringBuf("{}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TYsonSerializableWithNestedStructsAndCustomDefaults>>(testInput);
    EXPECT_EQ(deserialized->YsonSerializable->IntValue, 1);
    EXPECT_EQ(deserialized->YsonStruct->IntValue, 1);
}

////////////////////////////////////////////////////////////////////////////////

class TYsonSerializableWithNestedStructsAndPreprocessors
    : public TYsonSerializable
{
public:
    TIntrusivePtr<TSimpleYsonSerializable> YsonSerializable;
    TIntrusivePtr<TSimpleYsonStruct> YsonStruct;

    TYsonSerializableWithNestedStructsAndPreprocessors()
    {
        RegisterParameter("yson_serializable", YsonSerializable)
            .Default();
        RegisterParameter("yson_struct", YsonStruct)
            .Default();
        RegisterPreprocessor([&] () {
            YsonSerializable = CreateCustomDefault<TSimpleYsonSerializable>();
            YsonStruct = CreateCustomDefault<TSimpleYsonStruct>();
        });
    }
};

TEST(TYsonSerializableTest, TestPreprocessorsEffectsOnNestedStructsArePreservedOnDeserialize)
{
    auto testInput = TYsonString(TStringBuf("{}"));
    auto deserialized = ConvertTo<TIntrusivePtr<TYsonSerializableWithNestedStructsAndPreprocessors>>(testInput);
    EXPECT_EQ(deserialized->YsonSerializable->IntValue, 10);
    EXPECT_EQ(deserialized->YsonStruct->IntValue, 10);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonSerializableTest, TestStable)
{
    class TInner
        : public NYTree::TYsonSerializable
    {
    public:
        int A;
        int B;
        int C;
        int D;
        int Q;

        TInner()
        {
            RegisterParameter("b", B)
                .Default(2);
            RegisterParameter("a", A)
                .Default(1);
            RegisterParameter("c", C)
                .Default(3);
            RegisterParameter("q", Q)
                .Default(9);
            RegisterParameter("d", D)
                .Default(4);
        }
    };

    class TOuter
        : public TYsonSerializable
    {
    public:
        TIntrusivePtr<TInner> Inner;
        TOuter()
        {
            RegisterParameter("inner", Inner)
                .DefaultNew();
        }
    };

    {
        auto outer = New<TOuter>();
        auto output = ConvertToYsonString(*outer, NYson::EYsonFormat::Text);

        auto result = BuildYsonStringFluently(NYson::EYsonFormat::Text)
            .BeginMap()
                .Item("inner").BeginMap()
                    .Item("a").Value(1)
                    .Item("b").Value(2)
                    .Item("c").Value(3)
                    .Item("d").Value(4)
                    .Item("q").Value(9)
                .EndMap()
            .EndMap();

        EXPECT_EQ(result, output);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
