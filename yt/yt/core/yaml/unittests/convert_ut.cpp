#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yaml/convert.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYaml {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestConfig
    : public NYTree::TYsonStruct
{
    std::string Name;
    int Value;

    REGISTER_YSON_STRUCT(TTestConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("name", &TThis::Name)
            .Default();
        registrar.Parameter("value", &TThis::Value)
            .Default(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TTestConfig)

////////////////////////////////////////////////////////////////////////////////

TEST(TYamlHelpersTest, StringList)
{
    auto result = ConvertFromYaml<std::vector<std::string>>("- foo\n- bar\n");
    EXPECT_EQ(result, (std::vector<std::string>{"foo", "bar"}));
}

TEST(TYamlHelpersTest, Mapping)
{
    auto result = ConvertFromYaml<NYTree::INodePtr>("key: value\n");
    EXPECT_EQ(result->AsMap()->GetChildValueOrThrow<std::string>("key"), "value");
}

TEST(TYamlHelpersTest, YsonStruct)
{
    auto result = ConvertFromYaml<TIntrusivePtr<TTestConfig>>("name: hello\nvalue: 42\n");
    EXPECT_EQ(result->Name, "hello");
    EXPECT_EQ(result->Value, 42);
}

TEST(TYamlHelpersTest, InvalidYaml)
{
    EXPECT_THROW(ConvertFromYaml<NYTree::INodePtr>("key: [unclosed"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYaml
