#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TAttributesTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    { }

    void TearDown() override
    { }
};

TEST(TAttributesTest, CheckAccessors)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set<TString>("name", "Petr");
    attributes->Set<int>("age", 30);
    attributes->Set<double>("weight", 70.5);

    auto keys_ = attributes->ListKeys();
    THashSet<TString> keys(keys_.begin(), keys_.end());
    THashSet<TString> expectedKeys{
        "name",
        "age",
        "weight",
    };
    EXPECT_EQ(keys, expectedKeys);

    auto pairs_ = attributes->ListPairs();
    THashSet<IAttributeDictionary::TKeyValuePair> pairs(pairs_.begin(), pairs_.end());
    THashSet<IAttributeDictionary::TKeyValuePair> expectedPairs{
        {"name", ConvertToYsonString("Petr")},
        {"age", ConvertToYsonString(30)},
        {"weight", ConvertToYsonString(70.5)},
    };
    EXPECT_EQ(pairs, expectedPairs);

    EXPECT_EQ("Petr", attributes->Get<TString>("name"));
    EXPECT_THROW(attributes->Get<int>("name"), std::exception);

    EXPECT_EQ(30, attributes->Find<int>("age"));
    EXPECT_EQ(30, attributes->Get<int>("age"));
    EXPECT_THROW(attributes->Get<char>("age"), std::exception);

    EXPECT_EQ(70.5, attributes->Get<double>("weight"));
    EXPECT_THROW(attributes->Get<TString>("weight"), std::exception);

    EXPECT_FALSE(attributes->Find<int>("unknown_key"));
    EXPECT_EQ(42, attributes->Get<int>("unknown_key", 42));
    EXPECT_THROW(attributes->Get<double>("unknown_key"), std::exception);
}

TEST(TAttributesTest, MergeFromTest)
{
    auto attributesX = CreateEphemeralAttributes();
    attributesX->Set<TString>("name", "Petr");
    attributesX->Set<int>("age", 30);

    auto attributesY = CreateEphemeralAttributes();
    attributesY->Set<TString>("name", "Oleg");

    attributesX->MergeFrom(*attributesY);
    EXPECT_EQ("Oleg", attributesX->Get<TString>("name"));
    EXPECT_EQ(30, attributesX->Get<int>("age"));

    auto node = ConvertToNode(TYsonString(TStringBuf("{age=20}")));
    attributesX->MergeFrom(node->AsMap());
    EXPECT_EQ("Oleg", attributesX->Get<TString>("name"));
    EXPECT_EQ(20, attributesX->Get<int>("age"));
}

TEST(TAttributesTest, SerializeToNode)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set<TString>("name", "Petr");
    attributes->Set<int>("age", 30);

    auto node = ConvertToNode(*attributes);
    auto convertedAttributes = ConvertToAttributes(node);
    EXPECT_EQ(*attributes, *convertedAttributes);
}

TEST(TAttributesTest, TrySerializeProtoToRef)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set<TString>("name", "Petr");
    attributes->Set<int>("age", 30);

    NProto::TAttributeDictionary protoAttributes;
    ToProto(&protoAttributes, *attributes);
    auto convertedAttributes = FromProto(protoAttributes);
    EXPECT_EQ(*attributes, *convertedAttributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
