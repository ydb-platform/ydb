#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/list_verb_lazy_yson_consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYson {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TString BuildYsonAndGetResult(std::function<void(TFluentAny&)> build, std::optional<int> limit = std::nullopt)
{
    TStringStream yson;
    TYsonWriter writer(&yson, EYsonFormat::Text);
    TListVerbLazyYsonConsumer consumer(&writer, limit);
    auto fluent = BuildYsonFluently(&consumer);

    build(fluent);

    return yson.Str();
}

TEST(TListVerbLazyYsonConsumerTest, TestSimple)
{
    TString actual = BuildYsonAndGetResult([] (TFluentAny& fluent) {
        fluent
            .BeginMap()
                .Item("a").Value("x")
                .Item("b").Value("y")
            .EndMap();
    });

    EXPECT_EQ("[\"a\";\"b\";]", actual);
}

TEST(TListVerbLazyYsonConsumerTest, TestLimit)
{
    TString actual = BuildYsonAndGetResult([] (TFluentAny& fluent) {
        fluent
            .BeginMap()
                .Item("a").Value("x")
                .Item("b").Value("y")
                .Item("c").Value("z")
                .Item("d").Value("u")
            .EndMap();
    }, 3);

    EXPECT_EQ("[\"a\";\"b\";\"c\";]", actual);
}

TEST(TListVerbLazyYsonConsumerTest, TestNestedMap)
{
    TString actual = BuildYsonAndGetResult([] (TFluentAny& fluent) {
        fluent
            .BeginMap()
                .Item("a").Value("x")
                .Item("b").BeginMap()
                    .Item("bb").Value("z")
                .EndMap()
            .EndMap();
    });

    EXPECT_EQ("[\"a\";\"b\";]", actual);
}

TEST(TListVerbLazyYsonConsumerTest, TestThrowOnNonMapNode)
{
    EXPECT_THROW(BuildYsonAndGetResult([] (TFluentAny fluent) { fluent.Value("a"); }), std::exception);
    EXPECT_THROW(BuildYsonAndGetResult([] (TFluentAny fluent) { fluent.Value(1); }), std::exception);
    EXPECT_THROW(BuildYsonAndGetResult([] (TFluentAny fluent) { fluent.Value(2u); }), std::exception);
    EXPECT_THROW(BuildYsonAndGetResult([] (TFluentAny fluent) { fluent.Value(0.1); }), std::exception);
    EXPECT_THROW(BuildYsonAndGetResult([] (TFluentAny fluent) { fluent.Value(true); }), std::exception);
    EXPECT_THROW(BuildYsonAndGetResult([] (TFluentAny fluent) { fluent.Entity(); }), std::exception);
    EXPECT_THROW(BuildYsonAndGetResult([] (TFluentAny fluent) { fluent.BeginList(); }), std::exception);
}

TEST(TListVerbLazyYsonConsumerTest, TestIgnoreAttributes)
{
    TString actual = BuildYsonAndGetResult([] (TFluentAny& fluent) {
        fluent
            .BeginAttributes()
                .Item("attr1").Value("v1")
                .Item("attr2").Value("v2")
            .EndAttributes()
            .BeginMap()
                .Item("a")
                    .BeginAttributes()
                        .Item("attr1").Value("v1")
                        .Item("attr2").Value("v2")
                    .EndAttributes()
                    .Value("x")
                .Item("b").BeginMap()
                    .Item("bb").Value("z")
                .EndMap()
            .EndMap();
    });
    auto expected = std::vector<TString>{"a", "b"};
    EXPECT_EQ("[\"a\";\"b\";]", actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
