#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/composite_map.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <algorithm>

namespace NYT::NYTree {
namespace {

using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> SortedList(const IYPathServicePtr& service, std::optional<i64> limit = {})
{
    auto keys = WaitFor(AsyncYPathList(service, TYPath(), limit))
        .ValueOrThrow();
    std::sort(keys.begin(), keys.end());
    return keys;
}

int GetInt(const IYPathServicePtr& service, const TYPath& path)
{
    return ConvertTo<int>(WaitFor(AsyncYPathGet(service, path))
        .ValueOrThrow());
}

bool Exists(const IYPathServicePtr& service, const TYPath& path)
{
    return WaitFor(AsyncYPathExists(service, path))
        .ValueOrThrow();
}

IYPathServicePtr MakeValueService(int value)
{
    return IYPathService::FromProducer(BIND([value] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer).Value(value);
    }));
}

TYsonProducer MakeMapProducer(std::vector<std::pair<std::string, int>> items)
{
    return BIND([items = std::move(items)] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoMapFor(items, [] (TFluentMap fluent, const auto& item) {
                fluent.Item(item.first).Value(item.second);
            });
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCompositeMapServiceTest, AddChild)
{
    auto service = CreateCompositeMapService()
        ->AddChild("a", MakeValueService(1))
        ->AddChild("b", MakeValueService(2));

    EXPECT_EQ(SortedList(service), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(GetInt(service, "/a"), 1);
    EXPECT_EQ(GetInt(service, "/b"), 2);
    EXPECT_FALSE(Exists(service, "/c"));
}

TEST(TCompositeMapServiceTest, AddChildren)
{
    auto service = CreateCompositeMapService()
        ->AddChildren(MakeMapProducer({{"x", 10}, {"y", 20}}));

    EXPECT_EQ(SortedList(service), (std::vector<std::string>{"x", "y"}));
    EXPECT_EQ(GetInt(service, "/x"), 10);
    EXPECT_EQ(GetInt(service, "/y"), 20);
    EXPECT_FALSE(Exists(service, "/z"));
}

TEST(TCompositeMapServiceTest, ChildAndChildrenCombined)
{
    auto service = CreateCompositeMapService()
        ->AddChild("a", MakeValueService(1))
        ->AddChildren(MakeMapProducer({{"b", 2}, {"c", 3}}));

    EXPECT_EQ(SortedList(service), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(GetInt(service, "/a"), 1);
    EXPECT_EQ(GetInt(service, "/c"), 3);
}

TEST(TCompositeMapServiceTest, ListRespectsLimit)
{
    auto service = CreateCompositeMapService()
        ->AddChild("a", MakeValueService(1))
        ->AddChildren(MakeMapProducer({{"b", 2}, {"c", 3}}));

    EXPECT_EQ(std::ssize(SortedList(service, /*limit*/ 2)), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
