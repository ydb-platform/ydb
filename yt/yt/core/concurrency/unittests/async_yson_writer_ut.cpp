#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NYson {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonString ConvertToListFragment(const std::vector<T>& items)
{
    TStringStream output;
    TYsonWriter writer(&output, EYsonFormat::Binary, EYsonType::ListFragment);
    for (const auto& item : items) {
        using NYT::NYTree::Serialize;
        writer.OnListItem();
        Serialize(item, &writer);
    }
    return TYsonString(output.Str(), EYsonType::ListFragment);
}

template <class K, class V>
TYsonString ConvertToMapFragment(const std::vector<std::pair<K, V>>& items)
{
    TStringStream output;
    TYsonWriter writer(&output, EYsonFormat::Binary, EYsonType::MapFragment);
    for (const auto& item : items) {
        using NYT::NYTree::Serialize;
        writer.OnKeyedItem(item.first);
        Serialize(item.second, &writer);
    }
    return TYsonString(output.Str(), EYsonType::MapFragment);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncYsonWriterTest, SyncNode)
{
    TAsyncYsonWriter asyncWriter(EYsonType::Node);
    asyncWriter.OnInt64Scalar(123);
    EXPECT_EQ(
        ConvertToYsonString(123),
        asyncWriter.Finish().Get().ValueOrThrow());
}

TEST(TAsyncYsonWriterTest, SyncList)
{
    TAsyncYsonWriter writer(EYsonType::Node);
    writer.OnBeginList();
        writer.OnListItem();
        writer.OnStringScalar("a");
        writer.OnListItem();
        writer.OnStringScalar("b");
        writer.OnListItem();
        writer.OnStringScalar("c");
    writer.OnEndList();
    EXPECT_EQ(
        ConvertToYsonString(std::vector<TString>{"a", "b", "c"}),
        writer.Finish().Get().ValueOrThrow());
}

TEST(TAsyncYsonWriterTest, SyncListFragment)
{
    TAsyncYsonWriter writer(EYsonType::ListFragment);
    writer.OnListItem();
    writer.OnStringScalar("a");
    writer.OnListItem();
    writer.OnStringScalar("b");
    writer.OnListItem();
    writer.OnStringScalar("c");
    EXPECT_EQ(
        ConvertToListFragment(std::vector<TString>{"a", "b", "c"}),
        writer.Finish().Get().ValueOrThrow());
}

TEST(TAsyncYsonWriterTest, SyncMapFragment)
{
    TAsyncYsonWriter writer(EYsonType::MapFragment);
    writer.OnKeyedItem("a");
    writer.OnInt64Scalar(1);
    writer.OnKeyedItem("b");
    writer.OnInt64Scalar(2);
    writer.OnKeyedItem("c");
    writer.OnInt64Scalar(3);
    EXPECT_EQ(
        ConvertToMapFragment(std::vector<std::pair<TString, int>>{{"a", 1}, {"b", 2}, {"c", 3}}),
        writer.Finish().Get().ValueOrThrow());
}

TEST(TAsyncYsonWriterTest, AsyncNode)
{
    TAsyncYsonWriter asyncWriter(EYsonType::Node);
    asyncWriter.OnRaw(MakeFuture(ConvertToYsonString(123)));
    EXPECT_EQ(
        ConvertToYsonString(123),
        asyncWriter.Finish().Get().ValueOrThrow());
}

TEST(TAsyncYsonWriterTest, AsyncListFragment)
{
    TAsyncYsonWriter writer(EYsonType::ListFragment);
    writer.OnListItem();
    writer.OnRaw(MakeFuture(ConvertToYsonString(1)));
    writer.OnListItem();
    writer.OnRaw(MakeFuture(ConvertToYsonString(2)));
    writer.OnListItem();
    writer.OnRaw(MakeFuture(ConvertToYsonString(3)));
    EXPECT_EQ(
        ConvertToListFragment(std::vector<int>{1, 2, 3}),
        writer.Finish().Get().ValueOrThrow());
}

TEST(TAsyncYsonWriterTest, AsyncList)
{
    TAsyncYsonWriter writer(EYsonType::Node);
    writer.OnBeginList();
        writer.OnListItem();
        writer.OnRaw(MakeFuture(ConvertToYsonString(1)));
        writer.OnListItem();
        writer.OnRaw(MakeFuture(ConvertToYsonString(2)));
        writer.OnListItem();
        writer.OnRaw(MakeFuture(ConvertToYsonString(3)));
    writer.OnEndList();
    EXPECT_EQ(
        ConvertToYsonString(std::vector<int>{1, 2, 3}),
        writer.Finish().Get().ValueOrThrow());
}

TEST(TAsyncYsonWriterTest, AsyncMap)
{
    TAsyncYsonWriter writer(EYsonType::Node);
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnRaw(MakeFuture(ConvertToYsonString(1)));
        writer.OnKeyedItem("b");
        writer.OnRaw(MakeFuture(ConvertToYsonString(2)));
        writer.OnKeyedItem("c");
        writer.OnRaw(MakeFuture(ConvertToYsonString(3)));
    writer.OnEndMap();

    EXPECT_EQ(
        ConvertToYsonString(THashMap<TString, int>{{"a", 1}, {"b", 2}, {"c", 3}}),
        writer.Finish().Get().ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
