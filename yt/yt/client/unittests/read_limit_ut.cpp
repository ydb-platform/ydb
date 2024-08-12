#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NChunkClient {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedOwningRow MakeRow(std::vector<int> values)
{
    TUnversionedOwningRowBuilder builder;
    for (auto value : values) {
        builder.AddValue(MakeUnversionedInt64Value(value));
    }

    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString DumpToYson(T obj)
{
    return ConvertToYsonString(obj, NYson::EYsonFormat::Text).ToString();
}

TEST(TLegacyReadLimitTest, Simple)
{
    TLegacyReadLimit limit;
    EXPECT_EQ("{}", DumpToYson(limit));
    EXPECT_TRUE(limit.IsTrivial());

    limit.SetRowIndex(0);
    EXPECT_EQ("{\"row_index\"=0;}", DumpToYson(limit));
    EXPECT_FALSE(limit.IsTrivial());
}

TEST(TLegacyReadRangeTest, Simple)
{
    TLegacyReadRange range;
    EXPECT_EQ("{}", DumpToYson(range));
}

TEST(TLegacyReadRangeTest, Simple2)
{
    auto range = ConvertTo<TLegacyReadRange>(TYsonString(TStringBuf("{lower_limit={row_index=1};upper_limit={row_index=2}}")));
    EXPECT_EQ(1, range.LowerLimit().GetRowIndex());
    EXPECT_EQ(2, range.UpperLimit().GetRowIndex());
}

TEST(TReadLimitTest, ProtobufConversion)
{
    TReadLimit readLimit;
    auto keyBound = TOwningKeyBound::FromRow(/* prefix */ MakeRow({1, 42}), /* isUpper */ true, /* isInclusive */ true);
    readLimit.KeyBound() = keyBound;
    readLimit.SetRowIndex(1);
    readLimit.SetOffset(2);
    readLimit.SetChunkIndex(3);
    readLimit.SetTabletIndex(4);

    NProto::TReadLimit protoReadLimit;
    ToProto(&protoReadLimit, readLimit);

    TReadLimit newReadLimit(protoReadLimit, /* isUpper */ true);

    EXPECT_EQ(newReadLimit.KeyBound(), keyBound);
    EXPECT_EQ(newReadLimit.GetRowIndex(), 1);
    EXPECT_EQ(newReadLimit.GetOffset(), 2);
    EXPECT_EQ(newReadLimit.GetChunkIndex(), 3);
    EXPECT_EQ(newReadLimit.GetTabletIndex(), 4);
}

TEST(TReadLimitTest, LegacyKey)
{
    TReadLimit readLimit;
    auto keyBound = TOwningKeyBound::FromRow(/* prefix */ MakeRow({1, 42}), /* isInclusive */ true, /* isUpper */ true);
    readLimit.KeyBound() = keyBound;
    readLimit.SetRowIndex(1);
    readLimit.SetOffset(2);
    readLimit.SetChunkIndex(3);
    readLimit.SetTabletIndex(4);
    EXPECT_FALSE(readLimit.IsTrivial());

    NProto::TReadLimit protoReadLimit;
    ToProto(&protoReadLimit, readLimit);
    protoReadLimit.clear_key_bound_prefix();
    protoReadLimit.clear_key_bound_is_inclusive();

    TReadLimit newReadLimit(protoReadLimit, /* isUpper */ true, /* keyLength */ 2);

    EXPECT_EQ(newReadLimit.KeyBound(), keyBound);
    EXPECT_EQ(newReadLimit.GetRowIndex(), 1);
    EXPECT_EQ(newReadLimit.GetOffset(), 2);
    EXPECT_EQ(newReadLimit.GetChunkIndex(), 3);
    EXPECT_EQ(newReadLimit.GetTabletIndex(), 4);
    EXPECT_FALSE(newReadLimit.IsTrivial());
}

TEST(TReadLimitTest, Interop)
{
    TReadLimit readLimitA;
    readLimitA.SetRowIndex(1);
    readLimitA.SetOffset(2);
    readLimitA.SetChunkIndex(3);
    readLimitA.SetTabletIndex(4);

    TLegacyReadLimit legacyReadLimitA;
    legacyReadLimitA.SetRowIndex(1);
    legacyReadLimitA.SetOffset(2);
    legacyReadLimitA.SetChunkIndex(3);
    legacyReadLimitA.SetTabletIndex(4);

    // I am too lazy to write a proper comparison operator,
    // so let us just compare string read limit representations.

    EXPECT_EQ(ToString(legacyReadLimitA), ToString(ReadLimitToLegacyReadLimit(readLimitA)));
    EXPECT_EQ(ToString(readLimitA), ToString(ReadLimitFromLegacyReadLimitKeyless(legacyReadLimitA)));
    EXPECT_EQ(ToString(readLimitA), ToString(ReadLimitFromLegacyReadLimit(legacyReadLimitA, /* isUpper */ true, /* keyLength */ 1)));

    TReadLimit readLimitB;
    readLimitB.KeyBound() = TOwningKeyBound::FromRow() > MakeRow({42});

    TLegacyReadLimit legacyReadLimitB;
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(42));
    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
    legacyReadLimitB.SetLegacyKey(builder.FinishRow());

    EXPECT_EQ(ToString(legacyReadLimitB), ToString(ReadLimitToLegacyReadLimit(readLimitB)));
    EXPECT_EQ(ToString(readLimitB), ToString(ReadLimitFromLegacyReadLimit(legacyReadLimitB, /* isUpper */ false, /* keyLength */ 1)));
    // Crashes: ReadLimitFromLegacyReadLimitKeyless(legacyReadLimitB).
}

TEST(TReadLimitTest, Trivial)
{
    TReadLimit readLimit;
    EXPECT_TRUE(readLimit.IsTrivial());

    NProto::TReadLimit protoReadLimit;
    ToProto(&protoReadLimit, readLimit);

    TReadLimit newReadLimit(protoReadLimit, /* isUpper */ true);
    EXPECT_TRUE(newReadLimit.IsTrivial());
}

TEST(TReadRangeTest, ProtobufConversion)
{
    TReadRange readRange;
    auto lowerKeyBound = TOwningKeyBound::FromRow(/* prefix */ MakeRow({1, 42}), /* isInclusive */ true,  /* isUpper */ false);
    readRange.LowerLimit().KeyBound() = lowerKeyBound;
    readRange.LowerLimit().SetRowIndex(1);
    auto upperKeyBound = TOwningKeyBound::FromRow(/* prefix */ MakeRow({12, 13}), /* isInclusive */ true, /* isUpper */ true);
    readRange.UpperLimit().KeyBound() = upperKeyBound;
    readRange.UpperLimit().SetRowIndex(2);

    NProto::TReadRange protoReadRange;
    ToProto(&protoReadRange, readRange);

    TReadRange newReadRange(protoReadRange);

    EXPECT_EQ(newReadRange.LowerLimit().KeyBound(), lowerKeyBound);
    EXPECT_EQ(newReadRange.LowerLimit().GetRowIndex(), 1);
    EXPECT_EQ(newReadRange.UpperLimit().KeyBound(), upperKeyBound);
    EXPECT_EQ(newReadRange.UpperLimit().GetRowIndex(), 2);
}

TEST(TReadRangeTest, Trvial)
{
    TReadRange readRange;
    EXPECT_TRUE(readRange.LowerLimit().IsTrivial());
    EXPECT_TRUE(readRange.UpperLimit().IsTrivial());

    NProto::TReadRange protoReadRange;
    ToProto(&protoReadRange, readRange);

    TReadRange newReadRange(protoReadRange);

    EXPECT_TRUE(newReadRange.LowerLimit().IsTrivial());
    EXPECT_TRUE(newReadRange.UpperLimit().IsTrivial());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient

