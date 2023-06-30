#include "key_helpers.h"

#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/key_bound_compressor.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NTableClient {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyBoundCompressorTest, Simple)
{
    TRowBufferPtr rowBuffer = New<TRowBuffer>();
    auto preimageRow = [&] (std::optional<TStringBuf> first = std::nullopt, std::optional<int> second = std::nullopt) {
        TUnversionedRowBuilder builder;
        if (first) {
            builder.AddValue(MakeUnversionedStringValue(*first, 0));
        }
        if (second) {
            builder.AddValue(MakeUnversionedInt64Value(*second, 1));
        }
        return rowBuffer->CaptureRow(builder.GetRow());
    };

    auto imageRow = [&] (std::optional<int> first = std::nullopt, std::optional<int> second = std::nullopt) {
        TUnversionedRowBuilder builder;
        if (first) {
            builder.AddValue(MakeUnversionedInt64Value(*first, 0));
        }
        if (second) {
            builder.AddValue(MakeUnversionedInt64Value(*second, 1));
        }
        return rowBuffer->CaptureRow(builder.GetRow());
    };

    // See comment in key_bound_compressor.h for the readable version of the list below.
    std::vector<std::tuple<TKeyBound, TKeyBound, int>> tuples{
        {TKeyBound::FromRow() >= preimageRow(         ), TKeyBound::FromRow() >= imageRow(    ), 0},
        {TKeyBound::FromRow() >= preimageRow("bar"    ), TKeyBound::FromRow() >= imageRow(0   ), 1},
        {TKeyBound::FromRow() >= preimageRow("bar", 42), TKeyBound::FromRow() >= imageRow(0, 0), 2},
        {TKeyBound::FromRow() <= preimageRow("bar", 57), TKeyBound::FromRow() <= imageRow(0, 1), 3},
        {TKeyBound::FromRow() <= preimageRow("bar"    ), TKeyBound::FromRow() <= imageRow(0   ), 4},
        {TKeyBound::FromRow() <  preimageRow("foo"    ), TKeyBound::FromRow() <  imageRow(1   ), 5},
        {TKeyBound::FromRow() >= preimageRow("foo"    ), TKeyBound::FromRow() >= imageRow(1   ), 5},
        {TKeyBound::FromRow() >  preimageRow("foo", 30), TKeyBound::FromRow() >  imageRow(1, 0), 6},
        {TKeyBound::FromRow() <  preimageRow("foo", 99), TKeyBound::FromRow() <  imageRow(1, 1), 7},
        {TKeyBound::FromRow() <= preimageRow("foo", 99), TKeyBound::FromRow() <= imageRow(1, 1), 7},
        {TKeyBound::FromRow() >  preimageRow("foo", 99), TKeyBound::FromRow() >  imageRow(1, 1), 7},
        {TKeyBound::FromRow() <  preimageRow("qux", 18), TKeyBound::FromRow() <  imageRow(2, 0), 8},
        {TKeyBound::FromRow() <  preimageRow("zzz"    ), TKeyBound::FromRow() <  imageRow(3   ), 9},
        {TKeyBound::FromRow() <= preimageRow(         ), TKeyBound::FromRow() <= imageRow(    ), 10},
        {TKeyBound::FromRow() >  preimageRow(         ), TKeyBound::FromRow() >  imageRow(    ), 10},
    };

    TComparator comparator(std::vector<ESortOrder>{ESortOrder::Ascending, ESortOrder::Ascending});
    TKeyBoundCompressor compressor(comparator);

    for (const auto& [key, _1, _2] : tuples) {
        compressor.Add(key);
    }

    compressor.InitializeMapping();

    for (const auto& [key, componentWise, total] : tuples) {
        auto [actualComponentWise, actualTotal] = compressor.GetImage(key);
        TKeyBound qwe = actualComponentWise;
        (void)qwe;
        EXPECT_EQ(componentWise, actualComponentWise)
            << "For key " << ToString(key);
        EXPECT_EQ(total, actualTotal)
            << "For key " << ToString(key);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
