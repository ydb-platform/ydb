#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/dense_union.h>
#include <yql/essentials/public/udf/arrow/defs.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/string/cast.h>

#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>

#include <algorithm>

using namespace NYql::NUdf;

namespace {

template <typename T>
std::shared_ptr<arrow::Buffer> MakeBuffer(const TVector<T>& values) {
    auto pool = arrow::default_memory_pool();
    auto buf = ARROW_RESULT(arrow::AllocateBuffer(static_cast<i64>(values.size() * sizeof(T)), pool));
    std::copy(values.begin(), values.end(), reinterpret_cast<T*>(buf->mutable_data()));
    return buf;
}

std::shared_ptr<arrow::ArrayData> MakeDenseUnionDataWithChildLengths(
    TArrayRef<const i8> typeCodes,
    TArrayRef<const i32> valueOffsets,
    TArrayRef<const i64> childLengths)
{
    YQL_ENSURE(typeCodes.size() == valueOffsets.size());

    const size_t numChildren = childLengths.size();
    TVector<std::shared_ptr<arrow::Field>> fields;
    TVector<i8> typeIds;
    for (size_t i = 0; i < numChildren; ++i) {
        fields.push_back(arrow::field("f" + ToString(i), arrow::int32()));
        typeIds.push_back(static_cast<i8>(i));
    }
    auto unionType = arrow::dense_union(fields, TVector<i8>(typeIds.begin(), typeIds.end()));

    TVector<std::shared_ptr<arrow::ArrayData>> children(numChildren);
    for (size_t i = 0; i < numChildren; ++i) {
        children[i] = arrow::ArrayData::Make(arrow::int32(), childLengths[i], {nullptr, MakeBuffer(TVector<i32>(childLengths[i]))});
    }

    auto data = arrow::ArrayData::Make(
        unionType,
        static_cast<i64>(typeCodes.size()),
        {nullptr, MakeBuffer(TVector<i8>(typeCodes.begin(), typeCodes.end())), MakeBuffer(TVector<i32>(valueOffsets.begin(), valueOffsets.end()))});
    data->child_data = children;
    return data;
}

std::shared_ptr<arrow::ArrayData> MakeDenseUnionData(
    TArrayRef<const i8> typeCodes,
    TArrayRef<const i32> valueOffsets,
    size_t numChildren)
{
    TVector<i64> childLengths(numChildren, 0);
    for (size_t i = 0; i < typeCodes.size(); ++i) {
        const size_t typeCode = static_cast<size_t>(static_cast<ui8>(typeCodes[i]));
        childLengths[typeCode] = std::max(childLengths[typeCode], static_cast<i64>(valueOffsets[i]) + 1);
    }
    return MakeDenseUnionDataWithChildLengths(typeCodes, valueOffsets, childLengths);
}

void AssertUsage(const TDenseUnionChildUsage& usage, ui64 expectedOffset, ui64 expectedLength) {
    UNIT_ASSERT_EQUAL(usage.Offset, expectedOffset);
    UNIT_ASSERT_EQUAL(usage.Length, expectedLength);
}

} // namespace

Y_UNIT_TEST_SUITE(CalculateDenseUnionChildrenUsageTest) {

Y_UNIT_TEST(EmptyArray) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({}, {}, 2));
    UNIT_ASSERT_EQUAL(usage.size(), 2U);
    AssertUsage(usage[0], 0, 0);
    AssertUsage(usage[1], 0, 0);
}

Y_UNIT_TEST(NoChildren) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({}, {}, 0));
    UNIT_ASSERT(usage.empty());
}

Y_UNIT_TEST(SingleElement) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0}, {5}, 1));
    AssertUsage(usage[0], 5, 1);
}

Y_UNIT_TEST(SingleTypeCode) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0, 0, 0, 0}, {3, 5, 7, 10}, 2));
    AssertUsage(usage[0], 3, 8);
    AssertUsage(usage[1], 0, 0);
}

Y_UNIT_TEST(TwoTypeCodesInterleaved) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0, 1, 0, 1}, {0, 0, 1, 1}, 2));
    AssertUsage(usage[0], 0, 2);
    AssertUsage(usage[1], 0, 2);
}

Y_UNIT_TEST(TwoTypeCodesGrouped) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0, 0, 1, 1, 1}, {10, 20, 5, 8, 12}, 2));
    AssertUsage(usage[0], 10, 11);
    AssertUsage(usage[1], 5, 8);
}

Y_UNIT_TEST(OneTypeMissing) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0, 2, 0, 2}, {1, 0, 3, 2}, 3));
    AssertUsage(usage[0], 1, 3);
    AssertUsage(usage[1], 0, 0);
    AssertUsage(usage[2], 0, 3);
}

Y_UNIT_TEST(SingleElementEachType) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0, 1, 2}, {7, 3, 9}, 3));
    AssertUsage(usage[0], 7, 1);
    AssertUsage(usage[1], 3, 1);
    AssertUsage(usage[2], 9, 1);
}

Y_UNIT_TEST(OffsetStartsAtZero) {
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0, 0, 0}, {0, 1, 4}, 1));
    AssertUsage(usage[0], 0, 5);
}

Y_UNIT_TEST(LargeOffsets) {
    constexpr i32 bigOffset = 1 << 20;
    const auto usage = CalculateDenseUnionChildrenUsage(*MakeDenseUnionData({0, 0}, {bigOffset, bigOffset + 100}, 1));
    AssertUsage(usage[0], static_cast<ui64>(bigOffset), 101);
}

Y_UNIT_TEST(GhostChild) {
    // child 1 has length > 0 but never appears as a type code
    const auto data = MakeDenseUnionDataWithChildLengths({0, 0, 0}, {0, 1, 2}, {3, 5});
    const auto usage = CalculateDenseUnionChildrenUsage(*data);
    UNIT_ASSERT_EQUAL(usage.size(), 2U);
    AssertUsage(usage[0], 0, 3);
    AssertUsage(usage[1], 0, 0);
}

} // Y_UNIT_TEST_SUITE(CalculateDenseUnionChildrenUsageTest)
