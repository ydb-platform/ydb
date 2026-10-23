#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/compare.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <library/cpp/testing/unittest/registar.h>

#include <compare>
#include <initializer_list>
#include <limits>
#include <string>
#include <vector>

namespace NKikimr::NArrow {
namespace {

template <class TBuilder>
std::shared_ptr<arrow::Array> Finish(TBuilder& builder) {
    std::shared_ptr<arrow::Array> result;
    UNIT_ASSERT(builder.Finish(&result).ok());
    return result;
}

template <class TBuilder, class TValue>
std::shared_ptr<arrow::Array> MakeArray(std::initializer_list<TValue> values) {
    TBuilder builder;
    for (const auto& value : values) {
        UNIT_ASSERT(builder.Append(value).ok());
    }
    return Finish(builder);
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(const std::shared_ptr<arrow::Array>& values) {
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("key", values->type())}), values->length(), {values});
}

void CheckPair(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 lhs, const ui32 rhs,
    const std::partial_ordering expected) {
    const auto& values = batch->column(0);
    const TString context = TStringBuilder() << values->type()->ToString() << ": " << lhs << " vs " << rhs;
    UNIT_ASSERT_C(TComparator::TypedCompare<false>(*values, lhs, *values, rhs) == expected, context);
    UNIT_ASSERT_C((TSimpleRow(batch, lhs) <=> TSimpleRow(batch, rhs)) == expected, context);
    const NMerger::TSortableScanData positions(0, batch);
    UNIT_ASSERT_C(positions.Compare(lhs, positions, rhs) == expected, context);
    if (!values->IsNull(lhs) && !values->IsNull(rhs)) {
        UNIT_ASSERT_C(TComparator::TypedCompare<true>(*values, lhs, *values, rhs) == expected, context);
        const int scalarOrder = ScalarCompare(GetScalar(values, lhs), GetScalar(values, rhs));
        UNIT_ASSERT_C((scalarOrder <=> 0) == expected, context);
    }
}

void CheckOrderedValues(const std::shared_ptr<arrow::Array>& values) {
    const auto batch = MakeBatch(values);
    const ui32 count = values->length();
    for (ui32 lhs = 0; lhs < count; ++lhs) {
        for (ui32 rhs = 0; rhs < count; ++rhs) {
            CheckPair(batch, lhs, rhs, lhs <=> rhs);
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(ArrowComparison) {
    Y_UNIT_TEST(PrimitiveValues) {
        CheckOrderedValues(MakeArray<arrow::Int64Builder, i64>({std::numeric_limits<i64>::min(), -1, 0, std::numeric_limits<i64>::max()}));
        CheckOrderedValues(MakeArray<arrow::UInt64Builder, ui64>({0, 1, std::numeric_limits<ui64>::max()}));
        CheckOrderedValues(MakeArray<arrow::UInt8Builder, ui8>({0, 1, 127, 128, 255}));
        CheckOrderedValues(MakeArray<arrow::DoubleBuilder, double>({-std::numeric_limits<double>::infinity(), -1.0, 0.0, 1.0,
            std::numeric_limits<double>::infinity()}));
    }

    Y_UNIT_TEST(BinaryPrefixesAndHighBytes) {
        const std::vector<std::string> values = {"", std::string("\0", 1), "a", std::string("a\0", 2), "aa", "\x7f", "\x80", "\xff"};
        arrow::BinaryBuilder builder;
        for (const auto& value : values) {
            UNIT_ASSERT(builder.Append(value).ok());
        }
        CheckOrderedValues(Finish(builder));
        CheckOrderedValues(MakeArray<arrow::StringBuilder, std::string>({"", "a", std::string("a\0", 2), "aa", "é", "я", "😀"}));
    }

    Y_UNIT_TEST(EmptyScalarBuffer) {
        const arrow::BinaryScalar nullData(std::make_shared<arrow::Buffer>(nullptr, 0));
        const arrow::BinaryScalar empty(arrow::Buffer::FromString(""));
        const arrow::BinaryScalar nonempty(arrow::Buffer::FromString("a"));
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nullData, nullData), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nullData, empty), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(empty, nullData), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nullData, nonempty), -1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nonempty, nullData), 1);
    }

    Y_UNIT_TEST(FixedSizeBinaryKeepsPhysicalOrder) {
        std::string zero(16, '\0');
        std::string one = zero;
        one[0] = 1;
        std::string twoHundredFiftySix = zero;
        twoHundredFiftySix[1] = 1;
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        for (const auto& value : {zero, twoHundredFiftySix, one, std::string(16, '\xff')}) {
            UNIT_ASSERT(builder.Append(value).ok());
        }
        CheckOrderedValues(Finish(builder));
    }

    Y_UNIT_TEST(NullValues) {
        arrow::Int64Builder builder;
        UNIT_ASSERT(builder.AppendNull().ok());
        UNIT_ASSERT(builder.Append(-1).ok());
        UNIT_ASSERT(builder.AppendNull().ok());
        const auto batch = MakeBatch(Finish(builder));
        CheckPair(batch, 0, 2, std::partial_ordering::equivalent);
        CheckPair(batch, 0, 1, std::partial_ordering::less);
        CheckPair(batch, 1, 2, std::partial_ordering::greater);

        const std::shared_ptr<arrow::Scalar> missing;
        const auto value = std::make_shared<arrow::Int64Scalar>(-1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(missing, missing), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(missing, value), -1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(value, missing), 1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(value, value), 0);

        const auto invalid = std::make_shared<arrow::Int64Scalar>(2);
        invalid->is_valid = false;
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(invalid, value), 1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(value, invalid), -1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(invalid, invalid), 0);
    }

    Y_UNIT_TEST(NativeBooleanDispatchIsUnchanged) {
        const auto values = MakeArray<arrow::BooleanBuilder, bool>({false, true});
        const auto batch = MakeBatch(values);
        UNIT_ASSERT((TSimpleRow(batch, 0) <=> TSimpleRow(batch, 1)) == std::partial_ordering::less);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(GetScalar(values, 0), GetScalar(values, 1)), -1);
        UNIT_ASSERT(TComparator::TypedCompare<true>(*values, 0, *values, 1) == std::partial_ordering::equivalent);
    }

    Y_UNIT_TEST(NaNAndSignedZeroContracts) {
        const auto values = MakeArray<arrow::DoubleBuilder, double>({std::numeric_limits<double>::quiet_NaN(), 1.0,
            std::numeric_limits<double>::quiet_NaN(), -0.0, 0.0});
        const auto batch = MakeBatch(values);
        for (const ui32 rhs : {0u, 1u, 2u}) {
            UNIT_ASSERT(TComparator::TypedCompare<true>(*values, 0, *values, rhs) == std::partial_ordering::unordered);
            UNIT_ASSERT(TComparator::TypedCompare<true>(*values, rhs, *values, 0) == std::partial_ordering::unordered);
            UNIT_ASSERT((TSimpleRow(batch, 0) <=> TSimpleRow(batch, rhs)) == std::partial_ordering::equivalent);
            UNIT_ASSERT((TSimpleRow(batch, rhs) <=> TSimpleRow(batch, 0)) == std::partial_ordering::equivalent);
            UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(GetScalar(values, 0), GetScalar(values, rhs)), 0);
            UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(GetScalar(values, rhs), GetScalar(values, 0)), 0);
        }
        CheckPair(batch, 3, 4, std::partial_ordering::equivalent);
        CheckPair(batch, 4, 3, std::partial_ordering::equivalent);
    }

    Y_UNIT_TEST(CompositeKeysAndPrefixes) {
        const auto first = MakeArray<arrow::StringBuilder, std::string>({"a", "a", "b"});
        const auto second = MakeArray<arrow::Int32Builder, i32>({2, 1, 0});
        const auto schema = arrow::schema({arrow::field("first", first->type()), arrow::field("second", second->type())});
        const auto batch = arrow::RecordBatch::Make(schema, 3, {first, second});
        const TSimpleRow lhs(batch, 0);
        const TSimpleRow rhs(batch, 1);
        UNIT_ASSERT((lhs <=> rhs) == std::partial_ordering::greater);
        UNIT_ASSERT(lhs.ComparePartNotNull(rhs, 1) == std::partial_ordering::equivalent);
        UNIT_ASSERT(lhs.ComparePartNotNull(TSimpleRow(batch, 2), 1) == std::partial_ordering::less);
        const NMerger::TSortableScanData positions(0, batch);
        UNIT_ASSERT(positions.Compare(0, positions, 1) == std::partial_ordering::greater);
        UNIT_ASSERT(positions.ComparePrefix(0, positions, 1, 1) == std::partial_ordering::equivalent);

        const auto floats = MakeArray<arrow::DoubleBuilder, double>({std::numeric_limits<double>::quiet_NaN(), 1.0, 0.0});
        const auto floatSchema = arrow::schema({arrow::field("first", floats->type()), schema->field(1)});
        const auto floatBatch = arrow::RecordBatch::Make(floatSchema, 3, {floats, second});
        UNIT_ASSERT((TSimpleRow(floatBatch, 0) <=> TSimpleRow(floatBatch, 1)) == std::partial_ordering::greater);
        const NMerger::TSortableScanData floatPositions(0, floatBatch);
        UNIT_ASSERT(floatPositions.Compare(0, floatPositions, 1) == std::partial_ordering::unordered);
    }
}

} // namespace NKikimr::NArrow
