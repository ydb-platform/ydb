#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/formats/arrow/special_keys.h>

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

void CheckPair(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 lhs, const ui32 rhs, const std::partial_ordering expected,
    const NScheme::TTypeInfo& logicalType) {
    const std::vector<NScheme::TTypeInfo> logicalTypes = { logicalType };
    const auto& values = batch->column(0);
    const TString context = TStringBuilder() << values->type()->ToString() << ": " << lhs << " vs " << rhs;
    UNIT_ASSERT_C(TComparator::TypedCompare<false>(*values, lhs, *values, rhs, logicalType) == expected, context);
    UNIT_ASSERT_C((TSimpleRow(batch, lhs, logicalTypes) <=> TSimpleRow(batch, rhs, logicalTypes)) == expected, context);
    const NMerger::TSortableScanData positions(0, batch, logicalTypes);
    UNIT_ASSERT_C(positions.Compare(lhs, positions, rhs) == expected, context);
    if (!values->IsNull(lhs) && !values->IsNull(rhs)) {
        UNIT_ASSERT_C(TComparator::TypedCompare<true>(*values, lhs, *values, rhs, logicalType) == expected, context);
        const int scalarOrder = ScalarCompare(GetScalar(values, lhs), GetScalar(values, rhs), logicalType);
        UNIT_ASSERT_C((scalarOrder <=> 0) == expected, context);
    }
}

void CheckOrderedValues(const std::shared_ptr<arrow::Array>& values, const NScheme::TTypeInfo& logicalType) {
    const auto batch = MakeBatch(values);
    const ui32 count = values->length();
    for (ui32 lhs = 0; lhs < count; ++lhs) {
        for (ui32 rhs = 0; rhs < count; ++rhs) {
            CheckPair(batch, lhs, rhs, lhs <=> rhs, logicalType);
        }
    }
}

} // namespace

static_assert(!std::is_constructible_v<TSimpleRow, const std::shared_ptr<arrow::RecordBatch>&, ui32>);
static_assert(!std::is_constructible_v<TSimpleRowRef, TSimpleRowViewV0, const arrow::Schema&>);
static_assert(!std::is_constructible_v<TSimpleRowRef, TSimpleRowViewV0, const arrow::Schema&, std::vector<NScheme::TTypeInfo>&&>);

Y_UNIT_TEST_SUITE(ArrowComparison) {
    Y_UNIT_TEST(PackedRowsPreserveLogicalTypes) {
        const NScheme::TTypeInfo decimalType(NScheme::TDecimalType(22, 9));
        const NScheme::TTypeInfo uuidType(NScheme::NTypeIds::Uuid);
        std::vector<NScheme::TTypeInfo> logicalTypes = { decimalType, uuidType };
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        UNIT_ASSERT(builder.Append(std::string(16, '\0')).ok());
        const auto values = Finish(builder);
        const auto schema = arrow::schema({ arrow::field("decimal", values->type()), arrow::field("uuid", values->type()) });
        const auto batch = arrow::RecordBatch::Make(schema, 1, { values, values });
        const TSimpleRow original(batch, 0, logicalTypes);
        logicalTypes.clear();
        UNIT_ASSERT(original.GetLogicalTypes() == std::vector<NScheme::TTypeInfo>({ decimalType, uuidType }));
        const auto restored = original.GetContent().Build(schema, original.GetLogicalTypes());
        UNIT_ASSERT(restored.GetLogicalTypes() == original.GetLogicalTypes());
        UNIT_ASSERT(restored == original);
        const auto position = restored.BuildSortablePosition();
        UNIT_ASSERT(position.GetSorting()->GetLogicalTypes() == original.GetLogicalTypes());
        const TSimpleRowRef originalRef(TSimpleRowViewV0(original.GetData()), *schema, original.GetLogicalTypes());
        const TSimpleRowRef restoredRef(TSimpleRowViewV0(restored.GetData()), *schema, restored.GetLogicalTypes());
        UNIT_ASSERT(originalRef == restoredRef);
        const TFirstLastSpecialKeys borders(batch);
        UNIT_ASSERT(borders.GetFirst(original.GetLogicalTypes()) == original);
        UNIT_ASSERT(borders.GetLast(original.GetLogicalTypes()) == original);
    }

    Y_UNIT_TEST(SortAndDeduplicatePreserveLegacyDecimalOrder) {
        const std::vector<NScheme::TTypeInfo> logicalTypes = { NScheme::TTypeInfo(NScheme::TDecimalType(22, 9)),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uuid) };
        std::string zero(16, '\0');
        std::string one = zero;
        one[0] = 1;
        std::string twoHundredFiftySix = zero;
        twoHundredFiftySix[1] = 1;
        arrow::FixedSizeBinaryBuilder decimalBuilder(arrow::fixed_size_binary(16));
        arrow::FixedSizeBinaryBuilder uuidBuilder(arrow::fixed_size_binary(16));
        for (const auto& value : { one, twoHundredFiftySix, zero, twoHundredFiftySix }) {
            UNIT_ASSERT(decimalBuilder.Append(value).ok());
            UNIT_ASSERT(uuidBuilder.Append(zero).ok());
        }
        const auto decimals = Finish(decimalBuilder);
        const auto uuids = Finish(uuidBuilder);
        const auto ids = MakeArray<arrow::UInt32Builder, ui32>({ 0, 1, 2, 3 });
        const auto keySchema = arrow::schema({ arrow::field("decimal", decimals->type()), arrow::field("uuid", uuids->type()) });
        const auto schema = arrow::schema({ keySchema->field(0), keySchema->field(1), arrow::field("id", ids->type()) });
        const auto batch = arrow::RecordBatch::Make(schema, 4, { decimals, uuids, ids });
        const auto sorted = SortBatch(batch, keySchema, logicalTypes, true);
        UNIT_ASSERT(IsSortedAndUnique(sorted, keySchema, logicalTypes));
        UNIT_ASSERT_VALUES_EQUAL(sorted->num_rows(), 3);
        const auto& sortedIds = static_cast<const arrow::UInt32Array&>(*sorted->column(2));
        UNIT_ASSERT_VALUES_EQUAL(sortedIds.Value(0), 2);
        UNIT_ASSERT_VALUES_EQUAL(sortedIds.Value(1), 3);
        UNIT_ASSERT_VALUES_EQUAL(sortedIds.Value(2), 0);
    }

    Y_UNIT_TEST(PrimitiveValues) {
        CheckOrderedValues(MakeArray<arrow::Int64Builder, i64>({ std::numeric_limits<i64>::min(), -1, 0, std::numeric_limits<i64>::max() }),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int64));
        CheckOrderedValues(
            MakeArray<arrow::UInt64Builder, ui64>({ 0, 1, std::numeric_limits<ui64>::max() }), NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        CheckOrderedValues(MakeArray<arrow::UInt8Builder, ui8>({ 0, 1, 127, 128, 255 }), NScheme::TTypeInfo(NScheme::NTypeIds::Uint8));
        CheckOrderedValues(MakeArray<arrow::DoubleBuilder, double>(
                               { -std::numeric_limits<double>::infinity(), -1.0, 0.0, 1.0, std::numeric_limits<double>::infinity() }),
            NScheme::TTypeInfo(NScheme::NTypeIds::Double));
    }

    Y_UNIT_TEST(BinaryPrefixesAndHighBytes) {
        const std::vector<std::string> values = {"", std::string("\0", 1), "a", std::string("a\0", 2), "aa", "\x7f", "\x80", "\xff"};
        arrow::BinaryBuilder builder;
        for (const auto& value : values) {
            UNIT_ASSERT(builder.Append(value).ok());
        }
        CheckOrderedValues(Finish(builder), NScheme::TTypeInfo(NScheme::NTypeIds::String));
        CheckOrderedValues(MakeArray<arrow::StringBuilder, std::string>({ "", "a", std::string("a\0", 2), "aa", "é", "я", "😀" }),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8));
    }

    Y_UNIT_TEST(EmptyScalarBuffer) {
        const NScheme::TTypeInfo logicalType(NScheme::NTypeIds::String);
        const arrow::BinaryScalar nullData(std::make_shared<arrow::Buffer>(nullptr, 0));
        const arrow::BinaryScalar empty(arrow::Buffer::FromString(""));
        const arrow::BinaryScalar nonempty(arrow::Buffer::FromString("a"));
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nullData, nullData, logicalType), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nullData, empty, logicalType), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(empty, nullData, logicalType), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nullData, nonempty, logicalType), -1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(nonempty, nullData, logicalType), 1);
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
        const auto values = Finish(builder);
        CheckOrderedValues(values, NScheme::TTypeInfo(NScheme::NTypeIds::Uuid));
        CheckOrderedValues(values, NScheme::TTypeInfo(NScheme::TDecimalType(22, 9)));
    }

    Y_UNIT_TEST(NullValues) {
        const NScheme::TTypeInfo logicalType(NScheme::NTypeIds::Int64);
        arrow::Int64Builder builder;
        UNIT_ASSERT(builder.AppendNull().ok());
        UNIT_ASSERT(builder.Append(-1).ok());
        UNIT_ASSERT(builder.AppendNull().ok());
        const auto batch = MakeBatch(Finish(builder));
        CheckPair(batch, 0, 2, std::partial_ordering::equivalent, logicalType);
        CheckPair(batch, 0, 1, std::partial_ordering::less, logicalType);
        CheckPair(batch, 1, 2, std::partial_ordering::greater, logicalType);

        const std::shared_ptr<arrow::Scalar> missing;
        const auto value = std::make_shared<arrow::Int64Scalar>(-1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(missing, missing, logicalType), 0);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(missing, value, logicalType), -1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(value, missing, logicalType), 1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(value, value, logicalType), 0);

        const auto invalid = std::make_shared<arrow::Int64Scalar>(2);
        invalid->is_valid = false;
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(invalid, value, logicalType), 1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(value, invalid, logicalType), -1);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(invalid, invalid, logicalType), 0);
    }

    Y_UNIT_TEST(NativeBooleanDispatchIsUnchanged) {
        const NScheme::TTypeInfo logicalType(NScheme::NTypeIds::Bool);
        const std::vector<NScheme::TTypeInfo> logicalTypes = { logicalType };
        const auto values = MakeArray<arrow::BooleanBuilder, bool>({false, true});
        const auto batch = MakeBatch(values);
        UNIT_ASSERT((TSimpleRow(batch, 0, logicalTypes) <=> TSimpleRow(batch, 1, logicalTypes)) == std::partial_ordering::less);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(GetScalar(values, 0), GetScalar(values, 1), logicalType), -1);
        UNIT_ASSERT(TComparator::TypedCompare<true>(*values, 0, *values, 1, logicalType) == std::partial_ordering::equivalent);
    }

    Y_UNIT_TEST(NaNAndSignedZeroContracts) {
        const NScheme::TTypeInfo logicalType(NScheme::NTypeIds::Double);
        const std::vector<NScheme::TTypeInfo> logicalTypes = { logicalType };
        const auto values = MakeArray<arrow::DoubleBuilder, double>({std::numeric_limits<double>::quiet_NaN(), 1.0,
            std::numeric_limits<double>::quiet_NaN(), -0.0, 0.0});
        const auto batch = MakeBatch(values);
        for (const ui32 rhs : {0u, 1u, 2u}) {
            UNIT_ASSERT(TComparator::TypedCompare<true>(*values, 0, *values, rhs, logicalType) == std::partial_ordering::unordered);
            UNIT_ASSERT(TComparator::TypedCompare<true>(*values, rhs, *values, 0, logicalType) == std::partial_ordering::unordered);
            UNIT_ASSERT((TSimpleRow(batch, 0, logicalTypes) <=> TSimpleRow(batch, rhs, logicalTypes)) == std::partial_ordering::equivalent);
            UNIT_ASSERT((TSimpleRow(batch, rhs, logicalTypes) <=> TSimpleRow(batch, 0, logicalTypes)) == std::partial_ordering::equivalent);
            UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(GetScalar(values, 0), GetScalar(values, rhs), logicalType), 0);
            UNIT_ASSERT_VALUES_EQUAL(ScalarCompare(GetScalar(values, rhs), GetScalar(values, 0), logicalType), 0);
        }
        CheckPair(batch, 3, 4, std::partial_ordering::equivalent, logicalType);
        CheckPair(batch, 4, 3, std::partial_ordering::equivalent, logicalType);
    }

    Y_UNIT_TEST(CompositeKeysAndPrefixes) {
        const std::vector<NScheme::TTypeInfo> logicalTypes = { NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int32) };
        const std::vector<NScheme::TTypeInfo> floatTypes = { NScheme::TTypeInfo(NScheme::NTypeIds::Double), logicalTypes[1] };
        const auto first = MakeArray<arrow::StringBuilder, std::string>({"a", "a", "b"});
        const auto second = MakeArray<arrow::Int32Builder, i32>({2, 1, 0});
        const auto schema = arrow::schema({arrow::field("first", first->type()), arrow::field("second", second->type())});
        const auto batch = arrow::RecordBatch::Make(schema, 3, {first, second});
        const TSimpleRow lhs(batch, 0, logicalTypes);
        const TSimpleRow rhs(batch, 1, logicalTypes);
        UNIT_ASSERT((lhs <=> rhs) == std::partial_ordering::greater);
        UNIT_ASSERT(lhs.ComparePartNotNull(rhs, 1) == std::partial_ordering::equivalent);
        UNIT_ASSERT(lhs.ComparePartNotNull(TSimpleRow(batch, 2, logicalTypes), 1) == std::partial_ordering::less);
        const NMerger::TSortableScanData positions(0, batch, logicalTypes);
        UNIT_ASSERT(positions.Compare(0, positions, 1) == std::partial_ordering::greater);
        UNIT_ASSERT(positions.ComparePrefix(0, positions, 1, 1) == std::partial_ordering::equivalent);

        const auto floats = MakeArray<arrow::DoubleBuilder, double>({std::numeric_limits<double>::quiet_NaN(), 1.0, 0.0});
        const auto floatSchema = arrow::schema({arrow::field("first", floats->type()), schema->field(1)});
        const auto floatBatch = arrow::RecordBatch::Make(floatSchema, 3, {floats, second});
        UNIT_ASSERT((TSimpleRow(floatBatch, 0, floatTypes) <=> TSimpleRow(floatBatch, 1, floatTypes)) == std::partial_ordering::greater);
        const NMerger::TSortableScanData floatPositions(0, floatBatch, floatTypes);
        UNIT_ASSERT(floatPositions.Compare(0, floatPositions, 1) == std::partial_ordering::unordered);
    }
}

} // namespace NKikimr::NArrow
