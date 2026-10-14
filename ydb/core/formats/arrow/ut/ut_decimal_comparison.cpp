#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/formats/arrow/key_comparator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NArrow {

Y_UNIT_TEST_SUITE(DecimalKeyComparison) {
    Y_UNIT_TEST(SortRowsAndBorders) {
        const auto type = arrow::fixed_size_binary(sizeof(NYql::NDecimal::TInt128));
        auto field = arrow::field("key", type)->WithMetadata(
            arrow::key_value_metadata({TKeyColumnComparator::DecimalComparisonMetadata}, {"1"}));
        const std::vector<NYql::NDecimal::TInt128> values = {
            NYql::NDecimal::Nan(), 256, -1, NYql::NDecimal::Inf(), 1, -NYql::NDecimal::Inf(), -256, 0};
        arrow::FixedSizeBinaryBuilder builder(type);
        for (const auto& value : values) {
            UNIT_ASSERT(builder.Append(reinterpret_cast<const char*>(&value)).ok());
        }
        UNIT_ASSERT(builder.AppendNull().ok());
        std::shared_ptr<arrow::Array> array;
        UNIT_ASSERT(builder.Finish(&array).ok());
        auto schema = arrow::schema({field});
        auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});
        auto sorted = SortBatch(batch, schema, false);
        UNIT_ASSERT(IsSortedAndUnique(sorted, schema));
        UNIT_ASSERT(sorted->column(0)->IsNull(0));
        const std::vector<NYql::NDecimal::TInt128> expected = {
            -NYql::NDecimal::Inf(), -256, -1, 0, 1, 256, NYql::NDecimal::Inf(), NYql::NDecimal::Nan()};
        const auto& result = static_cast<const arrow::FixedSizeBinaryArray&>(*sorted->column(0));
        for (ui32 i = 0; i < expected.size(); ++i) {
            NYql::NDecimal::TInt128 value;
            memcpy(&value, result.GetValue(i + 1), sizeof(value));
            UNIT_ASSERT(value == expected[i]);
        }
        auto sliced = sorted->Slice(1);
        NMerger::TSortableScanData scan(0, sliced);
        for (ui32 i = 1; i < expected.size(); ++i) {
            UNIT_ASSERT(TSimpleRow(sliced, i - 1) < TSimpleRow(sliced, i));
            UNIT_ASSERT(scan.BuildCursor(i - 1).Compare(scan.BuildCursor(i)) == std::partial_ordering::less);
        }
        auto legacySchema = arrow::schema({arrow::field("key", type)});
        auto legacy = SortBatch(batch, legacySchema, false);
        UNIT_ASSERT(IsSortedAndUnique(legacy, legacySchema));
        UNIT_ASSERT(!IsSorted(legacy, schema));

        NAccessor::TAccessorsCollection collection;
        collection.AddVerified(1, std::make_shared<NAccessor::TTrivialArray>(sorted->column(0)), false);
        NSSA::TSchemaColumnResolver resolver(schema);
        auto restored = collection.ToGeneralContainer(&resolver);
        UNIT_ASSERT(TKeyColumnComparator::IsDecimal(*restored->GetSchema()->GetFieldByName("key")));
        NMerger::TSortableScanData restoredScan(0, restored);
        for (ui32 i = 1; i < (ui32)sorted->num_rows(); ++i) {
            UNIT_ASSERT(restoredScan.BuildCursor(i - 1).Compare(restoredScan.BuildCursor(i)) == std::partial_ordering::less);
        }
    }

    Y_UNIT_TEST(OtherFixedSizeBinaryKeepsByteOrder) {
        const NYql::NDecimal::TInt128 one = 1, nextByte = 256;
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(sizeof(NYql::NDecimal::TInt128)));
        UNIT_ASSERT(builder.Append(reinterpret_cast<const char*>(&one)).ok());
        UNIT_ASSERT(builder.Append(reinterpret_cast<const char*>(&nextByte)).ok());
        std::shared_ptr<arrow::Array> array;
        UNIT_ASSERT(builder.Finish(&array).ok());
        UNIT_ASSERT(TComparator::TypedCompare<true>(*array, 0, *array, 1) == std::partial_ordering::greater);
        const auto field = arrow::field("decimal", array->type())->WithMetadata(
            arrow::key_value_metadata({TKeyColumnComparator::DecimalComparisonMetadata}, {"1"}));
        UNIT_ASSERT(TKeyColumnComparator(*field).Compare(*array, 0, *array, 1) == std::partial_ordering::less);
    }

    Y_UNIT_TEST(StableSortPreservesVersions) {
        const auto type = arrow::fixed_size_binary(sizeof(NYql::NDecimal::TInt128));
        auto schema = arrow::schema({arrow::field("key", type)->WithMetadata(
            arrow::key_value_metadata({TKeyColumnComparator::DecimalComparisonMetadata}, {"1"}))});
        arrow::FixedSizeBinaryBuilder builder(type);
        for (const NYql::NDecimal::TInt128 value : {256, -1, 256, -1}) {
            UNIT_ASSERT(builder.Append(reinterpret_cast<const char*>(&value)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        UNIT_ASSERT(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 4, {array});
        auto permutation = MakeSortPermutation(batch, schema, false, true);
        UNIT_ASSERT(permutation);
        UNIT_ASSERT_VALUES_EQUAL(permutation->length(), 4);
        const std::vector<ui64> expected = {1, 3, 0, 2};
        for (ui32 i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(permutation->Value(i), expected[i]);
        }
    }
}

} // namespace NKikimr::NArrow
