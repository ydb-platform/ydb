#include <ydb/core/tx/columnshard/validation/not_null/validator.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_dict.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <library/cpp/testing/unittest/registar.h>

#include <optional>

namespace NKikimr::NOlap {
namespace {

std::shared_ptr<arrow::Array> Values(std::initializer_list<std::optional<int64_t>> values) {
    arrow::Int64Builder builder;
    for (const auto& value : values) {
        const auto status = value ? builder.Append(*value) : builder.AppendNull();
        UNIT_ASSERT_C(status.ok(), status.ToString());
    }
    std::shared_ptr<arrow::Array> result;
    const auto status = builder.Finish(&result);
    UNIT_ASSERT_C(status.ok(), status.ToString());
    return result;
}

std::shared_ptr<arrow::Table> Batch(const std::vector<std::shared_ptr<arrow::Array>>& chunks, const bool nullable = true) {
    return arrow::Table::Make(arrow::schema({ arrow::field("value", arrow::int64(), nullable) }),
        std::vector<std::shared_ptr<arrow::ChunkedArray>>{ std::make_shared<arrow::ChunkedArray>(chunks, arrow::int64()) });
}

}   // namespace

Y_UNIT_TEST_SUITE(TNotNullValidatorTests) {
    Y_UNIT_TEST(ValidatesOnlyAfterSuccessfulEndOfScan) {
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(!validator.IsValidated());
        UNIT_ASSERT(validator.AddBatch(*Batch({ Values({ 1, 2 }), Values({ 3 }) })).ok());
        UNIT_ASSERT(validator.AddBatch(*Batch({ Values({ 4, 5 }) })).ok());
        UNIT_ASSERT_VALUES_EQUAL(validator.GetCheckedRows(), 5);
        UNIT_ASSERT(!validator.IsValidated());
        UNIT_ASSERT(validator.Finish().ok());
        UNIT_ASSERT(validator.IsValidated());
        UNIT_ASSERT(validator.Finish().ok());
    }

    Y_UNIT_TEST(EmptyScanIsValidOnlyAfterEndOfScan) {
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(!validator.IsValidated());
        UNIT_ASSERT(validator.Finish().ok());
        UNIT_ASSERT(validator.IsValidated());
        UNIT_ASSERT_VALUES_EQUAL(validator.GetCheckedRows(), 0);
    }

    Y_UNIT_TEST(NullInLaterBatchPermanentlyFailsValidation) {
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(validator.AddBatch(*Batch({ Values({ 1, 2 }) })).ok());
        UNIT_ASSERT(!validator.AddBatch(*Batch({ Values({ 3 }), Values({ 4, std::nullopt }) })).ok());
        UNIT_ASSERT_STRING_CONTAINS(validator.GetErrorMessage(), "value");
        const auto error = validator.GetErrorMessage();
        UNIT_ASSERT(!validator.AddBatch(*Batch({ Values({ 5 }) })).ok());
        UNIT_ASSERT(!validator.Finish().ok());
        UNIT_ASSERT(validator.IsFailed());
        UNIT_ASSERT(!validator.IsValidated());
        UNIT_ASSERT_VALUES_EQUAL(validator.GetErrorMessage(), error);
        UNIT_ASSERT_VALUES_EQUAL(validator.GetCheckedRows(), 2);
    }

    Y_UNIT_TEST(ChecksOnlySelectedColumnsByName) {
        auto batch = arrow::Table::Make(arrow::schema({ arrow::field("other", arrow::int64()), arrow::field("value", arrow::int64()) }),
            std::vector<std::shared_ptr<arrow::Array>>{ Values({ std::nullopt, std::nullopt }), Values({ 1, 2 }) });
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(validator.AddBatch(*batch).ok());
        UNIT_ASSERT(validator.Finish().ok());
        TNotNullValidator both({ "value", "other" });
        UNIT_ASSERT(!both.AddBatch(*batch).ok());
        UNIT_ASSERT(!both.Finish().ok());
    }

    Y_UNIT_TEST(MissingOrAmbiguousColumnFails) {
        TNotNullValidator missing({ "absent" });
        UNIT_ASSERT(!missing.AddBatch(*Batch({ Values({ 1 }) })).ok());
        UNIT_ASSERT(!missing.Finish().ok());
        auto batch = arrow::Table::Make(arrow::schema({ arrow::field("value", arrow::int64()), arrow::field("value", arrow::int64()) }),
            std::vector<std::shared_ptr<arrow::Array>>{ Values({ 1 }), Values({ 2 }) });
        TNotNullValidator ambiguous({ "value" });
        UNIT_ASSERT(!ambiguous.AddBatch(*batch).ok());
        UNIT_ASSERT(!ambiguous.Finish().ok());
    }

    Y_UNIT_TEST(InvalidTargetColumnsFailEvenForEmptyScan) {
        TNotNullValidator empty({});
        UNIT_ASSERT(empty.IsFailed());
        UNIT_ASSERT(!empty.Finish().ok());
        TNotNullValidator duplicate({ "value", "value" });
        UNIT_ASSERT(duplicate.IsFailed());
        UNIT_ASSERT(!duplicate.Finish().ok());
    }

    Y_UNIT_TEST(ScanErrorCannotBecomeSuccessAtEndOfScan) {
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(validator.AddBatch(*Batch({ Values({ 1 }) })).ok());
        UNIT_ASSERT(!validator.Fail("read timeout").ok());
        UNIT_ASSERT(!validator.Finish().ok());
        UNIT_ASSERT_VALUES_EQUAL(validator.GetErrorMessage(), "read timeout");
        UNIT_ASSERT(!validator.IsValidated());
    }

    Y_UNIT_TEST(DataAfterEndOfScanInvalidatesProof) {
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(validator.Finish().ok());
        UNIT_ASSERT(!validator.AddBatch(*Batch({ Values({ 1 }) })).ok());
        UNIT_ASSERT(!validator.IsValidated());
        UNIT_ASSERT(!validator.Finish().ok());
    }

    Y_UNIT_TEST(LegacyNullableSchemaDoesNotPreventValidation) {
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(validator.AddBatch(*Batch({ Values({ 1, 2 }) }, true)).ok());
        UNIT_ASSERT(validator.Finish().ok());
    }

    Y_UNIT_TEST(NotNullSchemaDoesNotSubstituteForCheckingData) {
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(!validator.AddBatch(*Batch({ Values({ 1, std::nullopt }) }, false)).ok());
        UNIT_ASSERT(!validator.Finish().ok());
    }

    Y_UNIT_TEST(UnknownCachedNullCountIsComputedFromBitmap) {
        auto values = Values({ 1, std::nullopt });
        values->data()->SetNullCount(arrow::kUnknownNullCount);
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(!validator.AddBatch(*Batch({ values })).ok());
        UNIT_ASSERT(!validator.Finish().ok());
    }

    Y_UNIT_TEST(SlicedArraysCheckOnlyVisibleRange) {
        const auto values = Values({ std::nullopt, 1, 2, std::nullopt });
        TNotNullValidator valid({ "value" });
        UNIT_ASSERT(valid.AddBatch(*Batch({ values->Slice(1, 2) })).ok());
        UNIT_ASSERT(valid.Finish().ok());
        TNotNullValidator invalid({ "value" });
        UNIT_ASSERT(!invalid.AddBatch(*Batch({ values->Slice(1, 3) })).ok());
        UNIT_ASSERT(!invalid.Finish().ok());
    }

    Y_UNIT_TEST(EmptyBatchStillRequiresTargetColumn) {
        TNotNullValidator valid({ "value" });
        UNIT_ASSERT(valid.AddBatch(*Batch({ Values({}) })).ok());
        UNIT_ASSERT(valid.Finish().ok());
        TNotNullValidator invalid({ "absent" });
        UNIT_ASSERT(!invalid.AddBatch(*Batch({ Values({}) })).ok());
    }

    Y_UNIT_TEST(EncodedDictionaryCannotBeUsedAsProof) {
        auto type = arrow::dictionary(arrow::int64(), arrow::int64());
        auto values = std::make_shared<arrow::DictionaryArray>(type, Values({ 0, 1 }), Values({ 1, std::nullopt }));
        auto batch = arrow::Table::Make(arrow::schema({ arrow::field("value", type) }), std::vector<std::shared_ptr<arrow::Array>>{ values });
        TNotNullValidator validator({ "value" });
        UNIT_ASSERT(!validator.AddBatch(*batch).ok());
        UNIT_ASSERT(!validator.Finish().ok());
    }
}

}   // namespace NKikimr::NOlap
