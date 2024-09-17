#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/formats/arrow/size_calcer.h>


namespace {

void CheckStatus(arrow::Status status) {
    UNIT_ASSERT_C(status.ok(), status.ToString());
}

}

Y_UNIT_TEST_SUITE(SizeCalcer) {

    using namespace NKikimr::NArrow;

    Y_UNIT_TEST(SimpleStrings) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);
        Cerr << GetBatchDataSize(batch) << Endl;
        UNIT_ASSERT(GetBatchDataSize(batch) == 2048 * 512 + 2048 * 4);
        auto slice05 = batch->Slice(batch->num_rows() / 2, batch->num_rows() / 2);
        Cerr << GetBatchDataSize(slice05) << Endl;
        UNIT_ASSERT(GetBatchDataSize(slice05) == 0.5 * (2048 * 512 + 2048 * 4));
        auto slice025 = slice05->Slice(slice05->num_rows() / 3, slice05->num_rows() / 2);
        Cerr << GetBatchDataSize(slice025) << Endl;
        UNIT_ASSERT(GetBatchDataSize(slice025) == 0.25 * (2048 * 512 + 2048 * 4));
    }

    Y_UNIT_TEST(DictionaryStrings) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TDictionaryArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);
        Cerr << GetBatchDataSize(batch) << Endl;
        UNIT_ASSERT(GetBatchDataSize(batch) == 8 * 512 + 2048 + 4 * 8);
    }

    Y_UNIT_TEST(ZeroSimpleStrings) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(1, 0));
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);
        Cerr << GetBatchDataSize(batch) << Endl;
        UNIT_ASSERT(GetBatchDataSize(batch) == 2048 * 4);
    }

    Y_UNIT_TEST(ZeroDictionaryStrings) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TDictionaryArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(1, 0));
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);
        Cerr << GetBatchDataSize(batch) << Endl;
        UNIT_ASSERT(GetBatchDataSize(batch) == 2048 + 4);
    }

    Y_UNIT_TEST(SimpleInt64) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int64Type>>>("field");
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);
        Cerr << GetBatchDataSize(batch) << Endl;
        UNIT_ASSERT(GetBatchDataSize(batch) == 2048 * 8);
    }

    Y_UNIT_TEST(SimpleTimestamp) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::TimestampType>>>("field");
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);
        Cerr << GetBatchDataSize(batch) << Endl;
        UNIT_ASSERT(GetBatchDataSize(batch) == 2048 * 8);
    }

    Y_UNIT_TEST(NestedFixedSizeList) {
        constexpr i32 elementSize = 512;
        constexpr i32 listSize = 16;
        constexpr i32 recordsCount = 2048;

        auto valueBuilder = std::make_shared<arrow::StringBuilder>();
        CheckStatus(valueBuilder->Reserve(listSize * recordsCount));

        arrow::FixedSizeListBuilder listBuilder(arrow::default_memory_pool(), valueBuilder, listSize);
        CheckStatus(listBuilder.Reserve(recordsCount));

        NConstruction::TStringPoolFiller filler(8, elementSize);
        for (i32 recordId = 0; recordId < recordsCount; ++recordId) {
            CheckStatus(listBuilder.Append());
            for (i32 elementId = 0; elementId < listSize; ++elementId) {
                CheckStatus(valueBuilder->Append(filler.GetValue(recordId * listSize + elementId)));
            }
        }
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TStaticArrayConstructor>("field", *listBuilder.Finish());
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({column}).BuildBatch(recordsCount);

        constexpr i32 amountSize = recordsCount * listSize * (elementSize + 4);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(batch), amountSize);
        auto slice05 = batch->Slice(batch->num_rows() / 2, batch->num_rows() / 2);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(slice05), amountSize / 2);
        auto slice025 = slice05->Slice(slice05->num_rows() / 3, slice05->num_rows() / 2);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(slice025), amountSize / 4);
    }

    Y_UNIT_TEST(NestedList) {
        constexpr i32 elementSize = 512;
        constexpr i32 awerageListSize = 16;
        constexpr i32 recordsCount = 2048;

        auto valueBuilder = std::make_shared<arrow::StringBuilder>();
        CheckStatus(valueBuilder->Reserve(2 * awerageListSize * recordsCount));

        arrow::ListBuilder listBuilder(arrow::default_memory_pool(), valueBuilder);
        CheckStatus(listBuilder.Reserve(recordsCount));

        i32 amountListsSize = 0;
        i32 halfListsSize = 0;
        NConstruction::TStringPoolFiller filler(8, elementSize);
        for (i32 recordId = 0; recordId < recordsCount; ++recordId) {
            CheckStatus(listBuilder.Append());

            TReallyFastRng32 rand(recordId);
            i32 listSize = rand.Uniform(awerageListSize / 2, 3 * awerageListSize / 2);
            for (i32 elementId = 0; elementId < listSize; ++elementId) {
                CheckStatus(valueBuilder->Append(filler.GetValue(2 * recordId * awerageListSize + elementId)));
            }

            amountListsSize += listSize;
            if (recordId >= recordsCount / 2) {
                halfListsSize += listSize;
            }
        }
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TStaticArrayConstructor>("field", *listBuilder.Finish());
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({column}).BuildBatch(recordsCount);

        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(batch), amountListsSize * (elementSize + 4) + recordsCount * 4);
        auto slice05 = batch->Slice(batch->num_rows() / 2, batch->num_rows() / 2);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(slice05), halfListsSize * (elementSize + 4) + recordsCount * 2);
    }

    Y_UNIT_TEST(NestedStruct) {
        constexpr i32 stringSize = 512;
        constexpr i32 recordsCount = 2048;

        auto stringBuilder = std::make_shared<arrow::StringBuilder>();
        CheckStatus(stringBuilder->Reserve(recordsCount));

        auto intBuilder = std::make_shared<arrow::Int32Builder>();
        CheckStatus(intBuilder->Reserve(recordsCount));

        auto structType = std::make_shared<arrow::StructType>(std::vector{
            std::make_shared<arrow::Field>("string_field", stringBuilder->type()),
            std::make_shared<arrow::Field>("int32_field", intBuilder->type()),
        });

        arrow::StructBuilder structBuilder(structType, arrow::default_memory_pool(), {
            stringBuilder,
            intBuilder
        });
        CheckStatus(structBuilder.Reserve(recordsCount));

        NConstruction::TIntSeqFiller<arrow::Int32Type> intFiller(42);
        NConstruction::TStringPoolFiller stringFiller(8, stringSize);
        for (i32 recordId = 0; recordId < recordsCount; ++recordId) {
            CheckStatus(structBuilder.Append());
            CheckStatus(intBuilder->Append(intFiller.GetValue(recordId)));
            CheckStatus(stringBuilder->Append(stringFiller.GetValue(recordId)));
        }
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TStaticArrayConstructor>("field", *structBuilder.Finish());
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({column}).BuildBatch(recordsCount);

        constexpr i32 amountSize = recordsCount * (stringSize + 8);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(batch), amountSize);
        auto slice05 = batch->Slice(batch->num_rows() / 2, batch->num_rows() / 2);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(slice05), amountSize / 2);
        auto slice025 = slice05->Slice(slice05->num_rows() / 3, slice05->num_rows() / 2);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(slice025), amountSize / 4);
    }

    Y_UNIT_TEST(NestedSparseUnion) {
        constexpr i32 stringSize = 512;
        constexpr i32 recordsCount = 2048;

        auto stringBuilder = std::make_shared<arrow::StringBuilder>();
        CheckStatus(stringBuilder->Reserve(recordsCount));

        auto intBuilder = std::make_shared<arrow::Int32Builder>();
        CheckStatus(intBuilder->Reserve(recordsCount));

        arrow::SparseUnionBuilder sparseUnionBuilder(arrow::default_memory_pool());
        sparseUnionBuilder.AppendChild(stringBuilder);
        sparseUnionBuilder.AppendChild(intBuilder);
        CheckStatus(sparseUnionBuilder.Reserve(recordsCount));

        i32 amountStrings = 0;
        i32 halfStrings = 0;
        NConstruction::TIntSeqFiller<arrow::Int32Type> intFiller(42);
        NConstruction::TStringPoolFiller stringFiller(8, stringSize);
        for (i32 recordId = 0; recordId < recordsCount; ++recordId) {
            TReallyFastRng32 rand(recordId);
            i32 typeId = rand.Uniform(0, 2);

            CheckStatus(sparseUnionBuilder.Append(typeId));
            if (typeId) {
                CheckStatus(intBuilder->Append(intFiller.GetValue(recordId)));
                CheckStatus(stringBuilder->AppendEmptyValue());
            } else {
                CheckStatus(intBuilder->AppendEmptyValue());
                CheckStatus(stringBuilder->Append(stringFiller.GetValue(recordId)));

                amountStrings++;
                halfStrings += recordId >= recordsCount / 2;
            }
        }
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TStaticArrayConstructor>("field", *sparseUnionBuilder.Finish());
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({column}).BuildBatch(recordsCount);

        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(batch), recordsCount * 9 + amountStrings * stringSize);
        auto slice05 = batch->Slice(batch->num_rows() / 2, batch->num_rows() / 2);
        UNIT_ASSERT_VALUES_EQUAL(GetBatchDataSize(slice05), (recordsCount / 2) * 9 + halfStrings * stringSize);
    }
};
