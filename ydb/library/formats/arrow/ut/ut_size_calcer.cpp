#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/formats/arrow/size_calcer.h>

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

};
