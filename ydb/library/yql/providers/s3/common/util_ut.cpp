#include "util.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/unittest/registar.h>
#include <parquet/exception.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/formats/arrow/size_calcer.h>

namespace NYql::NS3Util {

Y_UNIT_TEST_SUITE(TestS3UrlEscape) {
    // Tests on force UrlEscape copied from library/cpp/string_utils/quote/quote_ut.cpp
    Y_UNIT_TEST(EscapeEscapedForce) {
        TString s;

        s = "hello%3dworld";
        UNIT_ASSERT_VALUES_EQUAL(NS3Util::UrlEscapeRet(s), "hello%253dworld");
    }

    Y_UNIT_TEST(EscapeUnescapeForceRet) {
        TString s;

        s = "hello%3dworld";
        UNIT_ASSERT_VALUES_EQUAL(UrlUnescapeRet(NS3Util::UrlEscapeRet(s)), "hello%3dworld");
    }

    // Test additional symbols escape
    Y_UNIT_TEST(EscapeAdditionalSymbols) {
        TString s = "hello#?world";

        UNIT_ASSERT_VALUES_EQUAL(NS3Util::UrlEscapeRet(s), "hello%23%3Fworld");
    }
}

Y_UNIT_TEST_SUITE(TestUrlBuilder) {
    Y_UNIT_TEST(UriOnly) {
        TUrlBuilder builder("https://localhost/abc");
        UNIT_ASSERT_VALUES_EQUAL(builder.Build(), "https://localhost/abc");
    }

    Y_UNIT_TEST(Basic) {
        TUrlBuilder builder("https://localhost/abc");
        builder.AddUrlParam("param1", "val1");
        builder.AddUrlParam("param2", "val2");

        UNIT_ASSERT_VALUES_EQUAL(builder.Build(), "https://localhost/abc?param1=val1&param2=val2");
    }

    Y_UNIT_TEST(BasicWithEncoding) {
        auto url = TUrlBuilder("https://localhost/abc")
                        .AddUrlParam("param1", "=!@#$%^&*(){}[]\" ")
                        .AddUrlParam("param2", "val2")
                        .Build();

        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc?param1=%3D%21%40%23%24%25%5E%26%2A%28%29%7B%7D%5B%5D%22+&param2=val2");
    }

    Y_UNIT_TEST(BasicWithAdditionalEncoding) {
        auto url = TUrlBuilder("https://localhost/abc")
                        .AddUrlParam("param1", ":/?#[]@!$&\'()*+,;=")
                        .AddUrlParam("param2", "val2")
                        .Build();

        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc?param1=%3A%2F%3F%23%5B%5D%40%21%24%26%27%28%29%2A%2B%2C%3B%3D&param2=val2");
    }
}

Y_UNIT_TEST_SUITE(TestArrowBlockSplitter) {
    using namespace NKikimr::NArrow;

    void ValidateSplit(std::shared_ptr<arrow::RecordBatch> initialBatch, ui64 numberParts, const std::vector<std::shared_ptr<arrow::RecordBatch>>& splttedBatches) {
        const ui64 expectedSplittedSize = initialBatch->num_rows() / numberParts;
        UNIT_ASSERT_VALUES_EQUAL(splttedBatches.size(), numberParts);

        ui64 rowsCount = 0;
        for (const auto& splttedBatch : splttedBatches) {
            const auto splittedSize = splttedBatch->num_rows();
            UNIT_ASSERT_VALUES_EQUAL_C(splittedSize, expectedSplittedSize, "Split result has unexpected number of rows from row " << rowsCount);
            UNIT_ASSERT_C(splttedBatch->Equals(*initialBatch->Slice(rowsCount, splittedSize)), "Split result is different from row " << rowsCount);
            rowsCount += splittedSize;
        }
        UNIT_ASSERT_VALUES_EQUAL(initialBatch->num_rows(), rowsCount);
    }

    Y_UNIT_TEST(SplitLargeBlock) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);

        const ui64 totalSize = GetBatchDataSize(batch);
        constexpr ui64 numberParts = 8;

        TArrowBlockSplitter splitter(totalSize / numberParts, 0, 0);
        std::vector<std::shared_ptr<arrow::RecordBatch>> splttedBatches;
        splitter.SplitRecordBatch(batch, 0, splttedBatches);
        ValidateSplit(batch, numberParts, splttedBatches);
    }

    Y_UNIT_TEST(SplitByRowSize) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int64Type>>>("field");
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);

        constexpr ui64 rowOverhead = sizeof(ui64);
        const ui64 totalSize = GetBatchDataSize(batch) + rowOverhead * batch->num_rows();
        constexpr ui64 numberParts = 8;

        TArrowBlockSplitter splitter(totalSize / numberParts, rowOverhead, 0);
        std::vector<std::shared_ptr<arrow::RecordBatch>> splttedBatches;
        splitter.SplitRecordBatch(batch, 0, splttedBatches);
        ValidateSplit(batch, numberParts, splttedBatches);
    }

    Y_UNIT_TEST(SplitByMetaSize) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int64Type>>>("field");
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);

        const ui64 totalSize = GetBatchDataSize(batch);
        const ui64 batchOverhead = totalSize / 2;

        TArrowBlockSplitter splitter(totalSize, 0, batchOverhead);
        std::vector<std::shared_ptr<arrow::RecordBatch>> splttedBatches;
        splitter.SplitRecordBatch(batch, 0, splttedBatches);
        ValidateSplit(batch, 2, splttedBatches);
    }

    Y_UNIT_TEST(PassSmallBlock) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(2048);

        constexpr ui64 rowOverhead = sizeof(ui64);
        const ui64 totalSize = GetBatchDataSize(batch) + rowOverhead * batch->num_rows();
        const ui64 batchOverhead = 1_MB;

        TArrowBlockSplitter splitter(totalSize + batchOverhead, rowOverhead, batchOverhead);
        std::vector<std::shared_ptr<arrow::RecordBatch>> splttedBatches;
        splitter.SplitRecordBatch(batch, 0, splttedBatches);
        ValidateSplit(batch, 1, splttedBatches);
    }

    Y_UNIT_TEST(CheckLargeRows) {
        constexpr ui64 strSize = 512;
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, strSize));
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(1);

        const ui64 totalSize = GetBatchDataSize(batch);

        constexpr ui64 rowId = 42;
        TArrowBlockSplitter splitter(strSize / 2, 0, 0);
        std::vector<std::shared_ptr<arrow::RecordBatch>> splttedBatches;
        UNIT_ASSERT_EXCEPTION_CONTAINS(splitter.SplitRecordBatch(batch, rowId, splttedBatches), parquet::ParquetException, TStringBuilder() << "Row " << rowId + 1 << " size is " << totalSize << ", that is larger than allowed limit " << strSize / 2);
    }
}

}  // namespace NYql::NS3Util
