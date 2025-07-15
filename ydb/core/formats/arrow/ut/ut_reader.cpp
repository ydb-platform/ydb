#include <ydb/core/formats/arrow/reader/position.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NArrow {

Y_UNIT_TEST_SUITE(SortableBatchPosition) {
    Y_UNIT_TEST(FindPosition) {
        std::shared_ptr<arrow::RecordBatch> data;
        std::shared_ptr<arrow::Schema> schema =
            std::make_shared<arrow::Schema>(arrow::Schema({ std::make_shared<arrow::Field>("class", std::make_shared<arrow::StringType>()),
                std::make_shared<arrow::Field>("name", std::make_shared<arrow::StringType>()) }));
        {
            std::unique_ptr<arrow::RecordBatchBuilder> batchBuilder;
            UNIT_ASSERT(arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), &batchBuilder).ok());

            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("c").ok());

            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());

            UNIT_ASSERT(batchBuilder->Flush(&data).ok());
        }

        std::shared_ptr<arrow::RecordBatch> search;
        {
            std::unique_ptr<arrow::RecordBatchBuilder> batchBuilder;
            UNIT_ASSERT(arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), &batchBuilder).ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->Flush(&search).ok());
        }

        NMerger::TSortableBatchPosition searchPosition(search, 0, false);
        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPosition, false, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 2);
        }

        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPosition, true, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 4);
        }

        NMerger::TSortableBatchPosition searchPositionReverse(search, 0, true);
        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPositionReverse, false, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 3);
        }
        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPositionReverse, true, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 1);
        }
    }
}

}   // namespace NKikimr::NArrow
