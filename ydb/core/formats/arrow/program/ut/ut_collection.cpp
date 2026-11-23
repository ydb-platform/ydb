#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/formats/arrow/program/collection.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NArrow::NAccessor {

Y_UNIT_TEST_SUITE(AccessorsCollectionRecordsCount) {
    std::shared_ptr<IChunkedArray> MakeUtf8Accessor(const ui32 recordsCount) {
        TTrivialArray::TPlainBuilder<arrow::StringType> builder;
        for (ui32 i = 0; i < recordsCount; ++i) {
            builder.AddRecord(i, "v");
        }
        return builder.Finish(recordsCount);
    }

    Y_UNIT_TEST(DictionaryOnlyAccessorLengthUsedWhenFilterNotApplied) {
        // Portion has 10 rows; dictionary-only fetch shortens the held array to 3 dictionary entries.
        TAccessorsCollection collection(10);
        collection.SetFilterUsage(false);
        collection.AddVerified(/*columnId=*/1, MakeUtf8Accessor(3), /*withFilter=*/false);
        UNIT_ASSERT_VALUES_EQUAL(collection.GetRecordsCountRobustVerified(), 3u);

        // A non-trivial not-applied filter must not fall back to RecordsCountOriginal (portion rows).
        collection.AddFilter(TColumnFilter::BuildConstFilter(true, { 1, 1, 1 }));
        UNIT_ASSERT(!collection.GetFilter().IsTotalAllowFilter());
        UNIT_ASSERT_VALUES_EQUAL(collection.GetRecordsCountRobustVerified(), 3u);
        UNIT_ASSERT_VALUES_UNEQUAL(collection.GetRecordsCountRobustVerified(), 10u);
    }
}

}   // namespace NKikimr::NArrow::NAccessor
