#include <ydb/core/formats/arrow/arrow_filter.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>

Y_UNIT_TEST_SUITE(ColumnFilter) {
    using namespace NKikimr::NArrow;

    Y_UNIT_TEST(MergeFilters) {
        TColumnFilter filter1({ true, false, true, true, false });
        TColumnFilter filter2({ true, true, true, true, false });

        auto result = filter1.Or(filter2);
        UNIT_ASSERT_VALUES_EQUAL(result.GetRecordsCountVerified(), 5);
        auto resultVec = result.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", resultVec), "1,1,1,1,0");
    }

    Y_UNIT_TEST(CombineFilters) {
        TColumnFilter filter1({ true, true, true, false, true, true, false });
        TColumnFilter filter2({ true, true, true, true, false });
        auto result = filter1.CombineSequentialAnd(filter2);

        UNIT_ASSERT_VALUES_EQUAL(result.GetRecordsCountVerified(), 7);
        auto resultVec = result.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", resultVec), "1,1,1,0,1,0,0");
    }

    Y_UNIT_TEST(FilterSlice) {
        TColumnFilter filter({ true, true, true, false, true, true, false });
        {
            TColumnFilter slice = filter.Slice(2, 3);
            AFL_VERIFY(slice.DebugString() == "{1}[1,1,1]")("slice_debug", slice.DebugString());
        }
        {
            TColumnFilter slice = filter.Slice(2, 5);
            AFL_VERIFY(slice.DebugString() == "{1}[1,1,2,1]")("slice_debug", slice.DebugString());
        }
        {
            TColumnFilter slice = filter.Slice(0, 3);
            AFL_VERIFY(slice.DebugString() == "{1}[3]")("slice_debug", slice.DebugString());
        }
        {
            TColumnFilter slice = filter.Slice(3, 1);
            AFL_VERIFY(slice.DebugString() == "{0}[1]")("slice_debug", slice.DebugString());
        }
        {
            TColumnFilter slice = filter.Slice(0, 7);
            AFL_VERIFY(slice.DebugString() == filter.DebugString());
        }
    }

    Y_UNIT_TEST(FilterCheckSlice) {
        TColumnFilter filter({ true, true, true, false, true, true, false });
        AFL_VERIFY(filter.CheckSlice(2, 3));
        AFL_VERIFY(filter.CheckSlice(0, 3));
        AFL_VERIFY(!filter.CheckSlice(3, 1));
        AFL_VERIFY(filter.CheckSlice(4, 3));
        AFL_VERIFY(filter.CheckSlice(4, 3));
        AFL_VERIFY(!filter.CheckSlice(6, 1));
    }
}
