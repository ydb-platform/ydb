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

    Y_UNIT_TEST(FilterSlice1) {
        TColumnFilter filter = TColumnFilter::BuildAllowFilter();
        filter.Add(true, 100);
        filter.Add(false, 1000);
        auto slice = filter.Slice(0, 130);
        AFL_VERIFY(slice.DebugString() == "{1}[100,30]")("slice_debug", slice.DebugString());
        AFL_VERIFY(filter.CheckSlice(0, 100));
        AFL_VERIFY(!filter.CheckSlice(100, 130));
        AFL_VERIFY(filter.CheckSlice(0, 130));
    }

    Y_UNIT_TEST(CutFilter1) {
        TColumnFilter filter = TColumnFilter::BuildAllowFilter();
        {
            auto cut = filter.Cut(1000, 10000, false);
            AFL_VERIFY(cut.DebugString() == "{1}[]")("val", cut.DebugString());
        }
        {
            auto cut = filter.Cut(100, 10, false);
            AFL_VERIFY(cut.DebugString() == "{1}[10,90]")("val", cut.DebugString());
            auto cut1 = cut.Cut(10, 3, false);
            AFL_VERIFY(cut1.DebugString() == "{1}[3,97]")("val", cut1.DebugString());
            auto cut2 = cut.Cut(10, 3, true);
            AFL_VERIFY(cut2.DebugString() == "{0}[7,3,90]")("val", cut2.DebugString());
        }
        {
            auto cut = filter.Cut(100, 10, true);
            AFL_VERIFY(cut.DebugString() == "{0}[90,10]")("val", cut.DebugString());
            auto cut1 = cut.Cut(10, 0, true);
            AFL_VERIFY(cut1.DebugString() == "{0}[100]")("val", cut1.DebugString());
        }
    }

    Y_UNIT_TEST(CutFilter2) {
        TColumnFilter filter = TColumnFilter::BuildAllowFilter();
        filter.Add(true, 4);
        filter.Add(false, 3);
        filter.Add(true, 2);
        filter.Add(false, 1);
        filter.Add(true, 4);
        filter.Add(false, 6);
        filter.Add(true, 6);
        filter.Add(false, 1);
        filter.Add(true, 3);
        filter.Add(false, 2);
        {
            auto cut = filter.Cut(filter.GetFilteredCountVerified(), 10, false);
            AFL_VERIFY(cut.DebugString() == "{1}[4,3,2,1,4,18]")("val", cut.DebugString());
            AFL_VERIFY(cut.GetRecordsCountVerified() == filter.GetRecordsCountVerified());
        }
        {
            auto cut = filter.Cut(filter.GetFilteredCountVerified(), 1, false);
            AFL_VERIFY(cut.DebugString() == "{1}[1,31]")("val", cut.DebugString());
            AFL_VERIFY(cut.GetRecordsCountVerified() == filter.GetRecordsCountVerified());
        }
        {
            auto cut = filter.Cut(filter.GetFilteredCountVerified(), 8, false);
            AFL_VERIFY(cut.DebugString() == "{1}[4,3,2,1,2,20]")("val", cut.DebugString());
            AFL_VERIFY(cut.GetRecordsCountVerified() == filter.GetRecordsCountVerified());
        }
        {
            auto cut = filter.Cut(filter.GetFilteredCountVerified(), 10, true);
            AFL_VERIFY(cut.DebugString() == "{0}[13,1,6,6,1,3,2]")("val", cut.DebugString());
            AFL_VERIFY(cut.GetRecordsCountVerified() == filter.GetRecordsCountVerified());
        }
        {
            auto cut = filter.Cut(filter.GetFilteredCountVerified(), 1000, true);
            AFL_VERIFY(cut.DebugString() == "{1}[4,3,2,1,4,6,6,1,3,2]")("val", cut.DebugString());
            AFL_VERIFY(cut.GetRecordsCountVerified() == filter.GetRecordsCountVerified());
        }
    }
}
