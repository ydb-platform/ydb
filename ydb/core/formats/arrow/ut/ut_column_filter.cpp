#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <util/string/join.h>

Y_UNIT_TEST_SUITE(ColumnFilter) {

    using namespace NKikimr::NArrow;

    Y_UNIT_TEST(MergeFilters) {
        TColumnFilter filter1({true, false, true, true, false});
        TColumnFilter filter2({true, true, true, true, false});

        auto result = filter1.Or(filter2);
        UNIT_ASSERT_VALUES_EQUAL(result.Size(), 5);
        auto resultVec = result.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", resultVec), "1,1,1,1,0");
    }

    Y_UNIT_TEST(CombineFilters) {
        TColumnFilter filter1({ true, true,true, false, true, true, false});
        TColumnFilter filter2({true, true, true, true, false});
        auto result = filter1.CombineSequentialAnd(filter2);

        UNIT_ASSERT_VALUES_EQUAL(result.Size(), 7);
        auto resultVec = result.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", resultVec), "1,1,1,0,1,0,0");
    }
}
