#include <ydb/core/tx/columnshard/engines/reader/duplicates/manager.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;

Y_UNIT_TEST_SUITE(TIntervalCounter) {

Y_UNIT_TEST(Basic) {
    TIntervalCounter counter({{0, 0}, {1, 1}});
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 1), 2);
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 0), 1);
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 1);
    UNIT_ASSERT_EQUAL(counter.DecAndGetZeros(0, 1), std::vector<ui32>({0, 1}));
    UNIT_ASSERT(counter.IsAllZeros());
}

Y_UNIT_TEST(Propagation) {
    TIntervalCounter counter({{0, 0}, {0, 1}, {1, 1}, {1, 1}});
    UNIT_ASSERT_EQUAL(counter.DecAndGetZeros(0, 0), std::vector<ui32>({}));
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 0), 1);
    UNIT_ASSERT_EQUAL(counter.DecAndGetZeros(0, 1), std::vector<ui32>({0}));
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 0), 0);
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 2);
    UNIT_ASSERT_EQUAL(counter.DecAndGetZeros(1, 1), std::vector<ui32>({}));
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 1);
    UNIT_ASSERT_EQUAL(counter.DecAndGetZeros(1, 1), std::vector<ui32>({1}));
    UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 0);
    UNIT_ASSERT(counter.IsAllZeros());
}

}