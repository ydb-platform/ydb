#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/interval_counter.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;
using namespace NKikimr::NOlap::NReader::NSimple;

Y_UNIT_TEST_SUITE(TIntervalCounter) {
    void TestDec(TIntervalCounter & counter, ui32 l, ui32 r, const std::vector<ui32>& expectedZeros) {
        const auto result = counter.DecAndGetZeros(l, r);
        UNIT_ASSERT_EQUAL_C(result, expectedZeros,
            TStringBuilder() << '[' << JoinSeq(',', result) << "] (actual) != [" << JoinSeq(',', expectedZeros) << "] (expected)");
    }

    Y_UNIT_TEST(Count) {
        TIntervalCounter counter({ { 0, 0 }, { 1, 1 } });
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 1), 2);
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 1);
        TestDec(counter, 0, 1, std::vector<ui32>({ 0, 1 }));
        UNIT_ASSERT(counter.IsAllZeros());
    }

    Y_UNIT_TEST(Dec0) {
        TIntervalCounter counter({ { 0, 0 }, { 0, 1 }, { 1, 1 }, { 1, 1 } });
        TestDec(counter, 0, 0, std::vector<ui32>({}));
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 0), 1);
        TestDec(counter, 0, 1, std::vector<ui32>({ 0 }));
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(0, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 2);
        TestDec(counter, 1, 1, std::vector<ui32>({}));
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 1);
        TestDec(counter, 1, 1, std::vector<ui32>({ 1 }));
        UNIT_ASSERT_VALUES_EQUAL(counter.GetCount(1, 1), 0);
        UNIT_ASSERT(counter.IsAllZeros());
    }

    Y_UNIT_TEST(Dec1) {
        TIntervalCounter counter({ { 0, 0 }, { 0, 1 } });
        TestDec(counter, 0, 1, std::vector<ui32>({ 1 }));
        TestDec(counter, 0, 0, std::vector<ui32>({ 0 }));
        UNIT_ASSERT(counter.IsAllZeros());
    }

    Y_UNIT_TEST(Dec2) {
        TIntervalCounter counter({ { 0, 0 }, { 0, 1 } });
        TestDec(counter, 0, 0, std::vector<ui32>({}));
        TestDec(counter, 0, 1, std::vector<ui32>({ 0, 1 }));
        UNIT_ASSERT(counter.IsAllZeros());
    }
}
