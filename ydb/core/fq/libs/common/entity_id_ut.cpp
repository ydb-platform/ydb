#include "entity_id.h"

#include <ydb/services/ydb/ydb_common_ut.h>
#include <limits>

using namespace NFq;

Y_UNIT_TEST_SUITE(EntityId) {
    Y_UNIT_TEST(Distinct) {
        int count = 100;
        TVector<TString> values;
        THashSet<TString> uniq;
        values.reserve(count);
        for (int i = 0; i < count; i++) {
            values.push_back(GetEntityIdAsString("de", EEntityType::UNDEFINED));
            uniq.insert(values.back());
        }

        UNIT_ASSERT_VALUES_EQUAL(uniq.size(), values.size());
    }

    Y_UNIT_TEST(Order) {
        int count = 100;
        TVector<TString> values, copy;
        values.reserve(count);
        for (int i = 0; i < count; i++) {
            values.push_back(GetEntityIdAsString("de", EEntityType::UNDEFINED));
            Sleep(TDuration::MilliSeconds(1)); // timer resolution
        }
        copy = values;
        std::sort(copy.begin(), copy.end(), [](const auto& a, const auto& b) {
            return a>b;
        });

        for (int i = 0; i < count; i++) {
            UNIT_ASSERT_VALUES_EQUAL(values[i], copy[i]);
        }
    }

    Y_UNIT_TEST(MinId) {
        UNIT_ASSERT_VALUES_EQUAL(GetEntityIdAsString("00", EEntityType::UNDEFINED, TInstant::FromValue(0x7FFFFFFFFFFFFFULL), 0), "00u00000000000000000");
    }

    Y_UNIT_TEST(MaxId) {
        UNIT_ASSERT_VALUES_EQUAL(GetEntityIdAsString("vv", EEntityType::UNDEFINED, TInstant::Zero(), std::numeric_limits<ui32>::max()), "vvuvvvvvvvvvvvvvvvvv");
    }

    Y_UNIT_TEST(CheckId) {
        UNIT_ASSERT_VALUES_EQUAL(GetEntityIdAsString("ut", EEntityType::QUERY, TInstant::ParseIso8601("2021-12-04"), 666666), "utquhdn55pa7vv00kb1a");
    }
}
