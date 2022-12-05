#include "mkql_rh_hash.h"

#include <library/cpp/testing/unittest/registar.h>

#include <unordered_map>
#include <unordered_set>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLRobinHoodHashTest) {
    Y_UNIT_TEST(Map) {
        TRobinHoodHashMap<i32> rh(sizeof(i64));
        std::unordered_map<i32, i64> h;
        for (ui64 i = 0; i < 10000; ++i) {
            auto k = i % 1000;
            auto [it, inserted] = h.emplace(k, 0);
            bool isNew;
            void* p = rh.Insert(k, isNew);
            UNIT_ASSERT_VALUES_EQUAL(isNew, inserted);
            it->second += i;
            *(i64*)p += i;
            if (isNew) {
                rh.CheckGrow();
            }

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        }

        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (rh.GetPSL(it) < 0) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            UNIT_ASSERT_VALUES_EQUAL(*(i64*)rh.GetPayload(it), hit->second);
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }

    Y_UNIT_TEST(Set) {
        TRobinHoodHashSet<i32> rh;
        std::unordered_set<i32> h;
        for (ui64 i = 0; i < 10000; ++i) {
            auto k = i % 1000;
            auto[it, inserted] = h.emplace(k);
            bool isNew;
            void* p = rh.Insert(k, isNew);
            Y_UNUSED(p);
            UNIT_ASSERT_VALUES_EQUAL(isNew, inserted);
            if (isNew) {
                rh.CheckGrow();
            }

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        }

        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (rh.GetPSL(it) < 0) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }
}

}
}
