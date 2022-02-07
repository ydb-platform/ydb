#include "hazard.h"

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

namespace NKikimr {

Y_UNIT_TEST_SUITE(THazardTest) {
    Y_UNIT_TEST(CachedPointers) {
        THazardDomain domain;
        UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 0u);
        {
            auto* ptr1 = domain.Acquire();
            auto* ptr2 = domain.Acquire();
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 2u);
            domain.Release(ptr1);
            domain.Release(ptr2);
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 2u);
            ptr1 = domain.Acquire();
            ptr2 = domain.Acquire();
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 2u);
            domain.Release(ptr1);
            domain.Release(ptr2);
            domain.DrainLocalCache();
        }
        UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 2u);
        {
            // First 2 Acquire()s should not allocate new pointers
            domain.Acquire();
            domain.Acquire();
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 2u);
            domain.Acquire();
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 3u);
        }
    }

    Y_UNIT_TEST(AutoProtectedPointers) {
        THazardDomain domain;
        int target1value, target2value;
        void* ptarget1 = &target1value;
        void* ptarget2 = &target2value;
        if (ptarget2 < ptarget1) {
            // sort for easier comparions
            std::swap(ptarget1, ptarget2);
        }

        std::vector<void*> hazards;
        std::atomic<void*> link{ ptarget1 };

        {
            TAutoHazardPointer ptr(domain);
            void* p = ptr.Protect(link);
            UNIT_ASSERT_VALUES_EQUAL(p, ptarget1);
            domain.CollectHazards([&](void* p) { hazards.push_back(p); });
            UNIT_ASSERT_VALUES_EQUAL(hazards.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(hazards[0], ptarget1);
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 1u);
        }

        // Since hazard pointer went out of scope nothing should be protected
        hazards.clear();
        domain.CollectHazards([&](void* p) { hazards.push_back(p); });
        UNIT_ASSERT_VALUES_EQUAL(hazards.size(), 0u);

        {
            link.store(ptarget2);
            TAutoHazardPointer ptr1(domain);
            void* p1 = ptr1.Protect(link);
            UNIT_ASSERT_VALUES_EQUAL(p1, ptarget2);
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 1u);

            link.store(ptarget1);
            TAutoHazardPointer ptr2(domain);
            void* p2 = ptr2.Protect(link);
            UNIT_ASSERT_VALUES_EQUAL(p2, ptarget1);
            UNIT_ASSERT_VALUES_EQUAL(domain.MaxPointers(), 2u);

            hazards.clear();
            domain.CollectHazards([&](void* p) { hazards.push_back(p); });
            std::sort(hazards.begin(), hazards.end());
            UNIT_ASSERT_VALUES_EQUAL(hazards.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(hazards[0], ptarget1);
            UNIT_ASSERT_VALUES_EQUAL(hazards[1], ptarget2);
        }
    }
}

}
