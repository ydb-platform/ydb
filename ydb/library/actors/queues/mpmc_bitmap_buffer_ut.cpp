#define MPMC_RING_QUEUE_COLLECT_STATISTICS

#include "mpmc_bitmap_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

#include <queue>


using namespace NActors;

namespace { // Tests


    void TestRandomUsage(ui32 iterationCount, ui32 MaxSize, TMPMCBitMapBuffer &buffer) {
        SetRandomSeed(727);
        std::unordered_set<ui32> validationSet;

        for (ui32 it = 0; it < iterationCount; ++it) {
            bool isPush = RandomNumber<ui32>(2);
            TString debugString = TStringBuilder() << "it: " << it << " action: " << (isPush && validationSet.size() < MaxSize? "push" : "pop") << " size: " << validationSet.size() << '/' << MaxSize;
            if (isPush && validationSet.size() < MaxSize) {
                if (validationSet.size() < MaxSize) {
                    buffer.Push(it);
                    validationSet.insert(it);
                }
            } else {
                if (validationSet.empty()) {
                    bool wasNotCalled = false;
                    UNIT_ASSERT_C(!buffer.Find(it, [&](ui64){ wasNotCalled = true; return false; }), debugString);
                    UNIT_ASSERT_C(!wasNotCalled, debugString);
                } else {
                    ui64 valEl = *validationSet.begin();
                    UNIT_ASSERT_C(buffer.Find(it, [&](ui64 el){ return el == valEl; }), debugString << " valEl: " << valEl);
                    validationSet.erase(validationSet.begin());
                }
            }
        }
    }

}


Y_UNIT_TEST_SUITE(MPMCBitmaskBufferSingleThreadTests) {


    Y_UNIT_TEST(RandomUsage) {
        ui32 sizeBits = 3;
        TMPMCBitMapBuffer buffer(sizeBits);
        TestRandomUsage(100'000, (ui64(1) << sizeBits), buffer);
    }

    Y_UNIT_TEST(RandomUsageMedium) {
        ui32 sizeBits = 7;
        TMPMCBitMapBuffer buffer(sizeBits);
        TestRandomUsage(100'000, (ui64(1) << sizeBits), buffer);
    }

    Y_UNIT_TEST(RandomUsageLarge) {
        ui32 sizeBits = 12;
        TMPMCBitMapBuffer buffer(sizeBits);
        TestRandomUsage(100'000, (ui64(1) << sizeBits), buffer);
    }

}
