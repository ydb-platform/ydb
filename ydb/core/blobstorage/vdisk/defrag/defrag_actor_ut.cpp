#include "defs.h"
#include "defrag_actor.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

#define STR Cnull

using namespace NKikimr;

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TVDiskDefrag) {
        Y_UNIT_TEST(HugeHeapDefragmentationRequired) {
            {
                TOutOfSpaceState oos(1, 0);
                ui32 hugeCanBeFreedChunks = 9;
                ui32 hugeUsedChunks = 20;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30);
                UNIT_ASSERT(!defrag);
            }
            {
                TOutOfSpaceState oos(1, 0);
                ui32 hugeCanBeFreedChunks = 200;
                ui32 hugeUsedChunks = 1000;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30);
                UNIT_ASSERT(!defrag);
            }
            {
                TOutOfSpaceState oos(1, 0);
                ui32 hugeCanBeFreedChunks = 301;
                ui32 hugeUsedChunks = 1000;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30);
                UNIT_ASSERT(defrag);
            }
        }
    }

} // NKikimr
