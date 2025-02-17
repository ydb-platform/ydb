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
                oos.UpdateLocalFreeSpaceShare(ui64(1 << 24) * 0.5);
                ui32 hugeCanBeFreedChunks = 9;
                ui32 hugeUsedChunks = 20;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30, 0.13);
                UNIT_ASSERT(!defrag);
            }
            {
                TOutOfSpaceState oos(1, 0);
                oos.UpdateLocalFreeSpaceShare(ui64(1 << 24) * 0.5);
                ui32 hugeCanBeFreedChunks = 200;
                ui32 hugeUsedChunks = 1000;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30, 0.13);
                UNIT_ASSERT(!defrag);
            }
            {
                TOutOfSpaceState oos(1, 0);
                oos.UpdateLocalFreeSpaceShare(ui64(1 << 24) * 0.5);
                ui32 hugeCanBeFreedChunks = 301;
                ui32 hugeUsedChunks = 1000;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30, 0.13);
                UNIT_ASSERT(defrag);
            }
            {
                TOutOfSpaceState oos(1, 0);
                oos.UpdateLocalFreeSpaceShare(ui64(1 << 24) * 0.05);
                ui32 hugeUsedChunks = 1000;
                ui32 hugeCanBeFreedChunks = ui32(5.0 / 13 * 0.3 * hugeUsedChunks) - 1;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30, 0.13);
                UNIT_ASSERT(!defrag);
            }
            {
                TOutOfSpaceState oos(1, 0);
                oos.UpdateLocalFreeSpaceShare(ui64(1 << 24) * 0.05);
                ui32 hugeUsedChunks = 1000;
                ui32 hugeCanBeFreedChunks = ui32(0.05 / 0.13 * 0.3 * hugeUsedChunks) + 1;
                bool defrag = HugeHeapDefragmentationRequired(oos, hugeCanBeFreedChunks, hugeUsedChunks, 0.30, 0.13);
                UNIT_ASSERT(defrag);
            }
        }
    }

} // NKikimr
