#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_color_limits.h"

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_actions.h"
#include "blobstorage_pdisk_ut_helpers.h"
#include "blobstorage_pdisk_ut_run.h"

#include <ydb/core/testlib/actors/test_runtime.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPDiskConfig) {
    
    Y_UNIT_TEST(GetOwnerWeight) {
        using namespace NPDisk;
        using namespace NKikimrBlobStorage;

        TPDiskConfig cfg ({}, 0, 0, 0);
        cfg.SlotSizeUnits = TPDiskSlotSizeUnits::UNSPECIFIED;
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::UNSPECIFIED) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::SINGLE) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::DOUBLE) == 2);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::QUAD) == 4);

        cfg.SlotSizeUnits = TPDiskSlotSizeUnits::SINGLE;
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::UNSPECIFIED) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::SINGLE) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::DOUBLE) == 2);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::QUAD) == 4);

        cfg.SlotSizeUnits = TPDiskSlotSizeUnits::DOUBLE;
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::UNSPECIFIED) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::SINGLE) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::DOUBLE) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::QUAD) == 2);

        cfg.SlotSizeUnits = TPDiskSlotSizeUnits::QUAD;
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::UNSPECIFIED) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::SINGLE) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::DOUBLE) == 1);
        UNIT_ASSERT(cfg.GetOwnerWeight(TPDiskSlotSizeUnits::QUAD) == 1);
    }
}
} // namespace NKikimr
