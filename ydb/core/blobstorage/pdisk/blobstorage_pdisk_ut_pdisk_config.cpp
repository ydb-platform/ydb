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

        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(0, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(1, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(2, 0), 2);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(3, 0), 3);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(4, 0), 4);

        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(0, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(1, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(2, 1), 2);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(3, 1), 3);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(4, 1), 4);

        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(0, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(1, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(2, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(3, 2), 2);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(4, 2), 2);

        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(0, 4), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(1, 4), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(2, 4), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(3, 4), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(4, 4), 1);

        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(5, 3), 2);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(99, 100), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPDiskConfig::GetOwnerWeight(101, 100), 2);

        TPDiskConfig pdiskConfig3u(0, 0, 0);
        pdiskConfig3u.SlotSizeInUnits = 3;
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig3u.GetOwnerWeight(10), 4);

        // TODO(ydynnikov): test the case of groupSizeInUnits > UI8_MAX (255)
    }
}
} // namespace NKikimr
