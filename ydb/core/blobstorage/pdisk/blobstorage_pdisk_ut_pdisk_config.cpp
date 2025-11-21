#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_color_limits.h"
#include "blobstorage_pdisk_impl.h"

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

    Y_UNIT_TEST(KeeperLogParamsEnabledForTinyDisk) {
        TIntrusivePtr<TPDiskConfig> cfg = new TPDiskConfig(0, 0, TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
        cfg->FeatureFlags.SetEnablePDiskLogForSmallDisks(true);

        alignas(16) NPDisk::TDiskFormat format;
        format.DiskSize = 4ull << 30;

        NPDisk::TKeeperParams params;
        InitializeKeeperLogParams(params, cfg, format);

        UNIT_ASSERT_VALUES_EQUAL(params.MaxCommonLogChunks, NPDisk::TinyDiskMaxCommonLogChunks);
        UNIT_ASSERT_VALUES_EQUAL(params.CommonStaticLogChunks, NPDisk::TinyDiskCommonStaticLogChunks);
        UNIT_ASSERT_VALUES_EQUAL(params.SeparateCommonLog, true);
    }

    Y_UNIT_TEST(KeeperLogParamsEnabledForSmallDisk) {
        TIntrusivePtr<TPDiskConfig> cfg = new TPDiskConfig(0, 0, TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
        cfg->FeatureFlags.SetEnablePDiskLogForSmallDisks(true);

        alignas(16) NPDisk::TDiskFormat format;
        format.DiskSize = 100ull << 30;

        NPDisk::TKeeperParams params;
        InitializeKeeperLogParams(params, cfg, format);

        UNIT_ASSERT_VALUES_EQUAL(params.MaxCommonLogChunks, 106ull);
        UNIT_ASSERT_VALUES_EQUAL(params.CommonStaticLogChunks, 36ull);
        UNIT_ASSERT_VALUES_EQUAL(params.SeparateCommonLog, true);
    }

    Y_UNIT_TEST(KeeperLogParamsEnabledForNormalDisk) {
        TIntrusivePtr<TPDiskConfig> cfg = new TPDiskConfig(0, 0, TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
        cfg->FeatureFlags.SetEnablePDiskLogForSmallDisks(true);

        alignas(16) NPDisk::TDiskFormat format;
        format.DiskSize = 1000ull << 30;

        NPDisk::TKeeperParams params;
        InitializeKeeperLogParams(params, cfg, format);

        UNIT_ASSERT_VALUES_EQUAL(params.MaxCommonLogChunks, 200ull);
        UNIT_ASSERT_VALUES_EQUAL(params.CommonStaticLogChunks, 70ull);
        UNIT_ASSERT_VALUES_EQUAL(params.SeparateCommonLog, true);
    }

    Y_UNIT_TEST(KeeperLogParamsDisabledForSmallDisk) {
        TIntrusivePtr<TPDiskConfig> cfg = new TPDiskConfig(0, 0, TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
        cfg->FeatureFlags.SetEnablePDiskLogForSmallDisks(false);

        alignas(16) NPDisk::TDiskFormat format;
        format.DiskSize = 100ull << 30;

        NPDisk::TKeeperParams params;
        InitializeKeeperLogParams(params, cfg, format);

        UNIT_ASSERT_VALUES_EQUAL(params.MaxCommonLogChunks, 200ull);
        UNIT_ASSERT_VALUES_EQUAL(params.CommonStaticLogChunks, 70ull);
        UNIT_ASSERT_VALUES_EQUAL(params.SeparateCommonLog, false);
    }

    Y_UNIT_TEST(KeeperLogParamsDisabledForNormalDisk) {
        TIntrusivePtr<TPDiskConfig> cfg = new TPDiskConfig(0, 0, TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
        cfg->FeatureFlags.SetEnablePDiskLogForSmallDisks(false);

        alignas(16) NPDisk::TDiskFormat format;
        format.DiskSize = 100ull << 30;

        NPDisk::TKeeperParams params;
        InitializeKeeperLogParams(params, cfg, format);

        UNIT_ASSERT_VALUES_EQUAL(params.MaxCommonLogChunks, 200ull);
        UNIT_ASSERT_VALUES_EQUAL(params.CommonStaticLogChunks, 70ull);
        UNIT_ASSERT_VALUES_EQUAL(params.SeparateCommonLog, false);
    }
}
} // namespace NKikimr
