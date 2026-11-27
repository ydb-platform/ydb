#include <ydb/core/protos/filestore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/generic/size_literals.h>

#include <optional>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

auto& InitCreateFileStoreConfig(
    const TString& name,
    NKikimrSchemeOp::TFileStoreDescription& vdescr,
    bool isSystem = false)
{
    vdescr.SetName(name);

    auto& config = *vdescr.MutableConfig();
    config.SetBlockSize(4_KB);
    config.SetBlocksCount(4096);
    config.SetFileSystemId(name);
    config.SetCloudId("cloud");
    config.SetFolderId("folder");
    if (isSystem) {
        // Only set when requested.
        config.SetIsSystem(true);
    }

    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

    return config;
}

// Checks the domain's __filestore_space_allocated_ssd_system runtime attribute.
// std::nullopt means the attribute must be absent, i.e. the allocated counter is zero.
NLs::TCheckFunc SsdSystemAllocated(std::optional<TString> expected) {
    return [expected](const NKikimrScheme::TEvDescribeSchemeResult& record) {
        std::optional<TString> actual;
        for (const auto& attr : record.GetPathDescription().GetUserAttributes()) {
            if (attr.GetKey() == "__filestore_space_allocated_ssd_system") {
                actual = attr.GetValue();
                break;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(expected.has_value(), actual.has_value());

        if (expected) {
            UNIT_ASSERT_VALUES_EQUAL(*expected, *actual);
        }
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TFileStore) {
    Y_UNIT_TEST(CreateFileStoreRejectsAllocatedSpaceOverflow) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TFileStoreDescription hugeDescr;
        auto& hugeConfig = InitCreateFileStoreConfig("HugeFS", hugeDescr);
        hugeConfig.SetStorageMediaKind(1);
        hugeConfig.SetBlocksCount(Max<ui64>() / 4_KB);

        TestCreateFileStore(runtime, ++txId, "/MyRoot", hugeDescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TFileStoreDescription smallDescr;
        auto& smallConfig = InitCreateFileStoreConfig("SmallFS", smallDescr);
        smallConfig.SetStorageMediaKind(1);
        smallConfig.SetBlocksCount(2);

        TestCreateFileStore(
            runtime,
            ++txId,
            "/MyRoot",
            smallDescr.DebugString(),
            {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDropFileStore(runtime, ++txId, "/MyRoot", "HugeFS");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST_FLAG(SystemSpaceAccountingOverflow, DisableFileStoreSSDSystemSpaceAccounting) {
        // Each system-SSD FileStore (filesystem) shard is resized to the final filesystem size,
        // which can reach petabytes. Summing thousands of such shards into the per-domain
        // ssd_system allocated counter overflows ui64. The feature flag disables that accounting,
        // so the counter never grows and the overflow can no longer occur.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .DisableFileStoreSSDSystemSpaceAccounting(DisableFileStoreSSDSystemSpaceAccounting));
        ui64 txId = 100;

        constexpr ui64 blockSize = 4_KB;
        const ui64 hugeBlocks = Max<ui64>() / blockSize; // a single shard nearly fills ui64

        NKikimrSchemeOp::TFileStoreDescription hugeDescr;
        auto& hugeConfig = InitCreateFileStoreConfig("HugeSystemFS", hugeDescr, /*isSystem*/ true);
        hugeConfig.SetStorageMediaKind(1); // STORAGE_MEDIA_SSD
        hugeConfig.SetBlocksCount(hugeBlocks);

        // A single shard always fits within ui64, regardless of the flag.
        TestCreateFileStore(runtime, ++txId, "/MyRoot", hugeDescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // The ssd_system allocated attribute is published on the domain only while accounting is on.
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            DisableFileStoreSSDSystemSpaceAccounting
                ? SsdSystemAllocated(std::nullopt)
                : SsdSystemAllocated(ToString(hugeBlocks * blockSize)),
        });

        // A second system-SSD shard would push the domain counter past Max<ui64>().
        NKikimrSchemeOp::TFileStoreDescription smallDescr;
        auto& smallConfig = InitCreateFileStoreConfig("SmallSystemFS", smallDescr, /*isSystem*/ true);
        smallConfig.SetStorageMediaKind(1);
        smallConfig.SetBlocksCount(2);

        if (DisableFileStoreSSDSystemSpaceAccounting) {
            // Accounting disabled: no counter growth => no overflow => the create succeeds.
            TestCreateFileStore(runtime, ++txId, "/MyRoot", smallDescr.DebugString());
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/SmallSystemFS"), {NLs::PathExist});

            TestDropFileStore(runtime, ++txId, "/MyRoot", "SmallSystemFS");
            env.TestWaitNotification(runtime, txId);
        } else {
            // Accounting enabled: the allocated counter overflows and the create is rejected.
            TestCreateFileStore(
                runtime,
                ++txId,
                "/MyRoot",
                smallDescr.DebugString(),
                {NKikimrScheme::StatusPreconditionFailed});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/SmallSystemFS"), {NLs::PathNotExist});
        }

        TestDropFileStore(runtime, ++txId, "/MyRoot", "HugeSystemFS");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(SystemSpaceAccountingSurvivesReenablingTheFlag) {
        // Turning accounting off and back on again must not desynchronize the domain counter.
        // It cannot: the counter is derived state, not persisted, and rebuilt from every FileStoreInfo
        // on each SchemeShard tablet start. Since the flag is (RequireRestart) = true, it can only change
        // across a restart, and that restart re-derives the counter with the new flag applied uniformly
        // to all filestores.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .DisableFileStoreSSDSystemSpaceAccounting(true));
        ui64 txId = 100;

        constexpr ui64 blockSize = 4_KB;
        constexpr ui64 blocks = 4096;

        // Created while accounting is disabled: nothing is added to the domain counter.
        NKikimrSchemeOp::TFileStoreDescription descr;
        auto& config = InitCreateFileStoreConfig("ToggleFS", descr, /*isSystem*/ true);
        config.SetStorageMediaKind(1); // STORAGE_MEDIA_SSD
        config.SetBlocksCount(blocks);

        TestCreateFileStore(runtime, ++txId, "/MyRoot", descr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {SsdSystemAllocated(std::nullopt)});

        // The operator re-enables accounting and restarts SchemeShard.
        runtime.GetAppData().FeatureFlags.SetDisableFileStoreSSDSystemSpaceAccounting(false);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Init re-derives the counter from all persisted FileStores, including the one created
        // while accounting was off, so the counter is correct for the new mode from the start.
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            SsdSystemAllocated(ToString(blocks * blockSize)),
        });

        // The drop therefore subtracts exactly what init added: no underflow.
        TestDropFileStore(runtime, ++txId, "/MyRoot", "ToggleFS");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {SsdSystemAllocated(std::nullopt)});
    }

    Y_UNIT_TEST(SystemSpaceAccountingSaturatesAndRecovers) {
        // The worst-case re-enable scenario: while accounting is disabled, the domain
        // accumulates several filestores whose true ssd_system total does not fit in ui64.
        // Re-enabling the flag and restarting must not wrap the counter or crash-loop the
        // tablet: the init rebuild saturates the counter at Max<ui64>(), drops clamp it back
        // down to zero, and once the huge filestores are gone the domain is fully usable.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .DisableFileStoreSSDSystemSpaceAccounting(true));
        ui64 txId = 100;

        constexpr ui64 blockSize = 4_KB;
        const ui64 hugeBlocks = Max<ui64>() / blockSize; // each filestore is ~2^64 bytes

        // With accounting disabled, several huge filestores can coexist even though their
        // combined size overflows ui64 several times over.
        const TVector<TString> hugeNames = {"HugeFS1", "HugeFS2", "HugeFS3"};
        for (const auto& name : hugeNames) {
            NKikimrSchemeOp::TFileStoreDescription descr;
            auto& config = InitCreateFileStoreConfig(name, descr, /*isSystem*/ true);
            config.SetStorageMediaKind(1); // STORAGE_MEDIA_SSD
            config.SetBlocksCount(hugeBlocks);

            TestCreateFileStore(runtime, ++txId, "/MyRoot", descr.DebugString());
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {SsdSystemAllocated(std::nullopt)});

        // Re-enable accounting and restart SchemeShard: the init rebuild sums all three
        // filestores and saturates at Max<ui64>() instead of silently wrapping around.
        runtime.GetAppData().FeatureFlags.SetDisableFileStoreSSDSystemSpaceAccounting(false);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            SsdSystemAllocated(ToString(Max<ui64>())),
        });

        // While the counter is saturated, new creates fail loudly with a precondition error.
        {
            NKikimrSchemeOp::TFileStoreDescription descr;
            auto& config = InitCreateFileStoreConfig("RejectedFS", descr, /*isSystem*/ true);
            config.SetStorageMediaKind(1);
            config.SetBlocksCount(2);

            TestCreateFileStore(
                runtime,
                ++txId,
                "/MyRoot",
                descr.DebugString(),
                {NKikimrScheme::StatusPreconditionFailed});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/RejectedFS"), {NLs::PathNotExist});
        }

        // Deleting the huge filestores clamps the counter at zero instead of aborting on
        // underflow (the second and third drops subtract more than the counter holds).
        for (const auto& name : hugeNames) {
            TestDropFileStore(runtime, ++txId, "/MyRoot", name);
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {SsdSystemAllocated(std::nullopt)});

        // The domain has fully recovered: a new filestore is created and accounted normally.
        constexpr ui64 blocks = 4096;
        NKikimrSchemeOp::TFileStoreDescription descr;
        auto& config = InitCreateFileStoreConfig("NewFS", descr, /*isSystem*/ true);
        config.SetStorageMediaKind(1);
        config.SetBlocksCount(blocks);

        TestCreateFileStore(runtime, ++txId, "/MyRoot", descr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            SsdSystemAllocated(ToString(blocks * blockSize)),
        });

        TestDropFileStore(runtime, ++txId, "/MyRoot", "NewFS");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {SsdSystemAllocated(std::nullopt)});
    }

    Y_UNIT_TEST(SizeMultiplicationOverflowCannotBypassDomainQuota) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        constexpr ui64 quotaBlocks = 32;
        constexpr ui64 blockSize = 4_KB;
        const ui64 overflowBlocks = Max<ui64>() / blockSize + 1;

        TestUserAttrs(
            runtime,
            ++txId,
            "",
            "MyRoot",
            AlterUserAttrs({
                {"__filestore_space_limit_ssd", ToString(quotaBlocks * blockSize)}
            }));
        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TFileStoreDescription overflowDescr;
        auto& overflowConfig = InitCreateFileStoreConfig("OverflowFS", overflowDescr);
        overflowConfig.SetStorageMediaKind(1);
        overflowConfig.SetBlocksCount(overflowBlocks);

        TestCreateFileStore(
            runtime,
            ++txId,
            "/MyRoot",
            overflowDescr.DebugString(),
            {{NKikimrScheme::StatusInvalidParameter, "FileStore size overflows ui64"}});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/OverflowFS"), {NLs::PathNotExist});

        NKikimrSchemeOp::TFileStoreDescription quotaDescr;
        auto& quotaConfig = InitCreateFileStoreConfig("QuotaFS", quotaDescr);
        quotaConfig.SetStorageMediaKind(1);
        quotaConfig.SetBlocksCount(quotaBlocks);

        TestCreateFileStore(runtime, ++txId, "/MyRoot", quotaDescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/QuotaFS"), {
            NLs::PathExist,
            NLs::Finished,
            [blockSize, quotaBlocks](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                const auto& config = record.GetPathDescription().GetFileStoreDescription().GetConfig();
                UNIT_ASSERT_VALUES_EQUAL(config.GetBlockSize(), blockSize);
                UNIT_ASSERT_VALUES_EQUAL(config.GetBlocksCount(), quotaBlocks);
            }
        });

        TestDropFileStore(runtime, ++txId, "/MyRoot", "QuotaFS");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateFileStoreRejectsZeroBlockSize) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TFileStoreDescription descr;
        auto& config = InitCreateFileStoreConfig("ZeroBlockSizeFS", descr);
        config.SetBlockSize(0);
        config.SetStorageMediaKind(1);
        config.SetBlocksCount(4096);

        TestCreateFileStore(
            runtime,
            ++txId,
            "/MyRoot",
            descr.DebugString(),
            {NKikimrScheme::StatusInvalidParameter});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ZeroBlockSizeFS"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(AlterFileStoreRejectsSizeMultiplicationOverflow) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        constexpr ui64 blockSize = 4_KB;
        constexpr ui64 initialBlocks = 4096;
        const ui64 overflowBlocks = Max<ui64>() / blockSize + 1;

        NKikimrSchemeOp::TFileStoreDescription descr;
        auto& config = InitCreateFileStoreConfig("AlterOverflowFS", descr);
        config.SetStorageMediaKind(1);

        TestCreateFileStore(runtime, ++txId, "/MyRoot", descr.DebugString());
        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TFileStoreDescription alterDescr;
        alterDescr.SetName("AlterOverflowFS");
        auto& alterConfig = *alterDescr.MutableConfig();
        alterConfig.SetVersion(1);
        alterConfig.SetBlocksCount(overflowBlocks);

        TestAlterFileStore(
            runtime,
            ++txId,
            "/MyRoot",
            alterDescr.DebugString(),
            {{NKikimrScheme::StatusInvalidParameter, "FileStore size overflows ui64"}});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/AlterOverflowFS"), {
            NLs::PathExist,
            NLs::Finished,
            [blockSize, initialBlocks](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                const auto& config = record.GetPathDescription().GetFileStoreDescription().GetConfig();
                UNIT_ASSERT_VALUES_EQUAL(config.GetBlockSize(), blockSize);
                UNIT_ASSERT_VALUES_EQUAL(config.GetBlocksCount(), initialBlocks);
            }
        });
    }

}
