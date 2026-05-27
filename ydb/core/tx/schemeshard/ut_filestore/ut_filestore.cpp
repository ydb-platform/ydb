#include <ydb/core/protos/filestore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/generic/size_literals.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

auto& InitCreateFileStoreConfig(
    const TString& name,
    NKikimrSchemeOp::TFileStoreDescription& vdescr)
{
    vdescr.SetName(name);

    auto& config = *vdescr.MutableConfig();
    config.SetBlockSize(4_KB);
    config.SetBlocksCount(4096);
    config.SetFileSystemId(name);
    config.SetCloudId("cloud");
    config.SetFolderId("folder");

    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

    return config;
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
