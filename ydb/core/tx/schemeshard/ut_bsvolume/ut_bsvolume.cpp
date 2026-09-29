#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

auto& InitVolumeDirectConfig(
    const TString& name,
    NKikimrSchemeOp::TBlockStoreVolumeDescription& vdescr)
{
    vdescr.SetName(name);
    auto& vc = *vdescr.MutableVolumeConfig();
    vc.SetTabletVersion(3);
    vc.SetBlockSize(4096);
    vc.AddPartitions()->SetBlockCount(16);
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
    return vc;
}

NKikimrSchemeOp::TBlockStoreVolumeDescription DescribeVolume(
    TTestActorRuntime& runtime,
    const TString& path)
{
    return DescribePath(runtime, path)
        .GetPathDescription()
        .GetBlockStoreVolumeDescription();
}

}   // namespace

Y_UNIT_TEST_SUITE(TBSV) {
    Y_UNIT_TEST(CleanupDroppedVolumesOnRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        runtime.GetAppData().DisableSchemeShardCleanupOnDropForTest = true;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.AddPartitions()->SetBlockCount(16);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(2)});

        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        env.TestWaitNotification(runtime, txId);

        expectedDomainPaths -= 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(0)});

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::PathNotExist});

        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(ShardsNotLeftInShardsToDelete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.AddPartitions()->SetBlockCount(16);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(2)});

        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        env.TestWaitNotification(runtime, txId);

        expectedDomainPaths -= 1;

        env.TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

        {
            // Read user table schema from new shard;
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, R"(
                                    (
                                        (let range '('('ShardIdx (Uint64 '0) (Void))))
                                        (let select '('ShardIdx))
                                        (let result (SelectRange 'ShardsToDelete range select '()))
                                        (return (AsList
                                            (SetResult 'ShardsToDelete result)
                                        ))
                                    )
                )", result, err);
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            Cerr << result << Endl;
            // Bad: Value { Struct { Optional { Struct {
            //          List { Struct { Optional { Uint64: 1 } } }
            //          List { Struct { Optional { Uint64: 2 } } }
            //      } Struct { Bool: false } } } } }
            // Good: Value { Struct { Optional { Struct { } Struct { Bool: false } } } } }
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStruct(0).GetOptional().GetStruct(0).ListSize(), 0);
        }
    }

    Y_UNIT_TEST(ShouldLimitBlockStoreVolumeDropRate) {
        struct TMockTimeProvider : public ITimeProvider
        {
            TInstant Time;

            TInstant Now() override
            {
                return Time;
            }
        };

        struct TTimeProviderMocker
        {
            TIntrusivePtr<ITimeProvider> OriginalTimeProvider;

            TTimeProviderMocker(TIntrusivePtr<ITimeProvider> timeProvider)
            {
                OriginalTimeProvider = NKikimr::TAppData::TimeProvider;
                NKikimr::TAppData::TimeProvider = timeProvider;
            }

            ~TTimeProviderMocker()
            {
                NKikimr::TAppData::TimeProvider = OriginalTimeProvider;
            }
        };

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        auto root = "/MyRoot";
        auto name = "BSVolume";
        auto throttled = NKikimrScheme::StatusNotAvailable;

        TestUserAttrs(runtime, ++txId, "", "MyRoot",
            AlterUserAttrs(
                {{"drop_blockstore_volume_rate_limiter_rate", "1.0"}}
            )
        );
        env.TestWaitNotification(runtime, txId);

        TestUserAttrs(runtime, ++txId, "", "MyRoot",
            AlterUserAttrs(
                {{"drop_blockstore_volume_rate_limiter_capacity", "10.0"}}
            )
        );
        env.TestWaitNotification(runtime, txId);

        TIntrusivePtr<TMockTimeProvider> mockTimeProvider =
            new TMockTimeProvider();
        TTimeProviderMocker mocker(mockTimeProvider);

        NKikimrSchemeOp::TBlockStoreVolumeDescription descr;
        descr.SetName(name);
        auto& c = *descr.MutableVolumeConfig();
        c.SetBlockSize(4096);
        for (int i = 0; i < 4; ++i) {
            c.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        }
        c.AddPartitions()->SetBlockCount(16);

        // consume all initial budget
        for (int i = 0; i < 10; ++i) {
            TestCreateBlockStoreVolume(runtime, ++txId, root, descr.DebugString());
            env.TestWaitNotification(runtime, txId);
            TestDropBlockStoreVolume(runtime, ++txId, root, name);
            env.TestWaitNotification(runtime, txId);
        }

        TestCreateBlockStoreVolume(runtime, ++txId, root, descr.DebugString());
        env.TestWaitNotification(runtime, txId);
        // drop should be throttled
        TestDropBlockStoreVolume(runtime, ++txId, root, name, 0, {throttled});
        env.TestWaitNotification(runtime, txId);

        mockTimeProvider->Time = TInstant::Seconds(1);

        // after 1 second, we should be able to drop one volume
        TestDropBlockStoreVolume(runtime, ++txId, root, name);
        env.TestWaitNotification(runtime, txId);

        TestCreateBlockStoreVolume(runtime, ++txId, root, descr.DebugString());
        env.TestWaitNotification(runtime, txId);
        // next drop should be throttled
        TestDropBlockStoreVolume(runtime, ++txId, root, name, 0, {throttled});
        env.TestWaitNotification(runtime, txId);

        // turn off rate limiter
        TestUserAttrs(runtime, ++txId, "", "MyRoot",
            AlterUserAttrs(
                {{"drop_blockstore_volume_rate_limiter_rate", "0.0"}}
            )
        );
        env.TestWaitNotification(runtime, txId);

        TestDropBlockStoreVolume(runtime, ++txId, root, name);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateBlockStoreVolumeDirect) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolumeDirect");
        auto& vc = *vdescr.MutableVolumeConfig();

        // Set TabletVersion = 3 for NBS 2.0 direct volume
        vc.SetTabletVersion(3);
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);

        // Add channel profiles for partition (only 2 channels)
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

        // Volume channel profiles are accepted and ignored
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

        // Test that schema operation is accepted and recognizes TabletVersion=3
        AsyncCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());

        // Wait for the operation to complete
        env.TestWaitNotification(runtime, txId);

        // With TabletVersion=3 there is no volume tablet:
        // the only shard is BlockStorePartitionDirect
        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolumeDirect"),
                           {NLs::PathExist, NLs::Finished});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolumeDirect", true, true, true),
                           {NLs::PathExist, NLs::ShardsInsideDomain(1)});

        ui32 volumeDirectCount = 0;
        ui32 partitionDirectCount = 0;
        ui64 partitionTabletId = 0;
        for (const auto& kv : env.GetHiveState()->Tablets) {
            if (kv.second.Type == TTabletTypes::BlockStoreVolumeDirect) {
                ++volumeDirectCount;
            } else if (kv.second.Type == TTabletTypes::BlockStorePartitionDirect) {
                ++partitionDirectCount;
                partitionTabletId = kv.second.TabletId;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(volumeDirectCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(partitionDirectCount, 1);

        const auto volume = DescribeVolume(runtime, "/MyRoot/BSVolumeDirect");
        UNIT_ASSERT_VALUES_EQUAL(volume.GetVolumeTabletId(), partitionTabletId);
        UNIT_ASSERT_VALUES_EQUAL(volume.PartitionsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetPartitions(0).GetTabletId(), partitionTabletId);
    }

    Y_UNIT_TEST(AlterBlockStoreVolumeDirect) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        auto& vc = InitVolumeDirectConfig("BSVolumeDirect", vdescr);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        const auto volumeTabletId =
            DescribeVolume(runtime, "/MyRoot/BSVolumeDirect").GetVolumeTabletId();
        UNIT_ASSERT_VALUES_UNEQUAL(volumeTabletId, 0);

        vc.Clear();
        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(32);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolumeDirect"),
                           {NLs::Finished, NLs::ShardsInsideDomain(1)});

        const auto volume = DescribeVolume(runtime, "/MyRoot/BSVolumeDirect");
        UNIT_ASSERT_VALUES_EQUAL(volume.GetVolumeTabletId(), volumeTabletId);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetVolumeConfig().GetVersion(), 2);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetVolumeConfig().PartitionsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetVolumeConfig().GetPartitions(0).GetBlockCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(volume.PartitionsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetPartitions(0).GetTabletId(), volumeTabletId);
    }

    Y_UNIT_TEST(DropBlockStoreVolumeDirect) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        InitVolumeDirectConfig("BSVolumeDirect", vdescr);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolumeDirect"),
                           {NLs::Finished, NLs::ShardsInsideDomain(1)});

        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolumeDirect");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolumeDirect"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(RebootKeepsVolumeTabletIdOfBlockStoreVolumeDirect) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        auto& vc = InitVolumeDirectConfig("BSVolumeDirect", vdescr);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        const auto volumeTabletId =
            DescribeVolume(runtime, "/MyRoot/BSVolumeDirect").GetVolumeTabletId();
        UNIT_ASSERT_VALUES_UNEQUAL(volumeTabletId, 0);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        UNIT_ASSERT_VALUES_EQUAL(
            DescribeVolume(runtime, "/MyRoot/BSVolumeDirect").GetVolumeTabletId(),
            volumeTabletId);

        // the restored volume shard is usable by alter and drop
        vc.Clear();
        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(32);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(
            DescribeVolume(runtime, "/MyRoot/BSVolumeDirect").GetVolumeTabletId(),
            volumeTabletId);

        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolumeDirect");
        env.TestWaitNotification(runtime, txId);

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::ShardsInsideDomain(0)});
    }
}
