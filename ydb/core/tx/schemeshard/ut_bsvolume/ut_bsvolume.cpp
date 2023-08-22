#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TBSV) {
    Y_UNIT_TEST(CleanupDroppedVolumesOnRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

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

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});

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

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        env.TestWaitNotification(runtime, txId);

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
}
