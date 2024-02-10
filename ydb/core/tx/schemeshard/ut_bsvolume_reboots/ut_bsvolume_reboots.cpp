#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

auto& InitCreateVolumeConfig(
    const TString& name,
    NKikimrSchemeOp::TBlockStoreVolumeDescription& vdescr)
{
    vdescr.SetName(name);
    auto& vc = *vdescr.MutableVolumeConfig();
    vc.SetBlockSize(4096);
    vc.SetDiskId("foo");
    vc.AddPartitions()->SetBlockCount(16);
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
    return vc;
}

void InitAlterVolumeConfig(NKikimrBlockStore::TVolumeConfig& vc)
{
    vc.Clear();
    vc.SetVersion(1);
    vc.AddPartitions()->SetBlockCount(32);
    vc.AddPartitions()->SetBlockCount(32);
}

}   // namespace

Y_UNIT_TEST_SUITE(TBSVWithReboots) {
    Y_UNIT_TEST(AlterAssignDrop) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            auto& vc = InitCreateVolumeConfig("BSVolume", vdescr);

            {
                TInactiveZone inactive(activeZone);
                t.RestoreLogging();
                TestCreateBlockStoreVolume(runtime, ++t.TxId, "/MyRoot", vdescr.DebugString());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            InitAlterVolumeConfig(vc);
            AsyncAlterBlockStoreVolume(runtime, ++t.TxId, "/MyRoot", vdescr.DebugString());

            AsyncAssignBlockStoreVolume(runtime, ++t.TxId, "/MyRoot", "BSVolume", "Owner123");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropBlockStoreVolume(runtime, ++t.TxId, "/MyRoot", "BSVolume", 0,
                                     {NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAccepted});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->TestWaitNotification(runtime, t.TxId - 2); // wait Alter

            {
                TInactiveZone inactive(activeZone);
                TestDropBlockStoreVolume(runtime, ++t.TxId, "/MyRoot", "BSVolume", 0,
                                         {NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusAccepted});
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                                   {NLs::PathNotExist});
                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+5));
            }
        });
    }

    Y_UNIT_TEST(Create) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            InitCreateVolumeConfig("BSVolume_1", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/BSVolume_1", false, NLs::Finished);
        });
    }

    Y_UNIT_TEST(CreateAlter) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            auto& vc = InitCreateVolumeConfig("BSVolume_2", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            TestLs(runtime, "/MyRoot/DirA/BSVolume_2", false, NLs::Finished);

            InitAlterVolumeConfig(vc);
            TestAlterBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/BSVolume_2"),
                               {NLs::PathExist,
                                NLs::Finished,
                                NLs::PathVersionEqual(3)});
        });
    }

    Y_UNIT_TEST(CreateAlterNoVersion) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            auto& vc = InitCreateVolumeConfig("BSVolume_2", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_2", false, NLs::Finished);

            InitAlterVolumeConfig(vc);
            TestAlterBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/BSVolume_2", false, NLs::PathExist);
        });
    }

    Y_UNIT_TEST(CreateDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            InitCreateVolumeConfig("BSVolume_3", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_3", false, NLs::Finished);

            TestDropBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_3");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/BSVolume_3", false, NLs::PathNotExist);
        });
    }

    Y_UNIT_TEST(CreateAssignAlterIsAllowed) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            auto& vc = InitCreateVolumeConfig("BSVolume_4", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::Finished);

            TestAssignBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4", "Owner123");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::CheckMountToken("BSVolume_4", "Owner123"));

            InitAlterVolumeConfig(vc);
            TestAlterBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::PathExist);
        });
    }

    Y_UNIT_TEST(CreateAssignAlterIsAllowedNoVersion) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            auto& vc = InitCreateVolumeConfig("BSVolume_4", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::Finished);

            TestAssignBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4", "Owner123");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::CheckMountToken("BSVolume_4", "Owner123"));

            InitAlterVolumeConfig(vc);
            TestAlterBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::PathExist);
        });
    }

    Y_UNIT_TEST(CreateAssignDropIsAllowed) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            InitCreateVolumeConfig("BSVolume_4", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::Finished);

            TestAssignBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4", "Owner123");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::CheckMountToken("BSVolume_4", "Owner123"));

            TestDropBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4");

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::PathNotExist);
        });
    }

    Y_UNIT_TEST(CreateAssignUnassignDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            InitCreateVolumeConfig("BSVolume_4", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::Finished);

            TestAssignBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4", "Owner123");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::CheckMountToken("BSVolume_4", "Owner123"));

            TestAssignBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4", "");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::CheckMountToken("BSVolume_4", ""));

            TestDropBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::PathNotExist);
        });
    }

    Y_UNIT_TEST(SimultaneousCreateDropNbs) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\""
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-1\""
                                    "  Kind: \"storage-pool-number-1\""
                                    "}"
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-2\""
                                    "  Kind: \"storage-pool-number-2\""
                                    "}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            auto& vc = InitCreateVolumeConfig("BSVolume", vdescr);
            vc.ClearExplicitChannelProfiles();
            vc.AddExplicitChannelProfiles()->SetPoolKind("storage-pool-number-1");
            vc.AddExplicitChannelProfiles()->SetPoolKind("storage-pool-number-1");
            vc.AddExplicitChannelProfiles()->SetPoolKind("storage-pool-number-1");
            vc.AddExplicitChannelProfiles()->SetPoolKind("storage-pool-number-2");
            TestCreateBlockStoreVolume(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", vdescr.DebugString());

            TestForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 3));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(7),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
            }
        });
    }

    Y_UNIT_TEST(CreateWithIntermediateDirs) {
        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        InitCreateVolumeConfig("Valid/x/y/z", vdescr);
        const auto validScheme = vdescr.DebugString();
        vdescr.Clear();
        InitCreateVolumeConfig("Invalid/wr0ng n@me", vdescr);
        const auto invalidScheme = vdescr.DebugString();
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateBlockStoreVolume(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop([](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            InitCreateVolumeConfig("x/y/z", vdescr);
            AsyncCreateBlockStoreVolume(runtime, txId, root, vdescr.DebugString());
        });
    }


    Y_UNIT_TEST(CreateAssignWithVersion) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            Y_UNUSED(activeZone);

            t.RestoreLogging();

            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            InitCreateVolumeConfig("BSVolume_4", vdescr);
            TestCreateBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::Finished);

            TestAssignBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4", "Owner123", 0);
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::CheckMountToken("BSVolume_4", "Owner123"));

            TestAssignBlockStoreVolume(runtime, t.TxId++, "/MyRoot/DirA", "BSVolume_4", "Owner124", 1);
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/BSVolume_4", false, NLs::CheckMountToken("BSVolume_4", "Owner124"));
        });
    }
}
