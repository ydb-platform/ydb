#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

auto& InitCreateFileStoreConfig(
    const TString& name,
    NKikimrSchemeOp::TFileStoreDescription& vdescr)
{
    vdescr.SetName(name);
    auto& vc = *vdescr.MutableConfig();
    vc.SetBlockSize(4096);
    vc.SetBlocksCount(4096);
    vc.SetFileSystemId(name);
    vc.SetCloudId("cloud");
    vc.SetFolderId("folder");

    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

    return vc;
}

void InitAlterFileStoreConfig(NKikimrFileStore::TConfig& vc, bool channels = false)
{
    vc.Clear();
    vc.SetVersion(1);
    vc.SetCloudId("baz");
    vc.SetFolderId("bar");

    if (channels) {
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TFileStoreWithReboots) {
    Y_UNIT_TEST(Create) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TFileStoreDescription vdescr;
            InitCreateFileStoreConfig("FS_1", vdescr);
            TestCreateFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/FS_1", false, NLs::Finished);
        });
    }

    Y_UNIT_TEST(CreateAlter) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TFileStoreDescription vdescr;
            auto& vc = InitCreateFileStoreConfig("FS_2", vdescr);
            TestCreateFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            TestLs(runtime, "/MyRoot/DirA/FS_2", false, NLs::Finished);

            InitAlterFileStoreConfig(vc);
            TestAlterFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/FS_2"),
                               {NLs::PathExist,
                                NLs::Finished,
                                NLs::PathVersionEqual(3)});
        });
    }

    Y_UNIT_TEST(CreateAlterNoVersion) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TFileStoreDescription vdescr;
            auto& vc = InitCreateFileStoreConfig("FS_2", vdescr);
            TestCreateFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/FS_2", false, NLs::Finished);

            InitAlterFileStoreConfig(vc);
            TestAlterFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/FS_2", false, NLs::PathExist);
        });
    }

    Y_UNIT_TEST(CreateDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TFileStoreDescription vdescr;
            InitCreateFileStoreConfig("FS_3", vdescr);
            TestCreateFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestLs(runtime, "/MyRoot/DirA/FS_3", false, NLs::Finished);

            TestDropFileStore(runtime, t.TxId++, "/MyRoot/DirA", "FS_3");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/DirA/FS_3", false, NLs::PathNotExist);
        });
    }


    Y_UNIT_TEST(CreateWithIntermediateDirs) {
        NKikimrSchemeOp::TFileStoreDescription vdescr;
        InitCreateFileStoreConfig("Valid/x/y/z", vdescr);
        const auto validScheme = vdescr.DebugString();
        vdescr.Clear();
        InitCreateFileStoreConfig("Invalid/wr0ng n@me", vdescr);
        const auto invalidScheme = vdescr.DebugString();
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateFileStore(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop(
            [](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
                NKikimrSchemeOp::TFileStoreDescription vdescr;
                InitCreateFileStoreConfig("x/y/z", vdescr);
                AsyncCreateFileStore(runtime, txId, root, vdescr.DebugString());
            });
    }

    Y_UNIT_TEST(SimultaneousCreateDropNfs) { //+
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

            NKikimrSchemeOp::TFileStoreDescription vdescr;
            InitCreateFileStoreConfig("FS_1", vdescr);
            TestCreateFileStore(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", vdescr.DebugString());

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

    Y_UNIT_TEST(AlterAssignDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            NKikimrSchemeOp::TFileStoreDescription vdescr;
            auto& vc = InitCreateFileStoreConfig("FS", vdescr);

            {
                TInactiveZone inactive(activeZone);
                t.RestoreLogging();
                TestCreateFileStore(runtime, ++t.TxId, "/MyRoot", vdescr.DebugString());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            InitAlterFileStoreConfig(vc);
            AsyncAlterFileStore(runtime, ++t.TxId, "/MyRoot", vdescr.DebugString());
            
            t.TestEnv->ReliablePropose(runtime, DropFileStoreRequest(++t.TxId, "/MyRoot", "FS"), {NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAccepted});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->TestWaitNotification(runtime, t.TxId - 1); // wait Alter

            {
                TInactiveZone inactive(activeZone);
                TestDropFileStore(runtime, ++t.TxId, "/MyRoot", "FS", {NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusAccepted});
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/FS"),
                                   {NLs::PathNotExist});
                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+5));
            }
        });
    }

    Y_UNIT_TEST(CreateAlterChannels) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.RestoreLogging();

            NKikimrSchemeOp::TFileStoreDescription vdescr;
            auto& vc = InitCreateFileStoreConfig("FS_2", vdescr);
            TestCreateFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            TestLs(runtime, "/MyRoot/DirA/FS_2", false, NLs::Finished);

            InitAlterFileStoreConfig(vc, true);
            TestAlterFileStore(runtime, t.TxId++, "/MyRoot/DirA", vdescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            activeZone = false;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/FS_2"),
                               {NLs::PathExist,
                                NLs::Finished,
                                NLs::PathVersionEqual(3)});
        });
    }
}
