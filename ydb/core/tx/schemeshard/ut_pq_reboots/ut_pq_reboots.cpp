#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TPqGroupTestReboots) {
    using ESts = NKikimrScheme::EStatus;

    const TString GroupConfig = "Name: \"Isolda\""
                                  "TotalGroupCount: 4 "
                                  "PartitionPerTablet: 2 "
                                  "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

    const TString GroupAlter = "Name: \"Isolda\""
                                  "TotalGroupCount: 5 ";

    const TString GroupAlter2 = "Name: \"Isolda\""
                                  "TotalGroupCount: 8 ";

    Y_UNIT_TEST_FLAG(Create, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.Runtime->SetScheduledLimit(400);

            TestCreatePQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                            "Name: \"PQGroup_2\""
                            "TotalGroupCount: 10 "
                            "PartitionPerTablet: 10 "
                            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}"
                            );

            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_2"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(2),
                                    NLs::PQPartitionsInsideDomain(10)});
            }

            TestCreatePQGroup(runtime, ++t.TxId, "/MyRoot/DirA/NotExistingDir",
                            "Name: \"PQGroup_2\""
                            "TotalGroupCount: 10 "
                            "PartitionPerTablet: 10 "
                            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}",
                            {ESts::StatusPathDoesNotExist}
                            );

        });
    }

    Y_UNIT_TEST_FLAG(CreateMultiplePqTablets, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.Runtime->SetScheduledLimit(400);

            TestCreatePQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                            "Name: \"PQGroup_2\""
                            "TotalGroupCount: 2 "
                            "PartitionPerTablet: 1 "
                            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}"
                            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_2"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(2),
                                    NLs::PQPartitionsInsideDomain(2)});
            }
        });
    }

    Y_UNIT_TEST(AlterWithProfileChange) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TPathVersion pqVer;

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::NoChildren});

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");

        env.TestWaitNotification(runtime, txId);

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 10 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}"
                        );

        env.TestWaitNotification(runtime, txId);

        pqVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                                   {NLs::Finished,
                                    NLs::CheckPartCount("PQGroup", 10, 10, 1, 10),
                                    NLs::PQPartitionsInsideDomain(10),
                                    NLs::PathVersionEqual(2)});

        auto numChannels = runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.size();
        {
            auto itTablet = env.GetHiveState()->Tablets.find({TTestTxConfig::SchemeShard, 1});
            UNIT_ASSERT_UNEQUAL(itTablet, env.GetHiveState()->Tablets.end());
            UNIT_ASSERT_VALUES_EQUAL(itTablet->second.Type, TTabletTypes::PersQueue);
            UNIT_ASSERT_VALUES_EQUAL(itTablet->second.BoundChannels.size(), numChannels);

        }

        // increasing number of channels in 0 profile
        runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.emplace_back(runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.back());
        runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.emplace_back(runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.back());
        runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.emplace_back(runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.back());
        runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.emplace_back(runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.back());
        auto numChannelsNew = runtime.GetAppData().ChannelProfiles->Profiles[0].Channels.size();

        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 10 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}",
                         {NKikimrScheme::StatusAccepted}, {pqVer});

        env.TestWaitNotification(runtime, txId);

        pqVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                                   {NLs::Finished,
                                    NLs::CheckPartCount("PQGroup", 10, 10, 1, 10),
                                    NLs::PQPartitionsInsideDomain(10),
                                    NLs::PathVersionEqual(3)});

        {
            auto itTablet = env.GetHiveState()->Tablets.find({TTestTxConfig::SchemeShard, 1});
            UNIT_ASSERT_UNEQUAL(itTablet, env.GetHiveState()->Tablets.end());
            UNIT_ASSERT_VALUES_EQUAL(itTablet->second.Type, TTabletTypes::PersQueue);
            UNIT_ASSERT_VALUES_UNEQUAL(itTablet->second.BoundChannels.size(), numChannels);
            UNIT_ASSERT_VALUES_EQUAL(itTablet->second.BoundChannels.size(), numChannelsNew);
        }
    }

    Y_UNIT_TEST_FLAG(AlterWithReboots, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.Runtime->SetScheduledLimit(400);

            TPathVersion pqVer;
            {
                TInactiveZone inactive(activeZone);
                TestCreatePQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                                "Name: \"PQGroup\""
                                "TotalGroupCount: 10 "
                                "PartitionPerTablet: 10 "
                                "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}"
                                );

                TestAlterPQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                                "Name: \"PQGroup\""
                                "TotalGroupCount: 9 "
                                "PartitionPerTablet: 10 ",
                                 {NKikimrScheme::StatusMultipleModifications});

                TestAlterPQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                                "Name: \"PQGroup\""
                                "TotalGroupCount: 10 "
                                "PartitionPerTablet: 9 ",
                                 {NKikimrScheme::StatusMultipleModifications});

                t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});

                pqVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                                           {NLs::Finished,
                                            NLs::CheckPartCount("PQGroup", 10, 10, 1, 10),
                                            NLs::PQPartitionsInsideDomain(10),
                                            NLs::PathVersionEqual(2)});
            }

            TestAlterPQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                            "Name: \"PQGroup\""
                            "TotalGroupCount: 11 "
                            "PartitionPerTablet: 11 "
                            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}",
                             {NKikimrScheme::StatusAccepted}, {pqVer});

            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestAlterPQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                            "Name: \"PQGroup\""
                            "TotalGroupCount: 12 "
                            "PartitionPerTablet: 12 "
                            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}",
                             {NKikimrScheme::StatusPreconditionFailed}, {pqVer});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                                   {NLs::Finished,
                                    NLs::CheckPartCount("PQGroup", 11, 11, 1, 11),
                                    NLs::PathVersionEqual(3),
                                    NLs::PQPartitionsInsideDomain(11)});
            }
        });
    }

    Y_UNIT_TEST_FLAG(CreateAlter, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.Runtime->SetScheduledLimit(400);

            t.RestoreLogging();

            AsyncCreatePQGroup(runtime, t.TxId++, "/MyRoot/DirA",
                            "Name: \"PQGroup_2\""
                            "TotalGroupCount: 2 "
                            "PartitionPerTablet: 10 "
                            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}"
                            );

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_2"),
                               {NLs::Finished,
                                NLs::PathVersionEqual(2),
                                NLs::PQPartitionsInsideDomain(2)});

            AsyncAlterPQGroup(runtime, t.TxId++, "/MyRoot/DirA",
                            "Name: \"PQGroup_2\""
                            "TotalGroupCount: 8 "
                            "PartitionPerTablet: 10 ");

            TestLs(runtime, "/MyRoot/DirA/PQGroup_2");

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_2"),
                               {NLs::Finished,
                                NLs::PathVersionEqual(3),
                                NLs::PQPartitionsInsideDomain(8)});

            AsyncAlterPQGroup(runtime, t.TxId++, "/MyRoot/DirA",
                            "Name: \"PQGroup_2\""
                            "TotalGroupCount: 20 "
                            "PartitionPerTablet: 12 ");

            TestLs(runtime, "/MyRoot/DirA/PQGroup_2");

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_2"),
                               {NLs::Finished,
                                NLs::PathVersionEqual(4),
                                NLs::PQPartitionsInsideDomain(20)});

            activeZone = false;
        });
    }

    Y_UNIT_TEST_FLAG(CreateDrop, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& /*activeZone*/) {
            t.Runtime->SetScheduledLimit(400);

            t.RestoreLogging();

            TestCreatePQGroup(runtime, t.TxId++, "/MyRoot/DirA", GroupConfig);
            auto status = TestDropPQGroup(runtime, t.TxId++, "/MyRoot/DirA", "Isolda", {ESts::StatusMultipleModifications, ESts::StatusAccepted});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId-2});

            if (status == ESts::StatusAccepted) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Isolda"),
                    {NLs::PathNotExist});
            } else {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Isolda"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(2)});
            }

            TestDropPQGroup(runtime, t.TxId++, "/MyRoot/DirA", "Isolda", {ESts::StatusAccepted, ESts::StatusPathDoesNotExist});
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 1, TTestTxConfig::FakeHiveTablets + 2});

            TestLs(runtime, "/MyRoot/DirA/Isolda", true, NLs::PathNotExist);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(7),
                                NLs::ChildrenCount(0),
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomainOneOf({0, 1, 2, 3}),
                                NLs::PQPartitionsInsideDomain(0)});
        });
    }

    Y_UNIT_TEST_FLAG(CreateDropAbort, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& /*activeZone*/) {
            t.Runtime->SetScheduledLimit(400);

            t.RestoreLogging();
            ui64& txId = t.TxId;

            TestCreatePQGroup(runtime, txId++, "/MyRoot", GroupConfig);
            TestForceDropUnsafe(runtime, txId++, 3);
            t.TestEnv->TestWaitNotification(runtime, {txId-2, txId-1});

            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 1, TTestTxConfig::FakeHiveTablets + 2});

            TestLs(runtime, "/MyRoot/Isolda", true, NLs::PathNotExist);
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::ChildrenCount(1),
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomainOneOf({0, 1, 2, 3}),
                                });
        });
    }

    //RUN: Reboot tablet 72075186233409547 (#44)
    //VERIFY failed:
    //ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.cpp:289
    //Handle(): requirement std::make_pair(msg->CollectGeneration, msg->CollectStep) >= barrier.MakeCollectPair() failed
    /*Y_UNIT_TEST_FLAG(CreateAlterAlterDrop, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.Runtime->SetScheduledLimit(400);

            t.RestoreLogging();
            ui64& txId = t.TxId;

            TestCreatePQGroup(runtime, txId++, "/MyRoot", GroupConfig);
            t.TestEnv->TestWaitNotification(runtime, txId-1);
            TestLs(runtime, "/MyRoot/Isolda", true, NLs::PathExist);

            TestAlterPQGroup(runtime, txId++, "/MyRoot", GroupAlter);
            t.TestEnv->TestWaitNotification(runtime, txId-1);
            TestLs(runtime, "/MyRoot/Isolda", true, NLs::PathExist);

            TestAlterPQGroup(runtime, txId++, "/MyRoot", GroupAlter2);
            t.TestEnv->TestWaitNotification(runtime, txId-1);
            TestLs(runtime, "/MyRoot/Isolda", true, NLs::CheckPartCount("Isolda", 8, 2, 4, 8));

            TestDropPQGroup(runtime, txId++, "/MyRoot", "Isolda", {ESts::StatusAccepted}, NKikimrSchemeOp::EDropWaitChanges);
            t.TestEnv->TestWaitNotification(runtime, txId-1);

            activeZone = false;
            TestLs(runtime, "/MyRoot/Isolda", true, NLs::PathNotExist);
        });
    }*/


    Y_UNIT_TEST_FLAG(CreateAlterDropPqGroupWithReboots, PQConfigTransactionsAtSchemeShard) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnablePQConfigTransactionsAtSchemeShard(PQConfigTransactionsAtSchemeShard);

        t.Run([&](TTestActorRuntime& runtime, bool& /*activeZone*/) {
            t.Runtime->SetScheduledLimit(400);

            using ESts = NKikimrScheme::EStatus;

            t.RestoreLogging();

            TString pqGroupConfig = "Name: \"Isolda\""
                "TotalGroupCount: 4 "
                "PartitionPerTablet: 2 "
                "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

            TString pqGroupAlter = "Name: \"Isolda\""
                "TotalGroupCount: 5 ";

            TString pqGroupAlter2 = "Name: \"Isolda\""
                "TotalGroupCount: 8 ";

            TAutoPtr<IEventHandle> handle;
            ui64& txId = t.TxId;

            // Create, Alter, Alter
            TestCreatePQGroup(runtime, txId++, "/MyRoot", pqGroupConfig);
            t.TestEnv->TestWaitNotification(runtime, txId-1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Isolda"),
                               {NLs::PathExist,
                                NLs::Finished,
                                NLs::PathVersionEqual(2),
                                NLs::PQPartitionsInsideDomain(4)});

            TestAlterPQGroup(runtime, txId++, "/MyRoot", pqGroupAlter);
            t.TestEnv->TestWaitNotification(runtime, txId-1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Isolda"),
                               {NLs::PathExist,
                                NLs::Finished,
                                NLs::PathVersionEqual(3),
                                NLs::PQPartitionsInsideDomain(5)});

            TestAlterPQGroup(runtime, txId++, "/MyRoot", pqGroupAlter2);
            t.TestEnv->TestWaitNotification(runtime, txId-1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Isolda", true),
                               {NLs::PathExist,
                                NLs::Finished,
                                NLs::PathVersionEqual(4),
                                NLs::CheckPartCount("Isolda", 8, 2, 4, 8),
                                NLs::PQPartitionsInsideDomain(8)});

            TestDropPQGroup(runtime, txId++, "/MyRoot", "Isolda", {ESts::StatusAccepted});
            t.TestEnv->TestWaitNotification(runtime, txId-1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Isolda"),
                               {NLs::PathNotExist});

            TestDescribeResult(DescribePath(runtime, "/MyRoot", true),
                               {NLs::PathExist,
                                NLs::PQPartitionsInsideDomain(0)});

        }, true);
    }

    Y_UNIT_TEST(CreateWithIntermediateDirs) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z"
            TotalGroupCount: 10
            PartitionPerTablet: 10
            PQTabletConfig: { PartitionConfig { LifetimeSeconds : 10 } }
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/wr0ng n@me"
        )";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreatePQGroup(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop([](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
            AsyncCreatePQGroup(runtime, txId, root, R"(
                Name: "x/y/z"
                TotalGroupCount: 10
                PartitionPerTablet: 10
                PQTabletConfig: { PartitionConfig { LifetimeSeconds : 10 } }
            )");
        });
    }
}
