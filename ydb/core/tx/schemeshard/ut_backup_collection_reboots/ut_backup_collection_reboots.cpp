#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#define DEFAULT_NAME_1 "MyCollection1"
#define DEFAULT_NAME_2 "MyCollection2"

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TBackupCollectionWithRebootsTests) {
    TString DefaultCollectionSettings() {
        return R"(
            Name: ")" DEFAULT_NAME_1 R"("

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster {}
        )";
    }

    TString CollectionSettings(const TString& name) {
        return Sprintf(R"(
            Name: "%s"

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster {}
        )", name.c_str());
    }

    Y_UNIT_TEST(CreateWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");

            AsyncCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

            NKikimrSchemeOp::TBackupCollectionDescription properties;
            properties.SetName(DEFAULT_NAME_1);
            auto& entry = *properties.MutableExplicitEntryList()->AddEntries();
            entry.SetType(::NKikimrSchemeOp::TBackupCollectionDescription_TBackupEntry_EType_ETypeTable);
            entry.SetPath("/MyRoot/Table1");
            properties.MutableCluster();

            {
                TInactiveZone inactive(activeZone);
                auto describeResult = DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1);
                TestDescribeResult(describeResult, {NLs::Finished});

                UNIT_ASSERT(describeResult.GetPathDescription().HasBackupCollectionDescription());
                const auto& backupCollectionDescription = describeResult.GetPathDescription().GetBackupCollectionDescription();
                UNIT_ASSERT_VALUES_EQUAL(backupCollectionDescription.GetName(), DEFAULT_NAME_1);
                UNIT_ASSERT_VALUES_EQUAL(backupCollectionDescription.GetVersion(), 0);
                UNIT_ASSERT_VALUES_EQUAL(backupCollectionDescription.DebugString(), properties.DebugString());
            }
        });
    }

    Y_UNIT_TEST(ParallelCreateDrop) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            AsyncCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
            AsyncDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId - 1);


            TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TInactiveZone inactive(activeZone);
                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropWithReboots2) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(DropWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                                   {NLs::PathNotExist});

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedAndDropWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                                   {NLs::PathNotExist});
            }
        });
    }
} // TBackupCollectionWithRebootsTests
