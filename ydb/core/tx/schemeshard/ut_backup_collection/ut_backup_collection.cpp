#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#define DEFAULT_NAME_1 "MyCollection1"

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TBackupCollectionTests) {
    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    }

    TString DefaultCollectionSettings() {
        return R"(
            Name: ")" DEFAULT_NAME_1 R"("
            Properties {
                ExplicitEntryList {
                    Entries {
                        Type: ETypeTable
                        Path: "/MyRoot/Table1"
                    }
                }
                Cluster: NULL_VALUE
            }
        )";
    }

    TString CollectionSettings(const TString& name) {
        return Sprintf(R"(
            Name: "%s"
            Properties {
                ExplicitEntryList {
                    Entries {
                        Type: ETypeTable
                        Path: "/MyRoot/Table1"
                    }
                }
                Cluster: NULL_VALUE
            }
        )", name.c_str());
    }

    void PrepareDirs(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(HiddenByFeatureFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot", DefaultCollectionSettings(), {NKikimrScheme::StatusPreconditionFailed});

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathNotExist,
        });

        // must not be there in any case, smoke test
        TestDescribeResult(DescribePath(runtime, "/MyRoot/" DEFAULT_NAME_1), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(DisallowedPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true).EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        {
            TestCreateBackupCollection(runtime, ++txId, "/MyRoot", DefaultCollectionSettings(), {NKikimrScheme::EStatus::StatusSchemeError});

            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathNotExist,
            });

            TestDescribeResult(DescribePath(runtime, "/MyRoot/" DEFAULT_NAME_1), {
                NLs::PathNotExist,
            });
        }

        {
            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups", DefaultCollectionSettings(), {NKikimrScheme::EStatus::StatusSchemeError});

            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathNotExist,
            });

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/" DEFAULT_NAME_1), {
                NLs::PathNotExist,
            });
        }

        {
            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", CollectionSettings("SomePrefix/MyCollection1"), {NKikimrScheme::EStatus::StatusSchemeError});

            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/SomePrefix/MyCollection1"), {
                NLs::PathNotExist,
            });
        }
    }

    Y_UNIT_TEST(CreateAbsolutePath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true).EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot", CollectionSettings("/MyRoot/.backups/collections/" DEFAULT_NAME_1));

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });
    }

    Y_UNIT_TEST(Create) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true).EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });
    }

    Y_UNIT_TEST(CreateTwice) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true).EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings(), {NKikimrScheme::EStatus::StatusSchemeError});

        env.TestWaitNotification(runtime, txId);
    }
} // TBackupCollectionTests
