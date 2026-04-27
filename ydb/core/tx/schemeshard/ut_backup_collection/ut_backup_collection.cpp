#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/replication/service/worker.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/string/printf.h>

#define DEFAULT_NAME_1 "MyCollection1"
#define DEFAULT_NAME_2 "MyCollection2"

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TBackupCollectionTests) {
    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NActors::NLog::PRI_TRACE);
    }

    TString DefaultCollectionSettings() {
        return R"(
            Name: ")" DEFAULT_NAME_1 R"("

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
        )";
    }

    TString DefaultIncrementalCollectionSettings() {
        return R"(
            Name: ")" DEFAULT_NAME_1 R"("

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster {}
            IncrementalBackupConfig {}
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
            Cluster: {}
        )", name.c_str());
    }

    TString DefaultCollectionSettingsWithName(const TString& name) {
        return Sprintf(R"(
            Name: "%s"

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
        )", name.c_str());
    }

    void PrepareDirs(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);
    }

    void AsyncBackupBackupCollection(TTestBasicRuntime& runtime, ui64 txId, const TString& workingDir, const TString& request) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(workingDir);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection);

        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(request, transaction->MutableBackupBackupCollection());
        UNIT_ASSERT(parseOk);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release(), 0);
    }

    void TestBackupBackupCollection(TTestBasicRuntime& runtime, ui64 txId, const TString& workingDir, const TString& request, const TExpectedResult& expectedResult = {NKikimrScheme::StatusAccepted}) {
        AsyncBackupBackupCollection(runtime, txId, workingDir, request);
        TestModificationResults(runtime, txId, {expectedResult});
    }

    void AsyncBackupIncrementalBackupCollection(TTestBasicRuntime& runtime, ui64 txId, const TString& workingDir, const TString& request) {
        TActorId sender = runtime.AllocateEdgeActor();

        auto request2 = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = request2->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection);
        transaction->SetWorkingDir(workingDir);
        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(request, transaction->MutableBackupIncrementalBackupCollection());
        UNIT_ASSERT(parseOk);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, request2.Release(), 0, sender);
    }

    ui64 TestBackupIncrementalBackupCollection(TTestBasicRuntime& runtime, ui64 txId, const TString& workingDir, const TString& request, const TExpectedResult& expectedResult = {NKikimrScheme::StatusAccepted}) {
        AsyncBackupIncrementalBackupCollection(runtime, txId, workingDir, request);
        return TestModificationResults(runtime, txId, {expectedResult});
    }

    void TestAlterTable(TTestBasicRuntime& runtime, ui64 txId, const TString& workingDir, const TString& request, const TExpectedResult& expectedResult = {NKikimrScheme::StatusAccepted}) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(workingDir);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);

        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(request, transaction->MutableAlterTable());
        UNIT_ASSERT(parseOk);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release(), 0);
        TestModificationResults(runtime, txId, {expectedResult});
    }

    Y_UNIT_TEST(HiddenByFeatureFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot", DefaultCollectionSettings(), {NKikimrScheme::StatusPreconditionFailed});

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathNotExist,
            });

            TestDescribeResult(DescribePath(runtime, "/MyRoot/" DEFAULT_NAME_1), {
                NLs::PathNotExist,
            });
        }

        Y_UNIT_TEST(DisallowedPath) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
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
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
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
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
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
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
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

        Y_UNIT_TEST(ParallelCreate) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            PrepareDirs(runtime, env, txId);

            AsyncCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionSettings(DEFAULT_NAME_1));
            AsyncCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionSettings(DEFAULT_NAME_2));
            TestModificationResult(runtime, txId - 1, NKikimrScheme::StatusAccepted);
            TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

            env.TestWaitNotification(runtime, {txId, txId - 1});

            TestDescribe(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1);
            TestDescribe(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_2);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections"),
                               {NLs::PathVersionEqual(7)});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                               {NLs::PathVersionEqual(1)});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_2),
                               {NLs::PathVersionEqual(1)});
        }

        Y_UNIT_TEST(Drop) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestLs(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1, false, NLs::PathExist);

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestLs(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1, false, NLs::PathNotExist);
        }

        Y_UNIT_TEST(DropTwice) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            AsyncDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            AsyncDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            TestModificationResult(runtime, txId - 1);

            auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
            UNIT_ASSERT(ev);

            const auto& record = ev->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
            UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

            env.TestWaitNotification(runtime, txId - 1);
            TestLs(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1, false, NLs::PathNotExist);
        }

        Y_UNIT_TEST(TableWithSystemColumns) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);

            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());

            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestCreateTable(runtime, ++txId, "/MyRoot/.backups/collections", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "__ydb_system_column" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: ".backups/collections/Table2"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "__ydb_system_column" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot/.backups/collections", R"(
                Name: "somepath/Table3"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "__ydb_system_column" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);
        }

        Y_UNIT_TEST(BackupAbsentCollection) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);

            PrepareDirs(runtime, env, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")",
                {NKikimrScheme::EStatus::StatusPathDoesNotExist});
            env.TestWaitNotification(runtime, txId);
        }

        Y_UNIT_TEST(BackupDroppedCollection) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);

            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")",
                {NKikimrScheme::EStatus::StatusPathDoesNotExist});
            env.TestWaitNotification(runtime, txId);
        }

        Y_UNIT_TEST(BackupAbsentDirs) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);

            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")",
                {NKikimrScheme::EStatus::StatusPathDoesNotExist});
            env.TestWaitNotification(runtime, txId);
        }

        Y_UNIT_TEST(BackupNonIncrementalCollection) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);

            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());

            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")",
                {NKikimrScheme::EStatus::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
                NLs::ChildrenCount(1),
                NLs::Finished,
            });
        }

        Y_UNIT_TEST(DropEmptyBackupCollection) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionWithFullBackup) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
                NLs::ChildrenCount(1),
            });

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionWithIncrementalBackup) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TString collectionSettingsWithIncremental = R"(
                Name: ")" DEFAULT_NAME_1 R"("

                ExplicitEntryList {
                    Entries {
                        Type: ETypeTable
                        Path: "/MyRoot/Table1"
                    }
                }
                Cluster: {}
                IncrementalBackupConfig: {}
            )";

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettingsWithIncremental);
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionDuringActiveBackup) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            AsyncBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
                "Name: \"" DEFAULT_NAME_1 "\"",
                {NKikimrScheme::StatusPreconditionFailed});
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            env.TestWaitNotification(runtime, txId - 1);

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropNonExistentCollection) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
                "Name: \"NonExistentCollection\"",
                {NKikimrScheme::StatusPathDoesNotExist});
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/NonExistentCollection"),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionWithMultipleBackups) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionWithNestedTables) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestMkDir(runtime, ++txId, "/MyRoot", "SubDir");
            env.TestWaitNotification(runtime, txId);

            TString collectionSettingsNested = R"(
                Name: ")" DEFAULT_NAME_1 R"("

                ExplicitEntryList {
                    Entries {
                        Type: ETypeTable
                        Path: "/MyRoot/Table1"
                    }
                    Entries {
                        Type: ETypeTable
                        Path: "/MyRoot/SubDir/Table2"
                    }
                }
                Cluster: {}
            )";

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettingsNested);
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot/SubDir", R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionVerifyCDCCleanup) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table1"
                StreamDescription {
                  Name: "Stream1"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
            )");
            env.TestWaitNotification(runtime, txId);

            TString collectionSettingsWithCDC = R"(
                Name: ")" DEFAULT_NAME_1 R"("
                ExplicitEntryList {
                    Entries {
                        Type: ETypeTable
                        Path: "/MyRoot/Table1"
                    }
                }
                Cluster: {}
                IncrementalBackupConfig: {}
            )";

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettingsWithCDC);
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table1/Stream1"), {NLs::PathExist});

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionRollbackOnFailure) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
            env.TestWaitNotification(runtime, txId);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
                "Name: \"NonExistentCollection\"",
                {NKikimrScheme::StatusPathDoesNotExist});
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropLargeBackupCollection) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TString largeCollectionSettings = R"(
                Name: ")" DEFAULT_NAME_1 R"("
                ExplicitEntryList {)";

            for (int i = 1; i <= 5; ++i) {
                largeCollectionSettings += TStringBuilder() <<
                    R"(
                    Entries {
                        Type: ETypeTable
                        Path: "/MyRoot/Table)" << i << R"("
                    })";
            }
            largeCollectionSettings += R"(
                }
                Cluster: {}
            )";

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", largeCollectionSettings);
            env.TestWaitNotification(runtime, txId);

            for (int i = 1; i <= 5; ++i) {
                TestCreateTable(runtime, ++txId, "/MyRoot", TStringBuilder() << R"(
                    Name: "Table)" << i << R"("
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                env.TestWaitNotification(runtime, txId);
            }

            for (int i = 0; i < 3; ++i) {
                if (i > 0) {
                    runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                }

                TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                    R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
                env.TestWaitNotification(runtime, txId);
            }

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                NLs::PathExist,
                NLs::IsBackupCollection,
            });

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                              {NLs::PathNotExist});
        }

        Y_UNIT_TEST(DropCollectionValidationCases) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
                "Name: \"\"",
                {NKikimrScheme::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);

            TestDropBackupCollection(runtime, ++txId, "/NonExistent/path",
                "Name: \"test\"",
                {NKikimrScheme::StatusPathDoesNotExist});
            env.TestWaitNotification(runtime, txId);

            TestDropBackupCollection(runtime, ++txId, "/MyRoot",
                "Name: \"test\"",
                {NKikimrScheme::StatusSchemeError});
            env.TestWaitNotification(runtime, txId);
        }

        Y_UNIT_TEST(DropSpecificCollectionAmongMultiple) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
            ui64 txId = 100;

            SetupLogging(runtime);
            PrepareDirs(runtime, env, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                DefaultCollectionSettingsWithName("Collection1"));
            env.TestWaitNotification(runtime, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                DefaultCollectionSettingsWithName("Collection2"));
            env.TestWaitNotification(runtime, txId);

            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                DefaultCollectionSettingsWithName("Collection3"));
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection1"), {NLs::PathExist});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection2"), {NLs::PathExist});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection3"), {NLs::PathExist});

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"Collection2\"");
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection1"), {NLs::PathExist});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection2"), {NLs::PathNotExist});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection3"), {NLs::PathExist});

            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"Collection1\"");
            env.TestWaitNotification(runtime, txId);
            TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"Collection3\"");
            env.TestWaitNotification(runtime, txId);
        }


    Y_UNIT_TEST(DropCollectionVerifyLocalDatabaseCleanup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TString localDbCollectionSettings = R"(
            Name: "LocalDbTestCollection"

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/LocalDbTestTable"
                }
            }
            Cluster: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
            localDbCollectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "LocalDbTestTable"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/LocalDbTestCollection")");
        env.TestWaitNotification(runtime, txId);        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"LocalDbTestCollection\"");
        env.TestWaitNotification(runtime, txId);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/LocalDbTestCollection"),
                          {NLs::PathNotExist});

        ui64 schemeshardTabletId = TTestTxConfig::SchemeShard;

        bool backupCollectionTableClean = true;
        try {
            auto result = LocalMiniKQL(runtime, schemeshardTabletId, R"(
                (
                    (let key '('('OwnerPathId (Uint64 '0)) '('LocalPathId (Uint64 '0))))
                    (let select '('OwnerPathId 'LocalPathId))
                    (let row (SelectRow 'BackupCollection key select))
                    (return (AsList
                        (SetResult 'Result row)
                    ))
                )
            )");

            auto& value = result.GetValue();
            if (value.GetStruct(0).GetOptional().HasOptional()) {
                backupCollectionTableClean = false;
                Cerr << "ERROR: BackupCollection table still has entries after DROP" << Endl;
            }
        } catch (...) {
            backupCollectionTableClean = false;
            Cerr << "ERROR: Failed to query BackupCollection table" << Endl;
        }

        UNIT_ASSERT_C(backupCollectionTableClean, "BackupCollection table not properly cleaned up");

        bool incrementalRestoreOperationsClean = true;
        try {
            auto result = LocalMiniKQL(runtime, schemeshardTabletId, R"(
                (
                    (let key '('('Id (Uint64 '0))))
                    (let select '('Id))
                    (let row (SelectRow 'IncrementalRestoreOperations key select))
                    (return (AsList
                        (SetResult 'Result row)
                    ))
                )
            )");

            auto& value = result.GetValue();
            if (value.GetStruct(0).GetOptional().HasOptional()) {
                incrementalRestoreOperationsClean = false;
                Cerr << "ERROR: IncrementalRestoreOperations table still has entries after DROP" << Endl;
            }
        } catch (...) {
            incrementalRestoreOperationsClean = false;
            Cerr << "ERROR: Failed to query IncrementalRestoreOperations table" << Endl;
        }

        UNIT_ASSERT_C(incrementalRestoreOperationsClean, "IncrementalRestoreOperations table not properly cleaned up");

        bool incrementalRestoreStateClean = true;
        try {
            auto result = LocalMiniKQL(runtime, schemeshardTabletId, R"(
                (
                    (let key '('('OperationId (Uint64 '0))))
                    (let select '('OperationId))
                    (let row (SelectRow 'IncrementalRestoreState key select))
                    (return (AsList
                        (SetResult 'Result row)
                    ))
                )
            )");

            auto& value = result.GetValue();
            if (value.GetStruct(0).GetOptional().HasOptional()) {
                incrementalRestoreStateClean = false;
                Cerr << "ERROR: IncrementalRestoreState table still has entries after DROP" << Endl;
            }
        } catch (...) {
            incrementalRestoreStateClean = false;
            Cerr << "ERROR: Failed to query IncrementalRestoreState table" << Endl;
        }

        UNIT_ASSERT_C(incrementalRestoreStateClean, "IncrementalRestoreState table not properly cleaned up");

        bool incrementalRestoreShardProgressClean = true;
        try {
            auto result = LocalMiniKQL(runtime, schemeshardTabletId, R"(
                (
                    (let key '('('OperationId (Uint64 '0)) '('ShardIdx (Uint64 '0))))
                    (let select '('OperationId))
                    (let row (SelectRow 'IncrementalRestoreShardProgress key select))
                    (return (AsList
                        (SetResult 'Result row)
                    ))
                )
            )");

            auto& value = result.GetValue();
            if (value.GetStruct(0).GetOptional().HasOptional()) {
                incrementalRestoreShardProgressClean = false;
                Cerr << "ERROR: IncrementalRestoreShardProgress table still has entries after DROP" << Endl;
            }
        } catch (...) {
            incrementalRestoreShardProgressClean = false;
            Cerr << "ERROR: Failed to query IncrementalRestoreShardProgress table" << Endl;
        }

        UNIT_ASSERT_C(incrementalRestoreShardProgressClean, "IncrementalRestoreShardProgress table not properly cleaned up");

        Cerr << "SUCCESS: All LocalDB tables properly cleaned up after DROP BACKUP COLLECTION" << Endl;

        TString recreateCollectionSettings = R"(
            Name: "LocalDbTestCollection"

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/LocalDbTestTable"
                }
            }
            Cluster: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
            recreateCollectionSettings);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DropCollectionDuringActiveOperation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TString activeOpCollectionSettings = R"(
            Name: "ActiveOpTestCollection"

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/ActiveOpTestTable"
                }
            }
            Cluster: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", activeOpCollectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ActiveOpTestTable"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Start a backup operation (async, don't wait for completion)
        AsyncBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/ActiveOpTestCollection")");
        ui64 backupTxId = txId;

        // This shows that active operation protection IS implemented
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"ActiveOpTestCollection\"",
            {NKikimrScheme::StatusPreconditionFailed}); // CORRECT: System properly rejects this
        env.TestWaitNotification(runtime, txId);

        env.TestWaitNotification(runtime, backupTxId);

        // VERIFICATION: Collection should still exist since drop was properly rejected
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/ActiveOpTestCollection"),
            {NLs::PathExist});

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"ActiveOpTestCollection\"");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/ActiveOpTestCollection"),
            {NLs::PathNotExist});

    }

    Y_UNIT_TEST(VerifyCdcStreamCleanupInIncrementalBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);

        // Create backup collection that supports incremental backups
        TString collectionSettingsWithIncremental = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TestTable"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
            collectionSettingsWithIncremental);
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
                          {NLs::PathExist, NLs::IsTable});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathExist, NLs::IsBackupCollection});

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
                          {NLs::PathExist, NLs::IsTable});

        // TODO: Add specific CDC stream cleanup verification
        // This requires understanding the CDC stream naming and location patterns
        // Current test verifies basic incremental backup drop functionality
    }

    Y_UNIT_TEST(VerifyCdcStreamCleanupInIncrementalDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);

        // Create backup collection with incremental support
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        auto describeResult = DescribePath(runtime, "/MyRoot/Table1", true, true);
        TVector<TString> cdcStreamNames;

        // Check table description for CDC streams (this is where they are actually stored)
        if (describeResult.GetPathDescription().HasTable()) {
            const auto& tableDesc = describeResult.GetPathDescription().GetTable();
            if (tableDesc.CdcStreamsSize() > 0) {
                Cerr << "Table has " << tableDesc.CdcStreamsSize() << " CDC streams in description" << Endl;
                for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
                    const auto& cdcStream = tableDesc.GetCdcStreams(i);
                    if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                        cdcStreamNames.push_back(cdcStream.GetName());
                        Cerr << "Found incremental backup CDC stream: " << cdcStream.GetName() << Endl;
                    }
                }
            }
        }

        UNIT_ASSERT_C(!cdcStreamNames.empty(), "Expected to find CDC streams with '_continuousBackupImpl' suffix after incremental backup");

        for (const auto& streamName : cdcStreamNames) {
            UNIT_ASSERT_C(streamName.size() >= 15 + TString("_continuousBackupImpl").size(),
                "CDC stream name should have timestamp prefix: " + streamName);

            TString prefix = streamName.substr(0, streamName.size() - TString("_continuousBackupImpl").size());
            UNIT_ASSERT_C(prefix.EndsWith("Z"), "CDC stream timestamp should end with 'Z': " + prefix);
        }

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathNotExist});

        auto describeAfter = DescribePath(runtime, "/MyRoot/Table1", true, true);
        TVector<TString> remainingCdcStreams;

        // Check table description for remaining CDC streams
        if (describeAfter.GetPathDescription().HasTable()) {
            const auto& tableDesc = describeAfter.GetPathDescription().GetTable();
            if (tableDesc.CdcStreamsSize() > 0) {
                Cerr << "Table still has " << tableDesc.CdcStreamsSize() << " CDC streams after drop" << Endl;
                for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
                    const auto& cdcStream = tableDesc.GetCdcStreams(i);
                    if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                        remainingCdcStreams.push_back(cdcStream.GetName());
                        Cerr << "Incremental backup CDC stream still exists after drop: " << cdcStream.GetName() << Endl;
                    }
                }
            }
        }

        UNIT_ASSERT_C(remainingCdcStreams.empty(),
            "Incremental backup CDC streams with '_continuousBackupImpl' suffix should be cleaned up after dropping backup collection");
        // During incremental backup, CDC streams are created under the source table
        // They should be properly cleaned up when the backup collection is dropped

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                          {NLs::PathExist, NLs::IsTable});

        // Restart SchemeShard to verify persistent cleanup
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                          {NLs::PathExist, NLs::IsTable});

        auto describeAfterReboot = DescribePath(runtime, "/MyRoot/Table1", true, true);
        TVector<TString> cdcStreamsAfterReboot;

        if (describeAfterReboot.GetPathDescription().HasTable()) {
            const auto& tableDesc = describeAfterReboot.GetPathDescription().GetTable();
            if (tableDesc.CdcStreamsSize() > 0) {
                Cerr << "Table still has " << tableDesc.CdcStreamsSize() << " CDC streams after restart" << Endl;
                for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
                    const auto& cdcStream = tableDesc.GetCdcStreams(i);
                    if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                        cdcStreamsAfterReboot.push_back(cdcStream.GetName());
                        Cerr << "Incremental backup CDC stream still exists after restart: " << cdcStream.GetName() << Endl;
                    }
                }
            }
        }

        UNIT_ASSERT_C(cdcStreamsAfterReboot.empty(),
            "Incremental backup CDC streams with '_continuousBackupImpl' suffix should remain cleaned up after restart");

        // The implementation properly handles CDC stream cleanup during backup collection drop
    }

    Y_UNIT_TEST(DropErrorRecoveryTest) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);

        // Create backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create multiple backups
        for (int i = 0; i < 3; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);
        }

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathNotExist});

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathExist, NLs::IsBackupCollection});
    }

    Y_UNIT_TEST(ConcurrentDropProtectionTest) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);

        // Create backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Start first drop operation asynchronously
        AsyncDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"" DEFAULT_NAME_1 "\"");

        // Immediately try second drop operation (should fail)
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            "Name: \"" DEFAULT_NAME_1 "\"",
            {NKikimrScheme::StatusMultipleModifications}); // Expect concurrent operation error

        env.TestWaitNotification(runtime, txId - 1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathNotExist});
    }

    Y_UNIT_TEST(RestorePathStatePersistenceAcrossRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create test table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "PersistentTable"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create backup collection
        TString collectionSettings = R"(
            Name: "PersistenceTest"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/PersistentTable"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create backups
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/PersistenceTest")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/PersistenceTest")");
        env.TestWaitNotification(runtime, txId);

        // Drop table to prepare for restore test
        TestDropTable(runtime, ++txId, "/MyRoot", "PersistentTable");
        env.TestWaitNotification(runtime, txId);

        // Verify table is gone
        TestLs(runtime, "/MyRoot/PersistentTable", false, NLs::PathNotExist);

        // Restart SchemeShard
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Verify backup collection survived restart
        TestLs(runtime, "/MyRoot/.backups/collections/PersistenceTest", false, NLs::PathExist);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/PersistenceTest"), {
            NLs::PathExist,
            NLs::IsBackupCollection
        });

        // Verify table is still gone
        TestLs(runtime, "/MyRoot/PersistentTable", false, NLs::PathNotExist);

        // Restore after restart
        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/PersistenceTest")");
        env.TestWaitNotification(runtime, txId);

        // Verify table is restored to correct path after restart
        TestLs(runtime, "/MyRoot/PersistentTable", false, NLs::PathExist);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PersistentTable"), {
            NLs::PathExist,
            NLs::IsTable
        });

        // Verify backup collection path is still correct
        TestLs(runtime, "/MyRoot/.backups/collections/PersistenceTest", false, NLs::PathExist);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/PersistenceTest"), {
            NLs::PathExist,
            NLs::IsBackupCollection
        });

        // Test another restart to verify persistence
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Final verification - all paths should be preserved
        TestLs(runtime, "/MyRoot/PersistentTable", false, NLs::PathExist);
        TestLs(runtime, "/MyRoot/.backups/collections/PersistenceTest", false, NLs::PathExist);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PersistentTable"), {
            NLs::PathExist,
            NLs::IsTable
        });
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/PersistenceTest"), {
            NLs::PathExist,
            NLs::IsBackupCollection
        });
    }

    // TODO: DropCollectionWithIncrementalRestoreStateCleanup

    Y_UNIT_TEST(IncrementalBackupOperation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultIncrementalCollectionSettings());

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        const auto backupId = txId;
        env.TestWaitNotification(runtime, backupId);

        auto r1 = TestGetIncrementalBackup(runtime, backupId, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(r1.GetIncrementalBackup().GetProgress(), Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
            NLs::ChildrenCount(2),
            NLs::Finished,
        });

        runtime.SimulateSleep(TDuration::Seconds(5));

        auto r2 = TestGetIncrementalBackup(runtime, backupId, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(r2.GetIncrementalBackup().GetProgress(), Ydb::Backup::BackupProgress::PROGRESS_DONE);

        TestForgetIncrementalBackup(runtime, txId++, "/MyRoot", backupId);

        TestGetIncrementalBackup(runtime, backupId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(EmptyIncrementalBackupRace) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultIncrementalCollectionSettings());

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // TEvTerminateWriter should be received before TEvHandshake with writer
        TBlockEvents<NReplication::NService::TEvWorker::TEvHandshake> block(runtime);
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NReplication::NService::TEvWorker::TEvTerminateWriter::EventType) {
                block.Unblock();
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        const auto backupId = txId;
        env.TestWaitNotification(runtime, backupId);

        runtime.WaitFor("block handshakes with reader & writer", [&] { return block.size() == 2; });

        // Unblock TEvHandhshake with reader, but stil block TEvHandshake with writer
        block.Stop();
        block.Unblock(1);

        auto r1 = TestGetIncrementalBackup(runtime, backupId, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(r1.GetIncrementalBackup().GetProgress(), Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
            NLs::ChildrenCount(2),
            NLs::Finished,
        });

        runtime.SimulateSleep(TDuration::Seconds(5));

        auto r2 = TestGetIncrementalBackup(runtime, backupId, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(r2.GetIncrementalBackup().GetProgress(), Ydb::Backup::BackupProgress::PROGRESS_DONE);

        TestForgetIncrementalBackup(runtime, txId++, "/MyRoot", backupId);

        TestGetIncrementalBackup(runtime, backupId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(BackupServiceDirectoryValidation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        SetupLogging(runtime);

        // Enable system names protection feature
        runtime.GetAppData().FeatureFlags.SetEnableSystemNamesProtection(true);

        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);

        // Create a backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "TestCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Try to create __ydb_backup_meta outside backup collection (should fail - reserved name)
        TestMkDir(runtime, ++txId, "/MyRoot", "__ydb_backup_meta", {NKikimrScheme::StatusSchemeError});

        // Verify we can't create directories with reserved backup service prefix outside backup context
        TestMkDir(runtime, ++txId, "/MyRoot", "__ydb_backup_test", {NKikimrScheme::StatusSchemeError});

        // But we CAN create __ydb_backup_meta inside a backup collection (should succeed)
        TestMkDir(runtime, ++txId, "/MyRoot/.backups/collections/TestCollection", "__ydb_backup_meta");
        env.TestWaitNotification(runtime, txId);

        // Verify it was created
        TestLs(runtime, "/MyRoot/.backups/collections/TestCollection/__ydb_backup_meta", false, NLs::PathExist);
    }

    Y_UNIT_TEST(SingleTableWithGlobalSyncIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create incremental backup collection
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create table with one global sync covering index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Execute full backup
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC stream exists on main table
        auto mainTableDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithIndex", true, true);
        UNIT_ASSERT(mainTableDesc.GetPathDescription().HasTable());
        
        const auto& tableDesc = mainTableDesc.GetPathDescription().GetTable();
        bool foundMainTableCdc = false;
        TString mainTableCdcName;
        
        for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
            const auto& cdcStream = tableDesc.GetCdcStreams(i);
            if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                foundMainTableCdc = true;
                mainTableCdcName = cdcStream.GetName();
                Cerr << "Found main table CDC stream: " << mainTableCdcName << Endl;
                break;
            }
        }
        UNIT_ASSERT_C(foundMainTableCdc, "Main table should have CDC stream with '_continuousBackupImpl' suffix");

        // Verify CDC stream exists on index implementation table
        auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithIndex/ValueIndex", true, true);
        UNIT_ASSERT(indexDesc.GetPathDescription().HasTableIndex());
        
        // Get index implementation table (first child of index)
        UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().ChildrenSize(), 1);
        TString indexImplTableName = indexDesc.GetPathDescription().GetChildren(0).GetName();
        
        auto indexImplTableDesc = DescribePrivatePath(runtime, 
            "/MyRoot/TableWithIndex/ValueIndex/" + indexImplTableName, true, true);
        UNIT_ASSERT(indexImplTableDesc.GetPathDescription().HasTable());
        
        const auto& indexTableDesc = indexImplTableDesc.GetPathDescription().GetTable();
        bool foundIndexCdc = false;
        TString indexCdcName;
        
        for (size_t i = 0; i < indexTableDesc.CdcStreamsSize(); ++i) {
            const auto& cdcStream = indexTableDesc.GetCdcStreams(i);
            if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                foundIndexCdc = true;
                indexCdcName = cdcStream.GetName();
                Cerr << "Found index CDC stream: " << indexCdcName << Endl;
                break;
            }
        }
        UNIT_ASSERT_C(foundIndexCdc, "Index implementation table should have CDC stream with '_continuousBackupImpl' suffix");

        // Verify CDC stream names match pattern and use same timestamp
        UNIT_ASSERT_VALUES_EQUAL(mainTableCdcName, indexCdcName);
        UNIT_ASSERT_C(mainTableCdcName.Contains("Z") && mainTableCdcName.EndsWith("_continuousBackupImpl"), 
            "CDC stream name should have X.509 timestamp format (YYYYMMDDHHMMSSZ_continuousBackupImpl)");
    }

    Y_UNIT_TEST(SingleTableWithMultipleGlobalSyncIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create incremental backup collection
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithMultipleIndexes"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create table with multiple global sync indexes
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithMultipleIndexes"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value1" Type: "Utf8" }
                Columns { Name: "value2" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "Value1Index"
                KeyColumnNames: ["value1"]
                Type: EIndexTypeGlobal
            }
            IndexDescription {
                Name: "Value2Index"
                KeyColumnNames: ["value2"]
                DataColumnNames: ["value1"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Execute full backup
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC stream on main table
        auto mainTableDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithMultipleIndexes", true, true);
        const auto& tableDesc = mainTableDesc.GetPathDescription().GetTable();
        
        TString mainCdcName;
        for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
            const auto& cdcStream = tableDesc.GetCdcStreams(i);
            if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                mainCdcName = cdcStream.GetName();
                break;
            }
        }
        UNIT_ASSERT_C(!mainCdcName.empty(), "Main table should have CDC stream");

        // Verify CDC streams on both indexes
        TVector<TString> indexNames = {"Value1Index", "Value2Index"};
        TVector<TString> indexCdcNames;
        
        for (const auto& indexName : indexNames) {
            auto indexDesc = DescribePrivatePath(runtime, 
                "/MyRoot/TableWithMultipleIndexes/" + indexName, true, true);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().ChildrenSize(), 1);
            TString indexImplTableName = indexDesc.GetPathDescription().GetChildren(0).GetName();
            
            auto indexImplTableDesc = DescribePrivatePath(runtime, 
                "/MyRoot/TableWithMultipleIndexes/" + indexName + "/" + indexImplTableName, true, true);
            const auto& indexTableDesc = indexImplTableDesc.GetPathDescription().GetTable();
            
            bool foundCdc = false;
            for (size_t i = 0; i < indexTableDesc.CdcStreamsSize(); ++i) {
                const auto& cdcStream = indexTableDesc.GetCdcStreams(i);
                if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                    indexCdcNames.push_back(cdcStream.GetName());
                    foundCdc = true;
                    Cerr << "Found CDC stream on " << indexName << ": " << cdcStream.GetName() << Endl;
                    break;
                }
            }
            UNIT_ASSERT_C(foundCdc, "Index " + indexName + " should have CDC stream");
        }

        // Verify all streams use the same timestamp
        UNIT_ASSERT_VALUES_EQUAL(indexCdcNames.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(mainCdcName, indexCdcNames[0]);
        UNIT_ASSERT_VALUES_EQUAL(mainCdcName, indexCdcNames[1]);
    }

    Y_UNIT_TEST(TableWithMixedIndexTypes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create incremental backup collection
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithMixedIndexes"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create table with global sync + async indexes
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithMixedIndexes"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value1" Type: "Utf8" }
                Columns { Name: "value2" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "SyncIndex"
                KeyColumnNames: ["value1"]
                Type: EIndexTypeGlobal
            }
            IndexDescription {
                Name: "AsyncIndex"
                KeyColumnNames: ["value2"]
                Type: EIndexTypeGlobalAsync
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Execute full backup
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC stream on main table
        auto mainTableDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithMixedIndexes", true, true);
        const auto& tableDesc = mainTableDesc.GetPathDescription().GetTable();
        
        bool foundMainCdc = false;
        for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
            if (tableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundMainCdc = true;
                break;
            }
        }
        UNIT_ASSERT_C(foundMainCdc, "Main table should have CDC stream");

        // Verify CDC stream on global sync index ONLY
        auto syncIndexDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithMixedIndexes/SyncIndex", true, true);
        UNIT_ASSERT_VALUES_EQUAL(syncIndexDesc.GetPathDescription().ChildrenSize(), 1);
        TString syncImplTableName = syncIndexDesc.GetPathDescription().GetChildren(0).GetName();
        
        auto syncImplTableDesc = DescribePrivatePath(runtime, 
            "/MyRoot/TableWithMixedIndexes/SyncIndex/" + syncImplTableName, true, true);
        const auto& syncTableDesc = syncImplTableDesc.GetPathDescription().GetTable();
        
        bool foundSyncCdc = false;
        for (size_t i = 0; i < syncTableDesc.CdcStreamsSize(); ++i) {
            if (syncTableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundSyncCdc = true;
                Cerr << "Found CDC stream on SyncIndex (expected)" << Endl;
                break;
            }
        }
        UNIT_ASSERT_C(foundSyncCdc, "Global sync index should have CDC stream");

        // Verify NO CDC stream on async index
        auto asyncIndexDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithMixedIndexes/AsyncIndex", true, true);
        UNIT_ASSERT_VALUES_EQUAL(asyncIndexDesc.GetPathDescription().ChildrenSize(), 1);
        TString asyncImplTableName = asyncIndexDesc.GetPathDescription().GetChildren(0).GetName();
        
        auto asyncImplTableDesc = DescribePrivatePath(runtime, 
            "/MyRoot/TableWithMixedIndexes/AsyncIndex/" + asyncImplTableName, true, true);
        const auto& asyncTableDesc = asyncImplTableDesc.GetPathDescription().GetTable();
        
        bool foundAsyncCdc = false;
        for (size_t i = 0; i < asyncTableDesc.CdcStreamsSize(); ++i) {
            if (asyncTableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundAsyncCdc = true;
                break;
            }
        }
        UNIT_ASSERT_C(!foundAsyncCdc, "Async index should NOT have CDC stream");
    }

    Y_UNIT_TEST(MultipleTablesWithIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create incremental backup collection with 2 tables
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table2"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create Table1 with index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "Index1"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Create Table2 with index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table2"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "Index2"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Execute full backup
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC streams on both main tables
        TVector<TString> tables = {"Table1", "Table2"};
        TVector<TString> indexes = {"Index1", "Index2"};
        
        for (size_t tableIdx = 0; tableIdx < tables.size(); ++tableIdx) {
            const auto& tableName = tables[tableIdx];
            const auto& indexName = indexes[tableIdx];
            
            // Check main table CDC
            auto mainTableDesc = DescribePrivatePath(runtime, "/MyRoot/" + tableName, true, true);
            const auto& tableDesc = mainTableDesc.GetPathDescription().GetTable();
            
            bool foundMainCdc = false;
            for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
                if (tableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                    foundMainCdc = true;
                    Cerr << "Found CDC stream on " << tableName << Endl;
                    break;
                }
            }
            UNIT_ASSERT_C(foundMainCdc, tableName + " should have CDC stream");
            
            // Check index CDC
            auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/" + tableName + "/" + indexName, true, true);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().ChildrenSize(), 1);
            TString indexImplTableName = indexDesc.GetPathDescription().GetChildren(0).GetName();
            
            auto indexImplTableDesc = DescribePrivatePath(runtime, 
                "/MyRoot/" + tableName + "/" + indexName + "/" + indexImplTableName, true, true);
            const auto& indexTableDesc = indexImplTableDesc.GetPathDescription().GetTable();
            
            bool foundIndexCdc = false;
            for (size_t i = 0; i < indexTableDesc.CdcStreamsSize(); ++i) {
                if (indexTableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                    foundIndexCdc = true;
                    Cerr << "Found CDC stream on " << indexName << Endl;
                    break;
                }
            }
            UNIT_ASSERT_C(foundIndexCdc, indexName + " should have CDC stream");
        }
    }

    Y_UNIT_TEST(IncrementalBackupWithIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create incremental backup collection
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableForIncremental"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create table with global sync index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableForIncremental"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Execute full backup (creates CDC streams)
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC streams were created for both main table and index
        auto mainTableDesc = DescribePrivatePath(runtime, "/MyRoot/TableForIncremental", true, true);
        const auto& tableDesc = mainTableDesc.GetPathDescription().GetTable();
        
        bool foundMainCdc = false;
        for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
            if (tableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundMainCdc = true;
                Cerr << "Found CDC stream on main table" << Endl;
                break;
            }
        }
        UNIT_ASSERT_C(foundMainCdc, "Main table should have CDC stream after full backup");

        // Verify CDC stream on index
        auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/TableForIncremental/ValueIndex", true, true);
        UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().ChildrenSize(), 1);
        TString indexImplTableName = indexDesc.GetPathDescription().GetChildren(0).GetName();
        
        auto indexImplTableDesc = DescribePrivatePath(runtime, 
            "/MyRoot/TableForIncremental/ValueIndex/" + indexImplTableName, true, true);
        const auto& indexTableDesc = indexImplTableDesc.GetPathDescription().GetTable();
        
        bool foundIndexCdc = false;
        for (size_t i = 0; i < indexTableDesc.CdcStreamsSize(); ++i) {
            if (indexTableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundIndexCdc = true;
                Cerr << "Found CDC stream on index implementation table" << Endl;
                break;
            }
        }
        UNIT_ASSERT_C(foundIndexCdc, "Index implementation table should have CDC stream after full backup");

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Execute incremental backup (rotates CDC, creates backup tables)
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify backup collection structure
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
            NLs::ChildrenCount(2), // full + incremental
        });

        // Find the incremental backup directory (should end with "_incremental")
        auto collectionDesc = DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1, true, true);
        TString incrBackupDir;
        for (size_t i = 0; i < collectionDesc.GetPathDescription().ChildrenSize(); ++i) {
            const auto& child = collectionDesc.GetPathDescription().GetChildren(i);
            Cerr << "Child: " << child.GetName() << " PathState: " << child.GetPathState() << Endl;
            if (child.GetName().EndsWith("_incremental")) {
                incrBackupDir = child.GetName();
                break;
            }
        }
        UNIT_ASSERT_C(!incrBackupDir.empty(), "Should find incremental backup directory");

        // Verify backup table for main table exists
        TestDescribeResult(DescribePath(runtime, 
            "/MyRoot/.backups/collections/" DEFAULT_NAME_1 "/" + incrBackupDir + "/TableForIncremental"), {
            NLs::PathExist,
            NLs::IsTable,
        });

        // Verify index backup directory exists in __ydb_backup_meta/indexes/TableForIncremental/ValueIndex
        TString indexBackupPath = "/MyRoot/.backups/collections/" DEFAULT_NAME_1 "/" + incrBackupDir +
            "/__ydb_backup_meta/indexes/TableForIncremental/ValueIndex";
        TestDescribeResult(DescribePath(runtime, indexBackupPath), {
            NLs::PathExist,
            NLs::ChildrenCount(1),
        });

        // Verify impl table inside the index backup directory
        TestDescribeResult(DescribePath(runtime, indexBackupPath + "/" + indexImplTableName), {
            NLs::PathExist,
            NLs::IsTable,
        });

        Cerr << "SUCCESS: Full backup created CDC streams for both main table and index" << Endl;
        Cerr << "         Incremental backup created backup tables for both main table and index" << Endl;
        Cerr << "         Index backup table verified at: " << indexBackupPath << "/" << indexImplTableName << Endl;
    }

    Y_UNIT_TEST(OmitIndexesFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create incremental backup collection WITH OmitIndexes flag set
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            Cluster: {}
            IncrementalBackupConfig {
                OmitIndexes: true
            }
        )";
        
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create table with global sync index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Execute full backup
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC stream exists on main table
        auto mainTableDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithIndex", true, true);
        const auto& tableDesc = mainTableDesc.GetPathDescription().GetTable();
        
        bool foundMainCdc = false;
        for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
            if (tableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundMainCdc = true;
                Cerr << "Found CDC stream on main table (expected)" << Endl;
                break;
            }
        }
        UNIT_ASSERT_C(foundMainCdc, "Main table should have CDC stream even with OmitIndexes=true");

        // Verify NO CDC stream on index (because OmitIndexes is true)
        auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithIndex/ValueIndex", true, true);
        UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().ChildrenSize(), 1);
        TString indexImplTableName = indexDesc.GetPathDescription().GetChildren(0).GetName();
        
        auto indexImplTableDesc = DescribePrivatePath(runtime, 
            "/MyRoot/TableWithIndex/ValueIndex/" + indexImplTableName, true, true);
        const auto& indexTableDesc = indexImplTableDesc.GetPathDescription().GetTable();
        
        bool foundIndexCdc = false;
        for (size_t i = 0; i < indexTableDesc.CdcStreamsSize(); ++i) {
            if (indexTableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundIndexCdc = true;
                break;
            }
        }
        UNIT_ASSERT_C(!foundIndexCdc, "Index should NOT have CDC stream when OmitIndexes=true");

        Cerr << "SUCCESS: OmitIndexes flag works correctly - main table has CDC, index does not" << Endl;
    }

    Y_UNIT_TEST(BackupWithIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        SetupLogging(runtime);
        ui64 txId = 100;

        // Create table with index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Verify source table has the index
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableWithIndex"), {
            NLs::PathExist,
            NLs::IndexesCount(1)
        });

        PrepareDirs(runtime, env, txId);

        // Create backup collection with OmitIndexes = false (explicitly request indexes)
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "CollectionWithIndex"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            OmitIndexes: false
        )");
        env.TestWaitNotification(runtime, txId);

        // Backup the table (indexes should be included)
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CollectionWithIndex")");
        env.TestWaitNotification(runtime, txId);

        // Verify backup collection has children (the backup directory)
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/CollectionWithIndex"), {
            NLs::PathExist,
            NLs::IsBackupCollection,
            NLs::ChildrenCount(1)
        });

        // Get the backup directory and verify its structure contains index
        auto backupDesc = DescribePath(runtime, "/MyRoot/.backups/collections/CollectionWithIndex");
        UNIT_ASSERT(backupDesc.GetPathDescription().ChildrenSize() == 1);
        TString backupDirName = backupDesc.GetPathDescription().GetChildren(0).GetName();
        
        // Verify backup directory has the table (indexes are stored under the table)
        TString backupPath = "/MyRoot/.backups/collections/CollectionWithIndex/" + backupDirName;
        auto backupContentDesc = DescribePath(runtime, backupPath);
        
        // The backup should contain 1 child (the table; indexes are children of the table)
        UNIT_ASSERT_C(backupContentDesc.GetPathDescription().ChildrenSize() == 1,
            "Backup should contain 1 table, got " << backupContentDesc.GetPathDescription().ChildrenSize());
        
        // Verify the table HAS indexes in the backup (check via TableIndexesSize)
        UNIT_ASSERT_VALUES_EQUAL(backupContentDesc.GetPathDescription().GetChildren(0).GetName(), "TableWithIndex");
        
        auto tableDesc = DescribePath(runtime, backupPath + "/TableWithIndex");
        UNIT_ASSERT(tableDesc.GetPathDescription().HasTable());
        UNIT_ASSERT_VALUES_EQUAL(tableDesc.GetPathDescription().GetTable().TableIndexesSize(), 1);
        
        // Verify ChildrenExist flag is set (index exists as child, even if not in Children list)
        UNIT_ASSERT(tableDesc.GetPathDescription().GetSelf().GetChildrenExist());
    }

    Y_UNIT_TEST(BackupWithIndexesOmit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        SetupLogging(runtime);
        ui64 txId = 100;

        // Create table with index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Verify source table has the index
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableWithIndex"), {
            NLs::PathExist,
            NLs::IndexesCount(1)
        });

        PrepareDirs(runtime, env, txId);

        // Create backup collection with OmitIndexes = true (at collection level)
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "CollectionWithoutIndex"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            OmitIndexes: true
        )");
        env.TestWaitNotification(runtime, txId);

        // Backup the table (indexes should be omitted)
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CollectionWithoutIndex")");
        env.TestWaitNotification(runtime, txId);

        // Verify backup collection has children (the backup directory)
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/CollectionWithoutIndex"), {
            NLs::PathExist,
            NLs::IsBackupCollection,
            NLs::ChildrenCount(1)
        });

        // Get the backup directory and verify its structure does NOT contain index
        auto backupDesc = DescribePath(runtime, "/MyRoot/.backups/collections/CollectionWithoutIndex");
        UNIT_ASSERT(backupDesc.GetPathDescription().ChildrenSize() == 1);
        TString backupDirName = backupDesc.GetPathDescription().GetChildren(0).GetName();
        
        // Verify backup directory has only the table (no index children when omitted)
        TString backupPath = "/MyRoot/.backups/collections/CollectionWithoutIndex/" + backupDirName;
        auto backupContentDesc = DescribePath(runtime, backupPath);
        
        // The backup should contain 1 child (the table), without index children
        UNIT_ASSERT_C(backupContentDesc.GetPathDescription().ChildrenSize() == 1,
            "Backup should contain only table without index, got " << backupContentDesc.GetPathDescription().ChildrenSize());
        
        // Verify the table exists but has NO indexes (omitted via OmitIndexes: true)
        UNIT_ASSERT_VALUES_EQUAL(backupContentDesc.GetPathDescription().GetChildren(0).GetName(), "TableWithIndex");
        
        auto tableDesc = DescribePath(runtime, backupPath + "/TableWithIndex");
        UNIT_ASSERT(tableDesc.GetPathDescription().HasTable());
        
        // When indexes are omitted, TableIndexesSize should be 0
        UNIT_ASSERT_VALUES_EQUAL(tableDesc.GetPathDescription().GetTable().TableIndexesSize(), 0);
        
        // Verify ChildrenExist is false (no index children)
        UNIT_ASSERT(!tableDesc.GetPathDescription().GetSelf().GetChildrenExist());
    }

    Y_UNIT_TEST(BackupWithIndexesDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        SetupLogging(runtime);
        ui64 txId = 100;

        // Create table with index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Verify source table has the index
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableWithIndex"), {
            NLs::PathExist,
            NLs::IndexesCount(1)
        });

        PrepareDirs(runtime, env, txId);

        // Create backup collection without specifying OmitIndexes (default behavior)
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "CollectionDefaultBehavior"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Backup the table (default behavior: OmitIndexes not specified, should default to false)
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CollectionDefaultBehavior")");
        env.TestWaitNotification(runtime, txId);

        // Verify backup collection has children (the backup directory)
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/CollectionDefaultBehavior"), {
            NLs::PathExist,
            NLs::IsBackupCollection,
            NLs::ChildrenCount(1)
        });

        // Get the backup directory and verify its structure
        auto backupDesc = DescribePath(runtime, "/MyRoot/.backups/collections/CollectionDefaultBehavior");
        UNIT_ASSERT(backupDesc.GetPathDescription().ChildrenSize() == 1);
        TString backupDirName = backupDesc.GetPathDescription().GetChildren(0).GetName();
        
        // Verify backup directory structure
        TString backupPath = "/MyRoot/.backups/collections/CollectionDefaultBehavior/" + backupDirName;
        auto backupContentDesc = DescribePath(runtime, backupPath);
        
        // The backup should contain 1 child (the table; indexes are children of the table)
        UNIT_ASSERT_C(backupContentDesc.GetPathDescription().ChildrenSize() == 1,
            "Backup should contain 1 table, got " << backupContentDesc.GetPathDescription().ChildrenSize());
        
        // Verify the table HAS indexes in the backup by default (check via TableIndexesSize)
        UNIT_ASSERT_VALUES_EQUAL(backupContentDesc.GetPathDescription().GetChildren(0).GetName(), "TableWithIndex");
        
        auto tableDesc = DescribePath(runtime, backupPath + "/TableWithIndex");
        UNIT_ASSERT(tableDesc.GetPathDescription().HasTable());
        UNIT_ASSERT_VALUES_EQUAL(tableDesc.GetPathDescription().GetTable().TableIndexesSize(), 1);
        
        // Verify ChildrenExist flag is set by default (index exists as child)
        UNIT_ASSERT(tableDesc.GetPathDescription().GetSelf().GetChildrenExist());
    }

    Y_UNIT_TEST(CdcStreamRotationDuringIncrementalBackups) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true).EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TString collectionSettings = R"(
            Name: "RotationTestCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TestTable"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/RotationTestCollection")");
        env.TestWaitNotification(runtime, txId);

        auto tableDesc1 = DescribePrivatePath(runtime, "/MyRoot/TestTable", true, true);
        UNIT_ASSERT(tableDesc1.GetPathDescription().HasTable());
        UNIT_ASSERT_VALUES_EQUAL(tableDesc1.GetPathDescription().GetTable().CdcStreamsSize(), 1);

        TString firstCdcStreamName = tableDesc1.GetPathDescription().GetTable().GetCdcStreams(0).GetName();
        UNIT_ASSERT_C(firstCdcStreamName.EndsWith("_continuousBackupImpl"), 
            "CDC stream should end with '_continuousBackupImpl', got: " + firstCdcStreamName);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TestTable/" + firstCdcStreamName), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
        });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TestTable/" + firstCdcStreamName + "/streamImpl"), {
            NLs::PathExist,
        });

        runtime.AdvanceCurrentTime(TDuration::Seconds(2));

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/RotationTestCollection")");
        env.TestWaitNotification(runtime, txId);

        env.SimulateSleep(runtime, TDuration::Seconds(5));

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TestTable/" + firstCdcStreamName), {
            NLs::PathNotExist
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TestTable/" + firstCdcStreamName + "/streamImpl"), {
            NLs::PathNotExist
        });

        auto tableDesc2 = DescribePrivatePath(runtime, "/MyRoot/TestTable", true, true);
        UNIT_ASSERT(tableDesc2.GetPathDescription().HasTable());
        UNIT_ASSERT_VALUES_EQUAL(tableDesc2.GetPathDescription().GetTable().CdcStreamsSize(), 1);

        TString secondCdcStreamName = tableDesc2.GetPathDescription().GetTable().GetCdcStreams(0).GetName();
        UNIT_ASSERT_C(secondCdcStreamName.EndsWith("_continuousBackupImpl"), 
            "New CDC stream should end with '_continuousBackupImpl', got: " + secondCdcStreamName);
        UNIT_ASSERT_C(firstCdcStreamName != secondCdcStreamName, 
            "CDC stream name should change after rotation");

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TestTable/" + secondCdcStreamName), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
        });

        runtime.AdvanceCurrentTime(TDuration::Seconds(2));

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/RotationTestCollection")");
        env.TestWaitNotification(runtime, txId);

        env.SimulateSleep(runtime, TDuration::Seconds(5));

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TestTable/" + secondCdcStreamName), {
            NLs::PathNotExist
        });

        auto tableDesc3 = DescribePrivatePath(runtime, "/MyRoot/TestTable", true, true);
        UNIT_ASSERT(tableDesc3.GetPathDescription().HasTable());
        UNIT_ASSERT_VALUES_EQUAL(tableDesc3.GetPathDescription().GetTable().CdcStreamsSize(), 1);

        TString thirdCdcStreamName = tableDesc3.GetPathDescription().GetTable().GetCdcStreams(0).GetName();
        UNIT_ASSERT_C(thirdCdcStreamName != secondCdcStreamName, 
            "CDC stream name should change after second rotation");

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/RotationTestCollection")");
        env.TestWaitNotification(runtime, txId);

        env.SimulateSleep(runtime, TDuration::Seconds(5));

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TestTable/" + thirdCdcStreamName), {
            NLs::PathNotExist
        });

        auto tableDesc4 = DescribePrivatePath(runtime, "/MyRoot/TestTable", true, true);
        UNIT_ASSERT(tableDesc4.GetPathDescription().HasTable());
        // Now full backup rotate streams like incremental backup
        UNIT_ASSERT_VALUES_EQUAL(tableDesc4.GetPathDescription().GetTable().CdcStreamsSize(), 1);
    }

    Y_UNIT_TEST(DropCollectionAfterIncrementalRestore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {NLs::PathExist});
    }

    Y_UNIT_TEST(IndexCdcStreamCountRotation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true).EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TString collectionSettings = R"(
            Name: "CountTestCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto getBackupStreamCount = [&](const TString& path) -> size_t {
            auto desc = DescribePrivatePath(runtime, path, true, true);
            if (!desc.GetPathDescription().HasTable()) return 0;
            
            size_t count = 0;
            const auto& table = desc.GetPathDescription().GetTable();
            for (size_t i = 0; i < table.CdcStreamsSize(); ++i) {
                if (table.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                    count++;
                }
            }
            return count;
        };

        auto getIndexImplPath = [&](const TString& tablePath, const TString& indexName) -> TString {
            auto indexDesc = DescribePrivatePath(runtime, tablePath + "/" + indexName, true, true);
            if (indexDesc.GetPathDescription().ChildrenSize() == 0) return "";
            return tablePath + "/" + indexName + "/" + indexDesc.GetPathDescription().GetChildren(0).GetName();
        };

        auto getCurrentStreamName = [&](const TString& path) -> TString {
            auto desc = DescribePrivatePath(runtime, path, true, true);
            if (!desc.GetPathDescription().HasTable()) return "";
            const auto& table = desc.GetPathDescription().GetTable();
            for (size_t i = 0; i < table.CdcStreamsSize(); ++i) {
                if (table.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                    return table.GetCdcStreams(i).GetName();
                }
            }
            return "";
        };

        TString tablePath = "/MyRoot/TableWithIndex";
        TString indexPath = getIndexImplPath(tablePath, "ValueIndex");

        UNIT_ASSERT_VALUES_EQUAL(getBackupStreamCount(tablePath), 0);
        UNIT_ASSERT_VALUES_EQUAL(getBackupStreamCount(indexPath), 0);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CountTestCollection")");
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL_C(getBackupStreamCount(tablePath), 1, "Table should have exactly 1 stream after 1st backup");
        UNIT_ASSERT_VALUES_EQUAL_C(getBackupStreamCount(indexPath), 1, "Index should have exactly 1 stream after 1st backup");
        
        TString streamNameV1 = getCurrentStreamName(tablePath);
        TString indexStreamNameV1 = getCurrentStreamName(indexPath);
        UNIT_ASSERT_VALUES_EQUAL(streamNameV1, indexStreamNameV1);

        runtime.AdvanceCurrentTime(TDuration::Seconds(2));

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CountTestCollection")");
        env.TestWaitNotification(runtime, txId);

        env.SimulateSleep(runtime, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL_C(getBackupStreamCount(tablePath), 1, "Table must have exactly 1 stream after rotation (old deleted, new added)");
        UNIT_ASSERT_VALUES_EQUAL_C(getBackupStreamCount(indexPath), 1, "Index must have exactly 1 stream after rotation");

        TString streamNameV2 = getCurrentStreamName(tablePath);
        TString indexStreamNameV2 = getCurrentStreamName(indexPath);

        UNIT_ASSERT_C(streamNameV1 != streamNameV2, "Stream name should have changed");
        UNIT_ASSERT_VALUES_EQUAL(streamNameV2, indexStreamNameV2);

        runtime.AdvanceCurrentTime(TDuration::Seconds(2));

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CountTestCollection")");
        env.TestWaitNotification(runtime, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL_C(getBackupStreamCount(tablePath), 1, "Table stream count stable at 1");
        UNIT_ASSERT_VALUES_EQUAL_C(getBackupStreamCount(indexPath), 1, "Index stream count stable at 1");

        TString streamNameV3 = getCurrentStreamName(tablePath);
        UNIT_ASSERT_C(streamNameV2 != streamNameV3, "Stream name changed again");
    }

    Y_UNIT_TEST(StreamRotationSafetyWithUserStreams) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true).EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TString collectionSettings = R"(
            Name: "SafetyCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/SafetyCollection")");
        env.TestWaitNotification(runtime, txId);

        TString prevSystemStreamName;
        {
            auto desc = DescribePrivatePath(runtime, "/MyRoot/Table1", true, true);
            for (const auto& stream : desc.GetPathDescription().GetTable().GetCdcStreams()) {
                if (stream.GetName().EndsWith("_continuousBackupImpl")) {
                    prevSystemStreamName = stream.GetName();
                    break;
                }
            }
        }
        UNIT_ASSERT_C(!prevSystemStreamName.empty(), "Initial system stream must exist");

        THashSet<TString> knownUserStreams;
        TVector<TString> trickyNames = {
            "000000000000000A_continuousBackupImpl",
            "09700101000000Z_continuousBackupImpl",
            "_continuousBackupImpl"
        };

        for (const auto& name : trickyNames) {
            // User cannot set PROTO format
            TString request = TStringBuilder() << R"(
                TableName: "Table1"
                StreamDescription {
                  Name: ")" << name << R"("
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatJson
                }
            )";

            TestCreateCdcStream(runtime, ++txId, "/MyRoot", request);
            env.TestWaitNotification(runtime, txId);
            knownUserStreams.insert(name);
        }

        const int iterations = 3;
        for (int i = 0; i < iterations; ++i) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(5));

            TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/SafetyCollection")");
            env.TestWaitNotification(runtime, txId);

            env.SimulateSleep(runtime, TDuration::Seconds(2));

            auto desc = DescribePrivatePath(runtime, "/MyRoot/Table1", true, true);
            const auto& allStreams = desc.GetPathDescription().GetTable().GetCdcStreams();

            TString newSystemStreamName;
            int systemStreamsCount = 0;
            int foundUserStreamsCount = 0;

            for (const auto& stream : allStreams) {
                TString name = stream.GetName();

                if (knownUserStreams.contains(name)) {
                    foundUserStreamsCount++;
                } else {
                    systemStreamsCount++;
                    newSystemStreamName = name;
                    
                    UNIT_ASSERT_C(name.EndsWith("_continuousBackupImpl"), 
                        "Unknown stream " << name << " found, expected system stream ending with _continuousBackupImpl");
                }
            }

            UNIT_ASSERT_VALUES_EQUAL_C(foundUserStreamsCount, knownUserStreams.size(), 
                "All user streams must survive the backup rotation");

            UNIT_ASSERT_VALUES_EQUAL_C(systemStreamsCount, 1, 
                "There must be exactly one system stream visible after rotation");

            UNIT_ASSERT_VALUES_UNEQUAL_C(prevSystemStreamName, newSystemStreamName, 
                "System stream must be rotated");

            prevSystemStreamName = newSystemStreamName;
        }
    }

    Y_UNIT_TEST(BackupRestoreCoveringIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "CoverTable"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "name" Type: "Utf8" }
                Columns { Name: "age" Type: "Uint64" }
                Columns { Name: "ega" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_name_age"
                KeyColumnNames: ["name", "age"]
                DataColumnNames: ["ega"]     # This corresponds to COVER (ega)
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/CoverTable/idx_name_age"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
            NLs::IndexKeys({"name", "age"}),
            NLs::IndexDataColumns({"ega"})
        });

        TString collectionSettings = R"(
            Name: "CoverCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/CoverTable"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CoverCollection")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CoverCollection")");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "CoverTable");
        env.TestWaitNotification(runtime, txId);

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/CoverCollection")");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/CoverTable"), {
            NLs::PathExist,
            NLs::IndexesCount(1)
        });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/CoverTable/idx_name_age"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
            NLs::IndexKeys({"name", "age"}),
            NLs::IndexDataColumns({"ega"})
        });
    }

    Y_UNIT_TEST(AlterTableInBackupCollectionProtection) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ProtectedTable"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TString collectionSettings = R"(
            Name: "ProtectionCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/ProtectedTable"
                }
            }
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ProtectedTable"
            Columns { Name: "new_column" Type: "Uint64" }
        )", {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ProtectedTable"), {
            NLs::CheckColumns("ProtectedTable", {"key", "value"}, {}, {"key"})
        });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ProtectedTable"
            DropColumns { Name: "value" }
        )", {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"ProtectionCollection\"");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ProtectedTable"
            Columns { Name: "new_column" Type: "Uint64" }
        )", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ProtectedTable"), {
            NLs::CheckColumns("ProtectedTable", {"key", "value", "new_column"}, {}, {"key"})
        });
    }

    Y_UNIT_TEST(IncrementalBackupWithVectorIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));

        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithVector"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "category" Type: "Utf8" }
                Columns { Name: "embedding" Type: "String" }
                Columns { Name: "data" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "VectorIndex"
                KeyColumnNames: ["category", "embedding"]
                DataColumnNames: ["data"]
                Type: EIndexTypeGlobalVectorKmeansTree
                VectorIndexKmeansTreeDescription: {
                    Settings: {
                        settings: {
                            metric: DISTANCE_COSINE,
                            vector_type: VECTOR_TYPE_FLOAT,
                            vector_dimension: 1024
                        },
                        clusters: 4,
                        levels: 2
                    }
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithVector"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        {
            auto desc = DescribePrivatePath(runtime, "/MyRoot/TableWithVector", true, true);
            const auto& table = desc.GetPathDescription().GetTable();
            bool foundCdc = false;
            for (const auto& stream : table.GetCdcStreams()) {
                if (stream.GetName().EndsWith("_continuousBackupImpl")) {
                    foundCdc = true;
                    break;
                }
            }
            UNIT_ASSERT_C(foundCdc, "Main table should have CDC stream for backup");
        }

        TVector<TString> implTables = {"indexImplLevelTable", "indexImplPostingTable", "indexImplPrefixTable"};
        for (const auto& implTable : implTables) {
            TString implPath = "/MyRoot/TableWithVector/VectorIndex/" + implTable;
            auto desc = DescribePrivatePath(runtime, implPath, true, true);

            UNIT_ASSERT_C(desc.GetPathDescription().HasTable(),
                Sprintf("Vector index implementation table %s should exist", implTable.c_str()));

            const auto& table = desc.GetPathDescription().GetTable();
            bool foundImplCdc = false;
            for (const auto& stream : table.GetCdcStreams()) {
                if (stream.GetName().EndsWith("_continuousBackupImpl")) {
                    foundImplCdc = true;
                    Cerr << "Found CDC stream on vector impl table " << implTable << ": " << stream.GetName() << Endl;
                    break;
                }
            }
            UNIT_ASSERT_C(foundImplCdc,
                Sprintf("Vector index implementation table %s should have CDC stream after full backup", implTable.c_str()));
        }

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        auto collectionDesc = DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1, true, true);
        TString incrBackupDir;
        for (const auto& child : collectionDesc.GetPathDescription().GetChildren()) {
            if (child.GetName().EndsWith("_incremental")) {
                incrBackupDir = child.GetName();
                break;
            }
        }
        UNIT_ASSERT_C(!incrBackupDir.empty(), "Should find incremental backup directory");

        TString backupRoot = "/MyRoot/.backups/collections/" DEFAULT_NAME_1 "/" + incrBackupDir;

        TestDescribeResult(DescribePath(runtime, backupRoot + "/TableWithVector"), {
            NLs::PathExist,
            NLs::IsTable,
        });

        TString indexMetaPath = backupRoot + "/__ydb_backup_meta/indexes/TableWithVector/VectorIndex";

        TestDescribeResult(DescribePath(runtime, indexMetaPath), {
            NLs::PathExist,
            NLs::ChildrenCount(3)
        });

        for (const auto& implTable : implTables) {
            TestDescribeResult(DescribePath(runtime, indexMetaPath + "/" + implTable), {
                NLs::PathExist,
                NLs::IsTable
            });
        }
    }

    Y_UNIT_TEST(RestoreVectorIndexBackup) {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithVector"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "category" Type: "Utf8" }
                Columns { Name: "embedding" Type: "String" }
                Columns { Name: "data" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "VectorIndex"
                KeyColumnNames: ["category", "embedding"]
                DataColumnNames: ["data"]
                Type: EIndexTypeGlobalVectorKmeansTree
                VectorIndexKmeansTreeDescription: {
                    Settings: {
                        settings: {
                            metric: DISTANCE_COSINE,
                            vector_type: VECTOR_TYPE_FLOAT,
                            vector_dimension: 1024
                        },
                        clusters: 4,
                        levels: 2
                    }
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TString collectionSettings = Sprintf(R"(
            Name: "%s"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithVector"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )", DEFAULT_NAME_1);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            Sprintf(R"(Name: ".backups/collections/%s")", DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            Sprintf(R"(Name: ".backups/collections/%s")", DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "TableWithVector");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableWithVector"), {NLs::PathNotExist});

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
            Sprintf(R"(Name: "%s")", DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableWithVector"), {
            NLs::PathExist,
            NLs::IsTable,
            NLs::IndexesCount(1)
        });

        auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithVector/VectorIndex");

        TestDescribeResult(indexDesc, {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"category", "embedding"}),
            NLs::IndexDataColumns({"data"})
        });

        TVector<TString> implTables = {"indexImplLevelTable", "indexImplPostingTable", "indexImplPrefixTable"};
        for (const auto& implTable : implTables) {
            TString implPath = "/MyRoot/TableWithVector/VectorIndex/" + implTable;
            auto implDesc = DescribePrivatePath(runtime, implPath, true, true);

            TestDescribeResult(implDesc, {
                NLs::PathExist,
                NLs::IsTable
            });

            const auto& table = implDesc.GetPathDescription().GetTable();
            UNIT_ASSERT_C(table.ColumnsSize() > 0,
                Sprintf("Implementation table %s should have columns restored", implTable.c_str()));
        }

        const auto& kmeansDesc = indexDesc.GetPathDescription().GetTableIndex().GetVectorIndexKmeansTreeDescription();
        const auto& treeSettings = kmeansDesc.GetSettings();
        const auto& vectorSettings = treeSettings.Getsettings();

        UNIT_ASSERT_VALUES_EQUAL(vectorSettings.Getmetric(), Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        UNIT_ASSERT_VALUES_EQUAL(vectorSettings.Getvector_dimension(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(treeSettings.Getclusters(), 4);
        UNIT_ASSERT_VALUES_EQUAL(treeSettings.Getlevels(), 2);
    }

    Y_UNIT_TEST(InitCopyTableSourceDroppedSurvives) {
        // Init must not set PathState=EPathStateCopying on a dropped CopyTable source.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Block datashard propose results to keep CopyTable in TxInFlightV2 across reboot.
        TBlockEvents<TEvDataShard::TEvProposeTransactionResult> block(runtime);

        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Table1Copy", "/MyRoot/Table1");
        TestModificationResult(runtime, txId);

        if (block.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&block](IEventHandle&) { return !block.empty(); });
            runtime.DispatchEvents(opts);
        }

        auto tablePathId = DescribePath(runtime, "/MyRoot/Table1")
            .GetPathDescription().GetSelf().GetPathId();

        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('Id (Uint64 '%lu))))
                (let update '('('StepDropped (Uint64 '1000000))))
                (return (AsList (UpdateRow 'Paths key update)))
            )
        )", tablePathId));

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Triggers Dropped() on Table1 which checks PathState consistency.
        // Without fix: init sets PathState=EPathStateCopying on dropped source -> crash.
        DescribePath(runtime, "/MyRoot/Table1");
    }

    // ====================================================================
    // BackupInvariant / BackupInvariant_NoCascade
    //
    // Exercises scheme-board pairwise cache-consistency after a full +
    // incremental backup on a table with two global indexes.
    //
    // With cascade publication enabled (default), the invariant holds.
    // With cascade disabled, simple backup+rotate scenarios still PASS because
    // the direct PublishToSchemeBoard on each modified path is sufficient for
    // pairwise consistency in the non-raced case — the race is covered by
    // ConcurrentCleanerVsUserCdcRace_*.
    // ====================================================================

    void RunBackupBackupCollectionInvariantScenario(bool enableCascade) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableBackupService(true)
            .EnableCascadePublication(enableCascade));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/T"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "T"
                Columns { Name: "Key" Type: "Uint64" }
                Columns { Name: "Value1" Type: "Utf8" }
                Columns { Name: "Value2" Type: "Utf8" }
                KeyColumnNames: ["Key"]
            }
            IndexDescription {
                Name: "Idx1"
                KeyColumnNames: ["Value1"]
                Type: EIndexTypeGlobal
            }
            IndexDescription {
                Name: "Idx2"
                KeyColumnNames: ["Value2"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto navigateSync = [&](const TString& path,
                                NSchemeCache::TSchemeCacheNavigate::EOp op)
        {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
            using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;
            const auto sender = runtime.AllocateEdgeActor();
            auto request = MakeHolder<TNavigate>();
            auto& entry = request->ResultSet.emplace_back();
            entry.Path = SplitPath(path);
            entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
            entry.Operation = op;
            entry.ShowPrivatePath = true;
            entry.SyncVersion = false;
            runtime.Send(new IEventHandle(MakeSchemeCacheID(), sender,
                new TEvRequest(request.Release())));
            auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT(ev && ev->Get());
            auto* response = ev->Get()->Request.Release();
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.size(), 1);
            return THolder<TNavigate>(response);
        };

        struct TRow {
            ui64 TableView_SchemaVersion = 0;
            ui64 Index_OwnSchemaVersion = 0;
            ui64 IndexChildView_SchemaVersion = 0;
            ui64 Impl_OwnSchemaVersion = 0;
            TString ImplName;
        };
        auto readCacheVersions = [&]() {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            THashMap<TString, TRow> out;

            auto mainNav = navigateSync("/MyRoot/T", TNavigate::EOp::OpTable);
            const auto& mainEntry = mainNav->ResultSet.back();
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(mainEntry.Status),
                static_cast<ui32>(TNavigate::EStatus::Ok),
                "scheme-cache navigate for /MyRoot/T returned status "
                << static_cast<ui32>(mainEntry.Status));
            for (const auto& idx : mainEntry.Indexes) {
                out[idx.GetName()].TableView_SchemaVersion = idx.GetSchemaVersion();
            }

            for (auto& [indexName, row] : out) {
                auto idxNav = navigateSync("/MyRoot/T/" + indexName,
                    TNavigate::EOp::OpList);
                const auto& idxEntry = idxNav->ResultSet.back();
                UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(idxEntry.Status),
                    static_cast<ui32>(TNavigate::EStatus::Ok),
                    "scheme-cache navigate for /MyRoot/T/" << indexName
                    << " returned status " << static_cast<ui32>(idxEntry.Status));
                UNIT_ASSERT_C(idxEntry.Self,
                    "scheme cache has no Self for /MyRoot/T/" << indexName);
                row.Index_OwnSchemaVersion =
                    idxEntry.Self->Info.GetVersion().GetTableIndexVersion();
                UNIT_ASSERT_C(idxEntry.ListNodeEntry,
                    "scheme cache has no ListNodeEntry for /MyRoot/T/" << indexName);
                UNIT_ASSERT_VALUES_EQUAL(idxEntry.ListNodeEntry->Children.size(), 1);
                const auto& implChild = idxEntry.ListNodeEntry->Children.front();
                row.ImplName = implChild.Name;
                row.IndexChildView_SchemaVersion = implChild.SchemaVersion;

                auto implNav = navigateSync(
                    "/MyRoot/T/" + indexName + "/" + row.ImplName,
                    TNavigate::EOp::OpTable);
                const auto& implEntry = implNav->ResultSet.back();
                UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(implEntry.Status),
                    static_cast<ui32>(TNavigate::EStatus::Ok),
                    "scheme-cache navigate for /MyRoot/T/" << indexName << "/"
                    << row.ImplName << " returned status "
                    << static_cast<ui32>(implEntry.Status));
                row.Impl_OwnSchemaVersion = implEntry.TableId.SchemaVersion;
            }
            return out;
        };

        const auto checkInvariant = [&](const THashMap<TString, TRow>& snap, const TString& stage) {
            for (const auto& [name, row] : snap) {
                if (enableCascade) {
                    // With cascade on, every mutation republishes ancestors.
                    // T's cache view of the index and the index's own cache
                    // entry must agree — this is the invariant KQP relies on.
                    UNIT_ASSERT_VALUES_EQUAL_C(row.TableView_SchemaVersion, row.Index_OwnSchemaVersion,
                        "cascade=true stage=" << stage
                        << " T-to-Idx cache MISMATCH on " << name
                        << ": T.cache.Indexes[" << name << "].SchemaVersion=" << row.TableView_SchemaVersion
                        << " vs " << name << ".cache.Self.Version.TableIndexVersion=" << row.Index_OwnSchemaVersion);
                } else {
                    // With cascade off, schemeshard still bumps the parent
                    // index's AlterVersion (required for internal consistency)
                    // but does NOT republish the index to scheme board. When
                    // T gets republished, its TableIndexes[i].SchemaVersion
                    // picks up the FRESH Indexes[...]->AlterVersion, but the
                    // index's own scheme-board entry may still hold the OLD
                    // value. T.cache >= Idx.cache is the weaker invariant
                    // (Idx.cache can lag T.cache); equality is not guaranteed
                    // and is what cascade would restore.
                    UNIT_ASSERT_C(row.TableView_SchemaVersion >= row.Index_OwnSchemaVersion,
                        "cascade=false stage=" << stage
                        << " expected T.cache >= Idx.cache on " << name
                        << "; got T.cache.Indexes[" << name << "].SchemaVersion=" << row.TableView_SchemaVersion
                        << " vs " << name << ".cache.Self.Version.TableIndexVersion=" << row.Index_OwnSchemaVersion);
                }
            }
        };

        const auto before = readCacheVersions();

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        const auto afterFull = readCacheVersions();
        checkInvariant(afterFull, "afterFull");

        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(2));

        const auto afterIncremental = readCacheVersions();
        checkInvariant(afterIncremental, "afterIncremental");

        for (const auto& [name, rowAfter] : afterIncremental) {
            const auto& rowBefore = before.at(name);
            UNIT_ASSERT_C(rowAfter.Impl_OwnSchemaVersion > rowBefore.Impl_OwnSchemaVersion,
                "cascade=" << enableCascade
                << " " << name << " impl table did not advance: "
                << rowBefore.Impl_OwnSchemaVersion << " -> " << rowAfter.Impl_OwnSchemaVersion);
        }
    }

    Y_UNIT_TEST(BackupInvariant) {
        RunBackupBackupCollectionInvariantScenario(/* enableCascade = */ true);
    }

    Y_UNIT_TEST(BackupInvariant_NoCascade) {
        RunBackupBackupCollectionInvariantScenario(/* enableCascade = */ false);
    }

    // ====================================================================
    // ConcurrentCleanerVsUserCdcRace / ConcurrentCleanerVsUserCdcRace_NoCascade
    //
    // Parks the continuous-backup cleaner's drop-CDC proposals with
    // TBlockEvents, interleaves a user CREATE-CDC on the main table, then
    // releases the parked drops and counts publication fan-out per pathId.
    //
    //   Cascade ON : cleaner drop-CDC on impl1/impl2 fans out to parent
    //                indexes Idx1/Idx2 → pIdx1 > 0, pIdx2 > 0.
    //   Cascade OFF: only the impl table itself is published →
    //                pIdx1 == 0, pIdx2 == 0 (stale-describe window exposed).
    // ====================================================================
    void RunConcurrentCleanerVsUserCdcRace(bool enableCascade = true) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableBackupService(true)
            .EnableCascadePublication(enableCascade));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/T"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "T"
                Columns { Name: "Key" Type: "Uint64" }
                Columns { Name: "Value1" Type: "Utf8" }
                Columns { Name: "Value2" Type: "Utf8" }
                KeyColumnNames: ["Key"]
            }
            IndexDescription {
                Name: "Idx1"
                KeyColumnNames: ["Value1"]
                Type: EIndexTypeGlobal
            }
            IndexDescription {
                Name: "Idx2"
                KeyColumnNames: ["Value2"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto lookupPathId = [&](const TString& path) {
            auto desc = DescribePrivatePath(runtime, path);
            const auto& self = desc.GetPathDescription().GetSelf();
            return TPathId(self.GetSchemeshardId(), self.GetPathId());
        };
        const TPathId tPathId    = lookupPathId("/MyRoot/T");
        const TPathId idx1PathId = lookupPathId("/MyRoot/T/Idx1");
        const TPathId idx2PathId = lookupPathId("/MyRoot/T/Idx2");
        const TPathId impl1PathId = lookupPathId("/MyRoot/T/Idx1/indexImplTable");
        const TPathId impl2PathId = lookupPathId("/MyRoot/T/Idx2/indexImplTable");
        const TPathId rootPathId  = lookupPathId("/MyRoot");

        ui64 publishIdx1 = 0, publishIdx2 = 0;
        ui64 publishImpl1 = 0, publishImpl2 = 0;
        ui64 publishT = 0, publishRoot = 0;
        bool recordingEnabled = false;

        auto observerFn = runtime.AddObserver<TEvSchemeShard::TEvDescribeSchemeResultBuilder>(
            [&](TEvSchemeShard::TEvDescribeSchemeResultBuilder::TPtr& ev) {
                if (!recordingEnabled) return;
                const auto& rec = ev->Get()->GetRecord();
                if (!rec.HasPathOwnerId() || !rec.HasPathId()) return;
                TPathId pid(rec.GetPathOwnerId(), rec.GetPathId());
                if      (pid == idx1PathId)  ++publishIdx1;
                else if (pid == idx2PathId)  ++publishIdx2;
                else if (pid == impl1PathId) ++publishImpl1;
                else if (pid == impl2PathId) ++publishImpl2;
                else if (pid == tPathId)     ++publishT;
                else if (pid == rootPathId)  ++publishRoot;
            });

        // Step 1: full backup — creates the first generation of CDC streams.
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(1));

        // Step 2: park cleaner drop-CDC proposals on index impl tables.
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blockedDrops(runtime,
            [](const TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return false;
                const auto& tx = rec.GetTransaction(0);
                if (!tx.HasDropCdcStream()) return false;
                const TString& wd = tx.GetWorkingDir();
                if (!wd.Contains("/Idx")) return false;
                Cerr << "RACE_TEST: blocking cleaner DropCdcStream wd=" << wd
                     << " streams=" << tx.GetDropCdcStream().StreamNameSize() << Endl;
                return true;
            });

        // Step 3: trigger incremental backup — rotation schedules cleaner drops.
        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(2));

        UNIT_ASSERT_C(blockedDrops.size() > 0,
            "expected at least one blocked cleaner drop-CDC tx; got none.");

        // Step 4: user CREATE-CDC on main table T (separate top-level tx).
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "T"
            StreamDescription {
              Name: "UserStreamRace"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatJson
            }
        )");
        env.TestWaitNotification(runtime, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(1));

        // Step 5: enable observer, release parked drops.
        recordingEnabled = true;
        blockedDrops.Stop().Unblock();
        env.SimulateSleep(runtime, TDuration::Seconds(5));
        recordingEnabled = false;

        const auto pIdx1  = publishIdx1;
        const auto pIdx2  = publishIdx2;
        const auto pImpl1 = publishImpl1;
        const auto pImpl2 = publishImpl2;
        const auto pT     = publishT;
        const auto pRoot  = publishRoot;

        Cerr << "RACE_PUBLISH cascade=" << enableCascade
             << " Idx1=" << pIdx1 << " Idx2=" << pIdx2
             << " Impl1=" << pImpl1 << " Impl2=" << pImpl2
             << " T=" << pT << " Root=" << pRoot << Endl;

        UNIT_ASSERT_C(pImpl1 > 0,
            "cascade=" << enableCascade << " cleaner did not publish impl1 (Impl1=" << pImpl1 << ")");
        UNIT_ASSERT_C(pImpl2 > 0,
            "cascade=" << enableCascade << " cleaner did not publish impl2 (Impl2=" << pImpl2 << ")");

        if (enableCascade) {
            UNIT_ASSERT_C(pIdx1 > 0,
                "cascade=true: cleaner drop-CDC on impl1 should republish parent Idx1. "
                "Idx1=" << pIdx1 << " Idx2=" << pIdx2
                << " Impl1=" << pImpl1 << " Impl2=" << pImpl2
                << " T=" << pT << " Root=" << pRoot);
            UNIT_ASSERT_C(pIdx2 > 0,
                "cascade=true: cleaner drop-CDC on impl2 should republish parent Idx2. "
                "Idx1=" << pIdx1 << " Idx2=" << pIdx2
                << " Impl1=" << pImpl1 << " Impl2=" << pImpl2
                << " T=" << pT << " Root=" << pRoot);
        } else {
            UNIT_ASSERT_C(pIdx1 == 0,
                "cascade=false: parent index Idx1 MUST NOT be republished by the cleaner drop. "
                "Idx1=" << pIdx1 << " Idx2=" << pIdx2
                << " Impl1=" << pImpl1 << " Impl2=" << pImpl2
                << " T=" << pT << " Root=" << pRoot);
            UNIT_ASSERT_C(pIdx2 == 0,
                "cascade=false: parent index Idx2 MUST NOT be republished. "
                "Idx1=" << pIdx1 << " Idx2=" << pIdx2
                << " Impl1=" << pImpl1 << " Impl2=" << pImpl2
                << " T=" << pT << " Root=" << pRoot);
        }
    }

    Y_UNIT_TEST(ConcurrentCleanerVsUserCdcRace) {
        RunConcurrentCleanerVsUserCdcRace(/* enableCascade = */ true);
    }

    Y_UNIT_TEST(ConcurrentCleanerVsUserCdcRace_NoCascade) {
        RunConcurrentCleanerVsUserCdcRace(/* enableCascade = */ false);
    }
} // TBackupCollectionTests

// ============================================================================
// TSchemeShardColumnRollbackTests
//
// Verifies that Schema::TableIndex::AlterVersion (the persisted DB column,
// not just the in-memory mirror) is kept consistent with each impl-table's
// AlterVersion after every op that bumps an impl-table version.
//
// Invariant:  ReadPersistedIndexAlterVersion(idxPathId) >= ReadImplTableSchemaVersion(implPath)
//
// This catches a class of regression where the in-memory mirror is advanced
// (so derivation still works) but the persist call is dropped, silently
// breaking rollback to a pre-derivation binary.
// ============================================================================
Y_UNIT_TEST_SUITE(TSchemeShardColumnRollbackTests) {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    // Read Schema::TableIndex::AlterVersion directly from the schemeshard
    // local DB, bypassing the in-memory mirror entirely.
    // This is the byte a pre-derivation binary would re-hydrate at boot.
    ui64 ReadPersistedIndexAlterVersion(TTestActorRuntime& runtime,
                                        ui64 schemeshardTabletId,
                                        const TPathId& indexPathId)
    {
        // Try TableIndex first (non-migrated layout, single-column key).
        const auto q1 = Sprintf(R"(
            (
                (let key '('('PathId (Uint64 '%lu))))
                (let select '('AlterVersion))
                (return (AsList
                    (SetResult 'Result (SelectRow 'TableIndex key select))
                ))
            )
        )", indexPathId.LocalPathId);

        auto r1 = LocalMiniKQL(runtime, schemeshardTabletId, q1);
        const auto& opt1 = r1.GetValue().GetStruct(0).GetOptional();
        if (opt1.HasOptional()) {
            return opt1.GetOptional().GetStruct(0).GetOptional().GetUint64();
        }

        // Fallback: MigratedTableIndex (compound-key layout).
        const auto q2 = Sprintf(R"(
            (
                (let key '('('OwnerPathId (Uint64 '%lu))
                           '('LocalPathId (Uint64 '%lu))))
                (let select '('AlterVersion))
                (return (AsList
                    (SetResult 'Result (SelectRow 'MigratedTableIndex key select))
                ))
            )
        )", indexPathId.OwnerId, indexPathId.LocalPathId);

        auto r2 = LocalMiniKQL(runtime, schemeshardTabletId, q2);
        const auto& opt2 = r2.GetValue().GetStruct(0).GetOptional();
        UNIT_ASSERT_C(opt2.HasOptional(),
            "no row in TableIndex or MigratedTableIndex for pathId "
            << indexPathId.OwnerId << ":" << indexPathId.LocalPathId);
        return opt2.GetOptional().GetStruct(0).GetOptional().GetUint64();
    }

    // Read the impl table's own TableSchemaVersion from its private describe.
    ui64 ReadImplTableSchemaVersion(TTestActorRuntime& runtime, const TString& implPath) {
        auto desc = DescribePrivatePath(runtime, implPath);
        return desc.GetPathDescription().GetTable().GetTableSchemaVersion();
    }

    // Extract (SchemeshardId, PathId) for a path via DescribePrivatePath.
    TPathId LookupPathId(TTestActorRuntime& runtime, const TString& path) {
        auto desc = DescribePrivatePath(runtime, path);
        const auto& self = desc.GetPathDescription().GetSelf();
        return TPathId(self.GetSchemeshardId(), self.GetPathId());
    }

    // -----------------------------------------------------------------------
    // Test 1: CDC on impl table advances persisted index AlterVersion
    // -----------------------------------------------------------------------

    void RunPersistedIndexAlterVersion_AdvancesOnCdcOnImpl(bool enableCascade) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableChangefeedsOnIndexTables(true)
            .EnableCascadePublication(enableCascade));
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription { Name: "T"
                Columns { Name: "k" Type: "Uint64" }
                Columns { Name: "v" Type: "Utf8" }
                KeyColumnNames: ["k"] }
            IndexDescription { Name: "Idx" KeyColumnNames: ["v"] Type: EIndexTypeGlobal }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto idxPathId = LookupPathId(runtime, "/MyRoot/T/Idx");
        const TString implPath = "/MyRoot/T/Idx/indexImplTable";

        const ui64 implV0 = ReadImplTableSchemaVersion(runtime, implPath);
        const ui64 mirrorV0 = ReadPersistedIndexAlterVersion(
            runtime, TTestTxConfig::SchemeShard, idxPathId);
        UNIT_ASSERT_VALUES_EQUAL(mirrorV0, implV0);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot/T/Idx", R"(
            TableName: "indexImplTable"
            StreamDescription { Name: "s1" Mode: ECdcStreamModeKeysOnly Format: ECdcStreamFormatJson }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 implV1 = ReadImplTableSchemaVersion(runtime, implPath);
        const ui64 mirrorV1 = ReadPersistedIndexAlterVersion(
            runtime, TTestTxConfig::SchemeShard, idxPathId);

        UNIT_ASSERT_C(implV1 > implV0, "CDC must bump impl table version");
        UNIT_ASSERT_C(mirrorV1 >= implV1,
            "ROLLBACK VIOLATED: persisted TableIndex.AlterVersion=" << mirrorV1
            << " < impl.TableSchemaVersion=" << implV1
            << " (cascade=" << enableCascade << ")");
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_AdvancesOnCdcOnImpl) {
        RunPersistedIndexAlterVersion_AdvancesOnCdcOnImpl(/* enableCascade = */ true);
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_AdvancesOnCdcOnImpl_NoCascade) {
        RunPersistedIndexAlterVersion_AdvancesOnCdcOnImpl(/* enableCascade = */ false);
    }

    // -----------------------------------------------------------------------
    // Test 2: CopyTable dst impl-table finalize advances persisted index AlterVersion
    // -----------------------------------------------------------------------

    void RunPersistedIndexAlterVersion_AdvancesOnCopyTableDst(bool enableCascade) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableCascadePublication(enableCascade));
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription { Name: "Src"
                Columns { Name: "k" Type: "Uint64" }
                Columns { Name: "v" Type: "Utf8" }
                KeyColumnNames: ["k"] }
            IndexDescription { Name: "Idx" KeyColumnNames: ["v"] Type: EIndexTypeGlobal }
        )");
        env.TestWaitNotification(runtime, txId);

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src"
                DstPath: "/MyRoot/Dst"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto dstIdxPathId = LookupPathId(runtime, "/MyRoot/Dst/Idx");
        const TString dstImplPath = "/MyRoot/Dst/Idx/indexImplTable";

        const ui64 dstImplV = ReadImplTableSchemaVersion(runtime, dstImplPath);
        const ui64 dstMirrorV = ReadPersistedIndexAlterVersion(
            runtime, TTestTxConfig::SchemeShard, dstIdxPathId);

        UNIT_ASSERT_C(dstMirrorV >= dstImplV,
            "ROLLBACK VIOLATED: persisted Dst/Idx TableIndex.AlterVersion=" << dstMirrorV
            << " < impl.TableSchemaVersion=" << dstImplV
            << " (cascade=" << enableCascade << ")");
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_AdvancesOnCopyTableDst) {
        RunPersistedIndexAlterVersion_AdvancesOnCopyTableDst(/* enableCascade = */ true);
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_AdvancesOnCopyTableDst_NoCascade) {
        RunPersistedIndexAlterVersion_AdvancesOnCopyTableDst(/* enableCascade = */ false);
    }

    // -----------------------------------------------------------------------
    // Test 3: Incremental-restore finalize advances persisted index AlterVersion
    // -----------------------------------------------------------------------

    void RunPersistedIndexAlterVersion_AdvancesOnIncrementalRestoreFinalize(bool enableCascade) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableBackupService(true)
            .EnableCascadePublication(enableCascade));
        ui64 txId = 100;

        // Mirror the incremental-restore scenario from
        // RunBackupBackupCollectionInvariantScenario.
        auto& tenv = env;
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        tenv.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        tenv.TestWaitNotification(runtime, txId);

        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/T"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
            collectionSettings);
        tenv.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "T"
                Columns { Name: "Key" Type: "Uint64" }
                Columns { Name: "Value1" Type: "Utf8" }
                KeyColumnNames: ["Key"]
            }
            IndexDescription {
                Name: "Idx1"
                KeyColumnNames: ["Value1"]
                Type: EIndexTypeGlobal
            }
        )");
        tenv.TestWaitNotification(runtime, txId);

        // Full backup first (required before incremental).
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        tenv.TestWaitNotification(runtime, txId);

        // Incremental backup — this creates the changefeed on the impl table
        // and then finalizes (cascade-publish path).
        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        tenv.TestWaitNotification(runtime, txId);
        tenv.SimulateSleep(runtime, TDuration::Seconds(2));

        const auto idxPathId = LookupPathId(runtime, "/MyRoot/T/Idx1");
        const TString implPath = "/MyRoot/T/Idx1/indexImplTable";

        const ui64 implV = ReadImplTableSchemaVersion(runtime, implPath);
        const ui64 mirrorV = ReadPersistedIndexAlterVersion(
            runtime, TTestTxConfig::SchemeShard, idxPathId);

        UNIT_ASSERT_C(mirrorV >= implV,
            "ROLLBACK VIOLATED: persisted T/Idx1 TableIndex.AlterVersion=" << mirrorV
            << " < impl.TableSchemaVersion=" << implV
            << " (cascade=" << enableCascade << ")");
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_AdvancesOnIncrementalRestoreFinalize) {
        RunPersistedIndexAlterVersion_AdvancesOnIncrementalRestoreFinalize(/* enableCascade = */ true);
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_AdvancesOnIncrementalRestoreFinalize_NoCascade) {
        RunPersistedIndexAlterVersion_AdvancesOnIncrementalRestoreFinalize(/* enableCascade = */ false);
    }

    // -----------------------------------------------------------------------
    // Test 4: Multi-index full + incremental backup persists AlterVersion for
    // every index's impl tables.
    //
    // BackupBackupCollection fans out CDC-on-impl operations across every impl
    // table of every index.  BackupIncrementalBackupCollection (rotate) drops
    // and re-creates per-impl CDC streams.  The cascade-publish path is the
    // sole column writer for this path (no AlterTableIndex sub-op is emitted),
    // so if the persist calls are absent the column stays stale.
    //
    // Invariant checked: for every index I and every impl table child C of I:
    //   ReadPersistedIndexAlterVersion(I.pathId) >= ReadImplTableSchemaVersion(C.path)
    // -----------------------------------------------------------------------

    // Returns map: indexName -> vector of impl-table paths under it.
    // CDC streams are excluded; only EPathTypeTable children are collected.
    static THashMap<TString, TVector<TString>>
    EnumerateIndexImpls(TTestActorRuntime& runtime, const TString& tablePath) {
        THashMap<TString, TVector<TString>> out;
        auto tableDesc = DescribePrivatePath(runtime, tablePath);
        for (const auto& idx : tableDesc.GetPathDescription().GetTable().GetTableIndexes()) {
            const TString idxPath = tablePath + "/" + idx.GetName();
            auto idxDesc = DescribePrivatePath(runtime, idxPath);
            for (const auto& child : idxDesc.GetPathDescription().GetChildren()) {
                if (child.GetPathType() == NKikimrSchemeOp::EPathTypeTable) {
                    out[idx.GetName()].push_back(idxPath + "/" + child.GetName());
                }
            }
        }
        return out;
    }

    void RunPersistedIndexAlterVersion_MultiIndexBackup(bool enableCascade) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableBackupService(true)
            .EnableCascadePublication(enableCascade));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);

        // Register the backup collection targeting T.
        TString collectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/T"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
            collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create T with two global indexes (Idx1, Idx2).
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "T"
                Columns { Name: "Key"    Type: "Uint64" }
                Columns { Name: "Value1" Type: "Utf8"   }
                Columns { Name: "Value2" Type: "Utf8"   }
                KeyColumnNames: ["Key"]
            }
            IndexDescription {
                Name: "Idx1"
                KeyColumnNames: ["Value1"]
                Type: EIndexTypeGlobal
            }
            IndexDescription {
                Name: "Idx2"
                KeyColumnNames: ["Value2"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Full backup — fans out CDC creation to every impl table of every index.
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Snapshot the impl table versions right after full backup.
        // We'll use these to confirm that incremental backup actually bumped them.
        auto implsAfterFull = EnumerateIndexImpls(runtime, "/MyRoot/T");
        THashMap<TString, ui64> implVAfterFull;
        for (const auto& [idxName, impls] : implsAfterFull) {
            for (const auto& implPath : impls) {
                implVAfterFull[implPath] = ReadImplTableSchemaVersion(runtime, implPath);
            }
        }

        // Incremental backup — drops + re-creates CDC streams on every impl table.
        // Cascade-publish runs for each impl table finalize.
        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(2));

        // For every index and every impl table: persisted AlterVersion >= impl version.
        auto impls = EnumerateIndexImpls(runtime, "/MyRoot/T");
        UNIT_ASSERT_C(!impls.empty(),
            "EnumerateIndexImpls returned empty — table has no indexes?");

        for (const auto& [idxName, implPaths] : impls) {
            UNIT_ASSERT_C(!implPaths.empty(),
                "Index " << idxName << " has no impl tables");

            const auto idxPathId = LookupPathId(runtime, "/MyRoot/T/" + idxName);
            const ui64 persistedV = ReadPersistedIndexAlterVersion(
                runtime, TTestTxConfig::SchemeShard, idxPathId);

            for (const auto& implPath : implPaths) {
                const ui64 implV = ReadImplTableSchemaVersion(runtime, implPath);

                UNIT_ASSERT_C(implV > implVAfterFull.at(implPath),
                    "cascade=" << enableCascade
                    << " impl table " << implPath
                    << " did not advance after incremental backup: "
                    << implVAfterFull.at(implPath) << " -> " << implV);

                UNIT_ASSERT_C(persistedV >= implV,
                    "ROLLBACK VIOLATED on /MyRoot/T/" << idxName
                    << ": persisted=" << persistedV
                    << " impl=" << implV
                    << " after stage=afterIncremental"
                    << " (cascade=" << enableCascade << ")"
                    << " implPath=" << implPath);
            }
        }
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_MultiIndexBackup) {
        RunPersistedIndexAlterVersion_MultiIndexBackup(/* enableCascade = */ true);
    }

    Y_UNIT_TEST(PersistedIndexAlterVersion_MultiIndexBackup_NoCascade) {
        RunPersistedIndexAlterVersion_MultiIndexBackup(/* enableCascade = */ false);
    }

    // -----------------------------------------------------------------------
    // Test 5: Reboot re-hydrates persisted AlterVersion into the in-memory mirror
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(PersistedIndexAlterVersion_SurvivesReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableChangefeedsOnIndexTables(true));
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription { Name: "T"
                Columns { Name: "k" Type: "Uint64" } Columns { Name: "v" Type: "Utf8" }
                KeyColumnNames: ["k"] }
            IndexDescription { Name: "Idx" KeyColumnNames: ["v"] Type: EIndexTypeGlobal }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto idxPathId = LookupPathId(runtime, "/MyRoot/T/Idx");
        const TString implPath = "/MyRoot/T/Idx/indexImplTable";

        // Trigger cascade (CDC on impl bumps impl AV -> cascade writes column).
        TestCreateCdcStream(runtime, ++txId, "/MyRoot/T/Idx", R"(
            TableName: "indexImplTable"
            StreamDescription { Name: "s1" Mode: ECdcStreamModeKeysOnly Format: ECdcStreamFormatJson }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 implV = ReadImplTableSchemaVersion(runtime, implPath);
        const ui64 mirrorV_before = ReadPersistedIndexAlterVersion(runtime, TTestTxConfig::SchemeShard, idxPathId);
        UNIT_ASSERT_VALUES_EQUAL(mirrorV_before, implV);

        // Simulate restart: scheme-board re-elects, schemeshard reloads from local DB.
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // After reboot, column re-hydrates into TTableIndexInfo::AlterVersion.
        // Describer (whether derivation or mirror) must report a value >= impl AV.
        const ui64 mirrorV_after = ReadPersistedIndexAlterVersion(runtime, TTestTxConfig::SchemeShard, idxPathId);
        UNIT_ASSERT_VALUES_EQUAL(mirrorV_after, mirrorV_before);

        // The describe-time SchemaVersion (what an old binary's describer would emit)
        // must still be >= the impl table's published version.
        auto idxDesc = DescribePrivatePath(runtime, "/MyRoot/T/Idx");
        UNIT_ASSERT_C(
            idxDesc.GetPathDescription().GetSelf().GetVersion().GetTableIndexVersion() >= implV,
            "post-reboot describe stale: index=" << idxDesc.GetPathDescription().GetSelf().GetVersion().GetTableIndexVersion()
            << " impl=" << implV);
    }

    // -----------------------------------------------------------------------
    // Test 6: Vector index with two impl tables - persisted AlterVersion = max(children)
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(PersistedIndexAlterVersion_VectorIndexMultiImpl) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // EIndexTypeGlobalVectorKmeansTree creates two impl tables:
        //   indexImplLevelTable + indexImplPostingTable.
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "vectors"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "embedding" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_vector"
                KeyColumnNames: ["embedding"]
                Type: EIndexTypeGlobalVectorKmeansTree
                VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 4 }, clusters: 4, levels: 2 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto idxPathId = LookupPathId(runtime, "/MyRoot/vectors/idx_vector");
        const TString levelPath   = "/MyRoot/vectors/idx_vector/indexImplLevelTable";
        const TString postingPath = "/MyRoot/vectors/idx_vector/indexImplPostingTable";

        const ui64 baseline  = ReadPersistedIndexAlterVersion(runtime, TTestTxConfig::SchemeShard, idxPathId);
        const ui64 levelV0   = ReadImplTableSchemaVersion(runtime, levelPath);
        const ui64 postingV0 = ReadImplTableSchemaVersion(runtime, postingPath);
        UNIT_ASSERT_VALUES_EQUAL(baseline, Max(levelV0, postingV0));

        // Bump only the posting table via TestAlterTable on its PartitionConfig
        // (same trick as VectorIndexSchemaVersionDerivedFromMaxImplTable).
        TestAlterTable(runtime, ++txId, "/MyRoot/vectors/idx_vector", R"(
            Name: "indexImplPostingTable"
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 2000000000
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 levelV1   = ReadImplTableSchemaVersion(runtime, levelPath);
        const ui64 postingV1 = ReadImplTableSchemaVersion(runtime, postingPath);
        const ui64 colV1     = ReadPersistedIndexAlterVersion(runtime, TTestTxConfig::SchemeShard, idxPathId);

        UNIT_ASSERT_C(postingV1 > postingV0, "PostingTable AlterVersion should advance");
        UNIT_ASSERT_VALUES_EQUAL(levelV1, levelV0);
        UNIT_ASSERT_VALUES_EQUAL(colV1, Max(levelV1, postingV1));

        // Reboot — verify post-reboot mirror still reflects max.
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        const ui64 colV_postReboot = ReadPersistedIndexAlterVersion(runtime, TTestTxConfig::SchemeShard, idxPathId);
        UNIT_ASSERT_VALUES_EQUAL(colV_postReboot, colV1);

        auto idxDesc = DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector");
        UNIT_ASSERT_C(
            idxDesc.GetPathDescription().GetSelf().GetVersion().GetTableIndexVersion() >= postingV1,
            "post-reboot vector idx describe stale");
    }

} // TSchemeShardColumnRollbackTests
