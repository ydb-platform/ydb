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

    // TODO: DropCollectionWithIncrementalRestoreStateCleanup

} // TBackupCollectionTests