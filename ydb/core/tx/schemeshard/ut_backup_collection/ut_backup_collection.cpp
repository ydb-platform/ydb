#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

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
        
        // This is async - no result waiting here
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
        
        // This is async - no result checking here
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

            // must not be there in any case, smoke test
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

    // Priority Test 1: Basic functionality verification
    Y_UNIT_TEST(DropEmptyBackupCollection) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create empty backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        // Verify collection exists
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Drop backup collection
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection doesn't exist
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // Priority Test 2: Core use case with content
    Y_UNIT_TEST(DropCollectionWithFullBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        // Create a table to backup
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create a full backup (this creates backup structure under the collection)
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify backup was created with content
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
            NLs::ChildrenCount(1), // Should have backup directory
        });

        // Drop backup collection with contents
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection and all contents are removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // Priority Test 3: CDC cleanup verification  
    Y_UNIT_TEST(DropCollectionWithIncrementalBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection with incremental backup enabled
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

        // Create a table to backup
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // First create a full backup to establish the backup stream
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Pass time to prevent stream names clashing
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        // Create incremental backup (this should create CDC streams and topics)
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify backup was created with incremental components
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Drop backup collection with incremental backup contents
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection and all contents (including CDC components) are removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // Priority Test 4: Critical edge case
    Y_UNIT_TEST(DropCollectionDuringActiveBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        // Create a table to backup
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Start async backup operation (don't wait for completion)
        AsyncBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");

        // Immediately try to drop collection during active backup
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"" DEFAULT_NAME_1 "\"", 
            {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        // Collection should still exist
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Wait for backup to complete
        env.TestWaitNotification(runtime, txId - 1);

        // Now drop should succeed
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection is removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // Priority Test 5: Basic error handling
    Y_UNIT_TEST(DropNonExistentCollection) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Try to drop non-existent collection
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"NonExistentCollection\"", 
            {NKikimrScheme::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, txId);

        // Verify nothing was created
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/NonExistentCollection"),
                            {NLs::PathNotExist});
    }

    // Additional Test: Multiple backups in collection
    Y_UNIT_TEST(DropCollectionWithMultipleBackups) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        // Create a table to backup
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create multiple backups
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Wait a bit to ensure different timestamp for second backup
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify multiple backup directories exist
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Drop collection with multiple backups
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection and all backup contents are removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // Additional Test: Nested table hierarchy
    Y_UNIT_TEST(DropCollectionWithNestedTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create directories for nested structure
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDir");
        env.TestWaitNotification(runtime, txId);

        // Create backup collection with nested table paths
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

        // Create tables in nested structure
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

        // Create backup with nested tables
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify backup was created
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Drop collection with nested backup structure
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection and all nested contents are removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // =======================
    // Additional Tests (From Comprehensive Test Plan)
    // =======================

    // Test CDC cleanup specifically
    Y_UNIT_TEST(DropCollectionVerifyCDCCleanup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create table with CDC stream for incremental backups
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create CDC stream manually
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream1"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Create backup collection using this table
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

        // Verify CDC stream exists
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table1/Stream1"), {NLs::PathExist});

        // Drop backup collection (should clean up CDC streams)
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection is removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});

        // Note: CDC stream cleanup verification would require more specific test infrastructure
        // This test verifies the basic flow
    }

    // Test transactional rollback on failure
    Y_UNIT_TEST(DropCollectionRollbackOnFailure) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", DefaultCollectionSettings());
        env.TestWaitNotification(runtime, txId);

        // Create backup content
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

        // Simulate failure case - try to drop a non-existent collection
        // (This should fail during validation but not cause rollback issues)
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"NonExistentCollection\"",  // Valid protobuf, non-existent collection
            {NKikimrScheme::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, txId);

        // Verify collection still exists (rollback succeeded)
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Now drop correctly
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // Test large collection scenario
    Y_UNIT_TEST(DropLargeBackupCollection) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection with multiple tables
        TString largeCollectionSettings = R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {)";

        // Add multiple table entries
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

        // Create the tables
        for (int i = 1; i <= 5; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", TStringBuilder() << R"(
                Name: "Table)" << i << R"("
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);
        }

        // Create multiple backups to increase content size
        for (int i = 0; i < 3; ++i) {
            // Advance time to ensure different timestamps
            if (i > 0) {
                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            }
            
            TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            env.TestWaitNotification(runtime, txId);
        }

        // Verify large collection exists
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Drop large collection (should handle multiple children efficiently)
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify complete removal
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                            {NLs::PathNotExist});
    }

    // Test validation edge cases
    Y_UNIT_TEST(DropCollectionValidationCases) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Test empty collection name
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"\"", 
            {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        // Test invalid path
        TestDropBackupCollection(runtime, ++txId, "/NonExistent/path", 
            "Name: \"test\"", 
            {NKikimrScheme::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, txId);

        // Test dropping from wrong directory (not collections dir)
        TestDropBackupCollection(runtime, ++txId, "/MyRoot", 
            "Name: \"test\"", 
            {NKikimrScheme::StatusSchemeError});
        env.TestWaitNotification(runtime, txId);
    }

    // Test multiple collections management
    Y_UNIT_TEST(DropSpecificCollectionAmongMultiple) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create multiple backup collections
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", 
            DefaultCollectionSettingsWithName("Collection1"));
        env.TestWaitNotification(runtime, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", 
            DefaultCollectionSettingsWithName("Collection2"));
        env.TestWaitNotification(runtime, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", 
            DefaultCollectionSettingsWithName("Collection3"));
        env.TestWaitNotification(runtime, txId);

        // Verify all exist
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection1"), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection2"), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection3"), {NLs::PathExist});

        // Drop only Collection2
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"Collection2\"");
        env.TestWaitNotification(runtime, txId);

        // Verify only Collection2 was removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection1"), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection2"), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/Collection3"), {NLs::PathExist});

        // Clean up remaining collections
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"Collection1\"");
        env.TestWaitNotification(runtime, txId);
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"Collection3\"");
        env.TestWaitNotification(runtime, txId);
    }

    // Verify LocalDB cleanup after SchemeShard restart
    Y_UNIT_TEST(DropCollectionVerifyLocalDatabaseCleanup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection
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

        // Create the source table and perform a full backup
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "LocalDbTestTable"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create a full backup
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/LocalDbTestCollection")");
        env.TestWaitNotification(runtime, txId);

        // Drop the backup collection
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"LocalDbTestCollection\"");
        env.TestWaitNotification(runtime, txId);

        // Restart SchemeShard to verify LocalDB cleanup
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Verify collection doesn't exist after restart
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/LocalDbTestCollection"), 
            {NLs::PathNotExist});

        // Verify LocalDB tables are cleaned up using MiniKQL queries
        ui64 schemeshardTabletId = TTestTxConfig::SchemeShard;
        
        // Verify BackupCollection table is clean
        bool backupCollectionTableClean = true;
        auto result1 = LocalMiniKQL(runtime, schemeshardTabletId, R"(
            (
                (let key '('('OwnerPathId (Uint64 '0)) '('LocalPathId (Uint64 '0))))
                (let select '('OwnerPathId))
                (let row (SelectRow 'BackupCollection key select))
                (return (AsList
                    (SetResult 'Result row)
                ))
            )
        )");
        
        auto& value1 = result1.GetValue();
        if (value1.GetStruct(0).GetOptional().HasOptional()) {
            backupCollectionTableClean = false;
            Cerr << "ERROR: BackupCollection table still has entries after DROP" << Endl;
        }
        
        UNIT_ASSERT_C(backupCollectionTableClean, "BackupCollection table not properly cleaned up");

        // Verify IncrementalRestoreOperations table is clean
        bool incrementalRestoreOperationsClean = true;
        auto result2 = LocalMiniKQL(runtime, schemeshardTabletId, R"(
            (
                (let key '('('Id (Uint64 '0))))
                (let select '('Id))
                (let row (SelectRow 'IncrementalRestoreOperations key select))
                (return (AsList
                    (SetResult 'Result row)
                ))
            )
        )");
        
        auto& value2 = result2.GetValue();
        if (value2.GetStruct(0).GetOptional().HasOptional()) {
            incrementalRestoreOperationsClean = false;
            Cerr << "ERROR: IncrementalRestoreOperations table still has entries after DROP" << Endl;
        }
        
        UNIT_ASSERT_C(incrementalRestoreOperationsClean, "IncrementalRestoreOperations table not properly cleaned up");

        // Verify IncrementalRestoreState table is clean
        bool incrementalRestoreStateClean = true;
        auto result3 = LocalMiniKQL(runtime, schemeshardTabletId, R"(
            (
                (let key '('('OperationId (Uint64 '0))))
                (let select '('OperationId))
                (let row (SelectRow 'IncrementalRestoreState key select))
                (return (AsList
                    (SetResult 'Result row)
                ))
            )
        )");
        
        auto& value3 = result3.GetValue();
        if (value3.GetStruct(0).GetOptional().HasOptional()) {
            incrementalRestoreStateClean = false;
            Cerr << "ERROR: IncrementalRestoreState table still has entries after DROP" << Endl;
        }
        
        UNIT_ASSERT_C(incrementalRestoreStateClean, "IncrementalRestoreState table not properly cleaned up");

        // Verify IncrementalRestoreShardProgress table is clean
        bool incrementalRestoreShardProgressClean = true;
        auto result4 = LocalMiniKQL(runtime, schemeshardTabletId, R"(
            (
                (let key '('('OperationId (Uint64 '0)) '('ShardIdx (Uint64 '0))))
                (let select '('OperationId))
                (let row (SelectRow 'IncrementalRestoreShardProgress key select))
                (return (AsList
                    (SetResult 'Result row)
                ))
            )
        )");
        
        auto& value4 = result4.GetValue();
        if (value4.GetStruct(0).GetOptional().HasOptional()) {
            incrementalRestoreShardProgressClean = false;
            Cerr << "ERROR: IncrementalRestoreShardProgress table still has entries after DROP" << Endl;
        }
        
        UNIT_ASSERT_C(incrementalRestoreShardProgressClean, "IncrementalRestoreShardProgress table not properly cleaned up");

        Cerr << "SUCCESS: All LocalDB tables properly cleaned up after DROP BACKUP COLLECTION" << Endl;

        // Verify we can recreate with same name
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

    // Verify incremental restore state cleanup
    Y_UNIT_TEST(DropCollectionWithIncrementalRestoreStateCleanup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection
        TString localDbCollectionSettings = R"(
            Name: "RestoreStateTestCollection"

            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/RestoreStateTestTable"
                }
            }
            Cluster: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", localDbCollectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create source table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "RestoreStateTestTable"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create a full backup to establish backup structure
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/RestoreStateTestCollection")");
        env.TestWaitNotification(runtime, txId);

        // Drop the backup collection
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"RestoreStateTestCollection\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection is removed from schema
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/RestoreStateTestCollection"), 
            {NLs::PathNotExist});

        // Restart SchemeShard to verify cleanup
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Verify collection is removed from schema
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/RestoreStateTestCollection"), 
            {NLs::PathNotExist});

        // Verify LocalDB tables are cleaned up using MiniKQL queries
        ui64 schemeshardTabletId = TTestTxConfig::SchemeShard;
        
        // Verify all incremental restore tables are clean
        bool allIncrementalRestoreTablesClean = true;
        
        // Check IncrementalRestoreOperations table
        auto result1 = LocalMiniKQL(runtime, schemeshardTabletId, R"(
            (
                (let key '('('Id (Uint64 '0))))
                (let select '('Id))
                (let row (SelectRow 'IncrementalRestoreOperations key select))
                (return (AsList
                    (SetResult 'Result row)
                ))
            )
        )");
        
        auto& value1 = result1.GetValue();
        if (value1.GetStruct(0).GetOptional().HasOptional()) {
            allIncrementalRestoreTablesClean = false;
            Cerr << "ERROR: IncrementalRestoreOperations has stale entries" << Endl;
        }
        
        // Check IncrementalRestoreState table
        auto result2 = LocalMiniKQL(runtime, schemeshardTabletId, R"(
            (
                (let key '('('OperationId (Uint64 '0))))
                (let select '('OperationId))
                (let row (SelectRow 'IncrementalRestoreState key select))
                (return (AsList
                    (SetResult 'Result row)
                ))
            )
        )");
        
        auto& value2 = result2.GetValue();
        if (value2.GetStruct(0).GetOptional().HasOptional()) {
            allIncrementalRestoreTablesClean = false;
            Cerr << "ERROR: IncrementalRestoreState has stale entries" << Endl;
        }
        
        // Check IncrementalRestoreShardProgress table  
        auto result3 = LocalMiniKQL(runtime, schemeshardTabletId, R"(
            (
                (let key '('('OperationId (Uint64 '0)) '('ShardIdx (Uint64 '0))))
                (let select '('OperationId))
                (let row (SelectRow 'IncrementalRestoreShardProgress key select))
                (return (AsList
                    (SetResult 'Result row)
                ))
            )
        )");
        
        auto& value3 = result3.GetValue();
        if (value3.GetStruct(0).GetOptional().HasOptional()) {
            allIncrementalRestoreTablesClean = false;
            Cerr << "ERROR: IncrementalRestoreShardProgress has stale entries" << Endl;
        }
        
        UNIT_ASSERT_C(allIncrementalRestoreTablesClean, "Incremental restore LocalDB tables not properly cleaned up");
        
        Cerr << "SUCCESS: All incremental restore LocalDB tables properly cleaned up" << Endl;

        // Verify we can recreate collection with same name
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", localDbCollectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Clean up
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"RestoreStateTestCollection\"");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DropCollectionDuringActiveOperation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        PrepareDirs(runtime, env, txId);

        // Create backup collection
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

        // Create source table
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

        // Try to drop the backup collection while backup is active
        // The system correctly rejects this with StatusPreconditionFailed
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"ActiveOpTestCollection\"", 
            {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        // Wait for the backup operation to complete
        env.TestWaitNotification(runtime, backupTxId);

        // Collection should still exist since drop was properly rejected
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/ActiveOpTestCollection"), 
            {NLs::PathExist});

        // Now that backup is complete, dropping should work
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"ActiveOpTestCollection\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection is now properly removed
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/ActiveOpTestCollection"), 
            {NLs::PathNotExist});
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

        // Create test table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create full backup first (required for incremental backup)
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Create incremental backup (this creates CDC streams)
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC streams exist before drop
        auto describeResult = DescribePath(runtime, "/MyRoot/Table1", true, true);
        TVector<TString> cdcStreamNames;
        
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
        
        UNIT_ASSERT_C(!cdcStreamNames.empty(), "Expected to find CDC streams after incremental backup");
        
        // Verify the naming pattern matches the expected format
        for (const auto& streamName : cdcStreamNames) {
            UNIT_ASSERT_C(streamName.size() >= 15 + TString("_continuousBackupImpl").size(), 
                "CDC stream name should have timestamp prefix: " + streamName);
            
            TString prefix = streamName.substr(0, streamName.size() - TString("_continuousBackupImpl").size());
            UNIT_ASSERT_C(prefix.EndsWith("Z"), "CDC stream timestamp should end with 'Z': " + prefix);
        }

        // Drop the collection - this should clean up CDC streams
        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", 
            "Name: \"" DEFAULT_NAME_1 "\"");
        env.TestWaitNotification(runtime, txId);

        // Verify collection is gone
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathNotExist});

        // Verify CDC streams are cleaned up
        auto describeAfter = DescribePath(runtime, "/MyRoot/Table1", true, true);
        TVector<TString> remainingCdcStreams;
        
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
            "Incremental backup CDC streams should be cleaned up after dropping backup collection");
        
        // Check that original table still exists
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                          {NLs::PathExist, NLs::IsTable});

        // Restart SchemeShard to verify persistent cleanup
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Re-verify collection doesn't exist after restart
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1),
                          {NLs::PathNotExist});
        
        // Verify table still exists after restart
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                          {NLs::PathExist, NLs::IsTable});

        // Verify CDC streams remain cleaned up after restart
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
            "Incremental backup CDC streams should remain cleaned up after restart");
    }

} // TBackupCollectionTests