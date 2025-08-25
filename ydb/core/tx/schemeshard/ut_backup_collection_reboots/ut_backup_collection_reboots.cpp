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

    TString DefaultIncrementalMultiTableCollectionSettings() {
        return R"(
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

    Y_UNIT_TEST(BackupBackupCollectionWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(1),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << DEFAULT_NAME_1 << '/' << b;
                    TVector<TString> tables;
                    TestDescribeResult(DescribePath(runtime,  backupPath), {
                        NLs::PathExist,
                        NLs::ChildrenCount(1),
                        NLs::ExtractChildren(&tables),
                        NLs::Finished,
                    });

                    for (const auto& t : tables) {
                        TString tablePath = TStringBuilder() << backupPath << '/' << t;
                        TestDescribeResult(DescribePath(runtime,  tablePath), {
                            NLs::PathExist,
                            NLs::IsTable,
                            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                            NLs::Finished,
                        });
                    }
                }

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }

    Y_UNIT_TEST(BackupIncrementalBackupCollectionWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultIncrementalCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(3),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << DEFAULT_NAME_1 << '/' << b;
                    TVector<TString> tables;
                    TestDescribeResult(DescribePath(runtime,  backupPath), {
                        NLs::PathExist,
                        NLs::ChildrenCount(1),
                        NLs::ExtractChildren(&tables),
                        NLs::Finished,
                    });

                    for (const auto& t : tables) {
                        TString tablePath = TStringBuilder() << backupPath << '/' << t;
                        TestDescribeResult(DescribePath(runtime,  tablePath), {
                            NLs::PathExist,
                            NLs::IsTable,
                            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                            NLs::Finished,
                        });
                    }
                }

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }

    Y_UNIT_TEST(BackupCycleBackupCollectionWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultIncrementalCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(6),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << DEFAULT_NAME_1 << '/' << b;
                    TVector<TString> tables;
                    TestDescribeResult(DescribePath(runtime,  backupPath), {
                        NLs::PathExist,
                        NLs::ChildrenCount(1),
                        NLs::ExtractChildren(&tables),
                        NLs::Finished,
                    });

                    for (const auto& t : tables) {
                        TString tablePath = TStringBuilder() << backupPath << '/' << t;
                        TestDescribeResult(DescribePath(runtime,  tablePath), {
                            NLs::PathExist,
                            NLs::IsTable,
                            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                            NLs::Finished,
                        });
                    }
                }

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }

    Y_UNIT_TEST(BackupCycleMultiTableBackupCollectionWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultIncrementalMultiTableCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table2"
                    Columns { Name: "key" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(6),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << DEFAULT_NAME_1 << '/' << b;
                    TVector<TString> tables;
                    TestDescribeResult(DescribePath(runtime,  backupPath), {
                        NLs::PathExist,
                        NLs::ChildrenCount(2),
                        NLs::ExtractChildren(&tables),
                        NLs::Finished,
                    });

                    for (const auto& t : tables) {
                        TString tablePath = TStringBuilder() << backupPath << '/' << t;
                        TestDescribeResult(DescribePath(runtime,  tablePath), {
                            NLs::PathExist,
                            NLs::IsTable,
                            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                            NLs::Finished,
                        });
                    }
                }

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table2"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }

    Y_UNIT_TEST(BackupCycleWithDataBackupCollectionWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", DefaultIncrementalCollectionSettings());
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});
            }

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(2u)});
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(3u)});
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(4u)});
            }

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(5u)});
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(6u)});
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(5));

            {
                TInactiveZone inactive(activeZone);

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(6),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << DEFAULT_NAME_1 << '/' << b;
                    TVector<TString> tables;
                    TestDescribeResult(DescribePath(runtime,  backupPath), {
                        NLs::PathExist,
                        NLs::ChildrenCount(1),
                        NLs::ExtractChildren(&tables),
                        NLs::Finished,
                    });

                    for (const auto& t : tables) {
                        TString tablePath = TStringBuilder() << backupPath << '/' << t;
                        TestDescribeResult(DescribePath(runtime,  tablePath), {
                            NLs::PathExist,
                            NLs::IsTable,
                            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                            NLs::Finished,
                        });

                        ui32 expectedRows = b.Contains("full") ? 2 : 1;
                        ui32 foundRows = CountRows(runtime, tablePath);
                        UNIT_ASSERT_EQUAL_C(foundRows, expectedRows, TStringBuilder()
                            << "Backup table " << tablePath << " has " << foundRows << " rows, "
                            << "but expected " << expectedRows << " rows");
                    }
                }

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }
} // TBackupCollectionWithRebootsTests
