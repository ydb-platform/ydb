#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSequenceReboots) {
    Y_UNIT_TEST_WITH_REBOOTS(CreateSequence) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TestCreateSequence(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "seq"
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/seq", false, NLs::PathExist);
            }
        });
    }

    void DoCreateMultipleSequences(TTestWithReboots& t, bool withInitialSequenceShard) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);

                if (withInitialSequenceShard) {
                    // Create a sequence, which will create initial sequenceshard
                    TestCreateSequence(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "seq0"
                    )");
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);

                    TestLs(runtime, "/MyRoot/seq0", false, NLs::PathExist);
                }
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateSequenceRequest(t.TxId += 3, "/MyRoot", R"(
                    Name: "seq1"
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->ReliablePropose(runtime,
                CreateSequenceRequest(t.TxId - 1, "/MyRoot", R"(
                    Name: "seq2"
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->ReliablePropose(runtime,
                CreateSequenceRequest(t.TxId - 2, "/MyRoot", R"(
                    Name: "seq3"
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 2, t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/seq1", false, NLs::PathExist);
                TestLs(runtime, "/MyRoot/seq2", false, NLs::PathExist);
                TestLs(runtime, "/MyRoot/seq3", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateMultipleSequencesNoInitialSequenceShard, 2, 1, false) {
        DoCreateMultipleSequences(t, false);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateMultipleSequencesHaveInitialSequenceShard, 2, 1, false) {
        DoCreateMultipleSequences(t, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateDropRecreate, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateSequenceRequest(++t.TxId, "/MyRoot", R"(
                    Name: "seq"
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/seq", false, NLs::PathExist);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropSequenceRequest(++t.TxId, "/MyRoot", "seq"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/seq", false, NLs::PathNotExist);
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateSequenceRequest(++t.TxId, "/MyRoot", R"(
                    Name: "seq"
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/seq", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateSequencesWithIndexedTable, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateIndexedTableRequest(++t.TxId, "/MyRoot", R"(
                    TableDescription {
                        Name: "Table"
                        Columns { Name: "key"   Type: "Uint64" }
                        Columns { Name: "value" Type: "Utf8" }
                        KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                        Name: "ValueIndex"
                        KeyColumnNames: ["value"]
                    }
                    SequenceDescription {
                        Name: "seq1"
                    }
                    SequenceDescription {
                        Name: "seq2"
                    }
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(
                    runtime, "/MyRoot/Table/seq1", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
                TestLs(
                    runtime, "/MyRoot/Table/seq2", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

            }

            t.TestEnv->ReliablePropose(runtime,
                DropTableRequest(++t.TxId, "/MyRoot", "Table"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/Table", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CopyTableWithSequence, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                    TableDescription {
                        Name: "Table"
                        Columns { Name: "key"   Type: "Uint64" DefaultFromSequence: "myseq" }
                        Columns { Name: "value" Type: "Utf8" }
                        KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                        Name: "ValueIndex"
                        KeyColumnNames: ["value"]
                    }
                    SequenceDescription {
                        Name: "myseq"
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                i64 value = DoNextVal(runtime, "/MyRoot/Table/myseq");
                UNIT_ASSERT_VALUES_EQUAL(value, 1);
            }

            t.TestEnv->ReliablePropose(runtime, CopyTableRequest(++t.TxId, "/MyRoot", "copy", "/MyRoot/Table"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists,
                NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(
                    runtime, "/MyRoot/copy/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

                i64 value = DoNextVal(runtime, "/MyRoot/copy/myseq");
                UNIT_ASSERT_VALUES_EQUAL(value, 2);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(AlterSequence) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);

                TestCreateSequence(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "seq"
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterSequence(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "seq"
                Increment: 2
                MaxValue: 5
                MinValue: 2
                Cache: 1
                StartValue: 2
                Cycle: true
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/seq", false, NLs::PathExist);
            }
        });
    }

    // Issue #33764: ConsistentCopyTables + DropTable with sequences and reboots
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ConsistentCopyThenDropTableWithSequenceAndReboots, 8, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                    TableDescription {
                        Name: "Table"
                        Columns { Name: "key" Type: "Uint64" DefaultFromSequence: "myseq" }
                        Columns { Name: "value" Type: "Utf8" }
                        KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                        Name: "ValueIndex"
                        KeyColumnNames: ["value"]
                    }
                    SequenceDescription {
                        Name: "myseq"
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                ConsistentCopyTablesRequest(++t.TxId, "/", R"(
                    CopyTableDescriptions {
                        SrcPath: "/MyRoot/Table"
                        DstPath: "/MyRoot/TableCopy"
                    }
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->ReliablePropose(runtime,
                DropTableRequest(++t.TxId, "/MyRoot", "Table"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications,
                 NKikimrScheme::StatusPathDoesNotExist});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/TableCopy"),
                                   {NLs::PathExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy/ValueIndex"),
                                   {NLs::PathExist});
                TestLs(runtime, "/MyRoot/TableCopy/myseq",
                    TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
                TestLs(runtime, "/MyRoot/Table", false, NLs::PathNotExist);
            }
        });
    }

} // Y_UNIT_TEST_SUITE(TSequenceReboots)
