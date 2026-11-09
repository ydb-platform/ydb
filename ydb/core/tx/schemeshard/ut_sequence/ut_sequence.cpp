#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSequence) {

namespace {

const NKikimrSchemeOp::TColumnDescription* FindColumn(
        const NKikimrSchemeOp::TTableDescription& table, const TString& name)
{
    for (const auto& column : table.GetColumns()) {
        if (column.GetName() == name) {
            return &column;
        }
    }
    return nullptr;
}

const NKikimrSchemeOp::TTableDescription* FindCreateTable(
        const TVector<NKikimrTxDataShard::TFlatSchemeTransaction>& txs, const TString& name)
{
    for (const auto& tx : txs) {
        if (tx.HasCreateTable() && tx.GetCreateTable().GetName() == name) {
            return &tx.GetCreateTable();
        }
    }
    return nullptr;
}

} // anonymous namespace

    Y_UNIT_TEST(CreateSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/seq", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateSequenceParallel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        // Make two passes, so we test parallel creation both when
        // sequenceshard doesn't exist yet, and when it exists already
        for (int j = 0; j < 2; ++j) {
            for (int i = 4*j + 1; i <= 4*j + 4; ++i) {
                TestCreateSequence(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "seq%d"
                )", i));
            }
            env.TestWaitNotification(runtime, {txId-3, txId-2, txId-1, txId});

            for (int i = 4*j + 1; i <= 4*j + 4; ++i) {
                TestLs(runtime, Sprintf("/MyRoot/seq%d", i), false, NLs::PathExist);
            }
        }
    }

    Y_UNIT_TEST(CreateSequenceSequential) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        for (int i = 1; i <= 4; ++i) {
            TestCreateSequence(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "seq%d"
            )", i));
            env.TestWaitNotification(runtime, txId);

            TestLs(runtime, Sprintf("/MyRoot/seq%d", i), false, NLs::PathExist);
        }
    }

    Y_UNIT_TEST(CreateDropRecreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/seq", false, NLs::PathExist);

        TestDropSequence(runtime, ++txId, "/MyRoot", "seq");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/seq", false, NLs::PathNotExist);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/seq", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateSequenceInsideSequenceNotAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateSequence(runtime, ++txId, "/MyRoot/seq", R"(
            Name: "seq"
        )", {NKikimrScheme::StatusPathIsNotDirectory});
    }

    Y_UNIT_TEST(CreateSequenceInsideTableThenDropSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateSequence(runtime, ++txId, "/MyRoot/Table", R"(
            Name: "seq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/seq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        TestDropSequence(runtime, ++txId, "/MyRoot/Table", "seq");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/seq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathNotExist);
    }

    Y_UNIT_TEST(CreateSequenceInsideTableThenDropTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateSequence(runtime, ++txId, "/MyRoot/Table", R"(
            Name: "seq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/seq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(CreateSequenceInsideIndexTableNotAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
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
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateSequence(runtime, ++txId, "/MyRoot/Table/ValueIndex/indexImplTable", R"(
            Name: "seq"
        )", {NKikimrScheme::StatusNameConflict});
    }

    Y_UNIT_TEST(CreateSequencesWithIndexedTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
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
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/seq1", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
        TestLs(runtime, "/MyRoot/Table/seq2", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(CreateTableWithDefaultFromSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        // Cannot use default from sequence that doesn't exist
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64" DefaultFromSequence: "/MyRoot/myseq" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // Cannot use default from sequence that doesn't match local sequences
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64" DefaultFromSequence: "myseq" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "someseq"
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table1"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" DefaultFromSequence: "myseq" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "myseq"
            }
        )");
        env.TestWaitNotification(runtime, txId);
        txId++;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table2"
                Columns { Name: "key"   Type: "Uint64" DefaultFromSequence: "myseq" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "myseq"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table2/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        // Cannot drop sequence used by a column
        TestDropSequence(runtime, ++txId, "/MyRoot/Table2", "myseq",
            {NKikimrScheme::StatusNameConflict});

        TestDropTable(runtime, ++txId, "/MyRoot", "Table2");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table2", false, NLs::PathNotExist);
    }

    // DataShard keeps its own copy of the table description and rebuilds it from its typed
    // column list, so a column property it does not know about is dropped on the next alter.
    // The sequence path of a serial column must reach the shards and survive there: during
    // replication the shard resolves it against the table path to advance the sequence.
    Y_UNIT_TEST(TableDescriptionCarriesSequenceDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TVector<NKikimrTxDataShard::TFlatSchemeTransaction> captured;
        auto observer = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>(
            [&](TEvDataShard::TEvProposeTransaction::TPtr& ev) {
                const auto& record = ev->Get()->Record;
                if (record.GetTxKind() != NKikimrTxDataShard::ETransactionKind::TX_KIND_SCHEME) {
                    return;
                }
                NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
                UNIT_ASSERT(schemeTx.ParseFromString(record.GetTxBody()));
                captured.push_back(std::move(schemeTx));
            });

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64" DefaultFromSequence: "myseq" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "myseq"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Create: the sequence path travels with the column description.
        {
            const auto* table = FindCreateTable(captured, "Table");
            UNIT_ASSERT_C(table, "the table description never reached data shards");
            const auto* column = FindColumn(*table, "key");
            UNIT_ASSERT_C(column, "column 'key' is missing in the shard description");
            UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultFromSequence(), "myseq");
        }

        observer.Remove();
    }

    // A fulltext index in row-id mode auto-provisions the hidden __ydb_row_id column with a
    // default from the table-local sequence. The alter that adds the column must carry that
    // path too, otherwise a replica cannot advance the sequence after the switchover.
    Y_UNIT_TEST(RowIdColumnCarriesSequenceDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto& ff = runtime.GetAppData().FeatureFlags;
        ff.SetEnableFulltextIndex(true);
        ff.SetEnableCompactFulltextIndex(true);
        ff.SetEnableFulltextIndexRowId(true);
        ff.SetEnableAddUniqueIndex(true);
        ff.SetEnableUniqConstraint(true);

        TVector<NKikimrTxDataShard::TFlatSchemeTransaction> captured;
        auto observer = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>(
            [&](TEvDataShard::TEvProposeTransaction::TPtr& ev) {
                const auto& record = ev->Get()->Record;
                if (record.GetTxKind() != NKikimrTxDataShard::ETransactionKind::TX_KIND_SCHEME) {
                    return;
                }
                NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
                UNIT_ASSERT(schemeTx.ParseFromString(record.GetTxBody()));
                captured.push_back(std::move(schemeTx));
            });

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "pk"   Type: "Utf8" NotNull: true }
                Columns { Name: "text" Type: "String" }
                KeyColumnNames: ["pk"]
            }
            IndexDescription {
                Name: "fulltext_idx"
                KeyColumnNames: ["text"]
                Type: EIndexTypeGlobalFulltextCompact
                FulltextIndexDescription {
                    Settings {
                        columns: {
                            column: "text"
                            analyzers: {
                                tokenizer: STANDARD
                                use_filter_lowercase: true
                            }
                        }
                    }
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // The impl tables of a fulltext index also have a __ydb_row_id column, so only the main
        // table body is checked here.
        const auto findRowIdTable = [&]() -> const NKikimrSchemeOp::TTableDescription* {
            for (const auto& tx : captured) {
                if (tx.HasAlterTable() && tx.GetAlterTable().GetName() == "texts"
                        && FindColumn(tx.GetAlterTable(), NTableIndex::NFulltext::RowIdColumn)) {
                    return &tx.GetAlterTable();
                }
                if (tx.HasCreateTable() && tx.GetCreateTable().GetName() == "texts"
                        && FindColumn(tx.GetCreateTable(), NTableIndex::NFulltext::RowIdColumn)) {
                    return &tx.GetCreateTable();
                }
            }
            return nullptr;
        };

        // The index is created together with the table, so the column arrives at the data shards
        // either in the create table body or in a follow-up alter of the provisioning pipeline.
        for (int i = 0; i < 60 && !findRowIdTable(); ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }

        const auto* table = findRowIdTable();
        UNIT_ASSERT_C(table, "the __ydb_row_id column was never sent to data shards");

        const auto* column = FindColumn(*table, NTableIndex::NFulltext::RowIdColumn);
        UNIT_ASSERT(column);
        UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultFromSequence(), NTableIndex::NFulltext::RowIdSequenceName);
        UNIT_ASSERT(column->GetNotNull());

        observer.Remove();
    }

    // A user may only get a column sequence through SERIAL: the sequence has to be generated for the
    // column and must not set an arbitrary position.
    Y_UNIT_TEST(CreateTableWithSerialSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "id"    Type: "Uint64" DefaultFromSequence: "_serial_column_id" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["id"]
            }
            SequenceDescription {
                Name: "_serial_column_id"
                MinValue: 1
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/_serial_column_id",
            TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
    }

    // Custom sequence parameters are not rejected internally.
    Y_UNIT_TEST(CreateTableWithCustomSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "id" Type: "Uint64" DefaultFromSequence: "myseq" }
                KeyColumnNames: ["id"]
            }
            SequenceDescription {
                Name: "myseq"
                SetVal { NextValue: 1000 NextUsed: true }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        auto table = DescribePath(runtime, "/MyRoot/Table").GetPathDescription().GetTable();
        const auto* column = FindColumn(table, "id");
        UNIT_ASSERT(column);
        UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultFromSequence(), "myseq");
    }

    // A fulltext index in row-id mode relies on two sequences: `_seq___ydb_row_id` on the main table
    // (provides the doc id) and `__ydb_gen_sequence` on the index impl table (generation of posting
    // rows). Both have to be visible in the describes, because a replication destination recreates
    // them from there.
    Y_UNIT_TEST(FulltextRowIdIndexSequences) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto& ff = runtime.GetAppData().FeatureFlags;
        ff.SetEnableFulltextIndex(true);
        ff.SetEnableCompactFulltextIndex(true);
        ff.SetEnableFulltextIndexRowId(true);
        ff.SetEnableAddUniqueIndex(true);
        ff.SetEnableUniqConstraint(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "pk"   Type: "Utf8" NotNull: true }
                Columns { Name: "text" Type: "String" }
                KeyColumnNames: ["pk"]
            }
            IndexDescription {
                Name: "fulltext_idx"
                KeyColumnNames: ["text"]
                Type: EIndexTypeGlobalFulltextCompact
                FulltextIndexDescription {
                    Settings {
                        columns: {
                            column: "text"
                            analyzers: {
                                tokenizer: STANDARD
                                use_filter_lowercase: true
                            }
                        }
                    }
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // The main table owns the sequence that generates doc ids.
        {
            auto table = DescribePath(runtime, "/MyRoot/texts").GetPathDescription().GetTable();
            const auto* column = FindColumn(table, NTableIndex::NFulltext::RowIdColumn);
            UNIT_ASSERT_C(column, "the row-id column was not provisioned");
            UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultFromSequence(), NTableIndex::NFulltext::RowIdSequenceName);

            UNIT_ASSERT_VALUES_EQUAL(table.SequencesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(table.GetSequences(0).GetName(), NTableIndex::NFulltext::RowIdSequenceName);
        }

        // The index impl table owns the sequence that generates posting generations.
        {
            const TString implTablePath = TStringBuilder()
                << "/MyRoot/texts/fulltext_idx/" << NTableIndex::ImplTable;
            auto table = DescribePrivatePath(runtime, implTablePath).GetPathDescription().GetTable();
            const auto* column = FindColumn(table, NTableIndex::NFulltext::GenColumn);
            UNIT_ASSERT_C(column, "the generation column was not found");
            UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultFromSequence(), NTableIndex::NFulltext::GenSequence);

            UNIT_ASSERT_VALUES_EQUAL(table.SequencesSize(), 1);
            const auto& sequence = table.GetSequences(0);
            UNIT_ASSERT_VALUES_EQUAL(sequence.GetName(), NTableIndex::NFulltext::GenSequence);
            UNIT_ASSERT_VALUES_EQUAL(sequence.GetIncrement(), -1);
        }
    }

    Y_UNIT_TEST(AlterTableDropColumnWithSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" DefaultFromSequence: "myseq" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "myseq"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        // Dropping the serial column drops its backing child sequence in the same operation.
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DropColumns { Name: "value" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathNotExist);
        TestLs(runtime, "/MyRoot/Table", false, NLs::PathExist);

        // The sequence path is really freed: its name can be reused under the table.
        TestCreateSequence(runtime, ++txId, "/MyRoot/Table", R"(
            Name: "myseq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
    }

    Y_UNIT_TEST(CreateTableWithDefaultFromSequenceAndIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
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
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table/ValueIndex", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
        TestLs(runtime, "/MyRoot/Table/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/Table", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(CopyTableWithSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
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

        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot", "copy", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/copy/myseq", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/seq", true),
            {
                NLs::SequenceIncrement(1),
                NLs::SequenceMinValue(1),
                NLs::SequenceCache(1),
                NLs::SequenceStartValue(1),
                NLs::SequenceCycle(false)
            }
        );

        auto value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 1);

        value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 2);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Increment: 2
            MaxValue: 5
            MinValue: 2
            Cache: 1
            StartValue: 2
            Cycle: true
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/seq", true),
                {
                    NLs::SequenceIncrement(2),
                    NLs::SequenceMaxValue(5),
                    NLs::SequenceMinValue(2),
                    NLs::SequenceCache(1),
                    NLs::SequenceStartValue(2),
                    NLs::SequenceCycle(true)
                }
        );

        value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 3);

        value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 5);

        value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 2);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Cycle: false
            MaxValue: 4
        )");
        env.TestWaitNotification(runtime, txId);

        value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 4);

        DoNextVal(runtime, "/MyRoot/seq", Ydb::StatusIds::SCHEME_ERROR);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Cycle: false
            MinValue: 7
        )", {{NKikimrScheme::StatusInvalidParameter, "MINVALUE (7) must be less than MAXVALUE (4)"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Cycle: false
            MinValue: 3
        )", {{NKikimrScheme::StatusInvalidParameter, "START value (2) cannot be less than MINVALUE (3)"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Cycle: false
            MinValue: 3
            StartValue: 3
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Cycle: false
            MinValue: 3
            MaxValue: 2
        )", {{NKikimrScheme::StatusInvalidParameter, "MINVALUE (3) must be less than MAXVALUE (2)"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Cycle: false
            MinValue: 1
            MaxValue: 65000
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            DataType: "Int16"
        )", {{NKikimrScheme::StatusInvalidParameter, "MAXVALUE (65000) is out of range for sequence data type Int16"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            DataType: "Int32"
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            MaxValue: 2147483647
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            DataType: "Int16"
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            DataType: "Int64"
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            MaxValue: 2147483648
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            DataType: "Int32"
        )", {{NKikimrScheme::StatusInvalidParameter, "MAXVALUE (2147483648) is out of range for sequence data type Int32"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            DataType: "Int16"
        )", {{NKikimrScheme::StatusInvalidParameter, "MAXVALUE (2147483648) is out of range for sequence data type Int16"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            MaxValue: 9223372036854775807
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            DataType: "Int16"
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Restart: true
            SetVal {
                NextValue: 77
            }
        )");
        env.TestWaitNotification(runtime, txId);

        value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 77);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Increment: 0
        )", {{NKikimrScheme::StatusInvalidParameter, "INCREMENT must not be zero"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Restart: true
            SetVal {
                NextValue: 650000
            }
        )", {{NKikimrScheme::StatusInvalidParameter, "RESTART value (650000) cannot be greater than MAXVALUE (32767)"}});

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Restart: true
            StartValue: 305
        )");
        env.TestWaitNotification(runtime, txId);

        value = DoNextVal(runtime, "/MyRoot/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 305);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq"
            Increment: 650000
        )");
        env.TestWaitNotification(runtime, txId);

        DoNextVal(runtime, "/MyRoot/seq", Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(AlterTableSetDefaultFromSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table1"
                Columns { Name: "key"   Type: "Int64" }
                Columns { Name: "value1" Type: "Int64" }
                Columns { Name: "value2" Type: "Int32" }
                KeyColumnNames: ["key"]
            }
        )");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key"   Type: "Int64" }
            Columns { Name: "value1" Type: "Int64" }
            Columns { Name: "value2" Type: "Int64" }
            KeyColumnNames: ["key"]
        )");

        env.TestWaitNotification(runtime, txId);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq1"
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "seq2"
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" DefaultFromSequence: "/MyRoot/seq1" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "value1" DefaultFromSequence: "/MyRoot/seq1" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "value2" DefaultFromSequence: "/MyRoot/seq1" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "value1" DefaultFromSequence: "/MyRoot/seq1" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "value2" DefaultFromSequence: "/MyRoot/seq1" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "value1" DefaultFromSequence: "/MyRoot/seq3" }
        )", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});

        auto table1 = DescribePath(runtime, "/MyRoot/Table1")
            .GetPathDescription()
            .GetTable();

        for (const auto& column: table1.GetColumns()) {
            UNIT_ASSERT(column.HasDefaultFromSequence());
            UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "/MyRoot/seq1");

            TestDescribeResult(DescribePath(runtime, column.GetDefaultFromSequence()),
                {
                    NLs::SequenceName("seq1"),
                }
            );
        }

        auto table2 = DescribePath(runtime, "/MyRoot/Table2")
            .GetPathDescription()
            .GetTable();

        for (const auto& column: table2.GetColumns()) {
            if (column.GetName() == "key") {
                continue;
            }
            UNIT_ASSERT(column.HasDefaultFromSequence());
            UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "/MyRoot/seq1");

            TestDescribeResult(DescribePath(runtime, column.GetDefaultFromSequence()),
                {
                    NLs::SequenceName("seq1"),
                }
            );
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "value1" DefaultFromSequence: "/MyRoot/seq2" }
        )");
        env.TestWaitNotification(runtime, txId);

        table2 = DescribePath(runtime, "/MyRoot/Table2")
            .GetPathDescription()
            .GetTable();

        for (const auto& column: table2.GetColumns()) {
            if (column.GetName() == "key") {
                continue;
            }
            if (column.GetName() == "value1") {
                UNIT_ASSERT(column.HasDefaultFromSequence());
                UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "/MyRoot/seq2");

                TestDescribeResult(DescribePath(runtime, column.GetDefaultFromSequence()),
                    {
                        NLs::SequenceName("seq2"),
                    }
                );
            } else if (column.GetName() == "value2") {
                UNIT_ASSERT(column.HasDefaultFromSequence());
                UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "/MyRoot/seq1");

                TestDescribeResult(DescribePath(runtime, column.GetDefaultFromSequence()),
                    {
                        NLs::SequenceName("seq1"),
                    }
                );
            }
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" DefaultFromSequence: "/MyRoot/seq2" }
            Columns { Name: "value1" EmptyDefault: NULL_VALUE }
        )");
        env.TestWaitNotification(runtime, txId);

        table2 = DescribePath(runtime, "/MyRoot/Table2")
            .GetPathDescription()
            .GetTable();

        for (const auto& column: table2.GetColumns()) {
            if (column.GetName() == "key") {
                UNIT_ASSERT(column.HasDefaultFromSequence());
                UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "/MyRoot/seq2");

                TestDescribeResult(DescribePath(runtime, column.GetDefaultFromSequence()),
                    {
                        NLs::SequenceName("seq2"),
                    }
                );
            }
            if (column.GetName() == "value1") {
                UNIT_ASSERT(!column.HasDefaultFromSequence());
                UNIT_ASSERT(!column.HasDefaultFromLiteral());
            } else if (column.GetName() == "value2") {
                UNIT_ASSERT(column.HasDefaultFromSequence());
                UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "/MyRoot/seq1");

                TestDescribeResult(DescribePath(runtime, column.GetDefaultFromSequence()),
                    {
                        NLs::SequenceName("seq1"),
                    }
                );
            }
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" EmptyDefault: NULL_VALUE }
        )");
        env.TestWaitNotification(runtime, txId);

        table1 = DescribePath(runtime, "/MyRoot/Table1")
            .GetPathDescription()
            .GetTable();

        for (const auto& column: table1.GetColumns()) {
            if (column.GetName() == "key") {
                UNIT_ASSERT(!column.HasDefaultFromSequence());
                UNIT_ASSERT(!column.HasDefaultFromLiteral());
            }
            if (column.GetName() == "value1") {
                UNIT_ASSERT(column.HasDefaultFromSequence());
                UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "/MyRoot/seq1");

                TestDescribeResult(DescribePath(runtime, column.GetDefaultFromSequence()),
                    {
                        NLs::SequenceName("seq1"),
                    }
                );
            }
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table3"
            Columns { Name: "key"   Type: "Int64" }
            Columns { Name: "value" Type: "Bool" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table3"
            Columns { Name: "value" DefaultFromSequence: "/MyRoot/seq1" }
        )", {{NKikimrScheme::StatusInvalidParameter, "Column 'value' is of type Bool but default expression is of type Int64"}});
    }

} // Y_UNIT_TEST_SUITE(TSequence)
