#include <ydb/core/base/path.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>


using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NTableIndex;
using namespace NKikimr::NTableIndex::NFulltext;

Y_UNIT_TEST_SUITE(TFulltextIndexTests) {
    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "covered" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: { 
                    Settings: { 
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()));
        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TDescribeOptions opts;
        opts.SetReturnChildren(true);
        opts.SetShowPrivateTable(true);
        Cout << DescribePath(runtime, "/MyRoot/texts/idx_fulltext/indexImplTable", opts).DebugString() << Endl;

        for (ui32 reboot = 0; reboot < 2; reboot++) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{ 
                NLs::PathExist,
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain),
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                NLs::IndexKeys({"text"}),
                NLs::IndexDataColumns({"covered"}),
                NLs::SpecializedIndexDescription(fulltextSettings),
                NLs::ChildrenCount(1),
            });

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext/indexImplTable"),{
                NLs::PathExist,
                NLs::CheckColumns("indexImplTable", 
                        { NTableIndex::NFulltext::TokenColumn, "id", "covered" }, {}, 
                        { NTableIndex::NFulltext::TokenColumn, "id" }, true) });

            Cerr << "Reboot SchemeShard.." << Endl;
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }
    }

    Y_UNIT_TEST(CreateTablePrefix) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        // The index key columns are [prefix..., text]; here "another" is a prefix column.
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "covered" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: [ "another", "text"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: {
                    Settings: {
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()));
        env.TestWaitNotification(runtime, txId);

        for (ui32 reboot = 0; reboot < 2; reboot++) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{
                NLs::PathExist,
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain),
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                NLs::IndexKeys({"another", "text"}),
                NLs::IndexDataColumns({"covered"}),
                NLs::SpecializedIndexDescription(fulltextSettings),
                NLs::ChildrenCount(1),
            });

            // Posting impl table key is [prefix..., __ydb_token, doc_id...] = [another, __ydb_token, id].
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext/indexImplTable"),{
                NLs::PathExist,
                NLs::CheckColumns("indexImplTable",
                        { NTableIndex::NFulltext::TokenColumn, "id", "covered", "another" }, {},
                        { "another", NTableIndex::NFulltext::TokenColumn, "id" }, true) });

            Cerr << "Reboot SchemeShard.." << Endl;
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }
    }

    Y_UNIT_TEST(CreateTablePrefixDisabled) {
        // Server-side enforcement: with EnableFulltextIndexPrefix off, SchemeShard must reject a
        // prefixed fulltext index even via a direct scheme operation (bypassing KQP validation).
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableFulltextIndexPrefix(false));
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        // "another" is a prefix column => index has more than one key column => rejected.
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: [ "another", "text"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: {
                    Settings: {
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()), {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableMultipleColumns) { // not supported for now, maybe later
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text1"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
            columns: {
                column: "text2"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text1" Type: "String" }
                Columns { Name: "text2" Type: "String" }
                Columns { Name: "covered" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text1", "text2"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: { 
                    Settings: { 
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()), {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{ 
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableNotText) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "Uint64" }
                Columns { Name: "covered" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: { 
                    Settings: { 
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()), {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{ 
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableColumnsMismatch) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text_wrong"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "covered" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: { 
                    Settings: { 
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()), {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{ 
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableNoColumnsSettings) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString fulltextSettings = R"(
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "covered" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: { 
                    Settings: { 
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()), {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{ 
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableUnsupportedSettings) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_edge_ngram: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "covered" Type: "String" }
                Columns { Name: "another" Type: "Uint64" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: { 
                    Settings: { 
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()), {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{
            NLs::PathNotExist,
        });
    }

    // A custom (non single-integer) PK has no usable doc_id, so CREATE TABLE auto-provisions the
    // __ydb_row_id column, its backing sequence and a unique index over it, and switches the fulltext
    // index to rowid mode - mirroring what the build-index path does for ALTER TABLE ADD INDEX.
    Y_UNIT_TEST(CreateTableAutoProvisionsRowId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "pk" Type: "Utf8" }
                Columns { Name: "text" Type: "String" }
                KeyColumnNames: ["pk"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: {
                    Settings: {
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()));
        env.TestWaitNotification(runtime, txId);

        for (ui32 reboot = 0; reboot < 2; reboot++) {
            // The synthetic __ydb_row_id Uint64 NOT NULL column was added to the main table, defaulted
            // from the auto-provisioned sequence.
            {
                auto tableDesc = DescribePath(runtime, "/MyRoot/texts");
                bool found = false;
                for (const auto& column : tableDesc.GetPathDescription().GetTable().GetColumns()) {
                    if (column.GetName() == NTableIndex::NFulltext::RowIdColumn) {
                        found = true;
                        UNIT_ASSERT_VALUES_EQUAL(column.GetType(), "Uint64");
                        UNIT_ASSERT(column.GetNotNull());
                        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(),
                            NTableIndex::NFulltext::RowIdSequenceName);
                    }
                }
                UNIT_ASSERT_C(found, "auto-provisioned __ydb_row_id column not found on the main table");
            }

            // The backing sequence exists.
            TestDescribeResult(DescribePrivatePath(runtime,
                TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdSequenceName), {
                NLs::PathExist,
            });

            // The auto-provisioned unique index over __ydb_row_id is Ready.
            TestDescribeResult(DescribePrivatePath(runtime,
                TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
                NLs::PathExist,
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                NLs::IndexKeys({TString(NTableIndex::NFulltext::RowIdColumn)}),
            });

            // The fulltext index runs in rowid mode and its posting impl-table is keyed by
            // [__ydb_token, __ydb_row_id] rather than by the main-table PK.
            auto idxDesc = DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext");
            UNIT_ASSERT_C(idxDesc.GetPathDescription().GetTableIndex()
                    .GetFulltextIndexDescription().GetUseRowIdAsDocId(),
                "fulltext index was not switched to __ydb_row_id doc_id mode");

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext/indexImplTable"), {
                NLs::PathExist,
                NLs::CheckColumns("indexImplTable",
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn }, {},
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn }, true) });

            Cerr << "Reboot SchemeShard.." << Endl;
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }
    }

    // With EnableFulltextIndexRowId off, __ydb_row_id auto-provisioning is disabled: a fulltext index
    // over a non-integer PK falls back to requiring a single integer PK and is rejected server-side.
    Y_UNIT_TEST(CreateTableRowIdDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableFulltextIndexRowId(false));
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "pk" Type: "Utf8" }
                Columns { Name: "text" Type: "String" }
                KeyColumnNames: ["pk"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: {
                    Settings: {
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()),
            {{NKikimrScheme::StatusInvalidParameter, "requires the __ydb_row_id doc_id feature, which is disabled (feature flag EnableFulltextIndexRowId)"}});
        env.TestWaitNotification(runtime, txId);

        // No __ydb_row_id infrastructure must have been provisioned.
        TestDescribeResult(DescribePath(runtime, "/MyRoot/texts"), {NLs::PathNotExist});
    }

    // A single-integer PK keeps the legacy doc_id=PK behaviour: no __ydb_row_id infrastructure is
    // provisioned even though the table has a fulltext index.
    Y_UNIT_TEST(CreateTableSingleIntegerPkKeepsLegacyDocId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        TString fulltextSettings = R"(
            columns: {
                column: "text"
                analyzers: {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )";
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "text" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_fulltext"
                KeyColumnNames: ["text"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription: {
                    Settings: {
                        %s
                    }
                }
            }
        )", fulltextSettings.c_str()));
        env.TestWaitNotification(runtime, txId);

        auto tableDesc = DescribePath(runtime, "/MyRoot/texts");
        for (const auto& column : tableDesc.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT_C(column.GetName() != NTableIndex::NFulltext::RowIdColumn,
                "__ydb_row_id must not be provisioned for a single-integer PK");
        }
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathNotExist,
        });
        auto idxDesc = DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext");
        UNIT_ASSERT(!idxDesc.GetPathDescription().GetTableIndex()
            .GetFulltextIndexDescription().GetUseRowIdAsDocId());
    }

}
