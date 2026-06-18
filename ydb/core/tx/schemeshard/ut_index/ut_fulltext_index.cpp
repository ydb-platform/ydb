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
}
