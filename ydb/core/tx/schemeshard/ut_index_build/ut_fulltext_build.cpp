#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/metering/metering.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(FulltextIndexBuildTest) {

    void DoCreateTextTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Ydb::Table::TableIndex FulltextIndexConfig(bool relevance) {
        Ydb::Table::TableIndex index;
        index.set_name("fulltext_idx");
        index.add_index_columns("text");
        if (relevance) {
            auto& fulltext = *index.mutable_global_fulltext_relevance_index()->mutable_fulltext_settings();
            auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
            fulltext.mutable_columns()->at(0).set_column("text");
            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        } else {
            auto& fulltext = *index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings();
            auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
            fulltext.mutable_columns()->at(0).set_column("text");
            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        }
        return index;
    }

    void DoCreateTextTableAndIndex(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
        bool relevance, std::function<void(Ydb::Table::TableIndex&)> cfg) {
        DoCreateTextTable(runtime, env, txId);

        auto fnWriteRow = [&] (ui64 id, TString text, TString data) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('id   (Uint64 '%u) ) ) )
                    (let row   '( '('text (String '"%s") )  '('data (String '"%s") ) ) )
                    (return (AsList (UpdateRow '__user__texts key row) ))
                )
            )", id, text.c_str(), data.c_str());

            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        };

        fnWriteRow(1, "green apple", "one");
        fnWriteRow(2, "red apple and blue apple", "two");
        fnWriteRow(3, "yellow apple", "three");
        fnWriteRow(4, "red car", "four");

        auto index = FulltextIndexConfig(relevance);
        if (cfg) {
            cfg(index);
        }

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);
    }

    void DoCheckPlainIndexTable(TTestBasicRuntime& runtime, const TString& index) {
        auto rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplTable").at(0);
        Cerr << index << "/indexImplTable rows: " << rows << "\n";
        if (runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            UNIT_ASSERT_VALUES_EQUAL("[[[["
                R"([%true;"18446744073709551615";"2";"\2";"and"];)"
                R"([%true;"18446744073709551615";"3";"\1\1\1";"apple"];)"
                R"([%true;"18446744073709551615";"2";"\2";"blue"];)"
                R"([%true;"18446744073709551615";"4";"\4";"car"];)"
                R"([%true;"18446744073709551615";"1";"\1";"green"];)"
                R"([%true;"18446744073709551615";"4";"\2\2";"red"];)"
                R"([%true;"18446744073709551615";"3";"\3";"yellow"])"
            "];%false]]]", rows);
        } else {
            UNIT_ASSERT_VALUES_EQUAL("[[[["
                R"(["and";["two"];["2"]];)"
                R"(["apple";["one"];["1"]];)"
                R"(["apple";["two"];["2"]];)"
                R"(["apple";["three"];["3"]];)"
                R"(["blue";["two"];["2"]];)"
                R"(["car";["four"];["4"]];)"
                R"(["green";["one"];["1"]];)"
                R"(["red";["two"];["2"]];)"
                R"(["red";["four"];["4"]];)"
                R"(["yellow";["three"];["3"]]];)"
            "%false]]]", rows);
        }
    }

    void DoCheckRelevanceIndexTables(TTestBasicRuntime& runtime, const TString& index) {
        auto rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplTable").at(0);
        Cerr << index << "/indexImplTable rows: " << rows << "\n";
        if (runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            UNIT_ASSERT_VALUES_EQUAL("[[[["
                R"([%true;"18446744073709551615";"2";"\2";"and"];)"
                R"([%true;"18446744073709551615";"3";"\1A\2\1";"apple"];)"
                R"([%true;"18446744073709551615";"2";"\2";"blue"];)"
                R"([%true;"18446744073709551615";"4";"\4";"car"];)"
                R"([%true;"18446744073709551615";"1";"\1";"green"];)"
                R"([%true;"18446744073709551615";"4";"\2\2";"red"];)"
                R"([%true;"18446744073709551615";"3";"\3";"yellow"])"
            "];%false]]]", rows);
        } else {
            UNIT_ASSERT_VALUES_EQUAL("[[[["
                R"(["1";"and";["2"]];)"
                R"(["1";"apple";["1"]];)"
                R"(["2";"apple";["2"]];)"
                R"(["1";"apple";["3"]];)"
                R"(["1";"blue";["2"]];)"
                R"(["1";"car";["4"]];)"
                R"(["1";"green";["1"]];)"
                R"(["1";"red";["2"]];)"
                R"(["1";"red";["4"]];)"
                R"(["1";"yellow";["3"]]];)"
            "%false]]]", rows);
        }

        rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplDocsTable").at(0);
        Cerr << index << "/indexImplDocsTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["2";["one"];["1"]];)"
            R"(["5";["two"];["2"]];)"
            R"(["2";["three"];["3"]];)"
            R"(["2";["four"];["4"]]];)"
        "%false]]]", rows);

        rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplStatsTable").at(0);
        Cerr << index << "/indexImplStatsTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["4";"0";"11"]];)"
        "%false]]]", rows);
    }

    Y_UNIT_TEST(Basic) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTableAndIndex(runtime, env, txId, false, [&](Ydb::Table::TableIndex& index) {
            index.add_data_columns("data");
        });

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
        }

        DoCheckPlainIndexTable(runtime, "/MyRoot/texts/fulltext_idx");
    }

    Y_UNIT_TEST(FlatRelevance) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTableAndIndex(runtime, env, txId, true, [&](Ydb::Table::TableIndex& index) {
            index.add_data_columns("data");
        });
        const ui64 buildIndexTx = txId;

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
        }

        DoCheckRelevanceIndexTables(runtime, "/MyRoot/texts/fulltext_idx");

        // Check that the index is successfully dropped
        TestDropTableIndex(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", R"(
            TableName: "texts"
            IndexName: "fulltext_idx"
        )");
        env.TestWaitNotification(runtime, txId);
    }

    // Helpers for the prefixed fulltext index tests below: the table carries a non-key prefix column
    // ("lang") in front of the text column, and the index is declared on (lang, text). The text column
    // is always the LAST index column; everything before it is a prefix key column.

    void DoCreatePrefixedTextTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "id"   Type: "Uint64" }
            Columns { Name: "lang" Type: "Utf8" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    void DoWriteRowsPrefixed(TTestBasicRuntime& runtime) {
        auto fnWriteRow = [&] (ui64 id, TString lang, TString text, TString data) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('id   (Uint64 '%u) ) ) )
                    (let row   '( '('lang (Utf8 '%s) )  '('text (String '"%s") )  '('data (String '"%s") ) ) )
                    (return (AsList (UpdateRow '__user__texts key row) ))
                )
            )", id, lang.c_str(), text.c_str(), data.c_str());

            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        };

        fnWriteRow(1, "en", "green apple",              "one");
        fnWriteRow(2, "en", "red apple and blue apple", "two");
        fnWriteRow(3, "fr", "yellow apple",             "three");
        fnWriteRow(4, "fr", "red car",                  "four");
    }

    Ydb::Table::TableIndex PrefixedFulltextIndexConfig(bool relevance) {
        // Index on (lang, text): "lang" is the prefix column, "text" is the (last) text column.
        // The fulltext settings only describe the text column - prefix columns are not analyzed.
        Ydb::Table::TableIndex index = FulltextIndexConfig(relevance);
        index.clear_index_columns();
        index.add_index_columns("lang");
        index.add_index_columns("text");
        return index;
    }

    // Regression test for the crash at build_index__progress.cpp SendUploadFulltextBordersRequest:
    // building a *prefixed* relevance index (e.g. ALTER TABLE ... ADD INDEX ... ON (lang, text))
    // hit `Y_ENSURE(buildInfo.IndexColumns.size() == 1)` because IndexColumns is [lang, text].
    Y_UNIT_TEST(PrefixedRelevanceBuilds) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableCompactFulltextIndex(true).EnableFulltextIndexPrefix(true));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreatePrefixedTextTable(runtime, env, txId);
        DoWriteRowsPrefixed(runtime);

        Ydb::Table::TableIndex index = PrefixedFulltextIndexConfig(true);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        // Without the fix the async build crashes; with it the build completes.
        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // The posting impl-table is keyed with the prefix column prepended before the token.
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), {
            NLs::PathExist,
        });
    }

    Y_UNIT_TEST(DropTableWithFlatRelevance) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTable(runtime, env, txId);

        Ydb::Table::TableIndex index = FulltextIndexConfig(true);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        // Check that the table with index is successfully dropped
        TestDropTable(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", "texts");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(FlatRelevanceLimit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        DoCreateTextTable(runtime, env, txId);

        auto describe = DescribePath(runtime, "/MyRoot/texts");
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NKikimrScheme::StatusSuccess, "Unexpected status: " << describe.GetStatus());
        auto curShards = describe.GetPathDescription().GetDomainDescription().GetShardsInside();

        Ydb::Table::TableIndex index = FulltextIndexConfig(true);
        const ui32 requiredPaths = runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex() ? 6 : 5;
        const ui32 requiredShards = runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex() ? 5 : 4;

        TSchemeLimits lowLimits;

        lowLimits.MaxPaths = 1 + requiredPaths;
        lowLimits.MaxShards = curShards + requiredShards - 1;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 5;
        lowLimits.MaxShards = curShards + requiredShards;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 1 + requiredPaths;
        lowLimits.MaxShards = curShards + requiredShards;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, txId);
    }

    // Helpers for __ydb_row_id opt-in tests below: tables have a Utf8 PK plus a __ydb_row_id Uint64 NOT NULL
    // column, and a Ready unique secondary index on __ydb_row_id is created before the fulltext build.

    void DoCreateTextTableWithRowId(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
            const TString& rowIdType = "Uint64",
            bool rowIdNotNull = true,
            bool createUniqueIndex = true,
            const TString& uniqueIndexKey = NTableIndex::NFulltext::RowIdColumn) {
        const TString tableColumns = Sprintf(R"(
                Columns { Name: "pk" Type: "Utf8" NotNull: true }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "data" Type: "String" }
                Columns { Name: "%s" Type: "%s" %s }
                KeyColumnNames: ["pk"]
        )", NTableIndex::NFulltext::RowIdColumn, rowIdType.c_str(),
            rowIdNotNull ? "NotNull: true" : "");

        if (!createUniqueIndex) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "texts"
                %s
            )", tableColumns.c_str()));
        } else {
            TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableDescription {
                    Name: "texts"
                    %s
                }
                IndexDescription {
                    Name: "uniq_rowid"
                    KeyColumnNames: ["%s"]
                    Type: EIndexTypeGlobalUnique
                }
            )", tableColumns.c_str(), uniqueIndexKey.c_str()));
        }
        env.TestWaitNotification(runtime, txId);
    }

    void DoWriteRowsWithRowId(TTestBasicRuntime& runtime) {
        auto tableDesc = DescribePath(runtime, "/MyRoot/texts", /*returnPartitioning*/ true, /*returnBoundaries*/ true);
        const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT(!tablePartitions.empty());
        const ui64 textsTabletId = tablePartitions[0].GetDatashardId();

        auto fnWriteRow = [&] (TString pk, ui64 rowId, TString text, TString data) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('pk     (Utf8 '%s) ) ) )
                    (let row   '( '('text   (String '"%s") )
                                  '('data   (String '"%s") )
                                  '('%s (Uint64 '%lu) ) ) )
                    (return (AsList (UpdateRow '__user__texts key row) ))
                )
            )", pk.c_str(), text.c_str(), data.c_str(),
                NTableIndex::NFulltext::RowIdColumn, rowId);

            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, textsTabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        };

        fnWriteRow("pone",   1, "green apple",              "one");
        fnWriteRow("ptwo",   2, "red apple and blue apple", "two");
        fnWriteRow("pthree", 3, "yellow apple",             "three");
        fnWriteRow("pfour",  4, "red car",                  "four");
    }

    Y_UNIT_TEST(RowIdOptIn_PlainBuildsAndKeysByRowId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTableWithRowId(runtime, env, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/uniq_rowid"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        DoWriteRowsWithRowId(runtime);

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                op.DebugString());
        }

        if (!runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), {
                NLs::PathExist,
                NLs::CheckColumns("indexImplTable",
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    {},
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    /*ensureNoOther=*/ true),
            });
        }
    }

    Y_UNIT_TEST(RowIdOptIn_RelevanceBuildsAndKeysByRowId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTableWithRowId(runtime, env, txId);
        DoWriteRowsWithRowId(runtime);

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ true);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                op.DebugString());
        }

        // docs impl-table must be keyed by [__ydb_row_id] (the synthetic doc_id), not by pk.
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplDocsTable"), {
            NLs::PathExist,
        });
    }

    Y_UNIT_TEST(RowIdOptIn_RejectsIfRowIdWrongType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        DoCreateTextTableWithRowId(runtime, env, txId,
            /*rowIdType=*/ "Uint32",
            /*rowIdNotNull=*/ true,
            /*createUniqueIndex=*/ false);

        Ydb::Table::TableIndex index = FulltextIndexConfig(false);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index,
            Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(RowIdOptIn_RejectsIfRowIdNullable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        DoCreateTextTableWithRowId(runtime, env, txId,
            /*rowIdType=*/ "Uint64",
            /*rowIdNotNull=*/ false,
            /*createUniqueIndex=*/ false);

        Ydb::Table::TableIndex index = FulltextIndexConfig(false);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index,
            Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(RowIdOptIn_AutoProvisionsMissingUniqueIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        // __ydb_row_id is well-formed (Uint64 NOT NULL) but has no unique index yet. With the unique-index
        // feature enabled (TTestEnv enables it), the build auto-provisions the missing unique index.
        DoCreateTextTableWithRowId(runtime, env, txId,
            /*rowIdType=*/ "Uint64",
            /*rowIdNotNull=*/ true,
            /*createUniqueIndex=*/ false);

        Ydb::Table::TableIndex index = FulltextIndexConfig(false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());

        // The unique index over __ydb_row_id was auto-provisioned and is Ready.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
    }

    Y_UNIT_TEST(RowIdOptIn_AutoProvisionsWhenUniqueIndexOnDifferentColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        // __ydb_row_id is well-formed, but the only existing unique index keys some other column. The build
        // ignores that unrelated index and auto-provisions its own unique index over __ydb_row_id.
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                Columns { Name: "pk" Type: "Utf8" NotNull: true }
                Columns { Name: "text" Type: "String" }
                Columns { Name: "data" Type: "String" }
                Columns { Name: "%s" Type: "Uint64" NotNull: true }
                Columns { Name: "other" Type: "Uint64" }
                KeyColumnNames: ["pk"]
            }
            IndexDescription {
                Name: "uniq_other"
                KeyColumnNames: ["other"]
                Type: EIndexTypeGlobalUnique
            }
        )", NTableIndex::NFulltext::RowIdColumn));
        env.TestWaitNotification(runtime, txId);

        Ydb::Table::TableIndex index = FulltextIndexConfig(false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());

        // A dedicated unique index over __ydb_row_id was auto-provisioned (uniq_other is left untouched).
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
    }

    Y_UNIT_TEST(RowIdOptIn_AutoProvisionsRowIdAndUniqueIndexForCustomPk) {
        // A custom (non single integer) PK without __ydb_row_id is auto-provisioned: the build adds the
        // __ydb_row_id column and a unique index over it (the unique-index feature is enabled by TTestEnv).
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "pk" Type: "Utf8" NotNull: true }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: ["pk"]
        )");
        env.TestWaitNotification(runtime, txId);

        Ydb::Table::TableIndex index = FulltextIndexConfig(false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());

        // Both the __ydb_row_id column and its unique index were auto-provisioned; the unique index is Ready.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
    }

    Y_UNIT_TEST(RowIdDisabled_RejectsCustomPkBuild) {
        // With EnableFulltextIndexRowId off, building a fulltext index over a custom (non single integer)
        // PK cannot use or auto-provision __ydb_row_id, so the build is rejected (mirrors the CREATE TABLE
        // path in TFulltextIndexTests::CreateTableRowIdDisabled).
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        // The gate reads this flag live at classify time, so setting it here disables rowid doc_id mode.
        appData.FeatureFlags.SetEnableFulltextIndexRowId(false);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "pk" Type: "Utf8" NotNull: true }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: ["pk"]
        )");
        env.TestWaitNotification(runtime, txId);

        Ydb::Table::TableIndex index = FulltextIndexConfig(false);
        AsyncBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        {
            TAutoPtr<IEventHandle> handle;
            auto* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL_C(event->Record.GetStatus(), Ydb::StatusIds::BAD_REQUEST,
                event->Record.GetIssues());
            UNIT_ASSERT_STRING_CONTAINS(event->Record.DebugString(),
                "requires the __ydb_row_id doc_id feature, which is disabled (feature flag EnableFulltextIndexRowId)");
        }

        // No __ydb_row_id unique index (nor the fulltext index) was provisioned.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx"), {
            NLs::PathNotExist,
        });
    }

    // Helpers for the auto-provisioning tests below: a table with a custom (Utf8) PK and NO __ydb_row_id
    // column / unique index - the schemeshard provisions both when the fulltext index is built.

    void DoCreateCustomPkTextTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "pk" Type: "Utf8" NotNull: true }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: ["pk"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    void DoWriteRowsCustomPk(TTestBasicRuntime& runtime) {
        auto tableDesc = DescribePath(runtime, "/MyRoot/texts", /*returnPartitioning*/ true, /*returnBoundaries*/ true);
        const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT(!tablePartitions.empty());
        const ui64 textsTabletId = tablePartitions[0].GetDatashardId();

        auto fnWriteRow = [&] (TString pk, TString text, TString data) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('pk     (Utf8 '%s) ) ) )
                    (let row   '( '('text   (String '"%s") )
                                  '('data   (String '"%s") ) ) )
                    (return (AsList (UpdateRow '__user__texts key row) ))
                )
            )", pk.c_str(), text.c_str(), data.c_str());

            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, textsTabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        };

        fnWriteRow("pone",   "green apple",              "one");
        fnWriteRow("ptwo",   "red apple and blue apple", "two");
        fnWriteRow("pthree", "yellow apple",             "three");
        fnWriteRow("pfour",  "red car",                  "four");
    }

    void EnableAutoProvisionFlags(TTestActorRuntime& runtime) {
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableFulltextIndex(true);
        appData.FeatureFlags.SetEnableAddUniqueIndex(true);
        appData.FeatureFlags.SetEnableUniqConstraint(true);
    }

    Y_UNIT_TEST(AutoProvision_FirstFulltextBuildAddsRowIdAndUniqueIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableAutoProvisionFlags(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                op.DebugString());
        }

        // The auto-provisioned unique index over __ydb_row_id exists and is Ready.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        if (!runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            // The fulltext posting impl-table is keyed by [__ydb_token, __ydb_row_id].
            // But with the compact index, it doesn't differ.
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), {
                NLs::PathExist,
                NLs::CheckColumns("indexImplTable",
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    {},
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    /*ensureNoOther=*/ true),
            });
        }
    }

    Y_UNIT_TEST(RejectDropRowIdUniqueIndexUsedByFulltext) {
        // The auto-provisioned unique index over __ydb_row_id must not be droppable while a fulltext
        // index resolves its documents through it - dropping it would orphan every posting entry. Once
        // the dependent fulltext index is gone, the unique index can be dropped.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);
        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        const TString uniqueIndexPath = TStringBuilder()
            << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName;

        // Dropping the unique index while the fulltext index depends on it is rejected.
        TestDropTableIndex(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "texts"
            IndexName: "%s"
        )", NTableIndex::NFulltext::RowIdUniqueIndexName),
            {NKikimrScheme::StatusPreconditionFailed});

        // ... and the unique index is still present and Ready.
        TestDescribeResult(DescribePrivatePath(runtime, uniqueIndexPath), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        // Drop the dependent fulltext index first ...
        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "texts"
            IndexName: "fulltext_idx"
        )");
        env.TestWaitNotification(runtime, txId);

        // ... now nothing depends on the unique index over __ydb_row_id, so it can be dropped.
        TestDropTableIndex(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "texts"
            IndexName: "%s"
        )", NTableIndex::NFulltext::RowIdUniqueIndexName));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, uniqueIndexPath), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(DropRowIdColumnAfterRemovingFulltextInfra) {
        // Once the fulltext index and the unique index over __ydb_row_id are gone, the synthetic
        // __ydb_row_id column itself can be dropped - and its backing sequence is removed with it.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);
        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        const TString rowIdSequencePath = TStringBuilder()
            << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdSequenceName;

        // The synthetic column's backing sequence was provisioned as a child of the table.
        TestDescribeResult(DescribePrivatePath(runtime, rowIdSequencePath), { NLs::PathExist });

        // While the unique index over __ydb_row_id exists, the column is an index key and
        // cannot be dropped.
        TestAlterTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "texts"
            DropColumns { Name: "%s" }
        )", NTableIndex::NFulltext::RowIdColumn),
            {NKikimrScheme::StatusPreconditionFailed});

        // Remove the dependents: the fulltext index, then the unique index over __ydb_row_id.
        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "texts"
            IndexName: "fulltext_idx"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "texts"
            IndexName: "%s"
        )", NTableIndex::NFulltext::RowIdUniqueIndexName));
        env.TestWaitNotification(runtime, txId);

        // Now __ydb_row_id is an ordinary sequence-backed column: dropping it cascade-drops the
        // backing sequence in the same operation.
        TestAlterTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "texts"
            DropColumns { Name: "%s" }
        )", NTableIndex::NFulltext::RowIdColumn));
        env.TestWaitNotification(runtime, txId);

        // The column is gone from the table ...
        TestDescribeResult(DescribePath(runtime, "/MyRoot/texts"), {
            NLs::CheckColumns("texts", {"pk", "text", "data"}, {NTableIndex::NFulltext::RowIdColumn}, {"pk"}),
        });

        // ... and its backing sequence was dropped together with it.
        TestDescribeResult(DescribePrivatePath(runtime, rowIdSequencePath), { NLs::PathNotExist });
    }

    Y_UNIT_TEST(AutoProvision_SecondFulltextBuildReusesInfra) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        // First fulltext index provisions __ydb_row_id + the unique index.
        {
            Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
            index.set_name("fulltext_one");
            const ui64 buildTx = ++txId;
            TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
            env.TestWaitNotification(runtime, buildTx);
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // Second fulltext index reuses the existing __ydb_row_id + unique index (no duplicates).
        {
            Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ true);
            index.set_name("fulltext_two");
            const ui64 buildTx = ++txId;
            TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
            env.TestWaitNotification(runtime, buildTx);
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // Exactly one unique index over __ydb_row_id exists, and both fulltext indexes key by __ydb_row_id.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        if (!runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_two/indexImplTable"), {
                NLs::PathExist,
                NLs::CheckColumns("indexImplTable",
                    // Relevance posting table also carries the __ydb_freq value column.
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn, NTableIndex::NFulltext::FreqColumn },
                    {},
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    /*ensureNoOther=*/ true),
            });
        }
    }

    Y_UNIT_TEST(AutoProvision_DropAllFulltextIndexesAndRecreateReusesInfra) {
        // Dropping fulltext indexes must not silently tear down the shared row-id infrastructure:
        // a later build reuses the same column, sequence and Ready unique index.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        for (const auto& [name, relevance] : TVector<std::pair<TString, bool>>{
                 {"fulltext_one", false}, {"fulltext_two", true}}) {
            auto index = FulltextIndexConfig(relevance);
            index.set_name(name);
            const ui64 buildTx = ++txId;
            TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
                "/MyRoot", "/MyRoot/texts", index);
            env.TestWaitNotification(runtime, buildTx);
            const auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        for (const TStringBuf name : {TStringBuf("fulltext_one"), TStringBuf("fulltext_two")}) {
            TestDropTableIndex(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "texts"
                IndexName: "%s"
            )", TString(name).c_str()));
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_one"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_two"), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/texts"), {
            NLs::CheckColumns("texts",
                {"pk", "text", "data", NTableIndex::NFulltext::RowIdColumn},
                {}, {"pk"}),
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdSequenceName), {
            NLs::PathExist,
        });

        {
            const auto table = DescribePath(runtime, "/MyRoot/texts", true, true);
            const auto& indexes = table.GetPathDescription().GetTable().GetTableIndexes();
            UNIT_ASSERT_VALUES_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexes.Get(0).GetName(),
                NTableIndex::NFulltext::RowIdUniqueIndexName);
        }

        auto recreated = FulltextIndexConfig(/*relevance*/ true);
        recreated.set_name("fulltext_one");
        const ui64 recreateTx = ++txId;
        TestBuildIndex(runtime, recreateTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", recreated);
        env.TestWaitNotification(runtime, recreateTx);
        const auto recreateOp = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", recreateTx);
        UNIT_ASSERT_VALUES_EQUAL_C(recreateOp.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, recreateOp.DebugString());

        const auto table = DescribePath(runtime, "/MyRoot/texts", true, true);
        THashSet<TString> indexNames;
        for (const auto& index : table.GetPathDescription().GetTable().GetTableIndexes()) {
            UNIT_ASSERT(indexNames.insert(index.GetName()).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(indexNames.size(), 2);
        UNIT_ASSERT(indexNames.contains(NTableIndex::NFulltext::RowIdUniqueIndexName));
        UNIT_ASSERT(indexNames.contains("fulltext_one"));
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_one/indexImplTable"), {
            NLs::PathExist,
            NLs::CheckColumns("indexImplTable",
                {NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn,
                 NTableIndex::NFulltext::FreqColumn},
                {},
                {NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn},
                /*ensureNoOther=*/ true),
        });
    }

    Y_UNIT_TEST(AutoProvision_ConcurrentFulltextBuildsSerializeAndReuseInfra) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableAutoProvisionFlags(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "tenant" Type: "Utf8" NotNull: true }
            Columns { Name: "external_id" Type: "Uint64" NotNull: true }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: ["tenant", "external_id"]
        )");
        env.TestWaitNotification(runtime, txId);

        Ydb::Table::TableIndex firstIndex = FulltextIndexConfig(/*relevance*/ false);
        firstIndex.set_name("fulltext_one");
        Ydb::Table::TableIndex secondIndex = FulltextIndexConfig(/*relevance*/ true);
        secondIndex.set_name("fulltext_two");

        // Start the second build after the first request has been accepted, but before waiting for its
        // completion. It therefore classifies the same composite-PK table while the first build is still
        // provisioning __ydb_row_id, its sequence and unique index.
        const ui64 firstBuildTx = ++txId;
        const ui64 secondBuildTx = ++txId;
        AsyncBuildIndex(runtime, firstBuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", firstIndex);

        THashMap<ui64, NKikimrIndexBuilder::TEvCreateResponse> responses;
        auto grabCreateResponse = [&] {
            TAutoPtr<IEventHandle> handle;
            auto* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
            UNIT_ASSERT(event);
            Cerr << "CONCURRENT BUILD RESPONSE: " << event->Record.DebugString() << Endl;
            UNIT_ASSERT_C(event->Record.GetTxId() == firstBuildTx || event->Record.GetTxId() == secondBuildTx,
                "response for unexpected build: " << event->Record.DebugString());
            UNIT_ASSERT_C(responses.emplace(event->Record.GetTxId(), event->Record).second,
                "duplicate response for build " << event->Record.GetTxId());
        };
        grabCreateResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(responses.at(firstBuildTx).GetStatus(), Ydb::StatusIds::SUCCESS,
            responses.at(firstBuildTx).GetIssues());

        AsyncBuildIndex(runtime, secondBuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", secondIndex);
        grabCreateResponse();

        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 2);
        const auto& firstResponse = responses.at(firstBuildTx);
        const auto& secondResponse = responses.at(secondBuildTx);

        UNIT_ASSERT_VALUES_EQUAL_C(firstResponse.GetStatus(), Ydb::StatusIds::SUCCESS,
            firstResponse.GetIssues());

        const auto secondStatus = secondResponse.GetStatus();
        UNIT_ASSERT_VALUES_EQUAL_C(secondStatus, Ydb::StatusIds::OVERLOADED,
            secondResponse.GetIssues());
        UNIT_ASSERT_STRING_CONTAINS(secondResponse.DebugString(), "StatusMultipleModifications");

        env.TestWaitNotification(runtime, firstBuildTx);
        auto firstOp = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", firstBuildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(firstOp.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, firstOp.DebugString());

        // A concurrent schema modification is a retryable serialization outcome. Once the winner has
        // provisioned row-id infrastructure, a fresh request must reuse it and complete normally.
        const ui64 retryBuildTx = ++txId;
        TestBuildIndex(runtime, retryBuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", secondIndex);
        env.TestWaitNotification(runtime, retryBuildTx);
        auto retryOp = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", retryBuildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(retryOp.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, retryOp.DebugString());

        // The fixed infrastructure names resolve to one Ready unique index and one backing sequence.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdSequenceName), {
            NLs::PathExist,
        });

        const auto tableDescription = DescribePath(runtime, "/MyRoot/texts", true, true);
        THashSet<TString> indexNames;
        for (const auto& index : tableDescription.GetPathDescription().GetTable().GetTableIndexes()) {
            UNIT_ASSERT_C(indexNames.insert(index.GetName()).second,
                "duplicate index in table description: " << index.GetName());
        }
        UNIT_ASSERT_VALUES_EQUAL_C(indexNames.size(), 3,
            "expected exactly the row-id unique index and two fulltext indexes");
        UNIT_ASSERT(indexNames.contains(NTableIndex::NFulltext::RowIdUniqueIndexName));
        UNIT_ASSERT(indexNames.contains("fulltext_one"));
        UNIT_ASSERT(indexNames.contains("fulltext_two"));

        // Both successful builds use the single synthetic document id.
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_one/indexImplTable"), {
            NLs::PathExist,
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                /*ensureNoOther=*/ true),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_two/indexImplTable"), {
            NLs::PathExist,
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn,
                  NTableIndex::NFulltext::FreqColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                /*ensureNoOther=*/ true),
        });
    }

    Y_UNIT_TEST(AutoProvision_SingleIntegerPkUnaffected) {
        // A single integer PK keeps the legacy doc_id=PK behaviour: no __ydb_row_id / unique index added.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateTextTable(runtime, env, txId);

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // No auto unique index was created.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST_TWIN(ImportExport, Materialized) {
        NKikimr::InitAwsAPI();

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        NWrappers::NTestHelpers::TS3Mock s3Mock({}, NWrappers::NTestHelpers::TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableIndexMaterialization(Materialized));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTableAndIndex(runtime, env, txId, false, [&](Ydb::Table::TableIndex& index) {
            index.add_data_columns("data");
        });

        {
            auto index = FulltextIndexConfig(true);
            index.set_name("fulltext_rel_idx");
            index.add_data_columns("data");
            const ui64 buildIndexTx = ++txId;
            TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
            env.TestWaitNotification(runtime, buildIndexTx);
        }

        auto checkIndexes = [&](const TString& path) {
            const auto d = DescribePath(runtime, path, true, true);
            THashSet<TString> found;
            for (const auto& idx: d.GetPathDescription().GetTable().GetTableIndexes()) {
                found.insert(idx.GetName());
                if (idx.GetName() == "fulltext_idx") {
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()
                        ? NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact
                        : NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain);
                } else if (idx.GetName() == "fulltext_rel_idx") {
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()
                        ? NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance
                        : NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance);
                }
            }
            UNIT_ASSERT_C(found.contains("fulltext_idx"), "missing fulltext_idx on " << path);
            UNIT_ASSERT_C(found.contains("fulltext_rel_idx"), "missing fulltext_rel_idx on " << path);
        };

        checkIndexes("/MyRoot/texts");

        const ui64 exportTxId = ++txId;
        TestExport(runtime, exportTxId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                    source_path: "/MyRoot/texts"
                    destination_prefix: "test"
                }
                %s
            }
        )", port, Materialized ? "include_index_data: true" : ""));
        env.TestWaitNotification(runtime, exportTxId);
        TestGetExport(runtime, exportTxId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        const ui64 importId = ++txId;
        const TString popMode = Materialized
            ? "index_population_mode: "+Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT)
            : "";
        TestImport(runtime, importId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                    source_prefix: "test"
                    destination_path: "/MyRoot/texts_imported"
                }
                %s
            }
        )", port, popMode.c_str()));
        env.TestWaitNotification(runtime, importId);
        TestGetImport(runtime, importId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        checkIndexes("/MyRoot/texts_imported");
        DoCheckPlainIndexTable(runtime, "/MyRoot/texts_imported/fulltext_idx");
        DoCheckRelevanceIndexTables(runtime, "/MyRoot/texts_imported/fulltext_rel_idx");

        NKikimr::ShutdownAwsAPI();
    }

    // TTestEnv already enables EnableFulltextIndex / EnableAddUniqueIndex by default; we only need the
    // compact-index flag so a fulltext_plain build proto is materialized as a compact (rowid-mode) index.
    // The schemeshard caches EnableCompactFulltextIndex at activation (it read appData before this runs),
    // so reboot it to pick up the updated value.
    void EnableCompactAutoProvisionFlags(TTestActorRuntime& runtime) {
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableFulltextIndex(true);
        appData.FeatureFlags.SetEnableCompactFulltextIndex(true);
        appData.FeatureFlags.SetEnableAddUniqueIndex(true);
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
    }

    void SetCompactFulltextFlag(TTestBasicRuntime& runtime, bool enabled) {
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto& flags = *request->Record.MutableConfig()->MutableFeatureFlags();
        // A console feature-flags item is a complete snapshot, not a field-level patch. Keep the
        // dependencies enabled while changing only the compact-layout selection under test.
        flags.SetEnableFulltextIndex(true);
        flags.SetEnableCompactFulltextIndex(enabled);
        flags.SetEnableAddUniqueIndex(true);
        flags.SetEnableUniqConstraint(true);
        SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
    }

    Y_UNIT_TEST(PublicBuildUsesLiveSchemeShardCompactFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Start with the legacy layout and some data, then deliver each SchemeShard config update
        // synchronously (SetConfig waits for TEvConfigNotificationResponse) before starting the next
        // public BuildIndex operation. This makes the physical-type decision deterministic.
        DoCreateTextTableAndIndex(runtime, env, txId, /*relevance*/ false,
            [](Ydb::Table::TableIndex& index) { index.add_data_columns("data"); });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        DoCheckPlainIndexTable(runtime, "/MyRoot/texts/fulltext_idx");

        SetCompactFulltextFlag(runtime, true);
        auto compact = FulltextIndexConfig(/*relevance*/ false);
        compact.set_name("compact_idx");
        const ui64 compactBuildTx = ++txId;
        TestBuildIndex(runtime, compactBuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", compact);
        env.TestWaitNotification(runtime, compactBuildTx);
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/compact_idx"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/compact_idx/indexImplTable"), {
            NLs::PathExist,
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn, NTableIndex::NFulltext::AddedColumn,
                  NTableIndex::NFulltext::SegmentColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn },
                /*strictCount=*/ true),
        });

        // Switching back affects only future builds. Both already-built physical layouts remain
        // independently describable and droppable; no reinterpretation through the current flag occurs.
        SetCompactFulltextFlag(runtime, false);
        auto legacyAfterToggle = FulltextIndexConfig(/*relevance*/ false);
        legacyAfterToggle.set_name("legacy_after_toggle");
        legacyAfterToggle.add_data_columns("data");
        const ui64 legacyBuildTx = ++txId;
        TestBuildIndex(runtime, legacyBuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", legacyAfterToggle);
        env.TestWaitNotification(runtime, legacyBuildTx);
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/legacy_after_toggle"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        DoCheckPlainIndexTable(runtime, "/MyRoot/texts/legacy_after_toggle");

        for (const TStringBuf index : {TStringBuf("compact_idx"), TStringBuf("fulltext_idx")}) {
            TestDropTableIndex(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "texts"
                IndexName: "%s"
            )", index.data()));
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePrivatePath(runtime,
                TStringBuilder() << "/MyRoot/texts/" << index), {NLs::PathNotExist});
        }
    }

    void RebootTableShardsAndAssertPartitions(TTestBasicRuntime& runtime, const TString& path,
        ui32 expectedPartitions)
    {
        const auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard,
            path, /*returnPartitioning=*/ true, /*returnBoundaries=*/ true, /*showPrivate=*/ true);
        const auto& partitions = describe.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT_VALUES_EQUAL_C(partitions.size(), expectedPartitions, path);
        for (const auto& partition : partitions) {
            RebootTablet(runtime, partition.GetDatashardId(), runtime.AllocateEdgeActor());
        }
    }

    Y_UNIT_TEST(RowIdOptIn_CompactTopologyImplSplitMainMergeRebootAndRebuild) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableCompactAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        auto initialIndex = FulltextIndexConfig(/*relevance=*/ false);
        const ui64 initialBuildTx = ++txId;
        TestBuildIndex(runtime, initialBuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", initialIndex);
        env.TestWaitNotification(runtime, initialBuildTx);
        const auto initialBuild = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot", initialBuildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(initialBuild.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, initialBuild.DebugString());

        const TString compactImpl = "/MyRoot/texts/fulltext_idx/indexImplTable";
        const TString rowIdImpl = TStringBuilder() << "/MyRoot/texts/"
            << NTableIndex::NFulltext::RowIdUniqueIndexName << "/" << NTableIndex::ImplTable;
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts"), {
            NLs::CheckColumns("texts",
                {"pk", "text", "data", NTableIndex::NFulltext::RowIdColumn},
                {}, {"pk"}, /*strictCount=*/ true),
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime, compactImpl), {
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn, NTableIndex::NFulltext::AddedColumn,
                  NTableIndex::NFulltext::SegmentColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn },
                /*strictCount=*/ true),
        });
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, compactImpl), 7u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rowIdImpl), 4u);
        const TString mainRowsBeforeTopology = ReadShards(
            runtime, TTestTxConfig::SchemeShard, "/MyRoot/texts").at(0);

        auto split = [&](const TString& path, const TString& boundary) {
            const auto before = DescribePath(runtime, TTestTxConfig::SchemeShard,
                path, /*returnPartitioning=*/ true, /*returnBoundaries=*/ true, /*showPrivate=*/ true);
            const auto& partitions = before.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_VALUES_EQUAL_C(partitions.size(), 1u, path);
            TestSplitTable(runtime, TTestTxConfig::SchemeShard, ++txId, path,
                Sprintf(R"(
                    SourceTabletId: %lu
                    SplitBoundary { KeyPrefix { %s } }
                )", partitions[0].GetDatashardId(), boundary.c_str()));
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, TTestTxConfig::SchemeShard,
                path, true, true, true), {
                NLs::PathExist,
                NLs::PartitionCount(2),
            });
        };

        // Special compact and unique implementation tables follow the same explicit split contract as
        // ordinary index implementation tables. Every operation is fenced by its SchemeShard notification.
        split("/MyRoot/texts", R"(Tuple { Optional { Text: "ptwo" } })");
        split(compactImpl, R"(Tuple { Optional { Bytes: "red" } })");
        split(rowIdImpl, R"(Tuple { Optional { Uint64: 9223372036854775808 } })");

        RebootTableShardsAndAssertPartitions(runtime, "/MyRoot/texts", 2);
        RebootTableShardsAndAssertPartitions(runtime, compactImpl, 2);
        RebootTableShardsAndAssertPartitions(runtime, rowIdImpl, 2);

        // Split children initially borrow parts from their common source. Materialize those parts on both
        // main-table children before merge; otherwise flat executor correctly rejects a back-borrow chain.
        const auto mainAfterReboot = DescribePath(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/texts", /*returnPartitioning=*/ true, /*returnBoundaries=*/ true,
            /*showPrivate=*/ true);
        const auto& mainChildren = mainAfterReboot.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT_VALUES_EQUAL(mainChildren.size(), 2u);
        const auto& mainSelf = mainAfterReboot.GetPathDescription().GetSelf();
        const TTableId mainTableId(mainSelf.GetSchemeshardId(), mainSelf.GetPathId());
        for (const auto& child : mainChildren) {
            const auto result = CompactTable(runtime, child.GetDatashardId(), mainTableId,
                /*compactBorrowed=*/ true, /*compactSinglePartedShards=*/ true);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(),
                NKikimrTxDataShard::TEvCompactTableResult::OK, result.DebugString());
        }

        // Merging special compact/unique implementation tables is not supported by the current DataShard
        // borrow logic: it aborts on back-borrowed parts ("must not back-borrow parts"). Keep their
        // supported split+reboot transition covered, and merge only the ordinary main table.
        const auto compactRowsAfterSplit = ReadShards(runtime, TTestTxConfig::SchemeShard, compactImpl);
        const auto rowIdRowsAfterSplit = ReadShards(runtime, TTestTxConfig::SchemeShard, rowIdImpl);
        TestSplitTable(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/texts",
            Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", mainChildren[0].GetDatashardId(), mainChildren[1].GetDatashardId()));
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/texts", true, true, true), {
            NLs::PathExist,
            NLs::PartitionCount(1),
        });

        // Exact physical-data oracles: the merged main table returns to its original one-shard image,
        // while a main-table merge must not mutate either still-split implementation table.
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, compactImpl), 7u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rowIdImpl), 4u);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/texts").at(0), mainRowsBeforeTopology);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard,
            compactImpl), compactRowsAfterSplit);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard,
            rowIdImpl), rowIdRowsAfterSplit);

        // Low-level MiniKQL intentionally bypasses index maintenance in this harness. Update an existing
        // row (so its provisioned row id remains intact), then rebuild a second compact index: its exact
        // token oracle proves the post-topology main-table DML was visible to a build scan.
        const auto main = DescribePath(runtime, "/MyRoot/texts", true, true);
        const ui64 firstShard = main.GetPathDescription().GetTablePartitions(0).GetDatashardId();
        NKikimrMiniKQL::TResult updateResult;
        TString updateError;
        const auto updateStatus = LocalMiniKQL(runtime, firstShard, R"(
            (
                (let key '( '('pk (Utf8 'pone) ) ) )
                (let row '( '('text (String '"topology kiwi") )
                             '('data (String '"updated") ) ) )
                (return (AsList (UpdateRow '__user__texts key row) ))
            )
        )", updateResult, updateError);
        UNIT_ASSERT_VALUES_EQUAL_C(updateStatus, NKikimrProto::EReplyStatus::OK, updateError);

        auto rebuiltIndex = FulltextIndexConfig(/*relevance=*/ false);
        rebuiltIndex.set_name("after_topology_idx");
        const ui64 rebuildTx = ++txId;
        TestBuildIndex(runtime, rebuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", rebuiltIndex);
        env.TestWaitNotification(runtime, rebuildTx);
        const auto rebuild = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", rebuildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(rebuild.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, rebuild.DebugString());

        const TString rebuiltImpl = "/MyRoot/texts/after_topology_idx/indexImplTable";
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rebuiltImpl), 8u);
        TString physicalRows;
        for (const auto& shard : ReadShards(runtime, TTestTxConfig::SchemeShard, rebuiltImpl)) {
            physicalRows += shard;
        }
        UNIT_ASSERT_C(physicalRows.Contains("topology"), physicalRows);
        UNIT_ASSERT_C(physicalRows.Contains("kiwi"), physicalRows);
    }

    TString RowIdSrcTablePath(const TString& indexPath) {
        return TStringBuilder() << indexPath << "/"
            << NTableIndex::ImplTable << NTableIndex::NFulltext::RowIdSrcBuildSuffix;
    }

    Y_UNIT_TEST(RowIdOptIn_CompactQuotaRejectionIsAtomicAndRetryable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        EnableCompactAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        const auto before = DescribePath(runtime, "/MyRoot/texts");
        const auto& domain = before.GetPathDescription().GetDomainDescription();
        const ui64 pathsBefore = domain.GetPathsInside();
        const ui64 shardsBefore = domain.GetShardsInside();

        // Leave no room for any of the row-id column/sequence/unique/compact implementation objects.
        // The whole public request must be rejected before the first provisioning sub-operation commits.
        TSchemeLimits limits;
        // MaxPaths is enforced against the operation's table subtree, not DomainDescription.PathsInside.
        // One path is enough for the existing table itself but not for any provisioning child.
        limits.MaxPaths = 1;
        limits.MaxShards = shardsBefore + 100;
        SetSchemeshardSchemaLimits(runtime, limits);

        const ui64 rejectedTx = ++txId;
        TestBuildIndex(runtime, rejectedTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", FulltextIndexConfig(/*relevance=*/ false),
            Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, rejectedTx);

        const auto afterReject = DescribePath(runtime, "/MyRoot/texts");
        UNIT_ASSERT_VALUES_EQUAL(
            afterReject.GetPathDescription().GetDomainDescription().GetPathsInside(), pathsBefore);
        UNIT_ASSERT_VALUES_EQUAL(
            afterReject.GetPathDescription().GetDomainDescription().GetShardsInside(), shardsBefore);
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdSequenceName), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/fulltext_idx")), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts"), {
            NLs::CheckColumns("texts", {"pk", "text", "data"}, {}, {"pk"}, /*strictCount=*/ true),
        });

        // Raising the quota must make the identical request succeed; rejection must not poison its name
        // or leave an invisible build record/schema transaction behind.
        limits.MaxPaths = pathsBefore + 100;
        limits.MaxShards = shardsBefore + 100;
        SetSchemeshardSchemaLimits(runtime, limits);
        const ui64 retryTx = ++txId;
        TestBuildIndex(runtime, retryTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", FulltextIndexConfig(/*relevance=*/ false));
        env.TestWaitNotification(runtime, retryTx);
        const auto retry = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", retryTx);
        UNIT_ASSERT_VALUES_EQUAL_C(retry.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, retry.DebugString());
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), 7u);
    }

    Y_UNIT_TEST(RowIdOptIn_CancelCompactPostingScanCleansUpAndAllowsRetry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableCompactAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        // This target is reached only after provisioning and the transient row-id source prepass have
        // completed, so cancellation exercises the deeper compact posting-scan cleanup boundary.
        TBlockEvents<TEvDataShard::TEvBuildFulltextIndexResponse> postingBlocker(runtime);

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", FulltextIndexConfig(/*relevance=*/ false));
        runtime.WaitFor("compact posting scan response", [&]{ return postingBlocker.size() > 0; });

        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        postingBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildTx);
        const auto cancelled = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(cancelled.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_CANCELLED, cancelled.DebugString());

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/fulltext_idx")), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        const ui64 retryTx = ++txId;
        TestBuildIndex(runtime, retryTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", FulltextIndexConfig(/*relevance=*/ false));
        env.TestWaitNotification(runtime, retryTx);
        const auto retry = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", retryTx);
        UNIT_ASSERT_VALUES_EQUAL_C(retry.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, retry.DebugString());

        const TString rowIdImpl = TStringBuilder() << "/MyRoot/texts/"
            << NTableIndex::NFulltext::RowIdUniqueIndexName << "/" << NTableIndex::ImplTable;
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), 7u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rowIdImpl), 4u);
    }

    Y_UNIT_TEST(RowIdOptIn_CancelCompactPrepassThenRestartAndRetryReusesInfra) {
        // Cancel after provisioning has completed and the compact row-id source prepass has started.
        // Cleanup removes the partial fulltext build, while the reusable row-id infrastructure remains
        // valid across a SchemeShard restart and a same-name retry.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableCompactAutoProvisionFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> prepassBlocker(runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTargetName().EndsWith(NTableIndex::NFulltext::RowIdSrcBuildSuffix);
        });

        auto index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", index);
        runtime.WaitFor("row-id source prepass scan request", [&]{ return prepassBlocker.size() > 0; });

        const auto runningOp = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(runningOp.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA, runningOp.DebugString());

        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        prepassBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildTx);

        const auto cancelledOp = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(cancelledOp.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_CANCELLED, cancelledOp.DebugString());
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/fulltext_idx")), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdSequenceName), {
            NLs::PathExist,
        });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        const ui64 retryTx = ++txId;
        TestBuildIndex(runtime, retryTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, retryTx);
        const auto retryOp = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", retryTx);
        UNIT_ASSERT_VALUES_EQUAL_C(retryOp.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, retryOp.DebugString());

        const auto table = DescribePath(runtime, "/MyRoot/texts", true, true);
        THashSet<TString> indexNames;
        for (const auto& tableIndex : table.GetPathDescription().GetTable().GetTableIndexes()) {
            UNIT_ASSERT(indexNames.insert(tableIndex.GetName()).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(indexNames.size(), 2);
        UNIT_ASSERT(indexNames.contains(NTableIndex::NFulltext::RowIdUniqueIndexName));
        UNIT_ASSERT(indexNames.contains("fulltext_idx"));
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/fulltext_idx")), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(RowIdOptIn_CompactBuildsOverCustomPkAndDropsRowIdSrc) {
        // Compact rowid-mode build over a custom (Utf8) PK: it runs the row-id source prepass, builds the
        // compact posting tables and, on completion, the transient "rowidsrc" build table is dropped.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableCompactFulltextIndex(true));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                op.DebugString());
        }

        // The auto-provisioned unique index over __ydb_row_id exists and is Ready.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        // The compact posting impl-table is keyed by [__ydb_token, __ydb_max_id, __ydb_generation] and
        // stores the delta-encoded __ydb_segment (this is what distinguishes a compact index from a plain
        // one, whose impl-table is keyed by [__ydb_token, __ydb_row_id] and has no segment column).
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), {
            NLs::PathExist,
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn, NTableIndex::NFulltext::AddedColumn,
                  NTableIndex::NFulltext::SegmentColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn },
                /*strictCount=*/ true),
        });

        // The transient row-id source build table was dropped on completion.
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/fulltext_idx")), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(RowIdOptIn_CompactBuildSurvivesSchemeShardRestart) {
        // The compact build adds a new prepass step (FulltextRowIdSrc substate). Reboot the schemeshard
        // while it is running the prepass and verify the persisted state lets the build resume and finish.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableCompactFulltextIndex(true));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateCustomPkTextTable(runtime, env, txId);
        DoWriteRowsCustomPk(runtime);

        // Pause the build in the prepass: the row-id source fill is a generic secondary-index build whose
        // target is the transient row-id source table.
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> prepassBlocker(runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTargetName().EndsWith(NTableIndex::NFulltext::RowIdSrcBuildSuffix);
        });

        Ydb::Table::TableIndex index = FulltextIndexConfig(/*relevance*/ false);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);

        runtime.WaitFor("row-id source prepass scan request", [&]{ return prepassBlocker.size() > 0; });

        // Crash + restart the schemeshard while parked in the FulltextRowIdSrc substate.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Let the (re-issued) prepass scan and the rest of the pipeline proceed.
        prepassBlocker.Stop().Unblock();

        // The reboot drops the build's in-memory completion subscribers, so poll the persisted build
        // state to completion instead of relying on a (now racy) notification subscription.
        Ydb::Table::IndexBuildState::State state = Ydb::Table::IndexBuildState::STATE_UNSPECIFIED;
        for (int i = 0; i < 100; ++i) {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            state = op.GetIndexBuild().GetState();
            if (state == Ydb::Table::IndexBuildState::STATE_DONE ||
                state == Ydb::Table::IndexBuildState::STATE_REJECTED ||
                state == Ydb::Table::IndexBuildState::STATE_CANCELLED) {
                break;
            }
            env.SimulateSleep(runtime, TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL_C((ui64)state, (ui64)Ydb::Table::IndexBuildState::STATE_DONE,
            "compact fulltext build did not finish after schemeshard restart, last state: " << (ui64)state);

        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        // The transient row-id source build table was dropped on completion.
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/fulltext_idx")), {
            NLs::PathNotExist,
        });
    }
}
