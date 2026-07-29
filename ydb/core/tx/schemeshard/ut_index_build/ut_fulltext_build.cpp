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

    void DoCheckRelevanceIndexTables(TTestBasicRuntime& runtime, const TString& index) {
        auto rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplTable").at(0);
        Cerr << index << "/indexImplTable rows: " << rows << "\n";
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

        rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplDictTable").at(0);
        Cerr << index << "/indexImplDictTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["1";"and"];)"
            R"(["3";"apple"];)"
            R"(["1";"blue"];)"
            R"(["1";"car"];)"
            R"(["1";"green"];)"
            R"(["2";"red"];)"
            R"(["1";"yellow"]];)"
        "%false]]]", rows);

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
    // The borders upload (indexImplDictTable) must use the text column (IndexColumns.back()), not [0].
    Y_UNIT_TEST(PrefixedRelevanceBuilds) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableFulltextIndexPrefix(true);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreatePrefixedTextTable(runtime, env, txId);
        DoWriteRowsPrefixed(runtime);

        Ydb::Table::TableIndex index = PrefixedFulltextIndexConfig(/*relevance*/ true);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        // Without the fix the async build crashes; with it the build completes.
        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // The dictionary table (produced by SendUploadFulltextBordersRequest) exists and is populated.
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplDictTable"), {
            NLs::PathExist,
        });
        auto dictRows = ReadShards(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/texts/fulltext_idx/indexImplDictTable").at(0);
        Cerr << "indexImplDictTable rows: " << dictRows << "\n";
        // "apple" appears in the corpus, so the dictionary borders must contain it.
        UNIT_ASSERT_C(dictRows.Contains("apple"), "indexImplDictTable missing tokens: " << dictRows);

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

        TSchemeLimits lowLimits;

        lowLimits.MaxPaths = 6;
        lowLimits.MaxShards = curShards + 3;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 5;
        lowLimits.MaxShards = curShards + 4;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 6;
        lowLimits.MaxShards = curShards + 4;
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

        // posting impl-table must be keyed by [__ydb_token, __ydb_row_id], not by [__ydb_token, pk].
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), {
            NLs::PathExist,
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                /*ensureNoOther=*/ true),
        });
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

        // The fulltext posting impl-table is keyed by [__ydb_token, __ydb_row_id].
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/fulltext_idx/indexImplTable"), {
            NLs::PathExist,
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                /*ensureNoOther=*/ true),
        });
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
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain);
                } else if (idx.GetName() == "fulltext_rel_idx") {
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance);
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

    TString RowIdSrcTablePath(const TString& indexPath) {
        return TStringBuilder() << indexPath << "/"
            << NTableIndex::ImplTable << NTableIndex::NFulltext::RowIdSrcBuildSuffix;
    }

    Y_UNIT_TEST(RowIdOptIn_CompactBuildsOverCustomPkAndDropsRowIdSrc) {
        // Compact rowid-mode build over a custom (Utf8) PK: it runs the row-id source prepass, builds the
        // compact posting/dict tables and, on completion, the transient "rowidsrc" build table is dropped.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableCompactAutoProvisionFlags(runtime);
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
        TTestEnv env(runtime);
        EnableCompactAutoProvisionFlags(runtime);
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
