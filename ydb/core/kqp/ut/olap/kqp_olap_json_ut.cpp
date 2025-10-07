#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/testlib/common_helper.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapFeatures) {

    void WaitCompaction(TKikimrRunner& kikimr) {
        while (true) {
            auto status = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                SELECT Kind FROM `/Root/olapTable/.sys/primary_index_portion_stats`;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

            NYdb::TResultSetParser result(status.GetResultSet(0));
            UNIT_ASSERT(result.TryNextRow());
            if (result.ColumnParser("Kind").GetOptionalUtf8() == "SPLIT_COMPACTED") {
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    Y_UNIT_TEST(RandomJsonDocumentCharacters) {
        auto settings = TKikimrSettings();
        settings.AppConfig.MutableColumnShardConfig()->SetAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto result = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                CREATE TABLE `/Root/olapTable` (
                    id Uint32 NOT NULL,
                    json_payload JsonDocument,
                    PRIMARY KEY (id)
                )
                WITH (
                    STORE = COLUMN,
                    PARTITION_COUNT = 1
                );

                ALTER OBJECT `/Root/olapTable`
                (TYPE TABLE)
                SET (ACTION=ALTER_COLUMN, NAME=json_payload,
                `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`true`,
                `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`false`, `COLUMNS_LIMIT`=`1024`,
                `SPARSED_DETECTOR_KFF`=`20`, `MEM_LIMIT_CHUNK`=`52428800`, `OTHERS_ALLOWED_FRACTION`=`0`,
                `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`, `COMPRESSION.LEVEL`=`4`);
                )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        // TODO: fix other symbols
        {
            auto result = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                UPSERT INTO `/Root/olapTable` (id, json_payload)
                VALUES (1, JsonDocument(@@{"":   null}@@));
                       -- (2, JsonDocument(@@{"'":  null}@@)),
                       -- (3, JsonDocument(@@{"\"": null}@@)),
                       -- (4, JsonDocument(@@{".":  null}@@));
                )",NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        {
            auto status = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                SELECT id, json_payload FROM  `/Root/olapTable`;
                )",NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

            TString result = FormatResultSetYson(status.GetResultSet(0));

            CompareYson(result, R"([[1u;["{\"\":\"NULL\"}"]]])");

        }
    }

    Y_UNIT_TEST(JsonDocumentValueWithSkipIndexesWithSubcolumns) {
        auto settings = TKikimrSettings();
        settings.AppConfig.MutableColumnShardConfig()->SetAlterObjectEnabled(true);
        settings.AppConfig.MutableColumnShardConfig()->SetPeriodicWakeupActivationPeriodMs(1000);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto result = kikimr.GetQueryClient()
                                .ExecuteQuery(R"(
                CREATE TABLE `/Root/olapTable` (
                    id Uint64 NOT NULL,
                    json_payload JsonDocument,
                    PRIMARY KEY (id)
                )
                WITH (
                    STORE = COLUMN,
                    PARTITION_COUNT = 1
                );

                ALTER OBJECT `/Root/olapTable`
                (TYPE TABLE)
                SET (ACTION=ALTER_COLUMN, NAME=json_payload,
                `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`true`,
                `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`false`, `COLUMNS_LIMIT`=`1024`,
                `SPARSED_DETECTOR_KFF`=`20`, `MEM_LIMIT_CHUNK`=`52428800`, `OTHERS_ALLOWED_FRACTION`=`0`);

                ALTER OBJECT `/Root/olapTable`
                (TYPE TABLE)
                SET (ACTION=UPSERT_INDEX, NAME=json_payload_category_bloom_index, TYPE=CATEGORY_BLOOM_FILTER,
                    FEATURES=`{"column_name" : "json_payload", "false_positive_probability" : 0.05,
                                "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "BITSET"}`);

                )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        auto query = Q1_(R"(
            DECLARE $id as Uint64;
            INSERT INTO `/Root/olapTable` (id, json_payload)
            VALUES ($id, JsonDocument(@@{"some": "value", "other": "value2"}@@));
        )");

        for (ui64 i = 0; i < 100; ++i) {
            auto param = client.GetParamsBuilder().AddParam("$id").Uint64(i).Build().Build();

            auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), param).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        WaitCompaction(kikimr);

        {
            auto status = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/olapTable` WHERE JSON_VALUE(json_payload, "$.\"some\"") = "value";
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

            TString result = FormatResultSetYson(status.GetResultSet(0));

            CompareYson(result, R"([[100u]])");
        }
    }

    Y_UNIT_TEST(JsonDocumentValueWithSkipIndexesAndNoSubcolumns) {
        auto settings = TKikimrSettings();
        settings.AppConfig.MutableColumnShardConfig()->SetAlterObjectEnabled(true);
        settings.AppConfig.MutableColumnShardConfig()->SetPeriodicWakeupActivationPeriodMs(1000);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto result = kikimr.GetQueryClient()
                                .ExecuteQuery(R"(
                CREATE TABLE `/Root/olapTable` (
                    id Uint64 NOT NULL,
                    json_payload JsonDocument,
                    PRIMARY KEY (id)
                )
                WITH (
                    STORE = COLUMN,
                    PARTITION_COUNT = 1
                );

                ALTER OBJECT `/Root/olapTable`
                (TYPE TABLE)
                SET (ACTION=UPSERT_INDEX, NAME=json_payload_category_bloom_index, TYPE=CATEGORY_BLOOM_FILTER,
                    FEATURES=`{"column_name" : "json_payload", "false_positive_probability" : 0.05,
                                "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "BITSET"}`);

                )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        auto query = Q1_(R"(
            DECLARE $id as Uint64;
            INSERT INTO `/Root/olapTable` (id, json_payload)
            VALUES ($id, JsonDocument(@@{"some": "value", "other": "value2"}@@));
        )");

        for (ui64 i = 0; i < 100; ++i) {
            auto param = client.GetParamsBuilder().AddParam("$id").Uint64(i).Build().Build();

            auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), param).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        WaitCompaction(kikimr);

        {
            auto status = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/olapTable` WHERE JSON_VALUE(json_payload, "$.\"some\"") = "value";
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

            TString result = FormatResultSetYson(status.GetResultSet(0));

            CompareYson(result, R"([[100u]])");
        }
    }
}
}
