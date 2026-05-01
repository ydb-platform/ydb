#include <ydb/core/kqp/ut/olap/combinatory/variator.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/typed_local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/limiter/grouped_memory/service/process.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <ydb/library/signals/object_counter.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/strip.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapDictionary) {

    TKikimrSettings GetDictionarySettings() {
        auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableCsDictionaryEncoding(true);
        return settings;
    }

    TString scriptDifferentPages = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk_int Uint64 NOT NULL,
            data Utf8 ENCODING(DICT),
            PRIMARY KEY (pk_int)
        )
        PARTITION BY HASH(pk_int)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        %s
        ------
        READ: SELECT COUNT(*) AS GROUPS_COUNT, SUM(COUNT) AS RECORDS_COUNT FROM (SELECT COUNT(*) as COUNT, data FROM `/Root/ColumnTable` GROUP BY data ORDER BY data);
        EXPECTED: [[10u;[800000u]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable/.sys/primary_index_stats`;
        ------
        ONE_COMPACTION
        ------
        READ: SELECT COUNT(*) AS GROUPS_COUNT, SUM(COUNT) AS RECORDS_COUNT FROM (SELECT COUNT(*) as COUNT, data FROM `/Root/ColumnTable` GROUP BY data ORDER BY data);
        EXPECTED: [[10u;[800000u]]]
        ------
        READ: SELECT SUM(Rows) AS ROWS, EntityName, ChunkIdx FROM `/Root/ColumnTable/.sys/primary_index_stats` WHERE Activity == 1 GROUP BY EntityName, ChunkIdx ORDER BY EntityName, ChunkIdx;
        EXPECTED: [[[133333u];["_yql_plan_step"];[0u]];[[133333u];["_yql_plan_step"];[1u]];[[133333u];["_yql_plan_step"];[2u]];[[133333u];["_yql_plan_step"];[3u]];[[133334u];["_yql_plan_step"];[4u]];[[133334u];["_yql_plan_step"];[5u]];[[133333u];["_yql_tx_id"];[0u]];[[133333u];["_yql_tx_id"];[1u]];[[133333u];["_yql_tx_id"];[2u]];[[133333u];["_yql_tx_id"];[3u]];[[133334u];["_yql_tx_id"];[4u]];[[133334u];["_yql_tx_id"];[5u]];[[800000u];["data"];[0u]];[[133333u];["pk_int"];[0u]];[[133333u];["pk_int"];[1u]];[[133333u];["pk_int"];[2u]];[[133333u];["pk_int"];[3u]];[[133334u];["pk_int"];[4u]];[[133334u];["pk_int"];[5u]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(DifferentPages, scriptDifferentPages) {
        NArrow::NConstruction::TStringPoolFiller sPool(10, 512);
        std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
        builders.emplace_back(
            NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable(
                "pk_int", 0));
        builders.emplace_back(
            std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TStringPoolFiller>>("data", sPool));
        NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
        auto arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(batchBuilder.BuildBatch(800000)));
        TString injection = Sprintf(R"(
            BULK_UPSERT:
                /Root/ColumnTable
                %s
                PARTS_COUNT:16
        )",
            arrowString.data());
        Variator::ToExecutor(Variator::SingleScript(Sprintf(__SCRIPT_CONTENT.c_str(), injection.c_str()))).Execute(GetDictionarySettings());
    }

    TString scriptEmptyStringVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL ENCODING(DICT),
            Col2 Utf8 ENCODING(DICT),
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, PARTITION_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (1u)
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;#]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(EmptyStringVariants, scriptEmptyStringVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute(GetDictionarySettings());
    }

    TString scriptSimpleStringVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL ENCODING(DICT),
            Col2 Utf8 ENCODING(DICT),
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, PARTITION_COUNT = $$1|2$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, 'abc')
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (2u)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (3u, 'abc')
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (4u, 'ab')
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["abc"]];[2u;#];[3u;["abc"]];[4u;["ab"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["abc"]];[2u;#];[3u;["abc"]];[4u;["ab"]]]
        ------
        ONE_COMPACTION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["abc"]];[2u;#];[3u;["abc"]];[4u;["ab"]]]
    )";
    Y_UNIT_TEST_STRING_VARIATOR(SimpleStringVariants, scriptSimpleStringVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute(GetDictionarySettings());
    }

    Y_UNIT_TEST(CreateWithEncodingDictionary) {
        auto settings = TKikimrSettings()
            .SetEnableCsDictionaryEncoding(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema()
            .SetName("key")
            .SetType(NScheme::NTypeIds::Uint64)
            .SetNullable(false)
            .SetDictionaryEncoding(true)
        };

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/EncodingDictionaryTable").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTableQuery(standaloneTable);

        testHelper.ReadDataExecQuery(R"(
            SELECT COUNT(*) > 0 FROM `/Root/EncodingDictionaryTable/.sys/primary_index_schema_stats`
                WHERE JSON_VALUE(CAST(SchemaDetails as JsonDocument), "$.index_info")
                        ILIKE "%key:serializer={class_name=ARROW_SERIALIZER;details={}};loader=accessor_constructor:DICTIONARY%";
            )", "[[%true]]");
    }

    Y_UNIT_TEST(CreateWithoutEncodingDictionary) {
        auto settings = TKikimrSettings()
            .SetEnableCsDictionaryEncoding(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema()
            .SetName("key")
            .SetType(NScheme::NTypeIds::Uint64)
            .SetNullable(false)
            .SetDictionaryEncoding(false)
        };

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/EncodingDictionaryTable").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTableQuery(standaloneTable);

        testHelper.ReadDataExecQuery(R"(
            SELECT COUNT(*) == 0 FROM `/Root/EncodingDictionaryTable/.sys/primary_index_schema_stats`
                WHERE JSON_VALUE(CAST(SchemaDetails as JsonDocument), "$.index_info")
                        ILIKE "%DICTIONARY%";
            )", "[[%true]]");
    }

    Y_UNIT_TEST(AlterAddEncodingDictionary) {
        auto settings = TKikimrSettings()
            .SetEnableCsDictionaryEncoding(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema()
            .SetName("key")
            .SetType(NScheme::NTypeIds::Uint64)
            .SetNullable(false)
            .SetDictionaryEncoding(false)
        };

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/EncodingDictionaryTable").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTableQuery(standaloneTable);
        testHelper.ExecuteQuery("ALTER TABLE `/Root/EncodingDictionaryTable` ALTER COLUMN `key` SET ENCODING(DICT);");

        testHelper.ReadDataExecQuery(R"(
            SELECT COUNT(*) > 0 FROM `/Root/EncodingDictionaryTable/.sys/primary_index_schema_stats`
                WHERE JSON_VALUE(CAST(SchemaDetails as JsonDocument), "$.index_info")
                        ILIKE "%key:serializer={class_name=ARROW_SERIALIZER;details={}};loader=accessor_constructor:DICTIONARY%";
            )", "[[%true]]");
    }

    Y_UNIT_TEST(AlterSetEncodingOffAfterDictionary) {
        auto settings = TKikimrSettings()
            .SetEnableCsDictionaryEncoding(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema()
            .SetName("key")
            .SetType(NScheme::NTypeIds::Uint64)
            .SetNullable(false)
            .SetDictionaryEncoding(true)
        };

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/EncodingDictionaryTable").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTableQuery(standaloneTable);
        testHelper.ExecuteQuery("ALTER TABLE `/Root/EncodingDictionaryTable` ALTER COLUMN `key` SET ENCODING(OFF);");

        testHelper.ReadDataExecQuery(R"(
            $V1 = SELECT max(SchemaVersion) FROM `/Root/EncodingDictionaryTable/.sys/primary_index_schema_stats`
                WHERE JSON_VALUE(CAST(SchemaDetails as JsonDocument), "$.index_info")
                        ILIKE "%key:serializer={class_name=ARROW_SERIALIZER;details={}};loader=accessor_constructor:DICTIONARY%";

            $V2 = SELECT max(SchemaVersion) FROM `/Root/EncodingDictionaryTable/.sys/primary_index_schema_stats`
                WHERE JSON_VALUE(CAST(SchemaDetails as JsonDocument), "$.index_info")
                        ILIKE "%key:serializer={class_name=ARROW_SERIALIZER;details={}};loader=accessor_constructor:PLAIN%";

            SELECT $V1 < $V2;
            )", "[[[%true]]]");
    }

    Y_UNIT_TEST(AlterDropEncodingDictionary) {
        auto settings = TKikimrSettings()
            .SetEnableCsDictionaryEncoding(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema()
            .SetName("key")
            .SetType(NScheme::NTypeIds::Uint64)
            .SetNullable(false)
            .SetDictionaryEncoding(true)
        };

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/EncodingDictionaryTable").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTableQuery(standaloneTable);
        testHelper.ExecuteQuery("ALTER TABLE `/Root/EncodingDictionaryTable` ALTER COLUMN `key` SET ENCODING();");

        testHelper.ReadDataExecQuery(R"(
            $V1 = SELECT max(SchemaVersion) FROM `/Root/EncodingDictionaryTable/.sys/primary_index_schema_stats`
                WHERE JSON_VALUE(CAST(SchemaDetails as JsonDocument), "$.index_info")
                        ILIKE "%key:serializer={class_name=ARROW_SERIALIZER;details={}};loader=accessor_constructor:DICTIONARY%";

            $V2 = SELECT max(SchemaVersion) FROM `/Root/EncodingDictionaryTable/.sys/primary_index_schema_stats`
                WHERE JSON_VALUE(CAST(SchemaDetails as JsonDocument), "$.index_info")
                        ILIKE "%key:serializer={class_name=ARROW_SERIALIZER;details={}};loader=accessor_constructor:PLAIN%";

            SELECT $V1 < $V2;
            )", "[[[%true]]]");
    }

    TString scriptGroupBySomeDictionary = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            otherPk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            other Uint64,
            PRIMARY KEY (pk, otherPk)
        )
        PARTITION BY HASH(pk, otherPk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, otherPk, message, other) VALUES
            (1u, 1u, 'a', 4u),
            (2u, 2u, 'b', 3u),
            (3u, 3u, 'a', 2u),
            (4u, 4u, 'c', 1u);
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"];["a"]];[["b"];["b"]];[["c"];["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 2
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 0 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 3
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 0 AND pk < 5 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 4
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk >= 1 AND pk <= 5 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 5
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk < 5 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 6
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"]];[["b"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(pk), pk FROM `/Root/ColumnTable` GROUP BY pk ORDER BY pk;
        EXPECTED: [[1u;1u];[2u;2u];[3u;3u];[4u;4u]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE other > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE otherPk > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE otherPk > 0 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk <= 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message, MIN(pk) FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"];["a"];1u];[["b"];["b"];2u];[["c"];["c"];4u]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message, MIN(message) FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"];["a"];["a"]];[["b"];["b"];["b"]];[["c"];["c"];["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 6
    )";
    Y_UNIT_TEST(GroupBySomeDictionary) {
        Variator::ToExecutor(Variator::SingleScript(scriptGroupBySomeDictionary)).Execute(GetDictionarySettings());
    }

    TString scriptGroupBySomeDictionaryWithCompaction = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            otherPk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            other Uint64,
            PRIMARY KEY (pk, otherPk)
        )
        PARTITION BY HASH(pk, otherPk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, otherPk, message, other) VALUES
            (1u, 1u, 'a', 4u),
            (2u, 2u, 'b', 3u),
            (3u, 3u, 'a', 2u),
            (4u, 4u, 'c', 1u);
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, otherPk, message, other) VALUES
            (1u, 1u, 'a', 4u),
            (2u, 2u, 'b', 3u),
            (3u, 3u, 'a', 2u),
            (4u, 4u, 'c', 1u);
        ------
        ONE_COMPACTION
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"];["a"]];[["b"];["b"]];[["c"];["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 2
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 0 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 3
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 0 AND pk < 5 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 4
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk >= 1 AND pk <= 5 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 5
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk < 5 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 6
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"]];[["b"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(pk), pk FROM `/Root/ColumnTable` GROUP BY pk ORDER BY pk;
        EXPECTED: [[1u;1u];[2u;2u];[3u;3u];[4u;4u]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE other > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE otherPk > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE otherPk > 0 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk <= 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message, MIN(pk) FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"];["a"];1u];[["b"];["b"];2u];[["c"];["c"];4u]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message, MIN(message) FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[["a"];["a"];["a"]];[["b"];["b"];["b"]];[["c"];["c"];["c"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), COUNT(*) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[["a"];2u];[["b"];1u];[["c"];1u]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 6
    )";
    Y_UNIT_TEST(GroupBySomeDictionaryWithCompaction) {
        Variator::ToExecutor(Variator::SingleScript(scriptGroupBySomeDictionaryWithCompaction)).Execute(GetDictionarySettings());
    }

    // TODO: fix bug that return "" here (and 2 more tests after):
    // READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
    // EXPECTED: [[#;[""]];[["a"];["a"]];[["b"];["b"]]]
    // Should be:
    // EXPECTED: [[#;#];[["a"];["a"]];[["b"];["b"]]]
    // The problem is with PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true", not with the dictionary itself
    TString scriptGroupBySomeDictionaryWithNulls = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            otherPk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            other Uint64,
            PRIMARY KEY (pk, otherPk)
        )
        PARTITION BY HASH(pk, otherPk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, otherPk, message, other) VALUES
            (1u, 1u, 'a', 4u),
            (2u, 2u, 'b', 3u),
            (3u, 3u, 'a', 2u),
            (4u, 4u, NULL, 1u);
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[#;[""]];[["a"];["a"]];[["b"];["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 2
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 0 GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 3
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 0 AND pk < 5 GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 4
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk >= 1 AND pk <= 5 GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 5
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk < 5 GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 6
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[#];[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(pk), pk FROM `/Root/ColumnTable` GROUP BY pk ORDER BY pk;
        EXPECTED: [[1u;1u];[2u;2u];[3u;3u];[4u;4u]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE other > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE otherPk > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE otherPk > 0 GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk > 2 GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` WHERE pk <= 2 GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message, MIN(pk) FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[#;[""];4u];[["a"];["a"];1u];[["b"];["b"];2u]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message, MIN(message) FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[#;[""];#];[["a"];["a"];["a"]];[["b"];["b"];["b"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), COUNT(*) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[#;1u];[["a"];2u];[["b"];1u]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 6
    )";
    Y_UNIT_TEST(GroupBySomeDictionaryWithNulls) {
        Variator::ToExecutor(Variator::SingleScript(scriptGroupBySomeDictionaryWithNulls)).Execute(GetDictionarySettings());
    }

    TString scriptGroupBySomeDoubleNullInsert = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, message) VALUES
            (1u, NULL);
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, message) VALUES
            (2u, NULL);
        ------
        ONE_COMPACTION
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[1u;#];[2u;#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message), message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[#;[""]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 2
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT message FROM `/Root/ColumnTable` GROUP BY message ORDER BY message;
        EXPECTED: [[#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 2
    )";
    Y_UNIT_TEST(GroupBySomeDictionaryDoubleNullInsert) {
        Variator::ToExecutor(Variator::SingleScript(scriptGroupBySomeDoubleNullInsert)).Execute(GetDictionarySettings());
    }

    TString scriptAddDictColumnThenUpsertSamePk = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk) VALUES (100u);
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN extra Utf8 ENCODING(DICT)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, extra) VALUES (100u, 'post_add');
        ------
        ONE_COMPACTION
        ------
        READ: SELECT pk, extra FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[100u;["post_add"]]]
    )";
    Y_UNIT_TEST(AddDictionaryColumnThenUpsertSamePkAndCompact) {
        Variator::ToExecutor(Variator::SingleScript(scriptAddDictColumnThenUpsertSamePk)).Execute(GetDictionarySettings());
    }

    TString scriptAddDictColumnThenUpsertOtherPk = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk) VALUES (200u);
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN extra Utf8 ENCODING(DICT)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, extra) VALUES (201u, 'only_new_pk');
        ------
        ONE_COMPACTION
        ------
        READ: SELECT pk, extra FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[200u;#];[201u;["only_new_pk"]]]
    )";
    Y_UNIT_TEST(AddDictionaryColumnThenUpsertOtherPkAndCompact) {
        Variator::ToExecutor(Variator::SingleScript(scriptAddDictColumnThenUpsertOtherPk)).Execute(GetDictionarySettings());
    }

    TString scriptDeleteOneDictionaryValue = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, message) VALUES
            (1u, 'a'),
            (2u, 'b'),
            (3u, 'a'),
            (4u, NULL);
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        DATA:
        DELETE FROM `/Root/ColumnTable` WHERE pk = 1;
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[2u;["b"]];[3u;["a"]];[4u;#]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        ONE_COMPACTION
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[2u;["b"]];[3u;["a"]];[4u;#]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
    )";
    Y_UNIT_TEST(DeleteDictionaryOneDictionaryValue) {
        Variator::ToExecutor(Variator::SingleScript(scriptDeleteOneDictionaryValue)).Execute(GetDictionarySettings());
    }

    TString scriptDeleteOneFullDictionaryValue = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, message) VALUES
            (1u, 'a'),
            (2u, 'b'),
            (3u, 'a'),
            (4u, NULL);
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        DATA:
        DELETE FROM `/Root/ColumnTable` WHERE pk = 2;
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[1u;["a"]];[3u;["a"]];[4u;#]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        ONE_COMPACTION
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[1u;["a"]];[3u;["a"]];[4u;#]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[#];[["a"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
    )";
    Y_UNIT_TEST(DeleteDictionaryOneFullDictionaryValue) {
        Variator::ToExecutor(Variator::SingleScript(scriptDeleteOneFullDictionaryValue)).Execute(GetDictionarySettings());
    }

    TString scriptDeleteOneNullDictionaryValue = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, message) VALUES
            (1u, 'a'),
            (2u, 'b'),
            (3u, 'a'),
            (4u, NULL);
        ------
        DATA:
        DELETE FROM `/Root/ColumnTable` WHERE pk = 4;
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[1u;["a"]];[2u;["b"]];[3u;["a"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        ONE_COMPACTION
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[1u;["a"]];[2u;["b"]];[3u;["a"]]]
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: [[["a"]];[["b"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
    )";
    Y_UNIT_TEST(DeleteDictionaryOneNullDictionaryValue) {
        Variator::ToExecutor(Variator::SingleScript(scriptDeleteOneNullDictionaryValue)).Execute(GetDictionarySettings());
    }

    TString scriptDeleteAllDictionaryValues = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            message Utf8 ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, message) VALUES
            (1u, 'a'),
            (2u, 'b'),
            (3u, 'a'),
            (4u, NULL);
        ------
        DATA:
        DELETE FROM `/Root/ColumnTable`;
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: []
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: []
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        ONE_COMPACTION
        ------
        READ: SELECT pk, message FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: []
        ------
        READ: PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true"; SELECT SOME(message) FROM `/Root/ColumnTable` GROUP BY message;
        EXPECTED_UNORDERED: []
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
    )";
    Y_UNIT_TEST(DeleteDictionaryAllDictionaryValues) {
        Variator::ToExecutor(Variator::SingleScript(scriptDeleteAllDictionaryValues)).Execute(GetDictionarySettings());
    }

    TString scriptDictCompactionAndActualization = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            field Utf8,
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`{"levels":[{"class_name":"Zero","portions_count_limit":1048576,"expected_blobs_size":1048576,"portions_count_available":1,"portions_live_duration":"1s"},{"class_name":"OneLayer","expected_portion_size":2097152,"size_limit_guarantee":134217728}]}`)
        ------
        %s
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[["a"];1u];[["b"];2u];[["a"];3u]]
    )";
    Y_UNIT_TEST(DictCompactionAndActualization) {
        constexpr int cycles = 10;
        constexpr int replacesPerBlock = 12;
        const char* rows[] = {"(1u, 'a')", "(2u, 'b')", "(3u, 'a')"};
        TString dataBlock;
        for (int r = 0; r < replacesPerBlock; ++r) {
            dataBlock += "DATA:\nREPLACE INTO `/Root/ColumnTable` (pk, field) VALUES ";
            dataBlock += rows[r % 3];
            dataBlock += ";\n------\n";
        }
        const char* readCheck = "READ: SELECT * FROM `/Root/ColumnTable` ORDER BY pk;\nEXPECTED: [[[\"a\"];1u];[[\"b\"];2u];[[\"a\"];3u]]\n------\n";
        TString cycleBlocks;
        for (int i = 0; i < cycles; ++i) {
            cycleBlocks += dataBlock;
            cycleBlocks += readCheck;
            cycleBlocks += "ONE_COMPACTION\n------\n";
            cycleBlocks += readCheck;
            cycleBlocks += dataBlock;
            cycleBlocks += readCheck;
            if (i % 2 == 0) {
                cycleBlocks += "SCHEMA:\nALTER TABLE `/Root/ColumnTable` ALTER COLUMN `field` SET ENCODING(DICT);\n------\n";
            } else {
                cycleBlocks += "SCHEMA:\nALTER TABLE `/Root/ColumnTable` ALTER COLUMN `field` SET ENCODING(OFF);\n------\n";
            }
            cycleBlocks += readCheck;
            cycleBlocks += dataBlock;
            cycleBlocks += readCheck;
            cycleBlocks += "SCHEMA:\nALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`)\n------\n";
            cycleBlocks += readCheck;
            cycleBlocks += dataBlock;
            cycleBlocks += readCheck;
            cycleBlocks += "ONE_ACTUALIZATION\n------\n";
            cycleBlocks += readCheck;
        }
        Variator::ToExecutor(Variator::SingleScript(Sprintf(scriptDictCompactionAndActualization.c_str(), cycleBlocks.c_str()))).Execute(GetDictionarySettings());
    }

    // Multiple inserts and compactions: 150+150, compact; 300+150, compact; 300+300, compact. Verify correct data after each step (incl. uint16 dictionary path).
    Y_UNIT_TEST(DictMultipleInsertsAndCompactions) {
        const TString scriptPrefix = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            field Utf8 ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`{"levels":[{"class_name":"Zero","portions_count_limit":1048576,"expected_blobs_size":1048576,"portions_count_available":1,"portions_live_duration":"1s"},{"class_name":"OneLayer","expected_portion_size":2097152,"size_limit_guarantee":134217728}]}`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        )";
        const std::vector<std::pair<ui32, ui32>> steps = {
            {150, 0},    // 150 rows, pk start 0
            {150, 150},  // 150 rows, pk start 150
            {0, 0},      // compaction (rows=0 means compaction only)
            {300, 300},  // 300 rows, pk start 300
            {150, 600},  // 150 rows, pk start 600
            {0, 0},      // compaction
            {300, 750},  // 300 rows, pk start 750
            {300, 1050}, // 300 rows, pk start 1050
            {0, 0},      // compaction
        };
        const std::vector<ui32> expectedCounts = {150, 300, 300, 600, 750, 750, 1050, 1350, 1350};
        NArrow::NConstruction::TStringPoolFiller sPool(300, 52);
        TString injection;
        ui32 expectedIdx = 0;
        const TString delimiter = "------\n";
        for (size_t i = 0; i < steps.size(); ++i) {
            const bool isLast = (i == steps.size() - 1);
            const ui32 numRows = steps[i].first;
            const ui32 pkStart = steps[i].second;
            if (numRows == 0) {
                injection += "ONE_COMPACTION\n" + delimiter;
                injection += "READ: SELECT COUNT(*) AS c FROM `/Root/ColumnTable`;\n";
                injection += "EXPECTED: [[" + ToString(expectedCounts[expectedIdx]) + "u]]";
                injection += isLast ? "\n" : (delimiter + "\n");
                ++expectedIdx;
                continue;
            }
            std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
            builders.emplace_back(
                NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable(
                    "pk", NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>(pkStart)));
            builders.emplace_back(
                std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TStringPoolFiller>>("field", sPool));
            NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
            TString arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(batchBuilder.BuildBatch(numRows)));
            injection += "BULK_UPSERT:\n    /Root/ColumnTable\n    " + arrowString + "\nPARTS_COUNT:1\n" + delimiter;
            injection += "READ: SELECT COUNT(*) AS c FROM `/Root/ColumnTable`;\n";
            injection += "EXPECTED: [[" + ToString(expectedCounts[expectedIdx]) + "u]]";
            injection += isLast ? "\n" : (delimiter + "\n");
            ++expectedIdx;
        }
        Variator::ToExecutor(Variator::SingleScript(scriptPrefix + injection)).Execute(GetDictionarySettings());
    }

    // Corner cases: 254+254, 254+255, 255+255 (no nulls) then same pattern with nulls. Compaction merges same/different portion sizes; every step must succeed.
    Y_UNIT_TEST(DictCornerCaseVariantsAndNulls) {
        const TString scriptPrefix = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            field Utf8 ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`{"levels":[{"class_name":"Zero","portions_count_limit":1048576,"expected_blobs_size":1048576,"portions_count_available":1,"portions_live_duration":"1s"},{"class_name":"OneLayer","expected_portion_size":2097152,"size_limit_guarantee":134217728}]}`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        )";
        NArrow::NConstruction::TStringPoolFiller sPool(260, 52);
        const TString delimiter = "------\n";
        TString injection;
        // addNull: false = numRows rows (all non-null); true = numRows non-null + 1 null (numRows+1 rows total), null at last row.
        // Returns pkStart + totalRows for the next call.
        auto addBulk = [&](ui32 numRows, ui32 pkStart, bool addNull = false) -> ui32 {
            const ui32 totalRows = addNull ? numRows + 1 : numRows;
            auto pkBuilder = NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable(
                "pk", NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>(pkStart));
            std::shared_ptr<arrow::Array> pkArray = pkBuilder->BuildArray(totalRows);
            auto fieldBuilder = NArrow::MakeBuilder(std::make_shared<arrow::Field>("field", arrow::utf8(), true), totalRows, totalRows * 52);
            auto* sb = static_cast<arrow::StringBuilder*>(fieldBuilder.get());
            for (ui32 i = 0; i < totalRows; ++i) {
                if (addNull && i == numRows) {
                    Y_ABORT_UNLESS(sb->AppendNull().ok());
                } else {
                    Y_ABORT_UNLESS(sb->Append(sPool.GetValue(i)).ok());
                }
            }
            std::shared_ptr<arrow::Array> fieldArray = NArrow::FinishBuilder(std::move(fieldBuilder));
            auto schema = arrow::schema({arrow::field("pk", arrow::uint64(), false), arrow::field("field", arrow::utf8(), true)});
            auto batch = arrow::RecordBatch::Make(schema, totalRows, {pkArray, fieldArray});
            TString arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(batch));
            injection += "BULK_UPSERT:\n    /Root/ColumnTable\n    " + arrowString + "\nPARTS_COUNT:1\n" + delimiter;
            return pkStart + totalRows;
        };
        auto addReadAndCompact = [&](ui32 expectedCount) {
            injection += "READ: SELECT COUNT(*) AS c FROM `/Root/ColumnTable`;\n";
            injection += "EXPECTED: [[" + ToString(expectedCount) + "u]]\n";
            injection += delimiter + "ONE_COMPACTION\n" + delimiter;
            injection += "READ: SELECT COUNT(*) AS c FROM `/Root/ColumnTable`;\n";
            injection += "EXPECTED: [[" + ToString(expectedCount) + "u]]\n";
            injection += delimiter;
        };
        const std::vector<std::pair<ui32, ui32>> pairs = {
            {253, 253}, {253, 254}, {254, 254}, {254, 255}, {255, 255}, {255, 256}, {256, 256}, {256, 257}, {257, 257}
        };
        ui32 pk = 0;
        for (const bool withNull : {false, true}) {
            for (size_t i = 0; i < pairs.size(); ++i) {
                const auto [a, b] = pairs[i];
                pk = addBulk(a, pk, withNull);
                pk = addBulk(b, pk, withNull);
                addReadAndCompact(pk);
            }
        }
        injection += "STOP_COMPACTION\n";
        Variator::ToExecutor(Variator::SingleScript(scriptPrefix + injection)).Execute(GetDictionarySettings());
    }

    // ChunkDetails in .sys/primary_index_stats for dictionary column: check deterministic output for 1 row.
    TString scriptChunkDetailsDictionary = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            field Utf8 ENCODING(DICT) COMPRESSION(algorithm=off),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`)
        ------
        ONE_ACTUALIZATION
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, field) VALUES (1u, 'x');
        ------
        READ: SELECT ChunkDetails FROM `/Root/ColumnTable/.sys/primary_index_stats` WHERE EntityName = 'field' ORDER BY ChunkIdx;
        EXPECTED: [[["{\"positions_blob_size\":152,\"dictionary_blob_size\":176}"]]]
    )";
    Y_UNIT_TEST(ChunkDetailsDictionary) {
        Variator::ToExecutor(Variator::SingleScript(scriptChunkDetailsDictionary)).Execute(GetDictionarySettings());
    }

    // One table with a column per supported (comparable) type; set DICTIONARY on each, insert one row, read back.
    // Supported for dictionary: Bool, Int*, Uint*, Float, Double, String/Utf8, Date, Datetime, Timestamp. (Bytes omitted: X'...' literal not supported in script.)
    TString scriptDictionarySupportedTypes = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            c_bool Bool ENCODING(DICT),
            c_int8 Int8 ENCODING(DICT),
            c_int16 Int16 ENCODING(DICT),
            c_int32 Int32 ENCODING(DICT),
            c_int64 Int64 ENCODING(DICT),
            c_uint8 Uint8 ENCODING(DICT),
            c_uint16 Uint16 ENCODING(DICT),
            c_uint32 Uint32 ENCODING(DICT),
            c_uint64 Uint64 ENCODING(DICT),
            c_float Float ENCODING(DICT),
            c_double Double ENCODING(DICT),
            c_utf8 Utf8 ENCODING(DICT),
            c_string String ENCODING(DICT),
            c_date Date ENCODING(DICT),
            c_datetime Datetime ENCODING(DICT),
            c_timestamp Timestamp ENCODING(DICT),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_bool` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_int8` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_int16` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_int32` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_int64` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_uint8` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_uint16` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_uint32` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_uint64` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_float` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_double` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_utf8` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_string` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_date` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_datetime` SET ENCODING(DICT)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ALTER COLUMN `c_timestamp` SET ENCODING(DICT)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, c_bool, c_int8, c_int16, c_int32, c_int64, c_uint8, c_uint16, c_uint32, c_uint64, c_float, c_double, c_utf8, c_string, c_date, c_datetime, c_timestamp) VALUES
            (1u, true, CAST(1 AS Int8), CAST(2 AS Int16), 3, CAST(4 AS Int64), CAST(5 AS Uint8), CAST(6 AS Uint16), CAST(7 AS Uint32), CAST(8 AS Uint64), CAST(1.0 AS Float), 2.0, 'u', 's', Date("2020-01-01"), Datetime("2020-01-01T00:00:00Z"), Timestamp("2020-01-01T00:00:00Z"));
        ------
        READ: SELECT pk, c_bool, c_int8, c_int16, c_int32, c_int64, c_uint8, c_uint16, c_uint32, c_uint64, c_float, c_double, c_utf8, c_string, c_date, c_datetime, c_timestamp FROM `/Root/ColumnTable` ORDER BY pk;
        EXPECTED: [[1u;[%true];[1];[2];[3];[4];[5u];[6u];[7u];[8u];[1.];[2.];["u"];["s"];[18262u];[1577836800u];[1577836800000000u]]]
    )";
    Y_UNIT_TEST(DictionarySupportedTypes) {
        Variator::ToExecutor(Variator::SingleScript(scriptDictionarySupportedTypes)).Execute(GetDictionarySettings());
    }

    // Dictionary cannot be applied to non-comparable types (Json, JsonDocument, Yson). ALTER must fail for each.
    Y_UNIT_TEST(DictionaryUnsupportedTypes) {
        auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        guard->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        TString createTableUnsupportedTable = R"(
            CREATE TABLE `/Root/UnsupportedTypesTable` (
                pk Uint64 NOT NULL,
                jcol Json ENCODING(DICT),
                jdcol JsonDocument ENCODING(DICT),
                ycol Yson ENCODING(DICT),
                PRIMARY KEY (pk)
            )
            PARTITION BY HASH(pk)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        )";
        auto createUnsupportedResult = session.ExecuteSchemeQuery(createTableUnsupportedTable).GetValueSync();
        UNIT_ASSERT_C(!createUnsupportedResult.IsSuccess(), TString("CREATE TABLE DICTIONARY with unsupported types must fail"));

        TString createTable = R"(
            CREATE TABLE `/Root/UnsupportedTypesTable` (
                pk Uint64 NOT NULL,
                jcol Json,
                jdcol JsonDocument,
                ycol Yson,
                PRIMARY KEY (pk)
            )
            PARTITION BY HASH(pk)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        )";
        auto createResult = session.ExecuteSchemeQuery(createTable).GetValueSync();
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());
        for (const TString& col : {"jcol", "jdcol", "ycol"}) {
            TString alterQuery = "ALTER TABLE `/Root/UnsupportedTypesTable` ALTER COLUMN `" + col + "` SET ENCODING(DICT)";
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_C(!alterResult.IsSuccess(), TString("ALTER COLUMN DICTIONARY on ") + col + " must fail");
        }
    }

    void RunDictionaryUtf8BeatsCompressionOnSize(ui32 rowsCount, ui32 distinctValues, ui32 stringLen, double blobFactor) {
        UNIT_ASSERT_C(blobFactor > 0, "blobFactor must be > 0");
        UNIT_ASSERT_C(distinctValues > 0, "distinctValues must be > 0");
        UNIT_ASSERT_C(stringLen > 0, "stringLen must be > 0");
        UNIT_ASSERT_C(rowsCount > 0, "rowsCount must be > 0");

        const TString scriptTemplate = R"(
            STOP_COMPACTION
            ------
            SCHEMA:
            CREATE TABLE `/Root/CompressionOnlyTable` (
                pk Uint64 NOT NULL,
                data Utf8 COMPRESSION(algorithm=zstd,level=5),
                PRIMARY KEY (pk)
            )
            PARTITION BY HASH(pk)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            ------
            SCHEMA:
            CREATE TABLE `/Root/CompressionAndDictTable` (
                pk Uint64 NOT NULL,
                data Utf8 ENCODING(DICT) COMPRESSION(algorithm=zstd,level=5),
                PRIMARY KEY (pk)
            )
            PARTITION BY HASH(pk)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/CompressionOnlyTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/CompressionAndDictTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/CompressionOnlyTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/CompressionAndDictTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            BULK_UPSERT:
                /Root/CompressionOnlyTable
                %s
                PARTS_COUNT:10
            ------
            ONE_COMPACTION
            ------
            BULK_UPSERT:
                /Root/CompressionAndDictTable
                %s
                PARTS_COUNT:10
            ------
            ONE_COMPACTION
            ------
            READ: $dictBlob = SELECT CAST(SUM(BlobRangeSize) AS Double) FROM `/Root/CompressionAndDictTable/.sys/primary_index_stats` WHERE EntityName = 'data';
                  $compBlob = SELECT CAST(SUM(BlobRangeSize) AS Double) FROM `/Root/CompressionOnlyTable/.sys/primary_index_stats` WHERE EntityName = 'data';
                  $dictRaw = SELECT CAST(SUM(RawBytes) AS Double) FROM `/Root/CompressionAndDictTable/.sys/primary_index_stats` WHERE EntityName = 'data';
                  $compRaw = SELECT CAST(SUM(RawBytes) AS Double) FROM `/Root/CompressionOnlyTable/.sys/primary_index_stats` WHERE EntityName = 'data';
                  SELECT ($compBlob / $dictBlob) AS blobRate,
                         ($compRaw / $dictRaw) AS rawRate,
                         $compBlob AS compBlob,
                         $dictBlob AS dictBlob,
                         $compRaw AS compRaw,
                         $dictRaw AS dictRaw;
            ------
            READ: SELECT
                (CAST((SELECT SUM(BlobRangeSize) FROM `/Root/CompressionAndDictTable/.sys/primary_index_stats` WHERE EntityName = 'data') AS Double) * %f
                    < CAST((SELECT SUM(BlobRangeSize) FROM `/Root/CompressionOnlyTable/.sys/primary_index_stats` WHERE EntityName = 'data') AS Double));
            EXPECTED: [[[%%true]]]
            ------
            READ: SELECT
                        (SELECT SUM(RawBytes) FROM `/Root/CompressionOnlyTable/.sys/primary_index_stats` WHERE EntityName = 'data') == %u
                    AND (SELECT SUM(RawBytes) FROM `/Root/CompressionAndDictTable/.sys/primary_index_stats` WHERE EntityName = 'data') == %u;
            EXPECTED: [[[%%true]]]
            )";

        NArrow::NConstruction::TStringPoolFiller sPool(/*poolSize*/ distinctValues, /*stringLen*/ stringLen);
        std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
        builders.emplace_back(NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable("pk", 0));
        builders.emplace_back(std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TStringPoolFiller>>("data", sPool));
        NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
        const TString arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(batchBuilder.BuildBatch(rowsCount)));

        const ui64 expectedNoDictRawBytes = rowsCount * (stringLen + 4);
        const ui64 expectedDictRawBytes = distinctValues * (stringLen + 4) + rowsCount * (distinctValues > 255 ? 2 : 1);
        Variator::ToExecutor(Variator::SingleScript(Sprintf(scriptTemplate.c_str(), arrowString.c_str(), arrowString.c_str(), blobFactor, expectedNoDictRawBytes, expectedDictRawBytes)))
            .Execute(GetDictionarySettings());
    }

    Y_UNIT_TEST(Utf8DictionaryVsCompressionSize) {
        const struct {
            ui32 Rows;
            ui32 Distinct;
            ui32 StringLen;
            // - factor > 1: dictionary smaller than compression only
            // - factor < 1: dictionary larger than compression only
            double BlobFactor;
        } cases[] = {
            // Dictionary should be better than compression with small amount of distinct values
            {10000, 100, 32, 7.0},
            {10000, 100, 100, 4.0},
            {10000, 1, 32, 50.0},
            {10000, 1, 100, 50.0},
            // Dictionary should not be much worse than compression with big amount of distinct values
            {10000, 10000, 32, 0.9},
            {10000, 10000, 100, 0.9},
            // Checks for corner cases
            {1000, 254, 100, 1.0},
            {1000, 255, 100, 1.0},
            {1000, 256, 100, 1.0},
            {1000, 257, 100, 1.0},
        };

        for (const auto& c : cases) {
            RunDictionaryUtf8BeatsCompressionOnSize(c.Rows, c.Distinct, c.StringLen, c.BlobFactor);
        }
    }
}

}   // namespace NKikimr::NKqp
