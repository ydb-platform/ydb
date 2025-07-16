#include "combinatory/variator.h"
#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
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

Y_UNIT_TEST_SUITE(KqpOlapJson) {

    TString scriptEmptyVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (1u), (2u), (3u), (4u)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (11u), (12u), (13u), (14u)
        ------
        ONE_COMPACTION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;#];[2u;#];[3u;#];[4u;#];[11u;#];[12u;#];[13u;#];[14u;#]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(EmptyVariants, scriptEmptyVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptEmptyStringVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `FORCE_SIMD_PARSING`=`$$true|false$$`, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5|1$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "", "b" : "", "c" : ""}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"\",\"b\":\"\",\"c\":\"\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(EmptyStringVariants, scriptEmptyStringVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptQuotedFilterVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1", "b.c.d" : "b1", "c.d.e" : "c1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')),
                                                                (3u, JsonDocument('{"b.c.d" : "b3", "d.e.f" : "d3"}')), (4u, JsonDocument('{"b.c.d" : "b4asdsasdaa", "a.b.c" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a2" ORDER BY Col1;
        EXPECTED: [[2u;["{\"a.b.c\":\"a2\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(QuotedFilterVariants, scriptQuotedFilterVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptFilterVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                    `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a2" ORDER BY Col1;
        EXPECTED: [[2u;["{\"a\":\"a2\"}"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"e.v\".c") = "1" ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"e.v\".e.\"c.a\"") = "2" ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(FilterVariants, scriptFilterVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptRestoreFirstLevelVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`true`,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                    `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3", "e" : ["a", {"v" : ["c", 5]}]}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":\"{\\\"c\\\":1,\\\"e\\\":{\\\"c.a\\\":2}}\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\",\"e\":\"[\\\"a\\\",{\\\"v\\\":[\\\"c\\\",5]}]\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(RestoreFirstLevelVariants, scriptRestoreFirstLevelVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptRestoreFullJsonVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                    `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3", "e" : ["a", {"v" : ["c", 5]}]}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\",\"e\":[\"a\",{\"v\":[\"c\",\"5\"]}]}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(RestoreFullJsonVariants, scriptRestoreFullJsonVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptBrokenJsonWriting= R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                    `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        %s
    )";
    Y_UNIT_TEST_STRING_VARIATOR(BrokenJsonWriting, scriptBrokenJsonWriting) {
        NColumnShard::TTableUpdatesBuilder updates(NArrow::MakeArrowSchema(
            { { "Col1", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64) }, { "Col2", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } }));
        updates.AddRow().Add<int64_t>(1).Add("{\"a\" : \"c}");
        auto arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(updates.BuildArrow()));
        TString injection = Sprintf(R"(
            BULK_UPSERT:
                /Root/ColumnTable
                %s
                EXPECT_STATUS:BAD_REQUEST
        )",
            arrowString.data());
        Variator::ToExecutor(Variator::SingleScript(Sprintf(__SCRIPT_CONTENT.c_str(), injection.c_str()))).Execute();
    }

    TString scriptRestoreJsonArrayVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                    `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('["a", {"v" : 4}, 1,2,3,4,5,6,7,8,9,10,11,12]'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["[\"a\",{\"v\":\"4\"},\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9\",\"10\",\"11\",\"12\"]"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(RestoreJsonArrayVariants, scriptRestoreJsonArrayVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptDoubleFilterVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.b") = "b3" AND JSON_VALUE(Col2, "$.d") = "d3" ORDER BY Col1;
        EXPECTED: [[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(DoubleFilterVariants, scriptDoubleFilterVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptOrFilterVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE (JSON_VALUE(Col2, "$.b") = "b3" AND JSON_VALUE(Col2, "$.d") = "d3") OR (JSON_VALUE(Col2, "$.b") = "b1" AND JSON_VALUE(Col2, "$.c") = "c1") ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(OrFilterVariants, scriptOrFilterVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptDoubleFilterReduceScopeVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            Col3 UTF8,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, JsonDocument('{"a" : "value_a", "b" : "b1", "c" : "c1"}'), "value1"), (2u, JsonDocument('{"a" : "value_a"}'), "value1"),
                                                                (3u, JsonDocument('{"a" : "value_a", "b" : "value_b"}'), "value2"), (4u, JsonDocument('{"b" : "value_b", "a" : "a4"}'), "value4")
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "value_a" AND Col3 = "value2" ORDER BY Col1;
        EXPECTED: [[3u;["{\"a\":\"value_a\",\"b\":\"value_b\"}"];["value2"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(DoubleFilterReduceScopeVariants, scriptDoubleFilterReduceScopeVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptDoubleFilterReduceScopeWithPredicateVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            Col3 UTF8,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, JsonDocument('{"a" : "value_a", "b" : "b1", "c" : "c1"}'), "value1"), (2u, JsonDocument('{"a" : "value_a"}'), "value1"),
                                                                (3u, JsonDocument('{"a" : "value_a", "b" : "value_b"}'), "value2"), (4u, JsonDocument('{"b" : "value_b", "a" : "a4"}'), "value4")
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "value_a" AND Col3 = "value2" AND Col1 > 1 ORDER BY Col1;
        EXPECTED: [[3u;["{\"a\":\"value_a\",\"b\":\"value_b\"}"];["value2"]]]

    )";

    Y_UNIT_TEST_STRING_VARIATOR(DoubleFilterReduceScopeWithPredicateVariants, scriptDoubleFilterReduceScopeWithPredicateVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptDoubleFilterReduceScopeWithPredicateVariantsWithSeparatedColumnAtFirst = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            Col3 UTF8,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, JsonDocument('{"a" : "value_a", "b" : "b1", "c" : "c1"}'), "value1"), (2u, JsonDocument('{"a" : "value_a"}'), "value1"),
                                                                (3u, JsonDocument('{"a" : "value_a", "b" : "value_b"}'), "value2"), (4u, JsonDocument('{"b" : "value_b", "a" : "a4dsadasdasdasdsdasdasdas"}'), "value4")
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "value_a" AND JSON_VALUE(Col2, "$.b") = "value_b" AND Col1 > 1 ORDER BY Col1;
        EXPECTED: [[3u;["{\"a\":\"value_a\",\"b\":\"value_b\"}"];["value2"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(DoubleFilterReduceScopeWithPredicateVariantsWithSeparatedColumnAtFirst, scriptDoubleFilterReduceScopeWithPredicateVariantsWithSeparatedColumnAtFirst) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptFilterVariantsCount = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT COUNT(*) FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a2";
        EXPECTED: [[1u]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(FilterVariantsCount, scriptFilterVariantsCount) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptSimpleVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(SimpleVariants, scriptSimpleVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptSimpleExistsVariants = R"(
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_EXISTS(Col2, "$.a") ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(SimpleExistsVariants, scriptSimpleExistsVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptCompactionVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4", "a" : "a4"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a" : "1a1"}')), (12u, JsonDocument('{"a" : "1a2"}')),
                                                                (13u, JsonDocument('{"b" : "1b3"}')), (14u, JsonDocument('{"b" : "1b4", "a" : "a4"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
        ------
        ONE_COMPACTION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4\"}"]];[10u;#];
                                [11u;["{\"a\":\"1a1\"}"]];[12u;["{\"a\":\"1a2\"}"]];[13u;["{\"b\":\"1b3\"}"]];[14u;["{\"a\":\"a4\",\"b\":\"1b4\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(CompactionVariants, scriptCompactionVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptBloomMixIndexesVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `SCAN_FIRST_LEVEL_ONLY`=`$$true|false$$`,
                    `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10$$`,
                    `MEM_LIMIT_CHUNK`=`$$0|1000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')),
                                                                (3u, JsonDocument('{"b.c.d" : "b3"}')), (4u, JsonDocument('{"b.c.d" : "b4", "a" : "a4"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a.b.c" : "1a1"}')), (12u, JsonDocument('{"a.b.c" : "1a2"}')),
                                                                (13u, JsonDocument('{"b.c.d" : "1b3"}')), (14u, JsonDocument('{"b.c.d" : "1b4", "a" : "a4"}'))
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=a_index, TYPE=$$CATEGORY_BLOOM_FILTER|BLOOM_FILTER$$,
                FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_b, TYPE=BLOOM_NGRAMM_FILTER,
            FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : false, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"b.c.d"'}}`);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_a, TYPE=BLOOM_NGRAMM_FILTER,
            FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : true, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : "a"}}`);
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
        ------
        ONE_ACTUALIZATION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" ORDER BY Col1;
        EXPECTED: [[1u;["{\"a.b.c\":\"a1\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" AND JSON_VALUE(Col2, "$.\"a\"") = "a4" ORDER BY Col1;
        EXPECTED: []
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" AND JSON_VALUE(Col2, "$.\"a\"") = "a4" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=DROP_INDEX, NAME=a_index)
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "1a1" ORDER BY Col1;
        EXPECTED: [[11u;["{\"a.b.c\":\"1a1\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=b_index, TYPE=CATEGORY_BLOOM_FILTER,
                FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "%1b4%" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1b4%" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1B4" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a4" ORDER BY Col1;
        EXPECTED: [[4u;["{\"a\":\"a4\",\"b.c.d\":\"b4\"}"]];[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 3, 2
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a\"") = "a4" ORDER BY Col1;
        EXPECTED: [[4u;["{\"a\":\"a4\",\"b.c.d\":\"b4\"}"]];[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 3, 2
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d111\"") = "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b3" ORDER BY Col1;
        EXPECTED: [[13u;["{\"b.c.d\":\"1b3\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0

    )";
    Y_UNIT_TEST_STRING_VARIATOR(BloomMixIndexesVariants, scriptBloomMixIndexesVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptBloomCategoryIndexesVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `SCAN_FIRST_LEVEL_ONLY`=`$$true|false$$`,
                    `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10$$`,
                    `MEM_LIMIT_CHUNK`=`$$0|1000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')),
                                                                (3u, JsonDocument('{"b.c.d" : "b3"}')), (4u, JsonDocument('{"b.c.d" : "b4", "a" : "a4"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a.b.c" : "1a1"}')), (12u, JsonDocument('{"a.b.c" : "1a2"}')),
                                                                (13u, JsonDocument('{"b.c.d" : "1b3"}')), (14u, JsonDocument('{"b.c.d" : "1b4", "a" : "a4"}'))
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=a_index, TYPE=$$CATEGORY_BLOOM_FILTER|BLOOM_FILTER$$,
                FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
        ------
        ONE_ACTUALIZATION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" ORDER BY Col1;
        EXPECTED: [[1u;["{\"a.b.c\":\"a1\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=DROP_INDEX, NAME=a_index)
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "1a1" ORDER BY Col1;
        EXPECTED: [[11u;["{\"a.b.c\":\"1a1\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=b_index, TYPE=CATEGORY_BLOOM_FILTER,
                FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a4" ORDER BY Col1;
        EXPECTED: [[4u;["{\"a\":\"a4\",\"b.c.d\":\"b4\"}"]];[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 3, 2
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d111\"") = "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0

    )";
    Y_UNIT_TEST_STRING_VARIATOR(BloomCategoryIndexesVariants, scriptBloomCategoryIndexesVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptBloomNGrammIndexesVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `SCAN_FIRST_LEVEL_ONLY`=`$$true|false$$`,
                    `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10$$`,
                    `MEM_LIMIT_CHUNK`=`$$0|1000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')),
                                                                (3u, JsonDocument('{"b.c.d" : "b3"}')), (4u, JsonDocument('{"b.c.d" : "b4", "a" : "a4"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a.b.c" : "1a1"}')), (12u, JsonDocument('{"a.b.c" : "1a2"}')),
                                                                (13u, JsonDocument('{"b.c.d" : "1b3"}')), (14u, JsonDocument('{"b.c.d" : "1b4", "a" : "a4"}'))
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_b, TYPE=BLOOM_NGRAMM_FILTER,
            FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : false, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"b.c.d"'}}`);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_a, TYPE=BLOOM_NGRAMM_FILTER,
            FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : true, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : "a"}}`);
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
        ------
        ONE_ACTUALIZATION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" ORDER BY Col1;
        EXPECTED: [[1u;["{\"a.b.c\":\"a1\"}"]]]
        IDX_ND_SKIP_APPROVE: 5, 0, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "%1b4%" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1b4%" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1B4" ORDER BY Col1;
        EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 5, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a4" ORDER BY Col1;
        EXPECTED: [[4u;["{\"a\":\"a4\",\"b.c.d\":\"b4\"}"]];[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 3, 2
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d111\"") = "1b5" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 5, 0, 0
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b3" ORDER BY Col1;
        EXPECTED: [[13u;["{\"b.c.d\":\"1b3\"}"]]]
        IDX_ND_SKIP_APPROVE: 0, 4, 1
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1B3" ORDER BY Col1;
        EXPECTED: []
        IDX_ND_SKIP_APPROVE: 0, 4, 1
    )";
    Y_UNIT_TEST_STRING_VARIATOR(BloomNGrammIndexesVariants, scriptBloomNGrammIndexesVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptSwitchAccessorCompactionVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4", "a" : "a4"}'))
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a" : "1a1"}')), (12u, JsonDocument('{"a" : "1a2"}')),
                                                                (13u, JsonDocument('{"b" : "1b3"}')), (14u, JsonDocument('{"b" : "1b4", "a" : "a4"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
        ------
        ONE_COMPACTION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4\"}"]];[10u;#];
                                [11u;["{\"a\":\"1a1\"}"]];[12u;["{\"a\":\"1a2\"}"]];[13u;["{\"b\":\"1b3\"}"]];[14u;["{\"a\":\"a4\",\"b\":\"1b4\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(SwitchAccessorCompactionVariants, scriptSwitchAccessorCompactionVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptDuplicationCompactionVariants = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4", "a" : "a4"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "1a1"}')), (2u, JsonDocument('{"a" : "1a2"}')),
                                                                (3u, JsonDocument('{"b" : "1b3"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(2u)
        ------
        ONE_COMPACTION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"1a1\"}"]];[2u;#];[3u;["{\"b\":\"1b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4\"}"]]]

    )";
    Y_UNIT_TEST_STRING_VARIATOR(DuplicationCompactionVariants, scriptDuplicationCompactionVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }
}

}   // namespace NKikimr::NKqp
