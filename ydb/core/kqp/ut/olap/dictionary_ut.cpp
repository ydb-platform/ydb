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

Y_UNIT_TEST_SUITE(KqpOlapDictionary) {
    Y_UNIT_TEST(DifferentPages) {
        NArrow::NConstruction::TStringPoolFiller sPool(10, 512);
        std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
        builders.emplace_back(
            NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable(
                "pk_int", 0));
        builders.emplace_back(
            std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TStringPoolFiller>>("data", sPool));
        NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
        auto arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(batchBuilder.BuildBatch(800000)));

        TString script = Sprintf(R"(
            STOP_COMPACTION
            ------
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                pk_int Uint64 NOT NULL,
                data Utf8,
                PRIMARY KEY (pk_int)
            )
            PARTITION BY HASH(pk_int)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=data, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            BULK_UPSERT:
                /Root/ColumnTable
                %s
                PARTS_COUNT:16
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
            
        )",
            arrowString.data());
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(EmptyStringVariants) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 Utf8,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col1, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (1u)
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;#]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(SimpleStringVariants) {
        TString script = R"(
            STOP_COMPACTION
            ------
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 Utf8,
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col1, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
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
        TScriptVariator(script).Execute();
    }
}

}   // namespace NKikimr::NKqp
