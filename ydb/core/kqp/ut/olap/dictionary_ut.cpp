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

    TString scriptDifferentPages = R"(
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
        Variator::ToExecutor(Variator::SingleScript(Sprintf(__SCRIPT_CONTENT.c_str(), injection.c_str()))).Execute();
    }

    TString scriptEmptyStringVariants = R"(
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
    Y_UNIT_TEST_STRING_VARIATOR(EmptyStringVariants, scriptEmptyStringVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptSimpleStringVariants = R"(
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
    Y_UNIT_TEST_STRING_VARIATOR(SimpleStringVariants, scriptSimpleStringVariants) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    // Loop 10 times: insert, wait compaction, insert, change schema, insert, set actualization, insert, wait actualization
    // lc-buckets with 2 layers: Zero (portions_live_duration 1s) + OneLayer.
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
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
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
                cycleBlocks += "SCHEMA:\nALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)\n------\n";
            } else {
                cycleBlocks += "SCHEMA:\nALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`)\n------\n";
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
        Variator::ToExecutor(Variator::SingleScript(Sprintf(scriptDictCompactionAndActualization.c_str(), cycleBlocks.c_str()))).Execute();
    }

    // ChunkDetails in .sys/primary_index_stats for dictionary column: check deterministic output for 1 row.
    TString scriptChunkDetailsDictionary = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            field Utf8 COMPRESSION(algorithm=off),
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
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
        EXPECTED: [[["{\"records_blob_size\":152,\"variants_blob_size\":176}"]]]
    )";
    Y_UNIT_TEST(ChunkDetailsDictionary) {
        Variator::ToExecutor(Variator::SingleScript(scriptChunkDetailsDictionary)).Execute();
    }
}

}   // namespace NKikimr::NKqp
