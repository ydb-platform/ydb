#include <ydb/core/kqp/ut/olap/combinatory/variator.h>

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <format>

namespace NKikimr::NKqp {

namespace {

// primary_index_stats assertion that every Col2 chunk's separated sub-column was serialized with
// `expectedType` (compared against the IChunkedArray::EType enum value, not a magic number).
// `minChunks` lets a caller additionally require more than N chunks (e.g. multi-chunk coverage).
TString SubColumnsColumnAccessorCheck(const NArrow::NAccessor::IChunkedArray::EType expectedType, const ui32 minChunks = 0) {
    return TString(std::format(R"(READ: $All = SELECT COUNT(*) AS cnt FROM `/Root/ColumnTable/.sys/primary_index_stats`
                  WHERE Activity == 1 AND EntityName = 'Col2';
              $Ok = SELECT SUM(CASE
                    WHEN CAST(JSON_VALUE(CAST(ChunkDetails AS JsonDocument), "$.columns.accessor[0]") AS Uint64) == {}u
                    THEN 1 ELSE 0 END) AS ok
                  FROM `/Root/ColumnTable/.sys/primary_index_stats`
                  WHERE Activity == 1 AND EntityName = 'Col2';
              SELECT ($All > {}u) AND ($All == $Ok);
        EXPECTED: [[[%true]]])",
        (ui32)expectedType, minChunks));
}

}   // namespace

Y_UNIT_TEST_SUITE(KqpOlapJsonDictionary) {

    // Two low-cardinality portions are merged by compaction; the merged sub-column must be
    // re-encoded as a dictionary and still read back correctly.
    Y_UNIT_TEST(Compaction) {
        const TString script = TStringBuilder() << R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `DICTIONARY_DETECTOR_KFF`=`2`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "x"}')), (2u, JsonDocument('{"a" : "y"}')),
                                                             (3u, JsonDocument('{"a" : "x"}')), (4u, JsonDocument('{"a" : "y"}'))
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (5u, JsonDocument('{"a" : "x"}')), (6u, JsonDocument('{"a" : "y"}')),
                                                             (7u, JsonDocument('{"a" : "x"}')), (8u, JsonDocument('{"a" : "y"}'))
        ------
        ONE_COMPACTION
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"x\"}"]];[2u;["{\"a\":\"y\"}"]];[3u;["{\"a\":\"x\"}"]];[4u;["{\"a\":\"y\"}"]];[5u;["{\"a\":\"x\"}"]];[6u;["{\"a\":\"y\"}"]];[7u;["{\"a\":\"x\"}"]];[8u;["{\"a\":\"y\"}"]]]
        ------
        )" << SubColumnsColumnAccessorCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary);
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // A freshly written low-cardinality separated column must actually be dictionary-encoded
    // (not just readable). Asserts the encoding engaged via primary_index_stats.
    Y_UNIT_TEST(FreshWrite) {
        const TString script = TStringBuilder() << R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_DETECTOR_KFF`=`1`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "x"}')), (2u, JsonDocument('{"a" : "y"}')),
                                                             (3u, JsonDocument('{"a" : "x"}')), (4u, JsonDocument('{"a" : "y"}')),
                                                             (5u, JsonDocument('{"a" : "x"}')), (6u, JsonDocument('{"a" : "y"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"x\"}"]];[2u;["{\"a\":\"y\"}"]];[3u;["{\"a\":\"x\"}"]];[4u;["{\"a\":\"y\"}"]];[5u;["{\"a\":\"x\"}"]];[6u;["{\"a\":\"y\"}"]]]
        ------
        )" << SubColumnsColumnAccessorCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary);
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // A separated column with many (>256) distinct values widens the dictionary positions index past
    // uint8. With KFF=1 the column is dictionary-encoded (distinct <= records) and must round-trip
    // through the wide index; with KFF=2 the gate rejects it (distinct*2 > records) so it stays Array.
    Y_UNIT_TEST(WideIndex) {
        NColumnShard::TTableUpdatesBuilder updates(NArrow::MakeArrowSchema(
            { { "Col1", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64) }, { "Col2", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } }));
        const ui32 rowsCount = 300;
        for (ui32 i = 0; i < rowsCount; ++i) {
            updates.AddRow().Add<int64_t>(i).Add((TStringBuilder() << R"({"a" : "v)" << i << R"("})").c_str());
        }
        const TString arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(updates.BuildArrow()));

        const auto runScript = [&](const TString& kff, const NArrow::NAccessor::IChunkedArray::EType expectedType) {
            const TString script = TString(std::format(R"(
                STOP_COMPACTION
                ------
                SCHEMA:
                CREATE TABLE `/Root/ColumnTable` (
                    Col1 Uint64 NOT NULL,
                    Col2 JsonDocument,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
                ------
                SCHEMA:
                ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
                ------
                SCHEMA:
                ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                            `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_DETECTOR_KFF`=`{}`)
                ------
                BULK_UPSERT:
                    /Root/ColumnTable
                    {}
                    EXPECT_STATUS:SUCCESS
                ------
                READ: SELECT COUNT(*) FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = ("v" || CAST(Col1 AS String));
                EXPECTED: [[{}u]]
                ------
                {})",
                kff.c_str(), arrowString.c_str(), rowsCount, SubColumnsColumnAccessorCheck(expectedType).c_str()));
            Variator::ToExecutor(Variator::SingleScript(script)).Execute();
        };

        runScript("1", NArrow::NAccessor::IChunkedArray::EType::Dictionary);   // dictionary-encoded, wide (uint16) positions index
        runScript("2", NArrow::NAccessor::IChunkedArray::EType::Array);        // Kff gate rejects the high-cardinality column -> plain Array
    }

    // Compaction re-chunks the merged output by MEM_LIMIT_CHUNK. With a tiny limit the merged column
    // spans several chunks; each chunk is dictionary-encoded independently (KFF=1 => distinct <= records
    // always holds), so every chunk must carry its own dictionary/positions blob split.
    Y_UNIT_TEST(MultipleChunks) {
        const auto buildBatch = [](const ui32 from, const ui32 to) {
            NColumnShard::TTableUpdatesBuilder updates(NArrow::MakeArrowSchema(
                { { "Col1", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64) }, { "Col2", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } }));
            for (ui32 i = from; i < to; ++i) {
                const TStringBuf v = (i % 2 == 0) ? "xxxxxxxxxx" : "yyyyyyyyyy";
                updates.AddRow().Add<int64_t>(i).Add((TStringBuilder() << R"({"a" : ")" << v << R"("})").c_str());
            }
            return Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(updates.BuildArrow()));
        };
        // Two disjoint-key batches -> two portions for ONE_COMPACTION to merge.
        const TString batch1 = buildBatch(0, 100);
        const TString batch2 = buildBatch(100, 200);

        const TString script = TString(std::format(R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `OTHERS_ALLOWED_FRACTION`=`0`, `MEM_LIMIT_CHUNK`=`100`, `DICTIONARY_DETECTOR_KFF`=`1`)
        ------
        BULK_UPSERT:
            /Root/ColumnTable
            {}
            EXPECT_STATUS:SUCCESS
        ------
        BULK_UPSERT:
            /Root/ColumnTable
            {}
            EXPECT_STATUS:SUCCESS
        ------
        ONE_COMPACTION
        ------
        READ: SELECT COUNT(*) FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") IN ("xxxxxxxxxx", "yyyyyyyyyy");
        EXPECTED: [[200u]]
        ------
        {})",
            batch1.c_str(), batch2.c_str(),
            SubColumnsColumnAccessorCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary, /*minChunks=*/1).c_str()));
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // A single first-level key ("a") holds every JSON value type across records (string, int, float,
    // bool, null, array, nested object). With SCAN_FIRST_LEVEL_ONLY the key stays one sub-column whose
    // leaf values are the raw serialized sub-values, so dictionary encode/decode (KFF=1) must round-trip
    // all of them losslessly.
    Y_UNIT_TEST(AllValueTypes) {
        const TString script = TStringBuilder() << R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`true`,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_DETECTOR_KFF`=`1`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "str"}')), (2u, JsonDocument('{"a" : 42}')),
                                                             (3u, JsonDocument('{"a" : 1.5}')), (4u, JsonDocument('{"a" : true}')),
                                                             (5u, JsonDocument('{"a" : false}')), (6u, JsonDocument('{"a" : null}')),
                                                             (7u, JsonDocument('{"a" : [1, 2, 3]}')), (8u, JsonDocument('{"a" : {"x" : 1, "y" : {"z" : 2}}}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"str\"}"]];[2u;["{\"a\":42}"]];[3u;["{\"a\":1.5}"]];[4u;["{\"a\":true}"]];[5u;["{\"a\":false}"]];[6u;["{\"a\":null}"]];[7u;["{\"a\":[1,2,3]}"]];[8u;["{\"a\":{\"x\":1,\"y\":{\"z\":2}}}"]]]
        ------
        )" << SubColumnsColumnAccessorCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary);
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // DICTIONARY_DETECTOR_KFF must be >= 1 (see sub_columns/request.cpp): fractional/negative values
    // fail the ALTER, 1 succeeds. Pure DDL validation - no data needed.
    Y_UNIT_TEST(KffValidation) {
        auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/ColumnTable` (
                    Col1 Uint64 NOT NULL,
                    Col2 JsonDocument,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
            )").GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const auto alterKff = [&](const TString& kff) {
            return session.ExecuteSchemeQuery(TStringBuilder()
                << "ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, "
                   "`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `DICTIONARY_DETECTOR_KFF`=`" << kff << "`)").GetValueSync();
        };

        UNIT_ASSERT_C(alterKff("0.5").GetStatus() != NYdb::EStatus::SUCCESS, "fractional KFF < 1 must be rejected");
        UNIT_ASSERT_C(alterKff("-1").GetStatus() != NYdb::EStatus::SUCCESS, "negative KFF must be rejected");
        {
            auto result = alterKff("1");
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }
}

}   // namespace NKikimr::NKqp
