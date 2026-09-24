#include "sub_columns_scenarios.h"

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

// The JsonDocument column table every dictionary test operates on.
constexpr const char* CreateColumnTableDdl = R"(CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);)";

// Common script preamble: stop background compaction, create the table, pin the SIMPLE scan reader
// (and, when the test will run compaction, the tiling++ planner). Callers append their own ALTER
// COLUMN (sub-columns/dictionary) settings, data, and assertions.
TString DictionaryTableSetup(const bool withCompactionPlanner = false) {
    TStringBuilder script;
    script << R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        )" << CreateColumnTableDdl << R"(
        ------)";
    if (withCompactionPlanner) {
        script << R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`)
        ------)";
    }
    script << R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
        ------)";
    return script;
}

}   // namespace

using NSubColumnsScenarios::AccessorTypeCheck;

Y_UNIT_TEST_SUITE(KqpOlapJsonDictionary) {

    // Two low-cardinality portions are merged by compaction; the merged sub-column must be
    // re-encoded as a dictionary and still read back correctly.
    Y_UNIT_TEST(Compaction) {
        const TString script = TStringBuilder() << DictionaryTableSetup(/*withCompactionPlanner=*/true) << R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `DICTIONARY_UNIQUE_FRACTION`=`0.5`)
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
        )" << AccessorTypeCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary);
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // A freshly written low-cardinality separated column must actually be dictionary-encoded
    // (not just readable). Asserts the encoding engaged via primary_index_stats.
    Y_UNIT_TEST(FreshWrite) {
        const TString script = TStringBuilder() << DictionaryTableSetup() << R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_UNIQUE_FRACTION`=`1`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "x"}')), (2u, JsonDocument('{"a" : "y"}')),
                                                             (3u, JsonDocument('{"a" : "x"}')), (4u, JsonDocument('{"a" : "y"}')),
                                                             (5u, JsonDocument('{"a" : "x"}')), (6u, JsonDocument('{"a" : "y"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"x\"}"]];[2u;["{\"a\":\"y\"}"]];[3u;["{\"a\":\"x\"}"]];[4u;["{\"a\":\"y\"}"]];[5u;["{\"a\":\"x\"}"]];[6u;["{\"a\":\"y\"}"]]]
        ------
        )" << AccessorTypeCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary);
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
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

        const TString script = DictionaryTableSetup(/*withCompactionPlanner=*/true) + TString(std::format(R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `OTHERS_ALLOWED_FRACTION`=`0`, `MEM_LIMIT_CHUNK`=`100`, `DICTIONARY_UNIQUE_FRACTION`=`1`)
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
        )",
            batch1.c_str(), batch2.c_str()))
            + AccessorTypeCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary, /*minChunks=*/1);
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // A single first-level key ("a") holds every JSON value type across records (string, int, float,
    // bool, null, array, nested object). With SCAN_FIRST_LEVEL_ONLY the key stays one sub-column whose
    // leaf values are the raw serialized sub-values. The $$...$$ variation runs the same round-trip
    // both with dictionary encoding (KFF=1) and without it, so heterogeneous values must reconstruct
    // losslessly under either encoding. (That KFF=1 actually engages the dictionary is covered by the
    // SubColumnsDictionary::KffGate unit test.)
    TString scriptAllValueTypes = TStringBuilder() << DictionaryTableSetup() << R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`true`,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0`$$, `DICTIONARY_UNIQUE_FRACTION`=`1`|$$)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "str"}')), (2u, JsonDocument('{"a" : 42}')),
                                                             (3u, JsonDocument('{"a" : 1.5}')), (4u, JsonDocument('{"a" : true}')),
                                                             (5u, JsonDocument('{"a" : false}')), (6u, JsonDocument('{"a" : null}')),
                                                             (7u, JsonDocument('{"a" : [1, 2, 3]}')), (8u, JsonDocument('{"a" : {"x" : 1, "y" : {"z" : 2}}}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"str\"}"]];[2u;["{\"a\":42}"]];[3u;["{\"a\":1.5}"]];[4u;["{\"a\":true}"]];[5u;["{\"a\":false}"]];[6u;["{\"a\":null}"]];[7u;["{\"a\":[1,2,3]}"]];[8u;["{\"a\":{\"x\":1,\"y\":{\"z\":2}}}"]]]
    )";
    Y_UNIT_TEST_STRING_VARIATOR(AllValueTypes, scriptAllValueTypes) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    // Read-path scenarios generated (with isDictionary=true) from the same source as json_ut's
    // *Variants (isDictionary=false, see sub_columns_scenarios.h): identical data/queries/expected, the
    // only difference being DICTIONARY_UNIQUE_FRACTION=1 and the "all sub-columns Dictionary" assertion the
    // generator appends. Reads must return the same results as the non-dictionary path.
    Y_UNIT_TEST(RestoreFullJson) {
        Variator::ToExecutor(Variator::SingleScript(NSubColumnsScenarios::RestoreFullJson(/*isDictionary=*/true))).Execute();
    }

    Y_UNIT_TEST(RestoreFirstLevel) {
        Variator::ToExecutor(Variator::SingleScript(NSubColumnsScenarios::RestoreFirstLevel(/*isDictionary=*/true))).Execute();
    }

    Y_UNIT_TEST(Filter) {
        Variator::ToExecutor(Variator::SingleScript(NSubColumnsScenarios::Filter(/*isDictionary=*/true))).Execute();
    }

    Y_UNIT_TEST(Simple) {
        Variator::ToExecutor(Variator::SingleScript(NSubColumnsScenarios::Simple(/*isDictionary=*/true))).Execute();
    }

    Y_UNIT_TEST(SimpleExists) {
        Variator::ToExecutor(Variator::SingleScript(NSubColumnsScenarios::SimpleExists(/*isDictionary=*/true))).Execute();
    }

    // Shared setup for the DISTINCT JSON_VALUE dictionary-only scenarios: one portion where every sub-column
    // ("a", "b") is dictionary encoded; row 6 has no "a" so DISTINCT must still observe NULL.
    constexpr const char* DistinctJsonPragmas =
        R"(PRAGMA Kikimr.OptEnableOlapPushdown = "true"; PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true"; PRAGMA Kikimr.OptForceOlapPushdownDistinct = "a";)";

    TString DistinctJsonDictionarySetup() {
        return TStringBuilder() << DictionaryTableSetup() << R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_UNIQUE_FRACTION`=`1`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "x", "b" : "p"}')), (2u, JsonDocument('{"a" : "y", "b" : "q"}')),
                                                             (3u, JsonDocument('{"a" : "x", "b" : "q"}')), (4u, JsonDocument('{"a" : "y", "b" : "p"}')),
                                                             (5u, JsonDocument('{"a" : "x", "b" : "r"}')), (6u, JsonDocument('{"b" : "r"}'))
        ------
        )" << AccessorTypeCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary) << R"(
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------)";
    }

    // Forced DISTINCT over JSON_VALUE of a dictionary encoded sub-column reads only the sub-column dictionary
    // (counter grows per scan), with and without LIMIT, and keeps NULL for rows without the key.
    Y_UNIT_TEST(DistinctJsonValueDictionaryOnly) {
        const TString script = TStringBuilder() << DistinctJsonDictionarySetup() << R"(
        READ: )" << DistinctJsonPragmas << R"( PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10"; SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable` LIMIT 10;
        EXPECTED_UNORDERED: [[["x"]];[["y"]];[#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 1
        ------
        READ: )" << DistinctJsonPragmas << R"( SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable`;
        EXPECTED_UNORDERED: [[["x"]];[["y"]];[#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 2
        ------
        READ: )" << DistinctJsonPragmas << R"( PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "2"; SELECT COUNT(*) FROM (SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable` LIMIT 2);
        EXPECTED: [[2u]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 3
        )";
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // A filter on another path of the same JSON column needs the rows: dictionary-only must stay off and
    // results must match the plain evaluation.
    Y_UNIT_TEST(DistinctJsonValueDictionaryOnly_FilterOnOtherPathDisables) {
        const TString script = TStringBuilder() << DistinctJsonDictionarySetup() << R"(
        READ: )" << DistinctJsonPragmas << R"( PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10"; SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.b") = "p" LIMIT 10;
        EXPECTED_UNORDERED: [[["x"]];[["y"]]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        READ: )" << DistinctJsonPragmas << R"( SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.b") = "r";
        EXPECTED_UNORDERED: [[["x"]];[#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        ------
        READ: )" << DistinctJsonPragmas << R"( SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable` WHERE Col1 > 2u;
        EXPECTED_UNORDERED: [[["x"]];[["y"]];[#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        )";
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }

    // Second portion written after switching the dictionary off: its "a" is a plain sub-column. Two portions require
    // duplicate filtering, which (as for plain dictionary columns) keeps dictionary-only off for the whole scan; the
    // point is that mixed encodings of the same sub-column return the right DISTINCT set with the forced pushdown.
    Y_UNIT_TEST(DistinctJsonValueDictionaryOnly_MixedPortions) {
        const TString script = TStringBuilder() << DistinctJsonDictionarySetup() << R"(
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                    `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_UNIQUE_FRACTION`=`0`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (7u, JsonDocument('{"a" : "z", "b" : "p"}')), (8u, JsonDocument('{"a" : "z", "b" : "q"}'))
        ------
        READ: )" << DistinctJsonPragmas << R"( PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10"; SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable` LIMIT 10;
        EXPECTED_UNORDERED: [[["x"]];[["y"]];[["z"]];[#]]
        ------
        READ: )" << DistinctJsonPragmas << R"( SELECT DISTINCT JSON_VALUE(Col2, "$.a") AS a FROM `/Root/ColumnTable`;
        EXPECTED_UNORDERED: [[["x"]];[["y"]];[["z"]];[#]]
        ------
        CHECK_COUNTER: Deriviative/Dictionary/OnlyOptimization/Count
        PATH: tablets/subsystem/columnshard/module_id/Scan
        EXPECTED: 0
        )";
        Variator::ToExecutor(Variator::SingleScript(script)).Execute();
    }
}

}   // namespace NKikimr::NKqp
