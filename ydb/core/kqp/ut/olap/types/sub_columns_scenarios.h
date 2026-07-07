#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

// Sub_columns read-path scenarios generated for both the non-dictionary and dictionary paths from a
// single place, so the two differ only by the `isDictionary` switch. Each `<Scenario>(isDictionary)`
// returns a full script:
//   * isDictionary == false -> a settings-sweep VARIATOR ($$...$$ axes) that asserts every separated
//                              Col2 sub-column is stored as EType::Array (dictionary never engages);
//   * isDictionary == true  -> a fixed setup with DICTIONARY_DETECTOR_KFF=1 that asserts every
//                              separated Col2 sub-column is stored as EType::Dictionary.
// The data + reads (the bodies below) and the expected results are identical either way.
namespace NKikimr::NKqp::NSubColumnsScenarios {

// ---- helpers --------------------------------------------------------------------------------------

// primary_index_stats assertion that in every Col2 chunk all separated sub-columns were serialized with
// `expectedType`. `minChunks` is the lower limit on the number of chunks for column. `requirePresence` additionally requires at least one
// separated sub-column per chunk -- used by the dictionary path to reject an empty pass; the
// non-dictionary sweep leaves it off, since COLUMNS_LIMIT=0 combos legitimately produce chunks with no
// separated sub-columns.
inline TString AccessorTypeCheck(
    const NArrow::NAccessor::IChunkedArray::EType expectedType, const ui32 minChunks = 0, const bool requirePresence = true) {
    const char* presence = requirePresence ? R"(JSON_EXISTS(CAST(ChunkDetails AS JsonDocument), "$.columns.accessor[0]") AND )" : "";
    return Sprintf(R"SQL(READ: $All = SELECT COUNT(*) AS cnt FROM `/Root/ColumnTable/.sys/primary_index_stats`
                  WHERE Activity == 1 AND EntityName = 'Col2';
              $Ok = SELECT SUM(CASE
                    -- $.columns.accessor[*] ? (@ != <>)  is an SQL/JSON expression meaning "value of current accessor != <>"
                    WHEN %sNOT JSON_EXISTS(CAST(ChunkDetails AS JsonDocument), "$.columns.accessor[*] ? (@ != %u)")
                    THEN 1 ELSE 0 END) AS ok
                  FROM `/Root/ColumnTable/.sys/primary_index_stats`
                  WHERE Activity == 1 AND EntityName = 'Col2';
              SELECT ($All > %uu) AND ($All == $Ok);
        EXPECTED: [[[%%true]]])SQL",
        presence, (ui32)expectedType, minChunks);
}

// Accumulates independent scenario steps and joins them into a single Variator script on Build(). The
// executor splits a script on the "------" token and strips each step (see Variator::SingleScript), so
// Build() only has to interleave the separators; a step may itself contain "------" (e.g. a `body` that
// bundles DATA + several READs), in which case it just expands into several steps.
class TScenarioBuilder {
private:
    TVector<TString> Steps;

public:
    TScenarioBuilder& Add(const TStringBuf step) {
        Steps.emplace_back(step);
        return *this;
    }

    TString Build() const {
        TStringBuilder result;
        for (size_t i = 0; i < Steps.size(); ++i) {
            if (i) {
                result << "\n        ------\n        ";
            }
            result << Steps[i];
        }
        return result;
    }
};

// Assemble a full scenario: STOP_COMPACTION + create + SIMPLE reader + ALTER COLUMN (SUB_COLUMNS +
// `alterColumnExtractor` + either the sweep or dictionary settings) + the `bodyParts` (each is added as
// its own step, so callers pass the DATA/READ steps directly instead of pre-joining them) + the matching
// accessor check. See the file header for the two modes.
template <typename... TBodyParts>
inline TString BuildScenario(const TStringBuf alterColumnExtractor, const bool isDictionary, const TBodyParts&... bodyParts) {
    const TStringBuf partitions = isDictionary ? TStringBuf("1") : TStringBuf("$$1|2|10$$");
    const TString alterColumn = isDictionary
        ? Sprintf(R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, %s`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_DETECTOR_KFF`=`1`))",
              alterColumnExtractor.data())
        : Sprintf(R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, %s`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`))",
              alterColumnExtractor.data());
    TScenarioBuilder builder;
    builder.Add("STOP_COMPACTION")
        .Add(Sprintf(R"(SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            Col2 JsonDocument,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s);)", partitions.data()))
        .Add(R"(SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`))")
        .Add(Sprintf(R"(SCHEMA:
        %s)", alterColumn.c_str()));
    (builder.Add(bodyParts), ...);
    builder.Add(isDictionary
            ? AccessorTypeCheck(NArrow::NAccessor::IChunkedArray::EType::Dictionary, 0, /*requirePresence*/ true)
            : AccessorTypeCheck(NArrow::NAccessor::IChunkedArray::EType::Array, 0, /*requirePresence*/ false));
    return builder.Build();
}

// ---- scenarios ------------------------------------------------------------------------------------

inline constexpr TStringBuf RestoreScenario = R"(
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3", "e" : ["a", {"v" : ["c", 5]}]}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":null,\"e.v\":{\"c\":1,\"e\":{\"c.a\":2}}}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\",\"e\":[\"a\",{\"v\":[\"c\",5]}]}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]])";

inline TString RestoreFullJson(const bool isDictionary) {
    return BuildScenario(R"(`DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`, )", isDictionary, RestoreScenario);
}

inline TString RestoreFirstLevel(const bool isDictionary) {
    return BuildScenario(R"(`DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`true`, )", isDictionary, RestoreScenario);
}

// JSON_VALUE filters (single, full read, and nested/dotted-key filters) over a heterogeneous dataset.
inline constexpr TStringBuf FilterScenario = R"(
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a2" ORDER BY Col1;
        EXPECTED: [[2u;["{\"a\":\"a2\"}"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":null,\"e.v\":{\"c\":1,\"e\":{\"c.a\":2}}}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"e.v\".c") = "1" ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":null,\"e.v\":{\"c\":1,\"e\":{\"c.a\":2}}}"]]]
        ------
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"e.v\".e.\"c.a\"") = "2" ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":null,\"e.v\":{\"c\":1,\"e\":{\"c.a\":2}}}"]]])";

inline TString Filter(const bool isDictionary) {
    return BuildScenario(R"(`DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`, )", isDictionary, FilterScenario);
}

// Heterogeneous-schema dataset (keys present in only some rows), shared by SimpleReadAll/SimpleReadExists.
inline constexpr TStringBuf SimpleData = R"(
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}')))";

inline constexpr TStringBuf SimpleReadAll = R"(
        READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]])";

inline constexpr TStringBuf SimpleReadExists = R"(
        READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_EXISTS(Col2, "$.a") ORDER BY Col1;
        EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]])";


inline TString Simple(const bool isDictionary) {
    return BuildScenario("", isDictionary, SimpleData, SimpleReadAll);
}

inline TString SimpleExists(const bool isDictionary) {
    return BuildScenario("", isDictionary, SimpleData, SimpleReadExists);
}

}   // namespace NKikimr::NKqp::NSubColumnsScenarios
