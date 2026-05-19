#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <optional>
#include <vector>

namespace NKikimr::NKqp {

// JSON shape categories for the generated corpus
enum class EJsonShape : uint8_t {
    // Top-level scalar: null / true / false / number / string
    Scalar = 0,

    // Flat object: {"u_<key>": <key>, "shared": "shared_v", "g5_<key%5>": true, "rank": <key%50>}
    FlatObj = 1,

    // Object with empty containers: {"u_<key>": [], "shared": {}, "empty_key": null}
    EmptyContainers = 2,

    // Object with empty-string key: {"": <key>, "u_<key>": "u_v_<key>"}
    EmptyKey = 3,

    // Top-level array: [null, true, false, <key>, "u_v_<key>", "shared_v"]
    ArrayLiterals = 4,

    // Object with array values: {"u_<key>": [<key>, <key+1>, "u_v_<key>"], "shared": [true, null]}
    ObjWithArray = 5,

    // Homogeneous array of objects: [{"u_<key>": <key>}, {"u_<key>": <key+1>}, {"shared": "v"}]
    HomogeneousArrayObjs = 6,

    // Heterogeneous array of objects: [{"k_a": <key>}, {"k_b": "u_v_<key>"}, {"shared": null}]
    HeterogeneousArrayObjs = 7,

    // 2-level nested object: {"shared": {"u_<key>": <key>, "g5_<key%5>": "v"}}
    NestedObj = 8,

    // 4-level deep nesting: {"a": {"b": {"c": {"u_<key>": <key>, "shared": null}}}}
    DeepNested = 9,

    // Mixed nested: {"shared": [{"u_<key>": <key>}, {"shared": [1,2,3]}], "g5_<key%5>": {"deep": {"v": <key>}}}
    Mixed = 10,

    // Array of arrays: [[<key>, <key+1>], [<key+2>], []]
    ArrayOfArrays = 11,

    // Object with array of objects: {"items": [{"id": <key>, "name": "u_v_<key>"}, {"id": <key+1>, "name": "shared_v"}]}
    ObjWithArrayObjs = 12,

    // Full literal mix: {"shared_n": <key>, "shared_s": "u_v_<key>", "shared_b": bool, "shared_null": null, "shared_arr": [...]}
    FullLiteralMix = 13,

    // SQL NULL - Text column is NULL
    SqlNull = 14,

    Count_ = 15,
};

static constexpr size_t kJsonCorpusNumShapes = static_cast<size_t>(EJsonShape::Count_);

struct TGeneratedRow {
    ui64 Key = 0;
    std::optional<TString> JsonText;
    EJsonShape Shape = EJsonShape::Scalar;
};

struct TCorpusOptions {
    size_t RowCount = 200;
    ui64 Seed = 0xC0DE;
    bool IncludeNulls = true;
    bool IncludeAllShapes = true;
};

// Generates a deterministic corpus of JSON rows for index correctness testing.
class TJsonCorpus {
public:
    explicit TJsonCorpus(TCorpusOptions opts = {});

    const std::vector<TGeneratedRow>& Rows() const {
        return Rows_;
    }

    // Insert Rows_[offset .. offset+count) into the given table.
    void UpsertRange(NYdb::NQuery::TQueryClient& db, std::string_view tableName,
        std::string_view jsonType, size_t offset, size_t count) const;

private:
    static TGeneratedRow MakeRow(ui64 key, EJsonShape shape);
    static TString SerializeJson(ui64 key, EJsonShape shape);

    void UpsertBatch(NYdb::NQuery::TQueryClient& db, std::string_view tableName,
        std::string_view jsonType, size_t from, size_t to) const;

    std::vector<TGeneratedRow> Rows_;
};

} // namespace NKikimr::NKqp
