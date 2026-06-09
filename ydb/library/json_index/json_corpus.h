#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NJsonIndex {

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

    // Object whose keys straddle the LEB128 1->2 byte length boundary (lengths 64/126/127/128/200/1000)
    // plus a long (>128 byte) string value. Every value equals <key> so lookups can match.
    LongKeys = 14,

    // Keys that require quoted / bracket JsonPath notation (".", space, "*", "[", "]", "?", "@", "$")
    // together with Unicode keys and values (Cyrillic / CJK / emoji).
    SpecialKeys = 15,

    // Wide object: ~130 "f_<i>" members (exercises $.* and the member-count boundary) plus numeric
    // edge values (0, negative, -0.0, 1e15, exponent, high precision) and string look-alikes ("123"/"true"/"null"/"").
    WideObject = 16,

    // Long array: ~130 numeric elements (exercises [*], [last], large index, ranges and the element-count boundary).
    LongArray = 17,

    // Deeply nested object chain {"n":{"n":{...{"u_<key>":<key>,"leaf":<key>}}}} for deep-path / recursion stress.
    VeryDeepNested = 18,

    // SQL NULL - Text column is NULL
    SqlNull = 19,

    Count_ = 20,
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

    static TGeneratedRow MakeRow(ui64 key, EJsonShape shape);
    static TString SerializeJson(ui64 key, EJsonShape shape);

    // Construct the length-controlled object key / string value used by the LongKeys shape.
    // Exposed so predicate generators can reproduce the exact key/value for targeted lookups.
    static std::string MakeLongKey(ui64 key, size_t len);
    static std::string MakeLongString(ui64 key, size_t len);

private:
    std::vector<TGeneratedRow> Rows_;
};

} // namespace NKikimr::NJsonIndex
