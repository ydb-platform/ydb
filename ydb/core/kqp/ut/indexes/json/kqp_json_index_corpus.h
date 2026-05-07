#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <optional>
#include <vector>

namespace NKikimr::NKqp {

// JSON shape categories for the generated corpus
enum class EJsonShape : uint8_t {
    // Top-level scalar: null / true / false / integer / string
    Scalar = 0,

    // Flat object: {"u_<key>": <key>, "shared": "shared_v", "g5_<key%5>": true}
    FlatObj = 1,

    // Object with empty containers: {"u_<key>": [], "shared": {}, "empty_key": null}
    EmptyContainers = 2,

    // Object with empty-string key: {"": <key>, "u_<key>": "u_v_<key>"}
    EmptyKey = 3,

    // Top-level array: [null, true, false, <key>, "u_v_<key>", "shared_v"]
    ArrayLiterals = 4,

    // Object with array values: {"u_<key>": [<key>, <key+1>, "u_v_<key>"], "shared": [true, null]}
    ObjWithArray = 5,

    // SQL NULL - Text column is NULL
    SqlNull = 6,

    Count_ = 7,
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
