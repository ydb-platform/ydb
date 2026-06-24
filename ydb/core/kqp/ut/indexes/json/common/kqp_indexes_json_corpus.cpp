#include "kqp_indexes_json_corpus.h"

#include <fmt/format.h>

namespace NKikimr::NKqp {

namespace {

void UpsertJsonCorpusBatch(const TJsonCorpus& corpus, NYdb::NQuery::TQueryClient& db,
    std::string_view tableName, std::string_view jsonType, size_t from, size_t to)
{
    std::string query = fmt::format("UPSERT INTO {} (Key, Text) VALUES\n", tableName);

    for (size_t i = from; i < to; ++i) {
        const auto& row = corpus.Rows()[i];

        std::string textVal;
        if (row.JsonText.has_value()) {
            std::string escaped;
            escaped.reserve(row.JsonText->size());
            for (char c : *row.JsonText) {
                if (c == '\'') {
                    escaped += "''";
                } else {
                    escaped += c;
                }
            }
            textVal = fmt::format("{}('{}')", jsonType, escaped);
        } else {
            textVal = "NULL";
        }

        query += fmt::format("  ({}, {}){}", row.Key, textVal, (i + 1 < to ? "," : ""));
    }

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace

void UpsertJsonCorpusRange(const TJsonCorpus& corpus, NYdb::NQuery::TQueryClient& db,
    std::string_view tableName, std::string_view jsonType, size_t offset, size_t count)
{
    Y_ABORT_UNLESS(offset + count <= corpus.Rows().size());
    constexpr size_t kBatchSize = 50;
    for (size_t start = offset; start < offset + count; start += kBatchSize) {
        const size_t end = std::min(start + kBatchSize, offset + count);
        UpsertJsonCorpusBatch(corpus, db, tableName, jsonType, start, end);
    }
}

} // namespace NKikimr::NKqp
