#include <contrib/libs/apache/arrow_next/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_vector.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/datum.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>

#include <iostream>
#include <memory>
#include <vector>

int main() {
    const int numBatches = 20;
    const int rowsPerBatch = 10000;

    {
        auto schema = arrow20::schema({
            arrow20::field("ts", arrow20::int64()),
        });

        std::vector<std::shared_ptr<arrow20::RecordBatch>> batches;
        batches.reserve(numBatches);
        for (int sourceIdx = 0; sourceIdx < numBatches; ++sourceIdx) {
            arrow20::Int64Builder tsBuilder;

            for (int row = 0; row < rowsPerBatch; ++row) {
                auto status = tsBuilder.Append(sourceIdx + static_cast<int64_t>(row) * 1000);
                if (!status.ok()) {
                    std::cerr << "append int64 ts failed: " << status.ToString() << '\n';
                    return 1;
                }
            }

            std::shared_ptr<arrow20::Array> ts;
            auto status = tsBuilder.Finish(&ts);
            if (!status.ok()) {
                std::cerr << "finish int64 ts failed: " << status.ToString() << '\n';
                return 1;
            }

            batches.push_back(arrow20::RecordBatch::Make(schema, rowsPerBatch, {ts}));
        }

        arrow20::compute::SortOptions options({
            arrow20::compute::SortKey("ts", arrow20::compute::SortOrder::Ascending),
        });

        auto table = arrow20::Table::FromRecordBatches(schema, batches).ValueOrDie();
        std::cerr << "sorting int64 multi-chunk Table; this works on buggy builds...\n";
        auto tableIndices = arrow20::compute::SortIndices(arrow20::Datum(table), options).ValueOrDie();
        std::cerr << "int64 Table sort ok, indices: " << tableIndices->length() << '\n';
    }
    {
        auto schema = arrow20::schema({
            arrow20::field("ts", arrow20::timestamp(arrow20::TimeUnit::MICRO)),
        });

        std::vector<std::shared_ptr<arrow20::RecordBatch>> batches;
        batches.reserve(numBatches);
        for (int sourceIdx = 0; sourceIdx < numBatches; ++sourceIdx) {
            arrow20::TimestampBuilder tsBuilder(
                arrow20::timestamp(arrow20::TimeUnit::MICRO),
                arrow20::default_memory_pool());

            for (int row = 0; row < rowsPerBatch; ++row) {
                auto status = tsBuilder.Append(sourceIdx + static_cast<int64_t>(row) * 1000);
                if (!status.ok()) {
                    std::cerr << "append ts failed: " << status.ToString() << '\n';
                    return 1;
                }
            }

            std::shared_ptr<arrow20::Array> ts;
            auto status = tsBuilder.Finish(&ts);
            if (!status.ok()) {
                std::cerr << "finish ts failed: " << status.ToString() << '\n';
                return 1;
            }

            batches.push_back(arrow20::RecordBatch::Make(schema, rowsPerBatch, {ts}));
        }

        arrow20::compute::SortOptions options({
            arrow20::compute::SortKey("ts", arrow20::compute::SortOrder::Ascending),
        });

        auto table = arrow20::Table::FromRecordBatches(schema, batches).ValueOrDie();
        std::cerr << "sorting timestamp multi-chunk Table; buggy builds crash here...\n";
        auto tableIndices = arrow20::compute::SortIndices(arrow20::Datum(table), options).ValueOrDie();
        std::cerr << "timestamp Table sort ok, indices: " << tableIndices->length() << '\n';
    }

    return 0;
}
