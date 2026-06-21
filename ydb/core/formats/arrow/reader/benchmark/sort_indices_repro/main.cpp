#include <contrib/libs/apache/arrow_next/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_vector.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/datum.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

int main() {
    const int numBatches = 20;
    const int rowsPerBatch = 10000;

    auto schema = arrow20::schema({
        arrow20::field("ts", arrow20::timestamp(arrow20::TimeUnit::MICRO)),
        arrow20::field("a", arrow20::utf8()),
        arrow20::field("b", arrow20::utf8()),
        arrow20::field("ver", arrow20::int64()),
    });

    std::vector<std::shared_ptr<arrow20::RecordBatch>> batches;
    batches.reserve(numBatches);
    for (int sourceIdx = 0; sourceIdx < numBatches; ++sourceIdx) {
        arrow20::TimestampBuilder tsBuilder(
            arrow20::timestamp(arrow20::TimeUnit::MICRO),
            arrow20::default_memory_pool());
        arrow20::StringBuilder aBuilder;
        arrow20::StringBuilder bBuilder;
        arrow20::Int64Builder verBuilder;

        const int aDomain = std::max(1, rowsPerBatch / 10);
        for (int row = 0; row < rowsPerBatch; ++row) {
            auto status = tsBuilder.Append(sourceIdx + static_cast<int64_t>(row) * 1000);
            if (!status.ok()) {
                std::cerr << "append ts failed: " << status.ToString() << '\n';
                return 1;
            }
            status = aBuilder.Append(std::to_string(row % aDomain));
            if (!status.ok()) {
                std::cerr << "append a failed: " << status.ToString() << '\n';
                return 1;
            }
            status = bBuilder.Append(std::to_string(row));
            if (!status.ok()) {
                std::cerr << "append b failed: " << status.ToString() << '\n';
                return 1;
            }
            status = verBuilder.Append(sourceIdx);
            if (!status.ok()) {
                std::cerr << "append ver failed: " << status.ToString() << '\n';
                return 1;
            }
        }

        std::shared_ptr<arrow20::Array> ts;
        std::shared_ptr<arrow20::Array> a;
        std::shared_ptr<arrow20::Array> b;
        std::shared_ptr<arrow20::Array> ver;
        auto status = tsBuilder.Finish(&ts);
        if (!status.ok()) {
            std::cerr << "finish ts failed: " << status.ToString() << '\n';
            return 1;
        }
        status = aBuilder.Finish(&a);
        if (!status.ok()) {
            std::cerr << "finish a failed: " << status.ToString() << '\n';
            return 1;
        }
        status = bBuilder.Finish(&b);
        if (!status.ok()) {
            std::cerr << "finish b failed: " << status.ToString() << '\n';
            return 1;
        }
        status = verBuilder.Finish(&ver);
        if (!status.ok()) {
            std::cerr << "finish ver failed: " << status.ToString() << '\n';
            return 1;
        }

        batches.push_back(arrow20::RecordBatch::Make(schema, rowsPerBatch, {ts, a, b, ver}));
    }

    arrow20::compute::SortOptions options({
        arrow20::compute::SortKey("ts", arrow20::compute::SortOrder::Ascending),
        arrow20::compute::SortKey("a", arrow20::compute::SortOrder::Ascending),
        arrow20::compute::SortKey("b", arrow20::compute::SortOrder::Ascending),
        arrow20::compute::SortKey("ver", arrow20::compute::SortOrder::Descending),
    });

    auto table = arrow20::Table::FromRecordBatches(schema, batches).ValueOrDie();
    std::cerr << "table rows: " << table->num_rows()
              << ", columns: " << table->num_columns() << '\n';
    for (int i = 0; i < table->num_columns(); ++i) {
        std::cerr << "column " << table->field(i)->name()
                  << " chunks: " << table->column(i)->num_chunks() << '\n';
    }

    auto combined = arrow20::ConcatenateRecordBatches(batches).ValueOrDie();
    std::cerr << "sorting concatenated RecordBatch...\n";
    auto batchIndices = arrow20::compute::SortIndices(arrow20::Datum(combined), options).ValueOrDie();
    std::cerr << "RecordBatch sort ok, indices: " << batchIndices->length() << '\n';

    std::cerr << "sorting multi-chunk Table; buggy builds crash here...\n";
    auto tableIndices = arrow20::compute::SortIndices(arrow20::Datum(table), options).ValueOrDie();
    std::cerr << "Table sort ok, indices: " << tableIndices->length() << '\n';

    return 0;
}
