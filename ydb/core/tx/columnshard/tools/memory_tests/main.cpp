#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/formats/arrow/reader/position.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <ranges>
#include <thread>
#include <vector>

std::shared_ptr<arrow::RecordBatch> ExtractBatch(std::shared_ptr<arrow::Table> table) {
    std::shared_ptr<arrow::RecordBatch> batch;

    arrow::TableBatchReader reader(*table);
    auto result = reader.Next();
    Y_ABORT_UNLESS(result.ok());
    batch = *result;
    result = reader.Next();
    Y_ABORT_UNLESS(result.ok() && !(*result));
    return batch;
}

std::shared_ptr<arrow::Schema> MakeFullSchema() {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("id1", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
        arrow::field("id2", arrow::utf8(), false),
        arrow::field("id3", arrow::uint64(), false),
    };

    return std::make_shared<arrow::Schema>(std::move(fields));
}

void PrintUsage() {
    std::cerr << "usage: ./memory_tests <N> <1|2|3> [sleep_time_sec]" << std::endl;
    std::cerr << " 1 - NKikimr::NArrow::TSimpleRow>" << std::endl;
    std::cerr << " 2 - arrow::RecordBatch>" << std::endl;
    std::cerr << " 3 - NKikimr::NArrow::NMerger::TSortableBatchPosition>" << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        PrintUsage();
        return -1;
    }

    long long N = 0;
    long long TYPE = 0;
    long long SLEEP_TIME_SEC = 60;
    try {
        N =  std::stoll(argv[1]);
        TYPE = std::stoll(argv[2]);
        if (argc > 3) {
            SLEEP_TIME_SEC = std::stoll(argv[3]);
        }
    } catch (...) {
        PrintUsage();
        return -2;
    }
    if (N <= 0) {
        PrintUsage();
        return -3;
    }

    if (SLEEP_TIME_SEC < 0) {
        PrintUsage();
        return -4;
    }

    std::vector<std::shared_ptr<NKikimr::NArrow::TSimpleRow>> simpleRows;
    std::vector<std::shared_ptr<arrow::RecordBatch>> recordBatches;
    std::vector<std::shared_ptr<NKikimr::NArrow::NMerger::TSortableBatchPosition>> sortableBatchPositions;

    switch (TYPE) {
        case 1:
            std::cout << "Generating " << N << " NKikimr::NArrow::TSimpleRow" << std::endl;
            simpleRows.reserve(N + 2);
            break;
        case 2:
            std::cout << "Generating " << N << " arrow::RecordBatch" << std::endl;
            recordBatches.reserve(N + 2);
            break;
        case 3:
            std::cout << "Generating " << N << " NKikimr::NArrow::NMerger::TSortableBatchPosition" << std::endl;
            sortableBatchPositions.reserve(N + 2);
            break;
        default:
            PrintUsage();
            return -5;
    }

    auto schema = MakeFullSchema();
    auto start = std::chrono::high_resolution_clock::now();
    for (auto i : std::ranges::views::iota(0ll, N)) {
        arrow::TimestampBuilder id1_B = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
        if (!id1_B.Append(i * 123123123ull).ok()) {
            return -6;
        }
        std::shared_ptr<arrow::TimestampArray> id1_A;
        if (!id1_B.Finish(&id1_A).ok()) {
            return -7;
        }

        arrow::StringBuilder id2_B;
        if (!id2_B.Append(std::to_string(i * 123123123ull)).ok()) {
            return -8;
        }
        std::shared_ptr<arrow::StringArray> id2_A;
        if (!id2_B.Finish(&id2_A).ok()) {
            return -9;
        }

        arrow::UInt64Builder id3_B;
        if (!id3_B.Append(i * 123123123ull).ok()) {
            return -10;
        }
        std::shared_ptr<arrow::UInt64Array> id3_A;
        if (!id3_B.Finish(&id3_A).ok()) {
            return -11;
        }

        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {id1_A, id2_A, id3_A});

        switch (TYPE) {
            case 1:
                simpleRows.emplace_back(std::make_shared<NKikimr::NArrow::TSimpleRow>(ExtractBatch(table), 0));
                break;
            case 2:
                recordBatches.emplace_back(ExtractBatch(table));
                break;
            case 3:
                sortableBatchPositions.emplace_back(std::make_shared<NKikimr::NArrow::NMerger::TSortableBatchPosition>(ExtractBatch(table), 0, false));
                break;
            default:
                return -12;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Data generated in " <<  elapsed.count() << " ms, now sleeping " << SLEEP_TIME_SEC << " seconds" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME_SEC));

    std::cout << "Done" << std::endl;

    return 0;
}
