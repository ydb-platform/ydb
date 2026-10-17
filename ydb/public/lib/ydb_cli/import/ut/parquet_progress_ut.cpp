#include <ydb/public/lib/ydb_cli/import/parquet_progress.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <barrier>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using NYdb::NConsoleClient::NPrivate::TParquetImportProgress;

namespace {

void AssertOk(const arrow::Status& status) {
    UNIT_ASSERT_C(status.ok(), status.ToString());
}

void CheckConcurrentBatches(parquet::Compression::type compression) {
    constexpr ui64 TotalRows = 32;
    constexpr ui64 BatchRows = 4;
    arrow::UInt64Builder keysBuilder;
    arrow::StringBuilder valuesBuilder;
    for (ui64 i = 0; i < TotalRows; ++i) {
        AssertOk(keysBuilder.Append(i));
        AssertOk(valuesBuilder.Append(std::string(128, 'x')));
    }
    std::shared_ptr<arrow::Array> keys, values;
    AssertOk(keysBuilder.Finish(&keys));
    AssertOk(valuesBuilder.Finish(&values));
    auto table = arrow::Table::Make(arrow::schema({
        arrow::field("key", arrow::uint64()), arrow::field("value", arrow::utf8()),
    }), {keys, values});
    auto output = arrow::io::BufferOutputStream::Create();
    UNIT_ASSERT_C(output.ok(), output.status().ToString());
    AssertOk(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *output,
        TotalRows, parquet::WriterProperties::Builder().compression(compression)->build()));
    auto buffer = (*output)->Finish();
    UNIT_ASSERT_C(buffer.ok(), buffer.status().ToString());
    const ui64 fileSize = (*buffer)->size();

    std::unique_ptr<parquet::arrow::FileReader> fileReader;
    AssertOk(parquet::arrow::OpenFile(std::make_shared<arrow::io::BufferReader>(*buffer),
        arrow::default_memory_pool(), &fileReader));
    // Row group size alone does not limit RecordBatch size. Exercise several
    // batches of one real Parquet file without generating 65,536+ rows.
    fileReader->set_batch_size(BatchRows);
    std::unique_ptr<arrow::RecordBatchReader> reader;
    AssertOk(fileReader->GetRecordBatchReader({0}, &reader));

    std::vector<ui64> batchRows;
    std::vector<ui64> deltas;
    std::mutex deltasLock;
    ui64 bufferedBytes = 0;
    TParquetImportProgress progress(TotalRows, fileSize,
        [&](ui64 bytes, ui64 total) {
            UNIT_ASSERT_VALUES_EQUAL(total, fileSize);
            UNIT_ASSERT(bytes > bufferedBytes);
            UNIT_ASSERT(bytes <= fileSize);
            bufferedBytes = bytes;
        },
        [&](ui64 bytes) {
            std::lock_guard<std::mutex> lock(deltasLock);
            deltas.push_back(bytes);
        });
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        AssertOk(reader->ReadNext(&batch));
        if (!batch) {
            break;
        }
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), BatchRows);
        batchRows.push_back(batch->num_rows());
        progress.OnRead(batch->num_rows());
    }
    UNIT_ASSERT_VALUES_EQUAL(batchRows.size(), 8);
    UNIT_ASSERT_VALUES_EQUAL(bufferedBytes, fileSize);

    std::barrier<> start(batchRows.size());
    std::vector<std::thread> workers;
    for (ui64 rows : batchRows) {
        workers.emplace_back([&, rows] {
            start.arrive_and_wait();
            progress.OnConfirm(rows);
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    UNIT_ASSERT_VALUES_EQUAL(deltas.size(), batchRows.size());
    ui64 confirmedBytes = 0;
    for (size_t i = 0; i < deltas.size(); ++i) {
        // Inspect the actual callback deltas: the UI cannot hide overcounting
        // by clamping the accumulated value to the file size here.
        UNIT_ASSERT(deltas[i] >= fileSize / batchRows.size());
        UNIT_ASSERT(deltas[i] <= (fileSize + batchRows.size() - 1) / batchRows.size());
        confirmedBytes += deltas[i];
        if (i + 1 < deltas.size()) {
            UNIT_ASSERT(confirmedBytes < fileSize);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(confirmedBytes, fileSize);
}

} // namespace

Y_UNIT_TEST_SUITE(TParquetImportProgressTests) {
    Y_UNIT_TEST(ReportsDeltasAndPreservesRoundingRemainder) {
        std::vector<ui64> buffered, deltas;
        TParquetImportProgress progress(10, 1003,
            [&](ui64 bytes, ui64 total) {
                UNIT_ASSERT_VALUES_EQUAL(total, 1003);
                buffered.push_back(bytes);
            },
            [&](ui64 bytes) { deltas.push_back(bytes); });

        progress.OnRead(3);
        progress.OnRead(2);
        progress.OnRead(5);
        progress.OnConfirm(3);
        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0], 300);
        progress.OnConfirm(2);
        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(deltas[1], 201);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0] + deltas[1], 501);
        progress.OnConfirm(5);
        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(deltas[2], 502);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0] + deltas[1] + deltas[2], 1003);
        UNIT_ASSERT_VALUES_EQUAL(buffered.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(buffered[0], 300);
        UNIT_ASSERT_VALUES_EQUAL(buffered[1], 501);
        UNIT_ASSERT_VALUES_EQUAL(buffered[2], 1003);
    }

    Y_UNIT_TEST(ConcurrentBatchesOfOneFile) {
        CheckConcurrentBatches(parquet::Compression::UNCOMPRESSED);
    }

    Y_UNIT_TEST(ConcurrentBatchesOfOneCompressedFile) {
        CheckConcurrentBatches(parquet::Compression::SNAPPY);
    }

    Y_UNIT_TEST(EmptyFileIncludesMetadata) {
        ui64 bufferedBytes = 0, confirmedBytes = 0;
        TParquetImportProgress progress(0, 123,
            [&](ui64 bytes, ui64 total) {
                UNIT_ASSERT_VALUES_EQUAL(total, 123);
                bufferedBytes = bytes;
            },
            [&](ui64 bytes) { confirmedBytes += bytes; });
        progress.OnRead(0);
        progress.OnConfirm(0);
        UNIT_ASSERT_VALUES_EQUAL(bufferedBytes, 123);
        UNIT_ASSERT_VALUES_EQUAL(confirmedBytes, 123);
    }
}
