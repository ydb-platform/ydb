#include <library/cpp/testing/unittest/registar.h>
#include <util/string/split.h>
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_raw_table_queue_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_raw_table_queue_writer.h>

namespace NYql::NFmr {

std::vector<TString> TableContentRows = {
    "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};",
    "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};",
    "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};",
    "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};"
};


Y_UNIT_TEST_SUITE(FmrRawTableQueueTests) {
    Y_UNIT_TEST(ReadWriteManyThreads) {
        ui64 inputStreamsNum = 5, repeatsNum = 10;
        TFmrRawTableQueueSettings queueSettings{.MaxInflightBytes = 128};
        auto queue = MakeIntrusive<TFmrRawTableQueue>(inputStreamsNum, queueSettings);
        char rowEnd = '|';  // Adding end symbol to row for splitting simplicity

        auto threadPool = CreateThreadPool(3);
        for (ui64 i = 0; i < inputStreamsNum; ++i) {
            // each thread writes repeatsNum copies of TableContent to queue
            threadPool->SafeAddFunc([queue, repeatsNum, i, rowEnd] {
                TFmrRawTableQueueWriterSettings writerSettings{.ChunkSize = 100};
                TFmrRawTableQueueWriter queueWriter(queue, writerSettings);
                for (ui64 j = 0; j < repeatsNum; ++j) {
                    for (auto& row: TableContentRows) {
                        queueWriter.Write(row + rowEnd);
                        queueWriter.NotifyRowEnd();
                    }
                }
                queueWriter.Flush();
                queue->NotifyInputFinished(i);
            });
        }
        TFmrRawTableQueueReader reader{queue};
        TString result = reader.ReadAll();
        std::vector<TString> splittedRows;
        StringSplitter(result).Split(rowEnd).AddTo(&splittedRows);
        std::unordered_map<TString, ui64> gottenRows;
        for (auto& row: splittedRows) {
            if (!row.empty()) {
                ++gottenRows[row];
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(gottenRows.size(), TableContentRows.size());
        for (auto& row: TableContentRows) {
            UNIT_ASSERT_VALUES_EQUAL(gottenRows[row], inputStreamsNum * repeatsNum);
        }
    }
}

} // namespace NYql::NFmr
