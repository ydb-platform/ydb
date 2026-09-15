#include <library/cpp/testing/unittest/registar.h>
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_raw_table_queue_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_queue_writer_with_lock.h>

namespace NYql::NFmr {

namespace {

// Mimics TFmrUserJob::FillQueueFromInputTablesOrdered: writes rowContent (possibly
// empty) into the shared queue under TableId_'s turn, then hands the turn to the
// next table, exactly like the real per-table worker threads do.
void WriteTableOrdered(
    TFmrRawTableQueue::TPtr queue,
    ui64 tableId,
    std::shared_ptr<TOrderedWriteState> state,
    const TString& rowContent
) {
    auto writer = MakeIntrusive<TFmrRawTableQueueWriterWithLock>(queue, tableId, state);
    if (!rowContent.empty()) {
        writer->Write(rowContent);
        writer->NotifyRowEnd();
    }
    writer->Flush();
    with_lock(state->Mutex) {
        state->NextToEmit++;
        state->CondVar.BroadCast();
    }
    queue->NotifyInputFinished(tableId);
}

} // namespace

Y_UNIT_TEST_SUITE(FmrRawTableQueueWriterWithLockTests) {
    Y_UNIT_TEST(EmptyTableDoesNotJumpTheQueue) {
        ui64 inputStreamsNum = 3;
        auto queue = MakeIntrusive<TFmrRawTableQueue>(inputStreamsNum);
        auto state = std::make_shared<TOrderedWriteState>();

        auto threadPool = CreateThreadPool(inputStreamsNum, 100, TThreadPool::TParams().SetBlocking(true).SetCatching(true));

        // Table 0 is deliberately slow, table 1 is empty and table 2 is fast, so an
        // out-of-turn flush of the empty table (or an eagerly-finishing table 2)
        // would previously desync NextToEmit and deadlock table 0 forever.
        threadPool->SafeAddFunc([queue, state] {
            Sleep(TDuration::MilliSeconds(200));
            WriteTableOrdered(queue, 0, state, "table0;");
        });
        threadPool->SafeAddFunc([queue, state] {
            WriteTableOrdered(queue, 1, state, "");
        });
        threadPool->SafeAddFunc([queue, state] {
            WriteTableOrdered(queue, 2, state, "table2;");
        });

        TFmrRawTableQueueReader reader{queue};
        TString result = reader.ReadAll();
        UNIT_ASSERT_VALUES_EQUAL(result, "table0;table2;");
    }
}

} // namespace NYql::NFmr
