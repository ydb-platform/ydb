#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <yt/yt/core/test_framework/framework.h>

#include <thread>
#include <atomic>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow MakeRow(i64 value)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(value, /*id*/ 0));
    return builder.FinishRow();
}

TUnversionedOwningRow MakeBigRow(const std::string& payload)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue(payload, /*id*/ 0));
    return builder.FinishRow();
}

// Drain all rows from the reader, waiting on GetReadyEvent() when needed.
// Returns all collected Int64 values from column 0.
std::vector<i64> DrainReaderInt64(const ISchemafulUnversionedReaderPtr& reader)
{
    std::vector<i64> result;
    TRowBatchReadOptions options;
    options.MaxRowsPerRead = 1000;

    while (true) {
        auto batch = reader->Read(options);
        if (!batch) {
            break;
        }
        auto rows = batch->MaterializeRows();
        if (rows.empty()) {
            auto waitResult = reader->GetReadyEvent().BlockingGet();
            EXPECT_TRUE(waitResult.IsOK()) << waitResult.GetMessage();
            continue;
        }
        for (auto row : rows) {
            result.push_back(row[0].Data.Int64);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TPipeTest
    : public ::testing::Test
{
protected:
    ISchemafulPipePtr CreatePipe()
    {
        return CreateSchemafulPipe(GetDefaultMemoryChunkProvider());
    }

    ISchemafulPipePtr CreatePipeWithBackpressure(int maxFlushBatchCount = 16)
    {
        return CreateSchemafulPipe(GetDefaultMemoryChunkProvider(), maxFlushBatchCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPipeTest, BasicWriteRead)
{
    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> rows = {
        rowBuffer->CaptureRow(MakeRow(1).Get()),
        rowBuffer->CaptureRow(MakeRow(2).Get()),
        rowBuffer->CaptureRow(MakeRow(3).Get()),
    };
    EXPECT_TRUE(writer->Write(rows));
    EXPECT_TRUE(writer->Close().BlockingGet().IsOK());

    auto result = DrainReaderInt64(reader);
    ASSERT_EQ(result, (std::vector<i64>{1, 2, 3}));
}

TEST_F(TPipeTest, EmptyWrite)
{
    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    EXPECT_TRUE(writer->Write(TRange<TUnversionedRow>{}));
    EXPECT_TRUE(writer->Close().BlockingGet().IsOK());

    auto result = DrainReaderInt64(reader);
    EXPECT_TRUE(result.empty());
}

TEST_F(TPipeTest, CloseWithNoData)
{
    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    EXPECT_TRUE(writer->Close().BlockingGet().IsOK());

    TRowBatchReadOptions options;
    auto batch = reader->Read(options);
    EXPECT_EQ(batch, nullptr);
}

TEST_F(TPipeTest, InitialReadyEventIsOK)
{
    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();
    EXPECT_TRUE(reader->GetReadyEvent().IsSet());
    EXPECT_TRUE(reader->GetReadyEvent().GetOrCrash().IsOK());
}

TEST_F(TPipeTest, InterleavedWriteRead)
{
    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    auto rowBuffer = New<TRowBuffer>();
    std::vector<i64> expected;

    for (int i = 0; i < 5; ++i) {
        auto row = rowBuffer->CaptureRow(MakeRow(i).Get());
        std::vector<TUnversionedRow> rows = {row};
        EXPECT_TRUE(writer->Write(rows));
        expected.push_back(i);
    }

    EXPECT_TRUE(writer->Close().BlockingGet().IsOK());

    auto result = DrainReaderInt64(reader);
    ASSERT_EQ(result, expected);
}

TEST_F(TPipeTest, ReadBeforeWrite)
{
    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    TRowBatchReadOptions options;
    options.MaxRowsPerRead = 100;

    // No data yet — should return empty non-null batch.
    auto batch = reader->Read(options);
    ASSERT_NE(batch, nullptr);
    EXPECT_TRUE(batch->MaterializeRows().empty());

    // Capture the ready future before writing.
    auto readyFuture = reader->GetReadyEvent();
    EXPECT_FALSE(readyFuture.IsSet());

    // Write one row from another thread — this must resolve the ready future.
    std::thread writerThread([&] {
        auto rowBuffer = New<TRowBuffer>();
        auto row = rowBuffer->CaptureRow(MakeRow(99).Get());
        std::vector<TUnversionedRow> rows = {row};
        EXPECT_TRUE(writer->Write(rows));
        EXPECT_TRUE(writer->Close().BlockingGet().IsOK());
    });

    EXPECT_TRUE(readyFuture.BlockingGet().IsOK());

    writerThread.join();

    auto result = DrainReaderInt64(reader);
    ASSERT_EQ(result, (std::vector<i64>{99}));
}

// Fail() before any Read(): reader gets empty batch with error on GetReadyEvent().
TEST_F(TPipeTest, FailPropagationBeforeRead)
{
    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();

    pipe->Fail(TError("test error"));

    TRowBatchReadOptions options;
    auto batch = reader->Read(options);
    // Non-null empty batch — caller must wait on GetReadyEvent().
    ASSERT_NE(batch, nullptr);
    EXPECT_TRUE(batch->MaterializeRows().empty());

    auto eventResult = reader->GetReadyEvent().BlockingGet();
    EXPECT_FALSE(eventResult.IsOK());
    EXPECT_THAT(eventResult.GetMessage(), ::testing::HasSubstr("test error"));
}

// Fail() while writer is mid-stream: Write() returns false.
TEST_F(TPipeTest, FailMakesWriteReturnFalse)
{
    auto pipe = CreatePipe();
    auto writer = pipe->GetWriter();

    pipe->Fail(TError("injected failure"));

    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->CaptureRow(MakeRow(42).Get());
    std::vector<TUnversionedRow> rows = {row};
    EXPECT_FALSE(writer->Write(rows));
}

// Fail() propagates error through writer's GetReadyEvent().
TEST_F(TPipeTest, FailPropagatesWriterReadyEvent)
{
    auto pipe = CreatePipe();
    auto writer = pipe->GetWriter();

    pipe->Fail(TError("writer error"));

    auto result = writer->GetReadyEvent().BlockingGet();
    EXPECT_FALSE(result.IsOK());
    EXPECT_THAT(result.GetMessage(), ::testing::HasSubstr("writer error"));
}

// Fail() is idempotent — calling it twice must not crash.
TEST_F(TPipeTest, FailIsIdempotent)
{
    auto pipe = CreatePipe();
    pipe->Fail(TError("first"));
    pipe->Fail(TError("second")); // must not crash or assert
}

TEST_F(TPipeTest, BackpressureWriterBlocked)
{
    auto pipe = CreatePipeWithBackpressure();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    // Each write > FlushDataWeight forces a flush and pushes one entry to FlushedRowsetQueue.
    // After 17 such writes the queue exceeds MaxFlushedRowsetQueueSize=16.
    constexpr size_t RowDataSize = FlushDataWeight + 1;
    std::string bigPayload(RowDataSize, 'x');

    auto rowBuffer = New<TRowBuffer>();
    bool writerBlocked = false;
    int writesBeforeBlock = 0;

    for (int i = 0; i < 20; ++i) {
        auto bigRow = rowBuffer->CaptureRow(MakeBigRow(bigPayload).Get());
        std::vector<TUnversionedRow> rows = {bigRow};
        bool canContinue = writer->Write(rows);
        if (!canContinue) {
            writerBlocked = true;
            writesBeforeBlock = i + 1;
            break;
        }
    }

    EXPECT_TRUE(writerBlocked) << "Writer should have been blocked after >16 flushes";
    EXPECT_GT(writesBeforeBlock, 16);

    // GetReadyEvent() must return an unresolved future.
    auto writerReady = writer->GetReadyEvent();
    EXPECT_FALSE(writerReady.IsSet());

    // Drain the reader — this should unblock the writer.
    TRowBatchReadOptions options;
    options.MaxRowsPerRead = 1000;
    int rowsRead = 0;
    while (!writerReady.IsSet()) {
        auto batch = reader->Read(options);
        ASSERT_NE(batch, nullptr);
        auto rows = batch->MaterializeRows();
        if (rows.empty()) {
            ASSERT_TRUE(reader->GetReadyEvent().BlockingGet().IsOK());
            continue;
        }
        rowsRead += std::ssize(rows);
    }

    EXPECT_GT(rowsRead, 0);
    EXPECT_TRUE(writerReady.BlockingGet().IsOK());
}

// Backpressure: writer blocked, then Fail() unblocks both sides with error.
TEST_F(TPipeTest, BackpressureFailUnblocksBoth)
{
    auto pipe = CreatePipeWithBackpressure();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    constexpr size_t RowDataSize = FlushDataWeight + 1;
    std::string bigPayload(RowDataSize, 'x');
    auto rowBuffer = New<TRowBuffer>();

    bool writerBlocked = false;
    for (int i = 0; i < 20; ++i) {
        auto bigRow = rowBuffer->CaptureRow(MakeBigRow(bigPayload).Get());
        std::vector<TUnversionedRow> rows = {bigRow};
        if (!writer->Write(rows)) {
            writerBlocked = true;
            break;
        }
    }
    ASSERT_TRUE(writerBlocked);

    auto writerReady = writer->GetReadyEvent();
    EXPECT_FALSE(writerReady.IsSet());

    // Fail the pipe — both sides must be unblocked with an error.
    pipe->Fail(TError("forced failure"));

    EXPECT_FALSE(writerReady.BlockingGet().IsOK());

    // Reader should also get the error.
    TRowBatchReadOptions options;
    auto batch = reader->Read(options);
    ASSERT_NE(batch, nullptr);
    EXPECT_TRUE(batch->MaterializeRows().empty());
    EXPECT_FALSE(reader->GetReadyEvent().BlockingGet().IsOK());
}

// Backpressure: reader drains everything before writer calls GetReadyEvent().
// GetReadyEvent() must return an already-set OK future (no hang).
TEST_F(TPipeTest, BackpressureReaderDrainsBeforeGetReadyEvent)
{
    auto pipe = CreatePipeWithBackpressure();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    constexpr size_t RowDataSize = FlushDataWeight + 1;
    std::string bigPayload(RowDataSize, 'x');
    auto rowBuffer = New<TRowBuffer>();

    bool writerBlocked = false;
    for (int i = 0; i < 20; ++i) {
        auto bigRow = rowBuffer->CaptureRow(MakeBigRow(bigPayload).Get());
        std::vector<TUnversionedRow> rows = {bigRow};
        if (!writer->Write(rows)) {
            writerBlocked = true;
            break;
        }
    }
    ASSERT_TRUE(writerBlocked);

    // Drain the reader completely BEFORE calling writer->GetReadyEvent().
    TRowBatchReadOptions options;
    options.MaxRowsPerRead = 1000;
    // We know exactly how many rowsets are in the queue (writesBeforeBlock rows,
    // each flushed). Just drain until the queue is empty.
    bool drained = false;
    for (int attempt = 0; attempt < 1000 && !drained; ++attempt) {
        auto batch = reader->Read(options);
        ASSERT_NE(batch, nullptr);
        auto rows = batch->MaterializeRows();
        if (rows.empty()) {
            // No more data available right now — check if writer is already unblocked.
            auto writerReady = writer->GetReadyEvent();
            if (writerReady.IsSet()) {
                EXPECT_TRUE(writerReady.GetOrCrash().IsOK());
                drained = true;
            } else {
                // Still blocked — wait for reader event (writer may push more data).
                // Since writer is blocked, this means we need to keep draining.
                // The reader event is unresolved because no new data is coming.
                // This means the queue is empty but writer hasn't been unblocked yet —
                // which would be a bug. Break to avoid infinite loop.
                break;
            }
        }
    }

    // Now call GetReadyEvent() — must be already set and OK.
    auto writerReady = writer->GetReadyEvent();
    EXPECT_TRUE(writerReady.IsSet());
    EXPECT_TRUE(writerReady.GetOrCrash().IsOK());
}

TEST_F(TPipeTest, StressConcurrentWriteRead)
{
    constexpr int TotalRows = 100'000;
    constexpr int BatchSize = 64;

    auto pipe = CreatePipe();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    std::vector<i64> received;
    received.reserve(TotalRows);

    std::thread readerThread([&] {
        TRowBatchReadOptions options;
        options.MaxRowsPerRead = BatchSize * 2;

        while (true) {
            auto batch = reader->Read(options);
            if (!batch) {
                break;
            }
            auto rows = batch->MaterializeRows();
            if (rows.empty()) {
                auto result = reader->GetReadyEvent().BlockingGet();
                ASSERT_TRUE(result.IsOK()) << result.GetMessage();
                continue;
            }
            for (auto row : rows) {
                received.push_back(row[0].Data.Int64);
            }
        }
    });

    std::thread writerThread([&] {
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> batch;
        batch.reserve(BatchSize);

        for (int i = 0; i < TotalRows; i += BatchSize) {
            batch.clear();
            rowBuffer->Clear();
            int end = std::min(i + BatchSize, TotalRows);
            for (int j = i; j < end; ++j) {
                batch.push_back(rowBuffer->CaptureRow(MakeRow(j).Get()));
            }
            bool canContinue = writer->Write(batch);
            if (!canContinue) {
                // Backpressure: rows were already written, just wait before next Write().
                auto result = writer->GetReadyEvent().BlockingGet();
                ASSERT_TRUE(result.IsOK()) << result.GetMessage();
            }
        }
        ASSERT_TRUE(writer->Close().BlockingGet().IsOK());
    });

    writerThread.join();
    readerThread.join();

    ASSERT_EQ(std::ssize(received), TotalRows);
    for (int i = 0; i < TotalRows; ++i) {
        ASSERT_EQ(received[i], i) << "Mismatch at index " << i;
    }
}

// Stress: writer writes large rowsets to trigger backpressure repeatedly,
// reader drains concurrently. Verifies no deadlock and correct row count.
TEST_F(TPipeTest, StressBackpressureConcurrent)
{
    constexpr int NumWrites = 50;
    constexpr size_t RowDataSize = FlushDataWeight + 1;

    auto pipe = CreatePipeWithBackpressure();
    auto reader = pipe->GetReader();
    auto writer = pipe->GetWriter();

    std::atomic<int> totalRead{0};

    std::thread readerThread([&] {
        TRowBatchReadOptions options;
        options.MaxRowsPerRead = 1000;

        while (true) {
            auto batch = reader->Read(options);
            if (!batch) {
                break;
            }
            auto rows = batch->MaterializeRows();
            if (rows.empty()) {
                auto result = reader->GetReadyEvent().BlockingGet();
                ASSERT_TRUE(result.IsOK()) << result.GetMessage();
                continue;
            }
            totalRead.fetch_add(std::ssize(rows), std::memory_order_relaxed);
        }
    });

    std::thread writerThread([&] {
        std::string bigPayload(RowDataSize, 'y');
        auto rowBuffer = New<TRowBuffer>();

        for (int i = 0; i < NumWrites; ++i) {
            auto bigRow = rowBuffer->CaptureRow(MakeBigRow(bigPayload).Get());
            std::vector<TUnversionedRow> rows = {bigRow};
            bool canContinue = writer->Write(rows);
            if (!canContinue) {
                auto result = writer->GetReadyEvent().BlockingGet();
                ASSERT_TRUE(result.IsOK()) << result.GetMessage();
            }
        }
        ASSERT_TRUE(writer->Close().BlockingGet().IsOK());
    });

    writerThread.join();
    readerThread.join();

    EXPECT_EQ(totalRead.load(), NumWrites);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
