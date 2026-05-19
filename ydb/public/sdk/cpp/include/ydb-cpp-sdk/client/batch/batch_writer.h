#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/trace/trace.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace NYdb::inline Dev {

namespace NTable {
class TTableClient;
} // namespace NTable

namespace NQuery {
class TQueryClient;
} // namespace NQuery

namespace NBatch {

// Backpressure policy applied when the in-process queue of pending rows
// reaches MaxQueueRows / MaxQueueBytes.
enum class EOverflowPolicy {
    // Append() blocks until the writer drains enough capacity.
    Block,

    // Append() returns immediately, the new row is rejected and counted
    // as ydb_batch_writer_rows_total{result="dropped"}.
    DropNewest,

    // The oldest queued row is dropped to make room. Counted under
    // ydb_batch_writer_rows_total{result="dropped"} as well.
    DropOldest,
};

// What caused the writer to start a flush. Surfaced as the "trigger"
// label on ydb_batch_writer_flushes_total and as an attribute on the
// BatchWriter.Flush span.
enum class EFlushTrigger {
    // MaxBatchRows reached.
    Size,

    // MaxBatchBytes reached.
    Bytes,

    // FlushInterval timer fired.
    Interval,

    // Manual Flush() call.
    Manual,

    // Close() / destructor draining the remaining rows.
    Shutdown,
};

class IBatchSink;

// Settings for TBatchWriter. All thresholds are independent and the
// writer flushes as soon as ANY of them is reached.
struct TBatchWriterSettings {
    using TSelf = TBatchWriterSettings;

    // Soft upper bound on the number of rows in a single flushed batch.
    FLUENT_SETTING_DEFAULT(std::size_t, MaxBatchRows, 1000);

    // Soft upper bound on the serialized size of a single flushed batch.
    FLUENT_SETTING_DEFAULT(std::size_t, MaxBatchBytes, 1u << 20);

    // Maximum time a row may sit in the queue before a flush is
    // triggered, even if the size thresholds are not yet reached.
    FLUENT_SETTING_DEFAULT(TDuration, FlushInterval, TDuration::MilliSeconds(500));

    // Hard upper bound on the queued (not-yet-flushed) row count.
    FLUENT_SETTING_DEFAULT(std::size_t, MaxQueueRows, 100'000);

    // Hard upper bound on the queued (not-yet-flushed) byte volume.
    FLUENT_SETTING_DEFAULT(std::size_t, MaxQueueBytes, 64u << 20);

    // Number of flush operations that may be in flight in parallel.
    // Concurrency=1 means strict ordering of flushes (but not of
    // individual rows within one flush, since BulkUpsert is unordered).
    FLUENT_SETTING_DEFAULT(std::size_t, Concurrency, 1);

    FLUENT_SETTING_DEFAULT(EOverflowPolicy, OverflowPolicy, EOverflowPolicy::Block);

    // Retry policy for an individual flush. See NRetry::TRetryOperationSettings.
    // The writer applies this on every flush; rows are not lost on
    // retryable failures, they remain owned by the in-flight batch.
    FLUENT_SETTING_DEFAULT(NRetry::TRetryOperationSettings, RetrySettings, {});

    // Optional observability hooks. When not null, the writer emits the
    // metrics / spans documented on TBatchWriter. Pass the same handles
    // that were given to TDriverConfig::SetMetricRegistry /
    // SetTraceProvider for a single coherent observability surface.
    FLUENT_SETTING(std::shared_ptr<NMetrics::IMetricRegistry>, MetricRegistry);
    FLUENT_SETTING(std::shared_ptr<NTrace::ITraceProvider>,    TraceProvider);

    // Free-form name appended as ydb.batch.writer.name to all metrics
    // and spans. When empty, the writer derives one from the table.
    FLUENT_SETTING(std::string, Name);
};

// TBatchWriter is a high-throughput, asynchronous client-side batcher
// for write workloads against YDB. It accepts rows from any number of
// producer threads, accumulates them in an in-process queue and flushes
// them in pre-configured batches via a pluggable IBatchSink (BulkUpsert
// or Query Service).
//
// Observability:
// * tracing through ITracer (configured on the parent TDriver):
//     BatchWriter.Flush  (INTERNAL, attributes: batch.rows, batch.bytes,
//                         batch.trigger, batch.attempt)
//       └── BulkUpsert / ExecuteQuery  (CLIENT, emitted by the sink)
//
// * metrics through IMetricRegistry (configured on the parent TDriver):
//     ydb_batch_writer_queue_depth_rows                    (Gauge)
//     ydb_batch_writer_queue_depth_bytes                   (Gauge)
//     ydb_batch_writer_inflight_flushes                    (Gauge)
//     ydb_batch_writer_batch_size_rows                     (Histogram)
//     ydb_batch_writer_batch_size_bytes                    (Histogram)
//     ydb_batch_writer_flush_duration_seconds              (Histogram)
//     ydb_batch_writer_flushes_total{trigger=...}          (Counter)
//     ydb_batch_writer_rows_total{result=ok|failed|dropped}(Counter)
//
// All metrics are tagged with the parent driver's standard
// db.system.name / db.namespace / server.address / server.port labels
// and additionally with ydb.batch.writer.target = <table>.
class TBatchWriter {
public:
    // Build a writer using TTableClient::BulkUpsert as the sink.
    // This is the recommended high-throughput path: no transactions,
    // no SQL parsing, no session pool involvement.
    TBatchWriter(NTable::TTableClient& client,
                 std::string_view table,
                 const TBatchWriterSettings& settings = {});

    // Build a writer that uses TQueryClient (Query Service / YQL).
    // Slower per-RPC than the BulkUpsert variant, but supports tables
    // with secondary indexes and arbitrary post-processing.
    TBatchWriter(NQuery::TQueryClient& client,
                 std::string_view table,
                 const TBatchWriterSettings& settings = {});

    // Build a writer with a user-provided sink. Mostly useful for tests
    // (a fake sink) and for plugging in alternative write paths.
    TBatchWriter(std::shared_ptr<IBatchSink> sink,
                 const TBatchWriterSettings& settings = {});

    ~TBatchWriter();

    TBatchWriter(const TBatchWriter&) = delete;
    TBatchWriter& operator=(const TBatchWriter&) = delete;

    // Append a single row built externally with TValueBuilder.
    // The row must be a List<Struct<...>> ELEMENT (i.e. one Struct).
    // The returned future resolves with the status of the flush that
    // actually carried this row, including any retries.
    NThreading::TFuture<TStatus> Append(TValue row, std::size_t sizeHint = 0);

    // Append many rows at once. They are appended atomically with
    // respect to backpressure: either all of them are accepted (the
    // future will eventually resolve with the result of one or more
    // flushes), or, on Drop policies, any subset may be rejected.
    NThreading::TFuture<TStatus> Append(std::vector<TValue> rows);

    // Force-flush whatever is currently queued. Returns a future that
    // resolves once the resulting batch has been delivered (or finally
    // failed after retries).
    NThreading::TFuture<TStatus> Flush();

    // Stop accepting new rows and drain the queue. After this returns,
    // no more producer side-effects are possible. Called automatically
    // by the destructor; explicit Close() is recommended so that errors
    // from the final flush can be observed.
    NThreading::TFuture<void> Close();

    // Diagnostic accessors. These are eventually-consistent snapshots
    // taken under the writer's internal lock.
    std::size_t QueuedRows() const noexcept;
    std::size_t QueuedBytes() const noexcept;
    std::size_t InFlightFlushes() const noexcept;

    std::string_view Table() const noexcept;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

// Pluggable sink interface — implemented by TBulkUpsertSink and
// TQuerySink, plus by test mocks.
class IBatchSink {
public:
    virtual ~IBatchSink() = default;

    struct TFlushRequest {
        // The sink owns and serializes these rows. The implementation
        // for TBulkUpsertSink wraps them into a List<Struct<...>>
        // TValue and calls TTableClient::BulkUpsert.
        std::vector<TValue> Rows;
        std::size_t TotalBytes = 0;
        EFlushTrigger Trigger = EFlushTrigger::Size;
        std::size_t Attempt = 0;  // 0 = first attempt, increases on retry
    };

    // Carry out a single batch write. The future must resolve once the
    // sink has either succeeded or finally given up. The writer applies
    // its retry policy on top of this, so the sink itself should NOT
    // retry internally.
    virtual NThreading::TFuture<TStatus> Flush(TFlushRequest request) = 0;

    // Identifies the sink in metrics labels (e.g. "bulk_upsert", "query").
    virtual std::string Kind() const = 0;
};

} // namespace NBatch
} // namespace NYdb
