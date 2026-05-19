#include "observability.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <util/string/cast.h>

namespace NYdb::inline Dev::NBatch {

namespace {

// Histogram buckets for batch size in rows (1, 10, ..., 100k). Picked
// to span the whole range of useful MaxBatchRows values.
const std::vector<double> kBatchSizeRowsBuckets = {
    1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000,
    10000, 25000, 50000, 100000};

// Histogram buckets for batch size in bytes (~64 B .. 64 MiB).
const std::vector<double> kBatchSizeBytesBuckets = {
    64,        256,        1024,       4096,       16384,
    65536,     262144,     1048576,    4194304,    16777216,
    67108864};

// Histogram buckets for flush duration in seconds (covers 100 us .. 60 s).
const std::vector<double> kFlushDurationBuckets = {
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05,
    0.1,    0.25,   0.5,   1.0,   2.5,  5.0,
    10.0,   30.0,   60.0};

} // namespace

TBatchObservability::TBatchObservability(
    std::shared_ptr<NMetrics::IMetricRegistry> registry,
    std::shared_ptr<NTrace::ITraceProvider> traceProvider,
    std::string_view writerName,
    std::string_view sinkKind,
    std::string_view table)
    : Registry_(std::move(registry))
    , Name_(writerName)
    , SinkKind_(sinkKind)
    , Table_(table)
{
    if (traceProvider) {
        Tracer_ = traceProvider->GetTracer("ydb-cpp-sdk.batch");
    }

    if (!Registry_) {
        return;
    }

    const NMetrics::TLabels base = {
        {"db.system.name",         "ydb"},
        {"ydb.batch.writer.name",  Name_},
        {"ydb.batch.writer.sink",  SinkKind_},
        {"ydb.batch.writer.table", Table_},
    };

    auto withTrigger = [&](std::string_view trigger) {
        auto labels = base;
        labels["trigger"] = trigger;
        return labels;
    };
    auto withResult = [&](std::string_view result) {
        auto labels = base;
        labels["result"] = result;
        return labels;
    };

    RowsOk_      = Registry_->Counter("ydb_batch_writer_rows_total", withResult("ok"),
        "Number of rows successfully delivered by the batch writer.", "1");
    RowsFailed_  = Registry_->Counter("ydb_batch_writer_rows_total", withResult("failed"),
        "Number of rows that ended up in a failed flush after all retries.", "1");
    RowsDropped_ = Registry_->Counter("ydb_batch_writer_rows_total", withResult("dropped"),
        "Number of rows rejected by the writer due to backpressure.", "1");

    QueueDepthRows_  = Registry_->Gauge("ydb_batch_writer_queue_depth_rows",  base,
        "Current number of rows queued for flushing.", "1");
    QueueDepthBytes_ = Registry_->Gauge("ydb_batch_writer_queue_depth_bytes", base,
        "Current size of the queued payload, bytes.", "By");
    InflightFlushes_ = Registry_->Gauge("ydb_batch_writer_inflight_flushes", base,
        "Number of flushes currently being executed by the sink.", "1");

    BatchSizeRows_ = Registry_->Histogram(
        "ydb_batch_writer_batch_size_rows", kBatchSizeRowsBuckets, base,
        "Distribution of batch sizes (rows).", "1");
    BatchSizeBytes_ = Registry_->Histogram(
        "ydb_batch_writer_batch_size_bytes", kBatchSizeBytesBuckets, base,
        "Distribution of batch sizes (bytes).", "By");
    FlushDuration_ = Registry_->Histogram(
        "ydb_batch_writer_flush_duration_seconds", kFlushDurationBuckets, base,
        "Wall-clock duration of a single flush, including retries.", "s");

    FlushSize_     = Registry_->Counter("ydb_batch_writer_flushes_total",
        withTrigger("size"),     "Flushes triggered by MaxBatchRows.",     "1");
    FlushBytes_    = Registry_->Counter("ydb_batch_writer_flushes_total",
        withTrigger("bytes"),    "Flushes triggered by MaxBatchBytes.",    "1");
    FlushInterval_ = Registry_->Counter("ydb_batch_writer_flushes_total",
        withTrigger("interval"), "Flushes triggered by FlushInterval.",    "1");
    FlushManual_   = Registry_->Counter("ydb_batch_writer_flushes_total",
        withTrigger("manual"),   "Flushes triggered by manual Flush().",   "1");
    FlushShutdown_ = Registry_->Counter("ydb_batch_writer_flushes_total",
        withTrigger("shutdown"), "Flushes triggered by Close()/destructor.", "1");
}

void TBatchObservability::OnRowAccepted(std::size_t /*bytes*/) noexcept {
    // We deliberately update the per-row "ok" counter only at flush time:
    // a row is not yet "delivered" when accepted, only queued. The
    // queue_depth gauge is updated separately by SetQueueDepth().
}

void TBatchObservability::OnRowDropped(std::size_t /*bytes*/) noexcept {
    if (RowsDropped_) {
        RowsDropped_->Inc();
    }
}

void TBatchObservability::OnFlushStarted(std::size_t /*rows*/, std::size_t /*bytes*/) noexcept {
    if (InflightFlushes_) {
        InflightFlushes_->Add(1.0);
    }
}

void TBatchObservability::OnFlushFinished(EFlushTrigger trigger,
                                          std::size_t rows,
                                          std::size_t bytes,
                                          std::chrono::nanoseconds duration,
                                          bool success) noexcept
{
    if (InflightFlushes_) {
        InflightFlushes_->Add(-1.0);
    }
    if (BatchSizeRows_) {
        BatchSizeRows_->Record(static_cast<double>(rows));
    }
    if (BatchSizeBytes_) {
        BatchSizeBytes_->Record(static_cast<double>(bytes));
    }
    if (FlushDuration_) {
        const double seconds =
            std::chrono::duration<double>(duration).count();
        FlushDuration_->Record(seconds);
    }
    if (auto counter = AcquireFlushTrigger(trigger)) {
        counter->Inc();
    }

    auto rowsCounter = success ? RowsOk_ : RowsFailed_;
    if (rowsCounter && rows > 0) {
        // The metric API only exposes Inc(); do row-by-row in a tight
        // loop. This is on the slow path (post-flush) so it's fine.
        for (std::size_t i = 0; i < rows; ++i) {
            rowsCounter->Inc();
        }
    }
}

void TBatchObservability::SetQueueDepth(std::size_t rows, std::size_t bytes) noexcept {
    if (QueueDepthRows_) {
        QueueDepthRows_->Set(static_cast<double>(rows));
    }
    if (QueueDepthBytes_) {
        QueueDepthBytes_->Set(static_cast<double>(bytes));
    }
}

std::shared_ptr<NMetrics::ICounter> TBatchObservability::AcquireFlushTrigger(EFlushTrigger trigger) {
    switch (trigger) {
        case EFlushTrigger::Size:     return FlushSize_;
        case EFlushTrigger::Bytes:    return FlushBytes_;
        case EFlushTrigger::Interval: return FlushInterval_;
        case EFlushTrigger::Manual:   return FlushManual_;
        case EFlushTrigger::Shutdown: return FlushShutdown_;
    }
    return nullptr;
}

std::shared_ptr<NTrace::ISpan> TBatchObservability::StartFlushSpan(
    EFlushTrigger trigger,
    std::size_t rows,
    std::size_t bytes,
    std::size_t attempt) noexcept
{
    if (!Tracer_) {
        return nullptr;
    }

    auto span = Tracer_->StartSpan("BatchWriter.Flush", NTrace::ESpanKind::INTERNAL);
    if (!span) {
        return nullptr;
    }

    span->SetAttribute("ydb.batch.writer.name",  Name_);
    span->SetAttribute("ydb.batch.writer.sink",  SinkKind_);
    span->SetAttribute("ydb.batch.writer.table", Table_);
    span->SetAttribute("batch.trigger",          FlushTriggerToString(trigger));
    span->SetAttribute("batch.rows",             static_cast<int64_t>(rows));
    span->SetAttribute("batch.bytes",            static_cast<int64_t>(bytes));
    if (attempt > 0) {
        span->SetAttribute("batch.attempt",      static_cast<int64_t>(attempt));
    }
    return span;
}

void TBatchObservability::FinishFlushSpan(const std::shared_ptr<NTrace::ISpan>& span,
                                          const TStatus& status) noexcept
{
    if (!span) {
        return;
    }
    if (status.IsSuccess()) {
        span->SetStatus(NTrace::ESpanStatus::Ok);
    } else {
        std::string desc = "status=";
        desc += ::ToString(status.GetStatus());
        span->SetStatus(NTrace::ESpanStatus::Error, desc);
    }
    span->End();
}

} // namespace NYdb::NBatch
