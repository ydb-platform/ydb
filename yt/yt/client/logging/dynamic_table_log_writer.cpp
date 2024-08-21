#include "dynamic_table_log_writer.h"

#include "config.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log_writer_detail.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/formatter.h>
#include <yt/yt/core/logging/system_log_event_provider.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <util/stream/length.h>

namespace NYT::NLogging {

using namespace NConcurrency;
using namespace NFormats;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// This log category is special in the default log manager: events with this
// category are logged solely to stderr.
// Note that this behaviour does not depend on the log level of the message.
// We use this logger to (try our best) to avoid perpetual log message production.
// Currently it is impossible to disable logging along a chain of invocations completely,
// so some induced log messages will loop back into our writer.
YT_DEFINE_GLOBAL(const NLogging::TLogger, SystemLogger, SystemLoggingCategoryName);

////////////////////////////////////////////////////////////////////////////////

struct TClientHolder final
{
    TAtomicIntrusivePtr<NApi::IClient> Client;
};

using TClientHolderPtr = TIntrusivePtr<TClientHolder>;

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableLogWriter
    : public TRateLimitingLogWriterBase
{
public:
    TDynamicTableLogWriter(
        TClientHolderPtr clientHolder,
        std::unique_ptr<ILogFormatter> formatter,
        std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
        const TDynamicTableLogWriterConfigPtr& config,
        const TString& name,
        IInvokerPtr invoker)
        : TRateLimitingLogWriterBase(
            std::move(systemEventProvider),
            name,
            config)
        , ClientHolder_(std::move(clientHolder))
        , Formatter_(std::move(formatter))
        , Config_(config)
        , Invoker_(std::move(invoker))
        , FlushExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TDynamicTableLogWriter::DoFlush, MakeWeak(this)),
            Config_->FlushPeriod))
        , Logger(SystemLogger().WithTag(
            "LogWriterName: %v, TablePath: %v",
            name,
            Config_->TablePath))
        , Profiler_(TProfiler("/dynamic_table_logging")
            .WithSparse()
            .WithTag("writer", name)
            .WithTag("table_path", Config_->TablePath))
        , DroppedEventsCounter_(Profiler_.Counter("/dropped_events"))
        , FlushedEventsCounter_(Profiler_.Counter("/flushed_events"))
        , FlushedBytesCounter_(Profiler_.Counter("/flushed_bytes"))
    {
        FlushExecutor_->Start();

        YT_LOG_INFO("Created dynamic table log writer (Config: %v)", ConvertToYsonString(Config_, EYsonFormat::Text));

        Profiler_.AddFuncGauge("/backlog_events", MakeStrong(this), [this] {
            return BacklogEventCount_.load(std::memory_order::relaxed);
        });
        Profiler_.AddFuncGauge("/backlog_weight", MakeStrong(this), [this] {
            return BacklogWeight_.load(std::memory_order::relaxed);
        });
    }

    // This method is called *extremely* often: once every 40ms.
    // We intentionally do not trigger any flush actions, because they are very likely to
    // take longer than 40ms to execute.
    // Using a periodic executor for flushes is debatable. Alternatively, we could have
    // implemented an infinite loop of flush iterations. The current solution seems better
    // from a batching perspective, particularly in cases when the logging rate is moderate.
    void Flush() override
    { }

    void Reload() override
    { }

private:
    const TClientHolderPtr ClientHolder_;
    const std::unique_ptr<ILogFormatter> Formatter_;
    const TDynamicTableLogWriterConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr FlushExecutor_;
    const TLogger Logger;

    const TProfiler Profiler_;

    TCounter DroppedEventsCounter_;
    TCounter FlushedEventsCounter_;
    TCounter FlushedBytesCounter_;

    DECLARE_THREAD_AFFINITY_SLOT(LoggingThread);

    //! Protects the fields below.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TString CurrentBuffer_;
    i64 CurrentRowCount_ = 0;
    //! This queue should typically contain no more than one element.
    //! We use it to limit the number of rows written within a single transaction.
    struct TBuffer
    {
        TSharedRef YsonRows;
        i64 RowCount;
    };
    std::deque<TBuffer> BufferQueue_;

    //! Only accessed in the logging thread.
    bool Suspended_ = false;

    //! Stored in terms of YSON bytes.
    std::atomic<i64> BacklogWeight_ = 0;
    std::atomic<i64> BacklogEventCount_ = 0;

    struct TDynamicTableLogWriterBufferTag
    { };

    // No synchronous YT writes/calls are actually performed by Write/Flush, since we cannot
    // switch contexts in log manager's main logging thread.
    // Moreover, the current log manager is not designed for handling asynchronous event
    // writers. All write/flush calls are made consecutively and are considered successful
    // at once. Changing this would require modifying the logging event pipeline significantly.
    i64 WriteImpl(const TLogEvent& event) override
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        // This writer has its own buffer, so we reimplement some of the log manager logic :(
        auto backlogWeight = BacklogWeight_.load(std::memory_order::relaxed);

        if (Suspended_ && backlogWeight < Config_->LowBacklogWeightWatermark) {
            Suspended_ = false;
            YT_LOG_INFO("Backlog weight has dropped below low watermark, dynamic table logging resumed (LowBacklogWeightWatermark: %v)",
                Config_->LowBacklogWeightWatermark);
        }

        if (!Suspended_ && backlogWeight > Config_->HighBacklogWeightWatermark) {
            Suspended_ = true;
            YT_LOG_WARNING("Backlog weight has exceeded high watermark, dynamic table logging suspended (HighBacklogWeightWatermark: %v)",
                Config_->HighBacklogWeightWatermark);
        }

        if (Suspended_) {
            DroppedEventsCounter_.Increment();
            return 0;
        }

        auto guard = Guard(SpinLock_);

        // This is necessary to avoid hitting the limit of allowed row modifications within a single transaction.
        // Also, there is no upside from huge batches.
        if (CurrentRowCount_ >= Config_->MaxBatchRowCount || std::ssize(CurrentBuffer_) >= Config_->MaxBatchWeight) {
            RotateBuffer();
        }

        TStringOutput outputStream(CurrentBuffer_);
        TCountingOutput countingOutputStream(&outputStream);
        Formatter_->WriteFormatted(&countingOutputStream, event);

        auto writtenBytes = static_cast<i64>(countingOutputStream.Counter());

        BacklogWeight_.fetch_add(writtenBytes, std::memory_order::relaxed);

        ++CurrentRowCount_;
        BacklogEventCount_.fetch_add(1, std::memory_order::relaxed);

        return writtenBytes;
    }

    void RotateBuffer()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (CurrentBuffer_.empty()) {
            return;
        }
        YT_VERIFY(CurrentRowCount_);

        BufferQueue_.push_back({
            .YsonRows = TSharedRef::FromString<TDynamicTableLogWriterBufferTag>(std::move(CurrentBuffer_)),
            .RowCount = CurrentRowCount_,
        });
        // Moved-from state should be fine, but just to be sure.
        CurrentBuffer_.clear();
        CurrentRowCount_ = 0;
    }

    // Periodic executor guarantees that at most one callback is executed at once.
    // This is important to avoid log reordering.
    void DoFlush()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        TBuffer bufferToFlush;

        {
            auto guard = Guard(SpinLock_);

            // Let's not increase the size of the queue if there is already something to flush.
            if (BufferQueue_.empty()) {
                RotateBuffer();
            }

            if (BufferQueue_.empty()) {
                return;
            }

            bufferToFlush = std::move(BufferQueue_.front());
            BufferQueue_.pop_front();
        }

        YT_VERIFY(!bufferToFlush.YsonRows.empty() && bufferToFlush.RowCount);

        auto decrementBacklogStatistics = Finally([this, size = std::ssize(bufferToFlush.YsonRows), eventCount = bufferToFlush.RowCount] {
            BacklogWeight_.fetch_sub(size, std::memory_order::relaxed);
            BacklogEventCount_.fetch_sub(eventCount, std::memory_order::relaxed);
        });

        TBackoffStrategy backoff(Config_->WriteBackoff);
        while (true) {
            try {
                auto dataWeightWritten = DoFlushIteration(bufferToFlush.YsonRows);
                FlushedEventsCounter_.Increment(bufferToFlush.RowCount);
                // We use the original yson size for symmetry with the backlog size gauge.
                FlushedBytesCounter_.Increment(bufferToFlush.YsonRows.size());
                // This value represents the size of the log "on disk".
                IncrementSegmentSize(dataWeightWritten);
                return;
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Error flushing log events to dynamic table");
            }

            if (backoff.Next()) {
                TDelayedExecutor::WaitForDuration(backoff.GetBackoff());
            } else {
                YT_LOG_ERROR(
                    "Flush retries exhausted, dropping log events (EventCount: %v, TotalSize: %v)",
                    bufferToFlush.RowCount,
                    bufferToFlush.YsonRows.size());
                DroppedEventsCounter_.Increment(bufferToFlush.RowCount);
                break;
            }
        }
    }

    i64 DoFlushIteration(const TSharedRef& ysonRows)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto client = ClientHolder_->Client.Acquire();

        if (!client) {
            THROW_ERROR_EXCEPTION("Cannot flush rows without configured YT client");
        }

        auto tableMountCache = client->GetTableMountCache();
        auto tableInfo = WaitForFast(tableMountCache->GetTableInfo(Config_->TablePath))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();

        TBuildingValueConsumer valueConsumer(
            tableInfo->Schemas[ETableSchemaKind::Write],
            TLogger{},
            Config_->InsertRowsFormat->EnableNullToYsonEntityConversion,
            Config_->TypeConversion);
        // This seems like a reasonable configuration.
        valueConsumer.SetAggregate(false);
        valueConsumer.SetTreatMissingAsNull(true);
        valueConsumer.SetAllowMissingKeyColumns(false);

        TMemoryInput inputStream(ysonRows.ToStringBuf());

        TTableOutput output(CreateParserForFormat(
            TFormat(EFormatType::Yson),
            &valueConsumer));

        PipeInputToOutput(&inputStream, &output, 64_KB);
        auto rowBuffer = New<TRowBuffer>(TDynamicTableLogWriterBufferTag());
        auto capturedRows = rowBuffer->CaptureRows(valueConsumer.GetRows());
        auto rowRange = MakeSharedRange(
            std::vector<TUnversionedRow>(capturedRows.begin(), capturedRows.end()),
            std::move(rowBuffer));
        auto dataWeight = GetDataWeight(rowRange);

        auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        transaction->WriteRows(
            Config_->TablePath,
            valueConsumer.GetNameTable(),
            std::move(rowRange));

        WaitFor(transaction->Commit())
            .ThrowOnError();

        return dataWeight;
    }

    // TODO(achulkov2): A lot of the logic above should be separated into some
    // sort of buffered dynamic table writer. It should be responsible for buffering
    // rows, low/high watermarks, and performing the actual YT writes with retries.
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableLogWriterFactory
    : public IDynamicTableLogWriterFactory
{
public:
    TDynamicTableLogWriterFactory()
        : ClientHolder_(New<TClientHolder>())
        , WriteQueue_(New<TActionQueue>("LogYTWrite"))
    { }

    void SetClient(NApi::IClientPtr client) override
    {
        ClientHolder_->Client.Store(std::move(client));
    }

    void ValidateConfig(const NYTree::IMapNodePtr& configNode) override
    {
        auto config = ParseConfig(configNode);
        if (config->Format != ELogFormat::Yson) {
            THROW_ERROR_EXCEPTION(
                "Writers of type %Qv must be configured to use %Qlv format",
                TDynamicTableLogWriterConfig::WriterType,
                ELogFormat::Yson);
        }
    }

    ILogWriterPtr CreateWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString name,
        const NYTree::IMapNodePtr& configNode,
        ILogWriterHost* /*host*/) noexcept override
    {
        auto config = ParseConfig(configNode);
        return New<TDynamicTableLogWriter>(
            ClientHolder_,
            std::move(formatter),
            CreateDefaultSystemLogEventProvider(config),
            std::move(config),
            std::move(name),
            WriteQueue_->GetInvoker());
    }

private:
    const TClientHolderPtr ClientHolder_;
    const TActionQueuePtr WriteQueue_;

    static TDynamicTableLogWriterConfigPtr ParseConfig(const NYTree::IMapNodePtr& configNode)
    {
        return ConvertTo<TDynamicTableLogWriterConfigPtr>(configNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterDynamicTableLogWriterFactory()
{
    TLogManager::Get()->RegisterWriterFactory(
        TString(TDynamicTableLogWriterConfig::WriterType),
        GetDynamicTableLogWriterFactory());
}

IDynamicTableLogWriterFactoryPtr GetDynamicTableLogWriterFactory()
{
    return LeakyRefCountedSingleton<TDynamicTableLogWriterFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
