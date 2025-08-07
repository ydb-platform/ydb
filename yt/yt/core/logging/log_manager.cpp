#include "log_manager.h"

#include "private.h"
#include "config.h"
#include "log.h"
#include "log_writer.h"
#include "log_writer_factory.h"
#include "formatter.h"
#include "file_log_writer.h"
#include "stream_log_writer.h"
#include "system_log_event_provider.h"

#include <yt/yt/core/concurrency/profiling_helpers.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_thread.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/invoker_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/inotify.h>
#include <yt/yt/core/misc/spsc_queue.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/pattern_formatter.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/signals/signal_registry.h>

#include <library/cpp/yt/misc/hash.h>
#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/misc/tls.h>

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <library/cpp/yt/containers/expiring_set.h>

#include <util/system/defaults.h>
#include <util/system/sigset.h>
#include <util/system/yield.h>

#include <util/generic/algorithm.h>

#include <atomic>
#include <mutex>

#ifdef _win_
    #include <io.h>
#else
    #include <unistd.h>
#endif

namespace NYT::NLogging {

using namespace NYTree;
using namespace NConcurrency;
using namespace NFS;
using namespace NProfiling;
using namespace NSignals;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, SystemLoggingCategoryName);

static constexpr auto DiskProfilingPeriod = TDuration::Minutes(5);
static constexpr auto AnchorProfilingPeriod = TDuration::Seconds(15);
static constexpr auto DequeuePeriod = TDuration::MilliSeconds(30);

static const TStringBuf StderrSystemWriterName("stderr");

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TLogWriterCacheKey& lhs, const TLogWriterCacheKey& rhs)
{
    return lhs.Category == rhs.Category && lhs.LogLevel == rhs.LogLevel && lhs.Family == rhs.Family;
}

////////////////////////////////////////////////////////////////////////////////

struct TConfigEvent
{
    TCpuInstant Instant = 0;
    TLogManagerConfigPtr Config;
    bool FromEnv;
    TPromise<void> Promise = NewPromise<void>();
};

using TLoggerQueueItem = std::variant<
    TLogEvent,
    TConfigEvent
>;

TCpuInstant GetEventInstant(const TLoggerQueueItem& item)
{
    return Visit(item,
        [&] (const TConfigEvent& event) {
            return event.Instant;
        },
        [&] (const TLogEvent& event) {
            return event.Instant;
        });
}

using TThreadLocalQueue = TSpscQueue<TLoggerQueueItem>;

static constexpr uintptr_t ThreadQueueDestroyedSentinel = -1;
YT_DEFINE_THREAD_LOCAL(TThreadLocalQueue*, PerThreadQueue);

////////////////////////////////////////////////////////////////////////////////

struct TLocalQueueReclaimer
{
    ~TLocalQueueReclaimer();
};

YT_DEFINE_THREAD_LOCAL(TLocalQueueReclaimer, LocalQueueReclaimer);

////////////////////////////////////////////////////////////////////////////////

class TLogManager::TImpl
    : public ISensorProducer
    , public ILogWriterHost
{
public:
    friend struct TLocalQueueReclaimer;

    TImpl()
        : EventQueue_(New<TMpscInvokerQueue>(
            EventCount_,
            NConcurrency::GetThreadTags("Logging")))
        , LoggingThread_(New<TLoggingThread>(this))
        , SystemWriters_({
            CreateStderrLogWriter(
                std::make_unique<TPlainTextLogFormatter>(),
                CreateDefaultSystemLogEventProvider(/*systemMessagesEnabled*/ true, /*systemMessageFamily*/ ELogFamily::PlainText),
                TString(StderrSystemWriterName),
                New<TStderrLogWriterConfig>())
        })
        , DiskProfilingExecutor_(New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::OnDiskProfiling, MakeWeak(this)),
            DiskProfilingPeriod))
        , AnchorProfilingExecutor_(New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::OnAnchorProfiling, MakeWeak(this)),
            AnchorProfilingPeriod))
        , DequeueExecutor_(New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::OnDequeue, MakeStrong(this)),
            DequeuePeriod))
        , FlushExecutor_(New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::FlushWriters, MakeStrong(this)),
            std::nullopt))
        , WatchExecutor_(New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::WatchWriters, MakeStrong(this)),
            std::nullopt))
        , CheckSpaceExecutor_(New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::CheckSpace, MakeStrong(this)),
            std::nullopt))
        , FileRotationExecutor_(New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::RotateFiles, MakeStrong(this)),
            std::nullopt))
        , CompressionThreadPool_(CreateThreadPool(
            /*threadCount*/ 1,
            /*threadNamePrefix*/ "LogCompress"))
    {
        RegisterWriterFactory(TString(TFileLogWriterConfig::WriterType), GetFileLogWriterFactory());
        RegisterWriterFactory(TString(TStderrLogWriterConfig::WriterType), GetStderrLogWriterFactory());
    }

    bool IsInitialized() const
    {
        return InitializationFinished_.Test();
    }

    void Initialize()
    {
        [[likely]] if (InitializationFinished_.Test()) {
            // Don't bother doing syscalls on a hot path.
            return;
        }

        // Sync is done via event so there is no need for stronger memory orders.
        // Case of recursive call is alright, because there sync is done via sequenced-before ordering.
        [[likely]] if (InitializationStarted_.exchange(true, std::memory_order::relaxed)) {
            NThreading::TThreadId initializerThreadId = NThreading::InvalidThreadId;
            while (initializerThreadId == NThreading::InvalidThreadId) {
                initializerThreadId = InitializerThreadId_.load(std::memory_order::relaxed);
            }
            if (GetCurrentThreadId() == initializerThreadId) {
                // Recursive call -- bail out.
                return;
            }
            // Another thread -- now wait for real.
            InitializationFinished_.Wait();
            return;
        }
        InitializerThreadId_.store(GetCurrentThreadId(), std::memory_order::relaxed);

        // NB: Cannot place this logic inside ctor since it may boot up Compression threads unexpected
        // and these will try to access TLogManager instance causing a deadlock.
        try {
            if (auto config = TLogManagerConfig::TryCreateFromEnv()) {
                DoUpdateConfig(config, /*fromEnv*/ true);
            }
        } catch (const std::exception& ex) {
            fprintf(stderr, "Error configuring logging from environment variables\n%s\n",
                ex.what());
        }

        if (!IsConfiguredFromEnv()) {
            DoUpdateConfig(TLogManagerConfig::CreateDefault(), /*fromEnv*/ false);
            DefaultConfigured_.store(true);
        }

        SystemCategory_ = GetCategory(SystemLoggingCategoryName);
        InitializationFinished_.NotifyAll();
    }

    void Configure(INodePtr node)
    {
        Configure(
            TLogManagerConfig::CreateFromNode(node),
            /*fromEnv*/ false,
            /*sync*/ true);
    }

    void Configure(TLogManagerConfigPtr config, bool fromEnv, bool sync)
    {
        if (LoggingThread_->IsStopping()) {
            return;
        }

        TConfigEvent event{
            .Instant = GetCpuInstant(),
            .Config = std::move(config),
            .FromEnv = fromEnv
        };

        auto future = event.Promise.ToFuture();

        PushEvent(std::move(event));
        EnsureStarted();

        DequeueExecutor_->ScheduleOutOfBand();

        if (sync) {
            future.Get().ThrowOnError();
        }

        DefaultConfigured_.store(false);
    }

    bool IsDefaultConfigured()
    {
        return DefaultConfigured_.load();
    }

    void ConfigureFromEnv()
    {
        if (auto config = TLogManagerConfig::TryCreateFromEnv()) {
            Configure(
                std::move(config),
                /*fromEnv*/ true,
                /*sync*/ true);
        }
    }

    bool IsConfiguredFromEnv()
    {
        return ConfiguredFromEnv_.load();
    }

    void Shutdown()
    {
        ShutdownRequested_.store(true);

        auto config = Config_.Acquire();

        if (LoggingThread_->GetThreadId() == GetCurrentThreadId()) {
            FlushWriters();
        } else {
            // Wait for all previously enqueued messages to be flushed
            // but no more than ShutdownGraceTimeout to prevent hanging.
            Synchronize(TInstant::Now() + config->ShutdownGraceTimeout);
        }

        // For now this is the only way to wait for log writers that perform asynchronous flushes.
        // TODO(achulkov2): Refactor log manager to support asynchronous operations.
        if (config->ShutdownBusyTimeout != TDuration::Zero()) {
            Sleep(config->ShutdownBusyTimeout);
        }

        EventQueue_->Shutdown();
    }

    /*!
     * In some cases (when configuration is being updated at the same time),
     * the actual version is greater than the version returned by this method.
     */
    int GetVersion() const
    {
        return Version_.load();
    }

    bool GetAbortOnAlert() const
    {
        return AbortOnAlert_.load();
    }

    const TLoggingCategory* GetCategory(TStringBuf categoryName)
    {
        if (!categoryName) {
            return nullptr;
        }

        auto guard = Guard(SpinLock_);
        auto config = Config_.Acquire();
        auto it = NameToCategory_.find(categoryName);
        if (it == NameToCategory_.end()) {
            auto category = std::make_unique<TLoggingCategory>();
            category->Name = categoryName;
            category->ActualVersion = &Version_;
            it = NameToCategory_.emplace(categoryName, std::move(category)).first;
            DoUpdateCategory(config, it->second.get());
        }
        return it->second.get();
    }

    void UpdateCategory(TLoggingCategory* category)
    {
        auto guard = Guard(SpinLock_);
        auto config = Config_.Acquire();
        DoUpdateCategory(config, category);
    }

    void UpdateAnchor(TLoggingAnchor* anchor)
    {
        auto guard = Guard(SpinLock_);
        auto config = Config_.Acquire();
        DoUpdateAnchor(config, anchor);
    }

    void RegisterStaticAnchor(
        TLoggingAnchor* anchor,
        ::TSourceLocation sourceLocation,
        TStringBuf message)
    {
        auto guard = Guard(SpinLock_);
        auto config = Config_.Acquire();
        anchor->SourceLocation = sourceLocation;
        anchor->AnchorMessage = BuildAnchorMessage(sourceLocation, message);
        DoRegisterAnchor(anchor);
        DoUpdateAnchor(config, anchor);
    }

    TLoggingAnchor* RegisterDynamicAnchor(TString anchorMessage)
    {
        auto guard = Guard(SpinLock_);
        if (auto it = AnchorMap_.find(anchorMessage)) {
            return it->second;
        }
        auto config = Config_.Acquire();
        auto anchor = std::make_unique<TLoggingAnchor>();
        anchor->AnchorMessage = std::move(anchorMessage);
        auto* rawAnchor = anchor.get();
        DynamicAnchors_.push_back(std::move(anchor));
        DoRegisterAnchor(rawAnchor);
        DoUpdateAnchor(config, rawAnchor);
        return rawAnchor;
    }

    void RegisterWriterFactory(const TString& typeName, const ILogWriterFactoryPtr& factory)
    {
        auto guard = Guard(SpinLock_);
        EmplaceOrCrash(TypeNameToWriterFactory_, typeName, factory);
    }

    void UnregisterWriterFactory(const TString& typeName)
    {
        auto guard = Guard(SpinLock_);
        EraseOrCrash(TypeNameToWriterFactory_, typeName);
    }

    void Enqueue(TLogEvent&& event)
    {
        if (event.Level == ELogLevel::Fatal) {
            bool shutdown = false;
            if (!ShutdownRequested_.compare_exchange_strong(shutdown, true)) {
                // Fatal events should not get out of this call.
                Sleep(TDuration::Max());
            }

            // Collect last-minute information.
            TRawFormatter<1024> formatter;
            formatter.AppendString("\n*** Fatal error ***\n");
            formatter.AppendString(event.MessageRef.ToStringBuf());
            formatter.AppendString("\n*** Aborting ***\n");

            HandleEintr(::write, 2, formatter.GetData(), formatter.GetBytesWritten());

            // Add fatal message to log and notify event log queue.
            PushEvent(std::move(event));

            // Flush everything and die.
            Shutdown();
            std::terminate();
        }

        if (ShutdownRequested_) {
            ++DroppedEvents_;
            return;
        }

        if (LoggingThread_->IsStopping()) {
            ++DroppedEvents_;
            return;
        }

        EnsureStarted();

        // Order matters here; inherent race may lead to negative backlog and integer overflow.
        ui64 writtenEvents = WrittenEvents_.load();
        ui64 enqueuedEvents = EnqueuedEvents_.load();
        ui64 backlogEvents = enqueuedEvents - writtenEvents;

        // NB: This is somewhat racy but should work fine as long as more messages keep coming.
        auto lowBacklogWatermark = LowBacklogWatermark_.load(std::memory_order::relaxed);
        auto highBacklogWatermark = HighBacklogWatermark_.load(std::memory_order::relaxed);

        BacklogQueueFillFraction_.store(static_cast<double>(backlogEvents) / highBacklogWatermark, std::memory_order::relaxed);

        if (Suspended_.load(std::memory_order::relaxed)) {
            if (backlogEvents < lowBacklogWatermark) {
                Suspended_.store(false, std::memory_order::relaxed);
                YT_LOG_INFO("Backlog size has dropped below low watermark, logging resumed (LowBacklogWatermark: %v)",
                    lowBacklogWatermark);
            }
        } else {
            if (backlogEvents >= lowBacklogWatermark && !ScheduledOutOfBand_.exchange(true)) {
                DequeueExecutor_->ScheduleOutOfBand();
            }

            if (backlogEvents >= highBacklogWatermark) {
                Suspended_.store(true, std::memory_order::relaxed);
                YT_LOG_WARNING("Backlog size has exceeded high watermark, logging suspended (HighBacklogWatermark: %v)",
                    highBacklogWatermark);
            }
        }

        // NB: Always allow system messages to pass through.
        if (Suspended_ && event.Category != SystemCategory_ && !event.Essential) {
            ++DroppedEvents_;
            return;
        }

        PushEvent(std::move(event));
    }

    void Reopen()
    {
        ReopenRequested_.store(true);
    }

    void EnableReopenOnSighup()
    {
#ifdef _unix_
        TSignalRegistry::Get()->PushCallback(
            SIGHUP,
            [this] { Reopen(); });
#endif
    }

    void SuppressRequest(TRequestId requestId)
    {
        if (RequestSuppressionEnabled_.load(std::memory_order_relaxed)) {
            SuppressedRequestIdQueue_.Enqueue(requestId);
        }
    }

    void Synchronize(TInstant deadline = TInstant::Max())
    {
        auto enqueuedEvents = EnqueuedEvents_.load();
        while (enqueuedEvents > FlushedEvents_.load() && TInstant::Now() < deadline) {
            SchedYield();
        }
    }

    // ILogWriterHost implementation
    IInvokerPtr GetCompressionInvoker() override
    {
        return CompressionThreadPool_->GetInvoker();
    }

    double GetBacklogQueueFillFraction() const
    {
        return BacklogQueueFillFraction_.load(std::memory_order::relaxed);
    }

private:
    class TLoggingThread
        : public TSchedulerThread
    {
    public:
        explicit TLoggingThread(TImpl* owner)
            : TSchedulerThread(
                owner->EventCount_,
                "Logging",
                "Logging",
                NThreading::TThreadOptions{
                    .ShutdownPriority = 200,
                })
            , Owner_(owner)
        { }

    private:
        TImpl* const Owner_;

        TEnqueuedAction CurrentAction_;

        TClosure BeginExecute() override
        {
            YT_ASSERT_THREAD_AFFINITY(Owner_->LoggingThread);

            return BeginExecuteImpl(Owner_->EventQueue_->BeginExecute(&CurrentAction_), &CurrentAction_);
        }

        void EndExecute() override
        {
            YT_ASSERT_THREAD_AFFINITY(Owner_->LoggingThread);

            Owner_->EventQueue_->EndExecute(&CurrentAction_);
        }
    };

    void EnsureStarted()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        std::call_once(Started_, [&] {
            if (LoggingThread_->IsStopping()) {
                return;
            }

            LoggingThread_->Start();
            EventQueue_->SetThreadId(LoggingThread_->GetThreadId());
            DiskProfilingExecutor_->Start();
            AnchorProfilingExecutor_->Start();
            DequeueExecutor_->Start();
            FlushExecutor_->Start();
            WatchExecutor_->Start();
            CheckSpaceExecutor_->Start();
            FileRotationExecutor_->Start();
        });
    }

    const std::vector<ILogWriterPtr>& GetWriters(
        const TLogManagerConfigPtr& config,
        const TLogEvent& event)
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        if (event.Category == SystemCategory_) {
            return SystemWriters_;
        }

        TLogWriterCacheKey cacheKey{event.Category->Name, event.Level, event.Family};
        auto it = KeyToCachedWriter_.find(cacheKey);
        if (it != KeyToCachedWriter_.end()) {
            return it->second;
        }

        THashSet<TString> writerNames;
        for (const auto& rule : config->Rules) {
            if (rule->IsApplicable(event.Category->Name, event.Level, event.Family)) {
                writerNames.insert(rule->Writers.begin(), rule->Writers.end());
            }
        }

        std::vector<ILogWriterPtr> writers;
        for (const auto& name : writerNames) {
            writers.push_back(GetOrCrash(NameToWriter_, name));
        }

        return EmplaceOrCrash(KeyToCachedWriter_, cacheKey, writers)->second;
    }

    TInotifyHandle* TryGetNotificationHandle()
    {
        if (!NotificationHandle_ && !NotificationHandleCreationFailed_) {
            try {
                NotificationHandle_ = std::make_unique<TInotifyHandle>();
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Error creating inotify handle, watching disabled");
                NotificationHandleCreationFailed_ = true;
            }
        }
        return NotificationHandle_.get();
    }

    std::unique_ptr<TInotifyWatch> TryCreateNotificationWatch(
        const TLogManagerConfigPtr& config,
        const IFileLogWriterPtr& writer)
    {
#ifdef _linux_
        if (config->WatchPeriod) {
            auto* notifcationHandle = TryGetNotificationHandle();
            if (!notifcationHandle) {
                return nullptr;
            }

            try {
                return std::make_unique<TInotifyWatch>(
                    notifcationHandle,
                    writer->GetFileName(),
                    EInotifyWatchEvents::DeleteSelf | EInotifyWatchEvents::MoveSelf);
            } catch (const std::exception& ex) {
                // Watch can fail to initialize if the writer is disabled
                // e.g. due to the lack of space.
                YT_LOG_ERROR(ex, "Error creating inotify watch (Path: %v)",
                    writer->GetFileName());
                return nullptr;
            }
        }
#else
        Y_UNUSED(config, writer);
#endif
        return nullptr;
    }

    void CreateNotificationWatchForWriter(
        const TLogManagerConfigPtr& config,
        const IFileLogWriterPtr& writer)
    {
        if (auto watch = TryCreateNotificationWatch(config, writer)) {
            EmplaceOrCrash(NotificationWatchWDToWriter_, watch->GetWD(), writer);
            EmplaceOrCrash(WriterToNotificationWatch_, writer, std::move(watch));
        } else {
            InsertOrCrash(WritersWithFailedNotificationWatches_, writer);
        }
    }

    void UpdateConfig(const TConfigEvent& event)
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        if (ShutdownRequested_) {
            return;
        }

        if (LoggingThread_->IsStopping()) {
            return;
        }

        EnsureStarted();
        FlushWriters();

        try {
            DoUpdateConfig(event.Config, event.FromEnv);
            event.Promise.Set();
        } catch (const std::exception& ex) {
            event.Promise.Set(ex);
        }
    }

    static std::unique_ptr<ILogFormatter> CreateFormatter(const TLogWriterConfigPtr& writerConfig)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        switch (writerConfig->Format) {
            case ELogFormat::PlainText:
                return std::make_unique<TPlainTextLogFormatter>(
                    writerConfig->EnableSourceLocation);

            case ELogFormat::Json: [[fallthrough]];
            case ELogFormat::Yson:
                return std::make_unique<TStructuredLogFormatter>(
                    writerConfig->Format,
                    writerConfig->CommonFields,
                    writerConfig->EnableSourceLocation,
                    writerConfig->EnableSystemFields,
                    writerConfig->EnableHostField,
                    writerConfig->JsonFormat);

            default:
                YT_ABORT();
        }
    }

    void DoUpdateConfig(const TLogManagerConfigPtr& config, bool fromEnv)
    {
        // This could be called both from ctor and from LoggingThread.

        auto oldConfig = Config_.Acquire();
        if (AreNodesEqual(ConvertToNode(oldConfig), ConvertToNode(config))) {
            return;
        }

        THashMap<TString, ILogWriterFactoryPtr> typeNameToWriterFactory;
        {
            auto guard = Guard(SpinLock_);
            for (const auto& [name, writerConfig] : config->Writers) {
                auto typedWriterConfig = ConvertTo<TLogWriterConfigPtr>(writerConfig);
                if (typeNameToWriterFactory.contains(typedWriterConfig->Type)) {
                    continue;
                }
                auto it = TypeNameToWriterFactory_.find(typedWriterConfig->Type);
                if (it == TypeNameToWriterFactory_.end()) {
                    THROW_ERROR_EXCEPTION("Unknown log writer type %Qv", typedWriterConfig->Type);
                }
                const auto& writerFactory = it->second;
                writerFactory->ValidateConfig(writerConfig);
                EmplaceOrCrash(typeNameToWriterFactory, typedWriterConfig->Type, writerFactory);
            }
            for (const auto& [_, category] : NameToCategory_) {
                category->StructuredValidationSamplingRate.store(config->StructuredValidationSamplingRate, std::memory_order::relaxed);
            }
        }

        NameToWriter_.clear();
        KeyToCachedWriter_.clear();

        WriterToNotificationWatch_.clear();
        NotificationWatchWDToWriter_.clear();
        WritersWithFailedNotificationWatches_.clear();

        for (const auto& [name, writerConfig] : config->Writers) {
            auto typedWriterConfig = ConvertTo<TLogWriterConfigPtr>(writerConfig);
            auto formatter = CreateFormatter(typedWriterConfig);
            auto writerFactory = GetOrCrash(typeNameToWriterFactory, typedWriterConfig->Type);
            auto writer = writerFactory->CreateWriter(
                std::move(formatter),
                name,
                writerConfig,
                this);

            writer->SetRateLimit(typedWriterConfig->RateLimit);
            writer->SetCategoryRateLimits(config->CategoryRateLimits);

            EmplaceOrCrash(NameToWriter_, name, writer);

            if (auto fileWriter = DynamicPointerCast<IFileLogWriter>(writer)) {
                CreateNotificationWatchForWriter(config, fileWriter);
            }
        }

        ConfiguredFromEnv_.store(fromEnv);
        HighBacklogWatermark_.store(config->HighBacklogWatermark);
        LowBacklogWatermark_.store(config->LowBacklogWatermark);
        RequestSuppressionEnabled_.store(config->RequestSuppressionTimeout != TDuration::Zero());
        AbortOnAlert_.store(config->AbortOnAlert);

        CompressionThreadPool_->SetThreadCount(config->CompressionThreadCount);

        if (RequestSuppressionEnabled_) {
            SuppressedRequestIdSet_.SetTtl((config->RequestSuppressionTimeout + DequeuePeriod) * 2);
        } else {
            SuppressedRequestIdSet_.Clear();
            SuppressedRequestIdQueue_.DequeueAll();
        }

        FlushExecutor_->SetPeriod(config->FlushPeriod);
        WatchExecutor_->SetPeriod(config->WatchPeriod);
        CheckSpaceExecutor_->SetPeriod(config->CheckSpacePeriod);
        FileRotationExecutor_->SetPeriod(config->RotationCheckPeriod);

        Config_.Store(std::move(config));
        Version_++;
    }

    void WriteEvent(
        const TLogManagerConfigPtr& config,
        const TLogEvent& event)
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        if (ReopenRequested_.exchange(false)) {
            ReloadWriters();
        }

        GetWrittenEventsCounter(event).Increment();

        if (event.Anchor) {
            event.Anchor->MessageCounter.Current += 1;
            event.Anchor->ByteCounter.Current += std::ssize(event.MessageRef);
        }

        for (const auto& writer : GetWriters(config, event)) {
            writer->Write(event);
        }
    }

    void FlushWriters()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        for (const auto& [name, writer] : NameToWriter_) {
            writer->Flush();
        }

        FlushedEvents_ = WrittenEvents_.load();
    }

    void RotateFiles()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        for (const auto& [name, writer] : NameToWriter_) {
            if (auto fileWriter = DynamicPointerCast<IFileLogWriter>(writer)) {
                fileWriter->MaybeRotate();
            }
        }
    }

    void ReloadWriters()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        Version_++;
        for (const auto& [name, writer] : NameToWriter_) {
            writer->Reload();
        }
    }

    void CheckSpace()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        auto config = Config_.Acquire();
        for (const auto& [name, writer] : NameToWriter_) {
            if (auto fileWriter = DynamicPointerCast<IFileLogWriter>(writer)) {
                fileWriter->CheckSpace(config->MinDiskSpace);
            }
        }
    }

    void WatchWriters()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        auto* notificationHandle = TryGetNotificationHandle();
        if (!notificationHandle) {
            return;
        }

        auto config = Config_.Acquire();

        // Always reload writers and retry registration for invalid watches.
        auto writersToReconsider = std::exchange(WritersWithFailedNotificationWatches_, {});

        while (auto pollResult = notificationHandle->Poll()) {
            if (auto writer = GetOrDefault(NotificationWatchWDToWriter_, pollResult->WD)) {
                EraseOrCrash(NotificationWatchWDToWriter_, pollResult->WD);
                EraseOrCrash(WriterToNotificationWatch_, writer);
                InsertOrCrash(writersToReconsider, writer);
            }
        }

        for (const auto& writer : writersToReconsider) {
            writer->Reload();
            CreateNotificationWatchForWriter(config, writer);
        }
    }

    void PushEvent(TLoggerQueueItem&& event)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto& perThreadQueue = PerThreadQueue();
        if (!perThreadQueue) {
            perThreadQueue = new TThreadLocalQueue();
            RegisteredLocalQueues_.Enqueue(perThreadQueue);
            Y_UNUSED(LocalQueueReclaimer()); // Touch thread-local variable so that its destructor is called.
        }

        ++EnqueuedEvents_;
        if (perThreadQueue == reinterpret_cast<TThreadLocalQueue*>(ThreadQueueDestroyedSentinel)) {
            GlobalQueue_.Enqueue(std::move(event));
        } else {
            perThreadQueue->Push(std::move(event));
        }
    }

    const TCounter& GetWrittenEventsCounter(const TLogEvent& event)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto key = std::pair(event.Category->Name, event.Level);
        auto it = WrittenEventsCounters_.find(key);
        if (it == WrittenEventsCounters_.end()) {
            // TODO(prime@): optimize sensor count
            auto counter = Profiler
                .WithSparse()
                .WithTag("category", TString{event.Category->Name})
                .WithTag("level", FormatEnum(event.Level))
                .Counter("/written_events");

            it = WrittenEventsCounters_.emplace(key, counter).first;
        }
        return it->second;
    }

    void CollectSensors(ISensorWriter* writer) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto writtenEvents = WrittenEvents_.load();
        auto enqueuedEvents = EnqueuedEvents_.load();
        auto suppressedEvents = SuppressedEvents_.load();
        auto droppedEvents = DroppedEvents_.load();
        auto messageBuffersSize = TRefCountedTracker::Get()->GetBytesAlive(GetRefCountedTypeKey<NDetail::TMessageBufferTag>());

        writer->AddCounter("/enqueued_events", enqueuedEvents);
        writer->AddGauge("/backlog_events", enqueuedEvents - writtenEvents);
        writer->AddCounter("/dropped_events", droppedEvents);
        writer->AddCounter("/suppressed_events", suppressedEvents);
        writer->AddGauge("/message_buffers_size", messageBuffersSize);
    }

    void OnDiskProfiling()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        try {
            auto minLogStorageAvailableSpace = std::numeric_limits<i64>::max();
            auto minLogStorageFreeSpace = std::numeric_limits<i64>::max();

            for (const auto& [name, writer] : NameToWriter_) {
                if (auto fileWriter = DynamicPointerCast<IFileLogWriter>(writer)) {
                    auto logStorageDiskSpaceStatistics = GetDiskSpaceStatistics(GetDirectoryName(fileWriter->GetFileName()));
                    minLogStorageAvailableSpace = std::min<i64>(minLogStorageAvailableSpace, logStorageDiskSpaceStatistics.AvailableSpace);
                    minLogStorageFreeSpace = std::min<i64>(minLogStorageFreeSpace, logStorageDiskSpaceStatistics.FreeSpace);
                }
            }

            if (minLogStorageAvailableSpace != std::numeric_limits<i64>::max()) {
                MinLogStorageAvailableSpace_.Update(minLogStorageAvailableSpace);
            }
            if (minLogStorageFreeSpace != std::numeric_limits<i64>::max()) {
                MinLogStorageFreeSpace_.Update(minLogStorageFreeSpace);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get log storage disk statistics");
        }
    }

    struct TLoggingAnchorStat
    {
        TLoggingAnchor* Anchor;
        double MessageRate;
        double ByteRate;
    };

    std::vector<TLoggingAnchorStat> CaptureAnchorStats()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        auto now = TInstant::Now();
        auto deltaSeconds = (now - LastAnchorStatsCaptureTime_).SecondsFloat();
        LastAnchorStatsCaptureTime_ = now;

        std::vector<TLoggingAnchorStat> result;
        auto* currentAnchor = FirstAnchor_.load();
        while (currentAnchor) {
            auto getRate = [&] (auto& counter) {
                auto current = counter.Current;
                auto rate = (current - counter.Previous) / deltaSeconds;
                counter.Previous = current;
                return rate;
            };

            result.push_back({
                .Anchor = currentAnchor,
                .MessageRate = getRate(currentAnchor->MessageCounter),
                .ByteRate = getRate(currentAnchor->ByteCounter),
            });

            currentAnchor = currentAnchor->NextAnchor;
        }
        return result;
    }

    void OnAnchorProfiling()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        auto config = Config_.Acquire();
        if (config->EnableAnchorProfiling && !AnchorBufferedProducer_) {
            AnchorBufferedProducer_ = New<TBufferedProducer>();
            Profiler
                .WithSparse()
                .WithDefaultDisabled()
                .WithProducerRemoveSupport()
                .AddProducer("/anchors", AnchorBufferedProducer_);
        } else if (!config->EnableAnchorProfiling && AnchorBufferedProducer_) {
            AnchorBufferedProducer_.Reset();
        }

        if (!AnchorBufferedProducer_) {
            return;
        }

        auto stats = CaptureAnchorStats();

        TSensorBuffer sensorBuffer;
        for (const auto& stat : stats) {
            if (stat.MessageRate < config->MinLoggedMessageRateToProfile) {
                continue;
            }
            TWithTagGuard tagGuard(&sensorBuffer, "message", stat.Anchor->AnchorMessage);
            sensorBuffer.AddGauge("/logged_messages/rate", stat.MessageRate);
            sensorBuffer.AddGauge("/logged_bytes/rate", stat.ByteRate);
        }

        AnchorBufferedProducer_->Update(std::move(sensorBuffer));
    }

    void OnDequeue()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        ScheduledOutOfBand_.store(false);

        auto currentInstant = GetCpuInstant();

        RegisteredLocalQueues_.DequeueAll(true, [&] (TThreadLocalQueue* item) {
            InsertOrCrash(LocalQueues_, item);
        });

        struct THeapItem
        {
            TThreadLocalQueue* Queue;

            explicit THeapItem(TThreadLocalQueue* queue)
                : Queue(queue)
            { }

            TLoggerQueueItem* Front() const
            {
                return Queue->Front();
            }

            void Pop()
            {
                Queue->Pop();
            }

            TCpuInstant GetInstant() const
            {
                auto* front = Front();
                if (Y_LIKELY(front)) {
                    return GetEventInstant(*front);
                } else {
                    return std::numeric_limits<TCpuInstant>::max();
                }
            }

            bool operator < (const THeapItem& other) const
            {
                return GetInstant() < other.GetInstant();
            }
        };

        std::vector<THeapItem> heap;
        for (auto* localQueue : LocalQueues_) {
            if (localQueue->Front()) {
                heap.emplace_back(localQueue);
            }
        }

        if (!heap.empty()) {
            // NB: Messages are not totally ordered because of race around high/low watermark check.

            MakeHeap(heap.begin(), heap.end());
            ExtractHeap(heap.begin(), heap.end());
            auto topItem = heap.back();
            heap.pop_back();

            while (!heap.empty()) {
                // Increment front instant by one to avoid live lock when there are two queueus
                // with equal front instants.
                auto nextInstant = heap.front().GetInstant() < currentInstant
                    ? heap.front().GetInstant() + 1
                    : currentInstant;

                // TODO(lukyan): Use exponential search to determine last element.
                // Use batch extraction from queue.
                while (topItem.GetInstant() < nextInstant) {
                    TimeOrderedBuffer_.emplace_back(std::move(*topItem.Front()));
                    topItem.Pop();
                }

                std::swap(topItem, heap.front());

                if (heap.front().GetInstant() < currentInstant) {
                    AdjustHeapFront(heap.begin(), heap.end());
                } else {
                    ExtractHeap(heap.begin(), heap.end());
                    heap.pop_back();
                }
            }

            while (topItem.GetInstant() < currentInstant) {
                TimeOrderedBuffer_.emplace_back(std::move(*topItem.Front()));
                topItem.Pop();
            }
        }

        UnregisteredLocalQueues_.DequeueAll(true, [&] (TThreadLocalQueue* item) {
            if (item->IsEmpty()) {
                EraseOrCrash(LocalQueues_, item);
                delete item;
            } else {
                UnregisteredLocalQueues_.Enqueue(item);
            }
        });

        // TODO(lukyan): To achieve total order of messages copy them from GlobalQueue to
        // separate TThreadLocalQueue sort it and merge it with LocalQueues
        // TODO(lukyan): Reuse nextEvents
        // NB: Messages from global queue are not sorted
        std::vector<TLoggerQueueItem> nextEvents;
        while (GlobalQueue_.DequeueAll(true, [&] (TLoggerQueueItem& event) {
            if (GetEventInstant(event) < currentInstant) {
                TimeOrderedBuffer_.emplace_back(std::move(event));
            } else {
                nextEvents.push_back(std::move(event));
            }
        }))
        { }

        for (auto& event : nextEvents) {
            GlobalQueue_.Enqueue(std::move(event));
        }

        auto eventsWritten = ProcessTimeOrderedBuffer();

        if (eventsWritten == 0) {
            return;
        }

        WrittenEvents_ += eventsWritten;

        auto config = Config_.Acquire();
        if (!config->FlushPeriod || ShutdownRequested_) {
            FlushWriters();
        }
    }

    int ProcessTimeOrderedBuffer()
    {
        YT_ASSERT_THREAD_AFFINITY(LoggingThread);

        int eventsWritten = 0;
        int eventsSuppressed = 0;

        SuppressedRequestIdSet_.InsertMany(Now(), SuppressedRequestIdQueue_.DequeueAll());

        auto requestSuppressionEnabled = RequestSuppressionEnabled_.load(std::memory_order::relaxed);
        auto config = Config_.Acquire();
        auto suppressionDeadline = GetCpuInstant() - DurationToCpuDuration(config->RequestSuppressionTimeout);

        while (!TimeOrderedBuffer_.empty()) {
            const auto& event = TimeOrderedBuffer_.front();

            if (requestSuppressionEnabled && GetEventInstant(event) > suppressionDeadline) {
                break;
            }

            ++eventsWritten;

            Visit(event,
                [&] (const TConfigEvent& event) {
                    UpdateConfig(event);
                    config = Config_.Acquire();
                },
                [&] (const TLogEvent& event) {
                    if (requestSuppressionEnabled && event.RequestId && SuppressedRequestIdSet_.Contains(event.RequestId)) {
                        ++eventsSuppressed;
                    } else {
                        WriteEvent(config, event);
                    }
                });

            TimeOrderedBuffer_.pop_front();
        }

        SuppressedEvents_ += eventsSuppressed;

        return eventsWritten;
    }

    void DoUpdateCategory(const TLogManagerConfigPtr& config, TLoggingCategory* category)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto minPlainTextLevel = ELogLevel::Maximum;
        for (const auto& rule : config->Rules) {
            if (rule->IsApplicable(category->Name, ELogFamily::PlainText)) {
                minPlainTextLevel = std::min(minPlainTextLevel, rule->MinLevel);
            }
        }

        category->MinPlainTextLevel.store(minPlainTextLevel, std::memory_order::relaxed);
        category->CurrentVersion.store(GetVersion(), std::memory_order::relaxed);
        category->StructuredValidationSamplingRate.store(config->StructuredValidationSamplingRate, std::memory_order::relaxed);
    }

    void DoRegisterAnchor(TLoggingAnchor* anchor)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        // NB: Duplicates are not desirable but possible.
        AnchorMap_.emplace(anchor->AnchorMessage, anchor);
        anchor->NextAnchor = FirstAnchor_;
        FirstAnchor_.store(anchor);
    }

    void DoUpdateAnchor(const TLogManagerConfigPtr& config, TLoggingAnchor* anchor)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        auto isPrefixOf = [] (const std::string& message, const std::vector<std::string>& prefixes) {
            for (const auto& prefix : prefixes) {
                if (message.starts_with(prefix)) {
                    return true;
                }
            }
            return false;
        };

        auto findByPrefix = [] (const std::string& message, const THashMap<std::string, ELogLevel>& levelOverrides) -> std::optional<ELogLevel> {
            for (const auto& [prefix, level] : levelOverrides) {
                if (message.starts_with(prefix)) {
                    return level;
                }
            }
            return std::nullopt;
        };

        anchor->Suppressed.store(isPrefixOf(anchor->AnchorMessage, config->SuppressedMessages));
        anchor->LevelOverride.store(findByPrefix(anchor->AnchorMessage, config->MessageLevelOverrides));
        anchor->CurrentVersion.store(GetVersion());
    }

    static TString BuildAnchorMessage(::TSourceLocation sourceLocation, TStringBuf message)
    {
        if (message) {
            auto index = message.find_first_of('(');
            return Strip(TString(message.substr(0, index)));
        } else {
            return Format("%v:%v",
                sourceLocation.File,
                sourceLocation.Line);
        }
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> EventCount_ = New<NThreading::TEventCount>();
    const TMpscInvokerQueuePtr EventQueue_;
    const TIntrusivePtr<TLoggingThread> LoggingThread_;
    const TShutdownCookie ShutdownCookie_ = RegisterShutdownCallback(
        "LogManager",
        BIND_NO_PROPAGATE(&TImpl::Shutdown, MakeWeak(this)),
        /*priority*/ 201);

    DECLARE_THREAD_AFFINITY_SLOT(LoggingThread);

    TAtomicIntrusivePtr<TLogManagerConfig> Config_;

    // Protects the section of members below.
    NThreading::TForkAwareSpinLock SpinLock_;
    THashMap<TString, std::unique_ptr<TLoggingCategory>> NameToCategory_;
    THashMap<TString, ILogWriterFactoryPtr> TypeNameToWriterFactory_;

    // Incrementing version forces loggers to update their own default configuration (default level etc.).
    std::atomic<int> Version_ = 0;

    std::atomic<bool> DefaultConfigured_ = false;
    std::atomic<bool> ConfiguredFromEnv_ = false;

    // These are just cached (for performance reason) copies from Config_.
    // The values are being read from arbitrary threads but stale values are fine.
    std::atomic<ui64> HighBacklogWatermark_ = Max<ui64>();
    std::atomic<ui64> LowBacklogWatermark_ = Max<ui64>();
    std::atomic<bool> AbortOnAlert_ = false;

    std::atomic<bool> InitializationStarted_ = false;
    std::atomic<NThreading::TThreadId> InitializerThreadId_ = NThreading::InvalidThreadId;
    NThreading::TEvent InitializationFinished_;

    std::once_flag Started_;
    std::atomic<bool> Suspended_ = false;
    std::atomic<bool> ScheduledOutOfBand_ = false;

    THashSet<TThreadLocalQueue*> LocalQueues_;
    TMpscStack<TThreadLocalQueue*> RegisteredLocalQueues_;
    TMpscStack<TThreadLocalQueue*> UnregisteredLocalQueues_;

    TMpscStack<TLoggerQueueItem> GlobalQueue_;
    TMpscStack<TRequestId> SuppressedRequestIdQueue_;

    std::deque<TLoggerQueueItem> TimeOrderedBuffer_;
    TExpiringSet<TRequestId> SuppressedRequestIdSet_;

    using TEventProfilingKey = std::pair<TString, ELogLevel>;
    THashMap<TEventProfilingKey, TCounter> WrittenEventsCounters_;

    const TProfiler Profiler{"/logging"};
    const TGauge MinLogStorageAvailableSpace_ = Profiler.Gauge("/min_log_storage_available_space");
    const TGauge MinLogStorageFreeSpace_ = Profiler.Gauge("/min_log_storage_free_space");

    TBufferedProducerPtr AnchorBufferedProducer_;
    TInstant LastAnchorStatsCaptureTime_;

    std::atomic<ui64> EnqueuedEvents_ = 0;
    std::atomic<ui64> WrittenEvents_ = 0;
    std::atomic<ui64> FlushedEvents_ = 0;
    std::atomic<ui64> SuppressedEvents_ = 0;
    std::atomic<ui64> DroppedEvents_ = 0;

    THashMap<TString, ILogWriterPtr> NameToWriter_;
    THashMap<TLogWriterCacheKey, std::vector<ILogWriterPtr>> KeyToCachedWriter_;

    const std::vector<ILogWriterPtr> SystemWriters_;
    const TLoggingCategory* SystemCategory_;

    std::atomic<bool> ReopenRequested_ = false;
    std::atomic<bool> ShutdownRequested_ = false;
    std::atomic<bool> RequestSuppressionEnabled_ = false;

    const TPeriodicExecutorPtr DiskProfilingExecutor_;
    const TPeriodicExecutorPtr AnchorProfilingExecutor_;
    const TPeriodicExecutorPtr DequeueExecutor_;
    const TPeriodicExecutorPtr FlushExecutor_;
    const TPeriodicExecutorPtr WatchExecutor_;
    const TPeriodicExecutorPtr CheckSpaceExecutor_;
    const TPeriodicExecutorPtr FileRotationExecutor_;

    const IThreadPoolPtr CompressionThreadPool_;

    std::unique_ptr<TInotifyHandle> NotificationHandle_;
    bool NotificationHandleCreationFailed_ = false;

    THashMap<IFileLogWriterPtr, std::unique_ptr<TInotifyWatch>> WriterToNotificationWatch_;
    THashMap<int, IFileLogWriterPtr> NotificationWatchWDToWriter_;
    THashSet<IFileLogWriterPtr> WritersWithFailedNotificationWatches_;

    THashMap<TString, TLoggingAnchor*> AnchorMap_;
    std::atomic<TLoggingAnchor*> FirstAnchor_ = nullptr;
    std::vector<std::unique_ptr<TLoggingAnchor>> DynamicAnchors_;

    std::atomic<double> BacklogQueueFillFraction_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TLocalQueueReclaimer::~TLocalQueueReclaimer()
{
    if (auto& perThreadQueue = PerThreadQueue()) {
        auto logManager = TLogManager::Get()->Impl_;
        logManager->UnregisteredLocalQueues_.Enqueue(perThreadQueue);
        perThreadQueue = reinterpret_cast<TThreadLocalQueue*>(ThreadQueueDestroyedSentinel);
    }
}

////////////////////////////////////////////////////////////////////////////////

TLogManager::TLogManager()
    : Impl_(New<TImpl>())
{
    // NB: TLogManager is instantiated before main. We can't rely on global variables here.
    TProfiler{""}.AddProducer("/logging", Impl_);
}

TLogManager::~TLogManager() = default;

TLogManager* TLogManager::Get()
{
    auto* logManager = LeakySingleton<TLogManager>();
    logManager->Impl_->Initialize();
    return logManager;
}

void TLogManager::Configure(TLogManagerConfigPtr config, bool sync)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->Configure(std::move(config), /*fromEnv*/ false, sync);
}

bool TLogManager::IsDefaultConfigured()
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return false;
    }
    return Impl_->IsDefaultConfigured();
}

void TLogManager::ConfigureFromEnv()
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->ConfigureFromEnv();
}

bool TLogManager::IsConfiguredFromEnv()
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return false;
    }
    return Impl_->IsConfiguredFromEnv();
}

void TLogManager::Shutdown()
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->Shutdown();
}

int TLogManager::GetVersion() const
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return 0;
    }
    return Impl_->GetVersion();
}

bool TLogManager::GetAbortOnAlert() const
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return false;
    }
    return Impl_->GetAbortOnAlert();
}

const TLoggingCategory* TLogManager::GetCategory(TStringBuf categoryName)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return nullptr;
    }
    return Impl_->GetCategory(categoryName);
}

void TLogManager::UpdateCategory(TLoggingCategory* category)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->UpdateCategory(category);
}

void TLogManager::UpdateAnchor(TLoggingAnchor* anchor)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->UpdateAnchor(anchor);
}

void TLogManager::RegisterStaticAnchor(TLoggingAnchor* anchor, ::TSourceLocation sourceLocation, TStringBuf anchorMessage)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->RegisterStaticAnchor(anchor, sourceLocation, anchorMessage);
}

TLoggingAnchor* TLogManager::RegisterDynamicAnchor(TString anchorMessage)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return nullptr;
    }
    return Impl_->RegisterDynamicAnchor(std::move(anchorMessage));
}

void TLogManager::RegisterWriterFactory(const TString& typeName, const ILogWriterFactoryPtr& factory)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->RegisterWriterFactory(typeName, factory);
}

void TLogManager::UnregisterWriterFactory(const TString& typeName)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->UnregisterWriterFactory(typeName);
}

void TLogManager::Enqueue(TLogEvent&& event)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        Cerr << NYT::Format("Trying to log event during logger initialization -- skipping") << Endl;
        return;
    }
    Impl_->Enqueue(std::move(event));
}

void TLogManager::Reopen()
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->Reopen();
}

void TLogManager::EnableReopenOnSighup()
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->EnableReopenOnSighup();
}

void TLogManager::SuppressRequest(TRequestId requestId)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->SuppressRequest(requestId);
}

void TLogManager::Synchronize(TInstant deadline)
{
    [[unlikely]] if (!Impl_->IsInitialized()) {
        return;
    }
    Impl_->Synchronize(deadline);
}

double TLogManager::GetBacklogQueueFillFraction() const
{
    return Impl_->GetBacklogQueueFillFraction();
}

////////////////////////////////////////////////////////////////////////////////

TFiberMinLogLevelGuard::TFiberMinLogLevelGuard(ELogLevel minLogLevel)
    : OldMinLogLevel_(GetThreadMinLogLevel())
{
    SetThreadMinLogLevel(minLogLevel);
}

TFiberMinLogLevelGuard::~TFiberMinLogLevelGuard()
{
    SetThreadMinLogLevel(OldMinLogLevel_);
}

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

ILogManager* GetDefaultLogManager()
{
    return TLogManager::Get();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
