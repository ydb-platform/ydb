#include "log_manager.h"

#include "private.h"
#include "config.h"
#include "log.h"
#include "log_writer.h"
#include "log_writer_factory.h"
#include "formatter.h"
#include "file_log_writer.h"
#include "stream_log_writer.h"

#include <yt/yt/core/concurrency/profiling_helpers.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_thread.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/invoker_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/spsc_queue.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/pattern_formatter.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/signal_registry.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/hash.h>
#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/misc/tls.h>

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

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

#ifdef _linux_
    #include <sys/inotify.h>
#endif

#include <errno.h>

namespace NYT::NLogging {

using namespace NYTree;
using namespace NConcurrency;
using namespace NFS;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);

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

class TNotificationHandle
    : private TNonCopyable
{
public:
    TNotificationHandle()
        : FD_(-1)
    {
#ifdef _linux_
        FD_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        YT_VERIFY(FD_ >= 0);
#endif
    }

    ~TNotificationHandle()
    {
#ifdef _linux_
        YT_VERIFY(FD_ >= 0);
        ::close(FD_);
#endif
    }

    int Poll()
    {
#ifdef _linux_
        YT_VERIFY(FD_ >= 0);

        char buffer[sizeof(struct inotify_event) + NAME_MAX + 1];
        ssize_t rv = HandleEintr(::read, FD_, buffer, sizeof(buffer));

        if (rv < 0) {
            if (errno != EAGAIN) {
                YT_LOG_ERROR(
                    TError::FromSystem(errno),
                    "Unable to poll inotify() descriptor %v",
                    FD_);
            }
        } else if (rv > 0) {
            YT_ASSERT(rv >= static_cast<ssize_t>(sizeof(struct inotify_event)));
            struct inotify_event* event = (struct inotify_event*)buffer;

            if (event->mask & IN_DELETE_SELF) {
                YT_LOG_TRACE(
                    "Watch %v has triggered a deletion (IN_DELETE_SELF)",
                    event->wd);
            }
            if (event->mask & IN_MOVE_SELF) {
                YT_LOG_TRACE(
                    "Watch %v has triggered a movement (IN_MOVE_SELF)",
                    event->wd);
            }

            return event->wd;
        } else {
            // Do nothing.
        }
#endif
        return 0;
    }

    DEFINE_BYVAL_RO_PROPERTY(int, FD);
};

////////////////////////////////////////////////////////////////////////////////

class TNotificationWatch
    : private TNonCopyable
{
public:
    TNotificationWatch(
        TNotificationHandle* handle,
        const TString& path,
        TClosure callback)
        : FD_(handle->GetFD())
        , WD_(-1)
        , Path_(path)
        , Callback_(std::move(callback))

    {
        FD_ = handle->GetFD();
        YT_VERIFY(FD_ >= 0);

        CreateWatch();
    }

    ~TNotificationWatch()
    {
        DropWatch();
    }

    DEFINE_BYVAL_RO_PROPERTY(int, FD);
    DEFINE_BYVAL_RO_PROPERTY(int, WD);

    bool IsValid() const
    {
        return WD_ >= 0;
    }

    void Run()
    {
        // Unregister before create a new file.
        DropWatch();
        Callback_();
        // Register the newly created file.
        CreateWatch();
    }

private:
    void CreateWatch()
    {
        YT_VERIFY(WD_ <= 0);
#ifdef _linux_
        WD_ = inotify_add_watch(
            FD_,
            Path_.c_str(),
            IN_DELETE_SELF | IN_MOVE_SELF);

        if (WD_ < 0) {
            YT_LOG_ERROR(TError::FromSystem(errno), "Error registering watch for %v",
                Path_);
            WD_ = -1;
        } else if (WD_ > 0) {
            YT_LOG_TRACE("Registered watch %v for %v",
                WD_,
                Path_);
        } else {
            YT_ABORT();
        }
#else
        WD_ = -1;
#endif
    }

    void DropWatch()
    {
#ifdef _linux_
        if (WD_ > 0) {
            YT_LOG_TRACE("Unregistering watch %v for %v",
                WD_,
                Path_);
            inotify_rm_watch(FD_, WD_);
        }
#endif
        WD_ = -1;
    }

private:
    TString Path_;
    TClosure Callback_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TElement>
class TExpiringSet
{
public:
    TExpiringSet()
    {
        Reconfigure(TDuration::Zero());
    }

    explicit TExpiringSet(TDuration lifetime)
    {
        Reconfigure(lifetime);
    }

    void Update(std::vector<TElement> elements)
    {
        RemoveExpired();
        Insert(std::move(elements));
    }

    bool Contains(const TElement& element)
    {
        return Set_.contains(element);
    }

    void Reconfigure(TDuration lifetime)
    {
        Lifetime_ = DurationToCpuDuration(lifetime);
    }

    void Clear()
    {
        Set_.clear();
        ExpirationQueue_ = std::priority_queue<TPack>();
    }

private:
    struct TPack
    {
        std::vector<TElement> Elements;
        TCpuInstant ExpirationTime;

        bool operator<(const TPack& other) const
        {
            // Reversed ordering for the priority queue.
            return ExpirationTime > other.ExpirationTime;
        }
    };

    TCpuDuration Lifetime_;
    THashSet<TElement> Set_;
    std::priority_queue<TPack> ExpirationQueue_;


    void Insert(std::vector<TElement> elements)
    {
        for (const auto& element : elements) {
            Set_.insert(element);
        }

        ExpirationQueue_.push(TPack{std::move(elements), GetCpuInstant() + Lifetime_});
    }

    void RemoveExpired()
    {
        auto now = GetCpuInstant();
        while (!ExpirationQueue_.empty() && ExpirationQueue_.top().ExpirationTime < now) {
            for (const auto& element : ExpirationQueue_.top().Elements) {
                Set_.erase(element);
            }

            ExpirationQueue_.pop();
        }
    }
};

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
YT_THREAD_LOCAL(TThreadLocalQueue*) PerThreadQueue;

/////////////////////////////////////////////////////////////////////////////

struct TLocalQueueReclaimer
{
    ~TLocalQueueReclaimer();
};

YT_THREAD_LOCAL(TLocalQueueReclaimer) LocalQueueReclaimer;

/////////////////////////////////////////////////////////////////////////////

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
        , LoggingThread_(New<TThread>(this))
        , SystemWriters_({
            CreateStderrLogWriter(
                std::make_unique<TPlainTextLogFormatter>(),
                TString(StderrSystemWriterName))
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
        RegisterWriterFactory(TString(TFileLogWriterConfig::Type), GetFileLogWriterFactory());
        RegisterWriterFactory(TString(TStderrLogWriterConfig::Type), GetStderrLogWriterFactory());
    }

    void Initialize()
    {
        std::call_once(Initialized_, [&] {
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
            }

            SystemCategory_ = GetCategory(SystemLoggingCategoryName);
        });
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

        EnsureStarted();

        TConfigEvent event{
            .Instant = GetCpuInstant(),
            .Config = std::move(config),
            .FromEnv = fromEnv
        };

        auto future = event.Promise.ToFuture();

        PushEvent(std::move(event));

        DequeueExecutor_->ScheduleOutOfBand();

        if (sync) {
            future.Get().ThrowOnError();
        }
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

        if (LoggingThread_->GetThreadId() == GetCurrentThreadId()) {
            FlushWriters();
        } else {
            // Wait for all previously enqueued messages to be flushed
            // but no more than ShutdownGraceTimeout to prevent hanging.
            Synchronize(TInstant::Now() + Config_->ShutdownGraceTimeout);
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
        auto it = NameToCategory_.find(categoryName);
        if (it == NameToCategory_.end()) {
            auto category = std::make_unique<TLoggingCategory>();
            category->Name = categoryName;
            category->ActualVersion = &Version_;
            it = NameToCategory_.emplace(categoryName, std::move(category)).first;
            DoUpdateCategory(it->second.get());
        }
        return it->second.get();
    }

    void UpdateCategory(TLoggingCategory* category)
    {
        auto guard = Guard(SpinLock_);
        DoUpdateCategory(category);
    }

    void UpdateAnchor(TLoggingAnchor* anchor)
    {
        auto guard = Guard(SpinLock_);
        bool enabled = true;
        for (const auto& prefix : Config_->SuppressedMessages) {
            if (anchor->AnchorMessage.StartsWith(prefix)) {
                enabled = false;
                break;
            }
        }

        anchor->Enabled.store(enabled, std::memory_order::relaxed);
        anchor->CurrentVersion.store(GetVersion(), std::memory_order::relaxed);
    }

    void RegisterStaticAnchor(TLoggingAnchor* anchor, ::TSourceLocation sourceLocation, TStringBuf message)
    {
        if (anchor->Registered.exchange(true)) {
            return;
        }

        auto guard = Guard(SpinLock_);
        anchor->SourceLocation = sourceLocation;
        anchor->AnchorMessage = BuildAnchorMessage(sourceLocation, message);
        DoRegisterAnchor(anchor);
    }

    TLoggingAnchor* RegisterDynamicAnchor(TString anchorMessage)
    {
        auto guard = Guard(SpinLock_);
        if (auto it = AnchorMap_.find(anchorMessage)) {
            return it->second;
        }
        auto anchor = std::make_unique<TLoggingAnchor>();
        anchor->Registered = true;
        anchor->AnchorMessage = std::move(anchorMessage);
        auto* rawAnchor = anchor.get();
        DynamicAnchors_.push_back(std::move(anchor));
        DoRegisterAnchor(rawAnchor);
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
        if (!RequestSuppressionEnabled_) {
            return;
        }

        SuppressedRequestIdQueue_.Enqueue(requestId);
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

private:
    class TThread
        : public TSchedulerThread
    {
    public:
        explicit TThread(TImpl* owner)
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
            VERIFY_THREAD_AFFINITY(Owner_->LoggingThread);

            return BeginExecuteImpl(Owner_->EventQueue_->BeginExecute(&CurrentAction_), &CurrentAction_);
        }

        void EndExecute() override
        {
            VERIFY_THREAD_AFFINITY(Owner_->LoggingThread);

            Owner_->EventQueue_->EndExecute(&CurrentAction_);
        }
    };

    void EnsureStarted()
    {
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

    const std::vector<ILogWriterPtr>& GetWriters(const TLogEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (event.Category == SystemCategory_) {
            return SystemWriters_;
        }

        TLogWriterCacheKey cacheKey{event.Category->Name, event.Level, event.Family};
        auto it = KeyToCachedWriter_.find(cacheKey);
        if (it != KeyToCachedWriter_.end()) {
            return it->second;
        }

        THashSet<TString> writerNames;
        for (const auto& rule : Config_->Rules) {
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

    std::unique_ptr<TNotificationWatch> CreateNotificationWatch(
        const TLogManagerConfigPtr& config,
        const IFileLogWriterPtr& writer)
    {
#ifdef _linux_
        if (config->WatchPeriod) {
            if (!NotificationHandle_) {
                NotificationHandle_ = std::make_unique<TNotificationHandle>();
            }
            return std::unique_ptr<TNotificationWatch>(
                new TNotificationWatch(
                    NotificationHandle_.get(),
                    writer->GetFileName().c_str(),
                    BIND(&ILogWriter::Reload, writer)));
        }
#else
        Y_UNUSED(config, writer);
#endif
        return nullptr;
    }

    void UpdateConfig(const TConfigEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (ShutdownRequested_) {
            return;
        }

        if (LoggingThread_->IsStopping()) {
            return;
        }

        AbortOnAlert_.store(event.Config->AbortOnAlert);

        EnsureStarted();

        FlushWriters();

        try {
            DoUpdateConfig(event.Config, event.FromEnv);
            event.Promise.Set();
        } catch (const std::exception& ex) {
            event.Promise.Set(ex);
        }
    }

    std::unique_ptr<ILogFormatter> CreateFormatter(const TLogWriterConfigPtr& writerConfig)
    {
        switch (writerConfig->Format) {
            case ELogFormat::PlainText:
                return std::make_unique<TPlainTextLogFormatter>(
                    writerConfig->AreSystemMessagesEnabled(),
                    writerConfig->EnableSourceLocation);

            case ELogFormat::Json: [[fallthrough]];
            case ELogFormat::Yson:
                return std::make_unique<TStructuredLogFormatter>(
                    writerConfig->Format,
                    writerConfig->CommonFields,
                    writerConfig->AreSystemMessagesEnabled(),
                    writerConfig->EnableSourceLocation,
                    writerConfig->EnableSystemFields,
                    writerConfig->JsonFormat);

            default:
                YT_ABORT();
        }
    }

    void DoUpdateConfig(const TLogManagerConfigPtr& config, bool fromEnv)
    {
        if (AreNodesEqual(ConvertToNode(Config_), ConvertToNode(config))) {
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
        }

        NameToWriter_.clear();
        KeyToCachedWriter_.clear();
        WDToNotificationWatch_.clear();
        NotificationWatches_.clear();
        InvalidNotificationWatches_.clear();

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
                auto watch = CreateNotificationWatch(config, fileWriter);
                if (watch) {
                    RegisterNotificatonWatch(watch.get());
                    NotificationWatches_.push_back(std::move(watch));
                }
            }
        }
        for (const auto& [_, category] : NameToCategory_) {
            category->StructuredValidationSamplingRate.store(config->StructuredValidationSamplingRate, std::memory_order::relaxed);
        }

        Config_ = config;
        ConfiguredFromEnv_.store(fromEnv);
        HighBacklogWatermark_.store(Config_->HighBacklogWatermark);
        LowBacklogWatermark_.store(Config_->LowBacklogWatermark);
        RequestSuppressionEnabled_.store(Config_->RequestSuppressionTimeout != TDuration::Zero());

        CompressionThreadPool_->Configure(Config_->CompressionThreadCount);

        if (RequestSuppressionEnabled_) {
            SuppressedRequestIdSet_.Reconfigure((Config_->RequestSuppressionTimeout + DequeuePeriod) * 2);
        } else {
            SuppressedRequestIdSet_.Clear();
            SuppressedRequestIdQueue_.DequeueAll();
        }

        FlushExecutor_->SetPeriod(Config_->FlushPeriod);
        WatchExecutor_->SetPeriod(Config_->WatchPeriod);
        CheckSpaceExecutor_->SetPeriod(Config_->CheckSpacePeriod);
        FileRotationExecutor_->SetPeriod(Config_->RotationCheckPeriod);

        Version_++;
    }

    void WriteEvent(const TLogEvent& event)
    {
        if (ReopenRequested_.exchange(false)) {
            ReloadWriters();
        }

        GetWrittenEventsCounter(event).Increment();

        for (const auto& writer : GetWriters(event)) {
            writer->Write(event);
        }
    }

    void FlushWriters()
    {
        for (const auto& [name, writer] : NameToWriter_) {
            writer->Flush();
        }
        FlushedEvents_ = WrittenEvents_.load();
    }

    void RotateFiles()
    {
        for (const auto& [name, writer] : NameToWriter_) {
            if (auto fileWriter = DynamicPointerCast<IFileLogWriter>(writer)) {
                fileWriter->MaybeRotate();
            }
        }
    }

    void ReloadWriters()
    {
        Version_++;
        for (const auto& [name, writer] : NameToWriter_) {
            writer->Reload();
        }
    }

    void CheckSpace()
    {
        for (const auto& [name, writer] : NameToWriter_) {
            if (auto fileWriter = DynamicPointerCast<IFileLogWriter>(writer)) {
                fileWriter->CheckSpace(Config_->MinDiskSpace);
            }
        }
    }

    void RegisterNotificatonWatch(TNotificationWatch* watch)
    {
        if (watch->IsValid()) {
            // Watch can fail to initialize if the writer is disabled
            // e.g. due to the lack of space.
            EmplaceOrCrash(WDToNotificationWatch_, watch->GetWD(), watch);
        } else {
            InvalidNotificationWatches_.push_back(watch);
        }
    }

    void WatchWriters()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (!NotificationHandle_) {
            return;
        }

        int previousWD = -1, currentWD = -1;
        while ((currentWD = NotificationHandle_->Poll()) > 0) {
            if (currentWD == previousWD) {
                continue;
            }
            auto it = WDToNotificationWatch_.find(currentWD);
            auto jt = WDToNotificationWatch_.end();
            if (it == jt) {
                continue;
            }

            auto* watch = it->second;
            watch->Run();

            if (watch->GetWD() != currentWD) {
                WDToNotificationWatch_.erase(it);
                RegisterNotificatonWatch(watch);
            }

            previousWD = currentWD;
        }
        // Handle invalid watches, try to register they again.
        {
            std::vector<TNotificationWatch*> invalidNotificationWatches;
            invalidNotificationWatches.swap(InvalidNotificationWatches_);
            for (auto* watch : invalidNotificationWatches) {
                watch->Run();
                RegisterNotificatonWatch(watch);
            }
        }
    }

    void PushEvent(TLoggerQueueItem&& event)
    {
        if (!PerThreadQueue) {
            PerThreadQueue = new TThreadLocalQueue();
            RegisteredLocalQueues_.Enqueue(GetTlsRef(PerThreadQueue));
            Y_UNUSED(LocalQueueReclaimer); // Touch thread-local variable so that its destructor is called.
        }

        ++EnqueuedEvents_;
        if (PerThreadQueue == reinterpret_cast<TThreadLocalQueue*>(ThreadQueueDestroyedSentinel)) {
            GlobalQueue_.Enqueue(std::move(event));
        } else {
            PerThreadQueue->Push(std::move(event));
        }
    }

    const TCounter& GetWrittenEventsCounter(const TLogEvent& event)
    {
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
        auto now = TInstant::Now();
        auto deltaSeconds = (now - LastAnchorStatsCaptureTime_).SecondsFloat();
        LastAnchorStatsCaptureTime_ = now;

        std::vector<TLoggingAnchorStat> result;
        auto* currentAnchor = FirstAnchor_.load();
        while (currentAnchor) {
            auto getRate = [&] (auto& counter) {
                auto current = counter.Current.load(std::memory_order::relaxed);
                auto rate = (current - counter.Previous) / deltaSeconds;
                counter.Previous = current;
                return rate;
            };

            auto messageRate = getRate(currentAnchor->MessageCounter);
            auto byteRate = getRate(currentAnchor->ByteCounter);
            result.push_back({
                currentAnchor,
                messageRate,
                byteRate
            });

            currentAnchor = currentAnchor->NextAnchor;
        }
        return result;
    }

    void OnAnchorProfiling()
    {
        if (Config_->EnableAnchorProfiling && !AnchorBufferedProducer_) {
            AnchorBufferedProducer_ = New<TBufferedProducer>();
            Profiler
                .WithSparse()
                .WithDefaultDisabled()
                .WithProducerRemoveSupport()
                .AddProducer("/anchors", AnchorBufferedProducer_);
        } else if (!Config_->EnableAnchorProfiling && AnchorBufferedProducer_) {
            AnchorBufferedProducer_.Reset();
        }

        if (!AnchorBufferedProducer_) {
            return;
        }

        auto stats = CaptureAnchorStats();

        TSensorBuffer sensorBuffer;
        for (const auto& stat : stats) {
            if (stat.MessageRate < Config_->MinLoggedMessageRateToProfile) {
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
        VERIFY_THREAD_AFFINITY(LoggingThread);

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
            THeapItem topItem = heap.back();
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

        if (!Config_->FlushPeriod || ShutdownRequested_) {
            FlushWriters();
        }
    }

    int ProcessTimeOrderedBuffer()
    {
        int eventsWritten = 0;
        int eventsSuppressed = 0;

        SuppressedRequestIdSet_.Update(SuppressedRequestIdQueue_.DequeueAll());

        auto requestSuppressionEnabled = RequestSuppressionEnabled_.load(std::memory_order::relaxed);
        auto deadline = GetCpuInstant() - DurationToCpuDuration(Config_->RequestSuppressionTimeout);

        while (!TimeOrderedBuffer_.empty()) {
            const auto& event = TimeOrderedBuffer_.front();

            if (requestSuppressionEnabled && GetEventInstant(event) > deadline) {
                break;
            }

            ++eventsWritten;

            Visit(event,
                [&] (const TConfigEvent& event) {
                    return UpdateConfig(event);
                },
                [&] (const TLogEvent& event) {
                    if (requestSuppressionEnabled && event.RequestId && SuppressedRequestIdSet_.Contains(event.RequestId)) {
                        ++eventsSuppressed;
                    } else {
                        WriteEvent(event);
                    }
                });

            TimeOrderedBuffer_.pop_front();
        }

        SuppressedEvents_ += eventsSuppressed;

        return eventsWritten;
    }

    void DoUpdateCategory(TLoggingCategory* category)
    {
        auto minPlainTextLevel = ELogLevel::Maximum;
        for (const auto& rule : Config_->Rules) {
            if (rule->IsApplicable(category->Name, ELogFamily::PlainText)) {
                minPlainTextLevel = std::min(minPlainTextLevel, rule->MinLevel);
            }
        }

        category->MinPlainTextLevel.store(minPlainTextLevel, std::memory_order::relaxed);
        category->CurrentVersion.store(GetVersion(), std::memory_order::relaxed);
        category->StructuredValidationSamplingRate.store(Config_->StructuredValidationSamplingRate, std::memory_order::relaxed);
    }

    void DoRegisterAnchor(TLoggingAnchor* anchor)
    {
        // NB: Duplicates are not desirable but possible.
        AnchorMap_.emplace(anchor->AnchorMessage, anchor);
        anchor->NextAnchor = FirstAnchor_;
        FirstAnchor_.store(anchor);
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
    const TIntrusivePtr<TThread> LoggingThread_;
    const TShutdownCookie ShutdownCookie_ = RegisterShutdownCallback(
        "LogManager",
        BIND_NO_PROPAGATE(&TImpl::Shutdown, MakeWeak(this)),
        /*priority*/ 201);

    DECLARE_THREAD_AFFINITY_SLOT(LoggingThread);

    // Configuration.
    NThreading::TForkAwareSpinLock SpinLock_;
    // Version forces this very module's Logger object to update to our own
    // default configuration (default level etc.).
    std::atomic<int> Version_ = 0;
    std::atomic<bool> AbortOnAlert_ = false;
    TLogManagerConfigPtr Config_;
    std::atomic<bool> ConfiguredFromEnv_ = false;
    THashMap<TString, std::unique_ptr<TLoggingCategory>> NameToCategory_;
    THashMap<TString, ILogWriterFactoryPtr> TypeNameToWriterFactory_;
    const TLoggingCategory* SystemCategory_;
    // These are just copies from Config_.
    // The values are being read from arbitrary threads but stale values are fine.
    std::atomic<ui64> HighBacklogWatermark_ = Max<ui64>();
    std::atomic<ui64> LowBacklogWatermark_ = Max<ui64>();

    std::once_flag Initialized_;
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

    TGauge MinLogStorageAvailableSpace_ = Profiler.Gauge("/min_log_storage_available_space");
    TGauge MinLogStorageFreeSpace_ = Profiler.Gauge("/min_log_storage_free_space");

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

    std::unique_ptr<TNotificationHandle> NotificationHandle_;
    std::vector<std::unique_ptr<TNotificationWatch>> NotificationWatches_;
    THashMap<int, TNotificationWatch*> WDToNotificationWatch_;
    std::vector<TNotificationWatch*> InvalidNotificationWatches_;

    THashMap<TString, TLoggingAnchor*> AnchorMap_;
    std::atomic<TLoggingAnchor*> FirstAnchor_ = nullptr;
    std::vector<std::unique_ptr<TLoggingAnchor>> DynamicAnchors_;
};

////////////////////////////////////////////////////////////////////////////////

TLocalQueueReclaimer::~TLocalQueueReclaimer()
{
    if (PerThreadQueue) {
        auto logManager = TLogManager::Get()->Impl_;
        logManager->UnregisteredLocalQueues_.Enqueue(GetTlsRef(PerThreadQueue));
        PerThreadQueue = reinterpret_cast<TThreadLocalQueue*>(ThreadQueueDestroyedSentinel);
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
    logManager->Initialize();
    return logManager;
}

void TLogManager::Configure(TLogManagerConfigPtr config, bool sync)
{
    Impl_->Configure(std::move(config), /*fromEnv*/ false, sync);
}

void TLogManager::ConfigureFromEnv()
{
    Impl_->ConfigureFromEnv();
}

bool TLogManager::IsConfiguredFromEnv()
{
    return Impl_->IsConfiguredFromEnv();
}

void TLogManager::Shutdown()
{
    Impl_->Shutdown();
}

int TLogManager::GetVersion() const
{
    return Impl_->GetVersion();
}

bool TLogManager::GetAbortOnAlert() const
{
    return Impl_->GetAbortOnAlert();
}

const TLoggingCategory* TLogManager::GetCategory(TStringBuf categoryName)
{
    return Impl_->GetCategory(categoryName);
}

void TLogManager::UpdateCategory(TLoggingCategory* category)
{
    Impl_->UpdateCategory(category);
}

void TLogManager::UpdateAnchor(TLoggingAnchor* anchor)
{
    Impl_->UpdateAnchor(anchor);
}

void TLogManager::RegisterStaticAnchor(TLoggingAnchor* anchor, ::TSourceLocation sourceLocation, TStringBuf anchorMessage)
{
    Impl_->RegisterStaticAnchor(anchor, sourceLocation, anchorMessage);
}

TLoggingAnchor* TLogManager::RegisterDynamicAnchor(TString anchorMessage)
{
    return Impl_->RegisterDynamicAnchor(std::move(anchorMessage));
}

void TLogManager::RegisterWriterFactory(const TString& typeName, const ILogWriterFactoryPtr& factory)
{
    Impl_->RegisterWriterFactory(typeName, factory);
}

void TLogManager::UnregisterWriterFactory(const TString& typeName)
{
    Impl_->UnregisterWriterFactory(typeName);
}

void TLogManager::Enqueue(TLogEvent&& event)
{
    Impl_->Enqueue(std::move(event));
}

void TLogManager::Reopen()
{
    Impl_->Reopen();
}

void TLogManager::EnableReopenOnSighup()
{
    Impl_->EnableReopenOnSighup();
}

void TLogManager::SuppressRequest(TRequestId requestId)
{
    Impl_->SuppressRequest(requestId);
}

void TLogManager::Synchronize(TInstant deadline)
{
    Impl_->Synchronize(deadline);
}

void TLogManager::Initialize()
{
    Impl_->Initialize();
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
