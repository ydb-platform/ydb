#pragma once

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/producer.h>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/yt/threading/event_count.h>
#include <util/thread/lfqueue.h>
#include <util/thread/factory.h>

#include <atomic>
#include <functional>
#include <limits>
#include <memory>

namespace NYdb::inline Dev::NTopic {

struct TProducerMessageInfoTestHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer

class TProducer : public IProducer,
                  public TContinuationTokenIssuer,
                  public std::enable_shared_from_this<TProducer> {
    friend struct TProducerMessageInfoTestHelper;

private:
    static constexpr size_t MAX_EPOCH = 1'000'000'000;
    static constexpr TDuration DEFAULT_START_BLOCK_TIMEOUT = TDuration::MilliSeconds(1);

    using WriteSessionPtr = std::shared_ptr<IWriteSession>;

    struct TPartitionInfo {
        using TSelf = TPartitionInfo;

        bool InRange(const std::string_view key) const;
        bool IsSplitted() const;

        FLUENT_SETTING(std::string, FromBound);
        FLUENT_SETTING(std::optional<std::string>, ToBound);
        FLUENT_SETTING(std::uint32_t, PartitionId);
        FLUENT_SETTING(std::vector<std::uint32_t>, Children);
        FLUENT_SETTING_DEFAULT(bool, Locked, false);
        FLUENT_SETTING_DEFAULT(NThreading::TFuture<void>, Future, NThreading::MakeFuture());

        std::optional<std::uint64_t> CachedMaxSeqNo;
    };

    struct TMessageInfo {
        static constexpr std::uint32_t UNKNOWN_PARTITION_ID = std::numeric_limits<std::uint32_t>::max();

        TMessageInfo() = default;
        TMessageInfo(TWriteMessage&& message);
        TMessageInfo(const std::string& key, const std::string& choosePartitionKey, TWriteMessage&& message, std::uint32_t partition);

        std::string Key;
        std::string Data;
        std::optional<ECodec> Codec;
        uint32_t OriginalSize = 0;
        std::optional<uint64_t> SeqNo;
        std::optional<TInstant> CreateTimestamp;
        TMessageMeta MessageMeta;
        std::optional<std::reference_wrapper<TTransactionBase>> Tx;
        std::optional<TDeferredPublication> DeferredPublication;
        bool HasKey = false;
        std::uint32_t Partition = UNKNOWN_PARTITION_ID;
        bool Sent = false;
        std::vector<NThreading::TPromise<TFlushResult>> FlushPromises;

        void AssignPartition(const std::string& key, const std::string& choosePartitionKey, std::uint32_t partition);
        TWriteMessage BuildMessage() const;
    };

    struct TClientRequest {
        enum class EKind {
            Message,
            Flush,
        };

        TClientRequest() = default;
        TClientRequest(TMessageInfo&& message);
        TClientRequest(NThreading::TPromise<TFlushResult>&& flushPromise);

        EKind Kind = EKind::Message;
        TMessageInfo Message;
        NThreading::TPromise<TFlushResult> FlushPromise;
    };

    struct TIdleSession;

    struct TWriteSessionWrapper {
        WriteSessionPtr Session;
        const std::uint32_t Partition;
        std::shared_ptr<TIdleSession> IdleSession = nullptr;
        bool DirectToPartition = true;
        bool Closed = false;

        // This field is used only when DirectToPartition is false.
        size_t NonDirectToPartitionOwnership = 1;

        TWriteSessionWrapper(WriteSessionPtr session, std::uint32_t partition, bool directToPartition = true);
    };

    using WrappedWriteSessionPtr = std::shared_ptr<TWriteSessionWrapper>;

    struct TIdleSession {
        TIdleSession(TWriteSessionWrapper* session, TInstant emptySince, TDuration idleTimeout)
            : Session(session)
            , EmptySince(emptySince)
            , IdleTimeout(idleTimeout)
        {}

        const TWriteSessionWrapper* Session;
        const TInstant EmptySince;
        const TDuration IdleTimeout;

        bool Less(const std::shared_ptr<TIdleSession>& other) const;
        bool IsExpired() const;

        struct Comparator {
            bool operator()(const std::shared_ptr<TIdleSession>& first, const std::shared_ptr<TIdleSession>& second) const;
        };
    };

    using IdleSessionPtr = std::shared_ptr<TIdleSession>;

    enum class ESeqNoStrategy {
        NotInitialized,
        WithoutSeqNo,
        WithSeqNo,
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Custom retry policy

    struct TProducerRetryPolicy : public ::IRetryPolicy<EStatus> {
        using TSelf = TProducerRetryPolicy;
        using TPtr = std::shared_ptr<TSelf>;

        TProducerRetryPolicy(TProducer* producer);
        ~TProducerRetryPolicy() = default;
        typename IRetryState::TPtr CreateRetryState() const override;

    private:
        TProducer* Producer;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Workers

    struct TEventsWorker;

    struct TSessionsWorker {
        TSessionsWorker(TProducer* producer);
        WrappedWriteSessionPtr GetOrCreateWriteSession(std::uint32_t partition, bool directToPartition = true);
        WrappedWriteSessionPtr GetWriteSession(std::uint32_t partition, bool directToPartition = true);
        void AddIdleSession(std::uint32_t partition);
        void DoWork();
        size_t GetSessionsCount() const;
        size_t GetIdleSessionsCount() const;
        void DestroyWriteSession(std::uint32_t partition);
        void RemoveIdleSession(std::uint32_t partition);
    
    private:
        WrappedWriteSessionPtr CreateWriteSession(std::uint32_t partition, bool directToPartition = true);
        
        using TSessionsIndexIterator = std::unordered_map<std::uint32_t, WrappedWriteSessionPtr>::iterator;

        std::string GetProducerId(std::uint32_t partitionId);

        TProducer* Producer;
        std::set<IdleSessionPtr, TIdleSession::Comparator> IdlerSessions;
        using IdlerSessionsIterator = std::set<IdleSessionPtr, TIdleSession::Comparator>::iterator;
        std::unordered_map<std::uint32_t, IdlerSessionsIterator> IdlerSessionsIndex;
        std::unordered_map<std::uint32_t, WrappedWriteSessionPtr> SessionsIndex;
        std::unordered_set<std::uint32_t> SessionsToRemove;
        std::list<WrappedWriteSessionPtr> ClosedSessionsToRemove;
    };

    struct TMessagesWorker : public std::enable_shared_from_this<TMessagesWorker> {
        TMessagesWorker(TProducer* producer);
        
        void DoWork();

        void AddMessage(const std::string& key, const std::string& choosePartitionKey, TWriteMessage&& message, std::uint32_t partition);
        void AddMessage(TMessageInfo&& message);
        void ScheduleResendMessages(std::uint32_t partition, std::uint64_t afterSeqNo);
        void RebuildPendingMessagesIndex(std::uint32_t partition);
        void HandleAck();
        void HandleContinuationToken(std::uint32_t partition, TContinuationToken&& continuationToken);
        void DropContinuationTokens(std::uint32_t partition);
        bool IsQueueEmpty() const;
        bool HasInFlightMessages() const;
        const TMessageInfo& GetFrontInFlightMessage() const;
        void SetClosedStatusToFlushPromises(std::optional<TCloseDescription> closedDescription);

    private:
        enum class EState : std::uint8_t {
            Init = 0,
            PendingSeqNo = 1,
            Ready = 2,
        };

        using MessageIter = std::list<TMessageInfo>::iterator;

        void PushInFlightMessage(std::uint32_t partition, TMessageInfo&& message);
        void PopInFlightMessage();
        bool SendMessage(WrappedWriteSessionPtr wrappedSession, const TMessageInfo& message);
        std::optional<TContinuationToken> GetContinuationToken(std::uint32_t partition);
        void RechoosePartitionIfNeeded(MessageIter message);
        bool LazyInit();
        void MoveTo(EState state);
        void HandleReadyInitSeqNoFutures();
        void FinishInit();

        TProducer* Producer;

        std::list<TMessageInfo> InFlightMessages;
        std::unordered_map<std::uint32_t, std::list<MessageIter>> InFlightMessagesIndex;
        std::unordered_map<std::uint32_t, std::list<MessageIter>> PendingMessagesIndex;
        std::unordered_map<std::uint32_t, std::list<MessageIter>> MessagesToResendIndex;
        std::unordered_map<std::uint32_t, std::deque<TContinuationToken>> ContinuationTokens;

        std::uint64_t CurrentSeqNo = 0;
        EState State = EState::Init;

        std::vector<WrappedWriteSessionPtr> InitWriteSessions;
        std::unordered_map<std::uint32_t, NThreading::TFuture<uint64_t>> InitGetMaxSeqNoFutures;

        std::mutex InitLock;
        std::vector<std::uint32_t> GotInitSeqNoPartitions;

        friend class TProducer;
    };

    struct TSplittedPartitionWorker : public std::enable_shared_from_this<TSplittedPartitionWorker> {
    private:
        enum class EState {
            Init = 0,
            PendingDescribe = 1,
            GotDescribe = 2,
            PendingMaxSeqNo = 3,
            GotMaxSeqNo = 4,
            Failed = 5,
            Done = 6,
        };

        void MoveTo(EState state);
        void UpdateMaxSeqNo(std::uint32_t partitionId, std::uint64_t maxSeqNo);
        void LaunchGetMaxSeqNoFutures(std::unique_lock<std::mutex>& lock);
        void HandleDescribeResult();

    public:
        TSplittedPartitionWorker(TProducer* producer, std::uint32_t partitionId);
        void DoWork();
        bool IsDone() const;
        bool IsInit() const;
        std::string GetStateName() const;
            
    private:
        TProducer* Producer;
        NThreading::TFuture<TDescribeTopicResult> DescribeTopicFuture;
        EState State = EState::Init;
        std::uint32_t PartitionId;
        std::uint64_t MaxSeqNo = 0;
        std::vector<WrappedWriteSessionPtr> WriteSessions;
        std::vector<WrappedWriteSessionPtr> WriteSessionsToCloseOnError;
        std::vector<std::uint32_t> WriteSessionPartitionsToDestroy;
        std::vector<NThreading::TFuture<uint64_t>> GetMaxSeqNoFutures;
        std::unordered_map<std::uint32_t, std::uint64_t> CachedMaxSeqNos;
        mutable std::mutex Lock;
        std::uint64_t NotReadyFutures = 0;
        size_t Retries = 0;
        TInstant DoneAt = TInstant::Max();
    };

    struct TEventsWorker : public std::enable_shared_from_this<TEventsWorker> {
        enum class EEventType {
            SessionClosed = 0,
            ReadyToAccept = 1,
            Ack = 2,
        };

        TEventsWorker(TProducer* producer);
        
        std::optional<NThreading::TPromise<void>> DoWork();
        NThreading::TFuture<void> WaitEvent();
        // Atomically takes ownership of the current EventsPromise and installs a fresh one.
        // The returned promise must be fulfilled (TrySetValue) by the caller — outside Lock.
        // This is the only sanctioned way to wake WaitEvent waiters; never set EventsPromise
        // from outside or recreate EventsPromise/EventsFuture in WaitEvent.
        NThreading::TPromise<void> WakeAndRotate();
        void UnsubscribeFromPartition(std::uint32_t partition);
        void SubscribeToPartition(std::uint32_t partition);
        void HandleAcksEvent(std::uint64_t partition, TWriteSessionEvent::TAcksEvent&& event);
        std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block, const std::vector<EEventType>& eventTypes = {});
        std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount = std::nullopt, const std::vector<EEventType>& eventTypes = {});
        std::list<TWriteSessionEvent::TEvent>::iterator AckQueueBegin(std::uint32_t partition);
        std::list<TWriteSessionEvent::TEvent>::iterator AckQueueEnd(std::uint32_t partition);
        std::optional<TSessionClosedEvent> GetSessionClosedEvent();
        bool RunEventLoop(WrappedWriteSessionPtr wrappedSession, std::uint32_t partition);

    private:
        void HandleSessionClosedEvent(TSessionClosedEvent&& event, std::uint32_t partition);
        void HandleReadyToAcceptEvent(std::uint32_t partition, TWriteSessionEvent::TReadyToAcceptEvent&& event);
        bool TransferEventsToOutputQueue();
        bool AddSessionClosedIfNeeded();
        std::optional<TWriteSessionEvent::TEvent> GetEventImpl(bool block, const std::vector<EEventType>& eventTypes = {});
        EEventType GetEventType(const TWriteSessionEvent::TEvent& event);

        TProducer* Producer;

        std::unordered_set<std::uint32_t> ReadyFutures;
        std::unordered_map<std::uint32_t, std::list<TWriteSessionEvent::TEvent>> PartitionsEventQueues;
        std::list<TWriteSessionEvent::TEvent> EventsOutputQueue;

        using EventsIter = std::list<TWriteSessionEvent::TEvent>::iterator;

        std::mutex Lock;
    
        NThreading::TPromise<void> EventsPromise;
        NThreading::TFuture<void> EventsFuture;

        std::optional<TSessionClosedEvent> CloseEvent;

        friend class TProducer;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Partition chooser

    struct IPartitionChooser {
        // this method returns the partition ID and the choose partition key for the given key
        // the choose partition key is used to identify the partition
        virtual std::pair<std::uint32_t, std::string> ChoosePartition(const std::string_view key) = 0;
        virtual ~IPartitionChooser() = default;
    };

    struct TBoundPartitionChooser : IPartitionChooser {
        TBoundPartitionChooser(TProducer* producer);
        std::pair<std::uint32_t, std::string> ChoosePartition(const std::string_view key) override;
    private:
        TProducer* Producer;
    };

    struct THashPartitionChooser : IPartitionChooser {
        THashPartitionChooser(std::vector<std::uint32_t>&& partitions);
        std::pair<std::uint32_t, std::string> ChoosePartition(const std::string_view key) override;
    private:
        std::vector<std::uint32_t> Partitions;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TMetricGauge {
        std::uint64_t MetricCount = 0;
        std::uint64_t Sum = 0;
        std::uint64_t Max = 0;

        long double Average();
        void Add(std::uint64_t value);
        std::uint64_t GetMax() const;
        std::uint64_t GetSum() const;
        void Clear();
    };

    struct TMetrics {
        TMetrics(TProducer* producer);

        TMetricGauge MainWorkerTimeMs;
        TMetricGauge CycleTimeMs;
        TMetricGauge WriteLagMs;
        TMetricGauge BufferFull;
        TMetricGauge IncomingMessages;
        TMetricGauge OutgoingMessages;
        std::mutex Lock;
        TProducer* Producer;

        void AddMainWorkerTime(std::uint64_t ms);
        void AddCycleTime(std::uint64_t ms);
        void AddWriteLag(std::uint64_t lagMs);
        void IncBufferFull();
        void IncIncomingMessages();
        void IncOutgoingMessages();
        void PrintMetrics();
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void RequestMainWorkerRun(std::int64_t owner);
    void RunMainWorker(std::int64_t owner);
    bool TryAcquireMainWorker();
    void WakeMainWorkerThread();
    void RunMainWorkerLoop();
    void RunMainWorkerAcquired(std::int64_t owner);

    void NonBlockingClose();

    void SetCloseDeadline(const TDuration& closeTimeout);

    TDuration GetCloseTimeout();

    void HandleAutoPartitioning(std::uint32_t partition);

    bool RunSplittedPartitionWorkers();

    void RunUserEventLoop();

    TInstant GetCloseDeadline();

    void GetSessionClosedEventAndDie(WrappedWriteSessionPtr wrappedSession, std::optional<TSessionClosedEvent> sessionClosedEvent = std::nullopt);

    TStringBuilder LogPrefix();

    void NextEpoch();

    bool TryReserveMemory(std::uint64_t size);
    void ReserveMemory(std::uint64_t size);
    void ReleaseReservedMemory(std::uint64_t size);
    std::optional<TWriteResult> ReserveMemoryForWrite(std::uint64_t size, bool checkMemory);
    void ValidateSeqNoStrategy(bool hasSeqNo);
    void DrainClientRequests();
    void HandleClientMessage(TMessageInfo&& message);
    void HandleClientFlush(NThreading::TPromise<TFlushResult> promise);

    TWriteResult WriteInternal(TWriteMessage& message, bool checkMemory);
    TWriteResult WriteToExplicitPartition(
        TWriteMessage& message,
        std::uint32_t partition,
        std::uint64_t memoryUsage,
        bool checkMemory);

    bool IsFederation(const std::string& endpoint);

public:
    TProducer(const TProducerSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);
    
    void Write(TContinuationToken&& continuationToken, TWriteMessage&& message);

    [[nodiscard]] TWriteResult Write(TWriteMessage&& message) override;

    [[nodiscard]] NThreading::TFuture<TFlushResult> Flush() override;

    TWriteStats GetWriteStats() override;

    NThreading::TFuture<void> WaitEvent();

    std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block = false);

    std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block = false, std::optional<size_t> maxEventsCount = std::nullopt);

    TCloseResult Close(TDuration closeTimeout = TDuration::Max()) override;

    TWriterCounters::TPtr GetCounters();

    std::vector<TPartitionInfo> GetPartitions() const;

    std::unordered_map<std::uint32_t, TPartitionInfo> GetPartitionsMap() const;

    std::map<std::string, std::uint32_t> GetPartitionsIndex() const;

    size_t GetSessionsCount();

    size_t GetIdleSessionsCount();

    ~TProducer();

private:
    // for logging
    std::string Id;

    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::shared_ptr<TTopicClient::TImpl> Client;
    TDbDriverStatePtr DbDriverState;

    TMetrics Metrics;

    std::unordered_map<std::uint32_t, TPartitionInfo> Partitions;
    std::map<std::string, std::uint32_t> PartitionsIndex;

    TProducerSettings Settings;
    std::atomic<ESeqNoStrategy> SeqNoStrategy = ESeqNoStrategy::NotInitialized;
    std::atomic<std::uint64_t> ReservedMemory = 0;
    TLockFreeQueue<TClientRequest> ClientRequests;
    THolder<IThreadFactory::IThread> MainWorkerThread;
    NYT::NThreading::TEventCount MainWorkerEvent;

    NThreading::TPromise<void> ClosePromise;
    NThreading::TFuture<void> CloseFuture;
    NThreading::TPromise<void> ShutdownPromise;
    NThreading::TFuture<void> ShutdownFuture;

    mutable std::mutex GlobalLock;
    std::atomic_bool Closed = false;
    std::atomic_bool Done = false;
    TInstant CloseDeadline = TInstant::Now();

    std::unique_ptr<IPartitionChooser> PartitionChooser;

    std::function<std::string(const std::string_view key)> PartitioningKeyHasher;

    std::shared_ptr<TEventsWorker> EventsWorker;
    std::shared_ptr<TSessionsWorker> SessionsWorker;
    std::unordered_map<std::uint32_t, std::shared_ptr<TSplittedPartitionWorker>> SplittedPartitionWorkers;
    std::unordered_map<std::uint32_t, std::shared_ptr<TSplittedPartitionWorker>> ReadySplittedPartitionWorkers;
    std::shared_ptr<TMessagesWorker> MessagesWorker;
    std::shared_ptr<TProducerRetryPolicy> RetryPolicy;

    // TFuture::Subscribe may invoke callback synchronously when the future is already ready.
    // Also, callbacks may arrive concurrently with the attempt to go idle.
    // Use a small state machine to avoid re-entrancy and lost wakeups.
    enum EMainWorkerState : std::uint8_t {
        Idle = 0,
        Running = 1,
        Rerun = 2,
    };
    std::atomic<std::uint8_t> MainWorkerState = Idle;
    // MainWorker has an owner, which can be:
    // - client-side wakeup, in this case the value of MainWorkerOwner is -1
    // - subsession callback, in this case the value of MainWorkerOwner is the subsession's partition ID
    std::int64_t MainWorkerOwner = -1;

    std::atomic<std::uint64_t> LastWrittenSeqNo = 0;
    std::uint64_t MessagesWritten = 0;

    std::atomic<size_t> Epoch = 0;
    
    std::list<std::pair<NThreading::TPromise<TFlushResult>, TFlushResult>> FlushPromises;

    std::vector<TEventsWorker::EEventType> EventTypesWithHandlers;
};

} // namespace NYdb::inline Dev::NTopic
