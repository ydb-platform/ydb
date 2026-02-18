#pragma once

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/producer.h>

#include <library/cpp/threading/future/future.h>

#include <atomic>
#include <functional>
#include <memory>

namespace NYdb::inline Dev::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer

class TProducer : public IKeyedWriteSession,
                  public IProducer,
                  public TContinuationTokenIssuer,
                  public std::enable_shared_from_this<TProducer> {
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
    };

    struct TMessageInfo {
        TMessageInfo(const std::string& key, TWriteMessage&& message, std::uint32_t partition, TTransactionBase* tx);

        std::string Key;
        std::string Data;
        std::optional<ECodec> Codec;
        uint32_t OriginalSize = 0;
        std::optional<uint64_t> SeqNo;
        std::optional<TInstant> CreateTimestamp;
        TMessageMeta MessageMeta;
        std::optional<std::reference_wrapper<TTransactionBase>> TxInMessage;
        TTransactionBase* Tx;
        std::uint32_t Partition;
        bool Sent = false;
        NThreading::TPromise<EFlushResult> FlushPromise;

        TWriteMessage BuildMessage() const;
    };

    struct TIdleSession;

    struct TWriteSessionWrapper {
        WriteSessionPtr Session;
        const std::uint32_t Partition;
        std::uint64_t QueueSize = 0;
        std::shared_ptr<TIdleSession> IdleSession = nullptr;

        TWriteSessionWrapper(WriteSessionPtr session, std::uint32_t partition);

        bool IsQueueEmpty() const;
        bool AddToQueue(std::uint64_t delta);
        bool RemoveFromQueue(std::uint64_t delta);
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
        WrappedWriteSessionPtr GetWriteSession(std::uint32_t partition, bool directToPartition = true);
        void OnReadFromSession(WrappedWriteSessionPtr wrappedSession);
        void OnWriteToSession(WrappedWriteSessionPtr wrappedSession);
        void DoWork();
    
    private:
        void AddIdleSession(WrappedWriteSessionPtr wrappedSession, TInstant emptySince, TDuration idleTimeout);
        void RemoveIdleSession(std::uint32_t partition);
        WrappedWriteSessionPtr CreateWriteSession(std::uint32_t partition, bool directToPartition = true);
        
        using TSessionsIndexIterator = std::unordered_map<std::uint32_t, WrappedWriteSessionPtr>::iterator;
        void DestroyWriteSession(TSessionsIndexIterator& it, TDuration closeTimeout, bool mustBeEmpty = true);

        std::string GetProducerId(std::uint32_t partitionId);

        TProducer* Producer;
        std::set<IdleSessionPtr, TIdleSession::Comparator> IdlerSessions;
        using IdlerSessionsIterator = std::set<IdleSessionPtr, TIdleSession::Comparator>::iterator;
        std::unordered_map<std::uint32_t, IdlerSessionsIterator> IdlerSessionsIndex;
        std::unordered_map<std::uint32_t, WrappedWriteSessionPtr> SessionsIndex;
    };

    struct TMessagesWorker {
        TMessagesWorker(TProducer* producer);
        
        void DoWork();

        void AddMessage(const std::string& key, TWriteMessage&& message, std::uint32_t partition, TTransactionBase* tx);
        void ScheduleResendMessages(std::uint32_t partition, std::uint64_t afterSeqNo);
        void RebuildPendingMessagesIndex(std::uint32_t partition);
        void HandleAck();
        void HandleContinuationToken(std::uint32_t partition, TContinuationToken&& continuationToken);
        bool IsMemoryUsageOK() const;
        bool IsQueueEmpty() const;
        bool HasInFlightMessages() const;
        const TMessageInfo& GetFrontInFlightMessage() const;
        void SetStatusToFlushPromises(EFlushResult status);

    private:
        using MessageIter = std::list<TMessageInfo>::iterator;

        void PushInFlightMessage(std::uint32_t partition, TMessageInfo&& message);
        void PopInFlightMessage();
        bool SendMessage(WrappedWriteSessionPtr wrappedSession, const TMessageInfo& message);
        std::optional<TContinuationToken> GetContinuationToken(std::uint32_t partition);
        void RechoosePartitionIfNeeded(MessageIter message);

        TProducer* Producer;

        std::list<TMessageInfo> InFlightMessages;
        std::unordered_map<std::uint32_t, std::list<MessageIter>> InFlightMessagesIndex;
        std::unordered_map<std::uint32_t, std::list<MessageIter>> PendingMessagesIndex;
        std::unordered_map<std::uint32_t, std::list<MessageIter>> MessagesToResendIndex;
        std::unordered_map<std::uint32_t, std::deque<TContinuationToken>> ContinuationTokens;
        
        std::uint64_t MemoryUsage = 0;

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
            Done = 5,
        };

        void MoveTo(EState state);
        void UpdateMaxSeqNo(uint64_t maxSeqNo);
        void LaunchGetMaxSeqNoFutures(std::unique_lock<std::mutex>& lock);
        void HandleDescribeResult();

    public:
        TSplittedPartitionWorker(TProducer* producer, std::uint32_t partitionId);
        void DoWork();
        bool IsDone();
        bool IsInit();
        bool CanBeRemoved();
        std::string GetStateName() const;
            
    private:
        TProducer* Producer;
        NThreading::TFuture<TDescribeTopicResult> DescribeTopicFuture;
        EState State = EState::Init;
        std::uint32_t PartitionId;
        std::uint64_t MaxSeqNo = 0;
        std::vector<WrappedWriteSessionPtr> WriteSessions;
        std::vector<NThreading::TFuture<uint64_t>> GetMaxSeqNoFutures;
        std::mutex Lock;
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
        void UnsubscribeFromPartition(std::uint32_t partition);
        void SubscribeToPartition(std::uint32_t partition);
        std::optional<NThreading::TPromise<void>> HandleNewMessage();
        void HandleAcksEvent(std::uint64_t partition, TWriteSessionEvent::TAcksEvent&& event);
        std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block, const std::vector<EEventType>& eventTypes = {});
        std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount = std::nullopt, const std::vector<EEventType>& eventTypes = {});
        std::list<TWriteSessionEvent::TEvent>::iterator AckQueueBegin(std::uint32_t partition);
        std::list<TWriteSessionEvent::TEvent>::iterator AckQueueEnd(std::uint32_t partition);
        std::optional<TContinuationToken> GetContinuationToken();
        std::optional<TSessionClosedEvent> GetSessionClosedEvent();

    private:
        void HandleSessionClosedEvent(TSessionClosedEvent&& event, std::uint32_t partition);
        void HandleReadyToAcceptEvent(std::uint32_t partition, TWriteSessionEvent::TReadyToAcceptEvent&& event);
        bool RunEventLoop(WrappedWriteSessionPtr wrappedSession, std::uint32_t partition);
        bool TransferEventsToOutputQueue();
        void AddReadyToAcceptEvent();
        bool AddSessionClosedIfNeeded();
        std::optional<TWriteSessionEvent::TEvent> GetEventImpl(bool block, const std::vector<EEventType>& eventTypes = {});
        EEventType GetEventType(const TWriteSessionEvent::TEvent& event);

        TProducer* Producer;

        std::unordered_set<std::uint32_t> ReadyFutures;
        std::unordered_map<std::uint32_t, std::list<TWriteSessionEvent::TEvent>> PartitionsEventQueues;
        std::list<TWriteSessionEvent::TEvent> EventsOutputQueue;

        using EventsIter = std::list<TWriteSessionEvent::TEvent>::iterator;
        std::list<EventsIter> ContinuationTokensIndex;

        std::mutex Lock;
    
        NThreading::TPromise<void> EventsPromise;
        NThreading::TFuture<void> EventsFuture;

        std::optional<TSessionClosedEvent> CloseEvent;

        friend class TProducer;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Partition chooser

    struct IPartitionChooser {
        virtual std::uint32_t ChoosePartition(const std::string_view key) = 0;
        virtual ~IPartitionChooser() = default;
    };

    struct TBoundPartitionChooser : IPartitionChooser {
        TBoundPartitionChooser(TProducer* producer);
        std::uint32_t ChoosePartition(const std::string_view key) override;
    private:
        TProducer* Producer;
    };

    struct THashPartitionChooser : IPartitionChooser {
        THashPartitionChooser(std::vector<std::uint32_t>&& partitions);
        std::uint32_t ChoosePartition(const std::string_view key) override;
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
        TMetricGauge ContinuationTokensSent;
        TMetricGauge BufferFull;
        TMetricGauge IncomingMessages;
        TMetricGauge OutgoingMessages;
        std::mutex Lock;
        TProducer* Producer;

        void AddMainWorkerTime(std::uint64_t ms);
        void AddCycleTime(std::uint64_t ms);
        void AddWriteLag(std::uint64_t lagMs);
        void IncContinuationTokensSent();
        void IncBufferFull();
        void IncIncomingMessages();
        void IncOutgoingMessages();
        void PrintMetrics();
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void RunMainWorker();

    void NonBlockingClose();

    void SetCloseDeadline(const TDuration& closeTimeout);

    TDuration GetCloseTimeout();

    std::string GetProducerId(std::uint32_t partition);

    void HandleAutoPartitioning(std::uint32_t partition);

    bool RunSplittedPartitionWorkers();

    void RunUserEventLoop();

    TInstant GetCloseDeadline();

    void GetSessionClosedEventAndDie(WrappedWriteSessionPtr wrappedSession, std::optional<TSessionClosedEvent> sessionClosedEvent = std::nullopt);

    TStringBuilder LogPrefix();

    void NextEpoch();

    std::uint32_t ChooseRandomPartition();

    EWriteResult WriteImpl(const std::string& key, TWriteMessage&& message,
               TTransactionBase* tx = nullptr);

public:
    TProducer(const TProducerSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);
    
    void Write(TContinuationToken&& continuationToken, const std::string& key, TWriteMessage&& message,
               TTransactionBase* tx = nullptr) override;

    [[nodiscard]] EWriteResult Write(TWriteMessage&& message,
               TTransactionBase* tx = nullptr) override;
    
    [[nodiscard]] EWriteResult Write(const std::string& key, TWriteMessage&& message,
               TTransactionBase* tx = nullptr) override;

    std::optional<TCloseDescription> ExplainClosed() override;

    NThreading::TFuture<EFlushResult> Flush() override;

    [[nodiscard]] EFlushResult FlushAndWait(TDuration timeout = TDuration::Max()) override;

    NThreading::TFuture<void> WaitEvent() override;

    std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block = false) override;

    std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block = false, std::optional<size_t> maxEventsCount = std::nullopt) override;

    ECloseResult Close(TDuration closeTimeout = TDuration::Max()) override;

    TWriterCounters::TPtr GetCounters() override;

    std::vector<TPartitionInfo> GetPartitions() const;

    std::unordered_map<std::uint32_t, TPartitionInfo> GetPartitionsMap() const;

    std::map<std::string, std::uint32_t> GetPartitionsIndex() const;

    ~TProducer();

private:
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::shared_ptr<TTopicClient::TImpl> Client;
    TDbDriverStatePtr DbDriverState;

    TMetrics Metrics;

    std::unordered_map<std::uint32_t, TPartitionInfo> Partitions;
    std::map<std::string, std::uint32_t> PartitionsIndex;

    TProducerSettings Settings;
    ESeqNoStrategy SeqNoStrategy = ESeqNoStrategy::NotInitialized;
    TProducerSettings::EPartitionChooserStrategy PartitionChooserStrategy = TProducerSettings::EPartitionChooserStrategy::Hash;

    NThreading::TPromise<void> ClosePromise;
    NThreading::TFuture<void> CloseFuture;
    NThreading::TPromise<void> ShutdownPromise;
    NThreading::TFuture<void> ShutdownFuture;

    std::mutex GlobalLock;
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
    std::atomic<std::uint8_t> MainWorkerState = 0;
    std::atomic<size_t> Epoch = 0;
    std::mt19937_64 RandomGenerator = std::mt19937_64(std::random_device()());
    
    std::list<NThreading::TPromise<EFlushResult>> FlushPromises;

    std::vector<TEventsWorker::EEventType> EventTypesWithHandlers;
};

} // namespace NYdb::inline Dev::NTopic
