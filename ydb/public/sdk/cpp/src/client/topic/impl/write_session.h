#pragma once

#include <util/system/thread.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>

#include <atomic>
#include <deque>
#include <functional>
#include <memory>

namespace NYdb::inline Dev::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSession

class TWriteSession : public IWriteSession,
                      public TContextOwner<TWriteSessionImpl> {
private:
    friend class TSimpleBlockingWriteSession;
    friend class TTopicClient;

public:
    TWriteSession(const TWriteSessionSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);

    std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block = false) override;
    std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block = false,
                                                  std::optional<size_t> maxEventsCount = std::nullopt) override;
    NThreading::TFuture<uint64_t> GetInitSeqNo() override;

    void Write(TContinuationToken&& continuationToken, std::string_view data,
               std::optional<uint64_t> seqNo = std::nullopt, std::optional<TInstant> createTimestamp = std::nullopt) override;

    void WriteEncoded(TContinuationToken&& continuationToken, std::string_view data, ECodec codec, ui32 originalSize,
               std::optional<uint64_t> seqNo = std::nullopt, std::optional<TInstant> createTimestamp = std::nullopt) override;

    void Write(TContinuationToken&& continuationToken, TWriteMessage&& message,
               TTransactionBase* tx = nullptr) override;

    void WriteEncoded(TContinuationToken&& continuationToken, TWriteMessage&& message,
                      TTransactionBase* tx = nullptr) override;

    NThreading::TFuture<void> WaitEvent() override;

    // Empty maybe - block till all work is done. Otherwise block at most at closeTimeout duration.
    bool Close(TDuration closeTimeout = TDuration::Max()) override;

    TWriterCounters::TPtr GetCounters() override {Y_ABORT("Unimplemented"); } //ToDo - unimplemented;

    ~TWriteSession(); // will not call close - destroy everything without acks

private:
    void Start(const TDuration& delay);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession

class TKeyedWriteSession : public IKeyedWriteSession,
                           public TContinuationTokenIssuer,
                           public std::enable_shared_from_this<TKeyedWriteSession> {
private:
    using WriteSessionPtr = std::shared_ptr<IWriteSession>;

    struct TPartitionInfo {
        using TSelf = TPartitionInfo;

        bool InRange(const std::string_view key) const;
        bool operator<(const std::string_view key) const;

        FLUENT_SETTING(std::string, FromBound);
        FLUENT_SETTING(std::optional<std::string>, ToBound);
        FLUENT_SETTING(ui32, PartitionId);
    };

    struct TMessageInfo {
        TMessageInfo(const std::string& key, TWriteMessage&& message, ui64 partition, TTransactionBase* tx)
            :Key(key)
            , Message(std::move(message))
            , Partition(partition)
            , Tx(tx)
        {}

        std::string Key;
        TWriteMessage Message;
        ui32 Partition;
        TTransactionBase* Tx;
        bool Resent = false;
    };

    struct TIdleSession;

    struct TWriteSessionWrapper {
        WriteSessionPtr Session;
        const ui32 Partition;
        ui64 QueueSize = 0;
        std::shared_ptr<TIdleSession> IdleSession = nullptr;

        TWriteSessionWrapper(WriteSessionPtr session, ui64 partition);

        bool IsQueueEmpty() const;
        bool AddToQueue(ui64 delta);
        bool RemoveFromQueue(ui64 delta);
    };

    using WrappedWriteSessionPtr = std::shared_ptr<TWriteSessionWrapper>;

    struct TIdleSession {
        TIdleSession(TWriteSessionWrapper* session, TInstant emptySince, TDuration idleTimeout)
            :Session(session)
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Workers

    struct TWorker {
        template<typename T>
        static std::shared_ptr<T> Get(std::weak_ptr<T> weakPtr) {
            auto sharedPtr = weakPtr.lock();
            Y_ABORT_UNLESS(sharedPtr, "Object is not alive");
            return sharedPtr;
        }
    };

    struct TEventsWorker;

    struct TSessionsWorker : TWorker {
        TSessionsWorker(TKeyedWriteSession* session);
        WrappedWriteSessionPtr GetWriteSession(ui64 partition, bool directToPartition = true);
        void OnReadFromSession(WrappedWriteSessionPtr wrappedSession);
        void OnWriteToSession(WrappedWriteSessionPtr wrappedSession);
        void DoWork();
        void Die(TDuration timeout = TDuration::Zero());
        void SetEventsWorker(std::shared_ptr<TEventsWorker> eventsWorker);
    
    private:
        void AddIdleSession(WrappedWriteSessionPtr wrappedSession, TInstant emptySince, TDuration idleTimeout);
        void RemoveIdleSession(ui64 partition);
        WrappedWriteSessionPtr CreateWriteSession(ui64 partition, bool directToPartition = true);
        
        using TSessionsIndexIterator = std::unordered_map<ui64, WrappedWriteSessionPtr>::iterator;
        void DestroyWriteSession(TSessionsIndexIterator& it, TDuration closeTimeout, bool mustBeEmpty = true);

        std::string GetProducerId(ui64 partitionId);

        TKeyedWriteSession* Session;
        std::weak_ptr<TEventsWorker> EventsWorker;
        std::set<IdleSessionPtr, TIdleSession::Comparator> IdlerSessions;
        std::unordered_map<ui64, IdleSessionPtr> IdlerSessionsIndex;
        std::unordered_map<ui64, WrappedWriteSessionPtr> SessionsIndex;
    };

    struct TMessagesWorker : TWorker {
        TMessagesWorker(TKeyedWriteSession* session, std::shared_ptr<TSessionsWorker> sessionsWorker);
        
        void DoWork();

        void AddMessage(const std::string& key, TWriteMessage&& message, ui64 partition, TTransactionBase* tx);
        void ResendMessages(ui64 partition, ui64 afterSeqNo);
        void HandleAck();
        void HandleContinuationToken(ui64 partition, TContinuationToken&& continuationToken);
        bool IsMemoryUsageOK() const;
        NThreading::TFuture<void> Wait();
        bool IsQueueEmpty() const;
        bool HasInFlightMessages() const;
        const TMessageInfo& GetFrontInFlightMessage() const;

    private:
        void PushInFlightMessage(ui64 partition, TMessageInfo&& message);
        void PopInFlightMessage();
        bool SendMessage(WrappedWriteSessionPtr wrappedSession, TMessageInfo&& message);
        std::optional<TContinuationToken> GetContinuationToken(ui64 partition);

        TKeyedWriteSession* Session;
        std::weak_ptr<TSessionsWorker> SessionsWorker;

        std::list<TMessageInfo> PendingMessages;
        std::list<TMessageInfo> InFlightMessages;
        std::unordered_map<ui64, std::list<std::list<TMessageInfo>::iterator>> InFlightMessagesIndex;

        using InFlightMessagesIndexIter = std::list<std::list<TMessageInfo>::iterator>::iterator;
        std::unordered_map<ui64, InFlightMessagesIndexIter> MessagesToResend;
        std::unordered_map<ui64, std::deque<TContinuationToken>> ContinuationTokens;

        NThreading::TPromise<void> MessagesNotEmptyPromise;
        NThreading::TFuture<void> MessagesNotEmptyFuture;
        
        ui64 MemoryUsage = 0;
    };

    struct TSplittedPartitionWorker : std::enable_shared_from_this<TSplittedPartitionWorker>, TWorker {
    private:
        enum class EState {
            Init,
            PendingDescribe,
            GotDescribe,
            PendingMaxSeqNo,
            GotMaxSeqNo,
            Done,
        };

        void MoveTo(EState state);
        void UpdateMaxSeqNo(uint64_t maxSeqNo);
        void LaunchGetMaxSeqNoFutures(std::unique_lock<std::mutex>& lock);
        void HandleDescribeResult();

    public:
        TSplittedPartitionWorker(TKeyedWriteSession* session, ui32 partitionId, ui64 partitionIdx, std::shared_ptr<TSessionsWorker> sessionsWorker, std::shared_ptr<TMessagesWorker> messagesWorker);
        void DoWork();
        NThreading::TFuture<void> Wait();
        bool IsDone();
        void AddPartition(ui64 partition);
            
    private:
        TKeyedWriteSession* Session;
        std::weak_ptr<TSessionsWorker> SessionsWorker;
        std::weak_ptr<TMessagesWorker> MessagesWorker;
        NThreading::TFuture<TDescribeTopicResult> DescribeTopicFuture;
        EState State = EState::Init;
        ui32 PartitionId;
        ui64 PartitionIdx;
        ui64 MaxSeqNo = 0;
        std::vector<WrappedWriteSessionPtr> WriteSessions;
        std::vector<NThreading::TFuture<uint64_t>> GetMaxSeqNoFutures;
        std::mutex Lock;
        ui64 NotReadyFutures = 0;
    };

    struct TEventsWorker : TWorker {
        TEventsWorker(TKeyedWriteSession* session, std::shared_ptr<TSessionsWorker> sessionsWorker, std::shared_ptr<TMessagesWorker> messagesWorker);
        
        void DoWork();
        NThreading::TFuture<void> Wait();
        NThreading::TFuture<void> WaitEvent();
        void UnsubscribeFromPartition(ui64 partition);
        void SubscribeToPartition(ui64 partition);
        void HandleNewMessage();
        std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block);
        std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount = std::nullopt);

    private:
        void HandleSessionClosedEvent(TSessionClosedEvent&& event, ui64 partition);
        void HandleReadyToAcceptEvent(ui64 partition, TWriteSessionEvent::TReadyToAcceptEvent&& event);
        void HandleAcksEvent(ui64 partition, TWriteSessionEvent::TAcksEvent&& event);
        void RunEventLoop(WrappedWriteSessionPtr wrappedSession, ui64 partition);
        void TransferEventsToOutputQueue();
        void AddReadyToAcceptEvent();
        void AddSessionClosedEvent();

        TKeyedWriteSession* Session;
        std::weak_ptr<TSessionsWorker> SessionsWorker;
        std::weak_ptr<TMessagesWorker> MessagesWorker;

        std::vector<NThreading::TFuture<void>> Futures;
        std::unordered_set<size_t> ReadyFutures;
        std::unordered_map<ui64, std::list<TWriteSessionEvent::TEvent>> PartitionsEventQueues;
        std::list<TWriteSessionEvent::TEvent> EventsOutputQueue;
        std::mutex Lock;
    
        NThreading::TFuture<void> NotReadyFuture;
        NThreading::TPromise<void> NotReadyPromise;
        NThreading::TPromise<void> EventsPromise;
        NThreading::TFuture<void> EventsFuture;

        std::optional<TSessionClosedEvent> CloseEvent;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Partition chooser

    struct IPartitionChooser {
        virtual ui32 ChoosePartition(const std::string_view key) = 0;
        virtual ~IPartitionChooser() = default;
    };

    struct TBoundPartitionChooser : IPartitionChooser {
        TBoundPartitionChooser(TKeyedWriteSession* session);
        ui32 ChoosePartition(const std::string_view key) override;
    private:
        TKeyedWriteSession* Session;
    };

    struct THashPartitionChooser : IPartitionChooser {
        THashPartitionChooser(TKeyedWriteSession* session);
        ui32 ChoosePartition(const std::string_view key) override;
    private:
        TKeyedWriteSession* Session;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void RunMainWorker();

    static void* RunMainWorkerThread(void* arg);

    void NonBlockingClose();

    void SetCloseDeadline(const TDuration& closeTimeout);

    TDuration GetCloseTimeout();

    std::string GetProducerId(ui64 partition);

    void HandleAutoPartitioning(ui64 partition);

    void RunSplittedPartitionWorkers();

    void RemoveSplittedPartition(ui32 partitionId);

    void Wait();

public:
    TKeyedWriteSession(const TKeyedWriteSessionSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);
    
    void Write(TContinuationToken&& continuationToken, const std::string& key, TWriteMessage&& message,
               TTransactionBase* tx = nullptr) override;

    NThreading::TFuture<void> WaitEvent() override;

    std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block = false) override;

    std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block = false, std::optional<size_t> maxEventsCount = std::nullopt) override;

    bool Close(TDuration closeTimeout = TDuration::Max()) override;

    TWriterCounters::TPtr GetCounters() override;

    const std::vector<TPartitionInfo>& GetPartitions() const;

    ~TKeyedWriteSession();

private:
    TThread MainWorker;

    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::shared_ptr<TTopicClient::TImpl> Client;
    TDbDriverStatePtr DbDriverState;

    std::vector<TPartitionInfo> Partitions;
    std::unordered_map<ui32, ui32> PartitionIdsMapping;
    std::map<std::string, ui64> PartitionsIndex;

    TKeyedWriteSessionSettings Settings;

    NThreading::TPromise<void> ClosePromise;
    NThreading::TFuture<void> CloseFuture;

    std::mutex GlobalLock;
    std::atomic_bool Closed = false;
    TInstant CloseDeadline = TInstant::Max();
    std::optional<TSessionClosedEvent> CloseEvent;

    std::unique_ptr<IPartitionChooser> PartitionChooser;

    std::function<std::string(const std::string_view key)> PartitioningKeyHasher;

    std::shared_ptr<TEventsWorker> EventsWorker;
    std::shared_ptr<TSessionsWorker> SessionsWorker;
    std::unordered_map<ui64, std::shared_ptr<TSplittedPartitionWorker>> SplittedPartitionWorkers;
    std::shared_ptr<TMessagesWorker> MessagesWorker;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingWriteSession

class TSimpleBlockingWriteSession : public ISimpleBlockingWriteSession {
public:
    TSimpleBlockingWriteSession(
            const TWriteSessionSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);

    bool Write(std::string_view data, std::optional<uint64_t> seqNo = std::nullopt, std::optional<TInstant> createTimestamp = std::nullopt,
               const TDuration& blockTimeout = TDuration::Max()) override;

    bool Write(TWriteMessage&& message,
               TTransactionBase* tx = nullptr,
               const TDuration& blockTimeout = TDuration::Max()) override;

    uint64_t GetInitSeqNo() override;

    bool Close(TDuration closeTimeout = TDuration::Max()) override;
    bool IsAlive() const override;

    TWriterCounters::TPtr GetCounters() override;

protected:
    std::shared_ptr<TWriteSession> Writer;

private:
    std::optional<TContinuationToken> WaitForToken(const TDuration& timeout);

    std::atomic_bool Closed = false;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingKeyedWriteSession

class TSimpleBlockingKeyedWriteSession : public ISimpleBlockingKeyedWriteSession {
private:
    std::optional<TContinuationToken> GetContinuationToken(TDuration timeout);

    void HandleAcksEvent(const TWriteSessionEvent::TAcksEvent& acksEvent);

    bool WaitForAck(ui64 seqNo, TDuration timeout);

    template<typename F>
    bool Wait(const TDuration& timeout, F&& stopFunc);

    void RunEventLoop();

public:
    TSimpleBlockingKeyedWriteSession(
            const TKeyedWriteSessionSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);


    bool Write(const std::string& key, TWriteMessage&& message, TTransactionBase* tx = nullptr,
        TDuration blockTimeout = TDuration::Max()) override;

    bool Close(TDuration closeTimeout = TDuration::Max()) override;

    TWriterCounters::TPtr GetCounters() override;

protected:
    std::shared_ptr<TKeyedWriteSession> Writer;
    std::unordered_set<ui64> AckedSeqNos;
    std::queue<TContinuationToken> ContinuationTokensQueue;

    NThreading::TPromise<void> ClosePromise;
    NThreading::TFuture<void> CloseFuture;

    std::mutex Lock;
    std::atomic_bool Closed = false;
};

} // namespace NYdb::NTopic
