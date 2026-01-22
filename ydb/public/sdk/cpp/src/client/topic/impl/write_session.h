#pragma once

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

    struct TPartitionBound {
        using TSelf = TPartitionBound;

        FLUENT_SETTING(std::optional<std::string>, Value);

        bool operator<(const TPartitionBound& other) const {
            return !Value_.has_value() || (other.Value_.has_value() && *Value_ < *other.Value_);
        }

        bool operator<(const std::string& key) const {
            return Value_.has_value() && *Value_ < key;
        }

        bool operator>(const std::string& key) const {  
            return Value_.has_value() && *Value_ > key;
        }
    };

    struct TPartitionInfo {
        using TSelf = TPartitionInfo;

        bool InRange(const std::string& key) const {
            if (FromBound_ > key)
                return false;
            if (ToBound_ < key)
                return false;
            return true;
        }

        bool operator==(const TPartitionInfo& other) const {
            return PartitionId_ == other.PartitionId_;
        }

        bool operator<(const std::string& key) const {
            return FromBound_ < key;
        }

        FLUENT_SETTING(TPartitionBound, FromBound);
        FLUENT_SETTING(TPartitionBound, ToBound);
        FLUENT_SETTING(ui64, PartitionId);
    };

    struct TMessageInfo {
        TMessageInfo(TWriteMessage&& message, ui64 partition, TTransactionBase* tx)
            :Message(std::move(message))
            , Partition(partition)
            , Tx(tx)
        {}

        TWriteMessage Message;
        ui64 Partition;
        TTransactionBase* Tx;
    };

    void WaitForEvents();

    void WaitSomeAction(std::unique_lock<std::mutex>& lock);

private:
    struct WriteSessionWrapper {
        WriteSessionPtr Session;
        ui64 Partition;
        TDuration IdleTimeout;
        std::optional<TInstant> EmptySince;
        ui64 QueueSize = 0;

        WriteSessionWrapper(WriteSessionPtr session, ui64 partition, TDuration idleTimeout)
            : Session(std::move(session))
            , Partition(partition)
            , IdleTimeout(idleTimeout)
            , QueueSize(0)
        {}

        bool IsExpired() {
            if (!EmptySince) {
                return false;
            }

            return TInstant::Now() - *EmptySince > IdleTimeout;
        }

        bool AddToQueue(ui64 delta) {
            bool idle = EmptySince.has_value();
            if (idle) {
                EmptySince = std::nullopt;
            }
            QueueSize += delta;
            return idle;
        }

        bool RemoveFromQueue(ui64 delta) {
            QueueSize -= delta;
            if (QueueSize == 0) {
                EmptySince = TInstant::Now();
            }
            return EmptySince.has_value();
        }

        bool Less(const std::shared_ptr<WriteSessionWrapper>& other) {
            if (!EmptySince.has_value() && !other->EmptySince.has_value()) {
                return Partition < other->Partition;
            }

            if (EmptySince.has_value() && !other->EmptySince.has_value()) {
                return true;
            }

            if (!EmptySince.has_value() && other->EmptySince.has_value()) {
                return false;
            }

            if (*EmptySince == *other->EmptySince) {
                return Partition < other->Partition;
            }

            return *EmptySince < *other->EmptySince;
        }

        struct Comparator {
            bool operator()(const std::shared_ptr<WriteSessionWrapper>& first, const std::shared_ptr<WriteSessionWrapper>& second) const {
                return first->Less(second);
            }
        };
    };

    using WrappedWriteSessionPtr = std::shared_ptr<WriteSessionWrapper>;

    struct IPartitionChooser {
        virtual ui64 ChoosePartition(const std::string& key) = 0;
        virtual ~IPartitionChooser() = default;
    };

    struct TBoundPartitionChooser : public IPartitionChooser {
        TBoundPartitionChooser(TKeyedWriteSession* session);
        ui64 ChoosePartition(const std::string& key) override;
    private:
        TKeyedWriteSession* Session;
    };

    struct THashPartitionChooser : public IPartitionChooser {
        THashPartitionChooser(TKeyedWriteSession* session);
        ui64 ChoosePartition(const std::string& key) override;
    private:
        TKeyedWriteSession* Session;
    };

    WrappedWriteSessionPtr GetWriteSession(ui64 partition);

    void RunEventLoop(ui64 partition, WrappedWriteSessionPtr wrappedSession);

    WrappedWriteSessionPtr CreateWriteSession(ui64 partition);

    using TSessionsIndexIterator = std::unordered_map<ui64, WrappedWriteSessionPtr>::iterator;
    void DestroyWriteSession(TSessionsIndexIterator& it, const TDuration& closeTimeout, bool alreadyClosed = false);

    void SaveMessage(TWriteMessage&& message, ui64 partition, TTransactionBase* tx);

    void RunMainWorker();

    std::optional<TContinuationToken> GetContinuationToken(ui64 partition);

    void TransferEventsToOutputQueue();

    void SubscribeToPartition(ui64 partition);

    void AddReadyToAcceptEvent();

    void AddSessionClosedEvent();

    void NonBlockingClose();

    void HandleAcksEvent(ui64 partition, TWriteSessionEvent::TEvent&& event);

    void HandleSessionClosedEvent(TSessionClosedEvent&& event);

    void HandleReadyToAcceptEvent(ui64 partition, TWriteSessionEvent::TReadyToAcceptEvent&& event);

    bool IsMemoryUsageOK() const;

    void SetCloseDeadline(const TDuration& closeTimeout);

    TDuration GetCloseTimeout();

    void CleanIdleSessions();

    void HandleReadyFutures(std::unique_lock<std::mutex>& lock);

    bool IsQueueEmpty();
    
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
    std::thread MainWorker;

    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::shared_ptr<TTopicClient::TImpl> Client;
    TDbDriverStatePtr DbDriverState;

    std::vector<TPartitionInfo> Partitions;
    std::vector<NThreading::TFuture<void>> Futures;
    std::unordered_set<size_t> ReadyFutures;
    std::unique_ptr<IPartitionChooser> PartitionChooser;

    std::set<WrappedWriteSessionPtr, WriteSessionWrapper::Comparator> IdlerSessions;
    std::unordered_map<ui64, WrappedWriteSessionPtr> SessionsIndex;
    std::unordered_map<ui64, std::deque<TContinuationToken>> ContinuationTokens;
    std::map<TPartitionBound, ui64> PartitionsIndex;

    TKeyedWriteSessionSettings Settings;
    std::unordered_map<ui64, std::list<TWriteSessionEvent::TEvent>> PartitionsEventQueues;
    std::list<TWriteSessionEvent::TEvent> EventsOutputQueue;
    std::list<TMessageInfo> PendingMessages;
    std::list<TMessageInfo> InFlightMessages;

    NThreading::TPromise<void> MessagesNotEmptyPromise;
    NThreading::TFuture<void> MessagesNotEmptyFuture;
    NThreading::TPromise<void> ClosePromise;
    NThreading::TFuture<void> CloseFuture;
    NThreading::TPromise<void> EventsProcessedPromise;
    NThreading::TFuture<void> EventsProcessedFuture;

    std::mutex GlobalLock;
    std::atomic_bool Closed = false;
    TInstant CloseDeadline = TInstant::Max();
    std::optional<TSessionClosedEvent> CloseEvent;

    size_t MemoryUsage;

    std::function<std::string(const std::string& key)> PartitioningKeyHasher;
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
    std::optional<TContinuationToken> GetContinuationToken(const TDuration& timeout);

    void HandleAcksEvent(const TWriteSessionEvent::TAcksEvent& acksEvent);

    bool WaitForAck(ui64 seqNo, const TDuration& timeout);

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
        const TDuration& blockTimeout = TDuration::Max()) override;

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
