#pragma once

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>

#include <atomic>
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
    static constexpr auto MAX_CLEANED_SESSIONS_COUNT = 100;
    static constexpr auto MAX_MESSAGES_IN_MEMORY = 100000;
    // static constexpr auto MESSAGES_NOT_EMPTY_FUTURE_INDEX = 0;
    // static constexpr auto CLOSE_FUTURE_INDEX = 1;

    using WriteSessionPtr = std::shared_ptr<IWriteSession>;

    struct TPartitionBound {
        using TSelf = TPartitionBound;

        FLUENT_SETTING(std::optional<std::string>, Value);

        bool operator<(const TPartitionBound& other) const {
            return !Value_.has_value() || !other.Value_.has_value() || *Value_ < *other.Value_;
        }

        bool operator<(const std::string& key) const {
            return !Value_.has_value() || *Value_ < key;
        }

        bool operator>(const std::string& key) const {  
            return Value_.has_value() && *Value_ > key;
        }
    };

    struct TPartitionInfo {
        using TSelf = TPartitionInfo;

        struct THash {
            size_t operator()(const TPartitionInfo& v) const noexcept {
                return std::hash<ui64>{}(v.PartitionId_);
            }
        };

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
        FLUENT_SETTING(bool, Bounded);
    };

    struct TMessageInfo {
        TMessageInfo(TWriteMessage&& message, ui64 partitionId, TTransactionBase* tx)
            :Message(std::move(message))
            , PartitionId(partitionId)
            , Tx(tx)
        {}

        TWriteMessage Message;
        ui64 PartitionId;
        TTransactionBase* Tx;
    };

    void WaitForEvents();

    void WaitSomeAction(std::unique_lock<std::mutex>& lock);

private:
    struct WriteSessionWrapper {
        WriteSessionPtr Session;
        ui64 PartitionId;
        TInstant ExpirationTime;

        bool operator<(const WriteSessionWrapper& other) const {
            return ExpirationTime < other.ExpirationTime;
        }

        bool IsExpired() const {
            return ExpirationTime < TInstant::Now();
        }
    };

    using WrappedWriteSessionPtr = std::shared_ptr<WriteSessionWrapper>;

    struct IPartitionChooser {
        virtual const TPartitionInfo& ChoosePartition(const std::string& key) = 0;
        virtual ~IPartitionChooser() = default;
    };

    struct TBoundPartitionChooser : public IPartitionChooser {
        TBoundPartitionChooser(TKeyedWriteSession* session);
        const TPartitionInfo& ChoosePartition(const std::string& key) override;
    private:
        TKeyedWriteSession* Session;
    };

    struct THashPartitionChooser : public IPartitionChooser {
        THashPartitionChooser(TKeyedWriteSession* session);
        const TPartitionInfo& ChoosePartition(const std::string& key) override;
    private:
        TKeyedWriteSession* Session;
    };

    bool AutoPartitioningEnabled(const TTopicDescription& topicDescription) {
        return topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy()
            != EAutoPartitioningStrategy::Disabled;
    }

    void CleanExpiredSessions();

    WrappedWriteSessionPtr GetWriteSession(ui64 partitionId);

    WrappedWriteSessionPtr CreateWriteSession(ui64 partitionId);

    using TSessionsIndexIterator = std::unordered_map<ui64, WrappedWriteSessionPtr>::iterator;
    void DestroyWriteSession(TSessionsIndexIterator& it, const TDuration& closeTimeout, bool alreadyClosed = false);

    void SaveMessage(TWriteMessage&& message, ui64 partitionId, TTransactionBase* tx);

    void RunMessageSender();

    std::optional<TContinuationToken> GetContinuationToken(ui64 partitionId);

    // void ProcessEventsUntilReadyToAccept(ui64 partitionId);

    void TransferEventsToGlobalQueue();

    void AddReadyFuture(ui64 index);

    void ConsumeEvents(WrappedWriteSessionPtr wrappedSession);

    void AddEventToPartitionQueue(ui64 partitionId, TWriteSessionEvent::TEvent& event);
    
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

    ~TKeyedWriteSession() = default;

private:
    std::thread MessageSenderWorker;

    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::shared_ptr<TTopicClient::TImpl> Client;
    TDbDriverStatePtr DbDriverState;

    std::vector<TPartitionInfo> Partitions;
    std::vector<NThreading::TFuture<void>> Futures;
    std::unique_ptr<IPartitionChooser> PartitionChooser;

    std::unordered_map<ui64, WrappedWriteSessionPtr> SessionsIndex;
    std::unordered_map<ui64, TContinuationToken> ContinuationTokens;
    std::unordered_map<ui64, ui64> PartitionsPrimaryIndex;
    std::map<TPartitionBound, ui64> PartitionsIndex;

    TKeyedWriteSessionSettings Settings;
    std::unordered_map<ui64, std::list<TWriteSessionEvent::TEvent>> PartitionsEventQueues;
    std::list<TWriteSessionEvent::TEvent> EventsGlobalQueue;
    std::unordered_set<ui64> PartitionsWithEvents;
    std::list<TMessageInfo> PendingMessages;
    std::list<TMessageInfo> InFlightMessages;

    NThreading::TPromise<void> MessagesNotEmptyPromise;
    NThreading::TPromise<void> ClosePromise;
    NThreading::TFuture<void> CloseFuture;
    NThreading::TPromise<void> EventsProcessedPromise;
    NThreading::TFuture<void> EventsProcessedFuture;

    std::mutex GlobalLock;
    std::atomic_bool Closed = false;
    TDuration CloseTimeout = TDuration::Zero();

    // IMPORTANT: must not be protected by GlobalLock, because callbacks from Future::Subscribe()
    // can be executed inline, and taking GlobalLock there can deadlock the session thread.
    std::mutex ReadyFuturesLock;
    std::vector<ui64> ReadyFutures;

    size_t MessagesNotEmptyFutureIndex;
    size_t CloseFutureIndex;
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

    std::atomic_bool Closed = false;
};

} // namespace NYdb::NTopic
