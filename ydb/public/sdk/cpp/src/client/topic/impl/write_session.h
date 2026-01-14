#pragma once

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>

#include <util/generic/buffer.h>

#include <atomic>
#include <functional>

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
    static constexpr auto MAX_CLEANED_SESSIONS_COUNT = 100;

    using WriteSessionPtr = std::shared_ptr<ISimpleBlockingWriteSession>;

    struct TPartitionInfo {
        using TSelf = TPartitionInfo;

        struct THash {
            size_t operator()(const TPartitionInfo& v) const noexcept {
                return std::hash<ui64>{}(v.PartitionId_);
            }
        };

        bool InRange(const std::string& key) const {
            if (FromBound_.has_value() && *FromBound_ > key)
                return false;
            if (ToBound_.has_value() && *ToBound_ < key)
                return false;
            return true;
        }

        bool operator==(const TPartitionInfo& other) const {
            return PartitionId_ == other.PartitionId_;
        }

        FLUENT_SETTING(std::optional<std::string>, FromBound);
        FLUENT_SETTING(std::optional<std::string>, ToBound);
        FLUENT_SETTING(ui64, PartitionId);
        FLUENT_SETTING(bool, Bounded);
    };

private:
    struct WriteSessionWrapper {
        WriteSessionPtr Session;
        TPartitionInfo PartitionInfo;
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
        TBoundPartitionChooser(TSimpleBlockingKeyedWriteSession* session);
        const TPartitionInfo& ChoosePartition(const std::string& key) override;
    private:
        TSimpleBlockingKeyedWriteSession* Session;
    };

    struct THashPartitionChooser : public IPartitionChooser {
        THashPartitionChooser(TSimpleBlockingKeyedWriteSession* session);
        const TPartitionInfo& ChoosePartition(const std::string& key) override;
    private:
        TSimpleBlockingKeyedWriteSession* Session;
    };

    bool AutoPartitioningEnabled(const TTopicDescription& topicDescription) {
        return topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy()
            != EAutoPartitioningStrategy::Disabled;
    }

    void CleanExpiredSessions();

    WrappedWriteSessionPtr GetWriteSession(const TPartitionInfo& partitionInfo);

    WrappedWriteSessionPtr CreateWriteSession(const TPartitionInfo& partitionInfo);

    void DestroyWriteSession(std::set<WrappedWriteSessionPtr>::iterator& writeSession, const TDuration& closeTimeout);

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
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::shared_ptr<TTopicClient::TImpl> Client;
    TDbDriverStatePtr DbDriverState;
    std::vector<TPartitionInfo> Partitions;
    std::unique_ptr<IPartitionChooser> PartitionChooser;
    std::set<WrappedWriteSessionPtr> SessionsPool;
    std::unordered_map<TPartitionInfo, std::set<WrappedWriteSessionPtr>::iterator, TPartitionInfo::THash> SessionsIndex;
    TKeyedWriteSessionSettings Settings;

    std::atomic_bool Closed = false;
};

} // namespace NYdb::NTopic
