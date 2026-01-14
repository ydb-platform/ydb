#pragma once

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>

#include <util/generic/buffer.h>

#include <atomic>

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

    using WriteSession = std::shared_ptr<ISimpleBlockingWriteSession>;

    struct TPartitionInfo {
        TPartitionInfo(ui64 partitionId) : PartitionId(partitionId), Bounded(false) {}
        TPartitionInfo(ui64 partitionId, std::optional<std::string> fromBound, std::optional<std::string> toBound) : FromBound(fromBound), ToBound(toBound), PartitionId(partitionId), Bounded(true) {}

        bool InRange(const std::string& key) const {
            if (FromBound.has_value() && *FromBound > key)
                return false;
            if (ToBound.has_value() && *ToBound < key)
                return false;
            return true;
        }

        bool operator==(const TPartitionInfo& other) const {
            return PartitionId == other.PartitionId;
        }

        std::optional<std::string> FromBound = std::nullopt;
        std::optional<std::string> ToBound = std::nullopt;
        ui64 PartitionId;
        bool Bounded;
    };

    struct WriteSessionWrapper {
        WriteSession Session;
        TPartitionInfo PartitionInfo;
        TInstant ExpirationTime;

        bool operator<(const WriteSessionWrapper& other) const {
            return ExpirationTime < other.ExpirationTime;
        }

        bool IsExpired() const {
            return ExpirationTime < TInstant::Now();
        }
    };

    using WrappedWriteSession = std::shared_ptr<WriteSessionWrapper>;

    struct PartitionChooser {
        virtual const TPartitionInfo& ChoosePartition(const std::string& key) = 0;
        virtual ~PartitionChooser() = default;
    };

    struct TBoundPartitionChooser : public PartitionChooser {
        TBoundPartitionChooser(TSimpleBlockingKeyedWriteSession* session) : Session(session) {}
        const TPartitionInfo& ChoosePartition(const std::string& key) override {
            auto it = std::find_if(Session->Partitions.begin(), Session->Partitions.end(), [key](const auto& partitionInfo) {
                return partitionInfo.InRange(key);
            });
            Y_ABORT_UNLESS(it != Session->Partitions.end());
            return *it;
        }
    private:
        TSimpleBlockingKeyedWriteSession* Session;
    };

    struct THashPartitionChooser : public PartitionChooser {
        THashPartitionChooser(TSimpleBlockingKeyedWriteSession* session) : Session(session) {}
        const TPartitionInfo& ChoosePartition(const std::string& key) override {
            auto keyHash = std::hash<std::string>{}(key);
            return Session->Partitions[keyHash % Session->Partitions.size()];
        }
    private:
        TSimpleBlockingKeyedWriteSession* Session;
    };

    bool AutoPartitioningEnabled(const TTopicDescription& topicDescription) {
        return topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy()
            != EAutoPartitioningStrategy::Disabled;
    }

    void CleanExpiredSessions();

    WrappedWriteSession GetWriteSession(const TPartitionInfo& partitionInfo);

    WrappedWriteSession CreateWriteSession(const TPartitionInfo& partitionInfo);

public:
    TSimpleBlockingKeyedWriteSession(
            const TKeyedWriteSessionSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);


    bool Write(const std::string& key, TWriteMessage&& message, TTransactionBase* tx = nullptr,
        const TDuration& blockTimeout = TDuration::Max()) override;

    // bool WriteEncoded(const std::string& key, TWriteMessage&& params, TTransactionBase* tx = nullptr,
    //     const TDuration& blockTimeout = TDuration::Max()) override;

    bool Close(TDuration closeTimeout = TDuration::Max()) override;

    TWriterCounters::TPtr GetCounters() override;

protected:
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    std::shared_ptr<TTopicClient::TImpl> Client;
    TDbDriverStatePtr DbDriverState;
    std::vector<TPartitionInfo> Partitions;
    std::unique_ptr<PartitionChooser> PartitionChooser;
    std::set<WrappedWriteSession> SessionsPool;
    TKeyedWriteSessionSettings Settings;

    std::atomic_bool Closed = false;
};

} // namespace NYdb::NTopic
