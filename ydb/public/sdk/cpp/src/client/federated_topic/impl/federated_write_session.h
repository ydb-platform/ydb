#pragma once

#include <src/client/federated_topic/impl/federated_topic_impl.h>

#include <src/client/topic/impl/write_session.h>

#include <deque>

namespace NYdb::NFederatedTopic {

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseByHashImpl(
    NTopic::TFederatedWriteSessionSettings const& settings,
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos);

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseImpl(
    NTopic::TFederatedWriteSessionSettings const& settings,
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos, std::string const& selfLocation);

class TFederatedWriteSessionImpl : public NTopic::TContinuationTokenIssuer,
                                   public NTopic::TEnableSelfContext<TFederatedWriteSessionImpl> {
    friend class TFederatedWriteSession;
    friend class TFederatedTopicClient::TImpl;

public:
    TFederatedWriteSessionImpl(const TFederatedWriteSessionSettings& settings,
                               std::shared_ptr<TGRpcConnectionsImpl> connections,
                               const TFederatedTopicClientSettings& clientSetttings,
                               std::shared_ptr<TFederatedDbObserver> observer,
                               std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> codecs);

    ~TFederatedWriteSessionImpl() = default;

    NThreading::TFuture<void> WaitEvent();
    std::optional<NTopic::TWriteSessionEvent::TEvent> GetEvent(bool block);
    std::vector<NTopic::TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount);

    NThreading::TFuture<uint64_t> GetInitSeqNo();

    void Write(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& message);

    void WriteEncoded(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& params);

    void Write(NTopic::TContinuationToken&&, std::string_view, std::optional<uint64_t> seqNo = std::nullopt,
                       std::optional<TInstant> createTimestamp = std::nullopt);

    void WriteEncoded(NTopic::TContinuationToken&&, std::string_view, NTopic::ECodec, ui32,
                              std::optional<uint64_t> seqNo = std::nullopt, std::optional<TInstant> createTimestamp = std::nullopt);

    bool Close(TDuration timeout);

private:

    struct TWrappedWriteMessage {
        const std::string Data;
        NTopic::TWriteMessage Message;
        TWrappedWriteMessage(NTopic::TWriteMessage&& message)
            : Data(message.Data)
            , Message(std::move(message))
        {
            Message.Data = Data;
        }
    };

    struct TDeferredWrite {
        explicit TDeferredWrite(std::shared_ptr<NTopic::IWriteSession> writer)
            : Writer(std::move(writer)) {
        }

        void DoWrite() {
            if (!Token.has_value() && !Message.has_value()) {
                return;
            }
            Y_ABORT_UNLESS(Token.has_value() && Message.has_value());
            return Writer->Write(std::move(*Token), std::move(*Message));
        }

        std::shared_ptr<NTopic::IWriteSession> Writer;
        std::optional<NTopic::TContinuationToken> Token;
        std::optional<NTopic::TWriteMessage> Message;
    };

private:
    void Start();
    void OpenSubsessionImpl(std::shared_ptr<TDbInfo> db);

    void OnFederationStateUpdateImpl();
    void ScheduleFederationStateUpdateImpl(TDuration delay);

    void WriteInternal(NTopic::TContinuationToken&&, TWrappedWriteMessage&& message);
    bool PrepareDeferredWriteImpl(TDeferredWrite& deferred);

    void CloseImpl(EStatus statusCode, NYql::TIssues&& issues);
    void CloseImpl(NTopic::TSessionClosedEvent const& ev);

    bool MessageQueuesAreEmptyImpl() const;
    void UpdateFederationStateImpl();

    void IssueTokenIfAllowed();

    TStringBuilder GetLogPrefixImpl() const;

private:
    // For subsession creation
    const NTopic::TFederatedWriteSessionSettings Settings;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const NTopic::TTopicClientSettings SubclientSettings;
    std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> ProvidedCodecs;

    NTopic::IRetryPolicy::IRetryState::TPtr RetryState;
    std::shared_ptr<TFederatedDbObserver> Observer;
    NThreading::TFuture<void> AsyncInit;
    std::shared_ptr<TFederatedDbState> FederationState;
    NYdbGrpc::IQueueClientContextPtr UpdateStateDelayContext;

    TLog Log;

    std::shared_ptr<TDbInfo> CurrentDatabase;

    std::string SessionId;
    const TInstant StartSessionTime = TInstant::Now();

    TAdaptiveLock Lock;

    std::shared_ptr<NTopic::IWriteSession> Subsession;

    std::shared_ptr<NTopic::TWriteSessionEventsQueue> ClientEventsQueue;

    std::optional<NTopic::TContinuationToken> PendingToken;  // from Subsession
    bool ClientHasToken = false;
    std::deque<TWrappedWriteMessage> OriginalMessagesToPassDown;
    std::deque<TWrappedWriteMessage> OriginalMessagesToGetAck;
    i64 BufferFreeSpace;

   enum class State {
        CREATED,  // The session has not been started.
        WORKING,  // Start method has been called.
        CLOSING,  // Close method has been called, but the session may still send some messages.
        CLOSED    // The session is closed, either due to the user request or some server error.
    };
    State SessionState{State::CREATED};
    NThreading::TPromise<void> MessageQueuesHaveBeenEmptied;
    NThreading::TPromise<void> HasBeenClosed;
};

class TFederatedWriteSession : public NTopic::IWriteSession,
                               public NTopic::TContextOwner<TFederatedWriteSessionImpl> {
    friend class TFederatedTopicClient::TImpl;

public:

    TFederatedWriteSession(const TFederatedWriteSessionSettings& settings,
                           std::shared_ptr<TGRpcConnectionsImpl> connections,
                           const TFederatedTopicClientSettings& clientSettings,
                           std::shared_ptr<TFederatedDbObserver> observer,
                           std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> codecs)
        : TContextOwner(settings, std::move(connections), clientSettings, std::move(observer), codecs) {}

    NThreading::TFuture<void> WaitEvent() override {
        return TryGetImpl()->WaitEvent();
    }
    std::optional<NTopic::TWriteSessionEvent::TEvent> GetEvent(bool block) override {
        return TryGetImpl()->GetEvent(block);
    }
    std::vector<NTopic::TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount) override {
        return TryGetImpl()->GetEvents(block, maxEventsCount);
    }
    NThreading::TFuture<uint64_t> GetInitSeqNo() override {
        return TryGetImpl()->GetInitSeqNo();
    }
    void Write(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& message) override {
        TryGetImpl()->Write(std::move(continuationToken), std::move(message));
    }
    void WriteEncoded(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& params) override {
        TryGetImpl()->WriteEncoded(std::move(continuationToken), std::move(params));
    }
    void Write(NTopic::TContinuationToken&& continuationToken, std::string_view data, std::optional<uint64_t> seqNo = std::nullopt,
                       std::optional<TInstant> createTimestamp = std::nullopt) override {
        TryGetImpl()->Write(std::move(continuationToken), data, seqNo, createTimestamp);
    }
    void WriteEncoded(NTopic::TContinuationToken&& continuationToken, std::string_view data, NTopic::ECodec codec, ui32 originalSize,
                      std::optional<uint64_t> seqNo = std::nullopt, std::optional<TInstant> createTimestamp = std::nullopt) override {
        TryGetImpl()->WriteEncoded(std::move(continuationToken), data, codec, originalSize, seqNo, createTimestamp);
    }
    bool Close(TDuration timeout) override {
        return TryGetImpl()->Close(timeout);
    }

    inline NTopic::TWriterCounters::TPtr GetCounters() override {Y_ABORT("Unimplemented"); } //ToDo - unimplemented;

private:

    void Start() {
        TryGetImpl()->Start();
    }
};

} // namespace NYdb::NFederatedTopic
