#pragma once

#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federated_topic_impl.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/write_session.h>

#include <deque>

namespace NYdb::NFederatedTopic {

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseByHashImpl(
    NTopic::TFederatedWriteSessionSettings const& settings,
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos);

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseImpl(
    NTopic::TFederatedWriteSessionSettings const& settings,
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos, TString const& selfLocation);

class TFederatedWriteSessionImpl : public NTopic::TContinuationTokenIssuer,
                                   public NTopic::TEnableSelfContext<TFederatedWriteSessionImpl> {
    friend class TFederatedWriteSession;
    friend class TFederatedTopicClient::TImpl;

public:

    TFederatedWriteSessionImpl(const TFederatedWriteSessionSettings& settings,
                               std::shared_ptr<TGRpcConnectionsImpl> connections,
                               const TFederatedTopicClientSettings& clientSetttings,
                               std::shared_ptr<TFederatedDbObserver> observer,
                               std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs,
                               NTopic::IExecutor::TPtr subsessionHandlersExecutor);

    ~TFederatedWriteSessionImpl() = default;

    NThreading::TFuture<void> WaitEvent();
    TMaybe<NTopic::TWriteSessionEvent::TEvent> GetEvent(bool block);
    TVector<NTopic::TWriteSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount);

    NThreading::TFuture<ui64> GetInitSeqNo();

    void Write(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& message);

    void WriteEncoded(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& params);

    void Write(NTopic::TContinuationToken&&, TStringBuf, TMaybe<ui64> seqNo = Nothing(),
                       TMaybe<TInstant> createTimestamp = Nothing());

    void WriteEncoded(NTopic::TContinuationToken&&, TStringBuf, NTopic::ECodec, ui32,
                              TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing());

    bool Close(TDuration timeout);

private:

    struct TWrappedWriteMessage {
        const TString Data;
        NTopic::TWriteMessage Message;
        TWrappedWriteMessage(NTopic::TWriteMessage&& message)
            : Data(message.Data)
            , Message(std::move(message))
        {
            Message.Data = Data;
        }
    };

private:
    void Start();

    std::shared_ptr<NTopic::IWriteSession> OpenSubsessionImpl(std::shared_ptr<TDbInfo> db);
    std::shared_ptr<NTopic::IWriteSession> UpdateFederationStateImpl();
    std::shared_ptr<NTopic::IWriteSession> OnFederationStateUpdateImpl();

    void ScheduleFederationStateUpdateImpl(TDuration delay);

    void WriteInternal(NTopic::TContinuationToken&&, TWrappedWriteMessage&& message);
    bool MaybeWriteImpl();

    void CloseImpl(EStatus statusCode, NYql::TIssues&& issues);
    void CloseImpl(NTopic::TSessionClosedEvent const& ev);

    bool MessageQueuesAreEmptyImpl() const;

    void IssueTokenIfAllowed();

    TStringBuilder GetLogPrefixImpl() const;

private:
    // For subsession creation
    const NTopic::TFederatedWriteSessionSettings Settings;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const NTopic::TTopicClientSettings SubclientSettings;
    std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> ProvidedCodecs;
    NTopic::IExecutor::TPtr SubsessionHandlersExecutor;

    NTopic::IRetryPolicy::IRetryState::TPtr RetryState;
    std::shared_ptr<TFederatedDbObserver> Observer;
    NThreading::TFuture<void> AsyncInit;
    std::shared_ptr<TFederatedDbState> FederationState;
    NYdbGrpc::IQueueClientContextPtr UpdateStateDelayContext;

    TLog Log;

    std::shared_ptr<TDbInfo> CurrentDatabase;

    TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();

    TAdaptiveLock Lock;

    size_t SubsessionGeneration = 0;
    std::shared_ptr<NTopic::IWriteSession> Subsession;

    std::shared_ptr<NTopic::TWriteSessionEventsQueue> ClientEventsQueue;

    TMaybe<NTopic::TContinuationToken> PendingToken;  // from Subsession
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
                           std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs,
                           NTopic::IExecutor::TPtr subsessionHandlersExecutor)
        : TContextOwner(settings, std::move(connections), clientSettings, std::move(observer), codecs, subsessionHandlersExecutor) {}

    NThreading::TFuture<void> WaitEvent() override {
        return TryGetImpl()->WaitEvent();
    }
    TMaybe<NTopic::TWriteSessionEvent::TEvent> GetEvent(bool block) override {
        return TryGetImpl()->GetEvent(block);
    }
    TVector<NTopic::TWriteSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount) override {
        return TryGetImpl()->GetEvents(block, maxEventsCount);
    }
    NThreading::TFuture<ui64> GetInitSeqNo() override {
        return TryGetImpl()->GetInitSeqNo();
    }
    void Write(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& message) override {
        TryGetImpl()->Write(std::move(continuationToken), std::move(message));
    }
    void WriteEncoded(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& params) override {
        TryGetImpl()->WriteEncoded(std::move(continuationToken), std::move(params));
    }
    void Write(NTopic::TContinuationToken&& continuationToken, TStringBuf data, TMaybe<ui64> seqNo = Nothing(),
                       TMaybe<TInstant> createTimestamp = Nothing()) override {
        TryGetImpl()->Write(std::move(continuationToken), data, seqNo, createTimestamp);
    }
    void WriteEncoded(NTopic::TContinuationToken&& continuationToken, TStringBuf data, NTopic::ECodec codec, ui32 originalSize,
                      TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) override {
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
