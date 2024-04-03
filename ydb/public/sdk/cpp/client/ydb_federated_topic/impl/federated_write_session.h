#pragma once

#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federated_topic_impl.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/write_session.h>

#include <deque>

namespace NYdb::NFederatedTopic {

class TFederatedWriteSessionImpl : public NTopic::TContinuationTokenIssuer,
                                   public NTopic::TEnableSelfContext<TFederatedWriteSessionImpl> {
    friend class TFederatedWriteSession;
    friend class TFederatedTopicClient::TImpl;

public:

    TFederatedWriteSessionImpl(const TFederatedWriteSessionSettings& settings,
                               std::shared_ptr<TGRpcConnectionsImpl> connections,
                               const TFederatedTopicClientSettings& clientSetttings,
                               std::shared_ptr<TFederatedDbObserver> observer,
                               std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs);

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

    struct TDeferredWrite {
        explicit TDeferredWrite(std::shared_ptr<NTopic::IWriteSession> writer)
            : Writer(std::move(writer)) {
        }

        void DoWrite() {
            if (Token.Empty() && Message.Empty()) {
                return;
            }
            Y_ABORT_UNLESS(Token.Defined() && Message.Defined());
            return Writer->Write(std::move(*Token), std::move(*Message));
        }

        std::shared_ptr<NTopic::IWriteSession> Writer;
        TMaybe<NTopic::TContinuationToken> Token;
        TMaybe<NTopic::TWriteMessage> Message;
    };

private:
    void Start();
    void OpenSubSessionImpl(std::shared_ptr<TDbInfo> db);

    std::shared_ptr<TDbInfo> SelectDatabaseImpl();

    void OnFederatedStateUpdateImpl();
    void ScheduleFederatedStateUpdateImpl(TDuration delay);

    void WriteInternal(NTopic::TContinuationToken&&, TWrappedWriteMessage&& message);
    bool PrepareDeferredWrite(TDeferredWrite& deferred);

    void CloseImpl(EStatus statusCode, NYql::TIssues&& issues, TDuration timeout = TDuration::Zero());
    void CloseImpl(NTopic::TSessionClosedEvent const& ev, TDuration timeout = TDuration::Zero());

    TStringBuilder GetLogPrefix() const;

private:
    // For subsession creation
    const NTopic::TFederatedWriteSessionSettings Settings;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const NTopic::TTopicClientSettings SubClientSetttings;
    std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> ProvidedCodecs;

    std::shared_ptr<TFederatedDbObserver> Observer;
    NThreading::TFuture<void> AsyncInit;
    std::shared_ptr<TFederatedDbState> FederationState;
    NYdbGrpc::IQueueClientContextPtr UpdateStateDelayContext;

    TLog Log;

    std::shared_ptr<TDbInfo> CurrentDatabase;

    TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();

    TAdaptiveLock Lock;

    std::shared_ptr<NTopic::IWriteSession> Subsession;

    std::shared_ptr<NTopic::TWriteSessionEventsQueue> ClientEventsQueue;

    TMaybe<NTopic::TContinuationToken> PendingToken;  // from Subsession
    bool ClientHasToken = false;
    std::deque<TWrappedWriteMessage> OriginalMessagesToPassDown;
    std::deque<TWrappedWriteMessage> OriginalMessagesToGetAck;
    i64 BufferFreeSpace;

    // Exiting.
    bool Closing = false;
};

class TFederatedWriteSession : public NTopic::IWriteSession,
                               public NTopic::TContextOwner<TFederatedWriteSessionImpl> {
    friend class TFederatedTopicClient::TImpl;

public:

    TFederatedWriteSession(const TFederatedWriteSessionSettings& settings,
                           std::shared_ptr<TGRpcConnectionsImpl> connections,
                           const TFederatedTopicClientSettings& clientSettings,
                           std::shared_ptr<TFederatedDbObserver> observer,
                           std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs)
        : TContextOwner(settings, std::move(connections), clientSettings, std::move(observer), codecs) {}

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
