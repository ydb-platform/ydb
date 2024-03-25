#pragma once

#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federated_topic_impl.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/write_session.h>

#include <deque>

namespace NYdb::NFederatedTopic {

class TFederatedWriteSession : public NTopic::IWriteSession,
                               public NTopic::TContinuationTokenIssuer,
                               public std::enable_shared_from_this<TFederatedWriteSession> {
    friend class TFederatedTopicClient::TImpl;

public:
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

    TFederatedWriteSession(const TFederatedWriteSessionSettings& settings,
                          std::shared_ptr<TGRpcConnectionsImpl> connections,
                          const TFederatedTopicClientSettings& clientSetttings,
                          std::shared_ptr<TFederatedDbObserver> observer,
                          std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs);

    ~TFederatedWriteSession() = default;

    NThreading::TFuture<void> WaitEvent() override;
    TMaybe<NTopic::TWriteSessionEvent::TEvent> GetEvent(bool block) override;
    TVector<NTopic::TWriteSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount) override;

    virtual NThreading::TFuture<ui64> GetInitSeqNo() override;

    virtual void Write(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& message) override;

    virtual void WriteEncoded(NTopic::TContinuationToken&& continuationToken, NTopic::TWriteMessage&& params) override;

    virtual void Write(NTopic::TContinuationToken&&, TStringBuf, TMaybe<ui64> seqNo = Nothing(),
                       TMaybe<TInstant> createTimestamp = Nothing()) override;

    virtual void WriteEncoded(NTopic::TContinuationToken&&, TStringBuf, NTopic::ECodec, ui32,
                              TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) override;

    bool Close(TDuration timeout) override;

    inline NTopic::TWriterCounters::TPtr GetCounters() override {Y_ABORT("Unimplemented"); } //ToDo - unimplemented;

private:
    void Start();
    void OpenSubSessionImpl(std::shared_ptr<TDbInfo> db);

    std::shared_ptr<TDbInfo> SelectDatabaseImpl();

    void OnFederatedStateUpdateImpl();
    void ScheduleFederatedStateUpdateImpl(TDuration delay);

    void WriteInternal(NTopic::TContinuationToken&&, NTopic::TWriteMessage&& message);
    bool PrepareDeferredWrite(TDeferredWrite& deferred);

    void CloseImpl(EStatus statusCode, NYql::TIssues&& issues);

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
    std::deque<NTopic::TWriteMessage> OriginalMessagesToPassDown;
    std::deque<NTopic::TWriteMessage> OriginalMessagesToGetAck;
    i64 BufferFreeSpace;

    // Exiting.
    bool Closing = false;
};

} // namespace NYdb::NFederatedTopic
