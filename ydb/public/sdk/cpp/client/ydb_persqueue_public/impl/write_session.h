#pragma once

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/aliases.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/persqueue_impl.h>

#include <util/generic/buffer.h>

namespace NYdb::NPersQueue {

namespace NTests {
    class TSimpleWriteSessionTestAdapter;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSession

class TWriteSession : public IWriteSession,
                      public TContextOwner<TWriteSessionImpl> {
private:
    friend class TSimpleBlockingWriteSession;
    friend class TPersQueueClient;
    friend class NTests::TSimpleWriteSessionTestAdapter;

public:
    TWriteSession(const TWriteSessionSettings& settings,
            std::shared_ptr<TPersQueueClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);

    TMaybe<TWriteSessionEvent::TEvent> GetEvent(bool block = false) override;
    TVector<TWriteSessionEvent::TEvent> GetEvents(bool block = false,
                                                  TMaybe<size_t> maxEventsCount = Nothing()) override;
    NThreading::TFuture<ui64> GetInitSeqNo() override;

    void Write(TContinuationToken&& continuationToken, TStringBuf data,
               TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) override;

    void WriteEncoded(TContinuationToken&& continuationToken, TStringBuf data, ECodec codec, ui32 originalSize,
               TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) override;


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
    friend class NTests::TSimpleWriteSessionTestAdapter;

private:
    using TClientMessage = TWriteSessionImpl::TClientMessage;
    using TServerMessage = TWriteSessionImpl::TServerMessage;
    using IWriteSessionConnectionProcessorFactory = TWriteSessionImpl::IWriteSessionConnectionProcessorFactory;
    using IProcessor = TWriteSessionImpl::IProcessor;

public:
    TSimpleBlockingWriteSession(
            const TWriteSessionSettings& settings,
            std::shared_ptr<TPersQueueClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState);

    bool Write(TStringBuf data, TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing(),
               const TDuration& blockTimeout = TDuration::Max()) override;

    ui64 GetInitSeqNo() override;

    bool Close(TDuration closeTimeout = TDuration::Max()) override;
    bool IsAlive() const override;

    TWriterCounters::TPtr GetCounters() override;

protected:
    std::shared_ptr<TWriteSession> Writer;

private:
    TMaybe<TContinuationToken> WaitForToken(const TDuration& timeout);
    void HandleAck(TWriteSessionEvent::TAcksEvent&);
    void HandleReady(TWriteSessionEvent::TReadyToAcceptEvent&);
    void HandleClosed(const TSessionClosedEvent&);

    std::atomic_bool Closed = false;
};


}  // namespace NYdb::NPersQueue
