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
               NTable::TTransaction* tx = nullptr) override;

    void WriteEncoded(TContinuationToken&& continuationToken, TWriteMessage&& message,
                      NTable::TTransaction* tx = nullptr) override;

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
               NTable::TTransaction* tx = nullptr,
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


} // namespace NYdb::NTopic
