#include "write_session.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>

namespace NYdb::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSession

TWriteSession::TWriteSession(
        const TWriteSessionSettings& settings,
         std::shared_ptr<TTopicClient::TImpl> client,
         std::shared_ptr<TGRpcConnectionsImpl> connections,
         TDbDriverStatePtr dbDriverState)
    : TContextOwner(settings, std::move(client), std::move(connections), std::move(dbDriverState)) {
}

void TWriteSession::Start(const TDuration& delay) {
    TryGetImpl()->Start(delay);
}

NThreading::TFuture<ui64> TWriteSession::GetInitSeqNo() {
    return TryGetImpl()->GetInitSeqNo();
}

TMaybe<TWriteSessionEvent::TEvent> TWriteSession::GetEvent(bool block) {
    return TryGetImpl()->EventsQueue->GetEvent(block);
}

TVector<TWriteSessionEvent::TEvent> TWriteSession::GetEvents(bool block, TMaybe<size_t> maxEventsCount) {
    return TryGetImpl()->EventsQueue->GetEvents(block, maxEventsCount);
}

NThreading::TFuture<void> TWriteSession::WaitEvent() {
    return TryGetImpl()->EventsQueue->WaitEvent();
}

void TWriteSession::WriteEncoded(TContinuationToken&& token, TStringBuf data, ECodec codec, ui32 originalSize,
                                 TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp) {
    auto message = TWriteMessage::CompressedMessage(std::move(data), codec, originalSize);
    if (seqNo.Defined())
        message.SeqNo(*seqNo);
    if (createTimestamp.Defined())
        message.CreateTimestamp(*createTimestamp);
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::WriteEncoded(TContinuationToken&& token, TWriteMessage&& message)
{
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::Write(TContinuationToken&& token, TStringBuf data, TMaybe<ui64> seqNo,
                          TMaybe<TInstant> createTimestamp) {
    TWriteMessage message{std::move(data)};
    if (seqNo.Defined())
        message.SeqNo(*seqNo);
    if (createTimestamp.Defined())
        message.CreateTimestamp(*createTimestamp);
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::Write(TContinuationToken&& token, TWriteMessage&& message) {
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

bool TWriteSession::Close(TDuration closeTimeout) {
    return TryGetImpl()->Close(closeTimeout);
}

TWriteSession::~TWriteSession() {
    TryGetImpl()->Close(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingWriteSession

TSimpleBlockingWriteSession::TSimpleBlockingWriteSession(
        const TWriteSessionSettings& settings,
        std::shared_ptr<TTopicClient::TImpl> client,
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        TDbDriverStatePtr dbDriverState
) {
    auto subSettings = settings;
    if (settings.EventHandlers_.AcksHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use AcksHandler, resetting.");
        subSettings.EventHandlers_.AcksHandler({});
    }
    if (settings.EventHandlers_.ReadyToAcceptHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use ReadyToAcceptHandler, resetting.");
        subSettings.EventHandlers_.ReadyToAcceptHandler({});
    }
    if (settings.EventHandlers_.SessionClosedHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use SessionClosedHandler, resetting.");
        subSettings.EventHandlers_.SessionClosedHandler({});
    }
    if (settings.EventHandlers_.CommonHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use CommonHandler, resetting.");
        subSettings.EventHandlers_.CommonHandler({});
    }
    Writer = std::make_shared<TWriteSession>(subSettings, client, connections, dbDriverState);
    Writer->Start(TDuration::Max());
}

ui64 TSimpleBlockingWriteSession::GetInitSeqNo() {
    return Writer->GetInitSeqNo().GetValueSync();
}

bool TSimpleBlockingWriteSession::Write(
        TStringBuf data, TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp, const TDuration& blockTimeout
) {
    auto message = TWriteMessage(std::move(data))
        .SeqNo(seqNo)
        .CreateTimestamp(createTimestamp);
    return Write(std::move(message), blockTimeout);
}

bool TSimpleBlockingWriteSession::Write(
        TWriteMessage&& message, const TDuration& blockTimeout
) {
    auto continuationToken = WaitForToken(blockTimeout);
    if (continuationToken.Defined()) {
        Writer->Write(std::move(*continuationToken), std::move(message));
        return true;
    }
    return false;
}

TMaybe<TContinuationToken> TSimpleBlockingWriteSession::WaitForToken(const TDuration& timeout) {
    TInstant startTime = TInstant::Now();
    TDuration remainingTime = timeout;

    TMaybe<TContinuationToken> token = Nothing();

    while (IsAlive() && remainingTime > TDuration::Zero()) {
        Writer->WaitEvent().Wait(remainingTime);

        for (auto event : Writer->GetEvents()) {
            if (auto* readyEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                Y_ABORT_UNLESS(token.Empty());
                token = std::move(readyEvent->ContinuationToken);
            } else if (auto* ackEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                // discard
            } else if (auto* closeSessionEvent = std::get_if<TSessionClosedEvent>(&event)) {
                Closed.store(true);
                return Nothing();
            }
        }

        if (token.Defined()) {
            return token;
        }

        remainingTime = timeout - (TInstant::Now() - startTime);
    }

    return Nothing();
}

TWriterCounters::TPtr TSimpleBlockingWriteSession::GetCounters() {
    return Writer->GetCounters();
}

bool TSimpleBlockingWriteSession::IsAlive() const {
    return !Closed.load();
}

bool TSimpleBlockingWriteSession::Close(TDuration closeTimeout) {
    Closed.store(true);
    return Writer->Close(std::move(closeTimeout));
}

}  // namespace NYdb::NTopic
