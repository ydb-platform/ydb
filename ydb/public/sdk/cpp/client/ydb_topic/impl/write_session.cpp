#include "write_session.h"

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
    auto message = TWriteMessage::CompressedMessage(data, codec, originalSize);
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
    TWriteMessage message{data};
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
    auto alteredSettings = settings;
    alteredSettings.EventHandlers_.AcksHandler_ = [this](TWriteSessionEvent::TAcksEvent& event) {this->HandleAck(event); };
    alteredSettings.EventHandlers_.ReadyToAcceptHander_ = [this](TWriteSessionEvent::TReadyToAcceptEvent& event)
            {this->HandleReady(event); };
    alteredSettings.EventHandlers_.SessionClosedHandler_ = [this](const TSessionClosedEvent& event) {this->HandleClosed(event); };

    Writer = std::make_shared<TWriteSession>(
                alteredSettings, client, connections, dbDriverState
    );
    Writer->Start(TDuration::Max());
}

ui64 TSimpleBlockingWriteSession::GetInitSeqNo() {
    return Writer->GetInitSeqNo().GetValueSync();
}

bool TSimpleBlockingWriteSession::Write(
        TStringBuf data, TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp, const TDuration& blockTimeout
) {
    if (!IsAlive())
        return false;

    auto continuationToken = WaitForToken(blockTimeout);
    if (continuationToken.Defined()) {
        Writer->Write(std::move(*continuationToken), std::move(data), seqNo, createTimestamp);
        return true;
    }
    return false;
}

bool TSimpleBlockingWriteSession::Write(
        TWriteMessage&& message, const TDuration& blockTimeout
) {
    if (!IsAlive())
        return false;

    auto continuationToken = WaitForToken(blockTimeout);
    if (continuationToken.Defined()) {
        Writer->Write(std::move(*continuationToken), std::move(message));
        return true;
    }
    return false;
}

TMaybe<TContinuationToken> TSimpleBlockingWriteSession::WaitForToken(const TDuration& timeout) {
    auto startTime = TInstant::Now();
    TDuration remainingTime = timeout;
    TMaybe<TContinuationToken> token = Nothing();
    while(!token.Defined() && remainingTime > TDuration::Zero()) {
        with_lock(Lock) {
            if (!ContinueTokens.empty()) {
                token = std::move(ContinueTokens.front());
                ContinueTokens.pop();
            }
        }
        if (!IsAlive())
            return Nothing();

        if (token.Defined()) {
            return std::move(*token);
        }
        else {
            remainingTime = timeout - (TInstant::Now() - startTime);
            Sleep(Min(remainingTime, TDuration::MilliSeconds(100)));
        }
    }
    return Nothing();
}

TWriterCounters::TPtr TSimpleBlockingWriteSession::GetCounters() {
    return Writer->GetCounters();
}


bool TSimpleBlockingWriteSession::IsAlive() const {
    bool closed = false;
    with_lock(Lock) {
        closed = Closed;
    }
    return !closed;
}

void TSimpleBlockingWriteSession::HandleAck(TWriteSessionEvent::TAcksEvent& event) {
    Y_UNUSED(event);
}

void TSimpleBlockingWriteSession::HandleReady(TWriteSessionEvent::TReadyToAcceptEvent& event) {
    with_lock(Lock) {
        ContinueTokens.emplace(std::move(event.ContinuationToken));
    }
}
void TSimpleBlockingWriteSession::HandleClosed(const TSessionClosedEvent&) {
    with_lock(Lock) {
        Closed = true;
    }
}
bool TSimpleBlockingWriteSession::Close(TDuration closeTimeout) {
    return Writer->Close(std::move(closeTimeout));
}

}; // namespace NYdb::NTopic
