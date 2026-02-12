#include "event_loop.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests {

TKeyedWriteSessionEventLoop::TKeyedWriteSessionEventLoop(std::shared_ptr<IKeyedWriteSession> session)
    : Session_(std::move(session))
{}

void TKeyedWriteSessionEventLoop::Run() {
    while (true) {
        auto event = Session_->GetEvent(false);
        if (!event) {
            break;
        }
        if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            std::lock_guard lk(Lock_);
            ReadyTokens_.push(std::move(ready->ContinuationToken));
            continue;
        }
        if (std::get_if<TSessionClosedEvent>(&*event)) {
            break;
        }
        if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
            std::lock_guard lk(Lock_);
            for (const auto& ack : acks->Acks) {
                auto [it, inserted] = AckedSeqNos_.insert(ack.SeqNo);
                UNIT_ASSERT_C(inserted, TStringBuilder() << "Ack already received: " << ack.SeqNo);
                AckOrder_.push_back(ack.SeqNo);
            }
        }
    }
}

std::optional<TContinuationToken> TKeyedWriteSessionEventLoop::GetContinuationToken(TDuration timeout) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        {
            std::lock_guard lock(Lock_);
            if (!ReadyTokens_.empty()) {
                auto token = std::move(ReadyTokens_.front());
                ReadyTokens_.pop();
                return token;
            }
        }
        Session_->WaitEvent().Wait(deadline);
        Run();
    }

    std::lock_guard lock(Lock_);
    if (!ReadyTokens_.empty()) {
        auto token = std::move(ReadyTokens_.front());
        ReadyTokens_.pop();
        return token;
    }
    return std::nullopt;
}

bool TKeyedWriteSessionEventLoop::WaitForAcks(size_t count, TDuration timeout) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        {
            std::lock_guard lock(Lock_);
            if (AckedSeqNos_.size() >= count) {
                return true;
            }
        }
        Session_->WaitEvent().Wait(deadline);
        Run();
    }
    return false;
}

void TKeyedWriteSessionEventLoop::CheckAcksOrder() {
    std::lock_guard lock(Lock_);
    size_t expectedAck = 1;
    UNIT_ASSERT_C(AckedSeqNos_.size() == AckOrder_.size(), TStringBuilder() << "Unexpected number of acks: got " << AckOrder_.size() << ", expected " << AckedSeqNos_.size());
    size_t index = 0;
    for (const auto& ack : AckOrder_) {
        TStringBuilder sb;
        if (ack != expectedAck) {
            for (size_t i = std::min(size_t(0), index - 10); i < std::min(index + 10, AckOrder_.size()); i++) {
                sb << "Ack " << i << ": " << AckOrder_[i] << " ";
            }

            sb << "Unexpected ack order: got " << ack << ", expected " << expectedAck;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(ack, expectedAck, sb);
        expectedAck++;
    }
}

} // namespace NYdb::inline Dev::NTopic::NTests
