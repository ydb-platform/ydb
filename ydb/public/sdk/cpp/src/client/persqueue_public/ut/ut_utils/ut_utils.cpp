#include "ut_utils.h"

namespace NYdb::NPersQueue::NTests {

void WaitMessagesAcked(std::shared_ptr<IWriteSession> writer, ui64 startSeqNo, ui64 endSeqNo) {
    THashSet<ui64> ackedSeqNo;
    while (ackedSeqNo.size() < endSeqNo - startSeqNo + 1) {
        auto event = *writer->GetEvent(true);
        if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
            continue;
        } else {
            UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event));
            for (auto& ack : std::get<TWriteSessionEvent::TAcksEvent>(event).Acks) {
                UNIT_ASSERT(!ackedSeqNo.contains(ack.SeqNo));
                UNIT_ASSERT(ack.SeqNo >= startSeqNo && ack.SeqNo <= endSeqNo);
                ackedSeqNo.insert(ack.SeqNo);
            }
        }
    }
}

TSimpleWriteSessionTestAdapter::TSimpleWriteSessionTestAdapter(TSimpleBlockingWriteSession* session)
    : Session(session)
{}

ui64 TSimpleWriteSessionTestAdapter::GetAcquiredMessagesCount() const {
    if (Session->Writer)
        return Session->Writer->TryGetImpl()->MessagesAcquired;
    else
        return 0;
}

}
