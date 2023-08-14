#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>

namespace NYdb::NFederatedTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers

std::pair<ui64, ui64> GetMessageOffsetRange(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent, ui64 index) {
    if (dataReceivedEvent.HasCompressedMessages()) {
        const auto& msg = dataReceivedEvent.GetCompressedMessages()[index];
        return {msg.GetOffset(), msg.GetOffset() + 1};
    }
    const auto& msg = dataReceivedEvent.GetMessages()[index];
    return {msg.GetOffset(), msg.GetOffset() + 1};
}

TReadSessionEvent::TEvent Federate(NTopic::TReadSessionEvent::TEvent event, std::shared_ptr<TDbInfo> db) {
    if (auto* ev = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
        return TReadSessionEvent::TDataReceivedEvent(std::move(*ev), db);
    } else if (auto* ev = std::get_if<NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event)) {
        return TReadSessionEvent::TFederated(*ev, db);
    } else if (auto* ev = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
        return TReadSessionEvent::TFederated(*ev, db);
    } else if (auto* ev = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
        return TReadSessionEvent::TFederated(*ev, db);
    } else if (auto* ev = std::get_if<NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>(&event)) {
        return TReadSessionEvent::TFederated(*ev, db);
    } else if (auto* ev = std::get_if<NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
        return TReadSessionEvent::TFederated(*ev, db);
    } else if (auto* ev = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
        return *ev;
    } else {
        Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Printable specializations

}

namespace NYdb::NTopic {

using namespace NFederatedTopic;

using TCommitOffsetAcknowledgementEvent = NFederatedTopic::TReadSessionEvent::TFederated<NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>;
using TStartPartitionSessionEvent = NFederatedTopic::TReadSessionEvent::TFederated<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>;
using TStopPartitionSessionEvent = NFederatedTopic::TReadSessionEvent::TFederated<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>;
using TPartitionSessionStatusEvent = NFederatedTopic::TReadSessionEvent::TFederated<NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>;
using TPartitionSessionClosedEvent = NFederatedTopic::TReadSessionEvent::TFederated<NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>;
using TMessage = NFederatedTopic::TReadSessionEvent::TFederated<NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>;
using TCompressedMessage = NFederatedTopic::TReadSessionEvent::TFederated<NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>;
using TDataReceivedEvent = NFederatedTopic::TReadSessionEvent::TDataReceivedEvent;

template<>
void TPrintable<TFederatedPartitionSession>::DebugString(TStringBuilder& res, bool) const {
    const auto* self = static_cast<const TFederatedPartitionSession*>(this);
    res << " Partition session id: " << self->GetPartitionSessionId()
        << " Topic: \"" << self->GetTopicPath() << "\""
        << " Partition: " << self->GetPartitionId()
        << " Database name: " << self->GetDatabaseName()
        << " Database path: " << self->GetDatabasePath()
        << " Database id: " << self->GetDatabaseId();
}

template<>
void TPrintable<TMessage>::DebugString(TStringBuilder& ret, bool printData) const {
    const auto* self = static_cast<const TMessage*>(this);
    ret << "Message {";
    auto ptr = dynamic_cast<const TReadSessionEvent::TDataReceivedEvent::TMessageBase*>(self);
    Y_VERIFY(ptr);
    ptr->DebugString(ret, printData);
    self->GetFederatedPartitionSession()->DebugString(ret);
    ret << " }";
}

template<>
void TPrintable<TCompressedMessage>::DebugString(TStringBuilder& ret, bool printData) const {
    const auto* self = static_cast<const TCompressedMessage*>(this);
    ret << "CompressedMessage {";
    static_cast<const TReadSessionEvent::TDataReceivedEvent::TMessageBase*>(self)->DebugString(ret, printData);
    self->GetFederatedPartitionSession()->DebugString(ret);
    ret << " Codec: " << self->GetCodec()
        << " Uncompressed size: " << self->GetUncompressedSize()
        << " }";
}

template<>
void TPrintable<TCommitOffsetAcknowledgementEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TCommitOffsetAcknowledgementEvent*>(this);
    ret << "CommitAcknowledgement {";
    self->GetFederatedPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " }";
}

template<>
void TPrintable<TStartPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TStartPartitionSessionEvent*>(this);
    ret << "StartPartitionSession {";
    self->GetFederatedPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " EndOffset: " << self->GetEndOffset()
        << " }";
}

template<>
void TPrintable<TStopPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TStopPartitionSessionEvent*>(this);
    ret << "StopPartitionSession {";
    self->GetFederatedPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " }";
}

template<>
void TPrintable<TPartitionSessionStatusEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TPartitionSessionStatusEvent*>(this);
    ret << "PartitionSessionStatus {";
    self->GetFederatedPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " ReadOffset: " << self->GetReadOffset()
        << " EndOffset: " << self->GetEndOffset()
        << " WriteWatermark: " << self->GetWriteTimeHighWatermark()
        << " }";
}

template<>
void TPrintable<TPartitionSessionClosedEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TPartitionSessionClosedEvent*>(this);
    ret << "PartitionSessionClosed {";
    self->GetFederatedPartitionSession()->DebugString(ret);
    ret << " Reason: " << self->GetReason()
        << " }";
}

template<>
void TPrintable<TDataReceivedEvent>::DebugString(TStringBuilder& ret, bool printData) const {
    const auto* self = static_cast<const TDataReceivedEvent*>(this);
    ret << "DataReceived {";
    self->GetFederatedPartitionSession()->DebugString(ret);
    if (self->HasCompressedMessages()) {
        for (const auto& message : self->GetCompressedMessages()) {
            ret << " ";
            message.DebugString(ret, printData);
        }
    } else {
        for (const auto& message : self->GetMessages()) {
            ret << " ";
            message.DebugString(ret, printData);
        }
    }
    ret << " }";
}


}

namespace NYdb::NFederatedTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NFederatedTopic::TReadSessionEvent::TDataReceivedEvent

TReadSessionEvent::TDataReceivedEvent::TDataReceivedEvent(NTopic::TReadSessionEvent::TDataReceivedEvent event, std::shared_ptr<TDbInfo> db)
    : NTopic::TReadSessionEvent::TPartitionSessionAccessor(event.GetPartitionSession())
    , TFederatedPartitionSessionAccessor(event.GetPartitionSession(), db)
{
    if (event.HasCompressedMessages()) {
        for (auto& msg : event.GetCompressedMessages()) {
            CompressedMessages.emplace_back(std::move(msg), db);
        }
    } else {
        for (auto& msg : event.GetMessages()) {
            Messages.emplace_back(std::move(msg), db);
        }
    }
}

TReadSessionEvent::TDataReceivedEvent::TDataReceivedEvent(
    TVector<TMessage> messages, TVector<TCompressedMessage> compressedMessages,
    NTopic::TPartitionSession::TPtr partitionSession, std::shared_ptr<TDbInfo> db)
    : NTopic::TReadSessionEvent::TPartitionSessionAccessor(partitionSession)
    , TFederatedPartitionSessionAccessor(partitionSession, db)
    , Messages(std::move(messages))
    , CompressedMessages(std::move(compressedMessages))
{
    for (size_t i = 0; i < GetMessagesCount(); ++i) {
        auto [from, to] = GetMessageOffsetRange(*this, i);
        if (OffsetRanges.empty() || OffsetRanges.back().second != from) {
            OffsetRanges.emplace_back(from, to);
        } else {
            OffsetRanges.back().second = to;
        }
    }
}

void TReadSessionEvent::TDataReceivedEvent::Commit() {
    for (auto [from, to] : OffsetRanges) {
        static_cast<NPersQueue::TPartitionStreamImpl<false>*>(PartitionSession.Get())->Commit(from, to);
    }
}

TString DebugString(const TReadSessionEvent::TEvent& event) {
    return std::visit([](const auto& ev) { return ev.DebugString(); }, event);
}

} // namespace NYdb::NFederatedTopic
