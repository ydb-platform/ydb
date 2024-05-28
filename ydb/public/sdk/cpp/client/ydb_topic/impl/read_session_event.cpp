#include "common.h"
#include "read_session_impl.ipp"

#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_events.h>

namespace NYdb::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Aliases for event types

using TDataReceivedEvent = TReadSessionEvent::TDataReceivedEvent;
using TMessageInformation = TDataReceivedEvent::TMessageInformation;
using TMessageBase = TDataReceivedEvent::TMessageBase;
using TMessage = TDataReceivedEvent::TMessage;
using TCompressedMessage = TDataReceivedEvent::TCompressedMessage;
using TCommitOffsetAcknowledgementEvent = TReadSessionEvent::TCommitOffsetAcknowledgementEvent;
using TStartPartitionSessionEvent = TReadSessionEvent::TStartPartitionSessionEvent;
using TStopPartitionSessionEvent = TReadSessionEvent::TStopPartitionSessionEvent;
using TEndPartitionSessionEvent = TReadSessionEvent::TEndPartitionSessionEvent;
using TPartitionSessionStatusEvent = TReadSessionEvent::TPartitionSessionStatusEvent;
using TPartitionSessionClosedEvent = TReadSessionEvent::TPartitionSessionClosedEvent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers

std::pair<ui64, ui64> GetMessageOffsetRange(const TDataReceivedEvent& dataReceivedEvent, ui64 index) {
    if (dataReceivedEvent.HasCompressedMessages()) {
        const auto& msg = dataReceivedEvent.GetCompressedMessages()[index];
        return {msg.GetOffset(), msg.GetOffset() + 1};
    }
    const auto& msg = dataReceivedEvent.GetMessages()[index];
    return {msg.GetOffset(), msg.GetOffset() + 1};
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation

TMessageInformation::TMessageInformation(
    ui64 offset,
    TString producerId,
    ui64 seqNo,
    TInstant createTime,
    TInstant writeTime,
    TWriteSessionMeta::TPtr meta,
    TMessageMeta::TPtr messageMeta,
    ui64 uncompressedSize,
    TString messageGroupId
)
    : Offset(offset)
    , ProducerId(producerId)
    , SeqNo(seqNo)
    , CreateTime(createTime)
    , WriteTime(writeTime)
    , Meta(meta)
    , MessageMeta(messageMeta)
    , UncompressedSize(uncompressedSize)
    , MessageGroupId(messageGroupId)
{}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TPartitionSessionAccessor

TReadSessionEvent::TPartitionSessionAccessor::TPartitionSessionAccessor(TPartitionSession::TPtr partitionSession)
    : PartitionSession(std::move(partitionSession))
{}

const TPartitionSession::TPtr& TReadSessionEvent::TPartitionSessionAccessor::GetPartitionSession() const {
    return PartitionSession;
}

template<>
void TPrintable<TPartitionSession>::DebugString(TStringBuilder& res, bool) const {
    const auto* self = static_cast<const TPartitionSession*>(this);
    res << " Partition session id: " << self->GetPartitionSessionId()
        << " Topic: \"" << self->GetTopicPath() << "\""
        << " Partition: " << self->GetPartitionId();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageBase

TMessageBase::TMessageBase(const TString& data, TMessageInformation info)
    : Data(data)
    , Information(std::move(info))
{}

const TString& TMessageBase::GetData() const {
    return Data;
}

ui64 TMessageBase::GetOffset() const {
    return Information.Offset;
}

const TString& TMessageBase::GetProducerId() const {
    return Information.ProducerId;
}

const TString& TMessageBase::GetMessageGroupId() const {
    return Information.MessageGroupId;
}

ui64 TMessageBase::GetSeqNo() const {
    return Information.SeqNo;
}

TInstant TMessageBase::GetCreateTime() const {
    return Information.CreateTime;
}

TInstant TMessageBase::GetWriteTime() const {
    return Information.WriteTime;
}

const TWriteSessionMeta::TPtr& TMessageBase::GetMeta() const {
    return Information.Meta;
}

const TMessageMeta::TPtr& TMessageBase::GetMessageMeta() const {
    return Information.MessageMeta;
}

template<>
void TPrintable<TMessageBase>::DebugString(TStringBuilder& ret, bool printData) const {
    const auto* self = static_cast<const TMessageBase*>(this);
    try {
        const TString& data = self->GetData();
        if (printData) {
            ret << " Data: \"" << data << "\"";
        } else {
            ret << " Data: .." << data.size() << " bytes..";
        }
    } catch (...) {
        ret << " DataDecompressionError: \"" << CurrentExceptionMessage() << "\"";
    }
    ret << " Information: {"
        << " Offset: " << self->GetOffset()
        << " ProducerId: \"" << self->GetProducerId() << "\""
        << " SeqNo: " << self->GetSeqNo()
        << " CreateTime: " << self->GetCreateTime()
        << " WriteTime: " << self->GetWriteTime()
        << " MessageGroupId: \"" << self->GetMessageGroupId() << "\"";
    ret << " Meta: {";
    bool firstKey = true;
    for (const auto& [k, v] : self->GetMeta()->Fields) {
        ret << (firstKey ? " \"" : ", \"") << k << "\": \"" << v << "\"";
        firstKey = false;
    }
    ret << " }";
    ret << " MessageMeta: {";
    firstKey = true;
    for (const auto& [k, v] : self->GetMessageMeta()->Fields) {
        ret << (firstKey ? " \"" : ", \"") << k << "\": \"" << v << "\"";
        firstKey = false;
    }
    ret << " } }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage

TMessage::TMessage(const TString& data,
                   std::exception_ptr decompressionException,
                   TMessageInformation information,
                   TPartitionSession::TPtr partitionSession)
    : TMessageBase(data, std::move(information))
    , TPartitionSessionAccessor(std::move(partitionSession))
    , DecompressionException(std::move(decompressionException)) {
}

const TString& TMessage::GetData() const {
    if (DecompressionException) {
        std::rethrow_exception(DecompressionException);
    }
    return TMessageBase::GetData();
}

bool TMessage::HasException() const {
    return DecompressionException != nullptr;
}

void TMessage::Commit() {
    static_cast<TPartitionStreamImpl<false>*>(PartitionSession.Get())
        ->Commit(Information.Offset, Information.Offset + 1);
}

template<>
void TPrintable<TMessage>::DebugString(TStringBuilder& ret, bool printData) const {
    const auto* self = static_cast<const TMessage*>(this);
    ret << "Message {";
    static_cast<const TMessageBase*>(self)->DebugString(ret, printData);
    self->GetPartitionSession()->DebugString(ret);
    ret << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage

TCompressedMessage::TCompressedMessage(ECodec codec,
                                       const TString& data,
                                       TMessageInformation information,
                                       TPartitionSession::TPtr partitionSession)
    : TMessageBase(data, std::move(information))
    , TPartitionSessionAccessor(std::move(partitionSession))
    , Codec(codec) {
}

ECodec TCompressedMessage::GetCodec() const {
    return Codec;
}

ui64 TCompressedMessage::GetUncompressedSize() const {
    return Information.UncompressedSize;
}

void TCompressedMessage::Commit() {
    static_cast<TPartitionStreamImpl<false>*>(PartitionSession.Get())
        ->Commit(Information.Offset, Information.Offset + 1);
}

template<>
void TPrintable<TCompressedMessage>::DebugString(TStringBuilder& ret, bool printData) const {
    const auto* self = static_cast<const TCompressedMessage*>(this);
    ret << "CompressedMessage {";
    static_cast<const TMessageBase*>(self)->DebugString(ret, printData);
    self->GetPartitionSession()->DebugString(ret);
    ret << " Codec: " << self->GetCodec()
        << " Uncompressed size: " << self->GetUncompressedSize()
        << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent

TDataReceivedEvent::TDataReceivedEvent(TVector<TMessage> messages, TVector<TCompressedMessage> compressedMessages,
                                       TPartitionSession::TPtr partitionSession)
    : TPartitionSessionAccessor(std::move(partitionSession))
    , Messages(std::move(messages))
    , CompressedMessages(std::move(compressedMessages)) {
    for (size_t i = 0; i < GetMessagesCount(); ++i) {
        auto [from, to] = GetMessageOffsetRange(*this, i);
        if (OffsetRanges.empty() || OffsetRanges.back().second != from) {
            OffsetRanges.emplace_back(from, to);
        } else {
            OffsetRanges.back().second = to;
        }
    }
}

void TDataReceivedEvent::Commit() {
    for (auto [from, to] : OffsetRanges) {
        static_cast<TPartitionStreamImpl<false>*>(PartitionSession.Get())->Commit(from, to);
    }
}

template<>
void TPrintable<TDataReceivedEvent>::DebugString(TStringBuilder& ret, bool printData) const {
    const auto* self = static_cast<const TDataReceivedEvent*>(this);
    ret << "DataReceived {";
    self->GetPartitionSession()->DebugString(ret);
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent

TCommitOffsetAcknowledgementEvent::TCommitOffsetAcknowledgementEvent(TPartitionSession::TPtr partitionSession,
                                                                     ui64 committedOffset)
    : TPartitionSessionAccessor(std::move(partitionSession))
    , CommittedOffset(committedOffset) {
}

template<>
void TPrintable<TCommitOffsetAcknowledgementEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TCommitOffsetAcknowledgementEvent*>(this);
    ret << "CommitAcknowledgement {";
    self->GetPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TStartPartitionSessionEvent

TStartPartitionSessionEvent::TStartPartitionSessionEvent(TPartitionSession::TPtr partitionSession, ui64 committedOffset,
                                                         ui64 endOffset)
    : TPartitionSessionAccessor(std::move(partitionSession))
    , CommittedOffset(committedOffset)
    , EndOffset(endOffset) {
}

void TStartPartitionSessionEvent::Confirm(TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset) {
    if (PartitionSession) {
        static_cast<TPartitionStreamImpl<false>*>(PartitionSession.Get())
            ->ConfirmCreate(readOffset, commitOffset);
    }
}

template<>
void TPrintable<TStartPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TStartPartitionSessionEvent*>(this);
    ret << "StartPartitionSession {";
    self->GetPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " EndOffset: " << self->GetEndOffset()
        << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TStopPartitionSessionEvent

TStopPartitionSessionEvent::TStopPartitionSessionEvent(TPartitionSession::TPtr partitionSession, ui64 committedOffset)
    : TPartitionSessionAccessor(std::move(partitionSession))
    , CommittedOffset(committedOffset) {
}

void TStopPartitionSessionEvent::Confirm() {
    if (PartitionSession) {
        static_cast<TPartitionStreamImpl<false>*>(PartitionSession.Get())->ConfirmDestroy();
    }
}

template<>
void TPrintable<TStopPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TStopPartitionSessionEvent*>(this);
    ret << "StopPartitionSession {";
    self->GetPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TEndPartitionSessionEvent

TEndPartitionSessionEvent::TEndPartitionSessionEvent(TPartitionSession::TPtr partitionSession, std::vector<ui32>&& adjacentPartitionIds, std::vector<ui32>&& childPartitionIds)
    : TPartitionSessionAccessor(std::move(partitionSession))
    , AdjacentPartitionIds(std::move(adjacentPartitionIds))
    , ChildPartitionIds(std::move(childPartitionIds)) {
}

void TEndPartitionSessionEvent::Confirm() {
    if (PartitionSession) {
        static_cast<TPartitionStreamImpl<false>*>(PartitionSession.Get())->ConfirmEnd(GetChildPartitionIds());
    }
}

void JoinIds(TStringBuilder& ret, const std::vector<ui32> ids) {
    ret << "[";
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i) {
            ret << ", ";
        }
        ret << ids[i];
    }
    ret << "]";
}

template<>
void TPrintable<TEndPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TEndPartitionSessionEvent*>(this);
    ret << "EndPartitionSession {";
    self->GetPartitionSession()->DebugString(ret);
    ret << " AdjacentPartitionIds: ";
    JoinIds(ret, self->GetAdjacentPartitionIds());
    ret << " ChildPartitionIds: ";
    JoinIds(ret, self->GetChildPartitionIds());
    ret << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TPartitionSessionStatusEvent

TPartitionSessionStatusEvent::TPartitionSessionStatusEvent(TPartitionSession::TPtr partitionSession,
                                                           ui64 committedOffset, ui64 readOffset, ui64 endOffset,
                                                           TInstant writeTimeHighWatermark)
    : TPartitionSessionAccessor(std::move(partitionSession))
    , CommittedOffset(committedOffset)
    , ReadOffset(readOffset)
    , EndOffset(endOffset)
    , WriteTimeHighWatermark(writeTimeHighWatermark) {
}

template<>
void TPrintable<TPartitionSessionStatusEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TPartitionSessionStatusEvent*>(this);
    ret << "PartitionSessionStatus {";
    self->GetPartitionSession()->DebugString(ret);
    ret << " CommittedOffset: " << self->GetCommittedOffset()
        << " ReadOffset: " << self->GetReadOffset()
        << " EndOffset: " << self->GetEndOffset()
        << " WriteWatermark: " << self->GetWriteTimeHighWatermark()
        << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TPartitionSessionClosedEvent

TPartitionSessionClosedEvent::TPartitionSessionClosedEvent(TPartitionSession::TPtr partitionSession, EReason reason)
    : TPartitionSessionAccessor(std::move(partitionSession))
    , Reason(reason)
{
}

template<>
void TPrintable<TPartitionSessionClosedEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TPartitionSessionClosedEvent*>(this);
    ret << "PartitionSessionClosed {";
    self->GetPartitionSession()->DebugString(ret);
    ret << " Reason: " << self->GetReason()
        << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TSessionClosedEvent

template<>
void TPrintable<TSessionClosedEvent>::DebugString(TStringBuilder& ret, bool) const {
    const auto* self = static_cast<const TSessionClosedEvent*>(this);
    ret << "SessionClosed { Status: " << self->GetStatus()
        << " Issues: \"" << IssuesSingleLineString(self->GetIssues())
        << "\" }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent

TString DebugString(const TReadSessionEvent::TEvent& event) {
    return std::visit([](const auto& ev) { return ev.DebugString(); }, event);
}

} // namespace NYdb::NPersQueue
