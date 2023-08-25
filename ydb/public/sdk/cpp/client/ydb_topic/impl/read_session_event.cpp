#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>

namespace NYdb::NTopic {

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation

TReadSessionEvent::TDataReceivedEvent::TMessageInformation::TMessageInformation(
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

static void DebugStringImpl(const TReadSessionEvent::TDataReceivedEvent::TMessageInformation& info, TStringBuilder& ret) {
    ret << " Information: {"
        << " Offset: " << info.Offset
        << " ProducerId: \"" << info.ProducerId << "\""
        << " SeqNo: " << info.SeqNo
        << " CreateTime: " << info.CreateTime
        << " WriteTime: " << info.WriteTime
        << " UncompressedSize: " << info.UncompressedSize
        << " MessageGroupId: \"" << info.MessageGroupId << "\"";
    ret << " Meta: {";
    bool firstKey = true;
    for (const auto& [k, v] : info.Meta->Fields) {
        ret << (firstKey ? " \"" : ", \"") << k << "\": \"" << v << "\"";
        firstKey = false;
    }
    ret << " } }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::IMessage

TReadSessionEvent::TDataReceivedEvent::IMessage::IMessage(const TString& data,
                                                          TPartitionSession::TPtr partitionSession)
    : Data(data)
    , PartitionSession(partitionSession)
{}

const TString& TReadSessionEvent::TDataReceivedEvent::IMessage::GetData() const {
    return Data;
}

const TPartitionSession::TPtr& TReadSessionEvent::TDataReceivedEvent::IMessage::GetPartitionSession() const {
    return PartitionSession;
}

TString TReadSessionEvent::TDataReceivedEvent::IMessage::DebugString(bool printData) const {
    TStringBuilder ret;
    DebugString(ret, printData);
    return std::move(ret);
}

template <class TSerializeInformationFunc>
static void DebugStringImpl(TStringBuilder& ret,
                               const TString& name,
                               const TReadSessionEvent::TDataReceivedEvent::IMessage& msg,
                               bool printData,
                               TSerializeInformationFunc serializeInformationFunc,
                               std::optional<ECodec> codec = std::nullopt)
{
    ret << name << " {";
    try {
        const TString& data = msg.GetData();
        if (printData) {
            ret << " Data: \"" << data << "\"";
        } else {
            ret << " Data: .." << data.size() << " bytes..";
        }
    } catch (...) {
        ret << " DataDecompressionError: \"" << CurrentExceptionMessage() << "\"";
    }
    auto partitionSession = msg.GetPartitionSession();
    ret << " Partition session id: " << partitionSession->GetPartitionSessionId()
        << " Topic: \"" << partitionSession->GetTopicPath() << "\""
        << " Partition: " << partitionSession->GetPartitionId();
    if (codec) {
        ret << " Codec: " << codec.value();
    }
    serializeInformationFunc(ret);
    ret << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage

TReadSessionEvent::TDataReceivedEvent::TMessage::TMessage(const TString& data,
                                                          std::exception_ptr decompressionException,
                                                          const TMessageInformation& information,
                                                          TPartitionSession::TPtr partitionSession)
    : IMessage(data, partitionSession)
    , DecompressionException(std::move(decompressionException))
    , Information(information)
{
}

const TString& TReadSessionEvent::TDataReceivedEvent::TMessage::GetData() const {
    if (DecompressionException) {
        std::rethrow_exception(DecompressionException);
    }
    return IMessage::GetData();
}

bool TReadSessionEvent::TDataReceivedEvent::TMessage::HasException() const {
    return DecompressionException != nullptr;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TMessage::GetOffset() const {
    return Information.Offset;
}

const TString& TReadSessionEvent::TDataReceivedEvent::TMessage::GetProducerId() const {
    return Information.ProducerId;
}

const TString& TReadSessionEvent::TDataReceivedEvent::TMessage::GetMessageGroupId() const {
    return Information.MessageGroupId;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TMessage::GetSeqNo() const {
    return Information.SeqNo;
}

TInstant TReadSessionEvent::TDataReceivedEvent::TMessage::GetCreateTime() const {
    return Information.CreateTime;
}

TInstant TReadSessionEvent::TDataReceivedEvent::TMessage::GetWriteTime() const {
    return Information.WriteTime;
}

const TWriteSessionMeta::TPtr& TReadSessionEvent::TDataReceivedEvent::TMessage::GetMeta() const {
    return Information.Meta;
}

const TMessageMeta::TPtr& TReadSessionEvent::TDataReceivedEvent::TMessage::GetMessageMeta() const {
    return Information.MessageMeta;
}

void TReadSessionEvent::TDataReceivedEvent::TMessage::Commit() {
    static_cast<NPersQueue::TPartitionStreamImpl<false>*>(PartitionSession.Get())
        ->Commit(Information.Offset, Information.Offset + 1);
}

void TReadSessionEvent::TDataReceivedEvent::TMessage::DebugString(TStringBuilder& ret, bool printData) const {
    DebugStringImpl(ret, "Message", *this, printData, [this](TStringBuilder& ret) {
        DebugStringImpl(this->Information, ret);
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage

TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::TCompressedMessage(ECodec codec,
                                                                              const TString& data,
                                                                              const TMessageInformation& information,
                                                                              TPartitionSession::TPtr partitionSession)
    : IMessage(data, partitionSession)
    , Codec(codec)
    , Information(information)
{}


ECodec TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetCodec() const {
    return Codec;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetOffset() const {
    return Information.Offset;
}

const TString& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetProducerId() const {
    return Information.ProducerId;
}

const TString& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetMessageGroupId() const {
    return Information.MessageGroupId;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetSeqNo() const {
    return Information.SeqNo;
}

TInstant TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetCreateTime() const {
    return Information.CreateTime;
}

TInstant TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetWriteTime() const {
    return Information.WriteTime;
}

const TWriteSessionMeta::TPtr& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetMeta() const {
    return Information.Meta;
}

const TMessageMeta::TPtr& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetMessageMeta() const {
    return Information.MessageMeta;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetUncompressedSize() const {
    return Information.UncompressedSize;
}

void TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::Commit() {
    static_cast<NPersQueue::TPartitionStreamImpl<false>*>(PartitionSession.Get())
        ->Commit(Information.Offset, Information.Offset + 1);
}

void TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::DebugString(TStringBuilder& ret, bool printData) const {
    DebugStringImpl(
        ret, "CompressedMessage", *this, printData,
        [this](TStringBuilder& ret) { DebugStringImpl(this->Information, ret); }, Codec);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TDataReceivedEvent

TReadSessionEvent::TDataReceivedEvent::TDataReceivedEvent(TVector<TMessage> messages,
                                                          TVector<TCompressedMessage> compressedMessages,
                                                          TPartitionSession::TPtr partitionSession)
    : Messages(std::move(messages))
    , CompressedMessages(std::move(compressedMessages))
    , PartitionSession(std::move(partitionSession))
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

TString TReadSessionEvent::TDataReceivedEvent::DebugString(bool printData) const {
    TStringBuilder ret;
    ret << "DataReceived { PartitionSessionId: " << GetPartitionSession()->GetPartitionSessionId()
        << " PartitionId: " << GetPartitionSession()->GetPartitionId();
    for (const auto& message : Messages) {
        ret << " ";
        message.DebugString(ret, printData);
    }
    for (const auto& message : CompressedMessages) {
        ret << " ";
        message.DebugString(ret, printData);
    }
    ret << " }";
    return std::move(ret);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent

TReadSessionEvent::TCommitOffsetAcknowledgementEvent::TCommitOffsetAcknowledgementEvent(TPartitionSession::TPtr partitionSession, ui64 committedOffset)
    : PartitionSession(std::move(partitionSession))
    , CommittedOffset(committedOffset)
{
}


TString TReadSessionEvent::TCommitOffsetAcknowledgementEvent::DebugString() const {
    return TStringBuilder() << "CommitAcknowledgement { PartitionSessionId: " << GetPartitionSession()->GetPartitionSessionId()
                            << " PartitionId: " << GetPartitionSession()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset()
                            << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TStartPartitionSessionEvent

TReadSessionEvent::TStartPartitionSessionEvent::TStartPartitionSessionEvent(TPartitionSession::TPtr partitionSession,
                                                                            ui64 committedOffset, ui64 endOffset)
    : PartitionSession(std::move(partitionSession))
    , CommittedOffset(committedOffset)
    , EndOffset(endOffset) {
}

void TReadSessionEvent::TStartPartitionSessionEvent::Confirm(TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset) {
    if (PartitionSession) {
        static_cast<NPersQueue::TPartitionStreamImpl<false>*>(PartitionSession.Get())
            ->ConfirmCreate(readOffset, commitOffset);
    }
}

TString TReadSessionEvent::TStartPartitionSessionEvent::DebugString() const {
    return TStringBuilder() << "CreatePartitionSession { PartitionSessionId: "
                            << GetPartitionSession()->GetPartitionSessionId()
                            << " TopicPath: " << GetPartitionSession()->GetTopicPath()
                            << " PartitionId: " << GetPartitionSession()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset()
                            << " EndOffset: " << GetEndOffset() << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TStopPartitionSessionEvent

TReadSessionEvent::TStopPartitionSessionEvent::TStopPartitionSessionEvent(TPartitionSession::TPtr partitionSession,
                                                                          bool committedOffset)
    : PartitionSession(std::move(partitionSession))
    , CommittedOffset(committedOffset) {
}

void TReadSessionEvent::TStopPartitionSessionEvent::Confirm() {
    if (PartitionSession) {
        static_cast<NPersQueue::TPartitionStreamImpl<false>*>(PartitionSession.Get())->ConfirmDestroy();
    }
}

TString TReadSessionEvent::TStopPartitionSessionEvent::DebugString() const {
    return TStringBuilder() << "DestroyPartitionSession { PartitionSessionId: "
                            << GetPartitionSession()->GetPartitionSessionId()
                            << " PartitionId: " << GetPartitionSession()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset() << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TPartitionSessionStatusEvent

TReadSessionEvent::TPartitionSessionStatusEvent::TPartitionSessionStatusEvent(TPartitionSession::TPtr partitionSession,
                                                                              ui64 committedOffset, ui64 readOffset,
                                                                              ui64 endOffset,
                                                                              TInstant writeTimeHighWatermark)
    : PartitionSession(std::move(partitionSession))
    , CommittedOffset(committedOffset)
    , ReadOffset(readOffset)
    , EndOffset(endOffset)
    , WriteTimeHighWatermark(writeTimeHighWatermark) {
}

TString TReadSessionEvent::TPartitionSessionStatusEvent::DebugString() const {
    return TStringBuilder() << "PartitionSessionStatus { PartitionSessionId: "
                            << GetPartitionSession()->GetPartitionSessionId()
                            << " PartitionId: " << GetPartitionSession()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset() << " ReadOffset: " << GetReadOffset()
                            << " EndOffset: " << GetEndOffset()
                            << " WriteWatermark: " << GetWriteTimeHighWatermark() << " }";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEvent::TPartitionSessionClosedEvent

TReadSessionEvent::TPartitionSessionClosedEvent::TPartitionSessionClosedEvent(TPartitionSession::TPtr partitionSession, EReason reason)
    : PartitionSession(std::move(partitionSession))
    , Reason(reason)
{
}

TString TReadSessionEvent::TPartitionSessionClosedEvent::DebugString() const {
    return TStringBuilder() << "PartitionSessionClosed { PartitionSessionId: "
                            << GetPartitionSession()->GetPartitionSessionId()
                            << " PartitionId: " << GetPartitionSession()->GetPartitionId()
                            << " Reason: " << GetReason() << " }";
}

TString TSessionClosedEvent::DebugString() const {
    return
        TStringBuilder() << "SessionClosed { Status: " << GetStatus()
                         << " Issues: \"" << NPersQueue::IssuesSingleLineString(GetIssues())
                         << "\" }";
}

TString DebugString(const TReadSessionEvent::TEvent& event) {
    return std::visit([](const auto& ev) { return ev.DebugString(); }, event);
}

} // namespace NYdb::NPersQueue
