#include "topic_message.h"

namespace NKikimr::NReplication {

using TDataEvent = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent;
using ECodec = NYdb::NTopic::ECodec;

TTopicMessage::TTopicMessage(const TDataEvent::TMessageBase& msg, ECodec codec, ui64 uncompressedSize)
    : TDataEvent::TMessageInformation(
        msg.GetOffset(),
        msg.GetProducerId(),
        msg.GetSeqNo(),
        msg.GetCreateTime(),
        msg.GetWriteTime(),
        msg.GetMeta(),
        msg.GetMessageMeta(),
        uncompressedSize,
        msg.GetMessageGroupId()
    )
    , Codec(codec)
    , Data(msg.GetData())
{
}

TTopicMessage::TTopicMessage(const TDataEvent::TMessage& msg)
    : TTopicMessage(msg, ECodec::RAW, msg.GetData().size())
{
}

TTopicMessage::TTopicMessage(const TDataEvent::TCompressedMessage& msg)
    : TTopicMessage(msg, msg.GetCodec(), msg.GetUncompressedSize())
{
}

TTopicMessage::TTopicMessage(TDataEvent::TMessageInformation&& msg, TString&& data)
    : TDataEvent::TMessageInformation(std::move(msg))
    , Codec(ECodec::RAW)
    , Data(std::move(data))
{
}

TTopicMessage::TTopicMessage(ui64 offset, const TString& data)
    : TDataEvent::TMessageInformation(offset, "", 0, TInstant::Zero(), TInstant::Zero(), nullptr, nullptr, 0, "")
    , Codec(ECodec::RAW)
    , Data(data)
{
}

NYdb::NTopic::TMessageMeta::TPtr TTopicMessage::GetMessageMeta() const {
    return MessageMeta;
}

ECodec TTopicMessage::GetCodec() const {
    return Codec;
}

const TString& TTopicMessage::GetData() const {
    return Data;
}

TString& TTopicMessage::GetData() {
    return Data;
}

ui64 TTopicMessage::GetOffset() const {
    return Offset;
}

ui64 TTopicMessage::GetSeqNo() const {
    return SeqNo;
}

TInstant TTopicMessage::GetCreateTime() const {
    return CreateTime;
}

TInstant TTopicMessage::GetWriteTime() const {
    return WriteTime;
}

TString TTopicMessage::GetMessageGroupId() const {
    return TString(MessageGroupId);
}

TString TTopicMessage::GetProducerId() const {
    return TString(ProducerId);
}

void TTopicMessage::Out(IOutputStream& out) const {
    out << "{"
        << " Codec: " << Codec
        << " Data: " << Data.size() << "b"
        << " Offset: " << Offset
        << " SeqNo: " << SeqNo
        << " CreateTime: " << CreateTime
        << " WriteTime: " << WriteTime
        << " MessageGroupId: " << MessageGroupId
        << " ProducerId: " << ProducerId
    << " }";
}

}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::TTopicMessage, o, x) {
    return x.Out(o);
}
