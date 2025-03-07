#include <src/client/persqueue_public/include/read_events.h>

#include <src/client/topic/impl/common.h>
#include <src/client/topic/impl/read_session_impl.ipp>
#include <src/client/persqueue_public/impl/aliases.h>

namespace NYdb::inline Dev::NPersQueue {

TReadSessionEvent::TDataReceivedEvent::TMessageInformation::TMessageInformation(
    ui64 offset,
    std::string messageGroupId,
    ui64 seqNo,
    TInstant createTime,
    TInstant writeTime,
    std::string ip,
    TWriteSessionMeta::TPtr meta,
    ui64 uncompressedSize
)
    : Offset(offset)
    , MessageGroupId(messageGroupId)
    , SeqNo(seqNo)
    , CreateTime(createTime)
    , WriteTime(writeTime)
    , Ip(ip)
    , Meta(meta)
    , UncompressedSize(uncompressedSize)
{}

static void DebugStringImpl(const TReadSessionEvent::TDataReceivedEvent::TMessageInformation& info, TStringBuilder& ret) {
    ret << " Information: {"
        << " Offset: " << info.Offset
        << " SeqNo: " << info.SeqNo
        << " MessageGroupId: \"" << info.MessageGroupId << "\""
        << " CreateTime: " << info.CreateTime
        << " WriteTime: " << info.WriteTime
        << " Ip: \"" << info.Ip << "\""
        << " UncompressedSize: " << info.UncompressedSize;
    ret << " Meta: {";
    bool firstKey = true;
    for (const auto& [k, v] : info.Meta->Fields) {
        ret << (firstKey ? " \"" : ", \"") << k << "\": \"" << v << "\"";
        firstKey = false;
    }
    ret << " } }";
}

template <class TSerializeInformationFunc>
static void DebugStringImpl(TStringBuilder& ret,
                               const std::string& name,
                               const TReadSessionEvent::TDataReceivedEvent::IMessage& msg,
                               bool printData,
                               TSerializeInformationFunc serializeInformationFunc,
                               std::optional<ECodec> codec = std::nullopt)
{
    ret << name << " {";
    try {
        const std::string& data = msg.GetData();
        if (printData) {
            ret << " Data: \"" << data << "\"";
        } else {
            ret << " Data: .." << data.size() << " bytes..";
        }
    } catch (...) {
        ret << " DataDecompressionError: \"" << CurrentExceptionMessage() << "\"";
    }
    auto partitionStream = msg.GetPartitionStream();
    ret << " Partition stream id: " << partitionStream->GetPartitionStreamId()
        << " Cluster: \"" << partitionStream->GetCluster() << "\". Topic: \"" << partitionStream->GetTopicPath() << "\""
        << " Partition: " << partitionStream->GetPartitionId()
        << " PartitionKey: \"" << msg.GetPartitionKey() << "\"";
    if (codec) {
        ret << " Codec: " << codec.value();
    }
    serializeInformationFunc(ret);
    ret << " }";
}

const std::string& TReadSessionEvent::TDataReceivedEvent::IMessage::GetData() const {
    return Data;
}

const TPartitionStream::TPtr& TReadSessionEvent::TDataReceivedEvent::IMessage::GetPartitionStream() const {
    return PartitionStream;
}

const std::string& TReadSessionEvent::TDataReceivedEvent::IMessage::GetPartitionKey() const {
    return PartitionKey;
}

const std::string TReadSessionEvent::TDataReceivedEvent::IMessage::GetExplicitHash() const {
    return ExplicitHash;
}

std::string TReadSessionEvent::TDataReceivedEvent::IMessage::DebugString(bool printData) const {
    TStringBuilder ret;
    DebugString(ret, printData);
    return std::move(ret);
}

TReadSessionEvent::TDataReceivedEvent::IMessage::IMessage(const std::string& data,
                                                          TPartitionStream::TPtr partitionStream,
                                                          const std::string& partitionKey,
                                                          const std::string& explicitHash)
    : Data(data)
    , PartitionStream(partitionStream)
    , PartitionKey(partitionKey)
    , ExplicitHash(explicitHash)
{}

const std::string& TReadSessionEvent::TDataReceivedEvent::TMessage::GetData() const {
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

const std::string& TReadSessionEvent::TDataReceivedEvent::TMessage::GetMessageGroupId() const {
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

const std::string& TReadSessionEvent::TDataReceivedEvent::TMessage::GetIp() const {
    return Information.Ip;
}

const TWriteSessionMeta::TPtr& TReadSessionEvent::TDataReceivedEvent::TMessage::GetMeta() const {
    return Information.Meta;
}

void TReadSessionEvent::TDataReceivedEvent::TMessage::DebugString(TStringBuilder& ret, bool printData) const {
    DebugStringImpl(ret, "Message", *this, printData, [this](TStringBuilder& ret) {
        DebugStringImpl(this->Information, ret);
    });
}

TReadSessionEvent::TDataReceivedEvent::TMessage::TMessage(const std::string& data,
                                                          std::exception_ptr decompressionException,
                                                          const TMessageInformation& information,
                                                          TPartitionStream::TPtr partitionStream,
                                                          const std::string& partitionKey,
                                                          const std::string& explicitHash)
    : IMessage(data, partitionStream, partitionKey, explicitHash)
    , DecompressionException(std::move(decompressionException))
    , Information(information)
{
}

void TReadSessionEvent::TDataReceivedEvent::TMessage::Commit() {
    static_cast<TPartitionStreamImpl*>(PartitionStream.Get())->Commit(Information.Offset, Information.Offset + 1);
}

ui64 TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetBlocksCount() const {
    return Information.size();
}

ECodec TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetCodec() const {
    return Codec;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetOffset(ui64 index) const {
    return Information.at(index).Offset;
}

const std::string& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetMessageGroupId(ui64 index) const {
    return Information.at(index).MessageGroupId;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetSeqNo(ui64 index) const {
    return Information.at(index).SeqNo;
}

TInstant TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetCreateTime(ui64 index) const {
    return Information.at(index).CreateTime;
}

TInstant TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetWriteTime(ui64 index) const {
    return Information.at(index).WriteTime;
}

const std::string& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetIp(ui64 index) const {
    return Information.at(index).Ip;
}

const TWriteSessionMeta::TPtr& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetMeta(ui64 index) const {
    return Information.at(index).Meta;
}

ui64 TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetUncompressedSize(ui64 index) const {
    return Information.at(index).UncompressedSize;
}

void TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::DebugString(TStringBuilder& ret, bool printData) const {
    DebugStringImpl(
        ret,
        "CompressedMessage",
        *this,
        printData,
        [this](TStringBuilder& ret) {
            for (auto& info : this->Information) {
                DebugStringImpl(info, ret);
            }
        },
        Codec
    );
}

TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::TCompressedMessage(ECodec codec,
                                                                              const std::string& data,
                                                                              const std::vector<TMessageInformation>& information,
                                                                              TPartitionStream::TPtr partitionStream,
                                                                              const std::string& partitionKey,
                                                                              const std::string& explicitHash)
    : IMessage(data, partitionStream, partitionKey, explicitHash)
    , Codec(codec)
    , Information(information)
{}

void TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::Commit() {
    static_cast<TPartitionStreamImpl*>(PartitionStream.Get())->Commit(
        Information.front().Offset,
        Information.back().Offset + 1
    );
}

} // namespace NYdb::NPersQueue
