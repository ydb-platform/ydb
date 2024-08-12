#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/include/read_events.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/read_session_impl.ipp>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/aliases.h>

namespace NYdb::NPersQueue {

TReadSessionEvent::TDataReceivedEvent::TMessageInformation::TMessageInformation(
    ui64 offset,
    TString messageGroupId,
    ui64 seqNo,
    TInstant createTime,
    TInstant writeTime,
    TString ip,
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

const TString& TReadSessionEvent::TDataReceivedEvent::IMessage::GetData() const {
    return Data;
}

const TPartitionStream::TPtr& TReadSessionEvent::TDataReceivedEvent::IMessage::GetPartitionStream() const {
    return PartitionStream;
}

const TString& TReadSessionEvent::TDataReceivedEvent::IMessage::GetPartitionKey() const {
    return PartitionKey;
}

const TString TReadSessionEvent::TDataReceivedEvent::IMessage::GetExplicitHash() const {
    return ExplicitHash;
}

TString TReadSessionEvent::TDataReceivedEvent::IMessage::DebugString(bool printData) const {
    TStringBuilder ret;
    DebugString(ret, printData);
    return std::move(ret);
}

TReadSessionEvent::TDataReceivedEvent::IMessage::IMessage(const TString& data,
                                                          TPartitionStream::TPtr partitionStream,
                                                          const TString& partitionKey,
                                                          const TString& explicitHash)
    : Data(data)
    , PartitionStream(partitionStream)
    , PartitionKey(partitionKey)
    , ExplicitHash(explicitHash)
{}

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

const TString& TReadSessionEvent::TDataReceivedEvent::TMessage::GetIp() const {
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

TReadSessionEvent::TDataReceivedEvent::TMessage::TMessage(const TString& data,
                                                          std::exception_ptr decompressionException,
                                                          const TMessageInformation& information,
                                                          TPartitionStream::TPtr partitionStream,
                                                          const TString& partitionKey,
                                                          const TString& explicitHash)
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

const TString& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetMessageGroupId(ui64 index) const {
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

const TString& TReadSessionEvent::TDataReceivedEvent::TCompressedMessage::GetIp(ui64 index) const {
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
                                                                              const TString& data,
                                                                              const TVector<TMessageInformation>& information,
                                                                              TPartitionStream::TPtr partitionStream,
                                                                              const TString& partitionKey,
                                                                              const TString& explicitHash)
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
