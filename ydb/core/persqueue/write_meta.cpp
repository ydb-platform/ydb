#include "write_meta.h"

#include <ydb/core/persqueue/codecs/pqv1.h>


namespace NKikimr {


void SetMetaField(NKikimrPQClient::TDataChunk& proto, const TString& key, const TString& value) {
    if (key == "server") {
        proto.MutableMeta()->SetServer(value);
    } else if (key == "ident") {
        proto.MutableMeta()->SetIdent(value);
    } else if (key == "logtype") {
        proto.MutableMeta()->SetLogType(value);
    } else if (key == "file") {
        proto.MutableMeta()->SetFile(value);
    } else {
        auto res = proto.MutableExtraFields()->AddItems();
        res->SetKey(key);
        res->SetValue(value);
    }
}

TString GetSerializedData(const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage& message) {
    NKikimrPQClient::TDataChunk proto;
    for (const auto& item : message.GetMeta(0)->Fields) {
        SetMetaField(proto, item.first, item.second);
    }
    proto.SetIp(message.GetIp(0));
    proto.SetSeqNo(message.GetSeqNo(0));
    proto.SetCreateTime(message.GetCreateTime(0).MilliSeconds());
    auto codec = NPQ::FromV1Codec(message.GetCodec());
    Y_ABORT_UNLESS(codec);
    proto.SetCodec(codec.value());
    proto.SetData(message.GetData());

    TString str;
    bool res = proto.SerializeToString(&str);
    Y_ABORT_UNLESS(res);
    return str;
}

TString GetSerializedData(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage& message) {
    NKikimrPQClient::TDataChunk proto;
    for (const auto& item : message.GetMeta()->Fields) {
        if (item.first == "_ip") {
            proto.SetIp(item.second);
        } else if (item.first == "_encoded_producer_id") {
            // Skip.
        } else {
            SetMetaField(proto, item.first, item.second);
        }
    }
    auto& fields = message.GetMessageMeta()->Fields;
    if (!fields.empty()) {
        for (const auto& item : fields) {
            auto& metaItem = *proto.AddMessageMeta();
            metaItem.set_key(item.first);
            metaItem.set_value(item.second);
        }
    }
    proto.SetSeqNo(message.GetSeqNo());
    proto.SetCreateTime(message.GetCreateTime().MilliSeconds());
    auto codec = NPQ::FromTopicCodec(message.GetCodec());
    proto.SetCodec(codec);
    proto.SetData(message.GetData());

    TString str;
    bool res = proto.SerializeToString(&str);
    Y_ABORT_UNLESS(res);
    return str;
}

NKikimrPQClient::TDataChunk GetDeserializedData(const TString& string) {
    NKikimrPQClient::TDataChunk proto;
    bool res = proto.ParseFromString(string);
    Y_UNUSED(res);
    //TODO: check errors of parsing
    return proto;
}

} // NKikimr

