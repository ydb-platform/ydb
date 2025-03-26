#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h>

namespace NKikimr::NReplication {

class TTopicMessage: protected NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation {
    using TDataEvent = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent;
    using ECodec = NYdb::NTopic::ECodec;

    explicit TTopicMessage(const TDataEvent::TMessageBase& msg, ECodec codec, ui64 uncompressedSize);

public:
    explicit TTopicMessage(const TDataEvent::TMessage& msg);
    explicit TTopicMessage(const TDataEvent::TCompressedMessage& msg);
    explicit TTopicMessage(ui64 offset, const TString& data); // from scratch

    ECodec GetCodec() const;
    const TString& GetData() const;
    TString& GetData();
    ui64 GetOffset() const;
    ui64 GetSeqNo() const;
    TInstant GetCreateTime() const;
    TString GetMessageGroupId() const;
    TString GetProducerId() const;
    void Out(IOutputStream& out) const;

private:
    ECodec Codec;
    TString Data;
};

}
