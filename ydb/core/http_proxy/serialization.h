#pragma once

#include <contrib/libs/protobuf/src/google/protobuf/message.h>
#include <library/cpp/mime/types/mime.h>

namespace NKikimr::NHttpProxy {

void DeserializeCbor(NProtoBuf::Message& message, const TStringBuf& input);
void DeserializeJson(NProtoBuf::Message& message, const TStringBuf& input);

class IDeserializer {
public:
    virtual ~IDeserializer() = default;

//    void Serialize(const TType& message, TString& output);
    virtual void Deserialize(NProtoBuf::Message& message, const TStringBuf& input) const = 0;
};

class TDefaultCborDeserializer: public IDeserializer {
public:
    void Deserialize(NProtoBuf::Message& message, const TStringBuf& input) const override;
};

class TDefaultJsonDeserializer: public IDeserializer {
public:
    void Deserialize(NProtoBuf::Message& message, const TStringBuf& input) const override;
};

class IDeserializerFactory {
public:
    virtual ~IDeserializerFactory() = default;

    virtual IDeserializer* Create(const MimeTypes mimeType) const = 0;
};

} // namespace NKikimr::NHttpProxy
