#pragma once

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TProtoStorageTypeString
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CProtobufMessage = std::derived_from<std::decay_t<T>, google::protobuf::Message>;

template <class T>
concept CProtobufMessageAsYson = CProtobufMessage<T> && !std::derived_from<std::decay_t<T>, TProtoStorageTypeString>;

template <class T>
concept CProtobufMessageAsString = CProtobufMessage<T> && std::derived_from<std::decay_t<T>, TProtoStorageTypeString>;

////////////////////////////////////////////////////////////////////////////////

template <CProtobufMessage TMessage>
class TProtoSerializedAsString
    : public TProtoStorageTypeString
    , public TMessage
{
public:
    using TMessage::TMessage;
    using TMessage::operator=;
};

template <CProtobufMessage TMessage>
class TProtoSerializedAsYson
    : public TMessage
{
public:
    using TMessage::TMessage;
    using TMessage::operator=;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
