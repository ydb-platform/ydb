#pragma once

#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.pb.h>

#include <util/generic/ptr.h>

class TDynamicPrototype;

using TDynamicPrototypePtr = TIntrusivePtr<TDynamicPrototype>;

class TDynamicPrototype : public TThrRefBase {
public:
    class TDynamicMessage {
    public:
        TDynamicMessage() = default;
        TDynamicMessage(THolder<NProtoBuf::Message> message, TIntrusivePtr<TDynamicPrototype> prototype);
        NProtoBuf::Message& operator*();
        NProtoBuf::Message* operator->();
        NProtoBuf::Message* Get();
    private:
        TIntrusivePtr<TDynamicPrototype> Prototype;
        THolder<NProtoBuf::Message> Message;
    };

    static TDynamicPrototypePtr Create(const NProtoBuf::FileDescriptorSet& fds, const TString& messageName, bool yqlHack = false);

    // https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.dynamic_message#DynamicMessageFactory
    // Must outlive created messages
    TDynamicMessage Create();

    THolder<NProtoBuf::Message> CreateUnsafe() const;

    const NProtoBuf::Descriptor* GetDescriptor() const;

private:
    TDynamicPrototype(const NProtoBuf::FileDescriptorSet& fds, const TString& messageName, bool yqlHack);

    NProtoBuf::DescriptorPool Pool;
    const NProtoBuf::Descriptor* Descriptor;
    NProtoBuf::DynamicMessageFactory Factory;
    const NProtoBuf::Message* Prototype;
};
