#include "dynamic_prototype.h"

#include <util/generic/yexception.h>

TDynamicPrototype::TDynamicMessage::TDynamicMessage(THolder<NProtoBuf::Message> message, TIntrusivePtr<TDynamicPrototype> prototype)
    : Prototype(std::move(prototype))
    , Message(std::move(message))
{
}

NProtoBuf::Message& TDynamicPrototype::TDynamicMessage::operator*() {
    return *Message;
}

NProtoBuf::Message* TDynamicPrototype::TDynamicMessage::operator->() {
    return Get();
}

NProtoBuf::Message* TDynamicPrototype::TDynamicMessage::Get() {
    return Message.Get();
}

TDynamicPrototypePtr TDynamicPrototype::Create(const NProtoBuf::FileDescriptorSet& fds, const TString& messageName, bool yqlHack) {
    return new TDynamicPrototype(fds, messageName, yqlHack);
}

TDynamicPrototype::TDynamicPrototype(const NProtoBuf::FileDescriptorSet& fileDescriptorSet, const TString& messageName, bool yqlHack)
{
    const NProtoBuf::FileDescriptor* fileDescriptor;

    for (int i = 0; i < fileDescriptorSet.file_size(); ++i) {
        fileDescriptor = Pool.BuildFile(fileDescriptorSet.file(i));
        if (fileDescriptor == nullptr) {
            ythrow yexception() << "can't build file descriptor";
        }
    }

    Descriptor = Pool.FindMessageTypeByName(messageName);

    // Первоначальный вариант поведения, когда тип определялся
    // по имени сообщения верхнего уровня в заданном файле.
    if (yqlHack && !Descriptor) {
        Descriptor = fileDescriptor->FindMessageTypeByName(messageName);
    }

    if (!Descriptor) {
        ythrow yexception() << "no descriptor for " << messageName;
    }

    Prototype = Factory.GetPrototype(Descriptor);
}

TDynamicPrototype::TDynamicMessage TDynamicPrototype::Create() {
    return TDynamicMessage(CreateUnsafe(), this);
}

THolder<NProtoBuf::Message> TDynamicPrototype::CreateUnsafe() const {
    return THolder<NProtoBuf::Message>(Prototype->New());
}

const NProtoBuf::Descriptor* TDynamicPrototype::GetDescriptor() const {
    return Descriptor;
}
