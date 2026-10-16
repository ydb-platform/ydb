#include "event.h"
#include "event_pb.h"

#include <ydb/library/actors/protos/actors.pb.h>

namespace NActors {

    const TScopeId TScopeId::LocallyGenerated{
        Max<ui64>(), Max<ui64>()
    };

    const TEventSerializedData IEventHandle::EmptyBuffer;

    IEventBase* IEventHandle::GetSlow(ui32 expectedType, TEventLoader loader, const std::type_info& typeInfo) {
        Y_ENSURE(Type == expectedType,
            "Event type " << Type << " doesn't match the expected type " << expectedType
            << " class " << TypeName(typeInfo));

        if (!Event) {
            if (loader) {
                Event.Reset(loader(Buffer ? Buffer.Get() : &EmptyBuffer));
                Buffer.Reset();
            } else {
                Y_ENSURE(false, "Event type " << Type << " cannot be loaded by class " << TypeName(typeInfo));
            }
        }

        if (Event) {
            return Event.Get();
        }

        Y_ENSURE(false, "Failed to Load() event type " << Type << " class " << TypeName(typeInfo));
    }

    void IEventHandle::ResetBuffer() {
        Buffer.Reset();
    }

    TString IEventHandle::GetTypeName() const {
        return HasEvent() ? TypeName(*(const_cast<IEventHandle*>(this)->GetBase())) : TypeName(*this);
    }

    TString IEventHandle::ToString() const {
        return HasEvent() ? const_cast<IEventHandle*>(this)->GetBase()->ToString().data() : "serialized?";
    }

    std::unique_ptr<IEventHandle> IEventHandle::Forward(std::unique_ptr<IEventHandle>&& ev, TActorId recipient) {
        return std::unique_ptr<IEventHandle>(ev->Forward(recipient).Release());
    }

    TIntrusivePtr<TEventSerializedData> IEventHandle::ReleaseChainBuffer() {
        if (Buffer) {
            TIntrusivePtr<TEventSerializedData> result;
            DoSwap(result, Buffer);
            Event.Reset();
            return result;
        }
        if (Event) {
            TAllocChunkSerializer serializer;
            Event->SerializeToArcadiaStream(&serializer);
            auto chainBuf = serializer.Release(Event->CreateSerializationInfo(false));
            Event.Reset();
            return chainBuf;
        }
        return new TEventSerializedData;
    }

    TIntrusivePtr<TEventSerializedData> IEventHandle::GetChainBuffer() {
        if (Buffer) {
            return Buffer;
        }
        if (Event) {
            TAllocChunkSerializer serializer;
            Event->SerializeToArcadiaStream(&serializer);
            Buffer = serializer.Release(Event->CreateSerializationInfo(false));
            return Buffer;
        }
        return new TEventSerializedData;
    }

    void IEventHandle::Preserialize(bool allowExternalDataChannel) {
        if (Event && !Buffer) {
            TAllocChunkSerializer serializer;
            Event->SerializeToArcadiaStream(&serializer);
            Buffer = serializer.Release(Event->CreateSerializationInfo(allowExternalDataChannel));
            Event.Reset();
        }
    }

#ifndef NDEBUG
    void IEventHandle::DoTrackNextEvent() {
        TrackNextEvent = true;
    }
#endif

}
