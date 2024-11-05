#include "event.h"
#include "event_pb.h"

#include <ydb/library/actors/protos/actors.pb.h>

namespace NActors {

    const TScopeId TScopeId::LocallyGenerated{
        Max<ui64>(), Max<ui64>()
    };

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
            auto chainBuf = serializer.Release(Event->CreateSerializationInfo());
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
            Buffer = serializer.Release(Event->CreateSerializationInfo());
            return Buffer;
        }
        return new TEventSerializedData;
    }
}
