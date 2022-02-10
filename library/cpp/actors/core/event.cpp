#include "event.h"
#include "event_pb.h"

namespace NActors {

    const TScopeId TScopeId::LocallyGenerated{
        Max<ui64>(), Max<ui64>()
    };

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
            auto chainBuf = serializer.Release(Event->IsExtendedFormat());
            Event.Reset();
            return chainBuf;
        }
        return new TEventSerializedData;
    }

    TIntrusivePtr<TEventSerializedData> IEventHandle::GetChainBuffer() {
        if (Buffer)
            return Buffer;
        if (Event) {
            TAllocChunkSerializer serializer;
            Event->SerializeToArcadiaStream(&serializer); 
            Buffer = serializer.Release(Event->IsExtendedFormat());
            return Buffer;
        }
        return new TEventSerializedData;
    }
}
