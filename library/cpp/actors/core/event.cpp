#include "event.h"
#include "event_pb.h"

namespace NActors {

    const TScopeId TScopeId::LocallyGenerated{
        Max<ui64>(), Max<ui64>()
    };

    TAutoPtr<IEventHandle>& IEventHandle::Forward(TAutoPtr<IEventHandle>& ev, TActorId recipient) {
        if (ev->IsEventLight()) {
            IEventHandleLight::GetLight(ev.Get())->Forward(recipient);
        } else {
            ev = IEventHandleFat::GetFat(ev.Get())->Forward(recipient);
        }
        return ev;
    }

    THolder<IEventHandle>& IEventHandle::Forward(THolder<IEventHandle>& ev, TActorId recipient) {
        if (ev->IsEventLight()) {
            IEventHandleLight::GetLight(ev.Get())->Forward(recipient);
        } else {
            ev = IEventHandleFat::GetFat(ev.Get())->Forward(recipient);
        }
        return ev;
    }

    TString IEventHandle::GetTypeName() const {
        if (IsEventFat()) {
            auto* ev = const_cast<IEventHandleFat*>(static_cast<const IEventHandleFat*>(this));
            return ev->HasEvent() ? TypeName(*(ev->GetBase())) : TypeName(*this);
        } else {
            return TypeName(*this);
        }
    }

    TString IEventHandle::ToString() const {
        if (IsEventFat()) {
            auto* ev = const_cast<IEventHandleFat*>(static_cast<const IEventHandleFat*>(this));
            return ev->HasEvent() ? ev->GetBase()->ToString().data() : "serialized?";
        } else {
            // TODO(xenoxeno):
            return TypeName(*this);
        }
    }

    bool IEventHandle::HasEvent() const {
        if (IsEventLight()) {
            return true;
        } else {
            return IEventHandleFat::GetFat(this)->HasEvent();
        }
    }

    bool IEventHandle::HasBuffer() const {
        if (IsEventLight()) {
            return false;
        } else {
            return IEventHandleFat::GetFat(this)->HasBuffer();
        }
    }

    TActorId IEventHandle::GetForwardOnNondeliveryRecipient() const {
        if (IsEventLight()) {
            return {};
        } else {
            return IEventHandleFat::GetFat(this)->GetForwardOnNondeliveryRecipient();
        }
    }

    size_t IEventHandle::GetSize() const {
        if (IsEventLight()) {
            if (IsEventSerializable()) {
                return IEventHandleLightSerializable::GetLightSerializable(this)->GetSize();
            }
        } else {
            return IEventHandleFat::GetFat(this)->GetSize();
        }
        return 0;
    }

    TIntrusivePtr<TEventSerializedData> IEventHandleFat::ReleaseChainBuffer() {
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

    TIntrusivePtr<TEventSerializedData> IEventHandleFat::GetChainBuffer() {
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

    std::vector<std::vector<IEventFactory*>*> TEventFactories::EventFactories;
}
