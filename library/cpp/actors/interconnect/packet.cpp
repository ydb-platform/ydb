#include "packet.h"

#include <library/cpp/actors/core/probes.h>

#include <util/system/datetime.h>

LWTRACE_USING(ACTORLIB_PROVIDER);

ui32 TEventHolder::Fill(IEventHandle& ev) {
    Serial = 0;
    Descr.Type = ev.Type;
    Descr.Flags = ev.Flags;
    Descr.Recipient = ev.Recipient;
    Descr.Sender = ev.Sender;
    Descr.Cookie = ev.Cookie;
    ForwardRecipient = ev.GetForwardOnNondeliveryRecipient();
    EventActuallySerialized = 0;
    Descr.Checksum = 0;

    if (ev.IsEventLight()) {
        if (ev.IsEventSerializable()) {
            NActors::IEventHandleLightSerializable& serializable(*NActors::IEventHandleLightSerializable::GetLightSerializable(&ev));
            EventSerializer = serializable.Serializer;
            EventSerializedSize = 100;
        } else {
            EventSerializedSize = 0;
        }
    } else {
        auto& evFat = *IEventHandleFat::GetFat(&ev);
        if (evFat.HasBuffer()) {
            Buffer = evFat.ReleaseChainBuffer();
            EventSerializedSize = Buffer->GetSize();
        } else if (evFat.HasEvent()) {
            Event.Reset(evFat.ReleaseBase());
            EventSerializedSize = Event->CalculateSerializedSize();
        } else {
            EventSerializedSize = 0;
        }
    }

    return EventSerializedSize;
}
