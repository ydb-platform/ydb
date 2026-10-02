#include "events.h"

namespace NKikimr {

void TEvLoad::TEvNbsWrite::FillRecord() {
    if (!Payload.IsEmpty()) {
        const ui32 id = AddPayload(std::move(Payload));
        Record.SetPayloadId(id);
    }
}

TEvLoad::TEvNbsWrite* TEvLoad::TEvNbsWrite::Load(const NActors::TEventSerializedData* data) {
    TEvNbsWrite* ev = TBase::Load(data);
    if (ev->Record.HasPayloadId()) {
        const ui32 id = ev->Record.GetPayloadId();
        if (id < ev->GetPayloadCount()) {
            ev->Payload = ev->GetPayload(id);
        }
    }
    return ev;
}

} // namespace NKikimr
