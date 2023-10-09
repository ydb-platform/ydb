#include "simple_proto.h"

#include <util/generic/cast.h>

#include <typeinfo>

using namespace NBus;

void TSimpleProtocol::Serialize(const TBusMessage* mess, TBuffer& data) {
    Y_ABORT_UNLESS(typeid(TSimpleMessage) == typeid(*mess));
    const TSimpleMessage* typed = static_cast<const TSimpleMessage*>(mess);
    data.Append((const char*)&typed->Payload, 4);
}

TAutoPtr<TBusMessage> TSimpleProtocol::Deserialize(ui16, TArrayRef<const char> payload) {
    if (payload.size() != 4) {
        return nullptr;
    }
    TAutoPtr<TSimpleMessage> r(new TSimpleMessage);
    memcpy(&r->Payload, payload.data(), 4);
    return r.Release();
}
