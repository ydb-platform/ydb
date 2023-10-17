#include "debug_receiver_proto.h"

using namespace NBus;

TDebugReceiverProtocol::TDebugReceiverProtocol()
    : TBusProtocol("debug receiver", 0)
{
}

void TDebugReceiverProtocol::Serialize(const NBus::TBusMessage*, TBuffer&) {
    Y_ABORT("it is receiver only");
}

TAutoPtr<NBus::TBusMessage> TDebugReceiverProtocol::Deserialize(ui16, TArrayRef<const char> payload) {
    THolder<TDebugReceiverMessage> r(new TDebugReceiverMessage(ECreateUninitialized()));

    r->Payload.Append(payload.data(), payload.size());

    return r.Release();
}
