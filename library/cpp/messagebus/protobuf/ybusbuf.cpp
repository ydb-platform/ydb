#include "ybusbuf.h"

#include <library/cpp/messagebus/actor/what_thread_does.h>

#include <google/protobuf/io/coded_stream.h>

using namespace NBus;

TBusBufferProtocol::TBusBufferProtocol(TBusService name, int port)
    : TBusProtocol(name, port)
{
}

TBusBufferProtocol::~TBusBufferProtocol() {
    for (auto& type : Types) {
        delete type;
    }
}

TBusBufferBase* TBusBufferProtocol::FindType(int type) {
    for (unsigned i = 0; i < Types.size(); i++) {
        if (Types[i]->GetHeader()->Type == type) {
            return Types[i];
        }
    }
    return nullptr;
}

bool TBusBufferProtocol::IsRegisteredType(unsigned type) {
    return TypeMask[type >> 5] & (1 << (type & ((1 << 5) - 1)));
}

void TBusBufferProtocol::RegisterType(TAutoPtr<TBusBufferBase> mess) {
    ui32 type = mess->GetHeader()->Type;
    TypeMask[type >> 5] |= 1 << (type & ((1 << 5) - 1));

    Types.push_back(mess.Release());
}

TArrayRef<TBusBufferBase* const> TBusBufferProtocol::GetTypes() const {
    return Types;
}

void TBusBufferProtocol::Serialize(const TBusMessage* mess, TBuffer& data) {
    TWhatThreadDoesPushPop pp("serialize protobuf message");

    const TBusHeader* header = mess->GetHeader();

    if (!IsRegisteredType(header->Type)) {
        Y_ABORT("unknown message type: %d", int(header->Type));
        return;
    }

    // cast the base from real message
    const TBusBufferBase* bmess = CheckedCast<const TBusBufferBase*>(mess);

    unsigned size = bmess->GetRecord()->ByteSize();
    data.Reserve(data.Size() + size);

    char* after = (char*)bmess->GetRecord()->SerializeWithCachedSizesToArray((ui8*)data.Pos());
    Y_ABORT_UNLESS(after - data.Pos() == size);

    data.Advance(size);
}

TAutoPtr<TBusMessage> TBusBufferProtocol::Deserialize(ui16 messageType, TArrayRef<const char> payload) {
    TWhatThreadDoesPushPop pp("deserialize protobuf message");

    TBusBufferBase* messageTemplate = FindType(messageType);
    if (messageTemplate == nullptr) {
        return nullptr;
        //Y_ABORT("unknown message type: %d", unsigned(messageType));
    }

    // clone the base
    TAutoPtr<TBusBufferBase> bmess = messageTemplate->New();

    // Need to override protobuf message size limit
    // NOTE: the payload size has already been checked against session MaxMessageSize
    google::protobuf::io::CodedInputStream input(reinterpret_cast<const ui8*>(payload.data()), payload.size());
    input.SetTotalBytesLimit(payload.size());

    bool ok = bmess->GetRecord()->ParseFromCodedStream(&input) && input.ConsumedEntireMessage();
    if (!ok) {
        return nullptr;
    }
    return bmess.Release();
}
