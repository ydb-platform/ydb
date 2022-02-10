#pragma once

#include <library/cpp/messagebus/ybus.h>

struct TSimpleMessage: public NBus::TBusMessage {
    ui32 Payload;

    TSimpleMessage()
        : TBusMessage(1)
        , Payload(0)
    {
    }

    TSimpleMessage(NBus::ECreateUninitialized)
        : TBusMessage(NBus::ECreateUninitialized())
    {
    }
};

struct TSimpleProtocol: public NBus::TBusProtocol {
    TSimpleProtocol()
        : NBus::TBusProtocol("simple", 55666)
    {
    }

    void Serialize(const NBus::TBusMessage* mess, TBuffer& data) override;

    TAutoPtr<NBus::TBusMessage> Deserialize(ui16 ty, TArrayRef<const char> payload) override;
};
