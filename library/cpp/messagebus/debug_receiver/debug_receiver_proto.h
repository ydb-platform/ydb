#pragma once

#include <library/cpp/messagebus/ybus.h>

struct TDebugReceiverMessage: public NBus::TBusMessage {
    /// constructor to create messages on sending end
    TDebugReceiverMessage(ui16 type)
        : NBus::TBusMessage(type)
    {
    }

    /// constructor with serialzed data to examine the header
    TDebugReceiverMessage(NBus::ECreateUninitialized)
        : NBus::TBusMessage(NBus::ECreateUninitialized())
    {
    }

    TBuffer Payload;
};

struct TDebugReceiverProtocol: public NBus::TBusProtocol {
    TDebugReceiverProtocol();

    void Serialize(const NBus::TBusMessage* mess, TBuffer& data) override;

    TAutoPtr<NBus::TBusMessage> Deserialize(ui16 messageType, TArrayRef<const char> payload) override;
};
