#pragma once

#include <library/cpp/messagebus/ybus.h>

struct TDebugReceiverHandler: public NBus::IBusServerHandler {
    NBus::TBusServerSession* ServerSession;

    void OnError(TAutoPtr<NBus::TBusMessage> pMessage, NBus::EMessageStatus status) override;
    void OnMessage(NBus::TOnMessageContext& message) override;
};
