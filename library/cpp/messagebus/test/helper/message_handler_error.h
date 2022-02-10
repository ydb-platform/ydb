#pragma once

#include <library/cpp/messagebus/ybus.h>

namespace NBus {
    namespace NTest {
        struct TBusClientHandlerError: public IBusClientHandler {
            void OnError(TAutoPtr<TBusMessage> pMessage, EMessageStatus status) override;
            void OnMessageSentOneWay(TAutoPtr<TBusMessage> pMessage) override;
            void OnReply(TAutoPtr<TBusMessage> pMessage, TAutoPtr<TBusMessage> pReply) override;
        };

        struct TBusServerHandlerError: public IBusServerHandler {
            void OnError(TAutoPtr<TBusMessage> pMessage, EMessageStatus status) override;
            void OnMessage(TOnMessageContext& pMessage) override;
        };

    }
}
