#include "debug_receiver_handler.h"

#include "debug_receiver_proto.h"

#include <util/generic/cast.h>
#include <util/string/printf.h>

void TDebugReceiverHandler::OnError(TAutoPtr<NBus::TBusMessage>, NBus::EMessageStatus status) {
    Cerr << "error " << status << "\n";
}

void TDebugReceiverHandler::OnMessage(NBus::TOnMessageContext& message) {
    TDebugReceiverMessage* typedMessage = VerifyDynamicCast<TDebugReceiverMessage*>(message.GetMessage());
    Cerr << "type=" << typedMessage->GetHeader()->Type
         << " size=" << typedMessage->GetHeader()->Size
         << " flags=" << Sprintf("0x%04x", (int)typedMessage->GetHeader()->FlagsInternal)
         << "\n";

    message.ForgetRequest();
}
