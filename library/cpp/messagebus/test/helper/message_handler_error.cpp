#include "message_handler_error.h"

#include <util/system/yassert.h>

using namespace NBus;
using namespace NBus::NTest;

void TBusClientHandlerError::OnError(TAutoPtr<TBusMessage>, EMessageStatus status) {
    Y_ABORT("must not be called, status: %s", ToString(status).data());
}

void TBusClientHandlerError::OnReply(TAutoPtr<TBusMessage>, TAutoPtr<TBusMessage>) {
    Y_ABORT("must not be called");
}

void TBusClientHandlerError::OnMessageSentOneWay(TAutoPtr<TBusMessage>) {
    Y_ABORT("must not be called");
}

void TBusServerHandlerError::OnError(TAutoPtr<TBusMessage>, EMessageStatus status) {
    Y_ABORT("must not be called, status: %s", ToString(status).data());
}

void TBusServerHandlerError::OnMessage(TOnMessageContext&) {
    Y_ABORT("must not be called");
}
