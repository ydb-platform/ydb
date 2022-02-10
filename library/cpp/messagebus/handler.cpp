#include "handler.h"

#include "remote_server_connection.h"
#include "ybus.h"

using namespace NBus;
using namespace NBus::NPrivate;

void IBusErrorHandler::OnError(TAutoPtr<TBusMessage> pMessage, EMessageStatus status) {
    Y_UNUSED(pMessage);
    Y_UNUSED(status);
}
void IBusServerHandler::OnSent(TAutoPtr<TBusMessage> pMessage) {
    Y_UNUSED(pMessage);
}
void IBusClientHandler::OnMessageSent(TBusMessage* pMessage) {
    Y_UNUSED(pMessage);
}
void IBusClientHandler::OnMessageSentOneWay(TAutoPtr<TBusMessage> pMessage) {
    Y_UNUSED(pMessage);
}

void IBusClientHandler::OnClientConnectionEvent(const TClientConnectionEvent&) {
}

void TOnMessageContext::ForgetRequest() {
    Session->ForgetRequest(Ident);
}

TNetAddr TOnMessageContext::GetPeerAddrNetAddr() const {
    return Ident.GetNetAddr();
}

bool TOnMessageContext::IsConnectionAlive() const {
    return !!Ident.Connection && Ident.Connection->IsAlive();
}
