#include "messagebus_client.h"

using namespace NRainCheck;
using namespace NBus;

TBusClientService::TBusClientService(
    const NBus::TBusSessionConfig& config,
    NBus::TBusProtocol* proto,
    NBus::TBusMessageQueue* queue) {
    Session = queue->CreateSource(proto, this, config);
}

TBusClientService::~TBusClientService() {
    Session->Shutdown();
}

void TBusClientService::SendCommon(NBus::TBusMessage* message, const NBus::TNetAddr&, TBusFuture* future) {
    TTaskRunnerBase* current = TTaskRunnerBase::CurrentTask();

    future->SetRunning(current);

    future->Task = current;

    // after this statement message is owned by both messagebus and future
    future->Request.Reset(message);

    // TODO: allow cookie in messagebus
    message->Data = future;
}

void TBusClientService::ProcessResultCommon(NBus::TBusMessageAutoPtr message,
                                            const NBus::TNetAddr&, TBusFuture* future,
                                            NBus::EMessageStatus status) {
    Y_UNUSED(message.Release());

    if (status == NBus::MESSAGE_OK) {
        return;
    }

    future->SetDoneAndSchedule(status, nullptr);
}

void TBusClientService::SendOneWay(
    NBus::TBusMessageAutoPtr message, const NBus::TNetAddr& addr,
    TBusFuture* future) {
    SendCommon(message.Get(), addr, future);

    EMessageStatus ok = Session->SendMessageOneWay(message.Get(), &addr, false);
    ProcessResultCommon(message, addr, future, ok);
}

NBus::TBusClientSessionPtr TBusClientService::GetSessionForMonitoring() const {
    return Session;
}

void TBusClientService::Send(
    TBusMessageAutoPtr message, const TNetAddr& addr,
    TBusFuture* future) {
    SendCommon(message.Get(), addr, future);

    EMessageStatus ok = Session->SendMessage(message.Get(), &addr, false);
    ProcessResultCommon(message, addr, future, ok);
}

void TBusClientService::OnReply(
    TAutoPtr<TBusMessage> request,
    TAutoPtr<TBusMessage> response) {
    TBusFuture* future = (TBusFuture*)request->Data;
    Y_ASSERT(future->Request.Get() == request.Get());
    Y_UNUSED(request.Release());
    future->SetDoneAndSchedule(MESSAGE_OK, response);
}

void NRainCheck::TBusClientService::OnMessageSentOneWay(
    TAutoPtr<NBus::TBusMessage> request) {
    TBusFuture* future = (TBusFuture*)request->Data;
    Y_ASSERT(future->Request.Get() == request.Get());
    Y_UNUSED(request.Release());
    future->SetDoneAndSchedule(MESSAGE_OK, nullptr);
}

void TBusClientService::OnError(
    TAutoPtr<TBusMessage> message, NBus::EMessageStatus status) {
    if (message->Data == nullptr) {
        return;
    }

    TBusFuture* future = (TBusFuture*)message->Data;
    Y_ASSERT(future->Request.Get() == message.Get());
    Y_UNUSED(message.Release());
    future->SetDoneAndSchedule(status, nullptr);
}

void TBusFuture::SetDoneAndSchedule(EMessageStatus status, TAutoPtr<TBusMessage> response) {
    Status = status;
    Response.Reset(response.Release());
    SetDone();
}
