#include "remote_client_session.h"

#include "mb_lwtrace.h"
#include "remote_client_connection.h"

#include <library/cpp/messagebus/scheduler/scheduler.h>

#include <util/generic/cast.h>
#include <util/system/defaults.h>

LWTRACE_USING(LWTRACE_MESSAGEBUS_PROVIDER)

using namespace NBus;
using namespace NBus::NPrivate;

TRemoteClientSession::TRemoteClientSession(TBusMessageQueue* queue,
                                           TBusProtocol* proto, IBusClientHandler* handler,
                                           const TBusClientSessionConfig& config, const TString& name)
    : TBusSessionImpl(true, queue, proto, handler, config, name)
    , ClientRemoteInFlight(config.MaxInFlight, "ClientRemoteInFlight")
    , ClientHandler(handler)
{
}

TRemoteClientSession::~TRemoteClientSession() {
    //Cerr << "~TRemoteClientSession" << Endl;
}

void TRemoteClientSession::OnMessageReceived(TRemoteConnection* c, TVectorSwaps<TBusMessagePtrAndHeader>& newMsg) {
    TAutoPtr<TVectorSwaps<TBusMessagePtrAndHeader>> temp(new TVectorSwaps<TBusMessagePtrAndHeader>);
    temp->swap(newMsg);
    c->ReplyQueue.EnqueueAll(temp);
    c->ScheduleWrite();
}

EMessageStatus TRemoteClientSession::SendMessageImpl(TBusMessage* msg, const TNetAddr* addr, bool wait, bool oneWay) {
    if (Y_UNLIKELY(IsDown())) {
        return MESSAGE_SHUTDOWN;
    }

    TBusSocketAddr resolvedAddr;
    EMessageStatus ret = GetMessageDestination(msg, addr, &resolvedAddr);
    if (ret != MESSAGE_OK) {
        return ret;
    }

    msg->ReplyTo = resolvedAddr;

    TRemoteConnectionPtr c = ((TBusSessionImpl*)this)->GetConnection(resolvedAddr, true);
    Y_ASSERT(!!c);

    return CheckedCast<TRemoteClientConnection*>(c.Get())->SendMessageImpl(msg, wait, oneWay);
}

EMessageStatus TRemoteClientSession::SendMessage(TBusMessage* msg, const TNetAddr* addr, bool wait) {
    return SendMessageImpl(msg, addr, wait, false);
}

EMessageStatus TRemoteClientSession::SendMessageOneWay(TBusMessage* pMes, const TNetAddr* addr, bool wait) {
    return SendMessageImpl(pMes, addr, wait, true);
}

int TRemoteClientSession::GetInFlight() const noexcept {
    return ClientRemoteInFlight.GetCurrent();
}

void TRemoteClientSession::FillStatus() {
    TBusSessionImpl::FillStatus();

    StatusData.Status.InFlightCount = ClientRemoteInFlight.GetCurrent();
    StatusData.Status.InputPaused = false;
}

void TRemoteClientSession::AcquireInFlight(TArrayRef<TBusMessage* const> messages) {
    for (auto message : messages) {
        Y_ASSERT(!(message->LocalFlags & MESSAGE_IN_FLIGHT_ON_CLIENT));
        message->LocalFlags |= MESSAGE_IN_FLIGHT_ON_CLIENT;
    }
    ClientRemoteInFlight.IncrementMultiple(messages.size());
}

void TRemoteClientSession::ReleaseInFlight(TArrayRef<TBusMessage* const> messages) {
    for (auto message : messages) {
        Y_ASSERT(message->LocalFlags & MESSAGE_IN_FLIGHT_ON_CLIENT);
        message->LocalFlags &= ~MESSAGE_IN_FLIGHT_ON_CLIENT;
    }
    ClientRemoteInFlight.ReleaseMultiple(messages.size());
}

void TRemoteClientSession::ReleaseInFlightAndCallOnReply(TNonDestroyingAutoPtr<TBusMessage> request, TBusMessagePtrAndHeader& response) {
    ReleaseInFlight({request.Get()});
    if (Y_UNLIKELY(AtomicGet(Down))) {
        InvokeOnError(request, MESSAGE_SHUTDOWN);
        InvokeOnError(response.MessagePtr.Release(), MESSAGE_SHUTDOWN);

        TRemoteConnectionReaderIncrementalStatus counter;
        LWPROBE(Error, ToString(MESSAGE_SHUTDOWN), "", "");
        counter.StatusCounter[MESSAGE_SHUTDOWN] += 1;
        GetDeadConnectionReaderStatusQueue()->EnqueueAndSchedule(counter);
    } else {
        TWhatThreadDoesPushPop pp("OnReply");
        ClientHandler->OnReply(request, response.MessagePtr.Release());
    }
}

EMessageStatus TRemoteClientSession::GetMessageDestination(TBusMessage* mess, const TNetAddr* addrp, TBusSocketAddr* dest) {
    if (addrp) {
        *dest = *addrp;
    } else {
        TNetAddr tmp;
        EMessageStatus ret = const_cast<TBusProtocol*>(GetProto())->GetDestination(this, mess, GetQueue()->GetLocator(), &tmp);
        if (ret != MESSAGE_OK) {
            return ret;
        }
        *dest = tmp;
    }
    return MESSAGE_OK;
}

void TRemoteClientSession::OpenConnection(const TNetAddr& addr) {
    GetConnection(addr)->OpenConnection();
}

TBusClientConnectionPtr TRemoteClientSession::GetConnection(const TNetAddr& addr) {
    // TODO: GetConnection should not open
    return CheckedCast<TRemoteClientConnection*>(((TBusSessionImpl*)this)->GetConnection(addr, true).Get());
}
