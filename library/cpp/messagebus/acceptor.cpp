#include "acceptor.h"

#include "key_value_printer.h"
#include "mb_lwtrace.h"
#include "network.h"

#include <util/network/init.h>
#include <util/system/defaults.h>
#include <util/system/error.h>
#include <util/system/yassert.h>

LWTRACE_USING(LWTRACE_MESSAGEBUS_PROVIDER)

using namespace NActor;
using namespace NBus;
using namespace NBus::NPrivate;

TAcceptor::TAcceptor(TBusSessionImpl* session, ui64 acceptorId, SOCKET socket, const TNetAddr& addr)
    : TActor<TAcceptor>(session->Queue->WorkQueue.Get())
    , AcceptorId(acceptorId)
    , Session(session)
    , GranStatus(session->Config.Secret.StatusFlushPeriod)
{
    SetNonBlock(socket, true);

    Channel = Session->ReadEventLoop.Register(socket, this);
    Channel->EnableRead();

    Stats.AcceptorId = acceptorId;
    Stats.Fd = socket;
    Stats.ListenAddr = addr;

    SendStatus(TInstant::Now());
}

void TAcceptor::Act(TDefaultTag) {
    EShutdownState state = ShutdownState.State.Get();

    if (state == SS_SHUTDOWN_COMPLETE) {
        return;
    }

    TInstant now = TInstant::Now();

    if (state == SS_SHUTDOWN_COMMAND) {
        if (!!Channel) {
            Channel->Unregister();
            Channel.Drop();
            Stats.Fd = INVALID_SOCKET;
        }

        SendStatus(now);

        Session->GetDeadAcceptorStatusQueue()->EnqueueAndSchedule(Stats);
        Stats.ResetIncremental();

        ShutdownState.CompleteShutdown();
        return;
    }

    THolder<TOpaqueAddr> addr(new TOpaqueAddr());
    SOCKET acceptedSocket = accept(Channel->GetSocket(), addr->MutableAddr(), addr->LenPtr());

    int acceptErrno = LastSystemError();

    if (acceptedSocket == INVALID_SOCKET) {
        if (LastSystemError() != EWOULDBLOCK) {
            Stats.LastAcceptErrorErrno = acceptErrno;
            Stats.LastAcceptErrorInstant = now;
            ++Stats.AcceptErrorCount;
        }
    } else {
        TSocketHolder s(acceptedSocket);
        try {
            SetKeepAlive(s, true);
            SetNoDelay(s, Session->Config.TcpNoDelay);
            SetSockOptTcpCork(s, Session->Config.TcpCork);
            SetCloseOnExec(s, true);
            SetNonBlock(s, true);
            if (Session->Config.SocketToS >= 0) {
                SetSocketToS(s, addr.Get(), Session->Config.SocketToS);
            }
        } catch (...) {
            // It means that connection was reset just now
            // TODO: do something better
            goto skipAccept;
        }

        {
            TOnAccept onAccept;
            onAccept.s = s.Release();
            onAccept.addr = TNetAddr(addr.Release());
            onAccept.now = now;

            LWPROBE(Accepted, ToString(onAccept.addr));

            Session->GetOnAcceptQueue()->EnqueueAndSchedule(onAccept);

            Stats.LastAcceptSuccessInstant = now;
            ++Stats.AcceptSuccessCount;
        }

    skipAccept:;
    }

    Channel->EnableRead();

    SendStatus(now);
}

void TAcceptor::SendStatus(TInstant now) {
    GranStatus.Listen.Update(Stats, now);
}

void TAcceptor::HandleEvent(SOCKET socket, void* cookie) {
    Y_UNUSED(socket);
    Y_UNUSED(cookie);

    GetActor()->Schedule();
}

void TAcceptor::Shutdown() {
    ShutdownState.ShutdownCommand();
    GetActor()->Schedule();

    ShutdownState.ShutdownComplete.WaitI();
}
