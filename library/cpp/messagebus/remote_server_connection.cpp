#include "remote_server_connection.h"

#include "mb_lwtrace.h"
#include "remote_server_session.h"

#include <util/generic/cast.h>

LWTRACE_USING(LWTRACE_MESSAGEBUS_PROVIDER)

using namespace NBus;
using namespace NBus::NPrivate;

TRemoteServerConnection::TRemoteServerConnection(TRemoteServerSessionPtr session, ui64 id, TNetAddr addr)
    : TRemoteConnection(session.Get(), id, addr)
{
}

void TRemoteServerConnection::Init(SOCKET socket, TInstant now) {
    WriterData.Status.ConnectTime = now;
    WriterData.Status.Connected = true;

    Y_ABORT_UNLESS(socket != INVALID_SOCKET, "must be a valid socket");

    TSocket readSocket(socket);
    TSocket writeSocket = readSocket;

    // this must not be done in constructor, because if event loop is stopped,
    // this is deleted
    WriterData.SetChannel(Session->WriteEventLoop.Register(writeSocket, this, WriteCookie));
    WriterData.SocketVersion = 1;

    ReaderGetSocketQueue()->EnqueueAndSchedule(TWriterToReaderSocketMessage(readSocket, WriterData.SocketVersion));
}

TRemoteServerSession* TRemoteServerConnection::GetSession() {
    return CheckedCast<TRemoteServerSession*>(Session.Get());
}

void TRemoteServerConnection::HandleEvent(SOCKET socket, void* cookie) {
    Y_UNUSED(socket);
    Y_ASSERT(cookie == ReadCookie || cookie == WriteCookie);
    if (cookie == ReadCookie) {
        GetSession()->ServerOwnedMessages.Wait();
        ScheduleRead();
    } else {
        ScheduleWrite();
    }
}

bool TRemoteServerConnection::NeedInterruptRead() {
    return !GetSession()->ServerOwnedMessages.TryWait();
}

void TRemoteServerConnection::MessageSent(TArrayRef<TBusMessagePtrAndHeader> messages) {
    TInstant now = TInstant::Now();

    GetSession()->ReleaseInWorkResponses(messages);
    for (auto& message : messages) {
        TInstant recvTime = message.MessagePtr->RecvTime;
        GetSession()->ServerHandler->OnSent(message.MessagePtr.Release());
        TDuration d = now - recvTime;
        WriterData.Status.DurationCounter.AddDuration(d);
        WriterData.Status.Incremental.ProcessDurationHistogram.AddTime(d);
    }
}

void TRemoteServerConnection::ReaderProcessMessageUnknownVersion(TArrayRef<const char> dataRef) {
    TBusHeader header(dataRef);
    // TODO: full version hex
    LWPROBE(ServerUnknownVersion, ToString(PeerAddr), header.GetVersionInternal());
    WrongVersionRequests.Enqueue(header);
    GetWriterActor()->Schedule();
}
