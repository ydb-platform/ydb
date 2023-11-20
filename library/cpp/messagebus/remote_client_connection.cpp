#include "remote_client_connection.h"

#include "mb_lwtrace.h"
#include "network.h"
#include "remote_client_session.h"

#include <library/cpp/messagebus/actor/executor.h>
#include <library/cpp/messagebus/actor/temp_tls_vector.h>

#include <util/generic/cast.h>
#include <util/thread/singleton.h>

LWTRACE_USING(LWTRACE_MESSAGEBUS_PROVIDER)

using namespace NActor;
using namespace NBus;
using namespace NBus::NPrivate;

TRemoteClientConnection::TRemoteClientConnection(TRemoteClientSessionPtr session, ui64 id, TNetAddr addr)
    : TRemoteConnection(session.Get(), id, addr)
    , ClientHandler(GetSession()->ClientHandler)
{
    Y_ABORT_UNLESS(addr.GetPort() > 0, "must connect to non-zero port");

    ScheduleWrite();
}

TRemoteClientSession* TRemoteClientConnection::GetSession() {
    return CheckedCast<TRemoteClientSession*>(Session.Get());
}

TBusMessage* TRemoteClientConnection::PopAck(TBusKey id) {
    return AckMessages.Pop(id);
}

SOCKET TRemoteClientConnection::CreateSocket(const TNetAddr& addr) {
    SOCKET handle = socket(addr.Addr()->sa_family, SOCK_STREAM, 0);
    Y_ABORT_UNLESS(handle != INVALID_SOCKET, "failed to create socket: %s", LastSystemErrorText());

    TSocketHolder s(handle);

    SetNonBlock(s, true);
    SetNoDelay(s, Config.TcpNoDelay);
    SetSockOptTcpCork(s, Config.TcpCork);
    SetCloseOnExec(s, true);
    SetKeepAlive(s, true);
    if (Config.SocketRecvBufferSize != 0) {
        SetInputBuffer(s, Config.SocketRecvBufferSize);
    }
    if (Config.SocketSendBufferSize != 0) {
        SetOutputBuffer(s, Config.SocketSendBufferSize);
    }
    if (Config.SocketToS >= 0) {
        SetSocketToS(s, &addr, Config.SocketToS);
    }

    return s.Release();
}

void TRemoteClientConnection::TryConnect() {
    if (AtomicGet(WriterData.Down)) {
        return;
    }
    Y_ABORT_UNLESS(!WriterData.Status.Connected);

    TInstant now = TInstant::Now();

    if (!WriterData.Channel) {
        if ((now - LastConnectAttempt) < TDuration::MilliSeconds(Config.RetryInterval)) {
            DropEnqueuedData(MESSAGE_CONNECT_FAILED, MESSAGE_CONNECT_FAILED);
            return;
        }
        LastConnectAttempt = now;

        TSocket connectSocket(CreateSocket(PeerAddr));
        WriterData.SetChannel(Session->WriteEventLoop.Register(connectSocket, this, WriteCookie));
    }

    if (BeforeSendQueue.IsEmpty() && WriterData.SendQueue.Empty() && !Config.ReconnectWhenIdle) {
        // TryConnect is called from Writer::Act, which is called in cycle
        // from session's ScheduleTimeoutMessages via Cron. This prevent these excessive connects.
        return;
    }

    ++WriterData.Status.ConnectSyscalls;

    int ret = connect(WriterData.Channel->GetSocket(), PeerAddr.Addr(), PeerAddr.Len());
    int err = ret ? LastSystemError() : 0;

    if (!ret || (ret && err == EISCONN)) {
        WriterData.Status.ConnectTime = now;
        ++WriterData.SocketVersion;

        WriterData.Channel->DisableWrite();
        WriterData.Status.Connected = true;
        AtomicSet(ReturnConnectFailedImmediately, false);

        WriterData.Status.MyAddr = TNetAddr(GetSockAddr(WriterData.Channel->GetSocket()));

        TSocket readSocket = WriterData.Channel->GetSocketPtr();

        ReaderGetSocketQueue()->EnqueueAndSchedule(TWriterToReaderSocketMessage(readSocket, WriterData.SocketVersion));

        FireClientConnectionEvent(TClientConnectionEvent::CONNECTED);

        ScheduleWrite();
    } else {
        if (WouldBlock() || err == EALREADY) {
            WriterData.Channel->EnableWrite();
        } else {
            WriterData.DropChannel();
            WriterData.Status.MyAddr = TNetAddr();
            WriterData.Status.Connected = false;
            WriterData.Status.ConnectError = err;

            DropEnqueuedData(MESSAGE_CONNECT_FAILED, MESSAGE_CONNECT_FAILED);
        }
    }
}

void TRemoteClientConnection::HandleEvent(SOCKET socket, void* cookie) {
    Y_UNUSED(socket);
    Y_ASSERT(cookie == WriteCookie || cookie == ReadCookie);
    if (cookie == ReadCookie) {
        ScheduleRead();
    } else {
        ScheduleWrite();
    }
}

void TRemoteClientConnection::WriterFillStatus() {
    TRemoteConnection::WriterFillStatus();
    WriterData.Status.AckMessagesSize = AckMessages.Size();
}

void TRemoteClientConnection::BeforeTryWrite() {
    ProcessReplyQueue();
    TimeoutMessages();
}

namespace NBus {
    namespace NPrivate {
        class TInvokeOnReply: public IWorkItem {
        private:
            TRemoteClientSession* RemoteClientSession;
            TNonDestroyingHolder<TBusMessage> Request;
            TBusMessagePtrAndHeader Response;

        public:
            TInvokeOnReply(TRemoteClientSession* session,
                           TNonDestroyingAutoPtr<TBusMessage> request, TBusMessagePtrAndHeader& response)
                : RemoteClientSession(session)
                , Request(request)
            {
                Response.Swap(response);
            }

            void DoWork() override {
                THolder<TInvokeOnReply> holder(this);
                RemoteClientSession->ReleaseInFlightAndCallOnReply(Request.Release(), Response);
                // TODO: TRemoteClientSessionSemaphore should be enough
                RemoteClientSession->JobCount.Decrement();
            }
        };

    }
}

void TRemoteClientConnection::ProcessReplyQueue() {
    if (AtomicGet(WriterData.Down)) {
        return;
    }

    bool executeInWorkerPool = Session->Config.ExecuteOnReplyInWorkerPool;

    TTempTlsVector<TBusMessagePtrAndHeader, void, TVectorSwaps> replyQueueTemp;
    TTempTlsVector< ::NActor::IWorkItem*> workQueueTemp;

    ReplyQueue.DequeueAllSingleConsumer(replyQueueTemp.GetVector());
    if (executeInWorkerPool) {
        workQueueTemp.GetVector()->reserve(replyQueueTemp.GetVector()->size());
    }

    for (auto& resp : *replyQueueTemp.GetVector()) {
        TBusMessage* req = PopAck(resp.Header.Id);

        if (!req) {
            WriterErrorMessage(resp.MessagePtr.Release(), MESSAGE_UNKNOWN);
            continue;
        }

        if (executeInWorkerPool) {
            workQueueTemp.GetVector()->push_back(new TInvokeOnReply(GetSession(), req, resp));
        } else {
            GetSession()->ReleaseInFlightAndCallOnReply(req, resp);
        }
    }

    if (executeInWorkerPool) {
        Session->JobCount.Add(workQueueTemp.GetVector()->size());
        Session->Queue->EnqueueWork(*workQueueTemp.GetVector());
    }
}

void TRemoteClientConnection::TimeoutMessages() {
    if (!TimeToTimeoutMessages.FetchTask()) {
        return;
    }

    TMessagesPtrs timedOutMessages;

    TInstant sendDeadline;
    TInstant ackDeadline;
    if (IsReturnConnectFailedImmediately()) {
        sendDeadline = TInstant::Max();
        ackDeadline = TInstant::Max();
    } else {
        TInstant now = TInstant::Now();
        sendDeadline = now - TDuration::MilliSeconds(Session->Config.SendTimeout);
        ackDeadline = now - TDuration::MilliSeconds(Session->Config.TotalTimeout);
    }

    {
        TMessagesPtrs temp;
        WriterData.SendQueue.Timeout(sendDeadline, &temp);
        timedOutMessages.insert(timedOutMessages.end(), temp.begin(), temp.end());
    }

    // Ignores message that is being written currently (that is stored
    // in WriteMessage). It is not a big problem, because after written
    // to the network, message will be placed to the AckMessages queue,
    // and timed out on the next iteration of this procedure.

    {
        TMessagesPtrs temp;
        AckMessages.Timeout(ackDeadline, &temp);
        timedOutMessages.insert(timedOutMessages.end(), temp.begin(), temp.end());
    }

    ResetOneWayFlag(timedOutMessages);

    GetSession()->ReleaseInFlight(timedOutMessages);
    WriterErrorMessages(timedOutMessages, MESSAGE_TIMEOUT);
}

void TRemoteClientConnection::ScheduleTimeoutMessages() {
    TimeToTimeoutMessages.AddTask();
    ScheduleWrite();
}

void TRemoteClientConnection::ReaderProcessMessageUnknownVersion(TArrayRef<const char>) {
    LWPROBE(Error, ToString(MESSAGE_INVALID_VERSION), ToString(PeerAddr), "");
    ReaderData.Status.Incremental.StatusCounter[MESSAGE_INVALID_VERSION] += 1;
    // TODO: close connection
    Y_ABORT("unknown message");
}

void TRemoteClientConnection::ClearOutgoingQueue(TMessagesPtrs& result, bool reconnect) {
    Y_ASSERT(result.empty());

    TRemoteConnection::ClearOutgoingQueue(result, reconnect);
    AckMessages.Clear(&result);

    ResetOneWayFlag(result);
    GetSession()->ReleaseInFlight(result);
}

void TRemoteClientConnection::MessageSent(TArrayRef<TBusMessagePtrAndHeader> messages) {
    for (auto& message : messages) {
        bool oneWay = message.LocalFlags & MESSAGE_ONE_WAY_INTERNAL;

        if (oneWay) {
            message.MessagePtr->LocalFlags &= ~MESSAGE_ONE_WAY_INTERNAL;

            TBusMessage* ackMsg = this->PopAck(message.Header.Id);
            if (!ackMsg) {
                // TODO: expired?
            }

            if (ackMsg != message.MessagePtr.Get()) {
                // TODO: non-unique id?
            }

            GetSession()->ReleaseInFlight({message.MessagePtr.Get()});
            ClientHandler->OnMessageSentOneWay(message.MessagePtr.Release());
        } else {
            ClientHandler->OnMessageSent(message.MessagePtr.Get());
            AckMessages.Push(message);
        }
    }
}

EMessageStatus TRemoteClientConnection::SendMessage(TBusMessage* req, bool wait) {
    return SendMessageImpl(req, wait, false);
}

EMessageStatus TRemoteClientConnection::SendMessageOneWay(TBusMessage* req, bool wait) {
    return SendMessageImpl(req, wait, true);
}

EMessageStatus TRemoteClientConnection::SendMessageImpl(TBusMessage* msg, bool wait, bool oneWay) {
    msg->CheckClean();

    if (Session->IsDown()) {
        return MESSAGE_SHUTDOWN;
    }

    if (wait) {
        Y_ABORT_UNLESS(!Session->Queue->GetExecutor()->IsInExecutorThread());
        GetSession()->ClientRemoteInFlight.Wait();
    } else {
        if (!GetSession()->ClientRemoteInFlight.TryWait()) {
            return MESSAGE_BUSY;
        }
    }

    GetSession()->AcquireInFlight({msg});

    EMessageStatus ret = MESSAGE_OK;

    if (oneWay) {
        msg->LocalFlags |= MESSAGE_ONE_WAY_INTERNAL;
    }

    msg->GetHeader()->SendTime = Now();

    if (IsReturnConnectFailedImmediately()) {
        ret = MESSAGE_CONNECT_FAILED;
        goto clean;
    }

    Send(msg);

    return MESSAGE_OK;
clean:
    msg->LocalFlags &= ~MESSAGE_ONE_WAY_INTERNAL;
    GetSession()->ReleaseInFlight({msg});
    return ret;
}

void TRemoteClientConnection::OpenConnection() {
    // TODO
}
