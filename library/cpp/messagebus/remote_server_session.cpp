#include "remote_server_session.h"

#include "remote_connection.h"
#include "remote_server_connection.h"

#include <library/cpp/messagebus/actor/temp_tls_vector.h>

#include <util/generic/cast.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

#include <typeinfo>

using namespace NActor;
using namespace NBus;
using namespace NBus::NPrivate;

TRemoteServerSession::TRemoteServerSession(TBusMessageQueue* queue,
                                           TBusProtocol* proto, IBusServerHandler* handler,
                                           const TBusServerSessionConfig& config, const TString& name)
    : TBusSessionImpl(false, queue, proto, handler, config, name)
    , ServerOwnedMessages(config.MaxInFlight, config.MaxInFlightBySize, "ServerOwnedMessages")
    , ServerHandler(handler)
{
    if (config.PerConnectionMaxInFlightBySize > 0) {
        if (config.PerConnectionMaxInFlightBySize < config.MaxMessageSize)
            ythrow yexception()
                << "too low PerConnectionMaxInFlightBySize value";
    }
}

namespace NBus {
    namespace NPrivate {
        class TInvokeOnMessage: public IWorkItem {
        private:
            TRemoteServerSession* RemoteServerSession;
            TBusMessagePtrAndHeader Request;
            TIntrusivePtr<TRemoteServerConnection> Connection;

        public:
            TInvokeOnMessage(TRemoteServerSession* session, TBusMessagePtrAndHeader& request, TIntrusivePtr<TRemoteServerConnection>& connection)
                : RemoteServerSession(session)
            {
                Y_ASSERT(!!connection);
                Connection.Swap(connection);

                Request.Swap(request);
            }

            void DoWork() override {
                THolder<TInvokeOnMessage> holder(this);
                RemoteServerSession->InvokeOnMessage(Request, Connection);
                // TODO: TRemoteServerSessionSemaphore should be enough
                RemoteServerSession->JobCount.Decrement();
            }
        };

    }
}

void TRemoteServerSession::OnMessageReceived(TRemoteConnection* c, TVectorSwaps<TBusMessagePtrAndHeader>& messages) {
    AcquireInWorkRequests(messages);

    bool executeInPool = Config.ExecuteOnMessageInWorkerPool;

    TTempTlsVector< ::IWorkItem*> workQueueTemp;

    if (executeInPool) {
        workQueueTemp.GetVector()->reserve(messages.size());
    }

    for (auto& message : messages) {
        // TODO: incref once
        TIntrusivePtr<TRemoteServerConnection> connection(CheckedCast<TRemoteServerConnection*>(c));
        if (executeInPool) {
            workQueueTemp.GetVector()->push_back(new TInvokeOnMessage(this, message, connection));
        } else {
            InvokeOnMessage(message, connection);
        }
    }

    if (executeInPool) {
        JobCount.Add(workQueueTemp.GetVector()->size());
        Queue->EnqueueWork(*workQueueTemp.GetVector());
    }
}

void TRemoteServerSession::InvokeOnMessage(TBusMessagePtrAndHeader& request, TIntrusivePtr<TRemoteServerConnection>& conn) {
    if (Y_UNLIKELY(AtomicGet(Down))) {
        ReleaseInWorkRequests(*conn.Get(), request.MessagePtr.Get());
        InvokeOnError(request.MessagePtr.Release(), MESSAGE_SHUTDOWN);
    } else {
        TWhatThreadDoesPushPop pp("OnMessage");

        TBusIdentity ident;

        ident.Connection.Swap(conn);
        request.MessagePtr->GetIdentity(ident);

        Y_ASSERT(request.MessagePtr->LocalFlags & MESSAGE_IN_WORK);
        DoSwap(request.MessagePtr->LocalFlags, ident.LocalFlags);

        ident.RecvTime = request.MessagePtr->RecvTime;

#ifndef NDEBUG
        auto& message = *request.MessagePtr;
        ident.SetMessageType(typeid(message));
#endif

        TOnMessageContext context(request.MessagePtr.Release(), ident, this);
        ServerHandler->OnMessage(context);
    }
}

EMessageStatus TRemoteServerSession::ForgetRequest(const TBusIdentity& ident) {
    ReleaseInWork(const_cast<TBusIdentity&>(ident));

    return MESSAGE_OK;
}

EMessageStatus TRemoteServerSession::SendReply(const TBusIdentity& ident, TBusMessage* reply) {
    reply->CheckClean();

    ConvertInWork(const_cast<TBusIdentity&>(ident), reply);

    reply->RecvTime = ident.RecvTime;

    ident.Connection->Send(reply);

    return MESSAGE_OK;
}

int TRemoteServerSession::GetInFlight() const noexcept {
    return ServerOwnedMessages.GetCurrentCount();
}

void TRemoteServerSession::FillStatus() {
    TBusSessionImpl::FillStatus();

    // TODO: weird
    StatusData.Status.InFlightCount = ServerOwnedMessages.GetCurrentCount();
    StatusData.Status.InFlightSize = ServerOwnedMessages.GetCurrentSize();
    StatusData.Status.InputPaused = ServerOwnedMessages.IsLocked();
}

void TRemoteServerSession::AcquireInWorkRequests(TArrayRef<const TBusMessagePtrAndHeader> messages) {
    TAtomicBase size = 0;
    for (auto message = messages.begin(); message != messages.end(); ++message) {
        Y_ASSERT(!(message->MessagePtr->LocalFlags & MESSAGE_IN_WORK));
        message->MessagePtr->LocalFlags |= MESSAGE_IN_WORK;
        size += message->MessagePtr->GetHeader()->Size;
    }

    ServerOwnedMessages.IncrementMultiple(messages.size(), size);
}

void TRemoteServerSession::ReleaseInWorkResponses(TArrayRef<const TBusMessagePtrAndHeader> responses) {
    TAtomicBase size = 0;
    for (auto response = responses.begin(); response != responses.end(); ++response) {
        Y_ASSERT((response->MessagePtr->LocalFlags & MESSAGE_REPLY_IS_BEGING_SENT));
        response->MessagePtr->LocalFlags &= ~MESSAGE_REPLY_IS_BEGING_SENT;
        size += response->MessagePtr->RequestSize;
    }

    ServerOwnedMessages.ReleaseMultiple(responses.size(), size);
}

void TRemoteServerSession::ReleaseInWorkRequests(TRemoteConnection& con, TBusMessage* request) {
    Y_ASSERT((request->LocalFlags & MESSAGE_IN_WORK));
    request->LocalFlags &= ~MESSAGE_IN_WORK;

    const size_t size = request->GetHeader()->Size;

    con.QuotaReturnAside(1, size);
    ServerOwnedMessages.ReleaseMultiple(1, size);
}

void TRemoteServerSession::ReleaseInWork(TBusIdentity& ident) {
    ident.SetInWork(false);
    ident.Connection->QuotaReturnAside(1, ident.Size);

    ServerOwnedMessages.ReleaseMultiple(1, ident.Size);
}

void TRemoteServerSession::ConvertInWork(TBusIdentity& req, TBusMessage* reply) {
    reply->SetIdentity(req);

    req.SetInWork(false);
    Y_ASSERT(!(reply->LocalFlags & MESSAGE_REPLY_IS_BEGING_SENT));
    reply->LocalFlags |= MESSAGE_REPLY_IS_BEGING_SENT;
    reply->RequestSize = req.Size;
}

void TRemoteServerSession::Shutdown() {
    ServerOwnedMessages.Stop();
    TBusSessionImpl::Shutdown();
}

void TRemoteServerSession::PauseInput(bool pause) {
    ServerOwnedMessages.PauseByUsed(pause);
}

unsigned TRemoteServerSession::GetActualListenPort() {
    Y_ABORT_UNLESS(Config.ListenPort > 0, "state check");
    return Config.ListenPort;
}
