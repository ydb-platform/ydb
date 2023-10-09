#include "remote_client_session.h"
#include "remote_connection.h"
#include "ybus.h"

using namespace NBus;
using namespace NBus::NPrivate;

/////////////////////////////////////////////////////////////////
/// Object that encapsulates all messgae data required for sending
/// a message synchronously and receiving a reply. It includes:
/// 1. ConditionVariable to wait on message reply
/// 2. Lock used by condition variable
/// 3. Message reply
/// 4. Reply status
struct TBusSyncMessageData {
    TCondVar ReplyEvent;
    TMutex ReplyLock;
    TBusMessage* Reply;
    EMessageStatus ReplyStatus;

    TBusSyncMessageData()
        : Reply(nullptr)
        , ReplyStatus(MESSAGE_DONT_ASK)
    {
    }
};

class TSyncHandler: public IBusClientHandler {
public:
    TSyncHandler(bool expectReply = true)
        : ExpectReply(expectReply)
        , Session(nullptr)
    {
    }
    ~TSyncHandler() override {
    }

    void OnReply(TAutoPtr<TBusMessage> pMessage0, TAutoPtr<TBusMessage> pReply0) override {
        TBusMessage* pMessage = pMessage0.Release();
        TBusMessage* pReply = pReply0.Release();

        if (!ExpectReply) { // Maybe need VERIFY, but it will be better to support backward compatibility here.
            return;
        }

        TBusSyncMessageData* data = static_cast<TBusSyncMessageData*>(pMessage->Data);
        SignalResult(data, pReply, MESSAGE_OK);
    }

    void OnError(TAutoPtr<TBusMessage> pMessage0, EMessageStatus status) override {
        TBusMessage* pMessage = pMessage0.Release();
        TBusSyncMessageData* data = static_cast<TBusSyncMessageData*>(pMessage->Data);
        if (!data) {
            return;
        }

        SignalResult(data, /*pReply=*/nullptr, status);
    }

    void OnMessageSent(TBusMessage* pMessage) override {
        Y_UNUSED(pMessage);
        Y_ASSERT(ExpectReply);
    }

    void OnMessageSentOneWay(TAutoPtr<TBusMessage> pMessage) override {
        Y_ASSERT(!ExpectReply);
        TBusSyncMessageData* data = static_cast<TBusSyncMessageData*>(pMessage.Release()->Data);
        SignalResult(data, /*pReply=*/nullptr, MESSAGE_OK);
    }

    void SetSession(TRemoteClientSession* session) {
        if (!ExpectReply) {
            Session = session;
        }
    }

private:
    void SignalResult(TBusSyncMessageData* data, TBusMessage* pReply, EMessageStatus status) const {
        Y_ABORT_UNLESS(data, "Message data is set to NULL.");
        TGuard<TMutex> G(data->ReplyLock);
        data->Reply = pReply;
        data->ReplyStatus = status;
        data->ReplyEvent.Signal();
    }

private:
    // This is weird, because in regular client one-way-ness is selected per call, not per session.
    bool ExpectReply;
    TRemoteClientSession* Session;
};

namespace NBus {
    namespace NPrivate {
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4250) //  'NBus::NPrivate::TRemoteClientSession' : inherits 'NBus::NPrivate::TBusSessionImpl::NBus::NPrivate::TBusSessionImpl::GetConfig' via dominance
#endif

        ///////////////////////////////////////////////////////////////////////////
        class TBusSyncSourceSessionImpl
           : private TSyncHandler
            // TODO: do not extend TRemoteClientSession
            ,
              public TRemoteClientSession {
        private:
            bool NeedReply;

        public:
            TBusSyncSourceSessionImpl(TBusMessageQueue* queue, TBusProtocol* proto, const TBusClientSessionConfig& config, bool needReply, const TString& name)
                : TSyncHandler(needReply)
                , TRemoteClientSession(queue, proto, this, config, name)
                , NeedReply(needReply)
            {
                SetSession(this);
            }

            TBusMessage* SendSyncMessage(TBusMessage* pMessage, EMessageStatus& status, const TNetAddr* addr = nullptr) {
                Y_ABORT_UNLESS(!Queue->GetExecutor()->IsInExecutorThread(),
                         "SendSyncMessage must not be called from executor thread");

                TBusMessage* reply = nullptr;
                THolder<TBusSyncMessageData> data(new TBusSyncMessageData());

                pMessage->Data = data.Get();

                {
                    TGuard<TMutex> G(data->ReplyLock);
                    if (NeedReply) {
                        status = SendMessage(pMessage, addr, false); // probably should be true
                    } else {
                        status = SendMessageOneWay(pMessage, addr);
                    }

                    if (status == MESSAGE_OK) {
                        data->ReplyEvent.Wait(data->ReplyLock);
                        TBusSyncMessageData* rdata = static_cast<TBusSyncMessageData*>(pMessage->Data);
                        Y_ABORT_UNLESS(rdata == data.Get(), "Message data pointer should not be modified.");
                        reply = rdata->Reply;
                        status = rdata->ReplyStatus;
                    }
                }

                // deletion of message and reply is a job of application.
                pMessage->Data = nullptr;

                return reply;
            }
        };

#ifdef _MSC_VER
#pragma warning(pop)
#endif
    }
}

TBusSyncSourceSession::TBusSyncSourceSession(TIntrusivePtr< ::NBus::NPrivate::TBusSyncSourceSessionImpl> session)
    : Session(session)
{
}

TBusSyncSourceSession::~TBusSyncSourceSession() {
    Shutdown();
}

void TBusSyncSourceSession::Shutdown() {
    Session->Shutdown();
}

TBusMessage* TBusSyncSourceSession::SendSyncMessage(TBusMessage* pMessage, EMessageStatus& status, const TNetAddr* addr) {
    return Session->SendSyncMessage(pMessage, status, addr);
}

int TBusSyncSourceSession::RegisterService(const char* hostname, TBusKey start, TBusKey end, EIpVersion ipVersion) {
    return Session->RegisterService(hostname, start, end, ipVersion);
}

int TBusSyncSourceSession::GetInFlight() {
    return Session->GetInFlight();
}

const TBusProtocol* TBusSyncSourceSession::GetProto() const {
    return Session->GetProto();
}

const TBusClientSession* TBusSyncSourceSession::GetBusClientSessionWorkaroundDoNotUse() const {
    return Session.Get();
}

TBusSyncClientSessionPtr TBusMessageQueue::CreateSyncSource(TBusProtocol* proto, const TBusClientSessionConfig& config, bool needReply, const TString& name) {
    TIntrusivePtr<TBusSyncSourceSessionImpl> session = new TBusSyncSourceSessionImpl(this, proto, config, needReply, name);
    Add(session.Get());
    return new TBusSyncSourceSession(session);
}

void TBusMessageQueue::Destroy(TBusSyncClientSessionPtr session) {
    Destroy(session->Session.Get());
    Y_UNUSED(session->Session.Release());
}
