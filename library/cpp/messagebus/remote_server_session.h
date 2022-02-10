#pragma once

#include "remote_server_session_semaphore.h"
#include "session_impl.h"

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4250) //  'NBus::NPrivate::TRemoteClientSession' : inherits 'NBus::NPrivate::TBusSessionImpl::NBus::NPrivate::TBusSessionImpl::GetConfig' via dominance
#endif

namespace NBus {
    namespace NPrivate {
        class TRemoteServerSession: public TBusServerSession, public TBusSessionImpl {
            friend class TRemoteServerConnection;

        private:
            TObjectCounter<TRemoteServerSession> ObjectCounter;

            TRemoteServerSessionSemaphore ServerOwnedMessages;
            IBusServerHandler* const ServerHandler;

        public:
            TRemoteServerSession(TBusMessageQueue* queue, TBusProtocol* proto,
                                 IBusServerHandler* handler,
                                 const TBusSessionConfig& config, const TString& name);

            void OnMessageReceived(TRemoteConnection* c, TVectorSwaps<TBusMessagePtrAndHeader>& newMsg) override;
            void InvokeOnMessage(TBusMessagePtrAndHeader& request, TIntrusivePtr<TRemoteServerConnection>& conn);

            EMessageStatus SendReply(const TBusIdentity& ident, TBusMessage* pRep) override;

            EMessageStatus ForgetRequest(const TBusIdentity& ident) override;

            int GetInFlight() const noexcept override;
            void FillStatus() override;

            void Shutdown() override;

            void PauseInput(bool pause) override;
            unsigned GetActualListenPort() override;

            void AcquireInWorkRequests(TArrayRef<const TBusMessagePtrAndHeader> requests);
            void ReleaseInWorkResponses(TArrayRef<const TBusMessagePtrAndHeader> responses);
            void ReleaseInWorkRequests(TRemoteConnection&, TBusMessage*);
            void ReleaseInWork(TBusIdentity&);
            void ConvertInWork(TBusIdentity& req, TBusMessage* reply);
        };

#ifdef _MSC_VER
#pragma warning(pop)
#endif

    }
}
