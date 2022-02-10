#pragma once

#include "remote_client_session_semaphore.h"
#include "session_impl.h"

#include <util/generic/array_ref.h>
#include <util/generic/object_counter.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4250) //  'NBus::NPrivate::TRemoteClientSession' : inherits 'NBus::NPrivate::TBusSessionImpl::NBus::NPrivate::TBusSessionImpl::GetConfig' via dominance
#endif

namespace NBus {
    namespace NPrivate {
        using TRemoteClientSessionPtr = TIntrusivePtr<TRemoteClientSession>;

        class TRemoteClientSession: public TBusClientSession, public TBusSessionImpl {
            friend class TRemoteClientConnection;
            friend class TInvokeOnReply;

        public:
            TObjectCounter<TRemoteClientSession> ObjectCounter;

            TRemoteClientSessionSemaphore ClientRemoteInFlight;
            IBusClientHandler* const ClientHandler;

        public:
            TRemoteClientSession(TBusMessageQueue* queue, TBusProtocol* proto,
                                 IBusClientHandler* handler,
                                 const TBusSessionConfig& config, const TString& name);

            ~TRemoteClientSession() override;

            void OnMessageReceived(TRemoteConnection* c, TVectorSwaps<TBusMessagePtrAndHeader>& newMsg) override;

            EMessageStatus SendMessageImpl(TBusMessage* msg, const TNetAddr* addr, bool wait, bool oneWay);
            EMessageStatus SendMessage(TBusMessage* msg, const TNetAddr* addr = nullptr, bool wait = false) override;
            EMessageStatus SendMessageOneWay(TBusMessage* msg, const TNetAddr* addr = nullptr, bool wait = false) override;

            int GetInFlight() const noexcept override;
            void FillStatus() override;
            void AcquireInFlight(TArrayRef<TBusMessage* const> messages);
            void ReleaseInFlight(TArrayRef<TBusMessage* const> messages);
            void ReleaseInFlightAndCallOnReply(TNonDestroyingAutoPtr<TBusMessage> request, TBusMessagePtrAndHeader& response);

            EMessageStatus GetMessageDestination(TBusMessage* mess, const TNetAddr* addrp, TBusSocketAddr* dest);

            void OpenConnection(const TNetAddr&) override;

            TBusClientConnectionPtr GetConnection(const TNetAddr&) override;
        };

#ifdef _MSC_VER
#pragma warning(pop)
#endif

    }
}
