#include "http_sender_actor.h"
#include "http_sender.h"

#include <ydb/library/actors/core/events.h>

using namespace NActors;

namespace NYql::NDq {
namespace {
    class THttpSenderActor : public NActors::TActor<THttpSenderActor> {
    public:
        static constexpr char ActorName[] = "YQL_HTTP_SENDER_ACTOR";

        THttpSenderActor(
                TActorId senderId,
                TActorId httpProxyId,
                const THttpSenderRetryPolicy::TPtr& retryPolicy)
            : TActor<THttpSenderActor>(&THttpSenderActor::StateFunc)
            , HttpProxyId(httpProxyId)
            , SenderId(senderId)
            , RetryState(retryPolicy->CreateRetryState())
        { }

    private:
        STRICT_STFUNC(StateFunc,
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(TEvents::TEvPoison, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        )

        void SendRequestToProxy() {
            Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(Request->Duplicate(), Timeout));
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& ev) {
            Request = ev->Get()->Request;
            Timeout = ev->Get()->Timeout;
            Cookie = ev->Cookie;
            SendRequestToProxy();
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
            const auto* res = ev->Get();
            const TString& error = res->GetError();

            auto nextRetryDelay = RetryState->GetNextRetryDelay(res);

            const bool isTerminal = error.empty() || nextRetryDelay.Empty();
            Send(SenderId, new TEvHttpBase::TEvSendResult(ev, RetryCount++, isTerminal), /*flags=*/0, Cookie);

            if (isTerminal) {
                PassAway();
                return;
            }

            Schedule(*nextRetryDelay, new TEvents::TEvWakeup());
        }

        void Handle(TEvents::TEvPoison::TPtr&) {
            PassAway();
        }

        void Handle(TEvents::TEvWakeup::TPtr&) {
            SendRequestToProxy();
        }

    private:
        const TActorId HttpProxyId;
        const TActorId SenderId;
        THttpSenderRetryPolicy::IRetryState::TPtr RetryState;
        ui32 RetryCount = 0;

        NHttp::THttpOutgoingRequestPtr Request;
        TDuration Timeout;
        ui64 Cookie = 0;
    };
} // namespace

NActors::IActor* CreateHttpSenderActor(TActorId senderId, TActorId httpProxyId, const THttpSenderRetryPolicy::TPtr& retryPolicy) {
    return new THttpSenderActor(senderId, httpProxyId, retryPolicy);
}

} // NYql::NDq
