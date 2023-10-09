#pragma once
#include "defs.h"
#include "action.h"
#include "actor.h"
#include "error.h"
#include "proxy_actor.h"

namespace NKikimr::NSQS {

class TBatchRequestReplyCallback : public IReplyCallback {
public:
    TBatchRequestReplyCallback(const TActorId& bacthActor, ui64 cookie)
        : BatchActor_(bacthActor)
        , Cookie_(cookie)
    {
    }

private:
    void DoSendReply(const NKikimrClient::TSqsResponse& resp) override {
        const TActorId sender = TActivationContext::AsActorContext().SelfID;
        TActivationContext::Send(new IEventHandle(BatchActor_, sender, new TSqsEvents::TEvSqsResponse(resp), 0, Cookie_));
    }

private:
    const TActorId BatchActor_;
    const ui64 Cookie_;
};

template <class TDerived>
class TCommonBatchActor
    : public TActionActor<TDerived>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    TCommonBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, const EAction action, THolder<IReplyCallback> cb)
        : TActionActor<TDerived>(sourceSqsRequest, action, std::move(cb))
    {
    }

private:
    virtual std::vector<NKikimrClient::TSqsRequest> GenerateRequestsFromBatch() const = 0;
    virtual void OnResponses(std::vector<NKikimrClient::TSqsResponse>&& responses) = 0; // Fills Response_ with merged response

    void DoAction() override {
        RLOG_SQS_TRACE("TCommonBatchActor::DoAction");
        this->Become(&TCommonBatchActor::StateFunc);

        std::vector<NKikimrClient::TSqsRequest> requests = GenerateRequestsFromBatch();
        if (requests.empty()) {
            MakeError(this->MutableErrorDesc(), NErrors::EMPTY_BATCH_REQUEST);
            this->SendReplyAndDie();
            return;
        }
        Responses.resize(requests.size());
        for (ui64 i = 0; i < requests.size(); ++i) {
            TStringBuilder reqId;
            reqId << RequestId_ << "-" << i;
            RLOG_SQS_DEBUG("Create proxy subactor[" << i << "]. Req id: " << reqId);
            requests[i].SetRequestId(reqId);
            requests[i].SetRequestRateLimit(false); // already requested
            this->Register(new TProxyActor(requests[i], MakeHolder<TBatchRequestReplyCallback>(this->SelfId(), i)));
        }
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvSqsResponse, HandleResponse);
        }
    }

    void HandleResponse(TSqsEvents::TEvSqsResponse::TPtr& ev) {
        const ui64 cookie = ev->Cookie;
        Y_ABORT_UNLESS(cookie < Responses.size());
        RLOG_SQS_TRACE("Batch actor got reply from proxy actor[" << cookie << "]: " << ev->Get()->Record);
        Responses[cookie] = std::move(ev->Get()->Record);
        if (++ResponsesReceived == Responses.size()) {
            OnResponses(std::move(Responses));
            this->SendReplyAndDie();
        }
    }

protected:
    using TActionActor<TDerived>::RequestId_;

private:
    std::vector<NKikimrClient::TSqsResponse> Responses;
    size_t ResponsesReceived = 0;
};

} // namespace NKikimr::NSQS
