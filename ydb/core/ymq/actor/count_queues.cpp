#include "action.h"
#include "error.h"
#include "log.h"
#include "params.h"
#include "serviceid.h"
#include "executor.h"

#include <ydb/public/lib/value/value.h>

#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/join.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TCountQueuesActor
    : public TActionActor<TCountQueuesActor>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    TCountQueuesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::CountQueues, std::move(cb))
    {
    }

private:
    TError* MutableErrorDesc() override {
        return Response_.MutableCountQueues()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvCountQueues(RequestId_, UserName_, FolderId_));
    }

    TString DoGetQueueName() const override {
        return TString();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TSqsEvents::TEvCountQueuesResponse, HandleCountQueuesResponse);
        }
    }

    void HandleCountQueuesResponse(TSqsEvents::TEvCountQueuesResponse::TPtr& ev) {
        if (ev->Get()->Failed) {
            RLOG_SQS_WARN("Count queues failed");
            MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
        } else {
            auto* result = Response_.MutableCountQueues();
            result->SetCount(ev->Get()->Count);
        }

        SendReplyAndDie();
    }

    const TCountQueuesRequest& Request() const {
        return SourceSqsRequest_.GetCountQueues();
    }
};

IActor* CreateCountQueuesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TCountQueuesActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
