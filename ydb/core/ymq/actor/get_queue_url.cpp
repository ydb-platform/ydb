#include "action.h"
#include "error.h"
#include "log.h"
#include "params.h"
#include "serviceid.h"

#include <ydb/public/lib/value/value.h>
#include <ydb/core/ymq/actor/executor.h>

#include <util/string/join.h>
#include <util/string/printf.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TGetQueueUrlActor
    : public TActionActor<TGetQueueUrlActor>
{
public:
    TGetQueueUrlActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::GetQueueUrl, std::move(cb))
    {
    }

    static constexpr bool NeedExistingQueue() {
        return false;
    }

private:
    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(MutableErrorDesc(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableGetQueueUrl()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);
        Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvGetQueueId(RequestId_, UserName_, Request().GetQueueName(), FolderId_));
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,      HandleWakeup);
            hFunc(TSqsEvents::TEvQueueId,  HandleQueueId);
        }
    }

    void HandleQueueId(TSqsEvents::TEvQueueId::TPtr& ev) {
        if (ev->Get()->Throttled) {
            RLOG_SQS_WARN("Get queue id was throttled");
            MakeError(MutableErrorDesc(), NErrors::THROTTLING_EXCEPTION);
        } else if (ev->Get()->Failed) {
            RLOG_SQS_WARN("Get queue id failed");
            MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
        } else {
            if (ev->Get()->Exists) {
                auto* result = Response_.MutableGetQueueUrl();
                if (IsCloud()) {
                    result->SetQueueUrl(MakeQueueUrl(TString::Join(ev->Get()->QueueId, "/", GetQueueName())));
                } else {
                    result->SetQueueUrl(MakeQueueUrl(ev->Get()->QueueId));
                }
            } else {
                MakeError(MutableErrorDesc(), NErrors::NON_EXISTENT_QUEUE);
            }
        }

        SendReplyAndDie();
    }

    const TGetQueueUrlRequest& Request() const {
        return SourceSqsRequest_.GetGetQueueUrl();
    }
};

IActor* CreateGetQueueUrlActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TGetQueueUrlActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
