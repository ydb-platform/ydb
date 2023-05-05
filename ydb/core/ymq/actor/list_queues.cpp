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

class TListQueuesActor
    : public TActionActor<TListQueuesActor>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    TListQueuesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::ListQueues, std::move(cb))
    {
    }

private:
    TError* MutableErrorDesc() override {
        return Response_.MutableListQueues()->MutableError();
    }

    void DiscoverQueues() {
        TExecutorBuilder(SelfId(), RequestId_)
            .User(UserName_)
            .QueryId(LIST_QUEUES_ID)
            .RetryOnTimeout()
            .Counters(UserCounters_)
            .Params()
                .Utf8("FOLDERID", FolderId_)
                .Utf8("USER_NAME", UserName_)
            .ParentBuilder().Start();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        if (!UserExists_) {
            if (!IsCloud()) {
                MakeError(Response_.MutableListQueues(), NErrors::OPT_IN_REQUIRED, "The specified account does not exist.");
            } // else respond with an empty list for inexistent account
            SendReplyAndDie();
            return;
        }

        DiscoverQueues();
    }

    TString DoGetQueueName() const override {
        return TString();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,      HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        auto* result = Response_.MutableListQueues();

        if (ev->Get()->IsOk()) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            const TValue queues(val["queues"]);
            const TString prefix = Request().GetQueueNamePrefix();

            for (size_t i = 0; i < queues.Size(); ++i) {
                const TString name((TString(queues[i]["QueueName"])));
                const TString customQueueName((TString(queues[i]["CustomQueueName"])));

                if (prefix.empty() || AsciiHasPrefix((IsCloud() ? customQueueName : name), prefix)) {
                    auto* item = result->AddQueues();
                    item->SetQueueName(name);
                    if (IsCloud()) {
                        item->SetQueueUrl(MakeQueueUrl(TString::Join(name, '/', customQueueName)));
                    } else {
                        item->SetQueueUrl(MakeQueueUrl(name));
                    }
                }
            }
        } else {
            RLOG_SQS_WARN("Request failed: " << record);
            MakeError(result, NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

    const TListQueuesRequest& Request() const {
        return SourceSqsRequest_.GetListQueues();
    }
};

IActor* CreateListQueuesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TListQueuesActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
