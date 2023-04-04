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

class TListDeadLetterSourceQueuesActor
    : public TActionActor<TListDeadLetterSourceQueuesActor>
{
public:
    TListDeadLetterSourceQueuesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::ListDeadLetterSourceQueues, std::move(cb))
    {
    }

    static constexpr bool NeedExistingQueue() {
        return true;
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,      HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        }
    }

    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(MutableErrorDesc(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableListDeadLetterSourceQueues()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        TExecutorBuilder builder(SelfId(), RequestId_);
        builder
            .User(UserName_)
            .Queue(GetQueueName())
            .QueueLeader(QueueLeader_)
            .QueryId(LIST_DEAD_LETTER_SOURCE_QUEUES_ID)
            .Counters(QueueCounters_)
            .RetryOnTimeout()
            .Params()
                .Utf8("USER_NAME", UserName_)
                .Utf8("FOLDERID", FolderId_);

        builder.Start();
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        auto* result = Response_.MutableListDeadLetterSourceQueues();

        if (ev->Get()->IsOk()) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            const TValue queues(val["queues"]);

            for (size_t i = 0; i < queues.Size(); ++i) {
                const TString name((TString(queues[i]["QueueName"])));
                const TString customQueueName((TString(queues[i]["CustomQueueName"])));

                auto* item = result->AddQueues();
                item->SetQueueName(name);
                if (IsCloud()) {
                    item->SetQueueUrl(MakeQueueUrl(TString::Join(name, '/', customQueueName)));
                } else {
                    item->SetQueueUrl(MakeQueueUrl(name));
                }
            }
        } else {
            RLOG_SQS_WARN("Request failed: " << record);
            MakeError(result, NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

    const TListDeadLetterSourceQueuesRequest& Request() const {
        return SourceSqsRequest_.GetListDeadLetterSourceQueues();
    }
};

IActor* CreateListDeadLetterSourceQueuesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TListDeadLetterSourceQueuesActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
