#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"

#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/dlq_helpers.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/public/lib/value/value.h>

#include <util/string/cast.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TUntagQueueActor
    : public TActionActor<TUntagQueueActor>
{
public:
    static constexpr bool NeedQueueAttributes() {
        return true;
    }
    static constexpr bool NeedQueueTags() {
        return true;
    }

    TUntagQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::UntagQueue, std::move(cb))
    {
        for (const auto& key : Request().tagkeys()) {
            TagKeys_.push_back(key);
        }
    }

private:
    bool DoValidate() override {
        // TODO(qyryq) Tag validation

        if (!GetQueueName()) {
            MakeError(Response_.MutableUntagQueue(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableUntagQueue()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        NJson::TJsonMap tagsJson;
        TStringStream tagsStr;
        if (QueueTags_.Defined()) {
            for (const auto& k : TagKeys_) {
                QueueTags_->erase(k);
            }
            for (const auto& [k, v] : *QueueTags_) {
                tagsJson[k] = v;
            }
        }
        WriteJson(&tagsStr, &tagsJson);
        Cerr << "XXXXX tagsStr=" << tagsStr.Str() << Endl;

        TExecutorBuilder builder(SelfId(), RequestId_);
        builder
            .User(UserName_)
            .Queue(GetQueueName())
            .TablesFormat(TablesFormat())
            .QueueLeader(QueueLeader_)
            .QueryId(TAG_QUEUE_ID)
            .Counters(QueueCounters_)
            .RetryOnTimeout()
            .Params()
                .Utf8("NAME", GetQueueName())
                .Utf8("USER_NAME", UserName_)
                .Utf8("TAGS", tagsStr.Str());

        builder.Start();
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui32 status = record.GetStatus();
        auto* result = Response_.MutableUntagQueue();
        bool queueExists = true;

        // TODO(qyryq) Handle case when the queue does not exist? SetQueueAttributes doesn't do it in this method.

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            RLOG_SQS_DEBUG("Sending clear attributes cache event for queue [" << UserName_ << "/" << GetQueueName() << "]");
            Send(QueueLeader_, MakeHolder<TSqsEvents::TEvClearQueueAttributesCache>());
        } else {
            RLOG_SQS_ERROR("Tag queue query failed, queue exists: " << queueExists << ", answer: " << record);
            MakeError(result, queueExists ? NErrors::INTERNAL_FAILURE : NErrors::NON_EXISTENT_QUEUE);
        }
        SendReplyAndDie();
    }

    const TUntagQueueRequest& Request() const {
        return SourceSqsRequest_.GetUntagQueue();
    }

private:
    TVector<TString> TagKeys_;
};

IActor* CreateUntagQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TUntagQueueActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
