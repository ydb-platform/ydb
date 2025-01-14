#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"

#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/dlq_helpers.h>
#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/public/lib/value/value.h>

#include <util/string/cast.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TTagQueueActor
    : public TActionActor<TTagQueueActor>
{
public:
    static constexpr bool NeedQueueAttributes() {
        return true;
    }
    static constexpr bool NeedQueueTags() {
        return true;
    }

    TTagQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::TagQueue, std::move(cb))
    {
        auto& map = Tags_.GetMapSafe();
        for (const auto& t : Request().tags()) {
            map.emplace(t.GetKey(), t.GetValue());
        }
    }

private:

    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableTagQueue(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        auto validator = TTagValidator(QueueTags_, Tags_);
        if (!validator.Validate()) {
            MakeError(Response_.MutableTagQueue(), NErrors::INVALID_PARAMETER_VALUE, validator.GetError());
            return false;
        }

        TagsJson_ = validator.GetJson();

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableTagQueue()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);
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
                .Utf8("TAGS", TagsJson_)
                .Utf8("OLD_TAGS", TagsToJson(*QueueTags_));

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
        auto* result = Response_.MutableTagQueue();

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            bool updated = val["updated"];
            if (updated) {
                RLOG_SQS_DEBUG("Sending clear attributes cache event for queue [" << UserName_ << "/" << GetQueueName() << "]");
                Send(QueueLeader_, MakeHolder<TSqsEvents::TEvClearQueueAttributesCache>());
            } else {
                auto message = "Tag queue query failed, conflicting query in parallel";
                RLOG_SQS_ERROR(message << ": " << record);
                MakeError(result, NErrors::INTERNAL_FAILURE, message);
            }
        } else {
            RLOG_SQS_ERROR("Tag queue query failed answer: " << record);
            MakeError(result, NErrors::INTERNAL_FAILURE);
        }
        SendReplyAndDie();
    }

    const TTagQueueRequest& Request() const {
        return SourceSqsRequest_.GetTagQueue();
    }

private:
    NJson::TJsonMap Tags_;
    TString TagsJson_;
};

IActor* CreateTagQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TTagQueueActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
