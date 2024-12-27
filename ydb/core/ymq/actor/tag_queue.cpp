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
        for (const auto& tag : Request().tags()) {
            Tags_[tag.GetKey()] = tag.GetValue();
        }
    }

private:
    bool ValidString(const TStringBuf str, const bool key = false) {
        if (str.empty()) {
            MakeError(Response_.MutableTagQueue(), NErrors::INVALID_PARAMETER_VALUE,
                key ? "Tag key must not be empty."
                    : "Tag value must not be empty.");
            return false;
        }

        if (key && !IsAsciiLower(str[0])) {
            MakeError(Response_.MutableTagQueue(), NErrors::INVALID_PARAMETER_VALUE,
                key ? "Tag key must start with a lowercase letter (a-z)."
                    : "Tag value must start with a lowercase letter (a-z).");
            return false;
        }

        constexpr size_t maxSize = 63;
        if (str.size() > maxSize) {
            MakeError(
                Response_.MutableTagQueue(), NErrors::INVALID_PARAMETER_VALUE,
                key ? "Tag key must not be longer than 63 characters."
                    : "Tag value must not be longer than 63 characters.");
            return false;
        }

        for (char c : str) {
            bool ok = IsAsciiLower(c) || IsAsciiDigit(c) || c == '-' || c == '_';
            if (!ok) {
                MakeError(
                    Response_.MutableTagQueue(), NErrors::INVALID_PARAMETER_VALUE,
                    key ? "Tag key can only consist of ASCII lowercase letters, digits, dashes and underscores."
                        : "Tag value can only consist of ASCII lowercase letters, digits, dashes and underscores.");
                return false;
            }
        }

        return true;
    }

    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableTagQueue(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        for (const auto& [k, v] : Tags_) {
            if (!ValidString(k, true) || !ValidString(v)) {
                return false;
            }
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableTagQueue()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        NJson::TJsonMap tagsJson;
        TStringStream tagsStr;
        if (QueueTags_.Defined()) {
            for (const auto& [k, v] : *QueueTags_) {
                tagsJson[k] = v;
            }
        }
        for (const auto& [k, v] : Tags_) {
            tagsJson[k] = v;
        }

        if (QueueTags_->size() > 50) {
            RLOG_SQS_ERROR("Tag queue query failed: Too many tags added for queue: " << GetQueueName());
            auto* result = Response_.MutableTagQueue();
            MakeError(result, NErrors::INVALID_PARAMETER_VALUE);
            SendReplyAndDie();
            return;
        }

        WriteJson(&tagsStr, &tagsJson);

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
        auto* result = Response_.MutableTagQueue();
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

    const TTagQueueRequest& Request() const {
        return SourceSqsRequest_.GetTagQueue();
    }

private:
    THashMap<TString, TString> Tags_;
    THashMap<TString, TString> ValidatedTags_;
};

IActor* CreateTagQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TTagQueueActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
