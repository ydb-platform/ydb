#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"
#include "serviceid.h"

#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/dlq_helpers.h>
#include <ydb/core/ymq/base/queue_attributes.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/scheme/scheme.h>

#include <util/generic/maybe.h>
#include <util/generic/utility.h>
#include <util/string/cast.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TSetQueueAttributesActor
    : public TActionActor<TSetQueueAttributesActor>
{
public:
    TSetQueueAttributesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::SetQueueAttributes, std::move(cb))
    {
        for (const auto& attr : Request().attributes()) {
            Attributes_[attr.GetName()] = attr.GetValue();
        }
    }

private:
    static TMaybe<ui64> ToMilliSeconds(const TMaybe<ui64>& value) {
        if (value) {
            return TDuration::Seconds(*value).MilliSeconds();
        }
        return Nothing();
    }

    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableSetQueueAttributes(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        const bool clampValues = !Cfg().GetEnableQueueAttributesValidation();
        ValidatedAttributes_ = TQueueAttributes::FromAttributesAndConfig(Attributes_, Cfg(), IsFifoQueue(), clampValues);
        if (!ValidatedAttributes_.Validate()) {
            MakeError(Response_.MutableSetQueueAttributes(), *ValidatedAttributes_.Error, ValidatedAttributes_.ErrorText);
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableSetQueueAttributes()->MutableError();
    }

    void RequestAttributesChange() const {
        TExecutorBuilder builder(SelfId(), RequestId_);
        builder
            .User(UserName_)
            .Queue(GetQueueName())
            .QueueLeader(QueueLeader_)
            .TablesFormat(TablesFormat())
            .QueryId(SET_QUEUE_ATTRIBUTES_ID)
            .Counters(QueueCounters_)
            .RetryOnTimeout();

        builder.Params().Uint64("QUEUE_ID_NUMBER", QueueVersion_.GetRef());
        builder.Params().Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_.GetRef()));

        builder.Params().OptionalUint64("MAX_RECEIVE_COUNT", ValidatedAttributes_.RedrivePolicy.MaxReceiveCount);
        builder.Params().OptionalUtf8("DLQ_TARGET_ARN", ValidatedAttributes_.RedrivePolicy.TargetArn);
        builder.Params().OptionalUtf8("DLQ_TARGET_NAME", ValidatedAttributes_.RedrivePolicy.TargetQueueName);

        builder.Params().OptionalUint64("VISIBILITY", ToMilliSeconds(ValidatedAttributes_.VisibilityTimeout));
        builder.Params().OptionalUint64("DELAY", ToMilliSeconds(ValidatedAttributes_.DelaySeconds));
        builder.Params().OptionalUint64("RETENTION", ToMilliSeconds(ValidatedAttributes_.MessageRetentionPeriod));
        builder.Params().OptionalUint64("WAIT", ToMilliSeconds(ValidatedAttributes_.ReceiveMessageWaitTimeSeconds));
        builder.Params().OptionalUint64("MAX_MESSAGE_SIZE", ValidatedAttributes_.MaximumMessageSize);

        if (IsFifoQueue()) {
            builder.Params().OptionalBool("CONTENT_BASED_DEDUPLICATION", ValidatedAttributes_.ContentBasedDeduplication);
        }

        builder.Params().Utf8("USER_NAME", UserName_);

        builder.Start();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        if (ValidatedAttributes_.HasClampedAttributes()) {
            RLOG_SQS_WARN("Clamped some queue attribute values for account " << UserName_ << " and queue name " << GetQueueName());
        }

        if (ValidatedAttributes_.RedrivePolicy.TargetQueueName && *ValidatedAttributes_.RedrivePolicy.TargetQueueName) {
            Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvGetQueueId(RequestId_, UserName_, *ValidatedAttributes_.RedrivePolicy.TargetQueueName, FolderId_));
        } else {
            RequestAttributesChange();
        }
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,      HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
            hFunc(TSqsEvents::TEvQueueId,  HandleQueueId);
        }
    }

    void HandleQueueId(TSqsEvents::TEvQueueId::TPtr& ev) {
        if (ev->Get()->Failed) {
            RLOG_SQS_WARN("Get queue id failed");
            MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
        } else if (!ev->Get()->Exists) {
            MakeError(MutableErrorDesc(), NErrors::NON_EXISTENT_QUEUE, "Target DLQ does not exist.");
        } else if (ev->Get()->QueueId == GetQueueName()) {
            MakeError(MutableErrorDesc(), NErrors::INVALID_PARAMETER_VALUE, "Using the queue itself as a dead letter queue is not allowed.");
        } else {
           RequestAttributesChange();
           return;
        }

        SendReplyAndDie();
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui32 status = record.GetStatus();
        auto* result = Response_.MutableSetQueueAttributes();

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            // OK
            RLOG_SQS_DEBUG("Sending clear attributes cache event for queue [" << UserName_ << "/" << GetQueueName() << "]");
            Send(QueueLeader_, MakeHolder<TSqsEvents::TEvClearQueueAttributesCache>());
        } else {
            RLOG_SQS_WARN("Request failed: " << record);
            MakeError(result, NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

    const TSetQueueAttributesRequest& Request() const {
        return SourceSqsRequest_.GetSetQueueAttributes();
    }

private:
    THashMap<TString, TString> Attributes_;

    TQueueAttributes ValidatedAttributes_;
};

IActor* CreateSetQueueAttributesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TSetQueueAttributesActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
