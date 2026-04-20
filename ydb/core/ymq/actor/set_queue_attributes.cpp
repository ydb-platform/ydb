#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"
#include "serviceid.h"

#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/dlq_helpers.h>
#include <ydb/core/ymq/base/queue_attributes.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/scheme/scheme.h>

#include <util/generic/maybe.h>
#include <util/generic/utility.h>
#include <util/string/cast.h>

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
            hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
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
            if (IsTopicCreated()) {
                return UpdateTopic();
            } else {
                NotifyQueueLeader();
            }
        } else {
            RLOG_SQS_WARN("Request failed: " << record);
            MakeError(result, NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

    const TSetQueueAttributesRequest& Request() const {
        return SourceSqsRequest_.GetSetQueueAttributes();
    }

    void UpdateTopic() {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(GetTopicName());
        if (ValidatedAttributes_.MessageRetentionPeriod) {
            request.mutable_set_retention_period()->set_seconds(*ValidatedAttributes_.MessageRetentionPeriod);
        }

        auto* consumer = request.add_alter_consumers();
        consumer->set_name(ConsumerName);

        auto* type = consumer->mutable_alter_shared_consumer_type();
        if (ValidatedAttributes_.VisibilityTimeout) {
            type->mutable_set_default_processing_timeout()->set_seconds(*ValidatedAttributes_.VisibilityTimeout);
        }
        if (ValidatedAttributes_.DelaySeconds) {
            type->mutable_set_receive_message_delay()->set_seconds(*ValidatedAttributes_.DelaySeconds);
        }
        if (ValidatedAttributes_.ReceiveMessageWaitTimeSeconds) {
            type->mutable_set_receive_message_wait_time()->set_seconds(*ValidatedAttributes_.ReceiveMessageWaitTimeSeconds);
        }

        auto* policy = type->mutable_alter_dead_letter_policy();
        if (ValidatedAttributes_.RedrivePolicy.MaxReceiveCount) {
            policy->set_set_enabled(true);
            policy->mutable_alter_condition()->set_set_max_processing_attempts(*ValidatedAttributes_.RedrivePolicy.MaxReceiveCount);
        }
        if (ValidatedAttributes_.RedrivePolicy.TargetQueueName) {
            policy->set_set_enabled(true);
            policy->mutable_set_move_action()->set_dead_letter_queue(TStringBuilder() << "sqs://" << UserName_ << "/" << FolderId_ << "/" << *ValidatedAttributes_.RedrivePolicy.TargetQueueName);
        }

        RLOG_SQS_DEBUG("Alter topic request: " << request.ShortDebugString());
        Register(NPQ::NSchema::CreateAlterTopicActor(SelfId(), {
            .Database = GetDatabaseName(),
            .Request = std::move(request),
        }));
    }

    void NotifyQueueLeader() {
        RLOG_SQS_DEBUG("Sending clear attributes cache event for queue [" << UserName_ << "/" << GetQueueName() << "]");
        Send(QueueLeader_, MakeHolder<TSqsEvents::TEvClearQueueAttributesCache>());
    }

    void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        if (response.Status == Ydb::StatusIds::SUCCESS) {
            NotifyQueueLeader();
        } else {
            RLOG_SQS_WARN("Failed to alter topic: " << response.ErrorMessage);
            MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

private:
    THashMap<TString, TString> Attributes_;

    TQueueAttributes ValidatedAttributes_;
};

IActor* CreateSetQueueAttributesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TSetQueueAttributesActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
