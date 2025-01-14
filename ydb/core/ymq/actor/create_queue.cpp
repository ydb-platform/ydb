#include "action.h"
#include "error.h"
#include "log.h"
#include "queue_schema.h"

#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/base/queue_id.h>

#include <util/string/join.h>
#include <util/string/type.h>

namespace NKikimr::NSQS {

class TCreateQueueActor
    : public TActionActor<TCreateQueueActor>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    static constexpr bool CreateMissingAccount() {
        return true;
    }

    TCreateQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::CreateQueue, std::move(cb))
    {
    }

protected:
    bool IsFifoQueue() const override {
        const TString& name = Request().GetCustomQueueName() ? Request().GetCustomQueueName() : Request().GetQueueName();
        return AsciiHasSuffixIgnoreCase(name, ".fifo"); // works for cloud too, since the custom name should end with '.fifo'
    }

private:
    bool DoValidate() override {
        auto* result = Response_.MutableCreateQueue();

        if (!IsCloud() && !UserExists_) {
            MakeError(result, NErrors::OPT_IN_REQUIRED, "The specified account does not exist.");
            return false;
        }

        TAttribute fifo;
        for (const auto& attr : Request().attributes()) {
            if (attr.GetName() == "FifoQueue") {
                fifo = attr;
                break;
            }
        }

        if (IsFifoQueue()) {
            if (!fifo.HasName() || !IsTrue(fifo.GetValue())) {
                MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "The FifoQueue attribute should be set to true for FIFO queue.");
                return false;
            }
        } else {
            if (fifo.HasName() && IsTrue(fifo.GetValue())) {
                MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Name of FIFO queue should end with \".fifo\".");
                return false;
            }
        }

        if (!Request().GetQueueName()) {
            MakeError(result, NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        if (!ValidateQueueNameOrUserName(Request().GetQueueName())) {
            MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Invalid queue name.");
            return false;
        }

        if (Request().HasCustomQueueName()) {
            if (IsCloud()) {
                if (!ValidateQueueNameOrUserName(Request().GetCustomQueueName())) {
                    MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Invalid custom queue name.");
                    return false;
                }
            } else {
                if (!Request().GetCustomQueueName().empty()) {
                    MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Custom queue name must be empty or unset.");
                    return false;
                }
            }
        }

        if (Request().HasCreatedTimestamp() && Request().GetCreatedTimestamp() > TActivationContext::AsActorContext().Now().Seconds()) {
            MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Invalid created timestamp.");
            return false;
        }

        if (Request().GetShards() > MAX_SHARDS_COUNT) {
            MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Too many shards.");
            return false;
        }

        if (Request().GetPartitions() > MAX_PARTITIONS_COUNT) {
            MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Too many partitions.");
            return false;
        }

        if (Request().GetEnableAutosplit() && Request().GetSizeToSplit() == 0) {
            MakeError(result, NErrors::INVALID_PARAMETER_VALUE, "Zero SizeToSplit.");
            return false;
        }

        {
            NJson::TJsonMap tags;
            auto& map = tags.GetMapSafe();
            for (const auto& t : Request().GetTags()) {
                map.emplace(t.GetKey(), t.GetValue());
            }
            auto tagValidator = TTagValidator({}, tags);
            if (!tagValidator.Validate()) {
                MakeError(result, NErrors::INVALID_PARAMETER_VALUE, tagValidator.GetError());
                return false;
            }
            TagsJson_ = tagValidator.GetJson();
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableCreateQueue()->MutableError();
    }

    void StartQueueCreation(const TString& queueName, const TString& accountName, const TString& customQueueName) {
        const auto& cfg = Cfg();
        SchemaActor_ = Register(
            new TCreateQueueSchemaActorV2(TQueuePath(cfg.GetRoot(), accountName, queueName),
                                          Request(), SelfId(), RequestId_, customQueueName, FolderId_, IsCloud(),
                                          cfg.GetEnableQueueAttributesValidation(), UserCounters_, QuoterResources_, TagsJson_)
        );
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        if (IsCloud()) {
            if (Request().GetCustomQueueName()) {
                ResourceId_ = Request().GetQueueName();
                StartQueueCreation(Request().GetQueueName(), UserName_, Request().GetCustomQueueName());
            } else {
                Register(new TAtomicCounterActor(SelfId(), Cfg().GetRoot(), RequestId_));
            }
        } else {
            StartQueueCreation(Request().GetQueueName(), UserName_, Request().GetCustomQueueName());
        }
    }

    TString DoGetQueueName() const override {
        return TString();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,          HandleWakeup);
            hFunc(TSqsEvents::TEvAtomicCounterIncrementResult, HandleAtomicCounterIncrement);
            hFunc(TSqsEvents::TEvQueueCreated, HandleQueueCreated);
        }
    }

    void HandleAtomicCounterIncrement(TSqsEvents::TEvAtomicCounterIncrementResult::TPtr& ev) {
        auto event = ev->Get();
        auto* result = Response_.MutableCreateQueue();

        if (event->Success) {
            const ui16 serviceId = Cfg().GetYandexCloudServiceId();
            const TString cloudId = UserName_; // should decode from creds
            ResourceId_ = MakeQueueId(serviceId, event->NewValue, UserName_);
            RLOG_SQS_DEBUG("Created resource id: " << MakeQueueId(serviceId, event->NewValue, UserName_)
                                 << " for service id: " << serviceId
                                 << " unique num: " << event->NewValue
                                 << " account name: " << UserName_);

            StartQueueCreation(ResourceId_, cloudId, Request().GetQueueName());
        } else {
            MakeError(result, NErrors::INTERNAL_FAILURE);
            SendReplyAndDie();
        }
    }

    void HandleQueueCreated(TSqsEvents::TEvQueueCreated::TPtr& ev) {
        SchemaActor_ = TActorId();
        auto event  = ev->Get();
        auto* result = Response_.MutableCreateQueue();

        TStringBuilder errMsg;
        if (!event->Success && ev->Get()->Error) {
            errMsg << "Cannot create queue: " << ev->Get()->Error;
        }
        switch (event->State) {
        case EQueueState::Creating:
            MakeError(result, *ev->Get()->ErrorClass, errMsg);
            break;

        case EQueueState::Active:
            if (event->Success) {
                const TString& name = Request().GetCustomQueueName() ? Request().GetCustomQueueName() : Request().GetQueueName();
                if (IsCloud()) {
                    const auto finalResourceId = event->AlreadyExists ? event->ExistingQueueResourceId : ResourceId_;
                    result->SetQueueName(finalResourceId);
                    result->SetQueueUrl(MakeQueueUrl(TString::Join(finalResourceId, '/', name)));
                } else {
                    result->SetQueueName(name);
                    result->SetQueueUrl(MakeQueueUrl(name));
                }
            } else {
                MakeError(result, *ev->Get()->ErrorClass, errMsg);
            }
            break;

        case EQueueState::Deleting:
            MakeError(result, NErrors::QUEUE_DELETED_RECENTLY, errMsg);
            break;
        }

        SendReplyAndDie();
    }

    void PassAway() override {
        if (SchemaActor_) {
            Send(SchemaActor_, new TEvPoisonPill());
            SchemaActor_ = TActorId();
        }
        TActionActor<TCreateQueueActor>::PassAway();
    }

    const TCreateQueueRequest& Request() const {
        return SourceSqsRequest_.GetCreateQueue();
    }

private:
    TString ResourceId_;
    TActorId SchemaActor_;
    TString TagsJson_;
};

IActor* CreateCreateQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TCreateQueueActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
