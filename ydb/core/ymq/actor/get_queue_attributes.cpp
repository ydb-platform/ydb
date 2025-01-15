#include "action.h"
#include "common_batch_actor.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"
#include "serviceid.h"

#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/dlq_helpers.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/public/lib/value/value.h>

#include <util/string/cast.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

struct TAttributeInfo {
    bool NeedRuntimeAttributes = false;
    bool NeedAttributesTable = false;
    bool NeedArn = false;
    bool FifoOnly = false;
};

static const std::map<TString, TAttributeInfo> AttributesInfo = {
    { "ApproximateNumberOfMessages",           {  true, false, false, false } },
    { "ApproximateNumberOfMessagesDelayed",    {  true, false, false, false } },
    { "ApproximateNumberOfMessagesNotVisible", {  true, false, false, false } },
    { "CreatedTimestamp",                      {  true, false, false, false } },
    { "DelaySeconds",                          { false,  true, false, false } },
    { "MaximumMessageSize",                    { false,  true, false, false } },
    { "MessageRetentionPeriod",                { false,  true, false, false } },
    { "ReceiveMessageWaitTimeSeconds",         { false,  true, false, false } },
    { "RedrivePolicy",                         { false,  true, false, false } },
    { "VisibilityTimeout",                     { false,  true, false, false } },
    { "FifoQueue",                             { false,  true, false,  true } },
    { "ContentBasedDeduplication",             { false,  true, false,  true } },
    { "QueueArn",                              { false, false,  true, false } },
};

class TGetQueueAttributesActor
    : public TActionActor<TGetQueueAttributesActor>
{
public:
    TGetQueueAttributesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::GetQueueAttributes, std::move(cb))
    {
    }

private:
    bool ExpandNames() {
        if (!Request().NamesSize()) {
            return false;
        }

        bool all = false;
        for (const auto& name : Request().names()) {
            if (name == "All") {
                all = true;
            } else {
                const auto info = AttributesInfo.find(name);
                if (info == AttributesInfo.end()) {
                    MakeError(MutableErrorDesc(), NErrors::INVALID_ATTRIBUTE_NAME);
                    return false;
                }
                if (info->second.NeedAttributesTable) {
                    NeedAttributesTable_ = true;
                }
                if (info->second.NeedRuntimeAttributes) {
                    NeedRuntimeAttributes_ = true;
                }
                if (info->second.NeedArn) {
                    NeedArn_ = true;
                }

                AttributesSet_.insert(name);
            }
        }

        if (all) {
            const bool isFifo = IsFifoQueue();
            for (const auto& [name, props] : AttributesInfo) {
                if (!props.FifoOnly || isFifo) {
                    AttributesSet_.insert(name);
                }
            }
            NeedRuntimeAttributes_ = true;
            NeedAttributesTable_ = true;
            NeedArn_ = true;
        }
        return true;
    }

private:
    bool HasAttributeName(const TStringBuf name) const {
        return IsIn(AttributesSet_, name);
    }

    TString MakeQueueArn(const TString& prefix, const TString& region, const TString& account, const TString& queueName) const {
        return Join(":", prefix, region, account, queueName);
    }

    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableGetQueueAttributes(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableGetQueueAttributes()->MutableError();
    }

    void ReplyIfReady() {
        if (WaitCount_ == 0) {
            SendReplyAndDie();
        }
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        if (!ExpandNames()) {
            SendReplyAndDie();
            return;
        }

        if (NeedAttributesTable_) {
            TExecutorBuilder builder(SelfId(), RequestId_);
            builder
                .User(UserName_)
                .Queue(GetQueueName())
                .TablesFormat(TablesFormat_.GetRef())
                .QueueLeader(QueueLeader_)
                .QueryId(INTERNAL_GET_QUEUE_ATTRIBUTES_ID)
                .Counters(QueueCounters_)
                .RetryOnTimeout()
                .Params()
                    .Uint64("QUEUE_ID_NUMBER", QueueVersion_.GetRef())
                    .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
                    .Utf8("NAME", GetQueueName())
                    .Utf8("USER_NAME", UserName_)
                .ParentBuilder().Start();
            ++WaitCount_;
        }

        if (NeedRuntimeAttributes_) {
            Send(QueueLeader_, MakeHolder<TSqsEvents::TEvGetRuntimeQueueAttributes>(RequestId_));
            ++WaitCount_;
        }

        if (NeedArn_) {
            if (IsCloud()) {
                Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvGetQueueFolderIdAndCustomName(RequestId_, UserName_, GetQueueName()));
                ++WaitCount_;
            } else {
                auto* result = Response_.MutableGetQueueAttributes();
                result->SetQueueArn(MakeQueueArn(yaSqsArnPrefix, Cfg().GetYandexCloudServiceRegion(), UserName_, GetQueueName()));
            }
        }

        ReplyIfReady();
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,      HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
            hFunc(TSqsEvents::TEvGetRuntimeQueueAttributesResponse, HandleRuntimeAttributes);
            hFunc(TSqsEvents::TEvQueueFolderIdAndCustomName, HandleQueueFolderIdAndCustomName);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui32 status = record.GetStatus();
        auto* result = Response_.MutableGetQueueAttributes();
        bool queueExists = true;

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            queueExists = val["queueExists"];
            if (queueExists) {
                const TValue& attrs(val["attrs"]);

                if (HasAttributeName("ContentBasedDeduplication")) {
                    result->SetContentBasedDeduplication(bool(attrs["ContentBasedDeduplication"]));
                }
                if (HasAttributeName("DelaySeconds")) {
                    result->SetDelaySeconds(TDuration::MilliSeconds(ui64(attrs["DelaySeconds"])).Seconds());
                }
                if (HasAttributeName("FifoQueue")) {
                    result->SetFifoQueue(bool(attrs["FifoQueue"]));
                }
                if (HasAttributeName("MaximumMessageSize")) {
                    result->SetMaximumMessageSize(ui64(attrs["MaximumMessageSize"]));
                }
                if (HasAttributeName("MessageRetentionPeriod")) {
                    result->SetMessageRetentionPeriod(TDuration::MilliSeconds(ui64(attrs["MessageRetentionPeriod"])).Seconds());
                }
                if (HasAttributeName("ReceiveMessageWaitTimeSeconds")) {
                    result->SetReceiveMessageWaitTimeSeconds(TDuration::MilliSeconds(ui64(attrs["ReceiveMessageWaitTime"])).Seconds());
                }
                if (HasAttributeName("VisibilityTimeout")) {
                    result->SetVisibilityTimeout(TDuration::MilliSeconds(ui64(attrs["VisibilityTimeout"])).Seconds());
                }
                if (HasAttributeName("RedrivePolicy")) {
                    const TValue& dlqArn(attrs["DlqArn"]);
                    if (dlqArn.HaveValue() && !TString(dlqArn).empty()) {
                        // the attributes can't be set separately, so we check only one
                        TRedrivePolicy redrivePolicy;
                        redrivePolicy.TargetArn = TString(dlqArn);
                        redrivePolicy.MaxReceiveCount = ui64(attrs["MaxReceiveCount"]);
                        result->SetRedrivePolicy(redrivePolicy.ToJson());
                    }
                }

                --WaitCount_;
                ReplyIfReady();
                return;
            }
        }

        RLOG_SQS_ERROR("Get queue attributes query failed, queue exists: " << queueExists << ", answer: " << record);
        MakeError(result, queueExists ? NErrors::INTERNAL_FAILURE : NErrors::NON_EXISTENT_QUEUE);
        SendReplyAndDie();
    }

    void HandleRuntimeAttributes(TSqsEvents::TEvGetRuntimeQueueAttributesResponse::TPtr& ev) {
        auto* result = Response_.MutableGetQueueAttributes();

        if (ev->Get()->Failed) {
            RLOG_SQS_ERROR("Get runtime queue attributes failed");
            MakeError(result, NErrors::INTERNAL_FAILURE);
            SendReplyAndDie();
            return;
        }

        if (HasAttributeName("CreatedTimestamp")) {
            result->SetCreatedTimestamp(ev->Get()->CreatedTimestamp.Seconds());
        }
        if (HasAttributeName("ApproximateNumberOfMessages")) {
            result->SetApproximateNumberOfMessages(ev->Get()->MessagesCount);
        }
        if (HasAttributeName("ApproximateNumberOfMessagesNotVisible")) {
            result->SetApproximateNumberOfMessagesNotVisible(ev->Get()->InflyMessagesCount);
        }
        if (HasAttributeName("ApproximateNumberOfMessagesDelayed")) {
            result->SetApproximateNumberOfMessagesDelayed(ev->Get()->MessagesDelayed);
        }

        --WaitCount_;
        ReplyIfReady();
    }

    void HandleQueueFolderIdAndCustomName(TSqsEvents::TEvQueueFolderIdAndCustomName::TPtr& ev) {
        auto* result = Response_.MutableGetQueueAttributes();

        if (ev->Get()->Throttled) {
            RLOG_SQS_DEBUG("Get queue folder id and custom name was throttled.");
            MakeError(result, NErrors::THROTTLING_EXCEPTION);
            SendReplyAndDie();
            return;
        }

        if (ev->Get()->Failed || !ev->Get()->Exists) {
            RLOG_SQS_DEBUG("Get queue folder id and custom name failed. Failed: " << ev->Get()->Failed << ". Exists: " << ev->Get()->Exists);
            MakeError(result, NErrors::INTERNAL_FAILURE);
            SendReplyAndDie();
            return;
        }

        if (NeedArn_) {
            result->SetQueueArn(MakeQueueArn(cloudArnPrefix, Cfg().GetYandexCloudServiceRegion(), ev->Get()->QueueFolderId, ev->Get()->QueueCustomName));
        }

        --WaitCount_;
        ReplyIfReady();
    }

    const TGetQueueAttributesRequest& Request() const {
        return SourceSqsRequest_.GetGetQueueAttributes();
    }

private:
    THashSet<TString> AttributesSet_;
    bool NeedRuntimeAttributes_ = false;
    bool NeedAttributesTable_ = false;
    bool NeedArn_ = false;
    size_t WaitCount_ = 0;
};

class TGetQueueAttributesBatchActor
    : public TCommonBatchActor<TGetQueueAttributesBatchActor>
{
public:
    TGetQueueAttributesBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TCommonBatchActor(sourceSqsRequest, EAction::GetQueueAttributesBatch, std::move(cb))
    {
    }

private:
    std::vector<NKikimrClient::TSqsRequest> GenerateRequestsFromBatch() const override {
        std::vector<NKikimrClient::TSqsRequest> ret;
        ret.resize(Request().EntriesSize());
        for (size_t i = 0; i < Request().EntriesSize(); ++i) {
            const auto& entry = Request().GetEntries(i);
            auto& req = *ret[i].MutableGetQueueAttributes();
            *req.MutableAuth() = Request().GetAuth();

            if (Request().HasCredentials()) {
                *req.MutableCredentials() = Request().GetCredentials();
            }

            req.SetQueueName(entry.GetQueueName());
            req.SetId(entry.GetId());
            *req.MutableNames() = Request().GetNames();
        }
        return ret;
    }

    void OnResponses(std::vector<NKikimrClient::TSqsResponse>&& responses) override {
        Y_ABORT_UNLESS(Request().EntriesSize() == responses.size());
        auto& resp = *Response_.MutableGetQueueAttributesBatch();
        for (size_t i = 0; i < Request().EntriesSize(); ++i) {
            const auto& reqEntry = Request().GetEntries(i);
            auto& respEntry = *resp.AddEntries();
            Y_ABORT_UNLESS(responses[i].HasGetQueueAttributes());
            respEntry = std::move(*responses[i].MutableGetQueueAttributes());
            respEntry.SetId(reqEntry.GetId());
        }
    }

    bool DoValidate() override {
        for (const auto& entry : Request().GetEntries()) {
            if (entry.GetQueueName().empty()) {
                MakeError(MutableErrorDesc(), NErrors::MISSING_PARAMETER, TStringBuilder() << "No QueueName parameter in entry " << entry.GetId() << ".");
                return false;
            }
        }
        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableGetQueueAttributesBatch()->MutableError();
    }

    TString DoGetQueueName() const override {
        return {};
    }

    const TGetQueueAttributesBatchRequest& Request() const {
        return SourceSqsRequest_.GetGetQueueAttributesBatch();
    }
};

IActor* CreateGetQueueAttributesActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TGetQueueAttributesActor(sourceSqsRequest, std::move(cb));
}

IActor* CreateGetQueueAttributesBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TGetQueueAttributesBatchActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
