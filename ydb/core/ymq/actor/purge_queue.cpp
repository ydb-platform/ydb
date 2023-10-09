#include "action.h"
#include "common_batch_actor.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"
#include "serviceid.h"

#include <ydb/core/ymq/queues/common/key_hashes.h>

#include <util/string/join.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TPurgeQueueActor
    : public TActionActor<TPurgeQueueActor>
{
public:
    TPurgeQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::PurgeQueue, std::move(cb))
    {
    }

private:
    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutablePurgeQueue(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutablePurgeQueue()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        TExecutorBuilder(SelfId(), RequestId_)
            .User(UserName_)
            .Queue(GetQueueName())
            .QueueLeader(QueueLeader_)
            .QueryId(SET_RETENTION_ID)
            .Counters(QueueCounters_)
            .RetryOnTimeout()
            .Params()
                .Uint64("QUEUE_ID_NUMBER", QueueVersion_.GetRef())
                .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_.GetRef()))
                .Uint64("NOW", Now().MilliSeconds())
                .Bool("PURGE", true)
            .ParentBuilder().Start();
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,      HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui32 status = record.GetStatus();
        auto* result = Response_.MutablePurgeQueue();

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            const TValue list(val["result"]);

            for (size_t i = 0; i < list.Size(); ++i) {
                auto req = MakeHolder<TSqsEvents::TEvPurgeQueue>();
                req->QueuePath = GetQueuePath();
                req->Boundary = TInstant::MilliSeconds(ui64(list[i]["RetentionBoundary"]));
                if (TablesFormat() == 0) {
                    req->Shard = ui64(list[i]["Shard"]);
                } else {
                    req->Shard = ui32(list[i]["Shard"]);
                }

                RLOG_SQS_INFO("Purging queue. Set retention boundary for queue [" << req->QueuePath << "/" << req->Shard << "] to " << req->Boundary.MilliSeconds() << " (" << req->Boundary << ")");

                Send(QueueLeader_, std::move(req));
            }
        } else {
            RLOG_SQS_ERROR("Failed to set retention boundary for queue [" << GetQueuePath() << "] while purging");
            RLOG_SQS_ERROR("Request failed: " << record);

            MakeError(result, NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

    const TPurgeQueueRequest& Request() const {
        return SourceSqsRequest_.GetPurgeQueue();
    }
};

class TPurgeQueueBatchActor
    : public TCommonBatchActor<TPurgeQueueBatchActor>
{
public:
    TPurgeQueueBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TCommonBatchActor(sourceSqsRequest, EAction::PurgeQueueBatch, std::move(cb))
    {
    }

private:
    std::vector<NKikimrClient::TSqsRequest> GenerateRequestsFromBatch() const override {
        std::vector<NKikimrClient::TSqsRequest> ret;
        ret.resize(Request().EntriesSize());
        for (size_t i = 0; i < Request().EntriesSize(); ++i) {
            const auto& entry = Request().GetEntries(i);
            auto& req = *ret[i].MutablePurgeQueue();
            *req.MutableAuth() = Request().GetAuth();

            if (Request().HasCredentials()) {
                *req.MutableCredentials() = Request().GetCredentials();
            }

            req.SetQueueName(entry.GetQueueName());
            req.SetId(entry.GetId());
        }
        return ret;
    }

    void OnResponses(std::vector<NKikimrClient::TSqsResponse>&& responses) override {
        Y_ABORT_UNLESS(Request().EntriesSize() == responses.size());
        auto& resp = *Response_.MutablePurgeQueueBatch();
        for (size_t i = 0; i < Request().EntriesSize(); ++i) {
            const auto& reqEntry = Request().GetEntries(i);
            auto& respEntry = *resp.AddEntries();
            Y_ABORT_UNLESS(responses[i].HasPurgeQueue());
            respEntry = std::move(*responses[i].MutablePurgeQueue());
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
        return Response_.MutablePurgeQueueBatch()->MutableError();
    }

    TString DoGetQueueName() const override {
        return {};
    }

    const TPurgeQueueBatchRequest& Request() const {
        return SourceSqsRequest_.GetPurgeQueueBatch();
    }
};

IActor* CreatePurgeQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TPurgeQueueActor(sourceSqsRequest, std::move(cb));
}

IActor* CreatePurgeQueueBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TPurgeQueueBatchActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
