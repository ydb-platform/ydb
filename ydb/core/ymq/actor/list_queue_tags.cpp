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

class TListQueueTagsActor
    : public TActionActor<TListQueueTagsActor>
{
public:
    TListQueueTagsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::ListQueueTags, std::move(cb))
    {
    }

private:
    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableListQueueTags(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableListQueueTags()->MutableError();
    }

    void ReplyIfReady() {
        if (WaitCount_ == 0) {
            SendReplyAndDie();
        }
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        TExecutorBuilder builder(SelfId(), RequestId_);
        builder
            .User(UserName_)
            .Queue(GetQueueName())
            .TablesFormat(TablesFormat_.GetRef())
            .QueueLeader(QueueLeader_)
            .QueryId(INTERNAL_LIST_QUEUE_TAGS_ID)
            .Counters(QueueCounters_)
            .RetryOnTimeout()
            .Params()
                .Uint64("QUEUE_ID_NUMBER", QueueVersion_.GetRef())
                .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
            .ParentBuilder().Start();
        ++WaitCount_;

        ReplyIfReady();
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
            hFunc(TSqsEvents::TEvQueueFolderIdAndCustomName, HandleQueueFolderIdAndCustomName);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui32 status = record.GetStatus();
        auto* result = Response_.MutableListQueueTags();
        bool queueExists = true;

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            // const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            // Cerr << "XXXXX " << val.DumpToString() << Endl;
            --WaitCount_;
            ReplyIfReady();
            return;
        }

        RLOG_SQS_ERROR("Get queue tags query failed, queue exists: " << queueExists << ", answer: " << record);
        MakeError(result, queueExists ? NErrors::INTERNAL_FAILURE : NErrors::NON_EXISTENT_QUEUE);
        SendReplyAndDie();
    }

    void HandleQueueFolderIdAndCustomName(TSqsEvents::TEvQueueFolderIdAndCustomName::TPtr& ev) {
        auto* result = Response_.MutableListQueueTags();

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

        --WaitCount_;
        ReplyIfReady();
    }

    const TListQueueTagsRequest& Request() const {
        return SourceSqsRequest_.GetListQueueTags();
    }

private:
    size_t WaitCount_ = 0;
};

IActor* CreateListQueueTagsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TListQueueTagsActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
