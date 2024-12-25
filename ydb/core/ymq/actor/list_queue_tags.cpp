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
                .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_));

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
        auto* result = Response_.MutableListQueueTags();
        bool queueExists = true;

        // TODO(qyryq) Handle case when the queue does not exist

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            // Cerr << "XXXXX " << val.DumpToString() << Endl;
            const auto tags = val["tags"];
            for (size_t i = 0; i < tags.Size(); ++i) {
                auto* tag = result->AddTags();
                tag->SetKey(TString(tags[i]["Key"]));
                tag->SetValue(TString(tags[i]["Value"]));
            }
            SendReplyAndDie();
            return;
        }

        RLOG_SQS_ERROR("List queue tags query failed, queue exists: " << queueExists << ", answer: " << record);
        MakeError(result, queueExists ? NErrors::INTERNAL_FAILURE : NErrors::NON_EXISTENT_QUEUE);
        SendReplyAndDie();
    }

    const TListQueueTagsRequest& Request() const {
        return SourceSqsRequest_.GetListQueueTags();
    }

};

IActor* CreateListQueueTagsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TListQueueTagsActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
