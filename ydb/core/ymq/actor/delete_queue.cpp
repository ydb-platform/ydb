#include "action.h"
#include "common_batch_actor.h"
#include "error.h"
#include "queue_schema.h"

#include <util/string/join.h>

namespace NKikimr::NSQS {

class TDeleteQueueActor
    : public TActionActor<TDeleteQueueActor>
{
public:
    TDeleteQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::DeleteQueue, std::move(cb))
    {
    }

private:
    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableDeleteQueue(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableDeleteQueue()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        SchemaActor_ = Register(
            new TDeleteQueueSchemaActorV2(
                TQueuePath(Cfg().GetRoot(), UserName_, GetQueueName(), QueueVersion_.GetRef()),
                IsFifo_.GetRef(),
                TablesFormat_.GetRef(),
                SelfId(),
                RequestId_,
                UserCounters_
            )
        );
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,          HandleWakeup);
            hFunc(TSqsEvents::TEvQueueDeleted, HandleQueueDeleted);
        }
    }

    void HandleQueueDeleted(TSqsEvents::TEvQueueDeleted::TPtr& ev) {
        SchemaActor_ = TActorId();
        if (!ev->Get()->Success) {
            MakeError(Response_.MutableDeleteQueue(), NErrors::INTERNAL_FAILURE, ev->Get()->Message);
        }

        SendReplyAndDie();
    }

    void PassAway() override {
        if (SchemaActor_) {
            Send(SchemaActor_, new TEvPoisonPill());
            SchemaActor_ = TActorId();
        }
        TActionActor<TDeleteQueueActor>::PassAway();
    }

    const TDeleteQueueRequest& Request() const {
        return SourceSqsRequest_.GetDeleteQueue();
    }

private:
    TActorId SchemaActor_;
};

class TDeleteQueueBatchActor
    : public TCommonBatchActor<TDeleteQueueBatchActor>
{
public:
    TDeleteQueueBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TCommonBatchActor(sourceSqsRequest, EAction::DeleteQueueBatch, std::move(cb))
    {
    }

private:
    std::vector<NKikimrClient::TSqsRequest> GenerateRequestsFromBatch() const override {
        std::vector<NKikimrClient::TSqsRequest> ret;
        ret.resize(Request().EntriesSize());
        for (size_t i = 0; i < Request().EntriesSize(); ++i) {
            const auto& entry = Request().GetEntries(i);
            auto& req = *ret[i].MutableDeleteQueue();
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
        auto& resp = *Response_.MutableDeleteQueueBatch();
        for (size_t i = 0; i < Request().EntriesSize(); ++i) {
            const auto& reqEntry = Request().GetEntries(i);
            auto& respEntry = *resp.AddEntries();
            Y_ABORT_UNLESS(responses[i].HasDeleteQueue());
            respEntry = std::move(*responses[i].MutableDeleteQueue());
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
        return Response_.MutableDeleteQueueBatch()->MutableError();
    }

    TString DoGetQueueName() const override {
        return {};
    }

    const TDeleteQueueBatchRequest& Request() const {
        return SourceSqsRequest_.GetDeleteQueueBatch();
    }
};

IActor* CreateDeleteQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TDeleteQueueActor(sourceSqsRequest, std::move(cb));
}

IActor* CreateDeleteQueueBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TDeleteQueueBatchActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
