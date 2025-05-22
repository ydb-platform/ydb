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
        auto* result = Response_.MutableListQueueTags();
        if (QueueTags_.Defined()) {
            for (const auto& [k, v] : QueueTags_->GetMapSafe()) {
                auto* tag = result->AddTags();
                tag->SetKey(k);
                tag->SetValue(v.GetStringSafe());
            }
        }
        SendReplyAndDie();
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    const TListQueueTagsRequest& Request() const {
        return SourceSqsRequest_.GetListQueueTags();
    }

};

IActor* CreateListQueueTagsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TListQueueTagsActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
