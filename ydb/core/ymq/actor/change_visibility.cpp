#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"

#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/proto/records.pb.h>

#include <library/cpp/string_utils/base64/base64.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TChangeMessageVisibilityActor
    : public TActionActor<TChangeMessageVisibilityActor>
{
public:
    TChangeMessageVisibilityActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, bool isBatch, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, isBatch ? EAction::ChangeMessageVisibilityBatch : EAction::ChangeMessageVisibility, std::move(cb))
        , IsBatch_(isBatch)
    {
    }

protected:
    void AppendEntry(const TChangeMessageVisibilityRequest& entry, TChangeMessageVisibilityResponse* resp, size_t requestIndexInBatch) {
        try {
            // Validate
            if (!entry.HasVisibilityTimeout()) {
                MakeError(resp, NErrors::MISSING_PARAMETER, "VisibilityTimeout was not provided.");
                return;
            }

            const TDuration newVisibilityTimeout = TDuration::Seconds(entry.GetVisibilityTimeout());
            if (newVisibilityTimeout > TLimits::MaxVisibilityTimeout) {
                MakeError(resp, NErrors::INVALID_PARAMETER_VALUE, "VisibilityTimeout parameter must be less than or equal to 12 hours.");
                return;
            }

            const TReceipt receipt = DecodeReceiptHandle(entry.GetReceiptHandle()); // can throw
            RLOG_SQS_DEBUG("Decoded receipt handle: " << receipt);
            if (receipt.GetShard() >= Shards_) {
                throw yexception() << "Invalid shard: " << receipt.GetShard();
            }

            const bool isFifo = IsFifoQueue();
            if (isFifo && !receipt.GetMessageGroupId()) {
                throw yexception() << "No message group id";
            }

            auto& shardInfo = ShardInfo_[receipt.GetShard()];
            // Create request
            if (!shardInfo.Request_) {
                ++RequestsToLeader_;
                shardInfo.Request_ = MakeHolder<TSqsEvents::TEvChangeMessageVisibilityBatch>();
                shardInfo.Request_->Shard = receipt.GetShard();
                shardInfo.Request_->RequestId = RequestId_;
                shardInfo.Request_->NowTimestamp = NowTimestamp_;
            }

            // Add new message to shard request
            if (IsBatch_) {
                shardInfo.RequestToReplyIndexMapping_.push_back(requestIndexInBatch);
            }
            shardInfo.Request_->Messages.emplace_back();
            auto& msgReq = shardInfo.Request_->Messages.back();
            msgReq.Offset = receipt.GetOffset();
            msgReq.LockTimestamp = TInstant::MilliSeconds(receipt.GetLockTimestamp());
            if (isFifo) {
                msgReq.MessageGroupId = receipt.GetMessageGroupId();
                msgReq.ReceiveAttemptId = receipt.GetReceiveRequestAttemptId();
            }

            msgReq.VisibilityDeadline = NowTimestamp_ + newVisibilityTimeout;
        } catch (...) {
            RLOG_SQS_WARN("Failed to process receipt handle " << entry.GetReceiptHandle() << ": " << CurrentExceptionMessage());
            MakeError(resp, NErrors::RECEIPT_HANDLE_IS_INVALID);
        }
    }

    void ProcessAnswer(TChangeMessageVisibilityResponse* resp, const TSqsEvents::TEvChangeMessageVisibilityBatchResponse::TMessageResult& answer) {
        switch (answer.Status) {
        case TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::OK: {
            break;
        }
        case TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::NotFound: {
            MakeError(resp, NErrors::INVALID_PARAMETER_VALUE, "No such message.");
            break;
        }
        case TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::NotInFly: {
            MakeError(resp, NErrors::MESSAGE_NOT_INFLIGHT);
            break;
        }
        case TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::Failed: {
            MakeError(resp, NErrors::INTERNAL_FAILURE);
            break;
        }
        }
    }

    bool DoValidate() override {
        if (IsBatch_) {
            if (BatchRequest().EntriesSize() == 0) {
                MakeError(Response_.MutableChangeMessageVisibilityBatch(), NErrors::EMPTY_BATCH_REQUEST);
                return false;
            } else if (BatchRequest().EntriesSize() > TLimits::MaxBatchSize) {
                MakeError(Response_.MutableChangeMessageVisibilityBatch(), NErrors::TOO_MANY_ENTRIES_IN_BATCH_REQUEST);
                return false;
            }
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return IsBatch_ ? Response_.MutableChangeMessageVisibilityBatch()->MutableError() : Response_.MutableChangeMessageVisibility()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);
        ShardInfo_.resize(Shards_);
        NowTimestamp_ = TActivationContext::Now();

        if (IsBatch_) {
            for (size_t i = 0, size = BatchRequest().EntriesSize(); i < size; ++i) {
                const auto& entry = BatchRequest().GetEntries(i);
                auto* response = Response_.MutableChangeMessageVisibilityBatch()->AddEntries();
                response->SetId(entry.GetId());
                AppendEntry(entry, response, i);
            }
        } else {
            AppendEntry(Request(), Response_.MutableChangeMessageVisibility(), 0);
        }

        if (RequestsToLeader_) {
            Y_ABORT_UNLESS(RequestsToLeader_ <= Shards_);
            for (auto& shardInfo : ShardInfo_) {
                if (shardInfo.Request_) {
                    Send(QueueLeader_, shardInfo.Request_.Release());
                }
            }
        } else {
            SendReplyAndDie();
        }
    }

    TString DoGetQueueName() const override {
        return IsBatch_ ? BatchRequest().GetQueueName() : Request().GetQueueName();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TSqsEvents::TEvChangeMessageVisibilityBatchResponse, HandleChangeMessageVisibilityBatchResponse);
        }
    }

    void HandleChangeMessageVisibilityBatchResponse(TSqsEvents::TEvChangeMessageVisibilityBatchResponse::TPtr& ev) {
        if (IsBatch_) {
            Y_ABORT_UNLESS(ev->Get()->Shard < Shards_);
            const auto& shardInfo = ShardInfo_[ev->Get()->Shard];
            Y_ABORT_UNLESS(ev->Get()->Statuses.size() == shardInfo.RequestToReplyIndexMapping_.size());
            for (size_t i = 0, size = ev->Get()->Statuses.size(); i < size; ++i) {
                const size_t entryIndex = shardInfo.RequestToReplyIndexMapping_[i];
                Y_ABORT_UNLESS(entryIndex < Response_.GetChangeMessageVisibilityBatch().EntriesSize());
                ProcessAnswer(Response_.MutableChangeMessageVisibilityBatch()->MutableEntries(entryIndex), ev->Get()->Statuses[i]);
            }
        } else {
            Y_ABORT_UNLESS(RequestsToLeader_ == 1);
            Y_ABORT_UNLESS(ev->Get()->Statuses.size() == 1);
            ProcessAnswer(Response_.MutableChangeMessageVisibility(), ev->Get()->Statuses[0]);
        }

        --RequestsToLeader_;
        if (RequestsToLeader_ == 0) {
            SendReplyAndDie();
        }
    }

    const TChangeMessageVisibilityRequest& Request() const {
        return SourceSqsRequest_.GetChangeMessageVisibility();
    }

    const TChangeMessageVisibilityBatchRequest& BatchRequest() const {
        return SourceSqsRequest_.GetChangeMessageVisibilityBatch();
    }

private:
    const bool IsBatch_;

    struct TShardInfo {
        std::vector<size_t> RequestToReplyIndexMapping_;
        THolder<TSqsEvents::TEvChangeMessageVisibilityBatch> Request_; // actual when processing initial request, then nullptr
    };
    size_t RequestsToLeader_ = 0;
    std::vector<TShardInfo> ShardInfo_;
    TInstant NowTimestamp_;
};

IActor* CreateChangeMessageVisibilityActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TChangeMessageVisibilityActor(sourceSqsRequest, false, std::move(cb));
}

IActor* CreateChangeMessageVisibilityBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TChangeMessageVisibilityActor(sourceSqsRequest, true, std::move(cb));
}

} // namespace NKikimr::NSQS
