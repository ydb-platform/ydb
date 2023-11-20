#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"

#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/proto/records.pb.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TDeleteMessageActor
    : public TActionActor<TDeleteMessageActor>
{
public:
    TDeleteMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, bool isBatch, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, isBatch ? EAction::DeleteMessageBatch : EAction::DeleteMessage, std::move(cb))
        , IsBatch_(isBatch)
    {
    }

    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableDeleteMessage(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        if (IsBatch_) {
            if (BatchRequest().EntriesSize() == 0) {
                MakeError(Response_.MutableDeleteMessageBatch(), NErrors::EMPTY_BATCH_REQUEST);
                return false;
            } else if (BatchRequest().EntriesSize() > TLimits::MaxBatchSize) {
                MakeError(Response_.MutableDeleteMessageBatch(), NErrors::TOO_MANY_ENTRIES_IN_BATCH_REQUEST);
                return false;
            }
        }

        return true;
    }

private:
    void AppendEntry(const TDeleteMessageRequest& entry, TDeleteMessageResponse* resp, size_t requestIndexInBatch) {
        try {
            // Validate
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
                shardInfo.Request_ = MakeHolder<TSqsEvents::TEvDeleteMessageBatch>();
                shardInfo.Request_->Shard = receipt.GetShard();
                shardInfo.Request_->RequestId = RequestId_;
            }

            // Add new message to shard request
            if (IsBatch_) {
                shardInfo.RequestToReplyIndexMapping_.push_back(requestIndexInBatch);
            }
            shardInfo.Request_->Messages.emplace_back();
            auto& msgReq = shardInfo.Request_->Messages.back();
            msgReq.Offset = receipt.GetOffset();
            const TInstant lockTimestamp = TInstant::MilliSeconds(receipt.GetLockTimestamp());
            msgReq.LockTimestamp = lockTimestamp;
            if (isFifo) {
                msgReq.MessageGroupId = receipt.GetMessageGroupId();
                msgReq.ReceiveAttemptId = receipt.GetReceiveRequestAttemptId();
            }

            // Calc metrics
            const TDuration processingDuration = TActivationContext::Now() - lockTimestamp;
            this->Send(
                QueueLeader_,
                new TSqsEvents::TEvLocalCounterChanged(
                    TSqsEvents::TEvLocalCounterChanged::ECounterType::ClientMessageProcessingDuration,
                    processingDuration.MilliSeconds()
                )
            );
        } catch (...) {
            RLOG_SQS_WARN("Failed to process receipt handle " << entry.GetReceiptHandle() << ": " << CurrentExceptionMessage());
            MakeError(resp, NErrors::RECEIPT_HANDLE_IS_INVALID);
        }
    }

    void ProcessAnswer(TDeleteMessageResponse* resp, const TSqsEvents::TEvDeleteMessageBatchResponse::TMessageResult& answer) {
        switch (answer.Status) {
            case TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::OK: {
                break;
            }
            case TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::NotFound: {
                // ignore missing handle just like proper SQS does
                break;
            }
            case TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::Failed: {
                MakeError(resp, NErrors::INTERNAL_FAILURE);
                break;
            }
        }
    }

    TError* MutableErrorDesc() override {
        return IsBatch_ ? Response_.MutableDeleteMessageBatch()->MutableError() : Response_.MutableDeleteMessage()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);
        ShardInfo_.resize(Shards_);

        if (IsBatch_) {
            for (size_t i = 0, size = BatchRequest().EntriesSize(); i < size; ++i) {
                const auto& entry = BatchRequest().GetEntries(i);
                auto* response = Response_.MutableDeleteMessageBatch()->AddEntries();
                response->SetId(entry.GetId());
                AppendEntry(entry, response, i);
            }
        } else {
            AppendEntry(Request(), Response_.MutableDeleteMessage(), 0);
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

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TSqsEvents::TEvDeleteMessageBatchResponse, HandleDeleteMessageBatchResponse);
        }
    }

    void HandleDeleteMessageBatchResponse(TSqsEvents::TEvDeleteMessageBatchResponse::TPtr& ev) {
        if (IsBatch_) {
            Y_ABORT_UNLESS(ev->Get()->Shard < Shards_);
            const auto& shardInfo = ShardInfo_[ev->Get()->Shard];
            Y_ABORT_UNLESS(ev->Get()->Statuses.size() == shardInfo.RequestToReplyIndexMapping_.size());
            for (size_t i = 0, size = ev->Get()->Statuses.size(); i < size; ++i) {
                const size_t entryIndex = shardInfo.RequestToReplyIndexMapping_[i];
                Y_ABORT_UNLESS(entryIndex < Response_.GetDeleteMessageBatch().EntriesSize());
                ProcessAnswer(Response_.MutableDeleteMessageBatch()->MutableEntries(entryIndex), ev->Get()->Statuses[i]);
            }
        } else {
            Y_ABORT_UNLESS(RequestsToLeader_ == 1);
            Y_ABORT_UNLESS(ev->Get()->Statuses.size() == 1);
            ProcessAnswer(Response_.MutableDeleteMessage(), ev->Get()->Statuses[0]);
        }

        --RequestsToLeader_;
        if (RequestsToLeader_ == 0) {
            SendReplyAndDie();
        }
    }

    const TDeleteMessageRequest& Request() const {
        return SourceSqsRequest_.GetDeleteMessage();
    }

    const TDeleteMessageBatchRequest& BatchRequest() const {
        return SourceSqsRequest_.GetDeleteMessageBatch();
    }

private:
    const bool IsBatch_;

    struct TShardInfo {
        std::vector<size_t> RequestToReplyIndexMapping_;
        THolder<TSqsEvents::TEvDeleteMessageBatch> Request_; // actual when processing initial request, then nullptr
    };
    size_t RequestsToLeader_ = 0;
    std::vector<TShardInfo> ShardInfo_;
};

IActor* CreateDeleteMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TDeleteMessageActor(sourceSqsRequest, false, std::move(cb));
}

IActor* CreateDeleteMessageBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TDeleteMessageActor(sourceSqsRequest, true, std::move(cb));
}

} // namespace NKikimr::NSQS
