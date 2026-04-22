#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"

#include <ydb/core/persqueue/public/mlp/mlp.h>
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
    TDeleteMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, bool isBatch, THolder<IReplyCallback> cb, const TString& peername)
        : TActionActor(sourceSqsRequest, isBatch ? EAction::DeleteMessageBatch : EAction::DeleteMessage, std::move(cb), peername)
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

            if (receipt.GetSource() == TReceipt::Table) {
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
            } else if (FeatureFlags_.EnableSQSMigrationCompatibility_) {
                if (IsBatch_) {
                    MLPRequestToReplyIndexMapping_.push_back(requestIndexInBatch);
                }

                MLPRequest_.Messages.emplace_back(static_cast<ui32>(receipt.GetShard()), receipt.GetOffset());
            } else {
                if (IsBatch_) {
                    ProcessAnswer(Response_.MutableDeleteMessageBatch()->MutableEntries(requestIndexInBatch),
                        TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::Failed);
                } else {
                    ProcessAnswer(Response_.MutableDeleteMessage(),
                        TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::Failed);
                }
            }
        } catch (...) {
            RLOG_SQS_WARN("Failed to process receipt handle " << entry.GetReceiptHandle() << ": " << CurrentExceptionMessage());
            MakeError(resp, NErrors::RECEIPT_HANDLE_IS_INVALID);
        }
    }

    void ProcessAnswer(TDeleteMessageResponse* resp, const TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus& status) {
        switch (status) {
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
        }

        if (!MLPRequest_.Messages.empty()) {
            MLPRequest_.DatabasePath = GetDatabaseName();
            MLPRequest_.TopicName = GetTopicName();
            MLPRequest_.Consumer = ConsumerName;

            Register(NPQ::NMLP::CreateCommitter(SelfId(), std::move(MLPRequest_)));

            ++RequestsToLeader_;
        }

        if (!RequestsToLeader_) {
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
            hFunc(NPQ::NMLP::TEvChangeResponse, Handle);
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
                ProcessAnswer(Response_.MutableDeleteMessageBatch()->MutableEntries(entryIndex), ev->Get()->Statuses[i].Status);
            }
        } else {
            Y_ABORT_UNLESS(RequestsToLeader_ == 1);
            Y_ABORT_UNLESS(ev->Get()->Statuses.size() == 1);
            ProcessAnswer(Response_.MutableDeleteMessage(), ev->Get()->Statuses[0].Status);
        }

        --RequestsToLeader_;
        if (RequestsToLeader_ == 0) {
            SendReplyAndDie();
        }
    }

    void Handle(NPQ::NMLP::TEvChangeResponse::TPtr& ev) {
        auto& messages = ev->Get()->Messages;

        auto status = [&](const auto& message) {
            if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
                return TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::Failed;
            }
            return message.Success ?
                  TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::OK
                : TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::Failed;
        };

        if (IsBatch_) {
            Y_ABORT_UNLESS(messages.size() == MLPRequestToReplyIndexMapping_.size());
            for (size_t i = 0, size = messages.size(); i < size; ++i) {
                const size_t entryIndex = MLPRequestToReplyIndexMapping_[i];
                Y_ABORT_UNLESS(entryIndex < Response_.GetDeleteMessageBatch().EntriesSize());

                ProcessAnswer(Response_.MutableDeleteMessageBatch()->MutableEntries(entryIndex), status(messages[i]));
            }
        } else {
            Y_ABORT_UNLESS(RequestsToLeader_ == 1);
            Y_ABORT_UNLESS(ev->Get()->Messages.size() == 1);
            ProcessAnswer(Response_.MutableDeleteMessage(), status(messages[0]));
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
    NPQ::NMLP::TCommitterSettings MLPRequest_;
    std::vector<size_t> MLPRequestToReplyIndexMapping_;
};

IActor* CreateDeleteMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb, const TString& peername) {
    return new TDeleteMessageActor(sourceSqsRequest, false, std::move(cb), peername);
}

IActor* CreateDeleteMessageBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb, const TString& peername) {
    return new TDeleteMessageActor(sourceSqsRequest, true, std::move(cb), peername);
}

} // namespace NKikimr::NSQS
