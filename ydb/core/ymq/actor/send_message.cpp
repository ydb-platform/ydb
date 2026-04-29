#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"
#include "serviceid.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>

#include <ydb/core/ymq/attributes/attributes.h>
#include <ydb/core/ymq/attributes/attributes_md5.h>
#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/utils.h>
#include <ydb/core/ymq/actor/sha256.h>
#include <ydb/core/ymq/proto/records.pb.h>

#include <ydb/public/lib/value/value.h>

#include <library/cpp/digest/md5/md5.h>
#include <util/random/random.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TSendMessageActor
    : public TActionActor<TSendMessageActor>
{
public:
    static constexpr bool NeedQueueAttributes() {
        return true;
    }

    TSendMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, bool isBatch, THolder<IReplyCallback> cb, const TString& peername)
        : TActionActor(sourceSqsRequest, isBatch ? EAction::SendMessageBatch : EAction::SendMessage, std::move(cb), peername)
        , IsBatch_(isBatch)
    {
    }

private:
    size_t CalculateMessageSize(const TSendMessageRequest& req) const {
        size_t ret = 0;
        for (const auto& a : req.GetMessageAttributes()) {
            ret += a.ByteSize();
        }
        ret += req.GetMessageBody().size();
        return ret;
    }

    TDuration GetDelay(const TSendMessageRequest& request) const {
        if (request.HasDelaySeconds()) {
            return TDuration::Seconds(request.GetDelaySeconds());
        } else {
            return QueueAttributes_->DelaySeconds;
        }
    }

    bool ValidateSingleRequest(const TSendMessageRequest& req, TSendMessageResponse* resp) {
        if (IsFifoQueue()) {
            if (!req.GetMessageGroupId()) {
                MakeError(resp, NErrors::MISSING_PARAMETER, "No MessageGroupId parameter.");
                return false;
            }

            if (!req.GetMessageDeduplicationId() && !QueueAttributes_->ContentBasedDeduplication) {
                MakeError(resp, NErrors::MISSING_PARAMETER, "No MessageDeduplicationId parameter.");
                return false;
            }
        }

        if (req.GetDelaySeconds() > TLimits::MaxDelaySeconds) {
            MakeError(resp, NErrors::INVALID_PARAMETER_VALUE, "Delay seconds are too big.");
            return false;
        }

        if (req.MessageAttributesSize() > TLimits::MaxMessageAttributes) {
            MakeError(resp, NErrors::INVALID_PARAMETER_COMBINATION, TStringBuilder() << "Message has more than " << TLimits::MaxMessageAttributes << " attributes.");
            return false;
        }

        TString description;
        if (!ValidateMessageBody(req.GetMessageBody(), description)) {
            if (Cfg().GetValidateMessageBody()) {
                MakeError(resp, NErrors::INVALID_PARAMETER_VALUE, TStringBuilder() << "Message body validation failed: " << description << ".");
                return false;
            } else {
                RLOG_SQS_WARN("Message body validation failed: " << description << ".");
            }
        }
        for (const auto& a : req.GetMessageAttributes()) {
            if (!ValidateMessageBody(a.GetStringValue(), description)) {
                if (Cfg().GetValidateMessageBody()) {
                    MakeError(resp, NErrors::INVALID_PARAMETER_VALUE, TStringBuilder() << "Message attribute \"" << a.GetName() << "\" validation failed: " << description << ".");
                    return false;
                } else {
                    RLOG_SQS_WARN("Message attribute \"" << a.GetName() << "\" validation failed: " << description << ".");
                }
            }
        }
        return true;
    }

    bool DoValidate() override {
        const size_t maxMessageSize = Min(TLimits::MaxMessageSize, QueueAttributes_->MaximumMessageSize);
        if (IsBatch_) {
            size_t size  = 0;
            size_t count = 0;
            bool tooBig = false;

            for (const auto& req : BatchRequest().GetEntries()) {
                const size_t msgSize = CalculateMessageSize(req);
                if (msgSize > maxMessageSize) {
                    tooBig = true;
                    break;
                }
                size  += msgSize;
                count += 1;
            }

            if (tooBig) {
                MakeError(Response_.MutableSendMessageBatch(), NErrors::INVALID_PARAMETER_VALUE,
                          TStringBuilder() << "Each message must be shorter than " << maxMessageSize << " bytes.");
                return false;
            }

            if (size > TLimits::MaxMessageSize) {
                MakeError(Response_.MutableSendMessageBatch(), NErrors::BATCH_REQUEST_TOO_LONG);
                return false;
            }
            if (count == 0) {
                MakeError(Response_.MutableSendMessageBatch(), NErrors::EMPTY_BATCH_REQUEST);
                return false;
            }
            if (count > TLimits::MaxBatchSize) {
                MakeError(Response_.MutableSendMessageBatch(), NErrors::TOO_MANY_ENTRIES_IN_BATCH_REQUEST);
                return false;
            }

            if (!GetQueueName()) {
                MakeError(Response_.MutableSendMessageBatch(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
                return false;
            }
        } else {
            if (CalculateMessageSize(Request()) > maxMessageSize) {
                MakeError(Response_.MutableSendMessage(), NErrors::INVALID_PARAMETER_VALUE, TStringBuilder() << "Message must be shorter than " << maxMessageSize << " bytes.");
                return false;
            }

            if (!GetQueueName()) {
                MakeError(Response_.MutableSendMessage(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
                return false;
            }
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return IsBatch_ ? Response_.MutableSendMessageBatch()->MutableError() : Response_.MutableSendMessage()->MutableError();
    }

    void DoAction() override {
        Y_ABORT_UNLESS(QueueAttributes_.Defined());
        if (FeatureFlags_.EnableSQSMigrationCompatibility_ && IsTopicCreated()) {
            if (IsFifoQueue() && !FeatureFlags_.EnableSQSMigrationFinished_) {
                DoActionDeduplicateTopicImplementation();
            } else {
                DoActionTopicImplementation();
            }
        } else {
            DoActionTableImplementation();
        }
    }

    void DoActionDeduplicateTopicImplementation() {
        Become(&TThis::StateFunc);

        auto req = MakeHolder<TSqsEvents::TEvDeduplicateMessageBatch>();
        req->RequestId = RequestId_;
        req->SenderId = UserSID_;

        for (size_t i = 0, size = IsBatch_ ? BatchRequest().EntriesSize() : 1; i < size; ++i) {
            auto* currentRequest = IsBatch_ ? &BatchRequest().GetEntries(i) : &Request();

            try {
                req->DeduplicationMessageIds.push_back(GetDeduplicationMessageId(*currentRequest));
            } catch (const std::exception& ex) {
                RLOG_SQS_ERROR("Failed to calculate SHA-256 of message body: " << ex.what());
            }
        }

        if (req->DeduplicationMessageIds.empty()) {
            DoActionTopicImplementation();
        } else {
            Send(QueueLeader_, std::move(req));
        }
    }

    TString GetDeduplicationMessageId(auto& request) {
        if (!IsFifoQueue()) {
            return {};
        }

        const auto& dedupParam = request.GetMessageDeduplicationId();
        if (dedupParam) {
            return dedupParam;
        }

        if (QueueAttributes_->ContentBasedDeduplication) {
            return CalcSHA256(request.GetMessageBody());
        }

        return {};
    }

    void DoActionTopicImplementation() {
        Become(&TThis::StateFunc);

        NPQ::NMLP::TWriterSettings writerSettings;
        writerSettings.DatabasePath = GetDatabaseName();
        writerSettings.TopicName = GetTopicName();
        writerSettings.Messages.reserve(IsBatch_ ? BatchRequest().EntriesSize() : 1);

        for (size_t i = 0, size = IsBatch_ ? BatchRequest().EntriesSize() : 1; i < size; ++i) {
            auto* currentRequest = IsBatch_ ? &BatchRequest().GetEntries(i) : &Request();
            auto* currentResponse = IsBatch_ ? Response_.MutableSendMessageBatch()->AddEntries() : Response_.MutableSendMessage();

            currentResponse->SetId(currentRequest->GetId());
            if (!ValidateSingleRequest(*currentRequest, currentResponse)) {
                continue;
            }

            TString deduplicationId;
            if (IsFifoQueue()) {
                try {
                    deduplicationId = GetDeduplicationMessageId(*currentRequest);
                } catch (const std::exception& ex) {
                    RLOG_SQS_ERROR("Failed to calculate SHA-256 of message body: " << ex.what());
                    MakeError(currentResponse, NErrors::INTERNAL_FAILURE);
                    continue;
                }

                auto it = BlockedDeduplicationMessageIds_.find(deduplicationId);
                if (it != BlockedDeduplicationMessageIds_.end()) {
                    const auto& [messageId, sequenceNumber] = it->second;
                    AddResponse(currentRequest, currentResponse, messageId, sequenceNumber);
                    continue;
                }
            }

            RequestToReplyIndexMapping_.push_back(i);

            writerSettings.Messages.emplace_back();
            auto& message = writerSettings.Messages.back();
            message.Index = i;
            message.MessageBody = std::move(currentRequest->GetMessageBody());
            message.Delay = GetDelay(*currentRequest);
            if (!deduplicationId.empty()) {
                message.MessageDeduplicationId = std::move(deduplicationId);
            }
            message.MessageGroupId = std::move(currentRequest->GetMessageGroupId());

           {
                Ydb::Ymq::V1::SendMessageBatchRequestEntry entry;
                for (const auto& attr : currentRequest->GetMessageAttributes()) {
                    Ydb::Ymq::V1::MessageAttribute dstAttribute;
                    dstAttribute.set_data_type(std::move(attr.GetDataType()));
                    if (const auto& value = attr.GetStringValue()) {
                        dstAttribute.set_string_value(std::move(value));
                    } else if (const auto& value = attr.GetBinaryValue()) {
                        dstAttribute.set_binary_value(std::move(value));
                    }
                    entry.mutable_message_attributes()->emplace(std::move(attr.GetName()), dstAttribute);
                }

                auto [attributes, md5] = NSQS::SerializeUserAttributes(entry);
                message.Attributes = std::move(attributes);
            }

            message.Attributes.emplace("sender_id", UserSID_);
        }

        if (writerSettings.Messages.size() > 0) {
            Register(NPQ::NMLP::CreateWriter(SelfId(), std::move(writerSettings)));
        } else {
            SendReplyAndDie();
        }
    }

    // coverity[var_deref_model]: false positive
    void DoActionTableImplementation() {
        Become(&TThis::StateFunc);

        const bool isFifo = IsFifoQueue();
        THolder<TSqsEvents::TEvSendMessageBatch> req;
        for (size_t i = 0, size = IsBatch_ ? BatchRequest().EntriesSize() : 1; i < size; ++i) {
            auto* currentRequest = IsBatch_ ? &BatchRequest().GetEntries(i) : &Request();
            auto* currentResponse = IsBatch_ ? Response_.MutableSendMessageBatch()->AddEntries() : Response_.MutableSendMessage();

            currentResponse->SetId(currentRequest->GetId());
            if (!ValidateSingleRequest(*currentRequest, currentResponse)) {
                continue;
            }

            TString deduplicationId;
            if (isFifo) {
                try {
                    deduplicationId = GetDeduplicationMessageId(*currentRequest);
                } catch (const std::exception& ex) {
                    RLOG_SQS_ERROR("Failed to calculate SHA-256 of message body: " << ex.what());
                    MakeError(currentResponse, NErrors::INTERNAL_FAILURE);
                    continue;
                }
            }

            if (!req) {
                req = MakeHolder<TSqsEvents::TEvSendMessageBatch>();
                req->RequestId = RequestId_;
                req->SenderId = UserSID_;
                req->Messages.reserve(size);
            }
            RequestToReplyIndexMapping_.push_back(i);
            req->Messages.emplace_back();
            auto& messageReq = req->Messages.back();
            messageReq.MessageId = CreateGuidAsString();
            messageReq.Body = currentRequest->GetMessageBody();
            messageReq.Delay = GetDelay(*currentRequest);
            messageReq.DeduplicationId = std::move(deduplicationId);
            messageReq.MessageGroupId = currentRequest->GetMessageGroupId();

            {
                TMessageAttributeList attrs;
                for (const auto& a : currentRequest->GetMessageAttributes()) {
                    attrs.AddAttributes()->CopyFrom(a);
                }
                messageReq.Attributes = ProtobufToString(attrs);
            }
        }

        if (req) {
            Send(QueueLeader_, req.Release());
        } else {
            SendReplyAndDie();
        }
    }


    TString DoGetQueueName() const override {
        return IsBatch_ ? BatchRequest().GetQueueName() : Request().GetQueueName();
    }

    static TString ProtobufToString(const NProtoBuf::Message& proto) {
        TString ret;
        Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&ret);
        return ret;
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,         HandleWakeup);
            hFunc(NPQ::NMLP::TEvWriteResponse, Handle);
            hFunc(TSqsEvents::TEvDeduplicateMessageBatchResponse, Handle);
            hFunc(TSqsEvents::TEvSendMessageBatchResponse, HandleSendResponseTableImplementation);
        }
    }

    void AddResponse(const auto* currentRequest, auto* currentResponse, const TString& messageId, ui64 sequenceNumber) {
        currentResponse->SetMessageId(messageId);
        if (IsFifoQueue()) {
            currentResponse->SetSequenceNumber(sequenceNumber);
        }
        currentResponse->SetMD5OfMessageBody(MD5::Calc(currentRequest->GetMessageBody()));
        if (currentRequest->MessageAttributesSize() > 0) {
            const TString md5 = CalcMD5OfMessageAttributes(currentRequest->GetMessageAttributes());
            currentResponse->SetMD5OfMessageAttributes(md5);
            RLOG_SQS_DEBUG("Calculating MD5 of message attributes. Request: " << *currentRequest << "\nMD5 of message attributes: " << md5);
        }
    }

    void Handle(NPQ::NMLP::TEvWriteResponse::TPtr& ev) {
        const auto* response = ev->Get();
        const auto& messages = response->Messages;

        for (size_t i = 0, size = messages.size(); i < size; ++i) {
            const auto& message = messages[i];

            auto* currentResponse = IsBatch_ ?
                  Response_.MutableSendMessageBatch()->MutableEntries(RequestToReplyIndexMapping_[i])
                : Response_.MutableSendMessage();
            auto* currentRequest = IsBatch_ ? &BatchRequest().GetEntries(RequestToReplyIndexMapping_[i]) : &Request();

            if (response->DescribeStatus != NPQ::NDescriber::EStatus::SUCCESS) {
                MakeError(currentResponse, NErrors::INTERNAL_FAILURE,
                    NPQ::NDescriber::Description(GetTopicName(), response->DescribeStatus));
            } else if (message.Status == Ydb::StatusIds::SUCCESS || message.Status == Ydb::StatusIds::ALREADY_EXISTS) {
                AddResponse(
                    currentRequest,
                    currentResponse,
                    ToMessageId(message.MessageId.value()),
                    message.MessageId->Offset // TODO: как быть с несколькими партициями?
                );
            } else {
                MakeError(currentResponse, NErrors::INTERNAL_FAILURE);
            }
        }

        SendReplyAndDie();
    }

    void Handle(TSqsEvents::TEvDeduplicateMessageBatchResponse::TPtr& ev) {
        if (ev->Get()->StatusCode == Ydb::StatusIds::SUCCESS) {
            BlockedDeduplicationMessageIds_ = std::move(ev->Get()->BlockedDeduplicationMessageIds);
            DoActionTopicImplementation();
        } else {
            RLOG_SQS_DEBUG("Message deduplication error");
            if (IsBatch_) {
                for (size_t i = 0, size = BatchRequest().EntriesSize(); i < size; ++i) {
                    auto* currentResponse = Response_.MutableSendMessageBatch()->MutableEntries(i);
                    MakeError(currentResponse, NErrors::INTERNAL_FAILURE);
                }
            } else {
                MakeError(Response_.MutableSendMessage(), NErrors::INTERNAL_FAILURE);
            }
            SendReplyAndDie();
        }
    }

    void HandleSendResponseTableImplementation(TSqsEvents::TEvSendMessageBatchResponse::TPtr& ev) {
        const bool isFifo = IsFifoQueue();
        for (size_t i = 0, size = ev->Get()->Statuses.size(); i < size; ++i) {
            const auto& status = ev->Get()->Statuses[i];
            Y_ABORT_UNLESS(!IsBatch_ || RequestToReplyIndexMapping_[i] < BatchRequest().EntriesSize());
            auto* currentResponse = IsBatch_ ? Response_.MutableSendMessageBatch()->MutableEntries(RequestToReplyIndexMapping_[i]) : Response_.MutableSendMessage();
            auto* currentRequest = IsBatch_ ? &BatchRequest().GetEntries(RequestToReplyIndexMapping_[i]) : &Request();
            if (status.Status == TSqsEvents::TEvSendMessageBatchResponse::ESendMessageStatus::OK
                || status.Status == TSqsEvents::TEvSendMessageBatchResponse::ESendMessageStatus::AlreadySent) {
                currentResponse->SetMessageId(status.MessageId);
                if (isFifo) {
                    currentResponse->SetSequenceNumber(status.SequenceNumber);
                }
                currentResponse->SetMD5OfMessageBody(MD5::Calc(currentRequest->GetMessageBody()));
                if (currentRequest->MessageAttributesSize() > 0) {
                    const TString md5 = CalcMD5OfMessageAttributes(currentRequest->GetMessageAttributes());
                    currentResponse->SetMD5OfMessageAttributes(md5);
                    RLOG_SQS_DEBUG("Calculating MD5 of message attributes. Request: " << *currentRequest << "\nMD5 of message attributes: " << md5);
                }
            } else {
                MakeError(currentResponse, NErrors::INTERNAL_FAILURE);
            }
        }

        SendReplyAndDie();
    }

    const TSendMessageRequest& Request() const {
        return SourceSqsRequest_.GetSendMessage();
    }

    const TSendMessageBatchRequest& BatchRequest() const {
        return SourceSqsRequest_.GetSendMessageBatch();
    }

private:
    std::vector<size_t> RequestToReplyIndexMapping_;

    const bool IsBatch_;
    // deduplication message id -> sequenceNumber
    std::unordered_map<TString, std::pair<TString, ui64>> BlockedDeduplicationMessageIds_;
};

IActor* CreateSendMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb, const TString& peername) {
    return new TSendMessageActor(sourceSqsRequest, false, std::move(cb), peername);
}

IActor* CreateSendMessageBatchActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb, const TString& peername) {
    return new TSendMessageActor(sourceSqsRequest, true, std::move(cb), peername);
}

} // namespace NKikimr::NSQS
