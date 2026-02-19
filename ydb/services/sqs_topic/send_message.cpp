#include "send_message.h"
#include "actor.h"
#include "utils.h"
#include "request.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/http_proxy/events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/ymq/attributes/attributes_md5.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/error/error.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>
#include <ydb/services/sqs_topic/queue_url/holder/queue_url_holder.h>

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/sqs.pb.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/services/sqs_topic/sqs_topic_proxy.h>

#include <ydb/public/api/grpc/draft/ydb_ymq_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/describer/describer.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/services/sqs_topic/statuses.h>

#include <library/cpp/digest/md5/md5.h>
#include <util/generic/guid.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {
    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    struct TSendMessageItem {
        TString MessageBody;
        TMaybe<TString> MessageGroupId;
        TMaybe<TString> MessageDeduplicationId;
        TMaybe<TString> SerializedMessageAttributes;
        int DelaySeconds{};
        TString BatchId;

        size_t BatchIndex{};
        TString MD5OfBody;
        TMaybe<TString> MD5OfMessageAttributes;

        TMaybe<NSQS::TError> ValidationError;
        TMaybe<NPQ::NMLP::TMessageId> MessageId;
    };

    template <class TProtoRequest>
    static std::expected<TRichQueueUrl, TString> ParseQueueUrlFromRequest(NKikimr::NGRpcService::IRequestOpCtx* request) {
        return ParseQueueUrl(GetRequest<TProtoRequest>(request).queue_url());
    }

    template <class TDerived, class TServiceRequest>
    class TSendMessageActorBase: public TQueueUrlHolder, public TGrpcActorBase<TSendMessageActorBase<TDerived, TServiceRequest>, TServiceRequest> {
    protected:
        using TBase = TGrpcActorBase<TSendMessageActorBase, TServiceRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TSendMessageActorBase(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrlFromRequest<TProtoRequest>(request))
            , TBase(request, GetTopicPath().value_or(""))
        {
        }

        ~TSendMessageActorBase() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::CheckAccessWithWriteTopicPermission = true;
            TBase::Bootstrap(ctx);

            if (this->Request().queue_url().empty()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!QueueUrl_.has_value()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            NACLib::TUserToken token(this->Request_->GetSerializedToken());
            ShouldBeCharged_ = FindPtr(AppData(ctx)->PQConfig.GetNonChargeableUser(), token.GetUserSID()) == nullptr;

            DoWrite();
        }

        void DoWrite() {
            this->Become(&TSendMessageActorBase::StateWork);

            const auto& request = Request();
            Items = ConvertRequestToWriteItems(request);
            for (ui32 i = 0; i < Items.size(); ++i) {
                Items[i].BatchIndex = i;
            }

            if (!ValidateMessages()) {
                return;
            }

            auto toOptional = [](TMaybe<TString>&& value) {
                return value.Empty() ? std::nullopt : std::make_optional(std::move(*value));
            };

            TVector<NPQ::NMLP::TWriterSettings::TMessage> validItems(Reserve(Items.size()));
            for (auto& item : Items) {
                if (item.ValidationError.Empty()) {
                    validItems.push_back(NPQ::NMLP::TWriterSettings::TMessage{
                        .Index = item.BatchIndex,
                        .MessageBody = std::move(item.MessageBody),
                        .MessageGroupId = toOptional(std::move(item.MessageGroupId)),
                        .MessageDeduplicationId = toOptional(std::move(item.MessageDeduplicationId)),
                        .SerializedMessageAttributes = toOptional(std::move(item.SerializedMessageAttributes)),
                        .Delay = TDuration::Seconds(item.DelaySeconds),
                    });
                }
            }

            this->Send(NHttpProxy::MakeMetricsServiceID(),
                new NHttpProxy::TEvServerlessProxy::TEvCounter{
                    static_cast<i64>(Items.size()), true, true,
                    GetRequestMessageCountMetricsLabels(
                        QueueUrl_->Database,
                        FullTopicPath_,
                        QueueUrl_->Consumer,
                        TDerived::Method)
                });

            this->Send(NHttpProxy::MakeMetricsServiceID(),
                new NHttpProxy::TEvServerlessProxy::TEvCounter{
                    static_cast<i64>(Request().ByteSizeLong()), true, true,
                    GetRequestSizeMetricsLabels(
                        QueueUrl_->Database,
                        FullTopicPath_,
                        QueueUrl_->Consumer,
                        TDerived::Method)
                });

            auto userToken = MakeIntrusive<NACLib::TUserToken>(this->Request_->GetSerializedToken());
            NPQ::NMLP::TWriterSettings writerSettings{
                .DatabasePath = QueueUrl_->Database,
                .TopicName = FullTopicPath_,
                .Messages = std::move(validItems),
                .ShouldBeCharged = ShouldBeCharged_,
                .UserToken = std::move(userToken),
            };
            WriterActor_ = this->RegisterWithSameMailbox(NPQ::NMLP::CreateWriter(this->SelfId(), std::move(writerSettings)));
        }

        void Handle(NPQ::NMLP::TEvWriteResponse::TPtr& ev) {
            WriterActor_ = {};

            if (ev->Get()->DescribeStatus != NPQ::NDescriber::EStatus::SUCCESS) {
                auto describerStatus = MapDescriberStatus(FullTopicPath_, ev->Get()->DescribeStatus);
                this->ReplyWithError(*describerStatus.Error);
                return;
            }

            ssize_t successCount = 0;
            ssize_t failedCount = 0;

            for (auto& message : ev->Get()->Messages) {
                if (message.MessageId.has_value()) {
                    Items[message.Index].MessageId = message.MessageId.value();
                    ++successCount;
                } else {
                    ++failedCount;
                }
            }

            this->Send(NHttpProxy::MakeMetricsServiceID(),
                new NHttpProxy::TEvServerlessProxy::TEvCounter{
                    successCount, true, true,
                    GetResponseMessageCountMetricsLabels(
                        QueueUrl_->Database,
                        FullTopicPath_,
                        QueueUrl_->Consumer,
                        TDerived::Method,
                        "success")
                });
            this->Send(NHttpProxy::MakeMetricsServiceID(),
                new NHttpProxy::TEvServerlessProxy::TEvCounter{
                    failedCount, true, true,
                    GetResponseMessageCountMetricsLabels(
                        QueueUrl_->Database,
                        FullTopicPath_,
                        QueueUrl_->Consumer,
                        TDerived::Method,
                        "failed")
                });

            static_cast<TDerived*>(this)->ReplyAndDie(TlsActivationContext->AsActorContext());
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NPQ::NMLP::TEvWriteResponse, Handle);
                default:
                    TBase::StateWork(ev);
            }
        }

        bool ValidateMessages() {
            if (Items.size() < NSQS::TLimits::MinBatchSize) {
                this->ReplyWithError(MakeError(NSQS::NErrors::EMPTY_BATCH_REQUEST));
                return false;
            }
            if (Items.size() > NSQS::TLimits::MaxBatchSize) {
                this->ReplyWithError(MakeError(NSQS::NErrors::TOO_MANY_ENTRIES_IN_BATCH_REQUEST));
                return false;
            }

            THashSet<TString> batchIds;
            for (auto& item : Items) {
                const size_t msgSize = item.MessageBody.size() + (item.SerializedMessageAttributes ? item.SerializedMessageAttributes->size() : 0);
                if (msgSize > NSQS::TLimits::MaxMessageSize) {
                    item.ValidationError = MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "The length the message is more than the limit.");
                }
                auto [_, unique] = batchIds.insert(item.BatchId);
                if (!unique) {
                    this->ReplyWithError(MakeError(NSQS::NErrors::BATCH_ENTRY_IDS_NOT_DISTINCT));
                    return false;
                }
            }
            return true;
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            Y_UNUSED(ev);
        }

        void Die(const TActorContext& ctx) override {
            if (DescriptorActorId_) {
                ctx.Send(DescriptorActorId_, new TEvents::TEvPoison);
            }
            if (WriterActor_) {
                ctx.Send(WriterActor_, new TEvents::TEvPoison);
            }
            this->TBase::Die(ctx);
        }

        void FillSingleSuccessMessage(const TSendMessageItem& item, auto& result) {
            result.set_message_id(GenerateMessageId(QueueUrl_->Database, FullTopicPath_, *item.MessageId));
            result.set_m_d_5_of_message_body(item.MD5OfBody);
            if (item.MD5OfMessageAttributes) {
                result.set_m_d_5_of_message_attributes(*item.MD5OfMessageAttributes);
            }
            if constexpr (!std::is_same_v<Ydb::Ymq::V1::SendMessageResult, std::remove_cvref_t<decltype(result)>>) {
                result.set_id(item.BatchId);
            }
            result.set_sequence_number("0");

            Y_ASSERT(result.IsInitialized());
        }

    private:
        TVector<TSendMessageItem> ConvertRequestToWriteItems(const TProtoRequest& request) {
            return static_cast<TDerived*>(this)->ConvertRequestToWriteItemsImpl(request);
        }

        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    protected:
        TVector<TSendMessageItem> Items;

    private:
        TActorId DescriptorActorId_;
        bool ShouldBeCharged_{};
        TActorId WriterActor_;
    };

    static TString GetBatchId(const Ydb::Ymq::V1::SendMessageBatchRequestEntry& batchEntry) {
        return batchEntry.id();
    }

    static TString GetBatchId(const Ydb::Ymq::V1::SendMessageRequest& /*request*/) {
        return {};
    }

    static TSendMessageItem ConvertSingleRequestToWriteItems(const auto& request) {
        TSendMessageItem item;
        item.MessageBody = request.message_body();
        item.MD5OfBody = MD5::Calc(request.message_body());
        if (request.has_message_group_id()) {
            item.MessageGroupId = request.message_group_id();
        }
        if (request.has_message_deduplication_id()) {
            item.MessageDeduplicationId = request.message_deduplication_id();
        }
        if (request.message_attributes_size()) {
            NKikimr::NSQS::TMessageAttributes messageAttributes;
            for (const auto& [attrName, attrValue] : request.message_attributes()) {
                auto* dstAttribute = messageAttributes.add_attributes();
                dstAttribute->SetName(attrName);
                if (const auto& value = attrValue.string_value()) {
                    dstAttribute->SetStringValue(value);
                }
                if (const auto& value = attrValue.binary_value()) {
                    dstAttribute->SetBinaryValue(value);
                }
                dstAttribute->SetDataType(attrValue.data_type());
            }
            TString serialized;
            bool res = messageAttributes.SerializeToString(&serialized);
            Y_ABORT_UNLESS(res);
            item.SerializedMessageAttributes = std::move(serialized);
            item.MD5OfMessageAttributes = NSQS::CalcMD5OfMessageAttributes(messageAttributes.attributes());
        }
        item.DelaySeconds = request.delay_seconds();
        item.BatchIndex = 0;
        item.BatchId = GetBatchId(request);
        return item;
    }

    static void FillSingleFailedMessage(const NSQS::TError& error, const TSendMessageItem& item, const TString& method, auto& result) {
        Y_UNUSED(method);
        result.set_code(error.GetErrorCode());
        result.set_id(item.BatchId);
        result.set_sender_fault(IsSenderFailure(error));
        result.set_message(error.GetMessage());
        Y_ASSERT(result.IsInitialized());
    }

    class TSendMessageActor: public TSendMessageActorBase<TSendMessageActor, TEvSqsTopicSendMessageRequest> {
    public:
        const static inline TString Method = "SendMessage";

    public:
        using TBase = TSendMessageActorBase<TSendMessageActor, TEvSqsTopicSendMessageRequest>;
        using TBase::TBase;

        TVector<TSendMessageItem> ConvertRequestToWriteItemsImpl(const Ydb::Ymq::V1::SendMessageRequest& request) const {
            TSendMessageItem pm = ConvertSingleRequestToWriteItems(request);
            return {std::move(pm)};
        }

        void ReplyAndDie(const TActorContext& ctx) {
            if (Items.size() != 1) {
                this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Unexpected number of messages to write: expected={}, got={}", 1, Items.size())));
                return;
            }
            const TSendMessageItem& item = Items.front();
            if (item.ValidationError.Defined()) {
                this->ReplyWithError(*item.ValidationError);
                return;
            }
            if (item.MessageId.Empty()) {
                this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Unavailable"));
                return;
            }

            Ydb::Ymq::V1::SendMessageResult result;
            FillSingleSuccessMessage(item, result);
            return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    };

    class TSendMessageBatchActor: public TSendMessageActorBase<TSendMessageBatchActor, TEvSqsTopicSendMessageBatchRequest> {
    public:
        const static inline TString Method = "SendMessageBatch";

    public:
        using TBase = TSendMessageActorBase<TSendMessageBatchActor, TEvSqsTopicSendMessageBatchRequest>;
        using TBase::TBase;

        TVector<TSendMessageItem> ConvertRequestToWriteItemsImpl(const Ydb::Ymq::V1::SendMessageBatchRequest& request) const {
            TVector<TSendMessageItem> result(Reserve(request.entries_size()));
            for (const auto& entry : request.entries()) {
                result.push_back(ConvertSingleRequestToWriteItems(entry));
            }
            return result;
        }

        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::SendMessageBatchResult result;
            for (const TSendMessageItem& item : Items) {
                if (item.ValidationError.Defined()) {
                    FillSingleFailedMessage(*item.ValidationError, item, Method, *result.mutable_failed()->Add());
                } else if (item.MessageId.Empty()) {
                    FillSingleFailedMessage(MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Unavailable"), item, Method, *result.mutable_failed()->Add());
                } else {
                    FillSingleSuccessMessage(item, *result.mutable_successful()->Add());
                }
            }
            return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    };

    std::unique_ptr<NActors::IActor> CreateSendMessageActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TSendMessageActor>(msg);
    }

    std::unique_ptr<NActors::IActor> CreateSendMessageBatchActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TSendMessageBatchActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
