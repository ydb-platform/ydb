#include "send_message.h"
#include "actor.h"
#include "utils.h"
#include "request.h"
#include "events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
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

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/mlp/mlp_message_attributes.h>

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
        TString MessageId;
        TMaybe<TString> MessageGroupId;
        TMaybe<TString> MessageDeduplicationId;
        TMaybe<TString> SerializedMessageAttributes;
        int DelaySeconds{};
        TString BatchId;

        ui32 BatchIndex{};
        TString MD5OfBody;
        TMaybe<TString> MD5OfMessageAttributes;

        TMaybe<NSQS::TError> ValidationError;
    };

    size_t SerializeTo(const TSendMessageItem& item, ::NKikimrClient::TPersQueuePartitionRequest_TCmdWrite* cmdWrite) {
        NKikimrPQClient::TDataChunk proto;
        proto.SetCodec(0); // NPersQueue::CODEC_RAW
        proto.SetData(item.MessageBody);
        {
            Y_ASSERT(!item.MessageId.empty());
            auto* m = proto.AddMessageMeta();
            m->set_key(NPQ::NMLP::NMessageConsts::MessageId);
            m->set_value(item.MessageId);
        }
        if (!item.MessageDeduplicationId.Empty()) {
            auto* m = proto.AddMessageMeta();
            m->set_key(NPQ::NMLP::NMessageConsts::MessageDeduplicationId);
            m->set_value(*item.MessageDeduplicationId);
        }
        if (!item.SerializedMessageAttributes.Empty()) {
            auto* m = proto.AddMessageMeta();
            m->set_key(NPQ::NMLP::NMessageConsts::MessageAttributes);
            m->set_value(*item.SerializedMessageAttributes);
        }
        if (item.DelaySeconds > 0) {
            auto* m = proto.AddMessageMeta();
            m->set_key(NPQ::NMLP::NMessageConsts::DelaySeconds);
            m->set_value(ToString(item.DelaySeconds));
        }

        TString dataStr;
        bool res = proto.SerializeToString(&dataStr);
        Y_ABORT_UNLESS(res);
        cmdWrite->SetSourceId(NPQ::NSourceIdEncoding::EncodeSimple(item.MessageGroupId.GetOrElse(TString{})));
        cmdWrite->SetData(dataStr);
        cmdWrite->SetDisableDeduplication(true);
        cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
        cmdWrite->SetUncompressedSize(item.MessageBody.size());
        cmdWrite->SetExternalOperation(true);

        size_t totalSize = item.MessageBody.size() + item.SerializedMessageAttributes.Cast<TStringBuf>().GetOrElse({}).size();
        return totalSize;
    }

    class TSendMessagePartitionActor: public TActorBootstrapped<TSendMessagePartitionActor> {
    public:
        using TBase = TActorBootstrapped<TSendMessagePartitionActor>;

        TSendMessagePartitionActor(NActors::TActorId parentId, ui64 tabletId, ui32 partition, const TString& topic, TVector<TSendMessageItem> dataToWrite, ui32 cookie, bool shouldBeCharged)
            : ParentId(std::move(parentId))
            , TabletId(tabletId)
            , Partition(partition)
            , Topic(topic)
            , DataToWrite(std::move(dataToWrite))
            , ShouldBeCharged(shouldBeCharged)
            , Cookie(cookie)
        {
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            if (DataToWrite.empty()) {
                // all messages fails validation
                return HandleEmptyTask(ctx);
            }
            SendWriteRequest(ctx);
            Become(&TSendMessagePartitionActor::PartitionWriteFunc);
        }

    private:
        STFUNC(PartitionWriteFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvPersQueue::TEvResponse, HandlePartitionWriteResult);
                HFunc(TEvPipeCache::TEvDeliveryProblem, HandleDeliveryProblem);
            };
        }

        void HandleDeliveryProblem(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
            if (ev->Cookie != PipeCookie) {
                return;
            }
            LOG_WARN_S(
                    ctx,
                    NKikimrServices::SQS,
                    "Delivery problem. TabletId=" << TabletId);
            ReplyWithError(ctx, NSQS::NErrors::SERVICE_UNAVAILABLE);
        }

        void SendWriteRequest(const TActorContext&) {
            NKikimrClient::TPersQueueRequest request;
            request.MutablePartitionRequest()->SetTopic(Topic);
            request.MutablePartitionRequest()->SetPartition(Partition);
            request.MutablePartitionRequest()->SetIsDirectWrite(true);

            ui64 totalSize = 0;
            for (const auto& item : DataToWrite) {
                Y_ASSERT(item.ValidationError.Empty());
                ::NKikimrClient::TPersQueuePartitionRequest_TCmdWrite* w = request.MutablePartitionRequest()->AddCmdWrite();
                totalSize += SerializeTo(item, w);
            }
            if (ShouldBeCharged) {
                request.MutablePartitionRequest()->SetPutUnitsSize(NPQ::PutUnitsSize(totalSize));
            }

            std::unique_ptr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
            req->Record.Swap(&request);

            auto forward = std::make_unique<TEvPipeCache::TEvForward>(req.release(), TabletId, true, ++PipeCookie);
            Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
        }

        void HandlePartitionWriteResult(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
            if (CheckForError(ev, ctx)) {
                return;
            }
            auto result = MakeHolder<NSqsTopic::V1::TEvSqsTopic::TEvPartitionActorResult>();
            result->PartitionId = Partition;
            for (ui32 i = 0; i < DataToWrite.size(); ++i) {
                const auto& cmdResult = ev->Get()->Record.GetPartitionResponse().GetCmdWriteResult(i);
                result->Messages.push_back({
                    .Offset = cmdResult.GetOffset(),
                    .Id = DataToWrite[i].BatchId,
                    .BatchIndex = DataToWrite[i].BatchIndex,
                });
            }
            ctx.Send(ParentId, result.Release(), Cookie);
            Die(ctx);
        }

        void HandleEmptyTask(const TActorContext& ctx) {
            auto result = MakeHolder<NSqsTopic::V1::TEvSqsTopic::TEvPartitionActorResult>();
            result->PartitionId = Partition;
            ctx.Send(ParentId, result.Release(), Cookie);
            Die(ctx);
        }

        void ReplyWithError(const NActors::TActorContext& ctx, const NSQS::TErrorClass& errorClass, const TString& message = TString()) {
            const NSQS::TError error = MakeError(errorClass, message);
            auto result = MakeHolder<NSqsTopic::V1::TEvSqsTopic::TEvPartitionActorResult>();
            result->PartitionId = Partition;
            for (ui32 i = 0; i < DataToWrite.size(); ++i) {
                TEvSqsTopic::TEvPartitionActorResult::TMessage msg{
                    .Offset = -1,
                    .Id = DataToWrite[i].BatchId,
                    .BatchIndex = DataToWrite[i].BatchIndex,
                    .Error = error,
                };
                result->Messages.push_back(std::move(msg));
            }
            ctx.Send(ParentId, result.Release());
            Die(ctx);
        }

        bool CheckForError(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx) {
            const auto& record = ev->Get()->Record;
            if (record.HasErrorCode() && record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
                ReplyWithError(ctx, NSQS::NErrors::SERVICE_UNAVAILABLE);
                return true;
            }
            if (!ev->Get()->Record.HasPartitionResponse()) {
                LOG_WARN_S(
                    ctx,
                    NKikimrServices::SQS,
                    "Missing partition response");
                ReplyWithError(ctx, NSQS::NErrors::INTERNAL_FAILURE);
                return true;
            }
            if (ev->Get()->Record.GetPartitionResponse().GetCmdWriteResult().size() != std::ssize(DataToWrite)) {
                LOG_WARN_S(
                    ctx,
                    NKikimrServices::SQS,
                    std::format("Partition response length mismatch: {} != {}", ev->Get()->Record.GetPartitionResponse().GetCmdWriteResult().size(), std::ssize(DataToWrite)));
                ReplyWithError(ctx, NSQS::NErrors::INTERNAL_FAILURE);
                return true;
            }
            return false;
        }

        void Die(const TActorContext& ctx) override {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
            TBase::Die(ctx);
        }

    private:
        NActors::TActorId ParentId;
        ui64 TabletId = 0;
        ui32 Partition = 0;
        TString Topic;
        TVector<TSendMessageItem> DataToWrite;
        bool ShouldBeCharged;
        ui32 Cookie = 0;
        ui64 PipeCookie = 1;
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
            TBase::Bootstrap(ctx);

            if (this->Request().queue_url().empty()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!QueueUrl_.has_value()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            TString serializedToken = this->Request_->GetSerializedToken();
            NPQ::NDescriber::TDescribeSettings describeSettings{
                .UserToken = MakeIntrusive<NACLib::TUserToken>(serializedToken),
                .AccessRights = NACLib::EAccessRights::UpdateRow,
            };

            std::unordered_set<TString> paths{FullTopicPath_};
            NACLib::TUserToken token(this->Request_->GetSerializedToken());
            ShouldBeCharged_ = FindPtr(AppData(ctx)->PQConfig.GetNonChargeableUser(), token.GetUserSID()) == nullptr;

            DescriptorActorId_ = ctx.RegisterWithSameMailbox(NPQ::NDescriber::CreateDescriberActor(this->SelfId(), this->QueueUrl_->Database, std::move(paths), describeSettings));
            this->Become(&TSendMessageActorBase::StateWork);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse); // override for testing
                HFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, Handle);
                HFunc(NSqsTopic::V1::TEvSqsTopic::TEvPartitionActorResult, Handle);
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

        void Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
            DescriptorActorId_ = {};
            const NPQ::NDescriber::TEvDescribeTopicsResponse* response = ev->Get();
            for (const auto& [name, info] : response->Topics) {
                LOG_TRACE_S(
                    ctx,
                    NKikimrServices::SQS,
                    NKikimr::NPQ::NDescriber::Description(name, info.Status));
            }
            AFL_ENSURE(response->Topics.size() == 1)
            ("#topics", response->Topics.size());
            const auto& [name, info] = *response->Topics.begin();

            if (info.Status != NKikimr::NPQ::NDescriber::EStatus::SUCCESS) {
                auto describerStatus = MapDescriberStatus(name, info.Status);
                this->ReplyWithError(*describerStatus.Error);
                return;
            }

            const auto& request = Request();
            Items = ConvertRequestToWriteItems(request);
            for (ui32 i = 0; i < Items.size(); ++i) {
                Items[i].BatchIndex = i;
            }

            if (!ValidateMessages()) {
                return;
            }

            const NKikimr::NPQ::IPartitionChooser::TPartitionInfo* targetPartition = [&]() {
                if (!Items.empty() && Items.front().MessageGroupId && !Items.front().MessageGroupId->empty()) {
                    return info.Info->PartitionChooser->GetPartition(*Items.front().MessageGroupId);
                } else {
                    return info.Info->PartitionChooser->GetRandomPartition();
                }
            }();
            if (!targetPartition) {
                LOG_WARN_S(
                    ctx,
                    NKikimrServices::SQS,
                    "Unable to choose active partition");
                return this->ReplyWithError(MakeError(NSQS::NErrors::SERVICE_UNAVAILABLE));
            };

            TVector<TSendMessageItem> validItems(Reserve(Items.size()));
            for (const auto& item : Items) {
                if (item.ValidationError.Empty()) {
                    validItems.push_back(item);
                }
            }

            PartitionActor_ = ctx.RegisterWithSameMailbox(
                new TSendMessagePartitionActor{
                    ctx.SelfID,
                    targetPartition->TabletId,
                    targetPartition->PartitionId,
                    FullTopicPath_,
                    std::move(validItems),
                    0,
                    ShouldBeCharged_});
        }

        void Handle(NSqsTopic::V1::TEvSqsTopic::TEvPartitionActorResult::TPtr& ev, const TActorContext& ctx) {
            PartitionActor_ = {};
            Result = std::move(ev);
            static_cast<TDerived*>(this)->ReplyAndDie(ctx);
        }

        void Die(const TActorContext& ctx) override {
            if (DescriptorActorId_) {
                ctx.Send(DescriptorActorId_, new TEvents::TEvPoison);
            }
            if (PartitionActor_) {
                ctx.Send(PartitionActor_, new TEvents::TEvPoison);
            }
            this->TBase::Die(ctx);
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            Y_UNUSED(ev);
        }

    private:
        TVector<TSendMessageItem> ConvertRequestToWriteItems(const TProtoRequest& request) {
            return static_cast<TDerived*>(this)->ConvertRequestToWriteItemsImpl(request);
        }

        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    protected:
        NSqsTopic::V1::TEvSqsTopic::TEvPartitionActorResult::TPtr Result{};
        TVector<TSendMessageItem> Items;

    private:
        TActorId DescriptorActorId_;
        bool ShouldBeCharged_{};
        TActorId PartitionActor_;
    };

    static TString GetBatchId(const Ydb::Ymq::V1::SendMessageBatchRequestEntry& batchEntry) {
        return batchEntry.id();
    }

    static TString GetBatchId(const Ydb::Ymq::V1::SendMessageRequest& /*request*/) {
        return {};
    }

    static TSendMessageItem ConvertSingleRequestToWriteItems(const auto& request) {
        TSendMessageItem item;
        item.MessageId = CreateGuidAsString();
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

    static void FillSingleSuccessMessage(const TSendMessageItem& item, auto& result) {
        result.set_message_id(item.MessageId);
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

    static void FillSingleFailedMessage(const NSQS::TError& error, const TSendMessageItem& item, const TString& method, auto& result) {
        Y_UNUSED(method);
        result.set_code(error.GetErrorCode());
        result.set_id(item.BatchId);
        result.set_sender_fault(IsSenderFailure(error));
        result.set_message(error.GetMessage());
        Y_ASSERT(result.IsInitialized());
    }

    class TSendMessageActor: public TSendMessageActorBase<TSendMessageActor, TEvSqsTopicSendMessageRequest> {
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
            for (auto&& message : Result->Get()->Messages) {
                if (message.Error.Defined()) {
                    this->ReplyWithError(*message.Error);
                    return;
                }
            }
            if (Result->Get()->Messages.size() != 1) {
                this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Unexpected number of messages to write: expected={}, got={}", 1, Result->Get()->Messages.size())));
                return;
            }
            Ydb::Ymq::V1::SendMessageResult result;
            FillSingleSuccessMessage(item, result);
            return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    };

    class TSendMessageBatchActor: public TSendMessageActorBase<TSendMessageBatchActor, TEvSqsTopicSendMessageBatchRequest> {
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
                const TEvSqsTopic::TEvPartitionActorResult::TMessage* message = FindIfPtr(Result->Get()->Messages, [&item](const auto& m) { return m.BatchIndex == item.BatchIndex; });
                if (item.ValidationError.Defined()) {
                    FillSingleFailedMessage(*item.ValidationError, item, Method, *result.mutable_failed()->Add());
                } else if (!message) {
                    return this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Missing write response"));
                } else if (message->Error.Defined()) {
                    FillSingleFailedMessage(*message->Error, item, Method, *result.mutable_failed()->Add());
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
