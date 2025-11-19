#include "receive_message.h"
#include "actor.h"
#include "error.h"
#include "request.h"
#include "receipt.h"

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/ymq/attributes/attributes_md5.h>
#include <ydb/core/ymq/attributes/attribute_name.h>
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

#include <ydb/core/persqueue/public/mlp/mlp_message_attributes.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/digest/md5/md5.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {
    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;



    class TReceiveMessageActor: public TQueueUrlHolder, public TGrpcActorBase<TReceiveMessageActor, TEvSqsTopicReceiveMessageRequest> {
    protected:
        using TBase = TGrpcActorBase<TReceiveMessageActor, TEvSqsTopicReceiveMessageRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TReceiveMessageActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrl(GetRequest<TProtoRequest>(request).queue_url()))
            , TBase(request, TQueueUrlHolder::GetTopicPath().value_or(""))
        {
        }

        ~TReceiveMessageActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);

            if (this->Request().queue_url().empty()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!QueueUrl_.has_value()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            TMaybe readerSettings = MakeReaderSettings();
            if (!readerSettings.Defined()) {
                return;
            }

            std::unique_ptr<IActor> actorPtr{NKikimr::NPQ::NMLP::CreateReader(this->SelfId(), std::move(*readerSettings))};
            ReaderActorId_ = ctx.RegisterWithSameMailbox(actorPtr.release());
            this->Become(&TReceiveMessageActor::StateWork);
        }

        TMaybe<NKikimr::NPQ::NMLP::TReaderSettings> MakeReaderSettings() {
            const Ydb::Ymq::V1::ReceiveMessageRequest& request = Request();

            const i32 maxNumberOfMessages = request.has_max_number_of_messages() ? request.max_number_of_messages() : 1;
            if (std::cmp_less(maxNumberOfMessages, NSQS::TLimits::MinBatchSize)) {
                ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("MaxNumberOfMessages is less than {}", NSQS::TLimits::MinBatchSize)));
                return Nothing();
            }
            if (std::cmp_greater(maxNumberOfMessages, NSQS::TLimits::MaxBatchSize)) {
                ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("MaxNumberOfMessages is greater than {}", NSQS::TLimits::MaxBatchSize)));
                return Nothing();
            }
            if (this->QueueUrl_->Consumer.empty()) {
                ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Malformed QueueUrl")));
                return Nothing();
            }

            const TDuration waitTime = TDuration::Seconds(request.wait_time_seconds());

            const TDuration visibilityTimeout = request.has_visibility_timeout() ? TDuration::Seconds(request.visibility_timeout()) : TDuration::Seconds(NSQS::TLimits::VisibilityTimeout);
            if (visibilityTimeout > NSQS::TLimits::MaxVisibilityTimeout) {
                ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Visibility timeout is greater than {} hours", NSQS::TLimits::MaxVisibilityTimeout.Hours())));
                return Nothing();
            }

            for (const auto& messageAttributeName : request.message_attribute_names()) {
                auto attrCheck = NSQS::CheckMessageAttributeNameRequest(messageAttributeName);
                if (!attrCheck) {
                    ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", attrCheck.ErrorMessage)));
                    return Nothing();
                }
            }

            auto userToken = MakeIntrusive<NACLib::TUserToken>(this->Request_->GetSerializedToken());
            NKikimr::NPQ::NMLP::TReaderSettings settings{
                .DatabasePath = this->QueueUrl_->Database,
                .TopicName = FullTopicPath_,
                .Consumer = this->QueueUrl_->Consumer,
                .WaitTime = waitTime,
                .VisibilityTimeout = visibilityTimeout,
                .MaxNumberOfMessage = static_cast<ui32>(maxNumberOfMessages),
                .UncompressMessages = true,
                .UserToken = std::move(userToken),
            };
            return settings;
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse); // override for testing
                HFunc(NKikimr::NPQ::NMLP::TEvReadResponse, Handle);
                default:
                    TBase::StateWork(ev);
            }
        }



        Ydb::Ymq::V1::Message ConvertMessage(NKikimr::NPQ::NMLP::TEvReadResponse::TMessage&& message) const {
            Ydb::Ymq::V1::Message result;


            result.set_body(std::move(message.Data));
            AFL_ENSURE(message.Codec == Ydb::Topic::Codec::CODEC_RAW)("codec", Ydb::Topic::Codec_Name(message.Codec));
            result.set_m_d_5_of_body("m_d_5_of_body");
            result.set_m_d_5_of_message_attributes("");//
/*
            • ApproximateReceiveCount
        • ApproximateFirstReceiveTimestamp
        • MessageDeduplicationId
        • MessageGroupId
        • SenderId
        • SentTimestamp
        • SequenceNumber
*/
        result.message_attributes(); // XX
        result.set_message_id("Fake");///
        result.receipt_handle();



            return result;
        }


        void Handle(NKikimr::NPQ::NMLP::TEvReadResponse::TPtr& ev, const TActorContext& ctx) {
            ReaderActorId_ = {};
            auto& response = *ev->Get();
            switch (response.Status) {
                case Ydb::StatusIds::SUCCESS: {
                    break;
                }
                case Ydb::StatusIds::SCHEME_ERROR: {
                    ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("Error reading from topic: {}", response.ErrorDescription.ConstRef())));
                    return;
                }
                default: {
                    ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Error reading from topic: {}", response.ErrorDescription.ConstRef())));
                    return;
                }
            }

            Ydb::Ymq::V1::ReceiveMessageResult result;
            for (auto& message : response.Messages) {
                Ydb::Ymq::V1::Message m = ConvertMessage(std::move(message));
                *result.add_messages() = std::move(m);
            }

            return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }

        void Die(const TActorContext& ctx) override {
            if (ReaderActorId_) {
                ctx.Send(ReaderActorId_, new TEvents::TEvPoison);
            }
            this->TBase::Die(ctx);
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            Y_UNUSED(ev);
        }

    private:

        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    private:
        TActorId ReaderActorId_;
    };

    std::unique_ptr<NActors::IActor> CreateReceiveMessageActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TReceiveMessageActor>(msg);
    }

} // namespace NKikimr::NSqsTopic::V1
