#include "set_queue_attributes.h"
#include "actor.h"
#include "consumer_attributes.h"
#include "error.h"
#include "request.h"
#include "utils.h"

#include <ydb/core/http_proxy/events.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/error/error.h>
#include <ydb/services/sqs_topic/queue_url/consumer.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>
#include <ydb/services/sqs_topic/queue_url/holder/queue_url_holder.h>

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/core/util/proto_duration.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/services/sqs_topic/sqs_topic_proxy.h>

#include <ydb/public/api/grpc/draft/ydb_ymq_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/persqueue/public/mlp/mlp.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/services/sqs_topic/statuses.h>

#include <library/cpp/json/json_writer.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {
    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    template <class TProtoRequest>
    static std::expected<TRichQueueUrl, TString> ParseQueueUrlFromRequest(NKikimr::NGRpcService::IRequestOpCtx* request) {
        return ParseQueueUrl(GetRequest<TProtoRequest>(request).queue_url());
    }

    class TSetQueueAttributesActor:
        public TQueueUrlHolder,
        public TGrpcActorBase<TSetQueueAttributesActor, TEvSqsTopicSetQueueAttributesRequest>,
        public TCdcStreamCompatible
    {
    protected:
        using TBase = TGrpcActorBase<TSetQueueAttributesActor, TEvSqsTopicSetQueueAttributesRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

        static const inline TString Method = "SetQueueAttributes";

    public:
        TSetQueueAttributesActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrlFromRequest<TProtoRequest>(request))
            , TBase(request, TQueueUrlHolder::GetTopicPath().value_or(""))
        {
        }

        ~TSetQueueAttributesActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);

            const Ydb::Ymq::V1::SetQueueAttributesRequest& request = Request();
            if (request.queue_url().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!FormalValidQueueUrl()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            DescribeTopic(NACLib::UpdateRow);
            Become(&TSetQueueAttributesActor::StateWork);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
                hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
                hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
                default:
                    TBase::StateWork(ev);
            }
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&) {
            // TODO remove it
        }

        void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
            const auto* result = ev->Get();
            Y_ABORT_UNLESS(result->Topics.size() == 1);
            const auto& topicInfo = result->Topics.begin()->second;

            switch(topicInfo.Status) {
                case NDescriber::EStatus::SUCCESS:
                    break;
                case NDescriber::EStatus::NOT_TOPIC:
                    return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                        TStringBuilder() << "Queue name used by another scheme object"));
                case NDescriber::EStatus::NOT_FOUND:
                case NDescriber::EStatus::UNAUTHORIZED:
                    return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE,
                        "The specified queue doesn't exist"));
                case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                    return ReplyWithError(MakeError(NSQS::NErrors::ACCESS_DENIED,
                        "Access denied"));
                case NDescriber::EStatus::UNKNOWN_ERROR:
                    return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                        NDescriber::Description(topicInfo.RealPath, topicInfo.Status)));
            }

            PQGroup = topicInfo.Info->Description;
            SelfInfo = topicInfo.Self->Info;

            const auto& pqConfig = PQGroup.GetPQTabletConfig();
            const NKikimrPQ::TPQTabletConfig::TConsumer* foundConsumer = FindIfPtr(
                pqConfig.GetConsumers(),
                [this](const auto& c) { return c.GetName() == QueueUrl_->Consumer; }
            );

            if (!foundConsumer) {
                return ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE,
                    std::format("The specified queue doesn't exist (consumer: \"{}\")", QueueUrl_->Consumer.c_str())));
            }

            if (foundConsumer->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                    TStringBuilder() << "Queue cannot be modified: consumer '" << QueueUrl_->Consumer
                                     << "' is not a shared consumer"));
            }

            ExistingConsumer = *foundConsumer;

            const Ydb::Ymq::V1::SetQueueAttributesRequest& request = Request();
            const TString& queueName = QueueUrl_->TopicPath;
            if (auto cc = ParseQueueAttributes(request.attributes(), queueName, QueueUrl_->Consumer, this->Database, EConsumerAttributeUsageTarget::Alter); !cc.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", cc.error())));
            } else {
                NewQueueAttributes = std::move(cc).value();
            }

            if (auto check = ValidateLimits(NewQueueAttributes); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", check.error())));
            }

            if (auto check = ValidateFifoImmutability(); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", check.error())));
            }

            return SendAlterTopicRequest();
        }

        std::expected<void, std::string> ValidateFifoImmutability() const {
            bool existingFifo = ExistingConsumer.GetKeepMessageOrder();
            if (existingFifo != NewQueueAttributes.FifoQueue) {
                return std::unexpected(std::format(
                    "FifoQueue attribute cannot be changed. Current value: {}",
                    existingFifo ? "true" : "false"
                ));
            }
            return {};
        }

        void SendAlterTopicRequest() {
            Ydb::Topic::AlterTopicRequest topicRequest;
            topicRequest.set_path(TopicPath);

            if (NewQueueAttributes.ContentBasedDeduplication.Defined()) {
                topicRequest.set_set_content_based_deduplication(*NewQueueAttributes.ContentBasedDeduplication);
            }

            auto* consumer = topicRequest.add_alter_consumers();
            consumer->set_name(QueueUrl_->Consumer);

            auto* consumerType = consumer->mutable_alter_shared_consumer_type();
            if (NewQueueAttributes.DefaultProcessingTimeout.Defined()) {
                SetDuration(*NewQueueAttributes.DefaultProcessingTimeout,
                    *consumerType->mutable_set_default_processing_timeout());
            }
            if (NewQueueAttributes.ReceiveMessageDelay.Defined()) {
                SetDuration(*NewQueueAttributes.ReceiveMessageDelay,
                    *consumerType->mutable_set_receive_message_delay());
            }
            if (NewQueueAttributes.ReceiveMessageWaitTime.Defined()) {
                SetDuration(*NewQueueAttributes.ReceiveMessageWaitTime,
                    *consumerType->mutable_set_receive_message_wait_time());
            }
            if (NewQueueAttributes.MaxReceiveCount.Defined()) {
                consumerType->mutable_alter_dead_letter_policy()->set_set_enabled(true);
                consumerType->mutable_alter_dead_letter_policy()->mutable_alter_condition()
                    ->set_set_max_processing_attempts(*NewQueueAttributes.MaxReceiveCount);
            }
            if (NewQueueAttributes.DeadLetterQueue.Defined()) {
                consumerType->mutable_alter_dead_letter_policy()->set_set_enabled(true);
                consumerType->mutable_alter_dead_letter_policy()->mutable_set_move_action()
                    ->set_dead_letter_queue(*NewQueueAttributes.DeadLetterQueue);
            }

            // should we increase global limit?
            if (NewQueueAttributes.MessageRetentionPeriod.Defined()) {
                ui64 requestedRetentionSeconds = NewQueueAttributes.MessageRetentionPeriod->Seconds();
                ui64 currentRetentionSeconds = PQGroup.GetPQTabletConfig().GetPartitionConfig().GetLifetimeSeconds();
                if (requestedRetentionSeconds > currentRetentionSeconds) {
                    SetDuration(*NewQueueAttributes.MessageRetentionPeriod, *topicRequest.mutable_set_retention_period());
                }
                SetDuration(*NewQueueAttributes.MessageRetentionPeriod, *consumer->mutable_set_availability_period());
            }

            RegisterWithSameMailbox(NPQ::NSchema::CreateAlterTopicActor(SelfId(), {
                .Database = this->Database,
                .PeerName = Request_->GetPeerName(),
                .Request = std::move(topicRequest),
                .UserToken = this->GetUserToken(),
            }));
        }

        void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
            const auto* result = ev->Get();
            if (result->Status != Ydb::StatusIds::SUCCESS) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, result->ErrorMessage));
            }
            return ReplyAndDie(ActorContext());
        }

        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::SetQueueAttributesResult result;
            return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }

    protected:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    private:
        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
        NKikimrPQ::TPQTabletConfig::TConsumer ExistingConsumer;
        TQueueAttributes NewQueueAttributes;
    };

    std::unique_ptr<NActors::IActor> CreateSetQueueAttributesActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TSetQueueAttributesActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
