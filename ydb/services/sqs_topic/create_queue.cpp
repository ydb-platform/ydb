#include "create_queue.h"
#include "actor.h"
#include "consumer_attributes.h"
#include "error.h"
#include "limits.h"
#include "request.h"
#include "utils.h"

#include <ydb/core/http_proxy/events.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/error/error.h>
#include <ydb/services/sqs_topic/queue_url/consumer.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>
#include <ydb/services/sqs_topic/queue_url/arn.h>
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

    class TCreateQueueActor:
        public TGrpcActorBase<TCreateQueueActor, TEvSqsTopicCreateQueueRequest>
    {
    protected:
        using TBase = TGrpcActorBase<TCreateQueueActor, TEvSqsTopicCreateQueueRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

        static const inline TString Method = "CreateQueue";

    public:
        TCreateQueueActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request, ToString(SplitExtendedQueueName(GetRequest<TProtoRequest>(request).queue_name()).QueueName))
        {
            auto split = SplitExtendedQueueName(GetRequest<TProtoRequest>(request).queue_name());
            QueueName = ToString(split.QueueName);
            ConsumerName = split.Consumer.empty() ? GetDefaultSqsConsumerName() : ToString(split.Consumer);
        }

        ~TCreateQueueActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            const Ydb::Ymq::V1::CreateQueueRequest& request = Request();
            if (request.queue_name().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueName parameter."));
            }
            if (!Request_->GetDatabaseName()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Request without database is forbidden"));
            }
            if (auto check = ValidateQueueName(QueueName, false); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Invalid queue name: {}", check.error())));
            }
            if (auto cc = ParseQueueAttributes(request.attributes(), QueueName, ConsumerName, this->Database, EConsumerAttributeUsageTarget::Create); !cc.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", cc.error())));
            } else {
                QueueAttributes = std::move(cc).value();
            }
            if (auto check = ValidateLimits(QueueAttributes); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", check.error())));
            }
            DescribeTopic(NACLib::UpdateRow);
            Become(&TCreateQueueActor::StateWork);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
                hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
                hFunc(NPQ::NSchema::TEvCreateTopicResponse, Handle);
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
                case NDescriber::EStatus::SUCCESS: {
                    if (topicInfo.CdcStream) {
                        return ReplyWithError(MakeError(NSQS::NErrors::UNSUPPORTED_OPERATION,
                            "Creating the changefeed is not supported"));
                    }

                    PQGroup = topicInfo.Info->Description;
                    SelfInfo = topicInfo.Self->Info;

                    return HandleExistingTopic(ActorContext());
                }
                case NDescriber::EStatus::NOT_TOPIC:
                    return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                        TStringBuilder() << "Queue name used by another scheme object"));
                case NDescriber::EStatus::NOT_FOUND:
                    return CreateTopic();
                case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                    return ReplyWithError(MakeError(NSQS::NErrors::ACCESS_DENIED,
                        "Access denied"));
                case NDescriber::EStatus::UNAUTHORIZED:
                case NDescriber::EStatus::UNKNOWN_ERROR:
                    return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                        NDescriber::Description(topicInfo.RealPath, topicInfo.Status)));
            }
        }

        void CreateTopic() {
            Ydb::Topic::CreateTopicRequest topicRequest;
            topicRequest.set_path(TopicPath);

            {
                auto* partitioningSettings = topicRequest.mutable_partitioning_settings();
                auto* autoPartitioning = partitioningSettings->mutable_auto_partitioning_settings();

                autoPartitioning->set_strategy(::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
                partitioningSettings->set_min_active_partitions(DEFAULT_MIN_PARTITION_COUNT);
                partitioningSettings->set_max_active_partitions(DEFAULT_MAX_PARTITION_COUNT);
            }

            SetDuration(QueueAttributes.MessageRetentionPeriod.GetOrElse(DEFAULT_MESSAGE_RETENTION_PERIOD), *topicRequest.mutable_retention_period());
            topicRequest.set_partition_write_speed_bytes_per_second(1_MB);
            topicRequest.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
            topicRequest.set_content_based_deduplication(QueueAttributes.ContentBasedDeduplication.GetOrElse(false));

            AddConsumerToRequest(topicRequest.add_consumers());

            this->RegisterWithSameMailbox(NPQ::NSchema::CreateCreateTopicActor(SelfId(), {
                .Database = this->Database,
                .PeerName = Request_->GetPeerName(),
                .Request = std::move(topicRequest),
                .UserToken = this->GetUserToken(),
            }));
        }

        void AddConsumer() {
            Ydb::Topic::AlterTopicRequest topicRequest;
            topicRequest.set_path(TopicPath);

            AddConsumerToRequest(topicRequest.add_add_consumers());

            this->RegisterWithSameMailbox(NPQ::NSchema::CreateAlterTopicActor(SelfId(), {
                .Database = this->Database,
                .PeerName = Request_->GetPeerName(),
                .Request = std::move(topicRequest),
                .UserToken = this->GetUserToken(),
            }));
        }

        void AddConsumerToRequest(Ydb::Topic::Consumer* consumer) {
            consumer->set_name(ConsumerName);
            auto* consumerType = consumer->mutable_shared_consumer_type();
            consumerType->set_keep_messages_order(QueueAttributes.FifoQueue);
            SetDuration(QueueAttributes.DefaultProcessingTimeout.GetOrElse(TDuration::Seconds(30)), *consumerType->mutable_default_processing_timeout());
            SetDuration(QueueAttributes.ReceiveMessageDelay.GetOrElse(TDuration::Seconds(0)), *consumerType->mutable_receive_message_delay());
            SetDuration(QueueAttributes.ReceiveMessageWaitTime.GetOrElse(TDuration::Seconds(0)), *consumerType->mutable_receive_message_wait_time());
            if (QueueAttributes.MessageRetentionPeriod.Defined()) {
                SetDuration(*QueueAttributes.MessageRetentionPeriod, *consumer->mutable_availability_period());
            }

            consumerType->mutable_dead_letter_policy()->set_enabled(QueueAttributes.DeadLetterQueue.Defined() || QueueAttributes.MaxReceiveCount.Defined());
            if (QueueAttributes.MaxReceiveCount.Defined()) {
                consumerType->mutable_dead_letter_policy()->mutable_condition()->set_max_processing_attempts(*QueueAttributes.MaxReceiveCount);
            }
            if (QueueAttributes.DeadLetterQueue.Defined()) {
                consumerType->mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue(*QueueAttributes.DeadLetterQueue);
            }
        }

        void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
            const auto* result = ev->Get();
            if (result->Status != Ydb::StatusIds::SUCCESS) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, result->ErrorMessage));
            }
            return ReplyAndDie(ActorContext());
        }

        void Handle(NPQ::NSchema::TEvCreateTopicResponse::TPtr& ev) {
            const auto* result = ev->Get();
            if (result->Status != Ydb::StatusIds::SUCCESS) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, result->ErrorMessage));
            }
            return ReplyAndDie(ActorContext());
        }

        void HandleExistingTopic(const TActorContext& ctx) {
            const auto& pqConfig = PQGroup.GetPQTabletConfig();
            const NKikimrPQ::TPQTabletConfig::TConsumer* foundConsumer = FindIfPtr(pqConfig.GetConsumers(), [this](const auto& c) { return c.GetName() == ConsumerName; });
            if (!foundConsumer) {
                return AddConsumer();
            }

            if (foundConsumer->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                                                TStringBuilder() << "Queue cannot be created: consumer '" << ConsumerName
                                                                 << "' exists but is not a shared consumer"));
            }

            auto comparison = CompareWithExistingQueueAttributes(pqConfig, *foundConsumer, QueueAttributes);
            if (!comparison.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::VALIDATION_ERROR,
                                                TStringBuilder() << "Queue attributes mismatch: " << comparison.error()));
            }

            return ReplyAndDie(ctx);
        }


        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::CreateQueueResult result;

            const TRichQueueUrl queueUrl{
                .Database = this->Database,
                .TopicPath = this->TopicPath,
                .Consumer = this->ConsumerName,
                .Fifo = QueueAttributes.FifoQueue,
            };

            TString path = PackQueueUrlPath(queueUrl);
            TString url = TStringBuilder() << GetEndpoint(Cfg()) << path;
            result.set_queue_url(std::move(url));

            return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }

    protected:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    private:
        TString QueueName;
        TString ConsumerName;
        TQueueAttributes QueueAttributes;
        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
    };

    std::unique_ptr<NActors::IActor> CreateCreateQueueActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TCreateQueueActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
