#include "get_queue_url.h"
#include "actor.h"
#include "config.h"
#include "error.h"
#include "limits.h"
#include "request.h"
#include "utils.h"

#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <ydb/core/http_proxy/events.h>
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

#include <library/cpp/json/json_writer.h>


using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {
    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    class TGetQueueUrlActor
        : public TGrpcActorBase<TGetQueueUrlActor, TEvSqsTopicGetQueueUrlRequest>
    {
    protected:
        using TBase = TGrpcActorBase<TGetQueueUrlActor, TEvSqsTopicGetQueueUrlRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

        static const inline TString Method = "GetQueueUrl";

    public:
        TGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request, ToString(SplitExtendedQueueName(GetRequest<TProtoRequest>(request).queue_name()).QueueName))
        {
            auto split = SplitExtendedQueueName(GetRequest<TProtoRequest>(request).queue_name());
            QueueName = ToString(split.QueueName);
            ConsumerName = split.Consumer.empty() ? GetDefaultSqsConsumerName() : ToString(split.Consumer);
        }

        ~TGetQueueUrlActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            const Ydb::Ymq::V1::GetQueueUrlRequest& request = Request();
            if (request.queue_name().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueName parameter."));
            }
            if (!Request_->GetDatabaseName()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Request without database is forbidden"));
            }
            if (auto check = ValidateQueueName(QueueName, true); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Invalid queue name: {}", check.error())));
            }
            DescribeTopic(NACLib::DescribeSchema);
            Become(&TGetQueueUrlActor::TBase::StateWork);
        }

        void OnTopicDescribed(const NPQ::NDescriber::TTopicInfo& topicInfo) {
            AFL_ENSURE(topicInfo.Info)("path", this->TopicPath);
            AFL_ENSURE(topicInfo.Self)("path", this->TopicPath);
            PQGroup = topicInfo.Info->Description;
            SelfInfo = topicInfo.Self->Info;
            ConsumerConfig = GetConsumerConfig(PQGroup.GetPQTabletConfig(), ConsumerName, ActorContext());
            if (!ConsumerConfig) {
                return ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("The specified queue doesn't exist (consumer: \"{}\")", ConsumerName.c_str())));
            }
            if (ConsumerConfig.Defined() && ConsumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                return ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("The specified queue doesn't exist (consumer \"{}\" is not a shared consumer)", ConsumerName.c_str())));
            }
            this->ChargeRequestUnits(ActorContext());
        }

        ui64 GetRUCost() override {
            return NBilling::RoundRu(NBilling::DEFAULT_REQUEST_COST);
        }

        void OnRequestUnitsCharged(const TActorContext& ctx) {
            ReplyAndDie(ctx);
        }

        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::GetQueueUrlResult result;

            const TString consumerInUrl = NPersQueue::ConvertOldConsumerName(ConsumerConfig->GetName(), ctx);

            const TRichQueueUrl queueUrl{
                .Database = this->Database,
                .TopicPath = this->TopicPath,
                .Consumer = consumerInUrl,
                .Fifo = QueueName.EndsWith(".fifo"),
            };

            TString url = MakeQueueUrl(queueUrl, Request_.get());
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
        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
        TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> ConsumerConfig;
    };

    std::unique_ptr<NActors::IActor> CreateGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TGetQueueUrlActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
