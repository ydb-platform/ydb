#include "create_queue.h"
#include "actor.h"
#include "consumer_attributes.h"
#include "error.h"
#include "limits.h"
#include "request.h"
#include "utils.h"

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
            CheckAccessWithWriteTopicPermission = true;
            TBase::Bootstrap(ctx);
            const Ydb::Ymq::V1::CreateQueueRequest& request = Request();
            if (request.queue_name().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueName parameter."));
            }
            if (!Request_->GetDatabaseName()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Request without database is forbidden"));
            }
            if (auto check = ValidateQueueName(QueueName); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Invalid queue name: {}", check.error())));
            }
            if (auto cc = ParseConsumerAttributes(request.attributes(), QueueName, ConsumerName, this->Database, EConsumerAttributeUsageTarget::Create); !cc.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", cc.error())));
            } else {
                ConsumerConfig = std::move(cc).value();
            }
            if (auto check = ValidateLimits(ConsumerConfig); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", check.error())));
            }
            SendDescribeProposeRequest(ctx);
            Become(&TCreateQueueActor::StateWork);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
                HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
                default:
                    TBase::StateWork(ev);
            }
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            Y_ABORT_UNLESS(result->ResultSet.size() == 1);
            const auto& response = result->ResultSet.front();

            if (response.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                if (response.Kind != NSchemeCache::TSchemeCacheNavigate::KindTopic) {
                    return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, TStringBuilder() << "Queue name used by another scheme object"));
                }
                Y_ABORT_UNLESS(response.PQGroupInfo);
                PQGroup = response.PQGroupInfo->Description;
                SelfInfo = response.Self->Info;

                return HandleExistingTopic(ActorContext());
            } else if (response.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown) {
                return SendProposeRequest(ActorContext());
            } else {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                                                TStringBuilder() << "Failed to describe topic: " << response.Status));
            }
        }

        void HandleExistingTopic(const TActorContext& ctx) {
            const auto& pqConfig = PQGroup.GetPQTabletConfig();
            const NKikimrPQ::TPQTabletConfig::TConsumer* foundConsumer = FindIfPtr(pqConfig.GetConsumers(), [this](const auto& c) { return c.GetName() == ConsumerName; });
            if (!foundConsumer) {
                AddingConsumer = true;
                return SendProposeRequest(ctx);
            }

            if (foundConsumer->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                                                TStringBuilder() << "Queue cannot be created: consumer '" << ConsumerName
                                                                 << "' exists but is not a shared consumer"));
            }

            auto comparison = CompareWithExistingQueueAttributes(pqConfig, *foundConsumer, ConsumerConfig);
            if (!comparison.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::VALIDATION_ERROR,
                                                TStringBuilder() << "Queue attributes mismatch: " << comparison.error()));
            }

            return ReplyAndDie(ctx);
        }

        void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal,
                                const TActorContext& ctx,
                                const TString& workingDir,
                                const TString& name) {
            NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
            modifyScheme.SetWorkingDir(workingDir);

            if (!AddingConsumer) {
                Ydb::Topic::CreateTopicRequest topicRequest;
                {
                    auto* partitioningSettings = topicRequest.mutable_partitioning_settings();
                    auto* autoPartitioning = partitioningSettings->mutable_auto_partitioning_settings();

                    autoPartitioning->set_strategy(::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
                    partitioningSettings->set_min_active_partitions(DEFAULT_MIN_PARTITION_COUNT);
                    partitioningSettings->set_max_active_partitions(DEFAULT_MAX_PARTITION_COUNT);
                }

                topicRequest.mutable_retention_period()->set_seconds(ConsumerConfig.MessageRetentionPeriod.GetOrElse(DEFAULT_MESSAGE_RETENTION_PERIOD).Seconds());
                topicRequest.set_partition_write_speed_bytes_per_second(1_MB);
                topicRequest.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);

                auto pqDescr = modifyScheme.MutableCreatePersQueueGroup();
                pqDescr->MutablePQTabletConfig()->AddConsumers()->CopyFrom(ConsumerConfig.Consumer);
                TString error;
                TYdbPqCodes codes = NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(name, topicRequest, modifyScheme, AppData(ctx), error,
                                                                                    workingDir, proposal.Record.GetDatabaseName());
                if (codes.YdbCode != Ydb::StatusIds::SUCCESS) {
                    return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Invalid parameters: {}", error.ConstRef())));
                }
            } else {
                modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
                {
                    auto applyIf = modifyScheme.AddApplyIf();
                    applyIf->SetPathId(SelfInfo.GetPathId());
                    applyIf->SetPathVersion(SelfInfo.GetPathVersion());
                }

                Ydb::Topic::AlterTopicRequest topicRequest;
                auto* pqDescr = modifyScheme.MutableAlterPersQueueGroup();
                pqDescr->SetName(name);
                pqDescr->MutablePQTabletConfig()->CopyFrom(PQGroup.GetPQTabletConfig());
                pqDescr->MutablePQTabletConfig()->AddConsumers()->CopyFrom(ConsumerConfig.Consumer);
                pqDescr->MutablePQTabletConfig()->ClearPartitionKeySchema();
                pqDescr->ClearTotalGroupCount();
                TString error;
                Ydb::StatusIds::StatusCode code = NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(topicRequest, *pqDescr, AppData(ctx), error, false);
                if (code != Ydb::StatusIds::SUCCESS) {
                    return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Invalid parameters: {}", error.ConstRef())));
                }
            }
        }

        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
            auto msg = ev->Get();
            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());

            if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
                if (msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists) {
                    // Topic was created concurrently - describe it again
                    if (RetryCount++ < 1) {
                        return SendDescribeProposeRequest(ctx);
                    } else {
                        ReplyWithError(
                            MakeError(NSQS::NErrors::INTERNAL_FAILURE, TStringBuilder() << "Queue already exists"));
                    }
                } else if (msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusSuccess) {
                    return ReplyAndDie(ctx);
                }
            }
            return TBase::TBase::TBase::Handle(ev, ctx);
        }


    void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) override {
        Y_UNUSED(ev);
        ReplyAndDie(ctx);
    }

        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::CreateQueueResult result;

            const TRichQueueUrl queueUrl{
                .Database = this->Database,
                .TopicPath = this->TopicPath,
                .Consumer = this->ConsumerName,
                .Fifo = QueueName.EndsWith(".fifo"),
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
        TConsumerAttributes ConsumerConfig;
        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
        int RetryCount = 0;
        bool AddingConsumer = false;
    };

    std::unique_ptr<NActors::IActor> CreateCreateQueueActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TCreateQueueActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
