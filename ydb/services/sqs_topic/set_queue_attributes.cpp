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
        public TGrpcActorBase<TSetQueueAttributesActor, TEvSqsTopicSetQueueAttributesRequest>
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
            CheckAccessWithWriteTopicPermission = true;
            TBase::Bootstrap(ctx);

            const Ydb::Ymq::V1::SetQueueAttributesRequest& request = Request();
            if (request.queue_url().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!FormalValidQueueUrl()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            SendDescribeProposeRequest(ctx);
            Become(&TSetQueueAttributesActor::StateWork);
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
            if (response.Kind != NSchemeCache::TSchemeCacheNavigate::KindTopic) {
                return ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE,
                                                std::format("The specified queue doesn't exist")));
            }

            Y_ABORT_UNLESS(response.PQGroupInfo);
            PQGroup = response.PQGroupInfo->Description;
            SelfInfo = response.Self->Info;

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
            if (auto cc = ParseConsumerAttributes(request.attributes(), queueName, QueueUrl_->Consumer, this->Database, EConsumerAttributeUsageTarget::Alter); !cc.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", cc.error())));
            } else {
                NewConsumerConfig = std::move(cc).value();
            }

            if (auto check = ValidateLimits(NewConsumerConfig); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", check.error())));
            }

            if (auto check = ValidateFifoImmutability(); !check.has_value()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("{}", check.error())));
            }

            return SendAlterTopicRequest(ActorContext());
        }

        std::expected<void, std::string> ValidateFifoImmutability() const {
            bool existingFifo = ExistingConsumer.GetKeepMessageOrder();
            if (NewConsumerConfig.Consumer.HasKeepMessageOrder()) {
                bool newFifo = NewConsumerConfig.Consumer.GetKeepMessageOrder();
                if (existingFifo != newFifo) {
                    return std::unexpected(std::format(
                        "FifoQueue attribute cannot be changed. Current value: {}",
                        existingFifo ? "true" : "false"
                    ));
                }
            }
            return {};
        }

        void SendAlterTopicRequest(const TActorContext& ctx) {
            std::pair<TString, TString> pathPair;
            try {
                pathPair = NKikimr::NGRpcService::SplitPath(TQueueUrlHolder::FullTopicPath_);
            } catch (const std::exception& ex) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, ex.what()));
            }

            const auto& workingDir = pathPair.first;

            auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
            SetDatabase(proposal.get(), *this->Request_);
            SetPeerName(proposal.get(), *this->Request_);

            if (!this->Request_->GetSerializedToken().empty()) {
                proposal->Record.SetUserToken(this->Request_->GetSerializedToken());
            }

            NKikimrSchemeOp::TModifyScheme& modifyScheme = *proposal->Record.MutableTransaction()->MutableModifyScheme();
            modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
            modifyScheme.SetWorkingDir(workingDir);

            auto* alterConfig = modifyScheme.MutableAlterPersQueueGroup();
            alterConfig->CopyFrom(PQGroup);
            alterConfig->ClearTotalGroupCount();
            alterConfig->MutablePQTabletConfig()->ClearPartitionKeySchema();

            auto applyIf = modifyScheme.AddApplyIf();
            applyIf->SetPathId(SelfInfo.GetPathId());
            applyIf->SetPathVersion(SelfInfo.GetPathVersion());

            auto* pqTabletConfig = alterConfig->MutablePQTabletConfig();
            NKikimrPQ::TPQTabletConfig::TConsumer* consumerToModify = nullptr;
            for (size_t i = 0; i < pqTabletConfig->ConsumersSize(); ++i) {
                if (pqTabletConfig->GetConsumers(i).GetName() == QueueUrl_->Consumer) {
                    consumerToModify = pqTabletConfig->MutableConsumers(i);
                    break;
                }
            }

            if (!consumerToModify) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Consumer not found in config"));
            }

            ApplyNewAttributes(consumerToModify);

            // should we increase global limit?
            if (NewConsumerConfig.MessageRetentionPeriod.Defined()) {
                ui64 requestedRetentionSeconds = NewConsumerConfig.MessageRetentionPeriod->Seconds();
                ui64 currentRetentionSeconds = PQGroup.GetPQTabletConfig().GetPartitionConfig().GetLifetimeSeconds();
                if (requestedRetentionSeconds > currentRetentionSeconds) {
                    pqTabletConfig->MutablePartitionConfig()->SetLifetimeSeconds(requestedRetentionSeconds);
                }
                consumerToModify->SetAvailabilityPeriodMs(requestedRetentionSeconds * 1000);
            }

            ctx.Send(MakeTxProxyID(), proposal.release());
        }

        void ApplyNewAttributes(NKikimrPQ::TPQTabletConfig::TConsumer* consumer) {
            const auto& newConsumer = NewConsumerConfig.Consumer;

            if (newConsumer.HasAvailabilityPeriodMs()) {
                consumer->SetAvailabilityPeriodMs(newConsumer.GetAvailabilityPeriodMs());
            }
            if (newConsumer.HasDefaultProcessingTimeoutSeconds()) {
                consumer->SetDefaultProcessingTimeoutSeconds(newConsumer.GetDefaultProcessingTimeoutSeconds());
            }
            if (newConsumer.HasDefaultDelayMessageTimeMs()) {
                consumer->SetDefaultDelayMessageTimeMs(newConsumer.GetDefaultDelayMessageTimeMs());
            }
            if (newConsumer.HasDefaultReceiveMessageWaitTimeMs()) {
                consumer->SetDefaultReceiveMessageWaitTimeMs(newConsumer.GetDefaultReceiveMessageWaitTimeMs());
            }
            if (newConsumer.HasContentBasedDeduplication()) {
                consumer->SetContentBasedDeduplication(newConsumer.GetContentBasedDeduplication());
            }
            if (newConsumer.HasMaxProcessingAttempts()) {
                consumer->SetMaxProcessingAttempts(newConsumer.GetMaxProcessingAttempts());
            }
            if (newConsumer.HasDeadLetterPolicyEnabled()) {
                consumer->SetDeadLetterPolicyEnabled(newConsumer.GetDeadLetterPolicyEnabled());
            }
            if (newConsumer.HasDeadLetterPolicy()) {
                consumer->SetDeadLetterPolicy(newConsumer.GetDeadLetterPolicy());
            }
            if (newConsumer.HasDeadLetterQueue()) {
                consumer->SetDeadLetterQueue(newConsumer.GetDeadLetterQueue());
            }
        }

        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
            auto msg = ev->Get();
            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());

            if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
                if (msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusSuccess) {
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
        TConsumerAttributes NewConsumerConfig;
    };

    std::unique_ptr<NActors::IActor> CreateSetQueueAttributesActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TSetQueueAttributesActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
