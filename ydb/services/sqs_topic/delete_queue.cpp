#include "delete_queue.h"
#include "actor.h"
#include "config.h"
#include "error.h"
#include "request.h"

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

    namespace {
        enum class EModifiedEntity {
            Topic,
            Consumer,
        };
    } // namespace

    template <class TProtoRequest>
    static std::expected<TRichQueueUrl, TString> ParseQueueUrlFromRequest(NKikimr::NGRpcService::IRequestOpCtx* request) {
        return ParseQueueUrl(GetRequest<TProtoRequest>(request).queue_url());
    }

    class TDeleteQueueActor: public TQueueUrlHolder, public TGrpcActorBase<TDeleteQueueActor, TEvSqsTopicDeleteQueueRequest> {
    protected:
        using TBase = TGrpcActorBase<TDeleteQueueActor, TEvSqsTopicDeleteQueueRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

        static const inline TString Method = "DeleteQueue";

    public:
        TDeleteQueueActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrlFromRequest<TProtoRequest>(request))
            , TBase(request, TQueueUrlHolder::GetTopicPath().value_or(""))
        {
        }

        ~TDeleteQueueActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            CheckAccessWithWriteTopicPermission = true;
            TBase::Bootstrap(ctx);

            const Ydb::Ymq::V1::DeleteQueueRequest& request = Request();
            if (request.queue_url().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!FormalValidQueueUrl()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            SendDescribeProposeRequest(ctx);
            Become(&TDeleteQueueActor::StateWork);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
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
                    return ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, TStringBuilder() << "Queue name used by another scheme object"));
                }
                // ok
            } else if (response.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown) {
                return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("The specified queue doesn't exist")));
            } else {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                                                TStringBuilder() << "Failed to describe topic: " << response.Status));
            }
            Y_ABORT_UNLESS(response.PQGroupInfo);
            PQGroup = response.PQGroupInfo->Description;
            SelfInfo = response.Self->Info;
            ConsumerConfig = GetConsumerConfig(PQGroup.GetPQTabletConfig(), QueueUrl_->Consumer);
            if (!ConsumerConfig) {
                return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("The specified queue doesn't exist (consumer: \"{}\")", QueueUrl_->Consumer.c_str())));
            }
            if (ConsumerConfig.Defined() && ConsumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("The specified queue doesn't exist (consumer \"{}\" is not a shared consumer)", QueueUrl_->Consumer.c_str())));
            }
            RemoveEntity = (PQGroup.GetPQTabletConfig().ConsumersSize() <= 1) ? EModifiedEntity::Topic : EModifiedEntity::Consumer;
            SendProposeRequest(ActorContext());
        }

        void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal,
                                const TActorContext& ctx,
                                const TString& workingDir,
                                const TString& name) {
            NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
            modifyScheme.SetWorkingDir(workingDir);
            if (RemoveEntity == EModifiedEntity::Topic) {
                modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
                modifyScheme.MutableDrop()->SetName(name);
            } else {
                Y_ASSERT(RemoveEntity == EModifiedEntity::Consumer);
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
                auto removeConsumerPred = [this](const auto& consumer) {
                    return consumer.GetName() == QueueUrl_->Consumer;
                };
                EraseIf(*pqDescr->MutablePQTabletConfig()->MutableConsumers(), removeConsumerPred);
                pqDescr->MutablePQTabletConfig()->ClearPartitionKeySchema();
                pqDescr->ClearTotalGroupCount();
                TString error;
                Ydb::StatusIds::StatusCode code = NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(topicRequest, *pqDescr, AppData(ctx), error, false);
                if (code != Ydb::StatusIds::SUCCESS) {
                    return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Invalid parameters: {}", error.ConstRef())));
                }
            }
        }

    protected:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    private:
        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
        TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> ConsumerConfig;
        EModifiedEntity RemoveEntity = EModifiedEntity::Topic;
    };

    std::unique_ptr<NActors::IActor> CreateDeleteQueueActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TDeleteQueueActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
