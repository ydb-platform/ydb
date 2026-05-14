#include "delete_queue.h"
#include "actor.h"
#include "config.h"
#include "error.h"
#include "request.h"

#include <ydb/core/http_proxy/events.h>
#include <ydb/core/persqueue/public/schema/schema.h>
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
            TBase::Bootstrap(ctx);

            const Ydb::Ymq::V1::DeleteQueueRequest& request = Request();
            if (request.queue_url().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!FormalValidQueueUrl()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            DescribeTopic(NACLib::UpdateRow); // TODO почему update row?
            Become(&TDeleteQueueActor::StateWork);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
                hFunc(NPQ::NSchema::TEvDropTopicResponse, Handle);
                hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
                default:
                    TBase::StateWork(ev);
            }
        }

        void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
            const auto* result = ev->Get();
            Y_ABORT_UNLESS(result->Topics.size() == 1);
            const auto& topicInfo = result->Topics.begin()->second;

            switch(topicInfo.Status) {
                case NDescriber::EStatus::SUCCESS: {
                    if (topicInfo.CdcStream) {
                        return ReplyWithError(MakeError(NSQS::NErrors::UNSUPPORTED_OPERATION,
                            "Deleting the changefeed is not supported"));
                    }
                    break;
                }
                case NDescriber::EStatus::NOT_TOPIC:
                    return ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE,
                        "Queue name used by another scheme object"));
                case NDescriber::EStatus::NOT_FOUND:
                case NDescriber::EStatus::UNAUTHORIZED:
                    return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE,
                        "The specified queue doesn't exist"));
                case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                    return ReplyWithError(MakeError(NSQS::NErrors::ACCESS_DENIED,
                        "Access denied"));
                case NDescriber::EStatus::UNKNOWN_ERROR:
                    return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                        "Failed to describe topic"));
            }

            const auto& pqGroup = topicInfo.Info->Description;

            auto consumerConfig = GetConsumerConfig(pqGroup.GetPQTabletConfig(), QueueUrl_->Consumer);
            if (!consumerConfig) {
                return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE,
                    std::format("The specified queue doesn't exist (consumer: \"{}\")", QueueUrl_->Consumer.c_str())));
            }
            if (consumerConfig.Defined() && consumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE,
                    std::format("The specified queue doesn't exist (consumer \"{}\" is not a shared consumer)", QueueUrl_->Consumer.c_str())));
            }

            if (pqGroup.GetPQTabletConfig().ConsumersSize() <= 1) {
                this->RegisterWithSameMailbox(NPQ::NSchema::CreateDropTopicActor(SelfId(), {
                    .Database = Database,
                    .PeerName = this->Request_->GetPeerName(),
                    .Path = topicInfo.RealPath,
                    .UserToken = this->GetUserToken(),
                }));
            } else {
                Ydb::Topic::AlterTopicRequest request;
                request.set_path(topicInfo.RealPath);
                request.add_drop_consumers(QueueUrl_->Consumer);
                this->RegisterWithSameMailbox(NPQ::NSchema::CreateAlterTopicActor(SelfId(), {
                    .Database = Database,
                    .PeerName = this->Request_->GetPeerName(),
                    .Request = std::move(request),
                    .UserToken = this->GetUserToken(),
                }));
            }
        }

        void Handle(NPQ::NSchema::TEvDropTopicResponse::TPtr& ev) {
            const auto* result = ev->Get();
            if (result->Status != Ydb::StatusIds::SUCCESS) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, result->ErrorMessage));
            }
            this->Reply(Ydb::StatusIds::SUCCESS);
        }

        void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
            const auto* result = ev->Get();
            if (result->Status != Ydb::StatusIds::SUCCESS) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, result->ErrorMessage));
            }
            this->Reply(Ydb::StatusIds::SUCCESS);
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&) {
            // TODO remove it
        }

    protected:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }
    };

    std::unique_ptr<NActors::IActor> CreateDeleteQueueActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TDeleteQueueActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
