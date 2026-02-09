#include "purge_queue.h"
#include "actor.h"
#include "error.h"
#include "request.h"
#include "receipt.h"
#include "utils.h"

#include <ydb/core/http_proxy/events.h>
#include <ydb/core/ymq/error/error.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>
#include <ydb/services/sqs_topic/queue_url/holder/queue_url_holder.h>

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/sqs.pb.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/services/sqs_topic/sqs_topic_proxy.h>

#include <ydb/public/api/grpc/draft/ydb_ymq_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/persqueue/public/mlp/mlp.h>

#include <ydb/library/actors/core/log.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {
    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;


    class TPurgeQueueActor: public TQueueUrlHolder,
                            public TGrpcActorBase<TPurgeQueueActor, TEvSqsTopicPurgeQueueRequest> {
    protected:
        using TBase = TGrpcActorBase<TPurgeQueueActor, TEvSqsTopicPurgeQueueRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TPurgeQueueActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrl(GetRequest<TProtoRequest>(request).queue_url()))
            , TBase(request, TQueueUrlHolder::GetTopicPath().value_or(""))
        {
        }

        ~TPurgeQueueActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);

            if (this->Request().queue_url().empty()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!QueueUrl_.has_value()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            TMaybe purgeSettings = MakePurgerSettings();
            if (!purgeSettings.Defined()) {
                return;
            }

            std::unique_ptr<IActor> actorPtr{NKikimr::NPQ::NMLP::CreatePurger(this->SelfId(), std::move(*purgeSettings))};
            ReaderActorId_ = ctx.RegisterWithSameMailbox(actorPtr.release());
            this->Become(&TPurgeQueueActor::StateWork);
        }

        TMaybe<NKikimr::NPQ::NMLP::TPurgerSettings> MakePurgerSettings() {
            if (this->QueueUrl_->Consumer.empty()) {
                ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("Malformed QueueUrl")));
                return Nothing();
            }

            auto userToken = MakeIntrusive<NACLib::TUserToken>(this->Request_->GetSerializedToken());
            NKikimr::NPQ::NMLP::TPurgerSettings settings{
                .DatabasePath = this->QueueUrl_->Database,
                .TopicName = FullTopicPath_,
                .Consumer = this->QueueUrl_->Consumer,
                .UserToken = std::move(userToken),
            };
            return settings;
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse); // override for testing
                HFunc(NKikimr::NPQ::NMLP::TEvPurgeResponse, Handle);
                default:
                    TBase::StateWork(ev);
            }
        }

        void Handle(NKikimr::NPQ::NMLP::TEvPurgeResponse::TPtr& ev, const TActorContext& ctx) {
            ReaderActorId_ = {};
            auto& response = *ev->Get();
            switch (response.Status) {
                case Ydb::StatusIds::SUCCESS: {
                    break;
                }
                case Ydb::StatusIds::SCHEME_ERROR: {
                    ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("Error purge a topic: {}", response.ErrorDescription.ConstRef())));
                    return;
                }
                default: {
                    ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Error purge a topic: {}", response.ErrorDescription.ConstRef())));
                    return;
                }
            }

            Ydb::Ymq::V1::PurgeQueueResult result;
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

    std::unique_ptr<NActors::IActor> CreatePurgeQueueActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TPurgeQueueActor>(msg);
    }

} // namespace NKikimr::NSqsTopic::V1
