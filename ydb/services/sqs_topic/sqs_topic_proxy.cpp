#include "sqs_topic_proxy.h"
#include "codes.h"

#include <ydb/services/sqs_topic/rpc_params.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/core/persqueue/pqtablet/partition/partition.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>

#include <ydb/library/http_proxy/error/error.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {

    static void SetQueueName(auto* result, const auto* request) {
        Y_VERIFY(result != nullptr);
        Y_VERIFY(request != nullptr);
        const auto& queueUrl = request->queue_url();
        auto parsedQueueUrl = ParseQueueUrl(queueUrl);
        result->SetQueueName(std::move(parsedQueueUrl).value().TopicPath);
    }

    template <class TRequest>
    static const TRequest& GetRequest(NGRpcService::IRequestOpCtx* ctx) {
        Y_ASSERT(ctx != nullptr);
        const auto* request = ctx->GetRequest();
        Y_ASSERT(request != nullptr);
        return dynamic_cast<const TRequest&>(*request);
    }

    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    class TGetQueueUrlActor: public TPQGrpcSchemaBase<TGetQueueUrlActor, TEvSqsTopicGetQueueUrlRequest> {
        using TBase = TPQGrpcSchemaBase<TGetQueueUrlActor, TEvSqsTopicGetQueueUrlRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TGetQueueUrlActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    private:
        void ReplyAndDie(const TActorContext& ctx);
        NKikimrSchemeOp::TDirEntry SelfInfo;
    };

    TGetQueueUrlActor::TGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, GetRequest<TProtoRequest>(request).queue_name())
    {
    }

    void TGetQueueUrlActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        if (!Request_->GetDatabaseName()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NKikimr::NSqsTopic::EErrorCodes::INVALID_ARGUMENT),
                                  "Request without dabase is forbiden");
        }
        SendDescribeProposeRequest(ctx);
        Become(&TGetQueueUrlActor::StateWork);
    }

    void TGetQueueUrlActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse); // override for testing
            default:
                TBase::StateWork(ev);
        }
    }

    void TGetQueueUrlActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_ABORT_UNLESS(result->ResultSet.size() == 1); // describe only one topic
        const auto& response = result->ResultSet.front();
        const TString path = JoinSeq("/", response.Path);

        if (0 && ReplyIfNotTopic(ev)) {
            return;
        }

        ReplyAndDie(ActorContext());
    }

    void TGetQueueUrlActor::ReplyAndDie(const TActorContext& ctx) {
        Ydb::SqsTopic::V1::GetQueueUrlResult result;
        result.set_queue_url("foobarr");
        return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    template <class TEvRequest>
    class TNotImplementedRequestActor: public TRpcSchemeRequestActor<TNotImplementedRequestActor<TEvRequest>, TEvRequest> {
        using TBase = TRpcSchemeRequestActor<TNotImplementedRequestActor, TEvRequest>;

    public:
        TNotImplementedRequestActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request)
        {
        }
        ~TNotImplementedRequestActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            this->Request_->RaiseIssue(FillIssue("Method is not implemented yet", static_cast<size_t>(NKikimr::NSqsTopic::EErrorCodes::ERROR)));
            this->Request_->ReplyWithYdbStatus(Ydb::StatusIds::UNSUPPORTED);
            this->Die(ctx);
        }
    };
} // namespace NKikimr::NSqsTopic::V1

namespace NKikimr::NGRpcService {

    using namespace NSqsTopic::V1;

#define DECLARE_RPC(name)                                                                           \
    template <>                                                                                     \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { \
        return new T##name##Actor(msg);                                                             \
    }                                                                                               \
    void DoSqsTopic##name##Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {    \
        TActivationContext::AsActorContext().Register(new T##name##Actor(p.release()));             \
    }

#define DECLARE_RPC_NI(name)                                                                                                                            \
    template <>                                                                                                                                         \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {                                                     \
        return new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvSqsTopic##name##Request>(msg);                                                 \
    }                                                                                                                                                   \
    void DoSqsTopic##name##Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {                                                        \
        TActivationContext::AsActorContext().Register(new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvSqsTopic##name##Request>(p.release())); \
    }

    DECLARE_RPC(GetQueueUrl);
    DECLARE_RPC_NI(CreateQueue);
    DECLARE_RPC_NI(SendMessage);
    DECLARE_RPC_NI(ReceiveMessage);
    DECLARE_RPC_NI(GetQueueAttributes);
    DECLARE_RPC_NI(ListQueues);
    DECLARE_RPC_NI(DeleteMessage);
    DECLARE_RPC_NI(PurgeQueue);
    DECLARE_RPC_NI(DeleteQueue);
    DECLARE_RPC_NI(ChangeMessageVisibility);
    DECLARE_RPC_NI(SetQueueAttributes);
    DECLARE_RPC_NI(SendMessageBatch);
    DECLARE_RPC_NI(DeleteMessageBatch);
    DECLARE_RPC_NI(ChangeMessageVisibilityBatch);
    DECLARE_RPC_NI(ListDeadLetterSourceQueues);
    DECLARE_RPC_NI(ListQueueTags);
    DECLARE_RPC_NI(TagQueue);
    DECLARE_RPC_NI(UntagQueue);

} // namespace NKikimr::NGRpcService
