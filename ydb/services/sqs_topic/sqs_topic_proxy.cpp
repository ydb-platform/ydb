#include "sqs_topic_proxy.h"
#include "actor.h"
#include "error.h"
#include "delete_message.h"
#include "request.h"
#include "receive_message.h"
#include "send_message.h"
#include "utils.h"

#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/sqs.pb.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/services/datastreams/codes/datastreams_codes.h>

#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>

#include <ydb/library/http_proxy/error/error.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {

    const TString DEFAULT_SQS_CONSUMER = "ydb-sqs-consumer";

    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    class TGetQueueUrlActor: public TGrpcActorBase<TGetQueueUrlActor, TEvSqsTopicGetQueueUrlRequest> {
        using TBase = TGrpcActorBase<TGetQueueUrlActor, TEvSqsTopicGetQueueUrlRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TGetQueueUrlActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    private:
        void ReplyAndDie(const TActorContext& ctx);
        TString Consumer;
    };

    TGetQueueUrlActor::TGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, ToString(SplitExtendedQueueName(GetRequest<TProtoRequest>(request).queue_name()).QueueName))
        , Consumer(SplitExtendedQueueName(GetRequest<TProtoRequest>(request).queue_name()).Consumer)
    {
    }

    void TGetQueueUrlActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        if (GetRequest<TProtoRequest>(Request_.get()).queue_name().empty()) {
            return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueName parameter."));
        }
        if (!Request_->GetDatabaseName()) {
            return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Request without database is forbiden"));
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
        ReplyAndDie(ActorContext());
    }

    void TGetQueueUrlActor::ReplyAndDie(const TActorContext& ctx) {
        Ydb::Ymq::V1::GetQueueUrlResult result;

        const TRichQueueUrl queueUrl{
            .Database = this->Database,
            .TopicPath = this->TopicPath,
            .Consumer = this->Consumer.empty() ? DEFAULT_SQS_CONSUMER : this->Consumer,
            .Fifo = AsciiHasSuffixIgnoreCase(this->Consumer, ".fifo"),
        };

        TString path = PackQueueUrlPath(queueUrl);
        TString url = TStringBuilder() << GetEndpoint(Cfg()) << path;
        result.set_queue_url(std::move(url));
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
            this->Request_->RaiseIssue(FillIssue("Method is not implemented yet", static_cast<size_t>(NYds::EErrorCodes::ERROR)));
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
    }

#define DECLARE_RPC_NI(name)                                                                            \
    template <>                                                                                         \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {     \
        return new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvSqsTopic##name##Request>(msg); \
    }

    DECLARE_RPC(GetQueueUrl);
    DECLARE_RPC_NI(CreateQueue);
    DECLARE_RPC_NI(GetQueueAttributes);
    DECLARE_RPC_NI(ListQueues);
    DECLARE_RPC_NI(PurgeQueue);
    DECLARE_RPC_NI(DeleteQueue);
    DECLARE_RPC_NI(ChangeMessageVisibility);
    DECLARE_RPC_NI(SetQueueAttributes);
    DECLARE_RPC_NI(ChangeMessageVisibilityBatch);
    DECLARE_RPC_NI(ListDeadLetterSourceQueues);
    DECLARE_RPC_NI(ListQueueTags);
    DECLARE_RPC_NI(TagQueue);
    DECLARE_RPC_NI(UntagQueue);

    template <>
    IActor* TEvSqsTopicSendMessageRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return CreateSendMessageActor(msg).release();
    }

    template <>
    IActor* TEvSqsTopicSendMessageBatchRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return CreateSendMessageBatchActor(msg).release();
    }

    template <>
    IActor* TEvSqsTopicReceiveMessageRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return CreateReceiveMessageActor(msg).release();
    }

    template <>
    IActor* TEvSqsTopicDeleteMessageRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return CreateDeleteMessageActor(msg).release();
    }

    template <>
    IActor* TEvSqsTopicDeleteMessageBatchRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return CreateDeleteMessageBatchActor(msg).release();
    }

} // namespace NKikimr::NGRpcService
