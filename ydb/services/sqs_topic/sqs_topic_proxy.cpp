#include "sqs_topic_proxy.h"
#include "actor.h"
#include "change_message_visibility.h"
#include "create_queue.h"
#include "error.h"
#include "delete_message.h"
#include "get_queue_attributes.h"
#include "list_queues.h"
#include "purge_queue.h"
#include "request.h"
#include "receive_message.h"
#include "send_message.h"
#include "set_queue_attributes.h"
#include "utils.h"

#include <ydb/services/sqs_topic/queue_url/consumer.h>
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
            .Consumer = this->Consumer.empty() ? GetDefaultSqsConsumerName() : this->Consumer,
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

    static std::unique_ptr<IActor> CreateGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::unique_ptr<IActor>{new TGetQueueUrlActor(msg)};
    }

#define DECLARE_RPC(name)                                                                           \
    template <>                                                                                     \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { \
        return Create##name##Actor(msg).release();                                                  \
    }

#define DECLARE_RPC_NI(name)                                                                            \
    template <>                                                                                         \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {     \
        return new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvSqsTopic##name##Request>(msg); \
    }

    DECLARE_RPC(ChangeMessageVisibility);
    DECLARE_RPC(ChangeMessageVisibilityBatch);
    DECLARE_RPC(DeleteMessage);
    DECLARE_RPC(DeleteMessageBatch);
    DECLARE_RPC(GetQueueUrl);
    DECLARE_RPC(GetQueueAttributes);
    DECLARE_RPC(ListQueues);
    DECLARE_RPC(ReceiveMessage);
    DECLARE_RPC(SendMessage);
    DECLARE_RPC(SendMessageBatch);
    DECLARE_RPC(CreateQueue);
    DECLARE_RPC(SetQueueAttributes);
    DECLARE_RPC(PurgeQueue);
    DECLARE_RPC_NI(DeleteQueue);
    DECLARE_RPC_NI(ListDeadLetterSourceQueues);
    DECLARE_RPC_NI(ListQueueTags);
    DECLARE_RPC_NI(TagQueue);
    DECLARE_RPC_NI(UntagQueue);

#undef DECLARE_RPC
#undef DECLARE_RPC_NI
} // namespace NKikimr::NGRpcService
