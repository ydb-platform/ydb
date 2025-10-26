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

#include <ydb/core/ymq/actor/auth_factory.h>  // reuse auth factory from ymq
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

    #define COPY_FIELD_IF_PRESENT(from, to) \
    if (GetProtoRequest()->Has##from()) { \
        result->Set##to(GetProtoRequest()->Get##from()); \
    }

    #define COPY_FIELD_IF_PRESENT_IN_ENTRY(from, to) \
    if (requestEntry.Has##from()) { \
        entry->Set##to(requestEntry.Get##from()); \
    }

    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    // Names of queue attributes
    static const TString APPROXIMATE_NUMBER_OF_MESSAGES = "ApproximateNumberOfMessages";
    static const TString APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED = "ApproximateNumberOfMessagesDelayed";
    static const TString APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE = "ApproximateNumberOfMessagesNotVisible";
    static const TString CONTENT_BASED_DEDUPLICATION = "ContentBasedDeduplication";
    static const TString CREATED_TIMESTAMP = "CreatedTimestamp";
    static const TString DELAY_SECONDS = "DelaySeconds";
    static const TString LAST_MODIFIED_TIMESTAMP = "LastModifiedTimestamp";
    static const TString MAXIMUM_MESSAGE_SIZE = "MaximumMessageSize";
    static const TString MESSAGE_RETENTION_PERIOD = "MessageRetentionPeriod";
    static const TString QUEUE_ARN = "QueueArn";
    static const TString RECEIVE_MESSAGE_WAIT_TIME_SECONDS = "ReceiveMessageWaitTimeSeconds";
    static const TString VISIBILITY_TIMEOUT = "VisibilityTimeout";
    static const TString REDRIVE_POLICY = "RedrivePolicy";

    // Names of message attributes
    static const TString APPROXIMATE_RECEIVE_COUNT = "ApproximateReceiveCount";
    static const TString APPROXIMATE_FIRST_RECEIVE_TIMESTAMP = "ApproximateFirstReceiveTimestamp";
    static const TString MESSAGE_DEDUPLICATION_ID = "MessageDeduplicationId";
    static const TString MESSAGE_GROUP_ID = "MessageGroupId";
    static const TString SENDER_ID = "SenderId";
    static const TString SENT_TIMESTAMP = "SentTimestamp";
    static const TString SEQUENCE_NUMBER = "SequenceNumber";

    static TString GetMetaValue(NKikimr::NGRpcService::IRequestOpCtx* request, const TString& key) {
        return request->GetPeerMetaValues(key).GetOrElse("");
    }

    template<class TResponse, class TResult>
    class TReplyCallback : public NSQS::IReplyCallback {
    public:
        explicit TReplyCallback(std::shared_ptr<NKikimr::NGRpcService::IRequestOpCtx> request)
            : Request(request)
        {
        }

        void DoSendReply(const NKikimrClient::TSqsResponse& resp) {
            if (this->GetResponse(resp).HasError()) {
                NYql::TIssue issue(this->GetResponse(resp).GetError().GetMessage());
                issue.SetCode(
                    NSQS::TErrorClass::GetId(this->GetResponse(resp).GetError().GetErrorCode()),
                    NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR
                );
                Request->RaiseIssue(issue);
                Request->ReplyWithYdbStatus(Ydb::StatusIds_StatusCode_STATUS_CODE_UNSPECIFIED);
            } else {
                Ydb::Operations::Operation operation;
                operation.set_ready(true);
                operation.set_status(Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS);
                Ydb::SqsTopic::V1::QueueTags queueTags;
                for (const auto& t: resp.GetQueueTags()) {
                    queueTags.mutable_tags()->emplace(t.GetKey(), t.GetValue());
                }
                operation.mutable_metadata()->PackFrom(queueTags);
                operation.mutable_result()->PackFrom(this->GetResult(resp));
                Request->SendOperation(operation);
            }
        }

    protected:
        virtual const TResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) = 0;
        virtual TResult GetResult(const NKikimrClient::TSqsResponse& resp) = 0;

        std::shared_ptr<NKikimr::NGRpcService::IRequestOpCtx> Request;
    };

    template<class TEvSqsTopicRequest, class TRequest, class TReplyCallback>
    class TBaseRpcRequestActor : public TRpcRequestWithOperationParamsActor<
            TBaseRpcRequestActor<TEvSqsTopicRequest, TRequest, TReplyCallback>,
            TEvSqsTopicRequest,
            true>
    {
        using TBase = TRpcRequestWithOperationParamsActor<
                TBaseRpcRequestActor<TEvSqsTopicRequest, TRequest, TReplyCallback>,
                TEvSqsTopicRequest,
                true>;

    public:
        TBaseRpcRequestActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request)
        , FolderId(GetMetaValue(request, FOLDER_ID))
        , CloudId(GetMetaValue(request, CLOUD_ID))
        , UserSid(GetMetaValue(request, USER_SID))
        , RequestId(GetMetaValue(request, REQUEST_ID))
        , SecurityToken(GetMetaValue(request, SECURITY_TOKEN))
        {
        }

        ~TBaseRpcRequestActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            LOG_DEBUG_S(
                ctx,
                NKikimrServices::SQS,
                TStringBuilder() << "Got new request in SQS Topic proxy."
                << " FolderId: " << FolderId
                << ", CloudId: " << CloudId
                << ", UserSid: " << UserSid
                << ", RequestId: " << RequestId;
            );
            TSqsRequest sqsRequest;

            sqsRequest.SetRequestId(RequestId);

            auto request = GetRequest(sqsRequest);

            request->MutableAuth()->SetUserName(CloudId);
            request->MutableAuth()->SetFolderId(FolderId);
            request->MutableAuth()->SetUserSID(UserSid);

            if (SecurityToken) {
                request->MutableCredentials()->SetOAuthToken(SecurityToken);
            }

            auto actor = CreateProxyActionActor(sqsRequest, CreateReplyCallback(), true);
            ctx.RegisterWithSameMailbox(actor);

            TBase::Die(ctx);
        }

    protected:
        virtual TRequest* GetRequest(TSqsRequest&) = 0;
        virtual THolder<TReplyCallback> CreateReplyCallback() = 0;
    private:
        const TString FolderId;
        const TString CloudId;
        const TString UserSid;
        const TString RequestId;
        const TString SecurityToken;
    };

    template<class TEvSqsTopicRequest, class TRequest, class TReplyCallback>
    class TRpcRequestActor : public TBaseRpcRequestActor<TEvSqsTopicRequest, TRequest, TReplyCallback> {
        using TBase = TBaseRpcRequestActor<TEvSqsTopicRequest, TRequest, TReplyCallback>;
    public:
        using TBase::TBaseRpcRequestActor;

    protected:
        THolder<TReplyCallback> CreateReplyCallback() {
            return MakeHolder<TReplyCallback>(TBase::Request_);
        }
    };

    class TGetQueueUrlReplyCallback: public TReplyCallback<
            NKikimr::NSQS::TGetQueueUrlResponse,
            Ydb::SqsTopic::V1::GetQueueUrlResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TGetQueueUrlResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetGetQueueUrl();
        }

        Ydb::SqsTopic::V1::GetQueueUrlResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::SqsTopic::V1::GetQueueUrlResult result;
            result.Setqueue_url(GetResponse(resp).GetQueueUrl());
            return result;
        }
    };

    class TGetQueueUrlActor : public TRpcRequestActor<TEvSqsTopicGetQueueUrlRequest, NKikimr::NSQS::TGetQueueUrlRequest, class TGetQueueUrlReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TGetQueueUrlRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableGetQueueUrl();
            result->SetQueueName(GetProtoRequest()->queue_name());
            return result;
        }
    };

    class TCreateQueueReplyCallback: public TReplyCallback<
            NKikimr::NSQS::TCreateQueueResponse,
            Ydb::SqsTopic::V1::CreateQueueResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TCreateQueueResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetCreateQueue();
        }

        Ydb::SqsTopic::V1::CreateQueueResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::SqsTopic::V1::CreateQueueResult result;
            result.Setqueue_url(GetResponse(resp).GetQueueUrl());
            return result;
        }
    };

    #undef COPY_FIELD_IF_PRESENT
    #undef COPY_FIELD_IF_PRESENT_IN_ENTRY

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
}

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

#define DECLARE_RPC_NI(name)                                                                                                                               \
    template <>                                                                                                                                            \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {                                                     \
        return new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvSqsTopic##name##Request>(msg);                                                 \
    }                                                                                                                                                      \
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

}
