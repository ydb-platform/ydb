#include "ymq_proxy.h"

#include <ydb/core/grpc_services/service_ymq.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/persqueue/partition.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/core/persqueue/write_meta.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>

#include <util/folder/path.h>
#include <ydb/core/ymq/actor/auth_factory.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/services/ymq/rpc_params.h>

#include "utils.h"
#include "ydb/core/ymq/actor/log.h"


using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;


namespace NKikimr::NYmq::V1 {

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
                Ydb::Ymq::V1::QueueTags queueTags;
                for (const auto& t: resp.GetQueueTags()) {
                    (*queueTags.mutable_tags())[t.GetKey()] = t.GetValue();
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

    template<class TEvYmqRequest, class TRequest, class TReplyCallback>
    class TBaseRpcRequestActor : public TRpcRequestWithOperationParamsActor<
            TBaseRpcRequestActor<TEvYmqRequest, TRequest, TReplyCallback>,
            TEvYmqRequest,
            true>
    {
        using TBase = TRpcRequestWithOperationParamsActor<
                TBaseRpcRequestActor<TEvYmqRequest, TRequest, TReplyCallback>,
                TEvYmqRequest,
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
                TStringBuilder() << "Got new request in YMQ proxy."
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

    template<class TEvYmqRequest, class TRequest, class TReplyCallback>
    class TRpcRequestActor : public TBaseRpcRequestActor<TEvYmqRequest, TRequest, TReplyCallback> {
        using TBase = TBaseRpcRequestActor<TEvYmqRequest, TRequest, TReplyCallback>;
    public:
        using TBase::TBaseRpcRequestActor;

    protected:
        THolder<TReplyCallback> CreateReplyCallback() {
            return MakeHolder<TReplyCallback>(TBase::Request_);
        }
    };

    class TGetQueueUrlReplyCallback: public TReplyCallback<
            NKikimr::NSQS::TGetQueueUrlResponse,
            Ydb::Ymq::V1::GetQueueUrlResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TGetQueueUrlResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetGetQueueUrl();
        }

        Ydb::Ymq::V1::GetQueueUrlResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::Ymq::V1::GetQueueUrlResult result;
            result.Setqueue_url(GetResponse(resp).GetQueueUrl());
            return result;
        }
    };

    class TGetQueueUrlActor : public TRpcRequestActor<TEvYmqGetQueueUrlRequest, NKikimr::NSQS::TGetQueueUrlRequest, class TGetQueueUrlReplyCallback> {
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
            Ydb::Ymq::V1::CreateQueueResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TCreateQueueResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetCreateQueue();
        }

        Ydb::Ymq::V1::CreateQueueResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::Ymq::V1::CreateQueueResult result;
            result.Setqueue_url(GetResponse(resp).GetQueueUrl());
            return result;
        }
    };

    class TCreateQueueActor : public TRpcRequestActor<
            TEvYmqCreateQueueRequest,
            NKikimr::NSQS::TCreateQueueRequest,
            TCreateQueueReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TCreateQueueRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableCreateQueue();
            result->SetQueueName(GetProtoRequest()->queue_name());
            for (auto &srcAttribute : GetProtoRequest()->attributes()) {
                auto dstAttribute = result->MutableAttributes()->Add();
                dstAttribute->SetName(srcAttribute.first);
                dstAttribute->SetValue(srcAttribute.second);
            }
            for (auto &srcTag : GetProtoRequest()->tags()) {
                auto dstTag = result->MutableTags()->Add();
                dstTag->SetKey(srcTag.first);
                dstTag->SetValue(srcTag.second);
            }
            return result;
        }
    };

    class TSendMessageReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TSendMessageResponse,
            Ydb::Ymq::V1::SendMessageResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TSendMessageResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetSendMessage();
        }

        Ydb::Ymq::V1::SendMessageResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::Ymq::V1::SendMessageResult result;
            result.set_m_d_5_of_message_attributes(GetResponse(resp).GetMD5OfMessageAttributes());
            result.set_m_d_5_of_message_body(GetResponse(resp).GetMD5OfMessageBody());
            result.set_message_id(GetResponse(resp).GetMessageId());
            result.set_sequence_number(std::to_string(GetResponse(resp).GetSequenceNumber()));
            return result;
        }
    };

    class TSendMessageActor : public TRpcRequestActor<
            TEvYmqSendMessageRequest,
            NKikimr::NSQS::TSendMessageRequest,
            TSendMessageReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TSendMessageRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableSendMessage();

            for (auto& srcAttribute: GetProtoRequest()->Getmessage_attributes()) {
                auto dstAttribute = result->MutableMessageAttributes()->Add();
                dstAttribute->SetName(srcAttribute.first);
                dstAttribute->SetStringValue(srcAttribute.second.Getstring_value());
                dstAttribute->SetBinaryValue(srcAttribute.second.Getbinary_value());
                dstAttribute->SetDataType(srcAttribute.second.Getdata_type());
            }

            COPY_FIELD_IF_PRESENT(delay_seconds, DelaySeconds);
            COPY_FIELD_IF_PRESENT(message_deduplication_id, MessageDeduplicationId);
            COPY_FIELD_IF_PRESENT(message_group_id, MessageGroupId);

            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->Getqueue_url()).second);

            result->SetMessageBody(GetProtoRequest()->Getmessage_body());

            return result;
        }
    };

    class TReceiveMessageReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TReceiveMessageResponse,
            Ydb::Ymq::V1::ReceiveMessageResult> {
        using TBase = TReplyCallback<NKikimr::NSQS::TReceiveMessageResponse, Ydb::Ymq::V1::ReceiveMessageResult>;
    public:
        TReceiveMessageReplyCallback(
                std::shared_ptr<NKikimr::NGRpcService::IRequestOpCtx> request,
                TVector<TString> attributes)
            : TBase(request)
            , AttributesNames(std::move(attributes))
        {
        }

        const NKikimr::NSQS::TReceiveMessageResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetReceiveMessage();
        }

        Ydb::Ymq::V1::ReceiveMessageResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::Ymq::V1::ReceiveMessageResult result;

            for (const auto& srcMessage: GetResponse(resp).GetMessages()) {
                Ydb::Ymq::V1::Message dstMessage;

                if (srcMessage.HasApproximateReceiveCount()) {
                    dstMessage.Mutableattributes()->insert({APPROXIMATE_RECEIVE_COUNT, std::to_string(srcMessage.GetApproximateReceiveCount())});
                }
                if (srcMessage.HasApproximateFirstReceiveTimestamp()) {
                    dstMessage.Mutableattributes()->insert({APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, std::to_string(srcMessage.GetApproximateFirstReceiveTimestamp())});
                }
                if (srcMessage.HasMessageDeduplicationId()) {
                    dstMessage.Mutableattributes()->insert({MESSAGE_DEDUPLICATION_ID, srcMessage.GetMessageDeduplicationId()});
                }
                if (srcMessage.HasMessageGroupId()) {
                    dstMessage.Mutableattributes()->insert({MESSAGE_GROUP_ID, srcMessage.GetMessageGroupId()});
                }
                if (srcMessage.HasSenderId()) {
                    dstMessage.Mutableattributes()->insert({SENDER_ID, srcMessage.GetSenderId()});
                }
                if (srcMessage.HasSentTimestamp()) {
                    dstMessage.Mutableattributes()->insert({SENT_TIMESTAMP, std::to_string(srcMessage.GetSentTimestamp())});
                }
                if (srcMessage.HasSequenceNumber()) {
                    dstMessage.Mutableattributes()->insert({SEQUENCE_NUMBER, std::to_string(srcMessage.GetSequenceNumber())});
                }

                dstMessage.set_body(srcMessage.GetData());
                dstMessage.set_m_d_5_of_body(srcMessage.GetMD5OfMessageBody());
                dstMessage.set_m_d_5_of_message_attributes(srcMessage.GetMD5OfMessageAttributes());

                for (const auto& srcAttribute: srcMessage.GetMessageAttributes()) {
                    Ydb::Ymq::V1::MessageAttribute dstAttribute;

                    dstAttribute.set_binary_value(srcAttribute.GetBinaryValue());
                    dstAttribute.set_data_type(srcAttribute.GetDataType());
                    dstAttribute.set_string_value(srcAttribute.GetStringValue());
                    dstMessage.mutable_message_attributes()->insert({srcAttribute.GetName(), dstAttribute});
                }

                dstMessage.set_message_id(srcMessage.GetMessageId());
                dstMessage.set_receipt_handle(srcMessage.GetReceiptHandle());
                result.Mutablemessages()->Add(std::move(dstMessage));
            }

            return result;
        }

    private:
        TVector<TString> AttributesNames;
    };

    class TReceiveMessageActor : public TBaseRpcRequestActor<TEvYmqReceiveMessageRequest, NKikimr::NSQS::TReceiveMessageRequest, class TReceiveMessageReplyCallback> {
    public:
        using TBaseRpcRequestActor::TBaseRpcRequestActor;

    private:
        NKikimr::NSQS::TReceiveMessageRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableReceiveMessage();

            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);

            COPY_FIELD_IF_PRESENT(max_number_of_messages, MaxNumberOfMessages);
            COPY_FIELD_IF_PRESENT(receive_request_attempt_id, ReceiveRequestAttemptId);
            COPY_FIELD_IF_PRESENT(visibility_timeout, VisibilityTimeout);
            COPY_FIELD_IF_PRESENT(wait_time_seconds, WaitTimeSeconds);

            auto systemAttributeNames = GetProtoRequest()->Getmessage_system_attribute_names();
            // We ignore AttributeNames if SystemAttributeNames is present,
            // because AttributeNames is deprecated in favour of SystemAttributeNames
            if (systemAttributeNames.size() > 0) {
                for (int i = 0; i < systemAttributeNames.size(); i++) {
                    result->AddAttributeName(systemAttributeNames[i]);
                }
            } else {
                auto attributeNames = GetProtoRequest()->Getattribute_names();
                for (int i = 0; i < attributeNames.size(); i++) {
                    result->AddAttributeName(attributeNames[i]);
                }
            }

            for (int i = 0; i < GetProtoRequest()->Getmessage_attribute_names().size(); i++) {
                result->AddMessageAttributeName(GetProtoRequest()->Getmessage_attribute_names()[i]);
            }

            return result;
        }

    private:
        THolder<TReceiveMessageReplyCallback> CreateReplyCallback() override {
            TVector<TString> attributesNames;
            for (const auto& attributeName : GetProtoRequest()->Getattribute_names()) {
                attributesNames.push_back(attributeName);
            }
            return MakeHolder<TReceiveMessageReplyCallback>(Request_, std::move(attributesNames));
        }
    };

    template <typename T>
    void AddAttribute(Ydb::Ymq::V1::GetQueueAttributesResult& result, const TString& name, T value) {
        result.Mutableattributes()->insert({name, std::to_string(value)});
    };

    template<>
    void AddAttribute<TString>(Ydb::Ymq::V1::GetQueueAttributesResult& result, const TString& name, TString value) {
        result.Mutableattributes()->insert({name, value});
    };

    template <>
    void AddAttribute(Ydb::Ymq::V1::GetQueueAttributesResult& result, const TString& name, bool value) {
        (*result.Mutableattributes())[name] = value ? "true" : "false";
    };

    class TGetQueueAttributesReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TGetQueueAttributesResponse,
            Ydb::Ymq::V1::GetQueueAttributesResult> {
    public:
        TGetQueueAttributesReplyCallback (
                std::shared_ptr<NKikimr::NGRpcService::IRequestOpCtx> request,
                TVector<TString> attributes)
            : TReplyCallback<
                NKikimr::NSQS::TGetQueueAttributesResponse,
                Ydb::Ymq::V1::GetQueueAttributesResult>(request)
            , Attributes(attributes)
        {
        }

    private:
        const NKikimr::NSQS::TGetQueueAttributesResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetGetQueueAttributes();
        }

        Ydb::Ymq::V1::GetQueueAttributesResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::Ymq::V1::GetQueueAttributesResult result;
            const auto& attrs = resp.GetGetQueueAttributes();
            if (attrs.HasApproximateNumberOfMessages()) {
                AddAttribute(result, APPROXIMATE_NUMBER_OF_MESSAGES, attrs.GetApproximateNumberOfMessages());
            }
            if (attrs.HasApproximateNumberOfMessagesDelayed()) {
                AddAttribute(result, APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, attrs.GetApproximateNumberOfMessagesDelayed());
            }
            if (attrs.HasApproximateNumberOfMessagesNotVisible()) {
                AddAttribute(result, APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, attrs.GetApproximateNumberOfMessagesNotVisible());
            }
            if (attrs.HasContentBasedDeduplication()) {
                AddAttribute(result, CONTENT_BASED_DEDUPLICATION, attrs.GetContentBasedDeduplication());
            }
            if (attrs.HasCreatedTimestamp()) {
                AddAttribute(result, CREATED_TIMESTAMP, attrs.GetCreatedTimestamp());
            }
            if (attrs.HasDelaySeconds()) {
                AddAttribute(result, DELAY_SECONDS, attrs.GetDelaySeconds());
            }
            if (attrs.HasLastModifiedTimestamp()) {
                AddAttribute(result, LAST_MODIFIED_TIMESTAMP, attrs.GetLastModifiedTimestamp());
            }
            if (attrs.HasMaximumMessageSize()) {
                AddAttribute(result, MAXIMUM_MESSAGE_SIZE, attrs.GetMaximumMessageSize());
            }
            if (attrs.HasMessageRetentionPeriod()) {
                AddAttribute(result, MESSAGE_RETENTION_PERIOD, attrs.GetMessageRetentionPeriod());
            }
            if (attrs.HasQueueArn()) {
                AddAttribute(result, QUEUE_ARN, attrs.GetQueueArn());
            }
            if (attrs.HasReceiveMessageWaitTimeSeconds()) {
                AddAttribute(result, RECEIVE_MESSAGE_WAIT_TIME_SECONDS, attrs.GetReceiveMessageWaitTimeSeconds());
            }
            if (attrs.HasVisibilityTimeout()) {
                AddAttribute(result, VISIBILITY_TIMEOUT, attrs.GetVisibilityTimeout());
            }
            if (attrs.HasRedrivePolicy()) {
                AddAttribute(result, REDRIVE_POLICY, attrs.GetRedrivePolicy());
            }

            return result;
        }

        TVector<TString> Attributes;
    };

    class TGetQueueAttributesActor : public TBaseRpcRequestActor<
            TEvYmqGetQueueAttributesRequest,
            NKikimr::NSQS::TGetQueueAttributesRequest,
            TGetQueueAttributesReplyCallback> {
    public:
        using TBaseRpcRequestActor::TBaseRpcRequestActor;

    private:
        NKikimr::NSQS::TGetQueueAttributesRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableGetQueueAttributes();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            for (const auto& attributeName : GetProtoRequest()->Getattribute_names()) {
                result->MutableNames()->Add()->assign(attributeName);
            }
            return result;
        }

        THolder<TGetQueueAttributesReplyCallback> CreateReplyCallback() override {
            TVector<TString> attributes;
            for (auto& attribute : GetProtoRequest()->Getattribute_names()) {
                attributes.push_back(attribute);
            }
            return MakeHolder<TGetQueueAttributesReplyCallback>(Request_, std::move(attributes));
        }
    };

    class TListQueuesReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TListQueuesResponse,
            Ydb::Ymq::V1::ListQueuesResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TListQueuesResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetListQueues();
        }

        Ydb::Ymq::V1::ListQueuesResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::Ymq::V1::ListQueuesResult result;
            for (auto& queue : GetResponse(resp).GetQueues()) {
                result.Mutablequeue_urls()->Add()->assign(queue.GetQueueUrl());
            }
            return result;
        }
    };

    class TListQueuesActor : public TRpcRequestActor<
            TEvYmqListQueuesRequest,
            NKikimr::NSQS::TListQueuesRequest,
            TListQueuesReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TListQueuesRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableListQueues();
            COPY_FIELD_IF_PRESENT(queue_name_prefix, QueueNamePrefix);
            return result;
        }
    };

    class TDeleteMessageReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TDeleteMessageResponse,
            Ydb::Ymq::V1::DeleteMessageResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TDeleteMessageResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetDeleteMessage();
        }

        Ydb::Ymq::V1::DeleteMessageResult GetResult(const NKikimrClient::TSqsResponse&) override {
            Ydb::Ymq::V1::DeleteMessageResult result;
            return result;
        }
    };

    class TDeleteMessageActor : public TRpcRequestActor<
            TEvYmqDeleteMessageRequest,
            NKikimr::NSQS::TDeleteMessageRequest,
            TDeleteMessageReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TDeleteMessageRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableDeleteMessage();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            result->SetReceiptHandle(GetProtoRequest()->receipt_handle());
            return result;
        }
    };

    class TPurgeQueueReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TPurgeQueueResponse,
            Ydb::Ymq::V1::PurgeQueueResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TPurgeQueueResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetPurgeQueue();
        }

        Ydb::Ymq::V1::PurgeQueueResult GetResult(const NKikimrClient::TSqsResponse&) override {
            Ydb::Ymq::V1::PurgeQueueResult result;
            return result;
        }
    };

    class TPurgeQueueActor : public TRpcRequestActor<
            TEvYmqPurgeQueueRequest,
            NKikimr::NSQS::TPurgeQueueRequest,
            TPurgeQueueReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TPurgeQueueRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutablePurgeQueue();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            return result;
        }
    };

    class TDeleteQueueReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TDeleteQueueResponse,
            Ydb::Ymq::V1::DeleteQueueResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TDeleteQueueResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetDeleteQueue();
        }

        Ydb::Ymq::V1::DeleteQueueResult GetResult(const NKikimrClient::TSqsResponse&) override {
            Ydb::Ymq::V1::DeleteQueueResult result;
            return result;
        }
    };

    class TDeleteQueueActor : public TRpcRequestActor<
            TEvYmqDeleteQueueRequest,
            NKikimr::NSQS::TDeleteQueueRequest,
            TDeleteQueueReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TDeleteQueueRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableDeleteQueue();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            return result;
        }
    };

    class TChangeMessageVisibilityReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TChangeMessageVisibilityResponse,
            Ydb::Ymq::V1::ChangeMessageVisibilityResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TChangeMessageVisibilityResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetChangeMessageVisibility();
        }

        Ydb::Ymq::V1::ChangeMessageVisibilityResult GetResult(const NKikimrClient::TSqsResponse&) override {
            Ydb::Ymq::V1::ChangeMessageVisibilityResult result;
            return result;
        }
    };

    class TChangeMessageVisibilityActor : public TRpcRequestActor<
            TEvYmqChangeMessageVisibilityRequest,
            NKikimr::NSQS::TChangeMessageVisibilityRequest,
            TChangeMessageVisibilityReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TChangeMessageVisibilityRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableChangeMessageVisibility();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            result->SetReceiptHandle(GetProtoRequest()->Getreceipt_handle());
            result->SetVisibilityTimeout(GetProtoRequest()->Getvisibility_timeout());
            return result;
        }
    };

    class TSetQueueAttributesReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TSetQueueAttributesResponse,
            Ydb::Ymq::V1::SetQueueAttributesResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TSetQueueAttributesResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetSetQueueAttributes();
        }

        Ydb::Ymq::V1::SetQueueAttributesResult GetResult(const NKikimrClient::TSqsResponse&) override {
            Ydb::Ymq::V1::SetQueueAttributesResult result;

            return result;
        }
    };


    void AddAttribute(TSqsRequest& requestHolder, const TString& name, TString value) {
        auto attribute = requestHolder.MutableSetQueueAttributes()->MutableAttributes()->Add();
        attribute->SetName(name);
        attribute->SetValue(value);
    };

    class TSetQueueAttributesActor : public TRpcRequestActor<
            TEvYmqSetQueueAttributesRequest,
            NKikimr::NSQS::TSetQueueAttributesRequest,
            TSetQueueAttributesReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TSetQueueAttributesRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableSetQueueAttributes();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            for (auto& [name, value]: GetProtoRequest()->Getattributes()) {
                AddAttribute(requestHolder, name, value);
            }
            return result;
        }
    };

    class TListDeadLetterSourceQueuesReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TListDeadLetterSourceQueuesResponse,
            Ydb::Ymq::V1::ListDeadLetterSourceQueuesResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TListDeadLetterSourceQueuesResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetListDeadLetterSourceQueues();
        }

        Ydb::Ymq::V1::ListDeadLetterSourceQueuesResult GetResult(const NKikimrClient::TSqsResponse& response) override {
            Ydb::Ymq::V1::ListDeadLetterSourceQueuesResult result;
            for (const auto& queue : response.GetListDeadLetterSourceQueues().GetQueues()) {
                result.Mutablequeue_urls()->Add()->assign(queue.GetQueueUrl());
            }
            return result;
        }
    };

    class TListDeadLetterSourceQueuesActor : public TRpcRequestActor<
            TEvYmqListDeadLetterSourceQueuesRequest,
            NKikimr::NSQS::TListDeadLetterSourceQueuesRequest,
            TListDeadLetterSourceQueuesReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TListDeadLetterSourceQueuesRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableListDeadLetterSourceQueues();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            return result;
        }
    };

    class TSendMessageBatchReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TSendMessageBatchResponse,
            Ydb::Ymq::V1::SendMessageBatchResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TSendMessageBatchResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetSendMessageBatch();
        }

        Ydb::Ymq::V1::SendMessageBatchResult GetResult(const NKikimrClient::TSqsResponse& response) override {
            Ydb::Ymq::V1::SendMessageBatchResult result;
            response.GetSendMessageBatch();
            for (auto& entry : response.GetSendMessageBatch().GetEntries()) {
                if (entry.GetError().HasErrorCode()) {
                    auto currentFailed = result.Addfailed();
                    currentFailed->Setcode(entry.GetError().GetErrorCode());
                    currentFailed->Setid(entry.GetId());
                    currentFailed->Setmessage(entry.GetError().GetMessage());

                    ui32 httpStatus = NSQS::TErrorClass::GetHttpStatus(entry.GetError().GetErrorCode()).GetOrElse(400);
                    currentFailed->Setsender_fault(400 <= httpStatus && httpStatus < 500);
                } else {
                    auto currentSuccessful = result.Addsuccessful();
                    currentSuccessful->Setid(entry.GetId());
                    currentSuccessful->set_m_d_5_of_message_attributes(entry.GetMD5OfMessageAttributes());
                    currentSuccessful->set_m_d_5_of_message_body(entry.GetMD5OfMessageBody());
                    currentSuccessful->Setmessage_id(entry.GetMessageId());
                    currentSuccessful->Setsequence_number(std::to_string(entry.GetSequenceNumber()));
                }
            }
            return result;
        }
    };

    class TSendMessageBatchActor : public TRpcRequestActor<
            TEvYmqSendMessageBatchRequest,
            NKikimr::NSQS::TSendMessageBatchRequest,
            TSendMessageBatchReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TSendMessageBatchRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableSendMessageBatch();

            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->Getqueue_url()).second);

            for (auto& requestEntry : GetProtoRequest()->Getentries()) {
                auto entry = requestHolder.MutableSendMessageBatch()->MutableEntries()->Add();

                entry->SetId(requestEntry.Getid());
                entry->SetMessageBody(requestEntry.Getmessage_body());

                COPY_FIELD_IF_PRESENT_IN_ENTRY(delay_seconds, DelaySeconds);
                COPY_FIELD_IF_PRESENT_IN_ENTRY(message_deduplication_id, MessageDeduplicationId);
                COPY_FIELD_IF_PRESENT_IN_ENTRY(message_group_id, MessageGroupId);

                for (auto& srcAttribute: requestEntry.Getmessage_attributes()) {
                    auto dstAttribute = entry->MutableMessageAttributes()->Add();
                    dstAttribute->SetName(srcAttribute.first);
                    dstAttribute->SetStringValue(srcAttribute.second.Getstring_value());
                    dstAttribute->SetBinaryValue(srcAttribute.second.Getbinary_value());
                    dstAttribute->SetDataType(srcAttribute.second.Getdata_type());
                }
            }
            return result;
        }
    };

    class TDeleteMessageBatchReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TDeleteMessageBatchResponse,
            Ydb::Ymq::V1::DeleteMessageBatchResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TDeleteMessageBatchResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetDeleteMessageBatch();
        }

        Ydb::Ymq::V1::DeleteMessageBatchResult GetResult(const NKikimrClient::TSqsResponse& response) override {
            Ydb::Ymq::V1::DeleteMessageBatchResult result;
            auto entries = response.GetDeleteMessageBatch().GetEntries();
            for (auto i = 0; i < entries.size(); i++) {
            auto &entry = response.GetDeleteMessageBatch().GetEntries()[i];
                if (entry.GetError().HasErrorCode()) {
                    auto currentFailed = result.Addfailed();
                    currentFailed->Setcode(entry.GetError().GetErrorCode());
                    currentFailed->Setid(entry.GetId());
                    currentFailed->Setmessage(entry.GetError().GetMessage());

                    ui32 httpStatus = NSQS::TErrorClass::GetHttpStatus(entry.GetError().GetErrorCode()).GetOrElse(400);
                    currentFailed->Setsender_fault(400 <= httpStatus && httpStatus < 500);
                } else {
                    auto currentSuccessful = result.Addsuccessful();
                    currentSuccessful->Setid(entry.GetId());
                }
            }
            return result;
        }
    };

    class TDeleteMessageBatchActor : public TRpcRequestActor<
            TEvYmqDeleteMessageBatchRequest,
            NKikimr::NSQS::TDeleteMessageBatchRequest,
            TDeleteMessageBatchReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TDeleteMessageBatchRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableDeleteMessageBatch();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->Getqueue_url()).second);
            for (auto& requestEntry : GetProtoRequest()->Getentries()) {
                auto entry = requestHolder.MutableDeleteMessageBatch()->AddEntries();
                entry->SetId(requestEntry.Getid());
                entry->SetReceiptHandle(requestEntry.Getreceipt_handle());
            }
            return result;
        }
    };

    class TChangeMessageVisibilityBatchReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TChangeMessageVisibilityBatchResponse,
            Ydb::Ymq::V1::ChangeMessageVisibilityBatchResult> {
    public:
        using TReplyCallback::TReplyCallback;

    private:
        const NKikimr::NSQS::TChangeMessageVisibilityBatchResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetChangeMessageVisibilityBatch();
        }

        Ydb::Ymq::V1::ChangeMessageVisibilityBatchResult GetResult(const NKikimrClient::TSqsResponse& response) override {
            Ydb::Ymq::V1::ChangeMessageVisibilityBatchResult result;
            for (auto& entry : response.GetChangeMessageVisibilityBatch().GetEntries()) {
                if (entry.GetError().HasErrorCode()) {
                    auto currentFailed = result.Addfailed();
                    currentFailed->Setcode(entry.GetError().GetErrorCode());
                    currentFailed->Setid(entry.GetId());
                    currentFailed->Setmessage(entry.GetError().GetMessage());

                    ui32 httpStatus = NSQS::TErrorClass::GetHttpStatus(entry.GetError().GetErrorCode()).GetOrElse(400);
                    currentFailed->Setsender_fault(400 <= httpStatus && httpStatus < 500);
                } else {
                    auto currentSuccessful = result.Addsuccessful();
                    currentSuccessful->Setid(entry.GetId());
                }
            }
            return result;
        }
    };

    class TChangeMessageVisibilityBatchActor : public TRpcRequestActor<
            TEvYmqChangeMessageVisibilityBatchRequest,
            NKikimr::NSQS::TChangeMessageVisibilityBatchRequest,
            TChangeMessageVisibilityBatchReplyCallback> {
    public:
        using TRpcRequestActor::TRpcRequestActor;

    private:
        NKikimr::NSQS::TChangeMessageVisibilityBatchRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableChangeMessageVisibilityBatch();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->Getqueue_url()).second);
            for (auto& requestEntry : GetProtoRequest()->Getentries()) {
                auto entry = requestHolder.MutableChangeMessageVisibilityBatch()->MutableEntries()->Add();
                entry->SetId(requestEntry.Getid());
                entry->SetReceiptHandle(requestEntry.Getreceipt_handle());
                COPY_FIELD_IF_PRESENT_IN_ENTRY(visibility_timeout, VisibilityTimeout)
            }
            return result;
        }
    };

    class TListQueueTagsReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TListQueueTagsResponse,
            Ydb::Ymq::V1::ListQueueTagsResult> {
    public:
        TListQueueTagsReplyCallback (
                std::shared_ptr<NKikimr::NGRpcService::IRequestOpCtx> request)
            : TReplyCallback<
                NKikimr::NSQS::TListQueueTagsResponse,
                Ydb::Ymq::V1::ListQueueTagsResult>(request)
        {
        }

    private:
        const NKikimr::NSQS::TListQueueTagsResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetListQueueTags();
        }

        Ydb::Ymq::V1::ListQueueTagsResult GetResult(const NKikimrClient::TSqsResponse& resp) override {
            Ydb::Ymq::V1::ListQueueTagsResult result;
            const auto& tags = GetResponse(resp);

            for (const auto& t: tags.GetTags()) {
                (*result.mutable_tags())[t.GetKey()] = t.GetValue();
            }

            return result;
        }
    };

    class TListQueueTagsActor : public TBaseRpcRequestActor<
            TEvYmqListQueueTagsRequest,
            NKikimr::NSQS::TListQueueTagsRequest,
            TListQueueTagsReplyCallback> {
    public:
        using TBaseRpcRequestActor::TBaseRpcRequestActor;

    private:
        NKikimr::NSQS::TListQueueTagsRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableListQueueTags();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            return result;
        }

        THolder<TListQueueTagsReplyCallback> CreateReplyCallback() override {
            return MakeHolder<TListQueueTagsReplyCallback>(Request_);
        }
    };

    class TTagQueueReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TTagQueueResponse,
            Ydb::Ymq::V1::TagQueueResult> {
    public:
        TTagQueueReplyCallback (
                std::shared_ptr<NKikimr::NGRpcService::IRequestOpCtx> request)
            : TReplyCallback<
                NKikimr::NSQS::TTagQueueResponse,
                Ydb::Ymq::V1::TagQueueResult>(request)
        {
        }

    private:
        const NKikimr::NSQS::TTagQueueResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetTagQueue();
        }

        Ydb::Ymq::V1::TagQueueResult GetResult(const NKikimrClient::TSqsResponse&) override {
            Ydb::Ymq::V1::TagQueueResult result;
            return result;
        }
    };

    class TTagQueueActor : public TBaseRpcRequestActor<
            TEvYmqTagQueueRequest,
            NKikimr::NSQS::TTagQueueRequest,
            TTagQueueReplyCallback> {
    public:
        using TBaseRpcRequestActor::TBaseRpcRequestActor;

    private:
        NKikimr::NSQS::TTagQueueRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableTagQueue();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            for (const auto& [key, value]: GetProtoRequest()->Gettags()) {
                auto tag = requestHolder.MutableTagQueue()->MutableTags()->Add();
                tag->SetKey(key);
                tag->SetValue(value);
            }
            return result;
        }

        THolder<TTagQueueReplyCallback> CreateReplyCallback() override {
            return MakeHolder<TTagQueueReplyCallback>(Request_);
        }
    };

    class TUntagQueueReplyCallback : public TReplyCallback<
            NKikimr::NSQS::TUntagQueueResponse,
            Ydb::Ymq::V1::UntagQueueResult> {
    public:
        TUntagQueueReplyCallback (
                std::shared_ptr<NKikimr::NGRpcService::IRequestOpCtx> request)
            : TReplyCallback<
                NKikimr::NSQS::TUntagQueueResponse,
                Ydb::Ymq::V1::UntagQueueResult>(request)
        {
        }

    private:
        const NKikimr::NSQS::TUntagQueueResponse& GetResponse(const NKikimrClient::TSqsResponse& resp) override {
            return resp.GetUntagQueue();
        }

        Ydb::Ymq::V1::UntagQueueResult GetResult(const NKikimrClient::TSqsResponse&) override {
            Ydb::Ymq::V1::UntagQueueResult result;
            return result;
        }
    };

    class TUntagQueueActor : public TBaseRpcRequestActor<
            TEvYmqUntagQueueRequest,
            NKikimr::NSQS::TUntagQueueRequest,
            TUntagQueueReplyCallback> {
    public:
        using TBaseRpcRequestActor::TBaseRpcRequestActor;

    private:
        NKikimr::NSQS::TUntagQueueRequest* GetRequest(TSqsRequest& requestHolder) override {
            auto result = requestHolder.MutableUntagQueue();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url()).second);
            for (const auto& key: GetProtoRequest()->Gettag_keys()) {
                requestHolder.MutableUntagQueue()->AddTagKeys(key);
            }
            return result;
        }

        THolder<TUntagQueueReplyCallback> CreateReplyCallback() override {
            return MakeHolder<TUntagQueueReplyCallback>(Request_);
        }
    };

    #undef COPY_FIELD_IF_PRESENT
    #undef COPY_FIELD_IF_PRESENT_IN_ENTRY
}

namespace NKikimr::NGRpcService {

using namespace NYmq::V1;

#define DECLARE_RPC(name) template<> IActor* TEvYmq##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { \
    return new T##name##Actor(msg);\
}\
void DoYmq##name##Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {\
    TActivationContext::AsActorContext().Register(new T##name##Actor(p.release())); \
}

DECLARE_RPC(GetQueueUrl);
DECLARE_RPC(CreateQueue);
DECLARE_RPC(SendMessage);
DECLARE_RPC(ReceiveMessage);
DECLARE_RPC(GetQueueAttributes);
DECLARE_RPC(ListQueues);
DECLARE_RPC(DeleteMessage);
DECLARE_RPC(PurgeQueue);
DECLARE_RPC(DeleteQueue);
DECLARE_RPC(ChangeMessageVisibility);
DECLARE_RPC(SetQueueAttributes);
DECLARE_RPC(SendMessageBatch);
DECLARE_RPC(DeleteMessageBatch);
DECLARE_RPC(ChangeMessageVisibilityBatch);
DECLARE_RPC(ListDeadLetterSourceQueues);
DECLARE_RPC(ListQueueTags);
DECLARE_RPC(TagQueue);
DECLARE_RPC(UntagQueue);

}
