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

    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    // Names of queue attributes
    static const TString APPROXIMATE_NUMBER_OF_MESSAGES = "ApproximateNumberOfMessages";
    static const TString APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED = "ApproximateNumberOfMessagesDelayed";
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
                Request->SendResult(this->GetResult(resp), Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS);
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
            auto requestHolder = MakeHolder<TSqsRequest>();

            requestHolder->SetRequestId(RequestId);

            auto request = GetRequest(requestHolder);

            request->MutableAuth()->SetUserName(CloudId);
            request->MutableAuth()->SetFolderId(FolderId);
            request->MutableAuth()->SetUserSID(UserSid);

            if (SecurityToken) {
                request->MutableCredentials()->SetOAuthToken(SecurityToken);
            }

            auto actor = CreateProxyActionActor(*requestHolder.Release(), CreateReplyCallback(), true);
            ctx.RegisterWithSameMailbox(actor);

            TBase::Die(ctx);
        }

    protected:
        virtual TRequest* GetRequest(THolder<TSqsRequest>&) = 0;
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
        NKikimr::NSQS::TGetQueueUrlRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableGetQueueUrl();
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
        NKikimr::NSQS::TCreateQueueRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableCreateQueue();
            result->SetQueueName(GetProtoRequest()->queue_name());

            for (auto &srcAttribute : GetProtoRequest()->attributes()) {
                auto dstAttribute = result->MutableAttributes()->Add();
                dstAttribute->SetName(srcAttribute.first);
                dstAttribute->SetValue(srcAttribute.second);
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
            result.set_md5_of_message_attributes(GetResponse(resp).GetMD5OfMessageAttributes());
            result.set_md5_of_message_body(GetResponse(resp).GetMD5OfMessageBody());
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
        NKikimr::NSQS::TSendMessageRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableSendMessage();
            for (auto& srcAttribute: GetProtoRequest()->Getmessage_attributes()) {
                auto dstAttribute = result->MutableMessageAttributes()->Add();
                dstAttribute->SetName(srcAttribute.first);
                dstAttribute->SetStringValue(srcAttribute.second.Getstring_value());
                dstAttribute->SetBinaryValue(srcAttribute.second.Getbinary_value());
                dstAttribute->SetDataType(srcAttribute.second.Getdata_type());
            }
            result->SetMessageDeduplicationId(GetProtoRequest()->message_deduplication_id());
            result->SetMessageGroupId(GetProtoRequest()->message_group_id());
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
            result->SetMessageBody(GetProtoRequest()->message_body());

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

                for (TString& attributeName : AttributesNames) {
                    if (attributeName == APPROXIMATE_RECEIVE_COUNT) {
                        dstMessage.Mutableattributes()->at(attributeName)
                            .assign(srcMessage.GetApproximateReceiveCount());
                    } else if (attributeName == APPROXIMATE_FIRST_RECEIVE_TIMESTAMP) {
                        dstMessage.Mutableattributes()->at(attributeName)
                            .assign(srcMessage.GetApproximateFirstReceiveTimestamp());
                    } else if (attributeName == MESSAGE_DEDUPLICATION_ID) {
                        dstMessage.Mutableattributes()->at(attributeName)
                            .assign(srcMessage.GetMessageDeduplicationId());
                    } else if (attributeName == MESSAGE_GROUP_ID) {
                        dstMessage.Mutableattributes()->at(attributeName)
                            .assign(srcMessage.GetMessageGroupId());
                    } else if (attributeName == SENDER_ID) {
                        dstMessage.Mutableattributes()->at(attributeName)
                            .assign(srcMessage.GetSenderId());
                    } else if (attributeName == SENT_TIMESTAMP) {
                        dstMessage.Mutableattributes()->at(attributeName)
                            .assign(srcMessage.GetSentTimestamp());
                    } else if (attributeName == SEQUENCE_NUMBER) {
                        dstMessage.Mutableattributes()->at(attributeName)
                            .assign(srcMessage.GetSequenceNumber());
                    }
                }

                dstMessage.set_body(srcMessage.GetData());
                dstMessage.set_md5_of_body(srcMessage.GetMD5OfMessageBody());
                dstMessage.set_md5_of_message_attributes(srcMessage.GetMD5OfMessageAttributes());

                for (const auto& srcAttribute: srcMessage.GetMessageAttributes()) {
                    Ydb::Ymq::V1::MessageAttribute dstAttribute;

                    dstAttribute.set_binary_value(srcAttribute.GetBinaryValue());
                    dstAttribute.set_data_type(srcAttribute.GetDataType());
                    dstAttribute.set_string_value(srcAttribute.GetStringValue());
                    dstMessage.mutable_message_attributes()->emplace(srcAttribute.GetName(), dstAttribute);
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
        NKikimr::NSQS::TReceiveMessageRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableReceiveMessage();
            auto systemAttributeNames = GetProtoRequest()->Getmessage_system_attribute_names();
            // We ignore AttributeNames if SystemAttributeNames is present,
            // because AttributeNames is deprecated in favour of SystemAttributeNames
            if (systemAttributeNames.size() > 0) {
                for (int i = 0; i < systemAttributeNames.size(); i++) {
                    result->SetAttributeName(i, systemAttributeNames[i]);
                }
            } else {
                auto attributeNames = GetProtoRequest()->Getattribute_names();
                for (int i = 0; i < attributeNames.size(); i++) {
                    result->SetAttributeName(i, attributeNames[i]);
                }
            }
            result->SetMaxNumberOfMessages(GetProtoRequest()->max_number_of_messages());
            for (int i = 0; i < GetProtoRequest()->Getmessage_attribute_names().size(); i++) {
                result->SetMessageAttributeName(i, GetProtoRequest()->Getmessage_attribute_names()[i]);
            }
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
            result->SetReceiveRequestAttemptId(GetProtoRequest()->Getreceive_request_attempt_id());
            result->SetVisibilityTimeout(GetProtoRequest()->Getvisibility_timeout());
            result->SetWaitTimeSeconds(GetProtoRequest()->Getwait_time_seconds());
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
        result.Mutableattributes()->emplace(name, std::to_string(value));
    };

    template<>
    void AddAttribute<TString>(Ydb::Ymq::V1::GetQueueAttributesResult& result, const TString& name, TString value) {
        result.Mutableattributes()->emplace(name, value);
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
            for (const auto& attributeName : Attributes) {
                if (attributeName == APPROXIMATE_NUMBER_OF_MESSAGES) {
                    AddAttribute(
                        result,
                        APPROXIMATE_NUMBER_OF_MESSAGES,
                        GetResponse(resp).GetApproximateNumberOfMessages()
                    );
                } else if (attributeName == APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED) {
                    AddAttribute(
                        result,
                        APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
                        GetResponse(resp).GetApproximateNumberOfMessagesDelayed()
                    );
                } else if (attributeName == CREATED_TIMESTAMP) {
                    AddAttribute(
                        result,
                        CREATED_TIMESTAMP,
                        GetResponse(resp).GetCreatedTimestamp()
                    );
                } else if (attributeName == DELAY_SECONDS) {
                    AddAttribute(
                        result,
                        DELAY_SECONDS,
                        GetResponse(resp).GetDelaySeconds()
                    );
                } else if (attributeName == LAST_MODIFIED_TIMESTAMP) {
                    AddAttribute(
                        result,
                        LAST_MODIFIED_TIMESTAMP,
                        GetResponse(resp).GetLastModifiedTimestamp()
                    );
                } else if (attributeName == MAXIMUM_MESSAGE_SIZE) {
                    AddAttribute(
                        result,
                        MAXIMUM_MESSAGE_SIZE,
                        GetResponse(resp).GetMaximumMessageSize()
                    );
                } else if (attributeName == MESSAGE_RETENTION_PERIOD) {
                    AddAttribute(
                        result,
                        MESSAGE_RETENTION_PERIOD,
                        GetResponse(resp).GetMessageRetentionPeriod()
                    );
                } else if (attributeName == QUEUE_ARN) {
                    AddAttribute(
                        result,
                        QUEUE_ARN,
                        GetResponse(resp).GetQueueArn()
                    );
                } else if (attributeName == RECEIVE_MESSAGE_WAIT_TIME_SECONDS) {
                    AddAttribute(
                        result,
                        RECEIVE_MESSAGE_WAIT_TIME_SECONDS,
                        GetResponse(resp).GetReceiveMessageWaitTimeSeconds()
                    );
                } else if (attributeName == VISIBILITY_TIMEOUT) {
                    AddAttribute(
                        result,
                        VISIBILITY_TIMEOUT,
                        GetResponse(resp).GetVisibilityTimeout()
                    );
                } else if (attributeName == REDRIVE_POLICY) {
                    AddAttribute(
                        result,
                        REDRIVE_POLICY,
                        GetResponse(resp).GetRedrivePolicy()
                    );
                }
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
        NKikimr::NSQS::TGetQueueAttributesRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableGetQueueAttributes();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
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
        NKikimr::NSQS::TListQueuesRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableListQueues();
            result->SetQueueNamePrefix(GetProtoRequest()->Getqueue_name_prefix());
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
        NKikimr::NSQS::TDeleteMessageRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableDeleteMessage();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
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
        NKikimr::NSQS::TPurgeQueueRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutablePurgeQueue();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
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
        NKikimr::NSQS::TDeleteQueueRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableDeleteQueue();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
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
        NKikimr::NSQS::TChangeMessageVisibilityRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableChangeMessageVisibility();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
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


    void AddAttribute(THolder<TSqsRequest>& requestHolder, const TString& name, TString value) {
        auto attribute = requestHolder->MutableSetQueueAttributes()->MutableAttributes()->Add();
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
        NKikimr::NSQS::TSetQueueAttributesRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableSetQueueAttributes();
            for (auto& [name, value]: GetProtoRequest()->Getattributes()) {
                AddAttribute(requestHolder, name, value);
            }
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->queue_url())->second);
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
                    currentSuccessful->Setmd5_of_message_body(entry.GetMD5OfMessageBody());
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
        NKikimr::NSQS::TSendMessageBatchRequest* GetRequest(THolder<TSqsRequest>& requestHolder) override {
            auto result = requestHolder->MutableSendMessageBatch();
            result->SetQueueName(CloudIdAndResourceIdFromQueueUrl(GetProtoRequest()->Getqueue_url())->second);
            for (auto& requestEntry : GetProtoRequest()->Getentries()) {
                auto entry = requestHolder->MutableSendMessageBatch()->AddEntries();
                entry->SetId(requestEntry.Getid());
                for (auto& srcAttribute: requestEntry.Getmessage_attributes()) {
                    auto dstAttribute = entry->MutableMessageAttributes()->Add();
                    dstAttribute->SetName(srcAttribute.first);
                    dstAttribute->SetStringValue(srcAttribute.second.Getstring_value());
                    dstAttribute->SetBinaryValue(srcAttribute.second.Getbinary_value());
                    dstAttribute->SetDataType(srcAttribute.second.Getdata_type());
                }
                entry->SetMessageDeduplicationId(requestEntry.Getmessage_deduplication_id());
                entry->SetMessageGroupId(requestEntry.Getmessage_group_id());
                entry->SetMessageBody(requestEntry.Getmessage_body());
            }
            return result;
        }
    };
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

}
