#include "auth_factory.h"
#include "events.h"
#include "http_req.h"
#include "json_proto_conversion.h"
#include "custom_metrics.h"
#include "exceptions_mapping.h"

#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/json_output_create.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/proto2json_printer.h>
#include <library/cpp/uri/uri.h>

#include <ydb/core/security/ticket_parser_impl.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_caching/cached_grpc_request_actor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/base/path.h>

#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/services/sqs_topic/utils.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/iam_token_service.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>

#include <ydb/services/datastreams/datastreams_proxy.h>
#include <ydb/services/datastreams/next_token.h>
#include <ydb/services/datastreams/shard_iterator.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <ydb/services/ymq/grpc_service.h>
#include <ydb/services/ymq/ymq_proxy.h>

#include <ydb/public/api/grpc/draft/ydb_sqs_topic_v1.grpc.pb.h>
#include <ydb/services/sqs_topic/sqs_topic_proxy.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/vector.h>

#include <nlohmann/json.hpp>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/events.h>

#include <ydb/core/ymq/actor/auth_multi_factory.h>
#include <ydb/core/ymq/actor/serviceid.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>

#include <ydb/services/ymq/rpc_params.h>
#include <ydb/services/ymq/utils.h>

namespace NKikimr::NHttpProxy {

    using namespace google::protobuf;
    using namespace Ydb::DataStreams::V1;
    using namespace NYdb::NDataStreams::V1;

    template<class TProto>
    TString ExtractStreamNameWithoutProtoField(const TProto& req)
    {
        using namespace NKikimr::NDataStreams::V1;
        if constexpr (std::is_same<TProto, GetRecordsRequest>::value) {
            return TShardIterator(req.shard_iterator()).GetStreamName();
        }
        if constexpr (std::is_same<TProto, GetRecordsResult>::value) {
            return TShardIterator(req.next_shard_iterator()).GetStreamName();
        }
        if constexpr (std::is_same<TProto, ListStreamConsumersRequest>::value ||
                      std::is_same<TProto, ListStreamConsumersResult>::value) {
            TNextToken tkn(req.next_token());
            return tkn.IsValid() ? tkn.GetStreamName() : req.stream_arn();
        }
        if constexpr (std::is_same<TProto, ListShardsRequest>::value ||
                      std::is_same<TProto, ListShardsResult>::value) {
            TNextToken tkn(req.next_token());
            return tkn.IsValid() ? tkn.GetStreamName() : req.stream_name();
        }
        return "";
    }

    template<class TProto>
    TString ExtractStreamName(const TProto& req)
    {
        constexpr bool has_stream_name = requires(const TProto& t) {
            t.stream_name();
        };

        if constexpr (has_stream_name) {
            return req.stream_name();
        } else {
            return ExtractStreamNameWithoutProtoField(req);
        }
    }

    template<class TProto>
    TString TruncateStreamName(const TProto& req, const TString& databasePath)
    {
        constexpr bool has_stream_name = requires(const TProto& t) {
            t.stream_name();
        };

        if constexpr (has_stream_name) {
            Y_ABORT_UNLESS(req.stream_name().StartsWith(databasePath));
            return req.stream_name().substr(databasePath.size(), -1);
        }
        return ExtractStreamNameWithoutProtoField<TProto>(req).substr(databasePath.size(), -1);
    }

    template <class TRequest>
    TString MaybeGetQueueUrl(const TRequest& request) {
        if constexpr (requires {request.Getqueue_url(); } ) {
            return request.Getqueue_url();
        }
        return {};
    }

    template <class TRequest>
    std::expected<TString, TString> MaybeGetDatabasePathFromSQSTopicQueueUrl(const TRequest& request) {
        TString queueUrl = MaybeGetQueueUrl(request);
        if (queueUrl.empty()) {
            return {};
        }
        auto parsedQueueUrl = NKikimr::NSqsTopic::ParseQueueUrl(queueUrl);
        if (!parsedQueueUrl.has_value()) {
            return std::unexpected(std::move(parsedQueueUrl).error());
        }
        return parsedQueueUrl->Database;
    }

    constexpr TStringBuf IAM_HEADER = "x-yacloud-subjecttoken";
    constexpr TStringBuf SECURITY_TOKEN_HEADER = "x-amz-security-token";
    constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";
    constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";
    constexpr TStringBuf REQUEST_ID_HEADER_EXT = "x-amzn-requestid";
    constexpr TStringBuf REQUEST_DATE_HEADER = "x-amz-date";
    constexpr TStringBuf REQUEST_FORWARDED_FOR = "x-forwarded-for";
    constexpr TStringBuf REQUEST_TARGET_HEADER = "x-amz-target";
    constexpr TStringBuf REQUEST_CONTENT_TYPE_HEADER = "content-type";
    constexpr TStringBuf CREDENTIAL_PARAM = "Credential";





    template<class TProtoRequest>
    TString ExtractQueueName(TProtoRequest& request) {
        return request.GetQueueUrl();
    };

    void THttpRequestProcessors::Initialize() {
        #define DECLARE_DATASTREAMS_PROCESSOR(name) Name2DataStreamsProcessor[#name] = MakeHolder<THttpRequestProcessor<DataStreamsService, name##Request, name##Response, name##Result,\
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::Async##name), NKikimr::NGRpcService::TEvDataStreams##name##Request>> \
                    (#name, &Ydb::DataStreams::V1::DataStreamsService::Stub::Async##name);

        DECLARE_DATASTREAMS_PROCESSOR(PutRecords);
        DECLARE_DATASTREAMS_PROCESSOR(CreateStream);
        DECLARE_DATASTREAMS_PROCESSOR(ListStreams);
        DECLARE_DATASTREAMS_PROCESSOR(DeleteStream);
        DECLARE_DATASTREAMS_PROCESSOR(UpdateStream);
        DECLARE_DATASTREAMS_PROCESSOR(DescribeStream);
        DECLARE_DATASTREAMS_PROCESSOR(ListShards);
        DECLARE_DATASTREAMS_PROCESSOR(PutRecord);
        DECLARE_DATASTREAMS_PROCESSOR(GetRecords);
        DECLARE_DATASTREAMS_PROCESSOR(GetShardIterator);
        DECLARE_DATASTREAMS_PROCESSOR(DescribeLimits);
        DECLARE_DATASTREAMS_PROCESSOR(DescribeStreamSummary);
        DECLARE_DATASTREAMS_PROCESSOR(DecreaseStreamRetentionPeriod);
        DECLARE_DATASTREAMS_PROCESSOR(IncreaseStreamRetentionPeriod);
        DECLARE_DATASTREAMS_PROCESSOR(UpdateShardCount);
        DECLARE_DATASTREAMS_PROCESSOR(UpdateStreamMode);
        DECLARE_DATASTREAMS_PROCESSOR(RegisterStreamConsumer);
        DECLARE_DATASTREAMS_PROCESSOR(DeregisterStreamConsumer);
        DECLARE_DATASTREAMS_PROCESSOR(DescribeStreamConsumer);
        DECLARE_DATASTREAMS_PROCESSOR(ListStreamConsumers);
        DECLARE_DATASTREAMS_PROCESSOR(AddTagsToStream);
        DECLARE_DATASTREAMS_PROCESSOR(DisableEnhancedMonitoring);
        DECLARE_DATASTREAMS_PROCESSOR(EnableEnhancedMonitoring);
        DECLARE_DATASTREAMS_PROCESSOR(ListTagsForStream);
        DECLARE_DATASTREAMS_PROCESSOR(MergeShards);
        DECLARE_DATASTREAMS_PROCESSOR(RemoveTagsFromStream);
        DECLARE_DATASTREAMS_PROCESSOR(SplitShard);
        DECLARE_DATASTREAMS_PROCESSOR(StartStreamEncryption);
        DECLARE_DATASTREAMS_PROCESSOR(StopStreamEncryption);
        #undef DECLARE_DATASTREAMS_PROCESSOR


        #define DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(name) Name2YmqProcessor[#name] = MakeHolder<TYmqHttpRequestProcessor<Ydb::Ymq::V1::YmqService, Ydb::Ymq::V1::name##Request, Ydb::Ymq::V1::name##Response, Ydb::Ymq::V1::name##Result,\
                    decltype(&Ydb::Ymq::V1::YmqService::Stub::AsyncYmq##name), NKikimr::NGRpcService::TEvYmq##name##Request>> \
                    (#name, &Ydb::Ymq::V1::YmqService::Stub::AsyncYmq##name, [](Ydb::Ymq::V1::name##Request&){return "";});
        DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(GetQueueUrl);
        DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(CreateQueue);
        DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(ListQueues);
        #undef DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN

        #define DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(name) Name2YmqProcessor[#name] = MakeHolder<TYmqHttpRequestProcessor<Ydb::Ymq::V1::YmqService, Ydb::Ymq::V1::name##Request, Ydb::Ymq::V1::name##Response, Ydb::Ymq::V1::name##Result,\
                    decltype(&Ydb::Ymq::V1::YmqService::Stub::AsyncYmq##name), NKikimr::NGRpcService::TEvYmq##name##Request>> \
                    (#name, &Ydb::Ymq::V1::YmqService::Stub::AsyncYmq##name, [](Ydb::Ymq::V1::name##Request& request){return request.Getqueue_url();});
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(SendMessage);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(ReceiveMessage);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(GetQueueAttributes);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(DeleteMessage);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(PurgeQueue);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(DeleteQueue);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(ChangeMessageVisibility);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(SetQueueAttributes);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(SendMessageBatch);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(DeleteMessageBatch);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(ChangeMessageVisibilityBatch);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(ListDeadLetterSourceQueues);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(ListQueueTags);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(TagQueue);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(UntagQueue);
        #undef DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN

#define DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN(name) Name2SqsTopicProcessor[#name] = MakeHolder<TSqsTopicHttpRequestProcessor<     \
                                                            Ydb::SqsTopic::V1::SqsTopicService,                                       \
                                                            Ydb::Ymq::V1::name##Request,                                              \
                                                            Ydb::Ymq::V1::name##Response,                                             \
                                                            Ydb::Ymq::V1::name##Result,                                               \
                                                            decltype(&Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name), \
                                                            NKikimr::NGRpcService::TEvSqsTopic##name##Request>>(#name, &Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name)

        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN(GetQueueUrl);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN(ListQueues);

#undef DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN

#define DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(name) Name2SqsTopicProcessor[#name] = MakeHolder<TSqsTopicHttpRequestProcessor< \
                                                          Ydb::SqsTopic::V1::SqsTopicService,                                   \
                                                          Ydb::Ymq::V1::name##Request,                                     \
                                                          Ydb::Ymq::V1::name##Response,                                    \
                                                          Ydb::Ymq::V1::name##Result,                                      \
                                                          decltype(&Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name),       \
                                                          NKikimr::NGRpcService::TEvSqsTopic##name##Request>>(#name, &Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name)
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(CreateQueue);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(DeleteMessage);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(DeleteQueue);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(GetQueueAttributes);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(ReceiveMessage);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(SendMessage);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(SendMessageBatch);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(SetQueueAttributes);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(DeleteMessageBatch);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(ChangeMessageVisibility);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(ChangeMessageVisibilityBatch);
        DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(PurgeQueue);
#undef DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN
    }

    void SetApiVersionDisabledErrorText(THttpRequestContext& context) {
        context.ResponseData.ErrorText = (TStringBuilder() << context.ApiVersion << " is disabled");
    }

    bool THttpRequestProcessors::Execute(const TString& name, THttpRequestContext&& context,
                                         THolder<NKikimr::NSQS::TAwsRequestSignV4> signature,
                                         const TActorContext& ctx) {
        const THashMap<TString, THolder<IHttpRequestProcessor>>* Name2Processor;
        if (context.ApiVersion == "AmazonSQS") {
            if (!context.ServiceConfig.GetHttpConfig().GetYmqEnabled() && !context.ServiceConfig.GetHttpConfig().GetSqsTopicEnabled()) {
                context.ResponseData.IsYmq = true;
                context.ResponseData.UseYmqStatusCode = true;
                context.ResponseData.YmqHttpCode = 400;
                SetApiVersionDisabledErrorText(context);
            }
            if (context.ServiceConfig.GetHttpConfig().GetSqsTopicEnabled()) {
                Name2Processor = &Name2SqsTopicProcessor;
            } else {
                Name2Processor = &Name2YmqProcessor;
            }
        } else {
            if (!context.ServiceConfig.GetHttpConfig().GetDataStreamsEnabled()) {
                context.ResponseData.Status = NYdb::EStatus::BAD_REQUEST;
                SetApiVersionDisabledErrorText(context);
            }
            Name2Processor = &Name2DataStreamsProcessor;
        }

        if (auto proc = Name2Processor->find(name); proc != Name2Processor->end()) {
            proc->second->Execute(std::move(context), std::move(signature), ctx);
            return true;
        }
        else if (name.empty()) {
            context.ResponseData.Status = NYdb::EStatus::UNSUPPORTED;
            context.ResponseData.ErrorText = TStringBuilder() << "Unknown method name " << name;
            context.DoReply(ctx, static_cast<size_t>(NYds::EErrorCodes::MISSING_ACTION));
        }
        else {
            context.ResponseData.Status = NYdb::EStatus::UNSUPPORTED;
            context.ResponseData.ErrorText = TStringBuilder() << "Missing method name " << name;
            context.DoReply(ctx);
        }
        return false;
    }


    TString GenerateRequestId(const TString& sourceReqId) {
        if (!sourceReqId.empty()) {
            return CreateGuidAsString() + "-" + sourceReqId;
        } else {
            return CreateGuidAsString();
        }
    }

    THttpRequestContext::THttpRequestContext(
        const NKikimrConfig::TServerlessProxyConfig& config,
        NHttp::THttpIncomingRequestPtr request,
        NActors::TActorId sender,
        NYdb::TDriver* driver,
        std::shared_ptr<NYdb::ICredentialsProvider> serviceAccountCredentialsProvider)
        : ServiceConfig(config)
        , Request(request)
        , Sender(sender)
        , Driver(driver)
        , ServiceAccountCredentialsProvider(serviceAccountCredentialsProvider) {
        char address[INET6_ADDRSTRLEN];
        if (inet_ntop(AF_INET6, &(Request->Address), address, INET6_ADDRSTRLEN) == nullptr) {
            SourceAddress = "unknown";
        } else {
            SourceAddress = address;
        }

        DatabasePath = Request->URL.Before('?');
        if (DatabasePath == "/") {
           DatabasePath = "";
        }
        auto params = TCgiParameters(Request->URL.After('?'));
        if (auto it = params.Find("folderId"); it != params.end()) {
            FolderId = it->second;
        }

        //TODO: find out databaseId
        ParseHeaders(Request->Headers);
    }

    THolder<NKikimr::NSQS::TAwsRequestSignV4> THttpRequestContext::GetSignature() {
        THolder<NKikimr::NSQS::TAwsRequestSignV4> signature;
        if (IamToken.empty()) {
            const TString fullRequest = TString(Request->Method) + " " +
                Request->URL + " " +
                Request->Protocol + "/" + Request->Version + "\r\n" +
                Request->Headers +
                Request->Body;
            signature = MakeHolder<NKikimr::NSQS::TAwsRequestSignV4>(fullRequest);
        }

        return signature;
    }

    void THttpRequestContext::DoReply(const TActorContext& ctx, size_t issueCode) {
        auto createResponse = [this](const auto& request,
                                     TStringBuf status,
                                     TStringBuf message,
                                     TStringBuf contentType,
                                     TStringBuf body) {
            NHttp::THttpOutgoingResponsePtr response =
                new NHttp::THttpOutgoingResponse(request, "HTTP", "1.1", status, message);
            response->Set<&NHttp::THttpResponse::Connection>(request->GetConnection());
            response->Set(REQUEST_ID_HEADER_EXT, RequestId);
            if (!contentType.empty() && !body.empty()) {
                response->Set<&NHttp::THttpResponse::ContentType>(contentType);
                if (!request->Endpoint->CompressContentTypes.empty()) {
                    contentType = NHttp::Trim(contentType.Before(';'), ' ');
                    if (Count(request->Endpoint->CompressContentTypes, contentType) != 0) {
                        response->EnableCompression();
                    }
                }
            }

            if (response->IsNeedBody() || !body.empty()) {
                if (request->Method == "HEAD") {
                    response->Set<&NHttp::THttpResponse::ContentLength>(ToString(body.size()));
                } else {
                    response->SetBody(body);
                }
            }
            return response;
        };
        auto strByMimeAws = [](MimeTypes contentType) {
            switch (contentType) {
            case MIME_JSON:
                return "application/x-amz-json-1.1";
            case MIME_CBOR:
                return "application/x-amz-cbor-1.1";
            default:
                return strByMime(contentType);
            }
        };

        if (ResponseData.Status == NYdb::EStatus::SUCCESS) {
            LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, "reply ok");
        } else {
            LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                          "reply with status: " << ResponseData.Status <<
                          " message: " << ResponseData.ErrorText);
            ResponseData.Body.SetType(NJson::JSON_MAP);
            ResponseData.Body["message"] = ResponseData.ErrorText;
            if (ResponseData.UseYmqStatusCode) {
                ResponseData.Body["__type"] = ResponseData.YmqStatusCode;
            } else {
                ResponseData.Body["__type"] = MapToException(ResponseData.Status, MethodName, issueCode).first;
            }
        }

        TString errorName;
        ui32 httpCode;
        if (ResponseData.UseYmqStatusCode) {
            httpCode = ResponseData.YmqHttpCode;
            errorName = ResponseData.YmqStatusCode;
        } else {
            std::tie(errorName, httpCode) = MapToException(ResponseData.Status, MethodName, issueCode);
        }
        auto response = createResponse(
            Request,
            TStringBuilder() << (ui32)httpCode,
            errorName,
            strByMimeAws(ContentType),
            ResponseData.DumpBody(ContentType)
        );

        if (ResponseData.IsYmq && ServiceConfig.GetHttpConfig().GetYandexCloudMode()) {
            // Send request attributes to the metering actor
            auto reportRequestAttributes = MakeHolder<NSQS::TSqsEvents::TEvReportProcessedRequestAttributes>();

            auto& requestAttributes = reportRequestAttributes->Data;

            requestAttributes.HttpStatusCode = httpCode;
            requestAttributes.IsFifo = ResponseData.YmqIsFifo;
            requestAttributes.FolderId = FolderId;
            requestAttributes.RequestSizeInBytes = Request->Size();
            requestAttributes.ResponseSizeInBytes = response->Size();
            requestAttributes.SourceAddress = SourceAddress;
            requestAttributes.ResourceId = ResourceId;
            requestAttributes.Action = NSQS::ActionFromString(MethodName);
            for (const auto& [k, v] : ResponseData.QueueTags) {
                requestAttributes.QueueTags[k] = v;
            }

            LOG_SP_DEBUG_S(
                ctx,
                NKikimrServices::HTTP_PROXY,
                TStringBuilder() << "Send metering event."
                << " HttpStatusCode: " << requestAttributes.HttpStatusCode
                << " IsFifo: " << requestAttributes.IsFifo
                << " FolderId: " << requestAttributes.FolderId
                << " RequestSizeInBytes: " << requestAttributes.RequestSizeInBytes
                << " ResponseSizeInBytes: " << requestAttributes.ResponseSizeInBytes
                << " SourceAddress: " << requestAttributes.SourceAddress
                << " ResourceId: " << requestAttributes.ResourceId
                << " Action: " << requestAttributes.Action
            );

            ctx.Send(NSQS::MakeSqsMeteringServiceID(), reportRequestAttributes.Release());
        }

        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    TMaybe<TStringBuf> ExtractUserName(const TStringBuf& authorizationHeader) {
        const size_t spacePos = authorizationHeader.find(' ');
        if (spacePos == TString::npos) {
            return Nothing();
        }
        auto restOfHeader = authorizationHeader.substr(spacePos + 1);
        if (restOfHeader.StartsWith(CREDENTIAL_PARAM)) {
            const size_t equalsPos = restOfHeader.find('=');
            if (equalsPos == TString::npos) {
                return Nothing();
            }
            const size_t slashPos = restOfHeader.find('/');
            if (slashPos == TString::npos || slashPos < equalsPos) {
                return Nothing();
            }
            return restOfHeader.substr(equalsPos + 1, slashPos - equalsPos - 1);
        }
        return Nothing();
    }

    void THttpRequestContext::ParseHeaders(TStringBuf str) {
        TString sourceReqId;
        NHttp::THeaders headers(str);
        for (const auto& header : headers.Headers) {
            if (AsciiEqualsIgnoreCase(header.first, IAM_HEADER)) {
                IamToken = header.second;
            } else if(AsciiEqualsIgnoreCase(header.first, SECURITY_TOKEN_HEADER)) {
                SecurityToken = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, AUTHORIZATION_HEADER)) {
                if (header.second.StartsWith("Bearer ")) {
                    IamToken = header.second;
                } else {
                    auto userName = ExtractUserName(header.second);
                    if (userName.Defined()) {
                        UserName = userName.GetRef();
                    }
                }
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_ID_HEADER)) {
                sourceReqId = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_FORWARDED_FOR)) {
                SourceAddress = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_TARGET_HEADER)) {
                TString requestTarget = TString(header.second);
                TVector<TString> parts = SplitString(requestTarget, ".");
                ApiVersion = parts.size() > 0 ? parts[0] : "";
                MethodName = parts.size() > 1 ? parts[1] : "";
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_CONTENT_TYPE_HEADER)) {
                ContentType = mimeByStr(header.second);
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_DATE_HEADER)) {
            }
        }
        RequestId = GenerateRequestId(sourceReqId);
    }

    TString THttpResponseData::DumpBody(MimeTypes contentType) {
        // according to https://json.nlohmann.me/features/binary_formats/cbor/#serialization
        auto cborBinaryTagBySize = [](size_t size) -> ui8 {
            if (size <= 23) {
                return 0x40 + static_cast<ui32>(size);
            } else if (size <= 255) {
                return 0x58;
            } else if (size <= 65536) {
                return 0x59;
            }

            return 0x5A;
        };
        switch (contentType) {
        case MIME_CBOR: {
            bool gotData = false;
            std::function<bool(int, nlohmann::json::parse_event_t, nlohmann::basic_json<>&)> bz =
                [&gotData, &cborBinaryTagBySize](int, nlohmann::json::parse_event_t event, nlohmann::json& parsed) {
                    if (event == nlohmann::json::parse_event_t::key and parsed == nlohmann::json("Data")) {
                        gotData = true;
                        return true;
                    }
                    if (event == nlohmann::json::parse_event_t::value and gotData) {
                        gotData = false;
                        std::string data = parsed.get<std::string>();
                        parsed = nlohmann::json::binary({data.begin(), data.end()},
                                                        cborBinaryTagBySize(data.size()));
                        return true;
                    }
                    return true;
                };

            auto toCborStr = NJson::WriteJson(Body, false);
            auto json =
                nlohmann::json::parse(TStringBuf(toCborStr).begin(), TStringBuf(toCborStr).end(), bz, false);
            auto toCbor = nlohmann::json::to_cbor(json);
            return {(char*)&toCbor[0], toCbor.size()};
        }
        default: {
        case MIME_JSON:
            return NJson::WriteJson(Body, false);
        }
        }
    }

    void THttpRequestContext::RequestBodyToProto(NProtoBuf::Message* request) {
        TStringBuf requestStr = Request->Body;
        if (requestStr.empty()) {
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                "Empty body";
        }

        // recursive is default setting
        if (auto listStreamsRequest = dynamic_cast<Ydb::DataStreams::V1::ListStreamsRequest*>(request)) {
            listStreamsRequest->set_recurse(true);
        }

        switch (ContentType) {
        case MIME_CBOR: {
            auto fromCbor = nlohmann::json::from_cbor(requestStr.begin(), requestStr.end(),
                                                      true, false,
                                                      nlohmann::json::cbor_tag_handler_t::ignore);
            if (fromCbor.is_discarded()) {
                throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                    "Can not parse request body from CBOR";
            } else {
                NlohmannJsonToProto(fromCbor, request);
            }
            break;
        }
        case MIME_JSON: {
            auto fromJson = nlohmann::json::parse(requestStr, nullptr, false);
            if (fromJson.is_discarded()) {
                throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                    "Can not parse request body from JSON";
            } else {
                NlohmannJsonToProto(fromJson, request);
            }
            break;
        }
        default:
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                "Unknown ContentType";
        }
    }

} // namespace NKikimr::NHttpProxy


template <>
void Out<NKikimr::NHttpProxy::THttpResponseData>(IOutputStream& o, const NKikimr::NHttpProxy::THttpResponseData& p) {
    TString s = TStringBuilder() << "NYdb status: " << std::to_string(static_cast<size_t>(p.Status)) <<
    ". Body: " << NJson::WriteJson(p.Body) << ". Error text: " << p.ErrorText;
    o.Write(s.data(), s.length());
}
