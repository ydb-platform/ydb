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

    TException MapToException(NYdb::EStatus status, const TString& method, size_t issueCode = ISSUE_CODE_ERROR) {
        auto IssueCode = static_cast<NYds::EErrorCodes>(issueCode);

        switch(status) {
        case NYdb::EStatus::SUCCESS:
            return TException("", HTTP_OK);
        case NYdb::EStatus::BAD_REQUEST:
            return BadRequestExceptions(method, IssueCode);
        case NYdb::EStatus::UNAUTHORIZED:
            return UnauthorizedExceptions(method, IssueCode);
        case NYdb::EStatus::INTERNAL_ERROR:
            return InternalErrorExceptions(method, IssueCode);
        case NYdb::EStatus::OVERLOADED:
            return OverloadedExceptions(method, IssueCode);
        case NYdb::EStatus::GENERIC_ERROR:
            return GenericErrorExceptions(method, IssueCode);
        case NYdb::EStatus::PRECONDITION_FAILED:
            return PreconditionFailedExceptions(method, IssueCode);
        case NYdb::EStatus::ALREADY_EXISTS:
            return AlreadyExistsExceptions(method, IssueCode);
        case NYdb::EStatus::SCHEME_ERROR:
            return SchemeErrorExceptions(method, IssueCode);
        case NYdb::EStatus::NOT_FOUND:
            return NotFoundExceptions(method, IssueCode);
        case NYdb::EStatus::UNSUPPORTED:
            return UnsupportedExceptions(method, IssueCode);
        case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
            return TException("Unauthenticated", HTTP_BAD_REQUEST);
        case NYdb::EStatus::ABORTED:
            return TException("Aborted", HTTP_BAD_REQUEST);
        case NYdb::EStatus::UNAVAILABLE:
            return TException("Unavailable", HTTP_SERVICE_UNAVAILABLE);
        case NYdb::EStatus::TIMEOUT:
            return TException("RequestExpired", HTTP_BAD_REQUEST);
        case NYdb::EStatus::BAD_SESSION:
            return TException("BadSession", HTTP_BAD_REQUEST);
        case NYdb::EStatus::SESSION_EXPIRED:
            return TException("SessionExpired", HTTP_BAD_REQUEST);
        default:
            return TException("InternalException", HTTP_INTERNAL_SERVER_ERROR);
        }
    }


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

    static TString LogHttpRequestResponseCommonInfoString(const THttpRequestContext& httpContext, TInstant startTime, TStringBuf api, TStringBuf topicPath, TStringBuf method, TStringBuf userSid, int httpCode, TStringBuf httpResponseMessage) {
        const TDuration duration = TInstant::Now() - startTime;
        TStringBuilder logString;
        logString << "Request done.";
        if (!api.empty()) {
            logString << " Api [" << api << "]";
        }
        if (!method.empty()) {
            logString << " Action [" << method << "]";
        }
        if (!httpContext.UserName.empty()) {
            logString << " User [" << httpContext.UserName << "]";
        }
        if (!httpContext.DatabasePath.empty()) {
            logString << " Database [" << httpContext.DatabasePath << "]";
        }
        if (!topicPath.empty()) {
            logString << " Queue [" << topicPath << "]";
        }
        logString << " IP [" << httpContext.SourceAddress << "] Duration [" << duration.MilliSeconds() << "ms]";
        if (!userSid.empty()) {
            logString << " Subject [" << userSid << "]";
        }
        logString << " Code [" << httpCode << "]";
        if (httpCode != 200) {
            logString << " Response [" << httpResponseMessage << "]";
        }
        return logString;
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


    template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoCall, class TRpcEv>
    class TBaseHttpRequestProcessor : public IHttpRequestProcessor {
    public:
        TBaseHttpRequestProcessor(TString method, TProtoCall protoCall)
            : Method(method)
            , ProtoCall(protoCall)
        {
        }

        const TString& Name() const override {
            return Method;
        }

        enum TRequestState {
            StateIdle,
            StateAuthentication,
            StateAuthorization,
            StateListEndpoints,
            StateGrpcRequest,
            StateFinished
        };
    protected:
        TString Method;
        TProtoCall ProtoCall;
    };

    template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoCall, class TRpcEv>
    class TYmqHttpRequestProcessor : public TBaseHttpRequestProcessor<TProtoService, TProtoRequest, TProtoResponse, TProtoResult, TProtoCall, TRpcEv>{
    using TProcessorBase = TBaseHttpRequestProcessor<TProtoService, TProtoRequest, TProtoResponse, TProtoResult, TProtoCall, TRpcEv>;
    public:
        TYmqHttpRequestProcessor(
                TString method,
                TProtoCall protoCall,
                std::function<TString(TProtoRequest&)> queueUrlExtractor)
            : TProcessorBase(method, protoCall)
            , QueueUrlExtractor(queueUrlExtractor)
        {
        }

        void Execute(THttpRequestContext&& context, THolder<NKikimr::NSQS::TAwsRequestSignV4> signature, const TActorContext& ctx) override {
            ctx.Register(
                new TYmqHttpRequestActor(
                    std::move(context),
                    std::move(signature),
                    TProcessorBase::ProtoCall,
                    TProcessorBase::Method,
                    QueueUrlExtractor
                )
            );
        }

    private:
        class TYmqHttpRequestActor : public NActors::TActorBootstrapped<TYmqHttpRequestActor> {
        public:
            using TBase = NActors::TActorBootstrapped<TYmqHttpRequestActor>;

            TYmqHttpRequestActor(
                    THttpRequestContext&& httpContext,
                    THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature,
                    TProtoCall protoCall,
                    const TString& method,
                    std::function<TString(TProtoRequest&)> queueUrlExtractor)
                : HttpContext(std::move(httpContext))
                , Signature(std::move(signature))
                , ProtoCall(protoCall)
                , Method(method)
                , QueueUrlExtractor(queueUrlExtractor)
            {
            }

            TStringBuilder LogPrefix() const {
                return HttpContext.LogPrefix();
            }

        private:
            STFUNC(StateWork)
            {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvents::TEvWakeup, HandleTimeout);
                    HFunc(TEvServerlessProxy::TEvGrpcRequestResult, HandleGrpcResponse);
                    HFunc(TEvYmqCloudAuthResponse, HandleYmqCloudAuthorizationResponse);
                    default:
                        HandleUnexpectedEvent(ev);
                        break;
                }
            }

            void SendGrpcRequestNoDriver(const TActorContext& ctx) {
                RequestState = TProcessorBase::TRequestState::StateGrpcRequest;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabasePath <<
                              "' iam token size: " << HttpContext.IamToken.size());
                TMap<TString, TString> peerMetadata {
                    {NYmq::V1::FOLDER_ID, FolderId},
                    {NYmq::V1::CLOUD_ID, CloudId ? CloudId : HttpContext.UserName },
                    {NYmq::V1::USER_SID, UserSid},
                    {NYmq::V1::REQUEST_ID, HttpContext.RequestId},
                    {NYmq::V1::SECURITY_TOKEN, HttpContext.SecurityToken},
                };
                RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(
                        std::move(Request),
                        HttpContext.DatabasePath,
                        HttpContext.SerializedUserToken,
                        Nothing(),
                        ctx.ActorSystem(),
                        peerMetadata
                );
                RpcFuture.Subscribe(
                    [actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()]
                    (const NThreading::TFuture<TProtoResponse>& future) {
                        auto& response = future.GetValueSync();
                        auto result = MakeHolder<TEvServerlessProxy::TEvGrpcRequestResult>();
                        Y_ABORT_UNLESS(response.operation().ready());
                        if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
                            TProtoResult rs;
                            response.operation().result().UnpackTo(&rs);
                            result->Message = MakeHolder<TProtoResult>(rs);
                        }
                        NYql::TIssues issues;
                        NYql::IssuesFromMessage(response.operation().issues(), issues);
                        result->Status = MakeHolder<NYdb::TStatus>(
                            NYdb::EStatus(response.operation().status()),
                            NYdb::NAdapters::ToSdkIssues(std::move(issues))
                        );
                        Ydb::Ymq::V1::QueueTags queueTags;
                        response.operation().metadata().UnpackTo(&queueTags);
                        for (const auto& [k, v] : queueTags.GetTags()) {
                            if (!result->QueueTags.Get()) {
                                result->QueueTags = MakeHolder<THashMap<TString, TString>>();
                            }
                            result->QueueTags->emplace(k, v);
                        }
                        actorSystem->Send(actorId, result.Release());
                    }
                );
                return;
            }

            void HandleUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev) {
                Y_UNUSED(ev);
            }

            void ReplyWithError(
                    const TActorContext& ctx,
                    NYdb::EStatus status,
                    const TString& errorText,
                    size_t issueCode = ISSUE_CODE_GENERIC) {
                HttpContext.ResponseData.Status = status;
                HttpContext.ResponseData.ErrorText = errorText;

                ReplyToHttpContext(ctx, issueCode);

                ctx.Send(AuthActor, new TEvents::TEvPoisonPill());

                TBase::Die(ctx);
            }

            void ReplyWithError(
                    const TActorContext& ctx,
                    ui32 httpStatusCode,
                    const TString& ymqStatusCode,
                    const TString& errorText) {
                HttpContext.ResponseData.IsYmq = true;
                HttpContext.ResponseData.UseYmqStatusCode = true;
                HttpContext.ResponseData.Status = NYdb::EStatus::STATUS_UNDEFINED;
                HttpContext.ResponseData.YmqHttpCode = httpStatusCode;
                HttpContext.ResponseData.YmqStatusCode = ymqStatusCode;
                HttpContext.ResponseData.ErrorText = errorText;

                ReplyToHttpContext(ctx);

                ctx.Send(AuthActor, new TEvents::TEvPoisonPill());

                TBase::Die(ctx);
            }

            void ReplyToHttpContext(const TActorContext& ctx, std::optional<size_t> issueCode = std::nullopt) {
                if (issueCode.has_value()) {
                    HttpContext.DoReply(ctx, issueCode.value());
                } else {
                    HttpContext.DoReply(ctx);
                }
            }

            void HandleGrpcResponse(TEvServerlessProxy::TEvGrpcRequestResult::TPtr ev,
                                    const TActorContext& ctx) {
                if (ev->Get()->Status->IsSuccess()) {
                    LOG_SP_DEBUG_S(
                        ctx,
                        NKikimrServices::HTTP_PROXY,
                        "Got succesfult GRPC response.";
                    );
                    ProtoToJson(
                        *ev->Get()->Message,
                        HttpContext.ResponseData.Body,
                        HttpContext.ContentType == MIME_CBOR
                    );
                    HttpContext.ResponseData.IsYmq = true;
                    HttpContext.ResponseData.UseYmqStatusCode = true;
                    HttpContext.ResponseData.YmqHttpCode = 200;
                    if (ev->Get()->QueueTags) {
                        HttpContext.ResponseData.QueueTags = std::move(*ev->Get()->QueueTags);
                    }
                    ReplyToHttpContext(ctx);
                } else {
                    auto retryClass = NYdb::NTopic::GetRetryErrorClass(ev->Get()->Status->GetStatus());

                    switch (retryClass) {
                    case ERetryErrorClass::ShortRetry:
                    case ERetryErrorClass::LongRetry:
                        LOG_SP_DEBUG_S(
                            ctx,
                            NKikimrServices::HTTP_PROXY,
                            "Retrying failed GRPC response"
                        );
                        RetryCounter.Click();
                        if (RetryCounter.HasAttemps()) {
                            return SendGrpcRequestNoDriver(ctx);
                        }
                    case ERetryErrorClass::NoRetry:
                        TString errorText;
                        TStringOutput stringOutput(errorText);

                        ev->Get()->Status->GetIssues().PrintTo(stringOutput);

                        RetryCounter.Void();

                        auto issues = ev->Get()->Status->GetIssues();
                        auto errorAndCode = issues.Empty()
                            ? std::make_tuple(
                                NSQS::NErrors::INTERNAL_FAILURE.ErrorCode,
                                NSQS::NErrors::INTERNAL_FAILURE.HttpStatusCode)
                            : NKikimr::NSQS::TErrorClass::GetErrorAndCode(issues.begin()->GetCode());

                        LOG_SP_DEBUG_S(
                            ctx,
                            NKikimrServices::HTTP_PROXY,
                            "Not retrying GRPC response."
                                << " Code: " << get<1>(errorAndCode)
                                << ", Error: " << get<0>(errorAndCode);
                        );

                        return ReplyWithError(
                            ctx,
                            get<1>(errorAndCode),
                            get<0>(errorAndCode),
                            TString{issues.begin()->GetMessage()}
                        );
                    }
                }
                TBase::Die(ctx);
            }

            void HandleTimeout(TEvents::TEvWakeup::TPtr ev, const TActorContext& ctx) {
                Y_UNUSED(ev);
                return ReplyWithError(ctx, NYdb::EStatus::TIMEOUT, "Request hasn't been completed by deadline");
            }

            void HandleYmqCloudAuthorizationResponse(TEvYmqCloudAuthResponse::TPtr ev, const TActorContext& ctx) {
                if (ev->Get()->IsSuccess) {
                    LOG_SP_DEBUG_S(
                        ctx,
                        NKikimrServices::HTTP_PROXY,
                        TStringBuilder() << "Got cloud auth response."
                        << " FolderId: " << ev->Get()->FolderId
                        << " CloudId: " << ev->Get()->CloudId
                        << " UserSid: " << ev->Get()->Sid;
                    );
                    HttpContext.FolderId = FolderId = ev->Get()->FolderId;
                    HttpContext.CloudId = CloudId = ev->Get()->CloudId;
                    UserSid = ev->Get()->Sid;
                    SendGrpcRequestNoDriver(ctx);
                } else {
                    LOG_SP_DEBUG_S(
                        ctx,
                        NKikimrServices::HTTP_PROXY,
                        TStringBuilder() << "Got cloud auth response."
                        << " HttpStatusCode: " << ev->Get()->Error->HttpStatusCode
                        << " ErrorCode: " << ev->Get()->Error->ErrorCode
                        << " Message: " << ev->Get()->Error->Message;
                    );
                    ReplyWithError(
                        ctx,
                        ev->Get()->Error->HttpStatusCode,
                        ev->Get()->Error->ErrorCode,
                        ev->Get()->Error->Message
                    );
                }
            }

        public:
            void Bootstrap(const TActorContext& ctx) {
                PoolId = ctx.SelfID.PoolID();
                StartTime = ctx.Now();
                try {
                    HttpContext.RequestBodyToProto(&Request);
                    auto queueUrl = QueueUrlExtractor(Request);
                    if (!queueUrl.empty()) {
                        auto cloudIdAndResourceId = NKikimr::NYmq::CloudIdAndResourceIdFromQueueUrl(queueUrl);
                        if (cloudIdAndResourceId.first.empty()) {
                            return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST, "Invalid queue url");
                        }
                        CloudId = cloudIdAndResourceId.first;
                        HttpContext.ResourceId = ResourceId = cloudIdAndResourceId.second;
                        HttpContext.ResponseData.YmqIsFifo = AsciiHasSuffixIgnoreCase(queueUrl, ".fifo");
                    }
                } catch (const NKikimr::NSQS::TSQSException& e) {
                    NYds::EErrorCodes issueCode = NYds::EErrorCodes::OK;
                    if (e.ErrorClass.ErrorCode == "MissingParameter") {
                        issueCode = NYds::EErrorCodes::MISSING_PARAMETER;
                    } else if (e.ErrorClass.ErrorCode == "InvalidQueryParameter"
                            || e.ErrorClass.ErrorCode == "MalformedQueryString") {
                        issueCode = NYds::EErrorCodes::INVALID_ARGUMENT;
                    }
                    return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST, e.what(), static_cast<size_t>(issueCode));
                } catch (const std::exception& e) {
                    LOG_SP_WARN_S(
                        ctx,
                        NKikimrServices::HTTP_PROXY,
                        "got new request with incorrect json from [" << HttpContext.SourceAddress << "] "
                    );
                    return ReplyWithError(
                        ctx,
                        NYdb::EStatus::BAD_REQUEST,
                        e.what(),
                        static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT)
                    );
                }

                LOG_SP_INFO_S(
                    ctx,
                    NKikimrServices::HTTP_PROXY,
                    "got new request from [" << HttpContext.SourceAddress << "]"
                );

                if (!HttpContext.ServiceConfig.GetHttpConfig().GetYandexCloudMode()) {
                    SendGrpcRequestNoDriver(ctx);
                } else {
                    auto requestHolder = MakeHolder<NKikimrClient::TSqsRequest>();

                    NSQS::EAction action = NSQS::ActionFromString(Method);
                    requestHolder->SetRequestId(HttpContext.RequestId);

                    NSQS::TAuthActorData data {
                        .SQSRequest = std::move(requestHolder),
                        .UserSidCallback = [](const TString& userSid) { Y_UNUSED(userSid); },
                        .EnableQueueLeader = true,
                        .Action = action,
                        .ExecutorPoolID = PoolId,
                        .CloudID = CloudId,
                        .ResourceID = ResourceId,
                        .Counters = nullptr,
                        .AWSSignature = std::move(HttpContext.GetSignature()),
                        .IAMToken = HttpContext.IamToken,
                        .FolderID = HttpContext.FolderId,
                        .RequestFormat = NSQS::TAuthActorData::Json,
                        .Requester = ctx.SelfID
                    };

                    AppData(ctx.ActorSystem())->SqsAuthFactory->RegisterAuthActor(
                        *ctx.ActorSystem(),
                        std::move(data));
                }

                ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

                TBase::Become(&TYmqHttpRequestActor::StateWork);
            }

        private:
            TInstant StartTime;
            typename TProcessorBase::TRequestState RequestState = TProcessorBase::TRequestState::StateIdle;
            TProtoRequest Request;
            TDuration RequestTimeout = TDuration::Seconds(60);
            ui32 PoolId;
            THttpRequestContext HttpContext;
            THolder<NKikimr::NSQS::TAwsRequestSignV4> Signature;
            THolder<NThreading::TFuture<TProtoResultWrapper<TProtoResult>>> Future;
            NThreading::TFuture<TProtoResponse> RpcFuture;
            THolder<NThreading::TFuture<void>> DiscoveryFuture;
            TProtoCall ProtoCall;
            TString Method;
            std::function<TString(TProtoRequest&)> QueueUrlExtractor;
            TRetryCounter RetryCounter;
            TActorId AuthActor;
            bool InputCountersReported = false;
            TString FolderId;
            TString CloudId;
            TString ResourceId;
            TString UserSid;
        };

        std::function<TString(TProtoRequest&)> QueueUrlExtractor;
    };

    template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoCall, class TRpcEv>
    class THttpRequestProcessor : public TBaseHttpRequestProcessor<TProtoService, TProtoRequest, TProtoResponse, TProtoResult, TProtoCall, TRpcEv>{
    using TProcessorBase = TBaseHttpRequestProcessor<TProtoService, TProtoRequest, TProtoResponse, TProtoResult, TProtoCall, TRpcEv>;
    public:
        THttpRequestProcessor(TString method, TProtoCall protoCall) : TProcessorBase(method, protoCall)
        {
        }

        void Execute(THttpRequestContext&& context, THolder<NKikimr::NSQS::TAwsRequestSignV4> signature, const TActorContext& ctx) override {
            ctx.Register(new THttpRequestActor(
                    std::move(context),
                    std::move(signature),
                    TProcessorBase::ProtoCall, TProcessorBase::Method));
        }

    private:

        class THttpRequestActor : public NActors::TActorBootstrapped<THttpRequestActor> {
        public:
            using TBase = NActors::TActorBootstrapped<THttpRequestActor>;

            THttpRequestActor(THttpRequestContext&& httpContext,
                              THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature,
                              TProtoCall protoCall, const TString& method)
                : HttpContext(std::move(httpContext))
                , Signature(std::move(signature))
                , ProtoCall(protoCall)
                , Method(method)
            {
            }

            TStringBuilder LogPrefix() const {
                return HttpContext.LogPrefix();
            }

        private:
            STFUNC(StateWork)
            {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvents::TEvWakeup, HandleTimeout);
                    HFunc(TEvServerlessProxy::TEvClientReady, HandleClientReady);
                    HFunc(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult, Handle);
                    HFunc(TEvServerlessProxy::TEvErrorWithIssue, HandleErrorWithIssue);
                    HFunc(TEvServerlessProxy::TEvGrpcRequestResult, HandleGrpcResponse);
                    HFunc(TEvServerlessProxy::TEvToken, HandleToken);
                    default:
                        HandleUnexpectedEvent(ev);
                        break;
                }
            }

            void SendYdbDriverRequest(const TActorContext& ctx) {
                Y_ABORT_UNLESS(HttpContext.Driver);

                RequestState = TProcessorBase::TRequestState::StateAuthorization;

                auto request = MakeHolder<TEvServerlessProxy::TEvDiscoverDatabaseEndpointRequest>();
                request->DatabasePath = HttpContext.DatabasePath;

                ctx.Send(MakeTenantDiscoveryID(), std::move(request));
            }

            void CreateClient(const TActorContext& ctx) {
                RequestState = TProcessorBase::TRequestState::StateListEndpoints;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "create client to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabasePath <<
                              "' iam token size: " << HttpContext.IamToken.size());

                auto clientSettings = NYdb::TCommonClientSettings()
                        .DiscoveryEndpoint(HttpContext.DiscoveryEndpoint)
                        .Database(HttpContext.DatabasePath)
                        .AuthToken(HttpContext.IamToken)
                        .DiscoveryMode(NYdb::EDiscoveryMode::Async);

                if (!HttpContext.DatabasePath.empty() && !HttpContext.ServiceConfig.GetTestMode()) {
                    clientSettings.Database(HttpContext.DatabasePath);
                }
                Y_ABORT_UNLESS(!Client);
                Client.Reset(new TDataStreamsClient(*HttpContext.Driver, clientSettings));
                DiscoveryFuture = MakeHolder<NThreading::TFuture<void>>(Client->DiscoveryCompleted());
                DiscoveryFuture->Subscribe(
                    [actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()] (const NThreading::TFuture<void>&) {
                        actorSystem->Send(actorId, new TEvServerlessProxy::TEvClientReady());
                    });
            }

            void HandleClientReady(TEvServerlessProxy::TEvClientReady::TPtr&, const TActorContext& ctx){
                HttpContext.Driver ? SendGrpcRequest(ctx) : SendGrpcRequestNoDriver(ctx);
            }

            void SendGrpcRequestNoDriver(const TActorContext& ctx) {
                RequestState = TProcessorBase::TRequestState::StateGrpcRequest;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabasePath <<
                              "' iam token size: " << HttpContext.IamToken.size());

                RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(Request), HttpContext.DatabasePath,
                                                            HttpContext.SerializedUserToken, ctx.ActorSystem());
                RpcFuture.Subscribe([actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()]
                                    (const NThreading::TFuture<TProtoResponse>& future) {
                    auto& response = future.GetValueSync();
                    auto result = MakeHolder<TEvServerlessProxy::TEvGrpcRequestResult>();
                    Y_ABORT_UNLESS(response.operation().ready());
                    if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
                        TProtoResult rs;
                        response.operation().result().UnpackTo(&rs);
                        result->Message = MakeHolder<TProtoResult>(rs);
                    }
                    NYql::TIssues issues;
                    NYql::IssuesFromMessage(response.operation().issues(), issues);
                    result->Status = MakeHolder<NYdb::TStatus>(NYdb::EStatus(response.operation().status()),
                                                               NYdb::NAdapters::ToSdkIssues(std::move(issues)));
                    actorSystem->Send(actorId, result.Release());
                });
                return;
            }

            void SendGrpcRequest(const TActorContext& ctx) {
                RequestState = TProcessorBase::TRequestState::StateGrpcRequest;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabasePath <<
                              "' iam token size: " << HttpContext.IamToken.size());

                Y_ABORT_UNLESS(Client);
                Y_ABORT_UNLESS(DiscoveryFuture->HasValue());

                TProtoResponse response;

                LOG_SP_DEBUG_S(ctx, NKikimrServices::HTTP_PROXY,
                               "sending grpc request " << Request.DebugString());

                Future = MakeHolder<NThreading::TFuture<TProtoResultWrapper<TProtoResult>>>(
                    Client->template DoProtoRequest<TProtoRequest, TProtoResponse, TProtoResult,
                    TProtoCall>(std::move(Request), ProtoCall));
                Future->Subscribe(
                    [actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()]
                    (const NThreading::TFuture<TProtoResultWrapper<TProtoResult>>& future) {
                        auto& response = future.GetValueSync();
                        auto result = MakeHolder<TEvServerlessProxy::TEvGrpcRequestResult>();
                        if (response.IsSuccess()) {
                            result->Message = MakeHolder<TProtoResult>(response.GetResult());

                        }
                        result->Status = MakeHolder<NYdb::TStatus>(response);
                        actorSystem->Send(actorId, result.Release());
                    });
            }

            void HandleUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev) {
                Y_UNUSED(ev);
            }

            void TryUpdateDbInfo(const TDatabase& db, const TActorContext& ctx) {
                if (db.Path) {
                    HttpContext.DatabasePath = db.Path;
                    HttpContext.DatabaseId = db.Id;
                    HttpContext.CloudId = db.CloudId;
                    HttpContext.FolderId = db.FolderId;
                    if (ExtractStreamName<TProtoRequest>(Request).StartsWith(HttpContext.DatabasePath + "/")) {
                        HttpContext.StreamName =
                            TruncateStreamName<TProtoRequest>(Request, HttpContext.DatabasePath + "/");
                    } else {
                        HttpContext.StreamName = ExtractStreamName<TProtoRequest>(Request);
                    }

                }
                ReportInputCounters(ctx);
            }

            void HandleToken(TEvServerlessProxy::TEvToken::TPtr& ev, const TActorContext& ctx) {
                HttpContext.ServiceAccountId = ev->Get()->ServiceAccountId;
                HttpContext.IamToken = ev->Get()->IamToken;
                HttpContext.SerializedUserToken = ev->Get()->SerializedUserToken;

                if (HttpContext.Driver) {
                    SendYdbDriverRequest(ctx);
                } else {
                    TryUpdateDbInfo(ev->Get()->Database, ctx);
                    SendGrpcRequestNoDriver(ctx);
                }
            }


            void HandleErrorWithIssue(TEvServerlessProxy::TEvErrorWithIssue::TPtr& ev, const TActorContext& ctx) {
                TryUpdateDbInfo(ev->Get()->Database, ctx);
                ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Response, ev->Get()->IssueCode);
            }

            void ReplyWithError(const TActorContext& ctx, NYdb::EStatus status, const TString& errorText, size_t issueCode = ISSUE_CODE_GENERIC) {
                /* deprecated metric: */ ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                             {{"method", Method},
                              {"cloud", HttpContext.CloudId},
                              {"folder", HttpContext.FolderId},
                              {"database", HttpContext.DatabaseId},
                              {"stream", HttpContext.StreamName},
                              {"code", TStringBuilder() << (int)MapToException(status, Method, issueCode).second},
                              {"name", "api.http.errors_per_second"}}
                         });

                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                            {{"database", HttpContext.DatabasePath},
                              {"method", Method},
                              {"cloud_id", HttpContext.CloudId},
                              {"folder_id", HttpContext.FolderId},
                              {"database_id", HttpContext.DatabaseId},
                              {"topic", HttpContext.StreamName},
                              {"code", TStringBuilder() << (int)MapToException(status, Method, issueCode).second},
                              {"name", "api.http.data_streams.response.count"}}
                         });

                HttpContext.ResponseData.Status = status;
                HttpContext.ResponseData.ErrorText = errorText;
                ReplyToHttpContext(ctx, issueCode);

                ctx.Send(AuthActor, new TEvents::TEvPoisonPill());

                TBase::Die(ctx);
            }

            void ReplyToHttpContext(const TActorContext& ctx, std::optional<size_t> issueCode = std::nullopt) {
                ReportLatencyCounters(ctx);
                LogHttpRequestResponse(ctx, issueCode);

                if (issueCode.has_value()) {
                    HttpContext.DoReply(ctx, issueCode.value());
                } else {
                    HttpContext.DoReply(ctx);
                }
            }

            void LogHttpRequestResponse(const TActorContext& ctx, const std::optional<size_t> issueCode) {
                const int httpCode = issueCode ? MapToException(HttpContext.ResponseData.Status, Method, *issueCode).second : 200;
                const bool isServerError = IsServerError(httpCode);
                auto priority = isServerError ? NActors::NLog::PRI_WARN : NActors::NLog::PRI_INFO;
                LOG_LOG_S_SAMPLED_BY(ctx, priority, NKikimrServices::HTTP_PROXY,
                                     NSqsTopic::SampleIdFromRequestId(HttpContext.RequestId),
                                     "Request [" << HttpContext.RequestId << "] " << LogHttpRequestResponseCommonInfoString(HttpContext, StartTime, "Kinesis", HttpContext.StreamName, Method, {}, httpCode, HttpContext.ResponseData.ErrorText));
            }

            void ReportInputCounters(const TActorContext& ctx) {

                if (InputCountersReported) {
                    return;
                }
                InputCountersReported = true;

                FillInputCustomMetrics<TProtoRequest>(Request, HttpContext, ctx);
                /* deprecated metric: */ ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{1, true, true,
                             BuildLabels(Method, HttpContext, "api.http.requests_per_second", setStreamPrefix)
                         });
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{1, true, true,
                             BuildLabels(Method, HttpContext, "api.http.data_streams.request.count")
                         });
            }

            void Handle(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult::TPtr ev,
                        const TActorContext& ctx) {
                if (ev->Get()->DatabaseInfo) {
                    auto& db = ev->Get()->DatabaseInfo;
                    HttpContext.FolderId = db->FolderId;
                    HttpContext.CloudId = db->CloudId;
                    HttpContext.DatabaseId = db->Id;
                    HttpContext.DiscoveryEndpoint = db->Endpoint;
                    HttpContext.DatabasePath = db->Path;

                    if (ExtractStreamName<TProtoRequest>(Request).StartsWith(HttpContext.DatabasePath + "/")) {
                        HttpContext.StreamName =
                            TruncateStreamName<TProtoRequest>(Request, HttpContext.DatabasePath + "/");
                    } else {
                        HttpContext.StreamName = ExtractStreamName<TProtoRequest>(Request);
                    }
                    ReportInputCounters(ctx);
                    CreateClient(ctx);
                    return;
                }

                ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Message);
            }

            void ReportLatencyCounters(const TActorContext& ctx) {
                TDuration dur = ctx.Now() - StartTime;
                /* deprecated metric: */ ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvHistCounter{static_cast<i64>(dur.MilliSeconds()), 1,
                             BuildLabels(Method, HttpContext, "api.http.requests_duration_milliseconds", setStreamPrefix)
                        });
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvHistCounter{static_cast<i64>(dur.MilliSeconds()), 1,
                             BuildLabels(Method, HttpContext, "api.http.data_streams.response.duration_milliseconds")
                        });
                //TODO: add api.http.response.duration_milliseconds
            }

            void HandleGrpcResponse(TEvServerlessProxy::TEvGrpcRequestResult::TPtr ev,
                                    const TActorContext& ctx) {
                if (ev->Get()->Status->IsSuccess()) {
                    ProtoToJson(*ev->Get()->Message, HttpContext.ResponseData.Body,
                                HttpContext.ContentType == MIME_CBOR);
                    FillOutputCustomMetrics<TProtoResult>(
                        *(dynamic_cast<TProtoResult*>(ev->Get()->Message.Get())), HttpContext, ctx);
                    /* deprecated metric: */ ctx.Send(MakeMetricsServiceID(),
                             new TEvServerlessProxy::TEvCounter{1, true, true,
                                 BuildLabels(Method, HttpContext, "api.http.success_per_second", setStreamPrefix)
                             });
                    ctx.Send(MakeMetricsServiceID(),
                             new TEvServerlessProxy::TEvCounter{
                                 1, true, true,
                                {{"database", HttpContext.DatabasePath},
                                  {"method", Method},
                                  {"cloud_id", HttpContext.CloudId},
                                  {"folder_id", HttpContext.FolderId},
                                  {"database_id", HttpContext.DatabaseId},
                                  {"topic", HttpContext.StreamName},
                                  {"code", "200"},
                                  {"name", "api.http.data_streams.response.count"}}
                         });
                    ReplyToHttpContext(ctx);
                } else {
                    auto retryClass =
                        NYdb::NTopic::GetRetryErrorClass(ev->Get()->Status->GetStatus());

                    switch (retryClass) {
                    case ERetryErrorClass::ShortRetry:
                        [[fallthrough]];

                    case ERetryErrorClass::LongRetry:
                        RetryCounter.Click();
                        if (RetryCounter.HasAttemps()) {
                            return HttpContext.Driver ? SendGrpcRequest(ctx) : SendGrpcRequestNoDriver(ctx);
                        }
                        [[fallthrough]];

                    case ERetryErrorClass::NoRetry: {
                        TString errorText;
                        TStringOutput stringOutput(errorText);
                        ev->Get()->Status->GetIssues().PrintTo(stringOutput);
                        RetryCounter.Void();
                        auto issues = ev->Get()->Status->GetIssues();
                        size_t issueCode = (
                            issues && issues.begin()->IssueCode != ISSUE_CODE_OK
                            ) ? issues.begin()->IssueCode : ISSUE_CODE_GENERIC;
                        return ReplyWithError(ctx, ev->Get()->Status->GetStatus(), errorText, issueCode);
                        }
                    }
                }
                TBase::Die(ctx);
            }

            void HandleTimeout(TEvents::TEvWakeup::TPtr ev, const TActorContext& ctx) {
                Y_UNUSED(ev);
                return ReplyWithError(ctx, NYdb::EStatus::TIMEOUT, "Request hasn't been completed by deadline");
            }

        public:
            void Bootstrap(const TActorContext& ctx) {
                StartTime = ctx.Now();
                try {
                    HttpContext.RequestBodyToProto(&Request);
                } catch (const NKikimr::NSQS::TSQSException& e) {
                    NYds::EErrorCodes issueCode = NYds::EErrorCodes::OK;
                    if (e.ErrorClass.ErrorCode == "MissingParameter")
                        issueCode = NYds::EErrorCodes::MISSING_PARAMETER;
                    else if (e.ErrorClass.ErrorCode == "InvalidQueryParameter" || e.ErrorClass.ErrorCode == "MalformedQueryString")
                        issueCode = NYds::EErrorCodes::INVALID_ARGUMENT;
                    return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST, e.what(), static_cast<size_t>(issueCode));
                } catch (const std::exception& e) {
                    LOG_SP_WARN_S(ctx, NKikimrServices::HTTP_PROXY,
                                  "got new request with incorrect json from [" << HttpContext.SourceAddress << "] " <<
                                  "database '" << HttpContext.DatabasePath << "'");
                    return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST, e.what(), static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT));
                }

                if (HttpContext.DatabasePath.empty()) {
                    HttpContext.DatabasePath = ExtractStreamName<TProtoRequest>(Request);
                }

                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "got new request from [" << HttpContext.SourceAddress << "] " <<
                              "database '" << HttpContext.DatabasePath << "' " <<
                              "stream '" << ExtractStreamName<TProtoRequest>(Request) << "'");

                // Use Signature or no sdk mode - then need to auth anyway
                if (HttpContext.IamToken.empty() || !HttpContext.Driver) {
                    // Test mode - no driver and no creds
                    if (HttpContext.IamToken.empty() && !Signature) {
                        SendGrpcRequestNoDriver(ctx);
                    } else {
                        AuthActor = ctx.Register(AppData(ctx)->DataStreamsAuthFactory->CreateAuthActor(
                                                     ctx.SelfID, HttpContext, std::move(Signature)));
                    }
                } else {
                    SendYdbDriverRequest(ctx);
                }
                ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

                TBase::Become(&THttpRequestActor::StateWork);
            }

        private:
            TInstant StartTime;
            typename TProcessorBase::TRequestState RequestState = TProcessorBase::TRequestState::StateIdle;
            TProtoRequest Request;
            TDuration RequestTimeout = TDuration::Seconds(60);
            ui32 PoolId;
            THttpRequestContext HttpContext;
            THolder<NKikimr::NSQS::TAwsRequestSignV4> Signature;
            THolder<NThreading::TFuture<TProtoResultWrapper<TProtoResult>>> Future;
            NThreading::TFuture<TProtoResponse> RpcFuture;
            THolder<NThreading::TFuture<void>> DiscoveryFuture;
            TProtoCall ProtoCall;
            TString Method;
            TRetryCounter RetryCounter;

            THolder<TDataStreamsClient> Client;

            TActorId AuthActor;
            bool InputCountersReported = false;
        };
    };

    template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoCall, class TRpcEv>
    class TSqsTopicHttpRequestProcessor : public TBaseHttpRequestProcessor<TProtoService, TProtoRequest, TProtoResponse, TProtoResult, TProtoCall, TRpcEv>{
    using TProcessorBase = TBaseHttpRequestProcessor<TProtoService, TProtoRequest, TProtoResponse, TProtoResult, TProtoCall, TRpcEv>;
    public:
        TSqsTopicHttpRequestProcessor(TString method, TProtoCall protoCall) : TProcessorBase(method, protoCall)
        {
        }

        void Execute(THttpRequestContext&& context, THolder<NKikimr::NSQS::TAwsRequestSignV4> signature, const TActorContext& ctx) override {
            ctx.Register(new TSqsTopicHttpRequestActor(
                    std::move(context),
                    std::move(signature),
                    TProcessorBase::ProtoCall, TProcessorBase::Method));
        }

    private:

        class TSqsTopicHttpRequestActor : public NActors::TActorBootstrapped<TSqsTopicHttpRequestActor> {
        public:
            using TBase = NActors::TActorBootstrapped<TSqsTopicHttpRequestActor>;

            TSqsTopicHttpRequestActor(THttpRequestContext&& httpContext,
                              THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature,
                              TProtoCall protoCall, const TString& method)
                : HttpContext(std::move(httpContext))
                , Signature(std::move(signature))
                , ProtoCall(protoCall)
                , Method(method)
            {
            }

            TStringBuilder LogPrefix() const {
                return HttpContext.LogPrefix();
            }

        private:
            STFUNC(StateWork)
            {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvents::TEvWakeup, HandleTimeout);
                    HFunc(TEvServerlessProxy::TEvErrorWithIssue, HandleErrorWithIssue);
                    HFunc(TEvServerlessProxy::TEvGrpcRequestResult, HandleGrpcResponse);
                    HFunc(TEvServerlessProxy::TEvToken, HandleToken);
                    default:
                        HandleUnexpectedEvent(ev);
                        break;
                }
            }

            void SendGrpcRequestNoDriver(const TActorContext& ctx) {
                RequestState = TProcessorBase::TRequestState::StateGrpcRequest;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabasePath <<
                              "' iam token size: " << HttpContext.IamToken.size());

                RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(Request), HttpContext.DatabasePath,
                                                            HttpContext.SerializedUserToken, ctx.ActorSystem());
                RpcFuture.Subscribe([actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()]
                                    (const NThreading::TFuture<TProtoResponse>& future) {
                    auto& response = future.GetValueSync();
                    auto result = MakeHolder<TEvServerlessProxy::TEvGrpcRequestResult>();
                    Y_ABORT_UNLESS(response.operation().ready());
                    if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
                        TProtoResult rs;
                        response.operation().result().UnpackTo(&rs);
                        result->Message = MakeHolder<TProtoResult>(rs);
                    }
                    NYql::TIssues issues;
                    NYql::IssuesFromMessage(response.operation().issues(), issues);
                    result->Status = MakeHolder<NYdb::TStatus>(NYdb::EStatus(response.operation().status()),
                                                               NYdb::NAdapters::ToSdkIssues(std::move(issues)));
                    actorSystem->Send(actorId, result.Release());
                });
                return;
            }

            void HandleUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev) {
                Y_UNUSED(ev);
            }

            void TryUpdateDbInfo(const TDatabase& db) {
                if (db.Path) {
                    HttpContext.DatabasePath = db.Path;
                    HttpContext.DatabaseId = db.Id;
                    HttpContext.CloudId = db.CloudId;
                    HttpContext.FolderId = db.FolderId;
                }
            }

            void HandleToken(TEvServerlessProxy::TEvToken::TPtr& ev, const TActorContext& ctx) {
                HttpContext.ServiceAccountId = ev->Get()->ServiceAccountId;
                HttpContext.IamToken = ev->Get()->IamToken;
                HttpContext.SerializedUserToken = ev->Get()->SerializedUserToken;

                if (TString databasePath = ev->Get()->Database.Path) {
                    if (!AssignDatabasePath(ctx, databasePath)) {
                        return;
                    }
                }
                TryUpdateDbInfo(ev->Get()->Database);

                SendGrpcRequestNoDriver(ctx);
            }

            void HandleErrorWithIssue(TEvServerlessProxy::TEvErrorWithIssue::TPtr& ev, const TActorContext& ctx) {
                TryUpdateDbInfo(ev->Get()->Database);
                ReplyWithYdbError(ctx, ev->Get()->Status, ev->Get()->Response, ev->Get()->IssueCode);
            }

            TVector<std::pair<TString, TString>> AddCommonLabels(TVector<std::pair<TString, TString>>&& labels) const {
                return NSqsTopic::GetMetricsLabels(HttpContext.DatabasePath, TopicPath, ConsumerName, Method, std::move(labels));
            }

            void ReplyWithYdbError(const TActorContext& ctx, NYdb::EStatus status, const TString& errorText, size_t issueCode = ISSUE_CODE_GENERIC) {
                HttpContext.ResponseData.Status = status;
                HttpContext.ResponseData.ErrorText = errorText;
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                             AddCommonLabels({
                                 {"code", TStringBuilder() << (int)MapToException(status, Method, issueCode).second},
                                 {"name", "api.sqs.response.count"},
                             })});
                ReplyToHttpContext(ctx, 0, issueCode);

                ctx.Send(AuthActor, new TEvents::TEvPoisonPill());

                TBase::Die(ctx);
            }

            void ReplyWithMessageQueueError(
                    const TActorContext& ctx,
                    ui32 httpStatusCode,
                    const TString& ymqStatusCode,
                    const TString& errorText) {
                HttpContext.ResponseData.IsYmq = false;
                HttpContext.ResponseData.UseYmqStatusCode = true;
                HttpContext.ResponseData.Status = NYdb::EStatus::STATUS_UNDEFINED;
                HttpContext.ResponseData.YmqHttpCode = httpStatusCode;
                HttpContext.ResponseData.YmqStatusCode = ymqStatusCode;
                HttpContext.ResponseData.ErrorText = errorText;
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                             AddCommonLabels({
                                 {"code", ToString(httpStatusCode)},
                                 {"name", "api.sqs.response.count"},
                             })});
                ReplyToHttpContext(ctx, errorText.size(), std::nullopt);

                ctx.Send(AuthActor, new TEvents::TEvPoisonPill());

                TBase::Die(ctx);
            }

            void ReplyToHttpContext(const TActorContext& ctx, size_t messageSize, std::optional<size_t> issueCode) {
                ReportLatencyCounters(ctx);
                ReportResponseSizeCounters(TStringBuilder() << HttpContext.ResponseData.YmqHttpCode, messageSize, ctx);
                LogHttpRequestResponse(ctx);

                if (issueCode.has_value()) {
                    HttpContext.DoReply(ctx, issueCode.value());
                } else {
                    HttpContext.DoReply(ctx);
                }
            }

            void LogHttpRequestResponse(const TActorContext& ctx) {
                const int httpCode = HttpContext.ResponseData.UseYmqStatusCode ? HttpContext.ResponseData.YmqHttpCode : 200;
                const bool isServerError = IsServerError(httpCode);
                auto priority = isServerError ? NActors::NLog::PRI_WARN : NActors::NLog::PRI_INFO;
                LOG_LOG_S_SAMPLED_BY(ctx, priority, NKikimrServices::SQS,
                                     NSqsTopic::SampleIdFromRequestId(HttpContext.RequestId),
                                     "Request [" << HttpContext.RequestId << "] " << LogHttpRequestResponseCommonInfoString(HttpContext, StartTime, "SqsTopic", TopicPath, Method, UserSid_, httpCode, HttpContext.ResponseData.ErrorText));
            }

            void ReportInputCounters(const TActorContext& ctx) {
                if (InputCountersReported) {
                    return;
                }
                InputCountersReported = true;
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{1, true, true,
                            AddCommonLabels({{"name", "api.sqs.request.count"}})
                         });
            }

            void ReportLatencyCounters(const TActorContext& ctx) {
                TDuration dur = ctx.Now() - StartTime;
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvHistCounter{static_cast<i64>(dur.MilliSeconds()), 1,
                            AddCommonLabels({{"name", "api.sqs.response.duration_milliseconds"}})
                        });
            }

            void ReportResponseSizeCounters(const TString& code, size_t value, const TActorContext& ctx) {
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{static_cast<i64>(value), true, true,
                            AddCommonLabels({
                                {"code", code},
                                {"name", "api.sqs.response.bytes"}
                            })
                        });
            }

            void HandleGrpcResponse(TEvServerlessProxy::TEvGrpcRequestResult::TPtr ev,
                                    const TActorContext& ctx) {
                if (ev->Get()->Status->IsSuccess()) {
                    ProtoToJson(*ev->Get()->Message, HttpContext.ResponseData.Body,
                                HttpContext.ContentType == MIME_CBOR);
                    FillOutputCustomMetrics<TProtoResult>(
                        *(dynamic_cast<TProtoResult*>(ev->Get()->Message.Get())), HttpContext, ctx);
                    ctx.Send(MakeMetricsServiceID(),
                             new TEvServerlessProxy::TEvCounter{
                                 1, true, true,
                                 AddCommonLabels({
                                     {"code", "200"},
                                     {"name", "api.sqs.response.count"}})});
                    ReplyToHttpContext(ctx, ev->Get()->Message->ByteSizeLong(), std::nullopt);
                } else {
                    auto retryClass =
                        NYdb::NTopic::GetRetryErrorClass(ev->Get()->Status->GetStatus());

                    switch (retryClass) {
                    case ERetryErrorClass::ShortRetry:
                        [[fallthrough]];

                    case ERetryErrorClass::LongRetry:
                        RetryCounter.Click();
                        if (RetryCounter.HasAttemps()) {
                            SendGrpcRequestNoDriver(ctx);
                        }
                        [[fallthrough]];

                    case ERetryErrorClass::NoRetry: {
                        TString errorText;
                        TStringOutput stringOutput(errorText);
                        ev->Get()->Status->GetIssues().PrintTo(stringOutput);
                        RetryCounter.Void();
                        auto issues = ev->Get()->Status->GetIssues();
                        auto [error, errorCode] = issues.Empty()
                            ? std::make_tuple(
                                NSQS::NErrors::INTERNAL_FAILURE.ErrorCode,
                                NSQS::NErrors::INTERNAL_FAILURE.HttpStatusCode)
                            : NKikimr::NSQS::TErrorClass::GetErrorAndCode(issues.begin()->GetCode());

                        LOG_SP_DEBUG_S(
                            ctx,
                            NKikimrServices::HTTP_PROXY,
                            "Not retrying GRPC response."
                                << " Code: " << errorCode
                                << ", Error: " << error;);
                        return ReplyWithMessageQueueError(
                            ctx,
                            errorCode,
                            error,
                            TString{!issues.Empty() ? issues.begin()->GetMessage() : NSQS::NErrors::INTERNAL_FAILURE.ErrorCode}
                        );
                        }
                    }
                }
                TBase::Die(ctx);
            }

            void HandleTimeout(TEvents::TEvWakeup::TPtr ev, const TActorContext& ctx) {
                Y_UNUSED(ev);
                return ReplyWithYdbError(ctx, NYdb::EStatus::TIMEOUT, "Request hasn't been completed by deadline");
            }

            // Fill HttpContext.DatabasePath and reply with error is databases missmatch
            bool AssignDatabasePath(const TActorContext& ctx, const TString& databasePath) {
                if (HttpContext.DatabasePath.empty()) {
                    HttpContext.DatabasePath = databasePath;
                } else {
                    if (HttpContext.DatabasePath != databasePath) {
                        ReplyWithYdbError(ctx, NYdb::EStatus::UNAUTHORIZED, "Queue url database  " + databasePath + " doesn't belong to " + HttpContext.DatabasePath, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT));
                        return false;
                    }
                }
                return true;
            }

        public:
            void Bootstrap(const TActorContext& ctx) {
                StartTime = ctx.Now();
                try {
                    HttpContext.RequestBodyToProto(&Request);
                } catch (const NKikimr::NSQS::TSQSException& e) {
                    NYds::EErrorCodes issueCode = NYds::EErrorCodes::OK;
                    if (e.ErrorClass.ErrorCode == "MissingParameter")
                        issueCode = NYds::EErrorCodes::MISSING_PARAMETER;
                    else if (e.ErrorClass.ErrorCode == "InvalidQueryParameter" || e.ErrorClass.ErrorCode == "MalformedQueryString")
                        issueCode = NYds::EErrorCodes::INVALID_ARGUMENT;
                    return ReplyWithYdbError(ctx, NYdb::EStatus::BAD_REQUEST, e.what(), static_cast<size_t>(issueCode));
                } catch (const std::exception& e) {
                    LOG_SP_WARN_S(ctx, NKikimrServices::HTTP_PROXY,
                                  "got new request with incorrect json from [" << HttpContext.SourceAddress << "] " <<
                                  "database '" << HttpContext.DatabasePath << "'");
                    return ReplyWithYdbError(ctx, NYdb::EStatus::BAD_REQUEST, e.what(), static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT));
                }

                if (auto queueUrl = MaybeGetQueueUrl(Request)) {
                    auto parsedQueueUrl = NKikimr::NSqsTopic::ParseQueueUrl(queueUrl);
                    if (!parsedQueueUrl.has_value()) {
                        return ReplyWithYdbError(ctx, NYdb::EStatus::BAD_REQUEST, "Invalid queue url: " + parsedQueueUrl.error(), static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT));
                    }
                    TopicPath = parsedQueueUrl->TopicPath;
                    ConsumerName = parsedQueueUrl->Consumer;
                    IsFifo = parsedQueueUrl->Fifo;

                    if (!AssignDatabasePath(ctx, parsedQueueUrl->Database)) {
                        return;
                    }
                    if (TopicPath.empty()) {
                        return ReplyWithYdbError(ctx, NYdb::EStatus::BAD_REQUEST, "Missing topic path", static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT));
                    }
                }

                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "got new request from [" << HttpContext.SourceAddress << "] " <<
                              "database '" << HttpContext.DatabasePath << "' " <<
                              "stream '" << ExtractStreamName<TProtoRequest>(Request) << "'");

                ReportInputCounters(ctx);
                if (HttpContext.IamToken.empty() && Signature) {
                    AuthActor = ctx.Register(AppData(ctx)->DataStreamsAuthFactory->CreateAuthActor(
                        ctx.SelfID, HttpContext, std::move(Signature)));
                } else {
                    SendGrpcRequestNoDriver(ctx);
                }

                ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

                TBase::Become(&TSqsTopicHttpRequestActor::StateWork);
            }

        private:
            TInstant StartTime;
            typename TProcessorBase::TRequestState RequestState = TProcessorBase::TRequestState::StateIdle;
            TProtoRequest Request;
            TDuration RequestTimeout = TDuration::Seconds(60);
            ui32 PoolId;
            THttpRequestContext HttpContext;
            THolder<NKikimr::NSQS::TAwsRequestSignV4> Signature;
            THolder<NThreading::TFuture<TProtoResultWrapper<TProtoResult>>> Future;
            NThreading::TFuture<TProtoResponse> RpcFuture;
            TProtoCall ProtoCall;
            TString Method;
            TString TopicPath;
            TString ConsumerName;
            bool IsFifo{};
            TRetryCounter RetryCounter;

            TActorId AuthActor;
            bool InputCountersReported = false;
            TString UserSid_;
        };
    };

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
