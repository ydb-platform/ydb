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
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/iam_token_service.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>

#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>

#include <ydb/services/datastreams/datastreams_proxy.h>
#include <ydb/services/datastreams/next_token.h>
#include <ydb/services/datastreams/shard_iterator.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <ydb/public/sdk/cpp/client/ydb_ymq/ymq.h>
#include <ydb/services/ymq/ymq_proxy.h>


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

#include <ydb/library/http_proxy/error/error.h>

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

    constexpr TStringBuf IAM_HEADER = "x-yacloud-subjecttoken";
    constexpr TStringBuf SECURITY_TOKEN_HEADER = "x-amz-security-token";
    constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";
    constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";
    constexpr TStringBuf REQUEST_ID_HEADER_EXT = "x-amzn-requestid";
    constexpr TStringBuf REQUEST_DATE_HEADER = "x-amz-date";
    constexpr TStringBuf REQUEST_FORWARDED_FOR = "x-forwarded-for";
    constexpr TStringBuf REQUEST_TARGET_HEADER = "x-amz-target";
    constexpr TStringBuf REQUEST_CONTENT_TYPE_HEADER = "content-type";
    constexpr TStringBuf CRC32_HEADER = "x-amz-crc32";
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
                    {NYmq::V1::CLOUD_ID, HttpContext.UserName ? HttpContext.UserName : CloudId},
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
                            std::move(issues)
                        );
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
                    HttpContext.ResponseData.YmqHttpCode = 200;
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
                            issues.begin()->GetMessage()
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
                    FolderId = ev->Get()->FolderId;
                    CloudId = ev->Get()->CloudId;
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
                StartTime = ctx.Now();
                try {
                    HttpContext.RequestBodyToProto(&Request);
                    auto queueUrl = QueueUrlExtractor(Request);
                    if (!queueUrl.empty()) {
                        auto cloudIdAndResourceId = NKikimr::NYmq::CloudIdAndResourceIdFromQueueUrl(queueUrl);
                        if(cloudIdAndResourceId.Empty()) {
                            return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST, "Invalid queue url");
                        }
                        CloudId = cloudIdAndResourceId.Get()->first;
                        ResourceId = cloudIdAndResourceId.Get()->second;
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
                    NSQS::EAction action = NSQS::EAction::Unknown;
                    if (Method == "CreateQueue") {
                        action = NSQS::EAction::CreateQueue;
                    } else if (Method == "GetQueueUrl") {
                        action = NSQS::EAction::GetQueueUrl;
                    } else if (Method == "SendMessage") {
                        action = NSQS::EAction::SendMessage;
                    } else if (Method == "ReceiveMessage") {
                        action = NSQS::EAction::ReceiveMessage;
                    } else if (Method == "GetQueueAttributes") {
                        action = NSQS::EAction::GetQueueAttributes;
                    } else if (Method == "ListQueues") {
                        action = NSQS::EAction::ListQueues;
                    } else if (Method == "DeleteMessage") {
                        action = NSQS::EAction::DeleteMessage;
                    } else if (Method == "PurgeQueue") {
                        action = NSQS::EAction::PurgeQueue;
                    } else if (Method == "DeleteQueue") {
                        action = NSQS::EAction::DeleteQueue;
                    } else if (Method == "ChangeMessageVisibility") {
                        action = NSQS::EAction::ChangeMessageVisibility;
                    } else if (Method == "SetQueueAttributes") {
                        action = NSQS::EAction::SetQueueAttributes;
                    } else if (Method == "SendMessageBatch") {
                        action = NSQS::EAction::SendMessageBatch;
                    }

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
                        .FolderID = "" 
                    };

                    auto authRequestProxy = MakeHolder<NSQS::THttpProxyAuthRequestProxy>(
                        std::move(data),
                        "",
                        ctx.SelfID);

                    ctx.RegisterWithSameMailbox(authRequestProxy.Release());
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
                                                               std::move(issues));
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

                if (issueCode.has_value()) {
                    HttpContext.DoReply(ctx, issueCode.value());
                } else {
                    HttpContext.DoReply(ctx);
                }
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
                    decltype(&Ydb::Ymq::V1::YmqService::Stub::Async##name), NKikimr::NGRpcService::TEvYmq##name##Request>> \
                    (#name, &Ydb::Ymq::V1::YmqService::Stub::Async##name, [](Ydb::Ymq::V1::name##Request&){return "";});
        DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(GetQueueUrl);
        DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(CreateQueue);
        DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(ListQueues);
        #undef DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN

        #define DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(name) Name2YmqProcessor[#name] = MakeHolder<TYmqHttpRequestProcessor<Ydb::Ymq::V1::YmqService, Ydb::Ymq::V1::name##Request, Ydb::Ymq::V1::name##Response, Ydb::Ymq::V1::name##Result,\
                    decltype(&Ydb::Ymq::V1::YmqService::Stub::Async##name), NKikimr::NGRpcService::TEvYmq##name##Request>> \
                    (#name, &Ydb::Ymq::V1::YmqService::Stub::Async##name, [](Ydb::Ymq::V1::name##Request& request){return request.Getqueue_url();});
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(SendMessage);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(ReceiveMessage);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(GetQueueAttributes);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(DeleteMessage);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(PurgeQueue);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(DeleteQueue);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(ChangeMessageVisibility);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(SetQueueAttributes);
        DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(SendMessageBatch);
        #undef DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN
    }

    void SetApiVersionDisabledErrorText(THttpRequestContext& context) {
        context.ResponseData.ErrorText = (TStringBuilder() << context.ApiVersion << " is disabled");
    }

    bool THttpRequestProcessors::Execute(const TString& name, THttpRequestContext&& context,
                                         THolder<NKikimr::NSQS::TAwsRequestSignV4> signature,
                                         const TActorContext& ctx) {
        THashMap<TString, THolder<IHttpRequestProcessor>>* Name2Processor;
        if (context.ApiVersion == "AmazonSQS") {
            if (!context.ServiceConfig.GetHttpConfig().GetYmqEnabled()) {
                context.ResponseData.IsYmq = true;
                context.ResponseData.YmqHttpCode = 400;
                SetApiVersionDisabledErrorText(context);
            }
            Name2Processor = &Name2YmqProcessor;
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

        DatabasePath = Request->URL;
        if (DatabasePath == "/") {
           DatabasePath = "";
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
                Request->Content;
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
                response->Set(CRC32_HEADER, ToString(crc32(body.data(), body.size())));
                response->Set<&NHttp::THttpResponse::ContentType>(contentType);
                if (!request->Endpoint->CompressContentTypes.empty()) {
                    contentType = contentType.Before(';');
                    NHttp::Trim(contentType, ' ');
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
            if (ResponseData.IsYmq) {
                ResponseData.Body["__type"] = ResponseData.YmqStatusCode;
            } else {
                ResponseData.Body["__type"] = MapToException(ResponseData.Status, MethodName, issueCode).first;
            }
        }

        TString errorName;
        ui32 httpCode;
        if (ResponseData.IsYmq) {
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

    class THttpAuthActor : public NActors::TActorBootstrapped<THttpAuthActor> {
    public:
        using TBase = NActors::TActorBootstrapped<THttpAuthActor>;

        THttpAuthActor(const TActorId sender, THttpRequestContext& context,
                          THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature)
            : Sender(sender)
            , Prefix(context.LogPrefix())
            , ServiceAccountId(context.ServiceAccountId)
            , ServiceAccountCredentialsProvider(context.ServiceAccountCredentialsProvider)
            , RequestId(context.RequestId)
            , Signature(std::move(signature))
            , ServiceConfig(context.ServiceConfig)
            , IamToken(context.IamToken)
            , Authorize(!context.Driver)
            , DatabasePath(context.DatabasePath)
            , StreamName(context.StreamName)
        {
        }

        TStringBuilder LogPrefix() const {
            return TStringBuilder() << Prefix << " [auth] ";
        }

    private:
        STFUNC(StateWork)
        {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
                HFunc(NCloud::TEvAccessService::TEvAuthenticateResponse, HandleAuthenticationResult);
                HFunc(NCloud::TEvIamTokenService::TEvCreateResponse, HandleServiceAccountIamToken);
                HFunc(TEvTicketParser::TEvAuthorizeTicketResult, HandleTicketParser);
                HFunc(TEvents::TEvPoisonPill, HandlePoison);
                default:
                    HandleUnexpectedEvent(ev);
                    break;
                }
        }

        void SendDescribeRequest(const TActorContext& ctx) {
            auto schemeCacheRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = NKikimr::SplitPath(DatabasePath);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.SyncVersion = false;
            schemeCacheRequest->ResultSet.emplace_back(entry);
            ctx.Send(MakeSchemeCacheID(), MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
            const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
            if (navigate->ErrorCount) {
                return ReplyWithError(
                    ctx, NYdb::EStatus::SCHEME_ERROR, TStringBuilder() << "Database with path '" << DatabasePath << "' doesn't exists",
                    NYds::EErrorCodes::NOT_FOUND
                );
            }
            Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
            if (navigate->ResultSet.front().PQGroupInfo) {
                const auto& description = navigate->ResultSet.front().PQGroupInfo->Description;
                FolderId = description.GetPQTabletConfig().GetYcFolderId();
                CloudId = description.GetPQTabletConfig().GetYcCloudId();
                DatabaseId = description.GetPQTabletConfig().GetYdbDatabaseId();
                DatabasePath = description.GetPQTabletConfig().GetYdbDatabasePath();
            }
            for (const auto& attr : navigate->ResultSet.front().Attributes) {
                if (attr.first == "folder_id") FolderId = attr.second;
                if (attr.first == "cloud_id") CloudId = attr.second;
                if (attr.first == "database_id") DatabaseId = attr.second;
            }
            SendAuthenticationRequest(ctx);
        }

        void HandlePoison(const TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
            TBase::Die(ctx);
        }

        void HandleTicketParser(const TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx) {

            if (ev->Get()->Error) {
                return ReplyWithError(ctx, ev->Get()->Error.Retryable ? NYdb::EStatus::UNAVAILABLE : NYdb::EStatus::UNAUTHORIZED, ev->Get()->Error.Message);
            }
            ctx.Send(Sender, new TEvServerlessProxy::TEvToken(ev->Get()->Token->GetUserSID(), "", ev->Get()->SerializedToken, {"", DatabaseId, DatabasePath, CloudId, FolderId}));

            LOG_SP_DEBUG_S(ctx, NKikimrServices::HTTP_PROXY, "Authorized successfully");

            TBase::Die(ctx);
        }

        void SendAuthenticationRequest(const TActorContext& ctx) {
            TInstant signedAt;
            if (!Signature.Get() && IamToken.empty()) {
                return ReplyWithError(ctx, NYdb::EStatus::UNAUTHORIZED,
                                      "Neither Credentials nor IAM token was provided",
                                      NYds::EErrorCodes::INCOMPLETE_SIGNATURE
                );
            }
            if (Signature) {
                bool found = false;
                for (auto& cr : ServiceConfig.GetHttpConfig().GetYandexCloudServiceRegion()) {
                    if (cr == Signature->GetRegion()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return ReplyWithError(ctx, NYdb::EStatus::UNAUTHORIZED,
                        TStringBuilder() << "Wrong service region: got " << Signature->GetRegion() <<
                        " expected " << ServiceConfig.GetHttpConfig().GetYandexCloudServiceRegion(0),
                        NYds::EErrorCodes::INCOMPLETE_SIGNATURE
                    );
                }

                if (!TInstant::TryParseIso8601(Signature->GetSigningTimestamp(), signedAt)) {
                    return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST,
                                          "Failed to parse Signature timestamp",
                                          NYds::EErrorCodes::INCOMPLETE_SIGNATURE
                    );
                }

                if (Signature->GetAccessKeyId().empty()) {
                    return ReplyWithError(ctx, NYdb::EStatus::UNAUTHORIZED,
                                          "Access key id should be provided",
                                          NYds::EErrorCodes::MISSING_AUTHENTICATION_TOKEN
                    );
                }
            }


            if (Authorize) {
                auto entries = NKikimr::NGRpcProxy::V1::GetTicketParserEntries(DatabaseId, FolderId);
                if (Signature.Get()) {
                    TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature;
                    signature.AccessKeyId = Signature->GetAccessKeyId();
                    signature.StringToSign = Signature->GetStringToSign();
                    signature.Signature = Signature->GetParsedSignature();
                    signature.Service = "kinesis";
                    signature.Region = Signature->GetRegion();
                    signature.SignedAt = signedAt;

                    THolder<TEvTicketParser::TEvAuthorizeTicket> request =
                        MakeHolder<TEvTicketParser::TEvAuthorizeTicket>(std::move(signature), "", entries);
                    ctx.Send(MakeTicketParserID(), request.Release());
                } else {
                    THolder<TEvTicketParser::TEvAuthorizeTicket> request =
                        MakeHolder<TEvTicketParser::TEvAuthorizeTicket>(IamToken, "", entries);
                    ctx.Send(MakeTicketParserID(), request.Release());

                }
                return;
            }

            THolder<NCloud::TEvAccessService::TEvAuthenticateRequest> request =
                MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
            request->RequestId = RequestId;

            auto& signature = *request->Request.mutable_signature();
            signature.set_access_key_id(Signature->GetAccessKeyId());
            signature.set_string_to_sign(Signature->GetStringToSign());
            signature.set_signature(Signature->GetParsedSignature());

            auto& v4params = *signature.mutable_v4_parameters();
            v4params.set_service("kinesis");
            v4params.set_region(Signature->GetRegion());

            const ui64 nanos = signedAt.NanoSeconds();
            const ui64 seconds = nanos / 1'000'000'000ull;
            const ui64 nanos_left = nanos % 1'000'000'000ull;

            v4params.mutable_signed_at()->set_seconds(seconds);
            v4params.mutable_signed_at()->set_nanos(nanos_left);

            ctx.Send(MakeAccessServiceID(), std::move(request));
        }

        void HandleUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev) {
            Y_UNUSED(ev);
        }

        void HandleAuthenticationResult(NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev,
                                        const TActorContext& ctx) {
            if (!ev->Get()->Status.Ok()) {
                RetryCounter.Click();
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, "retry #" << RetryCounter.AttempN() << "; " <<
                              "can not authenticate service account user: " << ev->Get()->Status.Msg);
                if (RetryCounter.HasAttemps()) {
                    SendAuthenticationRequest(ctx);
                    return;
                }
                return ReplyWithError(ctx, ev->Get()->Status.InternalError || NKikimr::IsRetryableGrpcError(ev->Get()->Status)
                                                                    ? NYdb::EStatus::UNAVAILABLE
                                                                    : NYdb::EStatus::UNAUTHORIZED,
                                         TStringBuilder() << "requestid " << RequestId
                                         << "; can not authenticate service account user");

            } else if (!ev->Get()->Response.subject().has_service_account()) {
                return ReplyWithError(ctx, NYdb::EStatus::INTERNAL_ERROR,
                                      "(this error should not have been reached).");
            }
            RetryCounter.Void();

            ServiceAccountId = ev->Get()->Response.subject().service_account().id();
            LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, "authenticated to " << ServiceAccountId);
            SendIamTokenRequest(ctx);
        }

        void SendIamTokenRequest(const TActorContext& ctx) {
            auto request = MakeHolder<NCloud::TEvIamTokenService::TEvCreateForServiceAccountRequest>();
            request->RequestId = RequestId;
            request->Token = ServiceAccountCredentialsProvider->GetAuthInfo();
            request->Request.set_service_account_id(ServiceAccountId);

            ctx.Send(MakeIamTokenServiceID(), std::move(request));
        }

        void ReplyWithError(const TActorContext& ctx, NYdb::EStatus status, const TString& errorText,
                            NYds::EErrorCodes issueCode = NYds::EErrorCodes::GENERIC_ERROR) {
            ctx.Send(Sender, new TEvServerlessProxy::TEvErrorWithIssue(status, errorText, {"", DatabaseId, DatabasePath, CloudId, FolderId}, static_cast<size_t>(issueCode)));
            TBase::Die(ctx);
        }

        void HandleServiceAccountIamToken(NCloud::TEvIamTokenService::TEvCreateResponse::TPtr& ev,
                                          const TActorContext& ctx) {
            if (!ev->Get()->Status.Ok()) {
                RetryCounter.Click();
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, "retry #" << RetryCounter.AttempN() << "; " <<
                              "IAM token issue error: " << ev->Get()->Status.Msg);

                if (RetryCounter.HasAttemps()) {
                    SendIamTokenRequest(ctx);
                    return;
                }
                return ReplyWithError(ctx, ev->Get()->Status.InternalError || NKikimr::IsRetryableGrpcError(ev->Get()->Status)
                                                                    ? NYdb::EStatus::UNAVAILABLE
                                                                    : NYdb::EStatus::UNAUTHORIZED,
                                            TStringBuilder() << "IAM token issue error: " << ev->Get()->Status.Msg);
            }
            RetryCounter.Void();

            Y_ABORT_UNLESS(!ev->Get()->Response.iam_token().empty());

            ctx.Send(Sender,
                     new TEvServerlessProxy::TEvToken(ServiceAccountId, ev->Get()->Response.iam_token(), "", {}));

            LOG_SP_DEBUG_S(ctx, NKikimrServices::HTTP_PROXY, "IAM token generated");

            TBase::Die(ctx);
        }

    public:
        void Bootstrap(const TActorContext& ctx) {
            TBase::Become(&THttpAuthActor::StateWork);

            if (Authorize) {
                SendDescribeRequest(ctx);
                return;
            }
            if (ServiceAccountId.empty()) {
                SendAuthenticationRequest(ctx);
            } else {
                SendIamTokenRequest(ctx);
            }
        }

    private:
        const TActorId Sender;
        const TString Prefix;
        TString ServiceAccountId;
        std::shared_ptr<NYdb::ICredentialsProvider> ServiceAccountCredentialsProvider;
        const TString RequestId;
        THolder<NKikimr::NSQS::TAwsRequestSignV4> Signature;
        TRetryCounter RetryCounter;
        const NKikimrConfig::TServerlessProxyConfig& ServiceConfig;
        TString IamToken;
        bool Authorize;
        TString FolderId;
        TString CloudId;
        TString DatabaseId;
        TString DatabasePath;
        TString StreamName;
    };


    NActors::IActor* CreateIamAuthActor(const TActorId sender, THttpRequestContext& context, THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature)
    {
        return new THttpAuthActor(sender, context, std::move(signature));
    }


    NActors::IActor* CreateIamTokenServiceActor(const NKikimrConfig::TServerlessProxyConfig& config)
    {
        NCloud::TIamTokenServiceSettings tsSettings;
        tsSettings.Endpoint = config.GetHttpConfig().GetIamTokenServiceEndpoint();
        if (config.GetCaCert()) {
            TString certificate = TFileInput(config.GetCaCert()).ReadAll();
            tsSettings.CertificateRootCA = certificate;
        }
        return NCloud::CreateIamTokenService(tsSettings);
    }

    NActors::IActor* CreateAccessServiceActor(const NKikimrConfig::TServerlessProxyConfig& config)
    {
        NCloud::TAccessServiceSettings asSettings;
        asSettings.Endpoint = config.GetHttpConfig().GetAccessServiceEndpoint();

        if (config.GetCaCert()) {
            TString certificate = TFileInput(config.GetCaCert()).ReadAll();
            asSettings.CertificateRootCA = certificate;
        }
        return NCloud::CreateAccessServiceWithCache(asSettings);
    }

} // namespace NKikimr::NHttpProxy


template <>
void Out<NKikimr::NHttpProxy::THttpResponseData>(IOutputStream& o, const NKikimr::NHttpProxy::THttpResponseData& p) {
    TString s = TStringBuilder() << "NYdb status: " << std::to_string(static_cast<size_t>(p.Status)) <<
    ". Body: " << NJson::WriteJson(p.Body) << ". Error text: " << p.ErrorText;
    o.Write(s.data(), s.length());
}
