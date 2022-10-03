#include "auth_factory.h"
#include "events.h"
#include "http_req.h"
#include "json_proto_conversion.h"
#include "custom_metrics.h"

#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/response.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_caching/cached_grpc_request_actor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/core/viewer/json/json.h>

#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>

#include <ydb/services/datastreams/datastreams_proxy.h>
#include <ydb/services/datastreams/next_token.h>
#include <ydb/services/datastreams/shard_iterator.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <library/cpp/uri/uri.h>

#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/vector.h>

#include <nlohmann/json.hpp>


namespace NKikimr::NHttpProxy {

    using namespace google::protobuf;
    using namespace Ydb::DataStreams::V1;
    using namespace NYdb::NDataStreams::V1;

    TString StatusToErrorType(NYdb::EStatus status) {
        switch(status) {
        case NYdb::EStatus::BAD_REQUEST:
            return "InvalidParameterValueException"; //TODO: bring here issues and parse from them
        case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
        case NYdb::EStatus::UNAUTHORIZED:
            return "AccessDeniedException";
        case NYdb::EStatus::INTERNAL_ERROR:
            return "InternalFailureException";
        case NYdb::EStatus::ABORTED:
            return "RequestExpiredException"; //TODO: find better code
        case NYdb::EStatus::UNAVAILABLE:
            return "ServiceUnavailableException";
        case NYdb::EStatus::OVERLOADED:
            return "ThrottlingException";
        case NYdb::EStatus::SCHEME_ERROR:
            return "ResourceNotFoundException";
        case NYdb::EStatus::GENERIC_ERROR:
            return "InternalFailureException"; //TODO: find better code
        case NYdb::EStatus::TIMEOUT:
            return "RequestTimeoutException";
        case NYdb::EStatus::BAD_SESSION:
            return "AccessDeniedException";
        case NYdb::EStatus::PRECONDITION_FAILED:
        case NYdb::EStatus::ALREADY_EXISTS:
            return "ValidationErrorException"; //TODO: find better code
        case NYdb::EStatus::NOT_FOUND:
            return "ResourceNotFoundException";
        case NYdb::EStatus::SESSION_EXPIRED:
            return "AccessDeniedException";
        case NYdb::EStatus::UNSUPPORTED:
            return "InvalidActionException";
        default:
            return "InternalFailureException";
        }

    }

    HttpCodes StatusToHttpCode(NYdb::EStatus status) {
        switch(status) {
        case NYdb::EStatus::UNSUPPORTED:
        case NYdb::EStatus::BAD_REQUEST:
            return HTTP_BAD_REQUEST;
        case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
        case NYdb::EStatus::UNAUTHORIZED:
            return HTTP_FORBIDDEN;
        case NYdb::EStatus::INTERNAL_ERROR:
            return HTTP_INTERNAL_SERVER_ERROR;
        case NYdb::EStatus::ABORTED:
            return HTTP_CONFLICT;
        case NYdb::EStatus::UNAVAILABLE:
            return HTTP_SERVICE_UNAVAILABLE;
        case NYdb::EStatus::OVERLOADED:
            return HTTP_BAD_REQUEST;
        case NYdb::EStatus::SCHEME_ERROR:
            return HTTP_NOT_FOUND;
        case NYdb::EStatus::GENERIC_ERROR:
            return HTTP_BAD_REQUEST;
        case NYdb::EStatus::TIMEOUT:
            return HTTP_GATEWAY_TIME_OUT;
        case NYdb::EStatus::BAD_SESSION:
            return HTTP_UNAUTHORIZED;
        case NYdb::EStatus::PRECONDITION_FAILED:
            return HTTP_PRECONDITION_FAILED;
        case NYdb::EStatus::ALREADY_EXISTS:
            return HTTP_CONFLICT;
        case NYdb::EStatus::NOT_FOUND:
            return HTTP_NOT_FOUND;
        case NYdb::EStatus::SESSION_EXPIRED:
            return HTTP_UNAUTHORIZED;
        default:
            return HTTP_INTERNAL_SERVER_ERROR;
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
    TString TruncateStreamName(const TProto& req, const TString& database)
    {
        constexpr bool has_stream_name = requires(const TProto& t) {
            t.stream_name();
        };

        if constexpr (has_stream_name) {
            Y_VERIFY(req.stream_name().StartsWith(database));
            return req.stream_name().substr(database.size(), -1);
        }
        return ExtractStreamNameWithoutProtoField<TProto>(req).substr(database.size(), -1);
    }

    constexpr TStringBuf IAM_HEADER = "x-yacloud-subjecttoken";
    constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";
    constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";
    constexpr TStringBuf REQUEST_DATE_HEADER = "x-amz-date";
    constexpr TStringBuf REQUEST_FORWARDED_FOR = "x-forwarded-for";
    constexpr TStringBuf REQUEST_TARGET_HEADER = "x-amz-target";
    constexpr TStringBuf REQUEST_CONTENT_TYPE_HEADER = "content-type";
    static const TString CREDENTIAL_PARAM = "credential";

    template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoCall, class TRpcEv>
    class THttpRequestProcessor : public IHttpRequestProcessor {
    public:
        enum TRequestState {
            StateIdle,
            StateAuthentication,
            StateAuthorization,
            StateListEndpoints,
            StateGrpcRequest,
            StateFinished
        };

        enum TEv {
            EvRequest,
            EvResponse,
            EvResult
        };

    public:
        THttpRequestProcessor(TString method, TProtoCall protoCall)
            : Method(method)
            , ProtoCall(protoCall)
        {
        }

        const TString& Name() const override {
            return Method;
        }

        void Execute(THttpRequestContext&& context, THolder<NKikimr::NSQS::TAwsRequestSignV4> signature, const TActorContext& ctx) override {
            ctx.Register(new THttpRequestActor(
                    std::move(context),
                    std::move(signature),
                    ProtoCall, Method));
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
                    HFunc(TEvServerlessProxy::TEvError, HandleError);
                    HFunc(TEvServerlessProxy::TEvGrpcRequestResult, HandleGrpcResponse);
                    HFunc(TEvServerlessProxy::TEvToken, HandleToken);
                    default:
                        HandleUnexpectedEvent(ev, ctx);
                        break;
                }
            }

            void SendYdbDriverRequest(const TActorContext& ctx) {
                Y_VERIFY(HttpContext.Driver);

                RequestState = StateAuthorization;

                auto request = MakeHolder<TEvServerlessProxy::TEvDiscoverDatabaseEndpointRequest>();
                request->DatabasePath = HttpContext.DatabaseName;

                ctx.Send(MakeTenantDiscoveryID(), std::move(request));
            }

            void CreateClient(const TActorContext& ctx) {
                RequestState = StateListEndpoints;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "create client to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabaseName <<
                              "' iam token size: " << HttpContext.IamToken.size());

                auto clientSettings = NYdb::TCommonClientSettings()
                        .DiscoveryEndpoint(HttpContext.DiscoveryEndpoint)
                        .Database(HttpContext.DatabaseName)
                        .AuthToken(HttpContext.IamToken)
                        .DiscoveryMode(NYdb::EDiscoveryMode::Async);

                if (!HttpContext.DatabaseName.empty() && !HttpContext.ServiceConfig.GetTestMode()) {
                    clientSettings.Database(HttpContext.DatabaseName);
                }
                Y_VERIFY(!Client);
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
                RequestState = StateGrpcRequest;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabaseName <<
                              "' iam token size: " << HttpContext.IamToken.size());

                RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(Request), HttpContext.DatabaseName,
                                                            HttpContext.SerializedUserToken, ctx.ActorSystem());
                RpcFuture.Subscribe([actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()]
                                    (const NThreading::TFuture<TProtoResponse>& future) {
                    auto& response = future.GetValueSync();
                    auto result = MakeHolder<TEvServerlessProxy::TEvGrpcRequestResult>();
                    Y_VERIFY(response.operation().ready());
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
                RequestState = StateGrpcRequest;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabaseName <<
                              "' iam token size: " << HttpContext.IamToken.size());

                Y_VERIFY(Client);
                Y_VERIFY(DiscoveryFuture->HasValue());

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

            void HandleUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev, const TActorContext& ctx) {
                Y_UNUSED(ev, ctx);
            }

            void HandleToken(TEvServerlessProxy::TEvToken::TPtr& ev, const TActorContext& ctx) {
                HttpContext.ServiceAccountId = ev->Get()->ServiceAccountId;
                HttpContext.IamToken = ev->Get()->IamToken;
                HttpContext.SerializedUserToken = ev->Get()->SerializedUserToken;

                if (HttpContext.Driver) {
                    SendYdbDriverRequest(ctx);
                } else {
                    SendGrpcRequestNoDriver(ctx);
                }
            }

            void HandleError(TEvServerlessProxy::TEvError::TPtr& ev, const TActorContext& ctx) {
                ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Response);
            }

            void ReplyWithError(const TActorContext& ctx, NYdb::EStatus status, const TString& errorText) {
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                             {{"method", Method},
                              {"cloud", HttpContext.CloudId},
                              {"folder", HttpContext.FolderId},
                              {"database", HttpContext.DatabaseId},
                              {"stream", HttpContext.StreamName},
                              {"code", TStringBuilder() << (int)StatusToHttpCode(status)},
                              {"name", "api.http.errors_per_second"}}
                         });

                HttpContext.ResponseData.Status = status;
                HttpContext.ResponseData.ErrorText = errorText;
                HttpContext.DoReply(ctx);

                ctx.Send(AuthActor, new TEvents::TEvPoisonPill());

                TBase::Die(ctx);
            }

            void Handle(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult::TPtr ev,
                        const TActorContext& ctx) {
                if (ev->Get()->DatabaseInfo) {
                    auto& db = ev->Get()->DatabaseInfo;
                    HttpContext.FolderId = db->FolderId;
                    HttpContext.CloudId = db->CloudId;
                    HttpContext.DatabaseId = db->Id;
                    HttpContext.DiscoveryEndpoint = db->Endpoint;
                    HttpContext.DatabaseName = db->Path;

                    if (ExtractStreamName<TProtoRequest>(Request).StartsWith(HttpContext.DatabaseName + "/")) {
                        HttpContext.StreamName =
                            TruncateStreamName<TProtoRequest>(Request, HttpContext.DatabaseName + "/");
                    } else {
                        HttpContext.StreamName = ExtractStreamName<TProtoRequest>(Request);
                    }

                    FillInputCustomMetrics<TProtoRequest>(Request, HttpContext, ctx);
                    ctx.Send(MakeMetricsServiceID(),
                             new TEvServerlessProxy::TEvCounter{1, true, true,
                                 BuildLabels(Method, HttpContext, "api.http.requests_per_second")
                             });
                    CreateClient(ctx);
                    return;
                }

                return ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Message);
            }

            void ReportLatencyCounters(const TActorContext& ctx) {
                TDuration dur = ctx.Now() - StartTime;
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvHistCounter{static_cast<i64>(dur.MilliSeconds()), 1,
                                    BuildLabels(Method, HttpContext, "api.http.requests_duration_milliseconds")
                        });
            }

            void HandleGrpcResponse(TEvServerlessProxy::TEvGrpcRequestResult::TPtr ev,
                                    const TActorContext& ctx) {
                if (ev->Get()->Status->IsSuccess()) {
                    ProtoToJson(*ev->Get()->Message, HttpContext.ResponseData.Body);
                    FillOutputCustomMetrics<TProtoResult>(
                        *(dynamic_cast<TProtoResult*>(ev->Get()->Message.Get())), HttpContext, ctx);
                    ReportLatencyCounters(ctx);

                    ctx.Send(MakeMetricsServiceID(),
                             new TEvServerlessProxy::TEvCounter{1, true, true,
                                 BuildLabels(Method, HttpContext, "api.http.success_per_second")
                             });

                    HttpContext.DoReply(ctx);
                } else {
                    auto retryClass =
                        NYdb::NPersQueue::GetRetryErrorClass(ev->Get()->Status->GetStatus());

                    switch (retryClass) {
                    case ERetryErrorClass::ShortRetry:
                    case ERetryErrorClass::LongRetry:
                        RetryCounter.Click();
                        if (RetryCounter.HasAttemps()) {
                            return HttpContext.Driver ? SendGrpcRequest(ctx) : SendGrpcRequestNoDriver(ctx);
                        }
                    case ERetryErrorClass::NoRetry: {
                        TString errorText;
                        TStringOutput stringOutput(errorText);
                        ev->Get()->Status->GetIssues().PrintTo(stringOutput);
                        RetryCounter.Void();
                        return ReplyWithError(ctx, ev->Get()->Status->GetStatus(), errorText);
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
                } catch (std::exception& e) {
                    LOG_SP_WARN_S(ctx, NKikimrServices::HTTP_PROXY,
                                  "got new request with incorrect json from [" << HttpContext.SourceAddress << "] " <<
                                  "database '" << HttpContext.DatabaseName << "'");

                    return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST, e.what());
                }

                if (HttpContext.DatabaseName.empty()) {
                    HttpContext.DatabaseName = ExtractStreamName<TProtoRequest>(Request);
                }

                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "got new request from [" << HttpContext.SourceAddress << "] " <<
                              "database '" << HttpContext.DatabaseName << "' " <<
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
            TRequestState RequestState = StateIdle;
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
        };

    private:
        TString Method;

        struct TAccessKeySignature {
            TString AccessKeyId;
            TString SignedString;
            TString Signature;
            TString Region;
            TInstant SignedAt;
        };

        TProtoCall ProtoCall;
    };


    void THttpRequestProcessors::Initialize() {
        #define DECLARE_PROCESSOR(name) Name2Processor[#name] = MakeHolder<THttpRequestProcessor<DataStreamsService, name##Request, name##Response, name##Result,\
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::Async##name), NKikimr::NGRpcService::TEvDataStreams##name##Request>> \
                    (#name, &Ydb::DataStreams::V1::DataStreamsService::Stub::Async##name);
        DECLARE_PROCESSOR(PutRecords);
        DECLARE_PROCESSOR(CreateStream);
        DECLARE_PROCESSOR(ListStreams);
        DECLARE_PROCESSOR(DeleteStream);
        DECLARE_PROCESSOR(DescribeStream);
        DECLARE_PROCESSOR(ListShards);
        DECLARE_PROCESSOR(PutRecord);
        DECLARE_PROCESSOR(GetRecords);
        DECLARE_PROCESSOR(GetShardIterator);
        DECLARE_PROCESSOR(DescribeLimits);
        DECLARE_PROCESSOR(DescribeStreamSummary);
        DECLARE_PROCESSOR(DecreaseStreamRetentionPeriod);
        DECLARE_PROCESSOR(IncreaseStreamRetentionPeriod);
        DECLARE_PROCESSOR(UpdateShardCount);
        DECLARE_PROCESSOR(UpdateStreamMode);
        DECLARE_PROCESSOR(RegisterStreamConsumer);
        DECLARE_PROCESSOR(DeregisterStreamConsumer);
        DECLARE_PROCESSOR(DescribeStreamConsumer);
        DECLARE_PROCESSOR(ListStreamConsumers);
        DECLARE_PROCESSOR(AddTagsToStream);
        DECLARE_PROCESSOR(DisableEnhancedMonitoring);
        DECLARE_PROCESSOR(EnableEnhancedMonitoring);
        DECLARE_PROCESSOR(ListTagsForStream);
        DECLARE_PROCESSOR(MergeShards);
        DECLARE_PROCESSOR(RemoveTagsFromStream);
        DECLARE_PROCESSOR(SplitShard);
        DECLARE_PROCESSOR(StartStreamEncryption);
        DECLARE_PROCESSOR(StopStreamEncryption);
        #undef DECLARE_PROCESSOR
    }

    bool THttpRequestProcessors::Execute(const TString& name, THttpRequestContext&& context,
                                         THolder<NKikimr::NSQS::TAwsRequestSignV4> signature,
                                         const TActorContext& ctx) {
        if (auto proc = Name2Processor.find(name); proc != Name2Processor.end()) {
            proc->second->Execute(std::move(context), std::move(signature), ctx);
            return true;
        }
        context.SendBadRequest(NYdb::EStatus::BAD_REQUEST,
                               TStringBuilder() << "Unknown method name " << name, ctx);
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

        DatabaseName = Request->URL;
        if (DatabaseName == "/") {
           DatabaseName = "";
        }

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

    void THttpRequestContext::SendBadRequest(NYdb::EStatus status, const TString& errorText,
                                             const TActorContext& ctx) {
        ResponseData.Body.SetType(NJson::JSON_MAP);
        ResponseData.Body["message"] = errorText;
        ResponseData.Body["__type"] = StatusToErrorType(status);

        LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                      "reply with status: " << status << " message: " << errorText);
        auto res = Request->CreateResponse(
                TStringBuilder() << (int)StatusToHttpCode(status),
                StatusToErrorType(status),
                strByMime(ContentType),
                ResponseData.DumpBody(ContentType)
            );
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(res));
    }

    void THttpRequestContext::DoReply(const TActorContext& ctx) {
        if (ResponseData.Status == NYdb::EStatus::SUCCESS) {
            LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, "reply ok");
            auto res = Request->CreateResponseOK(
                    ResponseData.DumpBody(ContentType),
                    strByMime(ContentType)
                );
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(res));
        } else {
            SendBadRequest(ResponseData.Status, ResponseData.ErrorText, ctx);
        }
    }

    void THttpRequestContext::ParseHeaders(TStringBuf str) {
        TString sourceReqId;
        NHttp::THeaders headers(str);
        for (const auto& header : headers.Headers) {
            if (AsciiEqualsIgnoreCase(header.first, IAM_HEADER)) {
                IamToken = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, AUTHORIZATION_HEADER)) {
                if (header.second.StartsWith("Bearer ")) {
                    IamToken = header.second;
                }
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_ID_HEADER)) {
                sourceReqId = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_FORWARDED_FOR)) {
                SourceAddress = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_TARGET_HEADER)) {
                TString requestTarget = TString(header.second);
                TVector<TString> parts = SplitString(requestTarget, ".");
                ApiVersion = parts[0];
                MethodName = parts[1];
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_CONTENT_TYPE_HEADER)) {
                ContentType = mimeByStr(header.second);
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_DATE_HEADER)) {
            }
        }
        RequestId = GenerateRequestId(sourceReqId);
    }

    TString THttpResponseData::DumpBody(MimeTypes contentType) {
        switch (contentType) {
        case MIME_CBOR: {
            auto toCborStr = NJson::WriteJson(Body, false);
            auto toCbor =  nlohmann::json::to_cbor({toCborStr.begin(), toCborStr.end()});
            return {(char*)&toCbor[0], toCbor.size()};
        }
        default: {
        case MIME_JSON:
            return NJson::WriteJson(Body, false);
        }
        }
    }

    void THttpRequestContext::RequestBodyToProto(NProtoBuf::Message* request) {
        auto requestJsonStr = Request->Body;
        if (requestJsonStr.empty()) {
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                "Empty body";
        }

        // recursive is default setting
        if (auto listStreamsRequest = dynamic_cast<Ydb::DataStreams::V1::ListStreamsRequest*>(request)) {
            listStreamsRequest->set_recurse(true);
        }

        std::string bufferStr;
        switch (ContentType) {
        case MIME_CBOR: {
            // CborToProto(HttpContext.Request->Body, request);
            auto fromCbor = nlohmann::json::from_cbor(Request->Body.begin(),
                                                      Request->Body.end(), true, false);
            if (fromCbor.is_discarded()) {
                throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                    "Can not parse request body from CBOR";
            } else {
                bufferStr = fromCbor.dump();
                requestJsonStr = TStringBuf(bufferStr.begin(), bufferStr.end());
            }
        }
        case MIME_JSON: {
            NJson::TJsonValue requestBody;
            auto fromJson = NJson::ReadJsonTree(requestJsonStr, &requestBody);
            if (fromJson) {
                JsonToProto(requestBody, request);
            } else {
                throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                    "Can not parse request body from JSON";
            }
            break;
        }
        default:
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                "Unknown ContentType";
        }
    }
} // namespace NKikimr::NHttpProxy
