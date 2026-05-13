#include "auth_factory.h"
#include "custom_metrics.h"
#include "exceptions_mapping.h"
#include "http_req.h"
#include "json_proto_conversion.h"
#include "utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/services/datastreams/datastreams_proxy.h>
#include <ydb/services/datastreams/next_token.h>
#include <ydb/services/datastreams/shard_iterator.h>
#include <ydb/services/sqs_topic/utils.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <optional>

namespace NKikimr::NHttpProxy {

    using namespace google::protobuf;
    using namespace Ydb::DataStreams::V1;
    using namespace NYdb::NDataStreams::V1;

    namespace {

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


    class TController : public IHttpController {
    public:
        TController() {
            #define DECLARE_DATASTREAMS_PROCESSOR(name) Name2Processor[#name] = MakeHolder<THttpRequestProcessor<\
                DataStreamsService,                                                     \
                name##Request,                                                          \
                name##Response,                                                         \
                name##Result,                                                           \
                decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::Async##name), \
                NKikimr::NGRpcService::TEvDataStreams##name##Request>>                  \
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

        }

        std::expected<IHttpRequestProcessor*, IHttpController::EError> GetProcessor(
            const TString& name,
            const THttpRequestContext& context
        ) const override {
            if (context.ApiVersion != "kinesisApi") {
                return std::unexpected(IHttpController::EError::NotMyProtocol);
            }

            if (auto proc = Name2Processor.find(name); proc != Name2Processor.end()) {
                return std::expected<IHttpRequestProcessor*, IHttpController::EError>(proc->second.Get());
            }

            return std::unexpected(IHttpController::EError::MethodNotFound);
        }

        private:
            THashMap<TString, THolder<IHttpRequestProcessor>> Name2Processor;
    };

    } // namespace

    std::shared_ptr<const IHttpController> CreateDataStreamsHttpController(const NKikimrConfig::TServerlessProxyConfig& config) {
        if (config.GetHttpConfig().GetDataStreamsEnabled()) {
            return std::make_shared<TController>();
        }
        return {};
    }

} // namespace NKikimr::NHttpProxy
