#include "sqs.h"

#include "auth_factory.h"
#include "controller_base.h"
#include "custom_metrics.h"
#include "exceptions_mapping.h"
#include "http_req.h"
#include "sqs_serialization.h"
#include "utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/core/ymq/actor/auth_multi_factory.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/public/api/grpc/draft/ydb_sqs_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>
#include <ydb/services/sqs_topic/sqs_topic_proxy.h>
#include <ydb/services/sqs_topic/utils.h>
#include <ydb/services/ymq/grpc_service.h>
#include <ydb/services/ymq/rpc_params.h>
#include <ydb/services/ymq/utils.h>
#include <ydb/services/ymq/ymq_proxy.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NHttpProxy {

    namespace {

    template <class TRequest>
    TString MaybeGetQueueUrl(const TRequest& request) {
        if constexpr (requires {request.Getqueue_url(); } ) {
            return request.Getqueue_url();
        }
        return {};
    }

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
                if (Signature && Signature->Empty()) {
                    Signature.Reset();
                }
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
                    HFunc(TEvTicketParser::TEvAuthorizeTicketResult, HandleSecurityTokenAuth);
                    default:
                        HandleUnexpectedEvent(ev);
                        break;
                }
            }

            void SendGrpcRequestNoDriver(const TActorContext& ctx) {
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabasePath <<
                              "' iam token size: " << HttpContext.IamToken.size());

                TMap<TString, TString> peerMetadata {
                    {NYmq::V1::REQUEST_ID, HttpContext.RequestId},
                };

                RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(
                    std::move(Request),
                    HttpContext.DatabasePath,
                    HttpContext.SerializedUserToken,
                    Nothing(),
                    ctx.ActorSystem(),
                    peerMetadata
                );
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

            void HandleSecurityTokenAuth(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx) {
                const auto& token = ev->Get()->Token;
                const bool isEnforceUserTokenRequirement = AppData(ctx)->EnforceUserTokenRequirement || AppData(ctx)->EnforceUserTokenCheckRequirement;
                if (ev->Get()->HasError()) {
                    if (isEnforceUserTokenRequirement) {
                        return ReplyWithYdbError(
                            ctx,
                            ev->Get()->Error.Retryable ? NYdb::EStatus::UNAVAILABLE : NYdb::EStatus::UNAUTHORIZED,
                            TString{ev->Get()->Error.Message});
                    }
                } else if (!token) {
                    if (isEnforceUserTokenRequirement) {
                        return ReplyWithYdbError(
                            ctx,
                            NYdb::EStatus::UNAUTHORIZED,
                            "Access denied");
                    }
                } else {
                    HttpContext.SerializedUserToken = token->GetSerializedToken();
                    UserSid_ = token->GetUserSID();
                }
                SendGrpcRequestNoDriver(ctx);
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
                const auto [errorName, httpCode] = MapToException(status, Method, issueCode);

                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                             AddCommonLabels({
                                 {"code", TStringBuilder() << (int)httpCode},
                                 {"name", "api.sqs.response.count"},
                             })});

                ReplyToHttpContext({
                    .HttpCode = static_cast<ui32>(httpCode),
                    .ContentType = HttpContext.ContentType,
                    .Message = errorName,
                    .Body = NSQS::Serialize(HttpContext.ContentType, {
                        .StatusCode = errorName,
                        .ErrorText = errorText,
                    })
                }, errorText.size(), errorText);

                if (AuthActor) {
                    ctx.Send(AuthActor, new TEvents::TEvPoisonPill());
                }

                TBase::Die(ctx);
            }

            void ReplyWithMessageQueueError(
                    const TActorContext& ctx,
                    ui32 httpStatusCode,
                    const TString& ymqStatusCode,
                    const TString& errorText) {

                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                             AddCommonLabels({
                                 {"code", ToString(httpStatusCode)},
                                 {"name", "api.sqs.response.count"},
                             })});

                ReplyToHttpContext({
                    .HttpCode = httpStatusCode,
                    .ContentType = HttpContext.ContentType,
                    .Message = ymqStatusCode,
                    .Body = NSQS::Serialize(HttpContext.ContentType, {
                        .StatusCode = ymqStatusCode,
                        .ErrorText = errorText,
                    })
                }, errorText.size(), errorText);

                if (AuthActor) {
                    ctx.Send(AuthActor, new TEvents::TEvPoisonPill());
                }

                TBase::Die(ctx);
            }

            void ReplyToHttpContext(THttpResponseData&& data, size_t messageSize, TStringBuf errorText = "") {
                const TActorContext& ctx = TlsActivationContext->AsActorContext();

                ReportLatencyCounters(ctx);
                ReportResponseSizeCounters(TStringBuilder() << data.HttpCode, messageSize, ctx);
                LogHttpRequestResponse(ctx, data.HttpCode, errorText);

                HttpContext.DoReply(std::move(data));
            }

            void LogHttpRequestResponse(const TActorContext& ctx, int httpCode, TStringBuf errorText) {
                const bool isServerError = IsServerError(httpCode);
                auto priority = isServerError ? NActors::NLog::PRI_WARN : NActors::NLog::PRI_INFO;
                LOG_LOG_S_SAMPLED_BY(ctx, priority, NKikimrServices::SQS,
                                     NSqsTopic::SampleIdFromRequestId(HttpContext.RequestId),
                                     "Request [" << HttpContext.RequestId << "] " << LogHttpRequestResponseCommonInfoString(HttpContext, StartTime, "SqsTopic", TopicPath, Method, UserSid_, httpCode, errorText));
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
                    FillOutputCustomMetrics<TProtoResult>(
                        *(dynamic_cast<TProtoResult*>(ev->Get()->Message.Get())),
                        HttpContext,
                        ctx
                    );
                    ctx.Send(MakeMetricsServiceID(),
                             new TEvServerlessProxy::TEvCounter{
                                 1, true, true,
                                 AddCommonLabels({
                                     {"code", "200"},
                                     {"name", "api.sqs.response.count"}})});
                    ReplyToHttpContext({
                        .HttpCode = 200,
                        .ContentType = HttpContext.ContentType,
                        .Message = "",
                        .Body = NSQS::Serialize(HttpContext.ContentType, *ev->Get()->Message)
                    }, ev->Get()->Message->ByteSizeLong());
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
                                NKikimr::NSQS::NErrors::INTERNAL_FAILURE.ErrorCode,
                                NKikimr::NSQS::NErrors::INTERNAL_FAILURE.HttpStatusCode)
                            : NKikimr::NSQS::TErrorClass::GetErrorAndCode(issues.begin()->GetCode());

                        LOG_SP_DEBUG_S(
                            ctx,
                            NKikimrServices::HTTP_PROXY,
                            "Not retrying GRPC response."
                                << " Code: " << errorCode
                                << ", Error: " << error);
                        return ReplyWithMessageQueueError(
                            ctx,
                            errorCode,
                            error,
                            TString{!issues.Empty() ? issues.begin()->GetMessage() : NKikimr::NSQS::NErrors::INTERNAL_FAILURE.ErrorCode}
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
                    NSQS::Deserialize<TProtoRequest>(HttpContext.ContentType, Request, HttpContext.Request->Body);
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
                              "stream '" << MaybeGetQueueUrl<TProtoRequest>(Request) << "'");

                ReportInputCounters(ctx);
                if (!HttpContext.SecurityToken.empty()) {
                    ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
                        .Ticket = HttpContext.SecurityToken,
                        .Database = HttpContext.DatabasePath,
                        .PeerName = HttpContext.SourceAddress,
                    }));
                } else if (!HttpContext.IamToken.empty() || Signature) {
                    AuthActor = ctx.Register(AppData(ctx)->DataStreamsAuthFactory->CreateAuthActor(
                        ctx.SelfID, HttpContext, std::move(Signature)));
                } else {
                    if (AppData(ctx)->EnforceUserTokenRequirement || AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                        return ReplyWithMessageQueueError(
                            ctx,
                            NKikimr::NSQS::NErrors::INCOMPLETE_SIGNATURE.HttpStatusCode,
                            NKikimr::NSQS::NErrors::INCOMPLETE_SIGNATURE.ErrorCode,
                            NKikimr::NSQS::NErrors::INCOMPLETE_SIGNATURE.DefaultMessage);
                    }
                    SendGrpcRequestNoDriver(ctx);
                }

                ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

                TBase::Become(&TSqsTopicHttpRequestActor::StateWork);
            }

        private:
            TInstant StartTime;
            TProtoRequest Request;
            TDuration RequestTimeout = TDuration::Seconds(60);
            THttpRequestContext HttpContext;
            THolder<NKikimr::NSQS::TAwsRequestSignV4> Signature;
            NThreading::TFuture<TProtoResponse> RpcFuture;
            TProtoCall ProtoCall;
            TString Method;
            TString TopicPath;
            TString ConsumerName;
            TRetryCounter RetryCounter;

            TActorId AuthActor;
            bool InputCountersReported = false;
            TString UserSid_;
        };
    };

    class TController : public TBaseHttpController {
        public:
            TController() {
                #define DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN(name) Name2Processor[#name] =          \
                    std::make_unique<TSqsTopicHttpRequestProcessor<                                      \
                            Ydb::SqsTopic::V1::SqsTopicService,                                          \
                            Ydb::Ymq::V1::name##Request,                                                 \
                            Ydb::Ymq::V1::name##Response,                                                \
                            Ydb::Ymq::V1::name##Result,                                                  \
                            decltype(&Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name),    \
                            NKikimr::NGRpcService::TEvSqsTopic##name##Request                            \
                        >>(#name, &Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name)        \

                DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN(GetQueueUrl);
                DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN(ListQueues);

                #undef DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_UNKNOWN

                #define DECLARE_SQS_TOPIC_PROCESSOR_QUEUE_KNOWN(name) Name2Processor[#name] =               \
                    std::make_unique<TSqsTopicHttpRequestProcessor<                                         \
                            Ydb::SqsTopic::V1::SqsTopicService,                                             \
                            Ydb::Ymq::V1::name##Request,                                                    \
                            Ydb::Ymq::V1::name##Response,                                                   \
                            Ydb::Ymq::V1::name##Result,                                                     \
                            decltype(&Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name),       \
                            NKikimr::NGRpcService::TEvSqsTopic##name##Request                               \
                        >>(#name, &Ydb::SqsTopic::V1::SqsTopicService::Stub::AsyncSqsTopic##name)           \

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

            THttpResponseData MakeError(MimeTypes contentType, NYdb::EStatus Status, const TStringBuf message, size_t issueCode) const override {
                const auto [errorName, httpCode] = MapToException(Status, "", issueCode);
                return {
                    .HttpCode = static_cast<ui32>(httpCode),
                    .ContentType = contentType,
                    .Message = errorName,
                    .Body = NSQS::Serialize(contentType, NSQS::TErrorResponse{
                        .StatusCode = errorName,
                        .ErrorText = TString(message),
                    })
                };
            }

            bool IsEnabled(const NKikimrConfig::THttpProxyConfig& config) const override {
                return config.GetSqsTopicEnabled();
            }

            bool IsPossible(const TStringBuf apiVersion, const NKikimrConfig::TServerlessProxyConfig&) const override {
                return apiVersion == "AmazonSQS";
            }
        };

        TController ControllerInstance;

    } // namespace

    const IHttpController* GetSqsHttpController() {
        return &ControllerInstance;
    }

} // namespace NKikimr::NHttpProxy

