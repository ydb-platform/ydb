#include "http_req.h"
#include "json_proto_conversion.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/ymq/actor/auth_multi_factory.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/errors.h>
#include <ydb/services/ymq/grpc_service.h>
#include <ydb/services/ymq/rpc_params.h>
#include <ydb/services/ymq/utils.h>
#include <ydb/services/ymq/ymq_proxy.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NHttpProxy {

    namespace {

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
                        "Got succesfult GRPC response."
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
                                << ", Error: " << get<0>(errorAndCode)
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
                        << " UserSid: " << ev->Get()->Sid
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
                        << " Message: " << ev->Get()->Error->Message
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

    class TController : public IHttpController {
        public:
            TController() {
                #define DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(name) Name2Processor[#name] = MakeHolder<TYmqHttpRequestProcessor< \
                    Ydb::Ymq::V1::YmqService, \
                    Ydb::Ymq::V1::name##Request, \
                    Ydb::Ymq::V1::name##Response, \
                    Ydb::Ymq::V1::name##Result, \
                    decltype(&Ydb::Ymq::V1::YmqService::Stub::AsyncYmq##name), \
                    NKikimr::NGRpcService::TEvYmq##name##Request>> \
                            (#name, &Ydb::Ymq::V1::YmqService::Stub::AsyncYmq##name, [](Ydb::Ymq::V1::name##Request&){return "";});

                DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(GetQueueUrl);
                DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(CreateQueue);
                DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN(ListQueues);

                #undef DECLARE_YMQ_PROCESSOR_QUEUE_UNKNOWN

                #define DECLARE_YMQ_PROCESSOR_QUEUE_KNOWN(name) Name2Processor[#name] = MakeHolder<TYmqHttpRequestProcessor< \
                    Ydb::Ymq::V1::YmqService, \
                    Ydb::Ymq::V1::name##Request, \
                    Ydb::Ymq::V1::name##Response, \
                    Ydb::Ymq::V1::name##Result,\
                    decltype(&Ydb::Ymq::V1::YmqService::Stub::AsyncYmq##name), \
                    NKikimr::NGRpcService::TEvYmq##name##Request>> \
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
            }

            std::expected<IHttpRequestProcessor*, IHttpController::EError> GetProcessor(
                const TString& name,
                const THttpRequestContext& context
            ) const override {
                if (context.ApiVersion != "AmazonSQS") {
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

    std::shared_ptr<const IHttpController> CreateYmqHttpController(const NKikimrConfig::TServerlessProxyConfig& config) {
        if (config.GetHttpConfig().GetYmqEnabled()) {
            return std::make_shared<TController>();
        }
        return {};
    }

} // namespace NKikimr::NHttpProxy
