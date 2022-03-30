#include "cloud_function_transform.h"

#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/providers/common/codec/yql_json_codec.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/actors/core/log.h>

#define LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::YANDEX_CLOUD_FUNCTION, "SelfId: " << this->SelfId() << ", Transform: " << TransformName << ". " << s)
#define LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::YANDEX_CLOUD_FUNCTION, "SelfId: " << this->SelfId() << ", Transform: " << TransformName << ". " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::YANDEX_CLOUD_FUNCTION, "SelfId: " << this->SelfId() << ", Transform: " << TransformName << ". " << s)


namespace NYql::NDq {

using namespace NActors;
using namespace NCommon;
using namespace NKikimr;

namespace {

constexpr TStringBuf CLOUD_FUNCTION_BASE_URL = "https://functions.yandexcloud.net/";

constexpr size_t MAX_RETRY = 3;

ERetryErrorClass RetryByHttpCode(long httpResponseCode) {
    switch (httpResponseCode) {
        case 429:
            return ERetryErrorClass::LongRetry;
        case 500:
        case 503:
        case 504:
            return ERetryErrorClass::ShortRetry;
    }
    return ERetryErrorClass::NoRetry;
}

const auto RETRY_POLICY = IRetryPolicy<long>::GetExponentialBackoffPolicy(RetryByHttpCode,
                                                                          TDuration::MilliSeconds(100),
                                                                          TDuration::MilliSeconds(400),
                                                                          TDuration::Seconds(5),
                                                                          MAX_RETRY);

}

TCloudFunctionTransformActor::TCloudFunctionTransformActor(TActorId owner,
                                                           NDqProto::TDqTransform transform, IHTTPGateway::TPtr gateway,
                                                           NYdb::TCredentialsProviderPtr credentialsProvider,
                                                           IDqOutputChannel::TPtr transformInput,
                                                           IDqOutputConsumer::TPtr taskOutput,
                                                           const NKikimr::NMiniKQL::THolderFactory& holderFactory,
                                                           const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
                                                           NKikimr::NMiniKQL::TProgramBuilder& programBuilder)
: TActor(&TCloudFunctionTransformActor::StateFunc)
, Owner(owner)
, Transform(transform)
, Gateway(std::move(gateway))
, CredentialsProvider(std::move(credentialsProvider))
, TransformInput(std::move(transformInput))
, TaskOutput(std::move(taskOutput))
, TransformName(UrlEscapeRet(transform.GetFunctionName(), true))
, HolderFactory(holderFactory)
, TypeEnv(typeEnv)
, ProgramBuilder(programBuilder)
{
    TStringStream err;
    InputRowType = ParseTypeFromYson(TStringBuf{transform.GetInputType()}, ProgramBuilder, err);
    YQL_ENSURE(InputRowType, "Can't parse cloud function input type");

    NMiniKQL::TType* outputType = ParseTypeFromYson(TStringBuf{transform.GetOutputType()}, ProgramBuilder, err);
    YQL_ENSURE(outputType, "Can't parse cloud function output type");
    OutputRowsType = NKikimr::NMiniKQL::TListType::Create(outputType, TypeEnv);
}


STRICT_STFUNC(TCloudFunctionTransformActor::StateFunc,
    hFunc(TEvents::TEvPoison, HandlePoison);
    hFunc(TCFTransformEvent::TEvTransformSuccess, Handle);
    hFunc(TCFTransformEvent::TEvExecuteTransform, Handle);
    hFunc(TEvDq::TEvAbortExecution, Handle);
)

STATEFN(TCloudFunctionTransformActor::DeadState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvPoison, HandlePoison);
    }
}

void TCloudFunctionTransformActor::Handle(TEvDq::TEvAbortExecution::TPtr& ev) {
    auto& msg = ev->Get()->Record;
    TIssues issues = ev->Get()->GetIssues();
    if (msg.GetStatusCode() == Ydb::StatusIds::INTERNAL_ERROR) {
        InternalError(issues);
    } else if (msg.GetStatusCode() == Ydb::StatusIds::ABORTED) {
        Aborted(issues);
    } else {
        RuntimeError(issues);
    }
}

void TCloudFunctionTransformActor::Handle(TCFTransformEvent::TEvExecuteTransform::TPtr& ev) {
    Y_UNUSED(ev);

    auto guard = BindAllocator();

    TString payload;
    TStringOutput jsonStream{payload};
    NJson::TJsonWriter jsonWriter{&jsonStream, false};

    jsonWriter.OpenArray();

    bool hasPayload = false;
    while (TransformInput->HasData() && !TaskOutput->IsFull()) {
        NKikimr::NMiniKQL::TUnboxedValueVector rowsToTransform;
        if (TransformInput->PopAll(rowsToTransform)) {
            hasPayload = true;
            for (auto&& row : rowsToTransform) {
                try {
                    NJsonCodec::WriteValueToJson(jsonWriter, row, InputRowType, NJsonCodec::DefaultPolicy::getInstance().CloudFunction());
                } catch (const std::exception& ex) {
                    auto message = TStringBuilder()
                            << "Failed to convert data to input json for cloud function '"
                            << TransformName << "': " << ex.what();
                    RuntimeError(message);
                    return;
                }
            }
        }
    }
    jsonWriter.CloseArray();
    jsonWriter.Flush();

    TCFReqContext context;
    if (!TransformInput->HasData() && TransformInput->IsFinished()) {
        context.LastBatch = true;
    }

    if (hasPayload) {
        TransformInProgress = true;

        auto* actorSystem = TActivationContext::ActorSystem();
        const auto iamToken = CredentialsProvider->GetAuthInfo();
        auto headers = iamToken.empty()
                ? IHTTPGateway::THeaders()
                : IHTTPGateway::THeaders{TString("Authorization: ") + iamToken};
        Gateway->Download(CLOUD_FUNCTION_BASE_URL + TransformName, headers, 10,
                          std::bind(&TCloudFunctionTransformActor::OnInvokeFinished, actorSystem, SelfId(),
                                    context,
                                    std::placeholders::_1),
                                    payload, RETRY_POLICY);
    } else if (context.LastBatch) {
        CompleteTransform();
    }
}

void TCloudFunctionTransformActor::Handle(TCFTransformEvent::TEvTransformSuccess::TPtr& ev) {
    auto guard = BindAllocator();

    NKikimr::NUdf::TUnboxedValue transformedData;
    try {
        auto body = std::string_view(ev->Get()->Result);
        NJson::TJsonValue bodyJson;
        if (!NJson::ReadJsonTree(body, &bodyJson, false)) {
            YQL_ENSURE(false, "Invalid json");
        }
        transformedData = NJsonCodec::ReadJsonValue(bodyJson, OutputRowsType, HolderFactory);
    } catch (const std::exception& ex) {
        auto message = TStringBuilder()
                << "Failed to convert output json from cloud function '"
                << TransformName << "': " << ex.what();
        RuntimeError(message);
        return;
    }

    TransformInProgress = false;

    //if (!TaskOutput->IsFull()) {
        TaskOutput->Consume(std::move(transformedData));
        auto newDataEv = new NTransformActor::TEvTransformNewData();
        Send(Owner, newDataEv);
    //}

    if (ev->Get()->LastBatch) {
        CompleteTransform();
    }
}

void TCloudFunctionTransformActor::DoTransform() {
    if (TransformInProgress)
        return;

    auto executeEv = new TCFTransformEvent::TEvExecuteTransform();
    Send(SelfId(), executeEv);
}

void TCloudFunctionTransformActor::OnInvokeFinished(TActorSystem* actorSystem, TActorId selfId,
                                                    TCFReqContext reqContext,
                                                    IHTTPGateway::TResult&& result) {
    switch (result.index()) {
        case 0U: {
            auto response = std::get<IHTTPGateway::TContent>(std::move(result));
            switch (response.HttpResponseCode) {
                case 200: {
                    auto event = new TCFTransformEvent::TEvTransformSuccess(std::move(response), reqContext.LastBatch);
                    actorSystem->Send(new IEventHandle(selfId, TActorId(), event));
                    break;
                }
                case 403:
                case 404: {
                    auto transformName = reqContext.TransformName;
                    auto message = TStringBuilder() << "Got '" << response.HttpResponseCode << "' response code from cloud function '"
                            << transformName << "'";
                    auto ev = TEvDq::TEvAbortExecution::Unavailable(message);
                    actorSystem->Send(new IEventHandle(selfId, TActorId(), ev.Release()));
                    break;
                }
                case 429:
                case 500:
                case 503:
                case 504: {
                    auto transformName = reqContext.TransformName;
                    auto message = TStringBuilder() << "Got '" << response.HttpResponseCode << "' response code from cloud function '"
                            << transformName << "' and retry has been out";
                    auto ev = TEvDq::TEvAbortExecution::Aborted(message);
                    actorSystem->Send(new IEventHandle(selfId, TActorId(), ev.Release()));
                    break;
                }
                default:
                    auto transformName = reqContext.TransformName;
                    auto message = TStringBuilder() << "Got unexpected response code (" << response.HttpResponseCode
                            << ") from cloud function '" << transformName;
                    auto ev = TEvDq::TEvAbortExecution::Aborted(message);
                    actorSystem->Send(new IEventHandle(selfId, TActorId(), ev.Release()));
            }
            break;
        }
        case 1U:
            auto transformName = reqContext.TransformName;
            auto message = TStringBuilder() << "Internal error during call cloud function '" << transformName;
            auto ev = TEvDq::TEvAbortExecution::InternalError(message, std::get<TIssues>(result));
            actorSystem->Send(new IEventHandle(selfId, TActorId(), ev.Release()));
            break;
    }
}


void TCloudFunctionTransformActor::HandlePoison(NActors::TEvents::TEvPoison::TPtr&) {
    PassAway();
}

void TCloudFunctionTransformActor::RuntimeError(const TString& message, const TIssues& subIssues) {
    LOG_E(message);
    RuntimeError(TEvDq::TEvAbortExecution::Unavailable(message, subIssues)->GetIssues());
}

void TCloudFunctionTransformActor::RuntimeError(const TIssues& issues) {
    TransformInProgress = false;
    auto ev = MakeHolder<TEvDq::TEvAbortExecution>(Ydb::StatusIds::UNAVAILABLE, issues);
    Send(Owner, ev.Release());

    Become(&TCloudFunctionTransformActor::DeadState);
}

void TCloudFunctionTransformActor::InternalError(const TString& message, const TIssues& subIssues) {
    LOG_E(message);
    InternalError(TEvDq::TEvAbortExecution::InternalError(message, subIssues)->GetIssues());
}

void TCloudFunctionTransformActor::InternalError(const TIssues& issues) {
    TransformInProgress = false;
    auto ev = MakeHolder<TEvDq::TEvAbortExecution>(Ydb::StatusIds::INTERNAL_ERROR, issues);
    Send(Owner, ev.Release());

    Become(&TCloudFunctionTransformActor::DeadState);
}

void TCloudFunctionTransformActor::Aborted(const TString& message, const TIssues& subIssues) {
    LOG_E(message);
    Aborted(TEvDq::TEvAbortExecution::Aborted(message, subIssues)->GetIssues());
}

void TCloudFunctionTransformActor::Aborted(const TIssues& issues) {
    TransformInProgress = false;
    auto ev = MakeHolder<TEvDq::TEvAbortExecution>(Ydb::StatusIds::ABORTED, issues);
    Send(Owner, ev.Release());

    Become(&TCloudFunctionTransformActor::DeadState);
}


void TCloudFunctionTransformActor::CompleteTransform() {
    TransformInProgress = false;
    TaskOutput->Finish();

    auto completeEv = new NTransformActor::TEvTransformCompleted();
    Send(Owner, completeEv);
}

TGuard<NKikimr::NMiniKQL::TScopedAlloc> TCloudFunctionTransformActor::BindAllocator() {
    return TypeEnv.BindAllocator();
}

std::pair<IDqTransformActor*, NActors::IActor*> CreateCloudFunctionTransformActor(
        const NDqProto::TDqTransform& transform, IHTTPGateway::TPtr gateway,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        TDqTransformActorFactory::TArguments&& args) {

    std::shared_ptr<NYdb::ICredentialsProviderFactory> credProviderFactory;
    if (transform.HasConnectionName()) {
        const auto token = args.SecureParams.Value(transform.GetConnectionName(), TString{});
        credProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, true);
    } else {
        credProviderFactory = NYdb::CreateInsecureCredentialsProviderFactory();
    }

    const auto actor = new TCloudFunctionTransformActor(
            args.ComputeActorId, transform,
            gateway, credProviderFactory->CreateProvider(),
            args.TransformInput, args.TransformOutput,
            args.HolderFactory, args.TypeEnv,
            args.ProgramBuilder);
    return {actor, actor};
}

}