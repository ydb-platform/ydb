#include "dq_solomon_read_actor.h"

#include <ydb/library/yql/providers/solomon/scheme/yql_solomon_scheme.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/actors/http_sender_actor.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/url_builder.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_reader.h>


#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/system/compiler.h>

#define SINK_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

namespace {

enum ESystemColumn{
    SC_KIND = 0,
    SC_LABELS,
    SC_VALUE,
    SC_TYPE,
    SC_TS
};

struct TDqSolomonReadParams {
    NSo::NProto::TDqSolomonSource Source;
};

TString GetReadSolomonUrl(const TString& endpoint, bool useSsl, const TString& project, const ::NYql::NSo::NProto::ESolomonClusterType& type) {
    TUrlBuilder builder((useSsl ? "https://" : "http://") + endpoint);

    switch (type) {
        case NSo::NProto::ESolomonClusterType::CT_SOLOMON: {
            builder.AddPathComponent("api");
            builder.AddPathComponent("v2");
            builder.AddPathComponent("projects");
            builder.AddPathComponent(project);
            builder.AddPathComponent("sensors");
            builder.AddPathComponent("data");
            break;
        }
        case NSo::NProto::ESolomonClusterType::CT_MONITORING: {
            [[fallthrough]];
        }
        default:
            Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(type));
    }

    return builder.Build();
}

auto RetryPolicy = NYql::NDq::THttpSenderRetryPolicy::GetExponentialBackoffPolicy(
    [](const NHttp::TEvHttpProxy::TEvHttpIncomingResponse* resp){
        if (!resp || !resp->Response) {
            // Connection wasn't established. Should retry.
            return ERetryErrorClass::ShortRetry;
        }

        if (resp->Response->Status == "401") {
            return ERetryErrorClass::NoRetry;
        }

        return ERetryErrorClass::ShortRetry;
    });

} // namespace


class TDqSolomonReadActor : public NActors::TActorBootstrapped<TDqSolomonReadActor>, public IDqComputeActorAsyncInput {
public:
    static constexpr char ActorName[] = "DQ_SOLOMON_READ_ACTOR";

    TDqSolomonReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        const NActors::TActorId& computeActorId,
        const THolderFactory& holderFactory,
        NKikimr::NMiniKQL::TProgramBuilder& programBuilder,
        TDqSolomonReadParams&& readParams,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider
        )
        : InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , HolderFactory(holderFactory)
        , ProgramBuilder(programBuilder)
        , LogPrefix(TStringBuilder() << "TxId: " << TxId << ", Solomon source. ")
        , ReadParams(std::move(readParams))
        , Url(GetUrl())
        , CredentialsProvider(credentialsProvider)
    {
        Y_UNUSED(counters);
        SINK_LOG_D("Init");
        IngressStats.Level = statsLevel;

        auto stringType = ProgramBuilder.NewDataType(NYql::NUdf::TDataType<char*>::Id);
        DictType = ProgramBuilder.NewDictType(stringType, stringType, false);

        FillSystemColumnPositionindex();
    }

    void FillSystemColumnPositionindex() {
        std::vector<TString> names(ReadParams.Source.GetSystemColumns().begin(), ReadParams.Source.GetSystemColumns().end());
        names.insert(names.end(), ReadParams.Source.GetLabelNames().begin(), ReadParams.Source.GetLabelNames().end());
        std::sort(names.begin(), names.end());
        size_t index = 0;
        for (auto& n : names) {
            Index[n] = index++;
        }
    }

    void Bootstrap() {
        Become(&TDqSolomonReadActor::StateFunc);
        RequestMetrics();
    }
    
    STRICT_STFUNC(StateFunc,
        hFunc(TEvHttpBase::TEvSendResult, Handle);
    )

    i64 GetAsyncInputData(TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        Y_UNUSED(freeSpace);
        YQL_ENSURE(!buffer.IsWide(), "Wide stream is not supported");

        for (auto j : Batch) {
            auto& a = j["vector"].GetArray();
            for (auto& aa : a) {
                auto& series = aa["timeseries"];
                auto& kind = series["kind"];
                auto& type = series["type"];
                auto& labels = series["labels"];
                auto dictValueBuilder = HolderFactory.NewDict(DictType, 0);
                for (auto& [k, v]: labels.GetMap()) {
                    dictValueBuilder->Add(NKikimr::NMiniKQL::MakeString(k), NKikimr::NMiniKQL::MakeString(v.GetString()));
                }                
                auto dictValue = dictValueBuilder->Build();

                auto& timestamps = series["timestamps"].GetArray();
                auto& values = series["values"].GetArray();

                for (size_t i = 0; i < timestamps.size(); ++i){
                    NUdf::TUnboxedValue* items = nullptr;
                    auto value = HolderFactory.CreateDirectArrayHolder(ReadParams.Source.GetSystemColumns().size() + ReadParams.Source.GetLabelNames().size(), items);
                    if (auto it = Index.find(SOLOMON_SCHEME_KIND); it != Index.end()) {
                        items[it->second] = NKikimr::NMiniKQL::MakeString(kind.GetString());
                    }

                    if (auto it = Index.find(SOLOMON_SCHEME_LABELS); it != Index.end()) {
                        items[it->second] = dictValue;
                    }

                    if (auto it = Index.find(SOLOMON_SCHEME_VALUE); it != Index.end()) {
                        items[it->second] = NUdf::TUnboxedValuePod(values[i].GetDouble());
                    }

                    if (auto it = Index.find(SOLOMON_SCHEME_TYPE); it != Index.end()) {
                        items[it->second] = NKikimr::NMiniKQL::MakeString(type.GetString());
                    }

                    if (auto it = Index.find(SOLOMON_SCHEME_TS); it != Index.end()) {
                        // convert ms to sec
                        items[it->second] = NUdf::TUnboxedValuePod((ui64)timestamps[i].GetUInteger() / 1000);
                    }

                    for (const auto& c : ReadParams.Source.GetLabelNames()) {
                        auto& v = items[Index[c]];
                        auto it = labels.GetMap().find(c);
                        if (it != labels.GetMap().end()) {
                            v = NKikimr::NMiniKQL::MakeString(it->second.GetString());
                        } else {
                            // empty string
                            v = NKikimr::NMiniKQL::MakeString("");
                        }
                    }

                    buffer.push_back(value);
                }
            }
        }

        finished = !Batch.empty();
        Batch.clear();
        return 0;
    }

    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) final {}
    void LoadState(const TSourceState&) override { }
    void CommitState(const NDqProto::TCheckpoint&) override { }

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

private:
    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        if (HttpProxyId) {
            Send(HttpProxyId, new TEvents::TEvPoison());
        }

        TActor<TDqSolomonReadActor>::PassAway();
    }

private:
    TSourceState BuildState() { return {}; }

    void NotifyComputeActorWithData() {
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    TString GetUrl() const {
        return GetReadSolomonUrl(ReadParams.Source.GetEndpoint(),
                ReadParams.Source.GetUseSsl(),
                ReadParams.Source.GetProject(),
                ReadParams.Source.GetClusterType());
    }

    NHttp::THttpOutgoingRequestPtr BuildSolomonRequest(TStringBuf data) {
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Url);
        FillAuth(httpRequest);
        httpRequest->Set("x-client-id", "yql");
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/json");
        httpRequest->Set<&NHttp::THttpRequest::Body>(data);
        return httpRequest;
    }

    void FillAuth(NHttp::THttpOutgoingRequestPtr& httpRequest) {
        const TString authorizationHeader = "Authorization";
        const TString authToken = CredentialsProvider->GetAuthInfo();

        switch (ReadParams.Source.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                httpRequest->Set(authorizationHeader, "OAuth " + authToken);
                break;
            case NSo::NProto::ESolomonClusterType::CT_MONITORING:
                httpRequest->Set(authorizationHeader, "Bearer " + authToken);
                break;
            default:
                Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(ReadParams.Source.GetClusterType()));
        }
    }

    void RequestMetrics() {
        if (Y_UNLIKELY(!HttpProxyId)) {
            HttpProxyId = Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
        }
        const auto& source = ReadParams.Source;
        const auto& ds = source.GetDownsampling();

        NJsonWriter::TBuf w;
        w.BeginObject()
            .UnsafeWriteKey("from").WriteString(TInstant::Seconds(source.GetFrom()).ToString())
            .UnsafeWriteKey("to").WriteString(TInstant::Seconds(source.GetTo()).ToString())
            .UnsafeWriteKey("program").WriteString(source.GetProgram())
            .UnsafeWriteKey("downsampling")
                .BeginObject()
                    .UnsafeWriteKey("disabled").WriteBool(ds.GetDisabled())
                    .UnsafeWriteKey("aggregation").WriteString(ds.GetAggregation())
                    .UnsafeWriteKey("fill").WriteString(ds.GetFill())
                    .UnsafeWriteKey("gridMillis").WriteLongLong(ds.GetGridMs())
                .EndObject()
        .EndObject();

        // const TString body = R"({
        //     "downsampling": {
        //         "aggregation": "MAX",
        //         "disabled": true,
        //         "fill": "PREVIOUS",
        //         "gridMillis": 3600000,
        //         "ignoreMinStepMillis": true,
        //         "maxPoints": 500
        //     },
        //     "forceCluster": "",
        //     "from": "2023-12-08T14:40:39Z",
        //     "program": "{execpool=User,activity=YQ_STORAGE_PROXY,sensor=ActorsAliveByActivity}",
        //     "to": "2023-12-08T14:45:39Z"
        //     })";

        // const TStringBuf body = w.Str();
        //Cerr << "EX: Sending request: " << body << Endl;
        const NHttp::THttpOutgoingRequestPtr httpRequest = BuildSolomonRequest(w.Str());

        //const size_t bodySize = body.size();
        const TActorId httpSenderId = Register(CreateHttpSenderActor(SelfId(), HttpProxyId, RetryPolicy));
        ui8 cookie = 0;
        Send(httpSenderId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest), /*flags=*/0, cookie);
        //SINK_LOG_T("Sent read to solomon, body size: " << bodySize);
        //Cerr << "EX: RequestMetrics" << Endl;
    }

    void Handle(TEvHttpBase::TEvSendResult::TPtr& ev) {
        const auto* res = ev->Get();
        const TString& error = res->HttpIncomingResponse->Get()->GetError();

        if (!error.empty() || (res->HttpIncomingResponse->Get()->Response && res->HttpIncomingResponse->Get()->Response->Status != "200")) {
            TStringBuilder errorBuilder;
            errorBuilder << "Error while sending request to monitoring api: " << error;
            const auto& response = res->HttpIncomingResponse->Get()->Response;
            if (response) {
                errorBuilder << ", response: " << response->Body.Head(10000);
            }

            TIssues issues { TIssue(errorBuilder) };
            SINK_LOG_W("Got " << (res->IsTerminal ? "terminal " : "") << "error response[" << ev->Cookie << "] from solomon: " << issues.ToOneLineString());
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        HandleSuccessSolomonResponse(*res->HttpIncomingResponse->Get(), ev->Cookie);
    }

    void HandleSuccessSolomonResponse(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& response, ui64 cookie) {
        Y_UNUSED(cookie);
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(response.Response->Body, &json, /*throwOnError*/ true);
        } catch (const std::exception& e) {
            SINK_LOG_E("Invalid JSON reponse from monitoring: " << e.what() << ", body: " << response.Response->Body.Head(10000));
            TIssues issues { TIssue(TStringBuilder() << "Failed to parse response from monitoring: " << e.what()) };
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        Batch.push_back(json);
        NotifyComputeActorWithData();
    }

private:
    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const TTxId TxId;
    const NActors::TActorId ComputeActorId;
    const THolderFactory& HolderFactory;
    NKikimr::NMiniKQL::TProgramBuilder& ProgramBuilder;
    const TString LogPrefix;
    const TDqSolomonReadParams ReadParams;
    const TString Url;
    TActorId HttpProxyId;

    TString SourceId;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    TType* DictType = nullptr;
    std::vector<size_t> SystemColumnPositionIndex;
    THashMap<TString, size_t> Index;
    std::vector<NJson::TJsonValue> Batch;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqSolomonReadActor(
    NYql::NSo::NProto::TDqSolomonSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const NActors::TActorId& computeActorId,
    const THolderFactory& holderFactory,
    NKikimr::NMiniKQL::TProgramBuilder& programBuilder,
    const THashMap<TString, TString>& secureParams,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
{
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());

    TDqSolomonReadParams params {
        .Source = std::move(settings),
    };

    auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    auto credentialsProvider = credentialsProviderFactory->CreateProvider();

    TDqSolomonReadActor* actor = new TDqSolomonReadActor(
        inputIndex,
        statsLevel,
        txId,
        computeActorId,
        holderFactory,
        programBuilder,
        std::move(params),
        counters,
        credentialsProvider);
    return {actor, actor};
}

void RegisterDQSolomonReadActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.RegisterSource<NSo::NProto::TDqSolomonSource>("SolomonSource",
        [credentialsFactory](
            NYql::NSo::NProto::TDqSolomonSource&& settings,
            IDqAsyncIoFactory::TSourceArguments&& args)
        {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

            return CreateDqSolomonReadActor(
                std::move(settings),
                args.InputIndex,
                args.StatsLevel,
                args.TxId,
                args.ComputeActorId,
                args.HolderFactory,
                args.ProgramBuilder,
                args.SecureParams,
                counters,
                credentialsFactory);
        });
}

}
