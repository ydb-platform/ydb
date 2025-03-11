#include "dq_solomon_read_actor.h"
#include "dq_solomon_actors_util.h"

#include <library/cpp/protobuf/util/pb_io.h>

#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/providers/solomon/actors/dq_solomon_metrics_queue.h>
#include <ydb/library/yql/providers/solomon/events/events.h>
#include <ydb/library/yql/providers/solomon/scheme/yql_solomon_scheme.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/actors/http_sender_actor.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/url_builder.h>
#include <yql/essentials/utils/yql_panic.h>

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

#define SOURCE_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG(prio, s) \
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
        ui64 maxInflightDataRequests,
        ui64 computeActorBatchSize,
        NActors::TActorId metricsQueueActor,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider
        )
        : InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , HolderFactory(holderFactory)
        , ProgramBuilder(programBuilder)
        , LogPrefix(TStringBuilder() << "TxId: " << TxId << ", TDqSolomonReadActor: ")
        , ReadParams(std::move(readParams))
        , MaxInflightDataRequests(maxInflightDataRequests)
        , ComputeActorBatchSize(computeActorBatchSize)
        , MetricsQueueActor(metricsQueueActor)
        , CredentialsProvider(credentialsProvider)
        , SolomonClient(NSo::ISolomonAccessorClient::Make(ReadParams.Source, CredentialsProvider))
    {
        Y_UNUSED(counters);
        SOURCE_LOG_D("Init");
        IngressStats.Level = statsLevel;

        UseMetricsQueue = ReadParams.Source.HasSelectors();

        auto stringType = ProgramBuilder.NewDataType(NYql::NUdf::TDataType<char*>::Id);
        DictType = ProgramBuilder.NewDictType(stringType, stringType, false);

        FillSystemColumnPositionIndex();
    }

    void FillSystemColumnPositionIndex() {
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
        if (UseMetricsQueue) {
            MetricsQueueEvents.Init(TxId, SelfId(), SelfId());
            MetricsQueueEvents.OnNewRecipientId(MetricsQueueActor);
            RequestMetrics();
        } else {
            RequestData();
        }
    }
    
    STRICT_STFUNC(StateFunc,
        hFunc(TEvSolomonProvider::TEvMetricsBatch, HandleMetricsBatch);
        hFunc(TEvSolomonProvider::TEvMetricsReadError, HandleMetricsReadError);
        hFunc(TEvSolomonProvider::TEvNewDataBatch, HandleNewDataBatch);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    )

    void HandleMetricsBatch(TEvSolomonProvider::TEvMetricsBatch::TPtr& metricsBatch) {
        if (!MetricsQueueEvents.OnEventReceived(metricsBatch)) {
            return;
        }

        YQL_ENSURE(IsWaitingMetricsQueueResponse);
        IsWaitingMetricsQueueResponse = false;
        auto& batch = metricsBatch->Get()->Record;
        IsMetricsQueueEmpty = batch.GetNoMoreMetrics();
        if (IsMetricsQueueEmpty && !IsConfirmedMetricsQueueFinish) {
            SOURCE_LOG_D("HandleMetricsBatch MetricsQueue empty, sending finish confirmation");
            RequestMetrics();
            IsConfirmedMetricsQueueFinish = true;
        }

        auto& metrics = batch.GetMetrics();

        SOURCE_LOG_D("HandleMetricsBatch batch of size " << metrics.size());
        Metrics.insert(Metrics.end(), metrics.begin(), metrics.end());
        ListedMetrics += metrics.size();

        while (TryRequestData()) {}

        if (LastMetricProcessed()) {
            NotifyComputeActorWithData();
        }
    }

    void HandleMetricsReadError(TEvSolomonProvider::TEvMetricsReadError::TPtr& metricsReadError) {
        if (!MetricsQueueEvents.OnEventReceived(metricsReadError)) {
            return;
        }

        IsMetricsQueueEmpty = true;
        if (!IsConfirmedMetricsQueueFinish) {
            SOURCE_LOG_D("HandleMetricsReadError sending finish confirmation to MetricsQueue");
            RequestMetrics();
            IsConfirmedMetricsQueueFinish = true;
        }

        TIssues issues { TIssue(metricsReadError->Get()->Record.GetIssues()) };
        SOURCE_LOG_W("Got " << "error response[" << metricsReadError->Cookie << "] from solomon: " << issues.ToOneLineString());
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        return;
    }

    void HandleNewDataBatch(TEvSolomonProvider::TEvNewDataBatch::TPtr& newDataBatch) {
        auto& batch = *newDataBatch->Get();
        InflightDataRequests--;

        if (!batch.Result.Success) {
            TIssues issues { TIssue(batch.Result.ErrorMsg) };
            SOURCE_LOG_W("Got " << "error response[" << newDataBatch->Cookie << "] from solomon: " << issues.ToOneLineString());
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        MetricsData.insert(MetricsData.end(), batch.Result.Result.begin(), batch.Result.Result.end());
        CompletedMetrics += batch.SelectorsCount;

        if (!Metrics.empty()) {
            while (TryRequestData()) {}
        } else if (MetricsData.size() >= ComputeActorBatchSize || LastMetricReceived()) {
            NotifyComputeActorWithData();
        }
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&) {
        SOURCE_LOG_D("Handle MetricsQueue retry");
        MetricsQueueEvents.Retry();
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        SOURCE_LOG_D("Handle MetricsQueue disconnected " << ev->Get()->NodeId);
        MetricsQueueEvents.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        SOURCE_LOG_D("Handle MetricsQueue connected " << ev->Get()->NodeId);
        MetricsQueueEvents.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        SOURCE_LOG_D("Handle MetricsQueue undelivered");
        if (MetricsQueueEvents.HandleUndelivered(ev) != NYql::NDq::TRetryEventsQueue::ESessionState::WrongSession) {
            TIssues issues{TIssue{TStringBuilder() << "MetricsQueue was lost"}};
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::UNAVAILABLE));
        }
    }

    i64 GetAsyncInputData(TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        Y_UNUSED(freeSpace);
        YQL_ENSURE(!buffer.IsWide(), "Wide stream is not supported");
        SOURCE_LOG_D("GetAsyncInputData sending " << MetricsData.size() << " metrics");

        for (const auto& data : MetricsData) {
            auto& labels = data.Labels;

            auto dictValueBuilder = HolderFactory.NewDict(DictType, 0);
            for (auto& [key, value] : labels) {
                dictValueBuilder->Add(NKikimr::NMiniKQL::MakeString(key), NKikimr::NMiniKQL::MakeString(value));
            }
            auto dictValue = dictValueBuilder->Build();

            auto& timestamps = data.Timestamps;
            auto& values = data.Values;
            auto& type = data.Type;

            for (size_t i = 0; i < timestamps.size(); ++i){
                NUdf::TUnboxedValue* items = nullptr;
                auto value = HolderFactory.CreateDirectArrayHolder(ReadParams.Source.GetSystemColumns().size() + ReadParams.Source.GetLabelNames().size(), items);

                if (auto it = Index.find(SOLOMON_SCHEME_LABELS); it != Index.end()) {
                    items[it->second] = dictValue;
                }

                if (auto it = Index.find(SOLOMON_SCHEME_VALUE); it != Index.end()) {
                    items[it->second] = NUdf::TUnboxedValuePod(values[i]);
                }

                if (auto it = Index.find(SOLOMON_SCHEME_TYPE); it != Index.end()) {
                    items[it->second] = NKikimr::NMiniKQL::MakeString(type);
                }

                if (auto it = Index.find(SOLOMON_SCHEME_TS); it != Index.end()) {
                    // convert ms to sec
                    items[it->second] = NUdf::TUnboxedValuePod((ui64)timestamps[i] / 1000);
                }

                for (const auto& c : ReadParams.Source.GetLabelNames()) {
                    auto& v = items[Index[c]];
                    auto it = labels.find(c);
                    if (it != labels.end()) {
                        v = NKikimr::NMiniKQL::MakeString(it->second);
                    } else {
                        // empty string
                        v = NKikimr::NMiniKQL::MakeString("");
                    }
                }

                buffer.push_back(value);
            }
        }

        finished = LastMetricProcessed();
        MetricsData.clear();
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
        SOURCE_LOG_I("PassAway, processed " << CompletedMetrics << " metrics.");
        TActor<TDqSolomonReadActor>::PassAway();
    }

private:
    TSourceState BuildState() { return {}; }

    void NotifyComputeActorWithData() const {
        SOURCE_LOG_D("NotifyComputeActorWithData");
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    bool LastMetricReceived() const {
        if (UseMetricsQueue) {
            return IsConfirmedMetricsQueueFinish && CompletedMetrics == ListedMetrics;
        } else {
            return CompletedMetrics == 1;
        }
    }

    bool LastMetricProcessed() const {
        if (UseMetricsQueue) {
            return IsConfirmedMetricsQueueFinish && CompletedMetrics == ListedMetrics;
        } else {
            return MetricsData.empty() && CompletedMetrics == 1;
        }
    }

    void TryRequestMetrics() {
        if (Metrics.size() < MetricsPerDataQuery * MaxInflightDataRequests && !IsMetricsQueueEmpty && !IsWaitingMetricsQueueResponse) {
            RequestMetrics();
        }
    }

    void RequestMetrics() {
        MetricsQueueEvents.Send(new TEvSolomonProvider::TEvGetNextBatch());
        IsWaitingMetricsQueueResponse = true;
    }

    bool TryRequestData() {
        TryRequestMetrics();
        if (Metrics.empty()) {
            return false;
        }

        if (InflightDataRequests >= MaxInflightDataRequests) {
            return false;
        }

        RequestData();
        return true;
    }

    void RequestData() {
        std::vector<TString> dataSelectors;
        if (UseMetricsQueue) {
            while (Metrics.size() > 0 && dataSelectors.size() < MetricsPerDataQuery) {
                dataSelectors.push_back(BuildSelectorsString(Metrics.back()));
                Metrics.pop_back();
            }
        } else {
            dataSelectors.push_back(ReadParams.Source.GetProgram());
        }

        auto getDataFuture = SolomonClient->GetData(dataSelectors);
        InflightDataRequests++;

        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        getDataFuture.Subscribe([actorSystem, selectorsCount = dataSelectors.size(), selfId = SelfId()](
            const NThreading::TFuture<NSo::ISolomonAccessorClient::TGetDataResult>& result) -> void
        {
            actorSystem->Send(selfId, new TEvSolomonProvider::TEvNewDataBatch(selectorsCount, result.GetValue()));
        });
    }

    TString BuildSelectorsString(const NSo::MetricQueue::TMetricLabels& metric) const {
        TStringBuilder result;
        bool first = true;

        result << "{";
        for (const auto& [key, value] : metric.GetLabels()) {
            if (!first) {
                result << ",";
            }
            first = false;
            result << key << "=\"" << value << "\"";
        }
        result << "}";

        return result;
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
    const ui64 MaxInflightDataRequests;
    const ui64 ComputeActorBatchSize;

    bool UseMetricsQueue;
    TRetryEventsQueue MetricsQueueEvents;
    NActors::TActorId MetricsQueueActor;
    bool IsWaitingMetricsQueueResponse = false;
    bool IsMetricsQueueEmpty = false;
    bool IsConfirmedMetricsQueueFinish = false;
    std::deque<NSo::MetricQueue::TMetricLabels> Metrics;
    std::deque<NSo::TTimeseries> MetricsData;
    size_t InflightDataRequests = 0;
    size_t ListedMetrics = 0;
    size_t CompletedMetrics = 0;
    const ui64 MetricsPerDataQuery = 15;

    TString SourceId;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    NSo::ISolomonAccessorClient::TPtr SolomonClient;
    TType* DictType = nullptr;
    std::vector<size_t> SystemColumnPositionIndex;
    THashMap<TString, size_t> Index;
};


} // namespace

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqSolomonReadActor(
    NYql::NSo::NProto::TDqSolomonSource&& source,
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
    const TString& tokenName = source.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());

    TDqSolomonReadParams params {
        .Source = std::move(source),
    };

    auto& settings = params.Source.settings();

    NActors::TActorId metricsQueueActor;
    if (auto it = settings.find("metricsQueueActor"); it != settings.end()) {
        NActorsProto::TActorId protoId;
        TMemoryInput inputStream(it->second);
        ParseFromTextFormat(inputStream, protoId);
        metricsQueueActor = ActorIdFromProto(protoId);
    }

    ui64 maxInflightDataRequests = 1;
    if (auto it = settings.find("maxInflightDataRequests"); it != settings.end()) {
        maxInflightDataRequests = FromString<ui64>(it->second);
    }

    ui64 computeActorBatchSize = 1;
    if (auto it = settings.find("computeActorBatchSize"); it != settings.end()) {
        computeActorBatchSize = FromString<ui64>(it->second);
    }

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
        maxInflightDataRequests,
        computeActorBatchSize,
        metricsQueueActor,
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
