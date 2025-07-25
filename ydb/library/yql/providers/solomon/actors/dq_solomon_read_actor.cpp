#include "dq_solomon_read_actor.h"
#include "dq_solomon_actors_util.h"

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/string/join.h>
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

class TDqSolomonReadActor : public NActors::TActorBootstrapped<TDqSolomonReadActor>, public IDqComputeActorAsyncInput {
private:
    struct TMetricTimeRange {
        NSo::TMetric Metric;
        TInstant From;
        TInstant To;
    };

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
        ui64 computeActorBatchSize,
        ui32 truePointsFindRange,
        ui64 metricsQueueConsumersCountDelta,
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
        , ComputeActorBatchSize(computeActorBatchSize)
        , TrueRangeFrom(TInstant::Seconds(ReadParams.Source.GetFrom()) - TDuration::Seconds(truePointsFindRange))
        , TrueRangeTo(TInstant::Seconds(ReadParams.Source.GetTo()) + TDuration::Seconds(truePointsFindRange))
        , MetricsQueueConsumersCountDelta(metricsQueueConsumersCountDelta)
        , MetricsQueueActor(metricsQueueActor)
        , CredentialsProvider(credentialsProvider)
        , SolomonClient(NSo::ISolomonAccessorClient::Make(ReadParams.Source, CredentialsProvider))
    {
        assert(MaxPointsPerOneRequest != 0);
        Y_UNUSED(counters);
        SOURCE_LOG_D("Init");
        IngressStats.Level = statsLevel;

        UseMetricsQueue = !ReadParams.Source.HasProgram();

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
        if (UseMetricsQueue) {
            Become(&TDqSolomonReadActor::LimitlessModeState);
            MetricsQueueEvents.Init(TxId, SelfId(), SelfId());
            MetricsQueueEvents.OnNewRecipientId(MetricsQueueActor);

            if (MetricsQueueConsumersCountDelta > 0) {
                MetricsQueueEvents.Send(new TEvSolomonProvider::TEvUpdateConsumersCount(MetricsQueueConsumersCountDelta));
            }

            RequestMetrics();
        } else {
            Become(&TDqSolomonReadActor::LimitedModeState);
            RequestData();
        }
    }
    
    STRICT_STFUNC(LimitlessModeState,
        hFunc(TEvSolomonProvider::TEvMetricsBatch, HandleMetricsBatch);
        hFunc(TEvSolomonProvider::TEvMetricsReadError, HandleMetricsReadError);
        hFunc(TEvSolomonProvider::TEvPointsCountBatch, HandlePointsCountBatch);
        hFunc(TEvSolomonProvider::TEvNewDataBatch, HandleNewDataBatch);
        hFunc(TEvSolomonProvider::TEvAck, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    )

    STRICT_STFUNC(LimitedModeState,
        hFunc(TEvSolomonProvider::TEvNewDataBatch, HandleNewDataBatchLimited);
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

        auto& listedMetrics = batch.GetMetrics();

        SOURCE_LOG_D("HandleMetricsBatch batch of size " << listedMetrics.size());
        for (const auto& metric : listedMetrics) {
            std::map<TString, TString> labels(metric.GetLabels().begin(), metric.GetLabels().end());
            ListedMetrics.emplace_back(std::move(labels), metric.GetType());
        }
        ListedMetricsCount += listedMetrics.size();

        while (TryRequestPointsCount()) {}

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
        SOURCE_LOG_W("Got " << "error list metrics response[" << metricsReadError->Cookie << "] from solomon: " << issues.ToOneLineString());
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        return;
    }

    void HandlePointsCountBatch(TEvSolomonProvider::TEvPointsCountBatch::TPtr& pointsCountBatch) {
        auto& batch = *pointsCountBatch->Get();

        if (batch.Response.Status != NSo::EStatus::STATUS_OK) {
            TIssues issues { TIssue(batch.Response.Error) };
            SOURCE_LOG_W("Got " << "error points count response[" << pointsCountBatch->Cookie << "] from solomon: " << issues.ToOneLineString());
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        auto& metric = batch.Metric;
        auto& pointsCount = batch.Response.Result.PointsCount;
        ParsePointsCount(metric, pointsCount);

        TryRequestData();
    }

    void HandleNewDataBatch(TEvSolomonProvider::TEvNewDataBatch::TPtr& newDataBatch) {
        auto& batch = *newDataBatch->Get();

        if (batch.Response.Status == NSo::EStatus::STATUS_FATAL_ERROR) {
            TIssues issues { TIssue(batch.Response.Error) };
            SOURCE_LOG_W("Got " << "error data response[" << newDataBatch->Cookie << "] from solomon: " << issues.ToOneLineString());
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }
        if (batch.Response.Status == NSo::EStatus::STATUS_RETRIABLE_ERROR) {
            MetricsWithTimeRange.emplace_back(batch.Metric, batch.From, batch.To);
            TryRequestData();
            return;
        }

        MetricsData.insert(MetricsData.end(), batch.Response.Result.Timeseries.begin(), batch.Response.Result.Timeseries.end());
        CompletedMetricsCount++;

        if (!MetricsWithTimeRange.empty()) {
            TryRequestData();
        } else if (MetricsData.size() >= ComputeActorBatchSize || LastMetricProcessed()) {
            NotifyComputeActorWithData();
        }
    }

    void Handle(TEvSolomonProvider::TEvAck::TPtr& ev) {
        MetricsQueueEvents.OnEventReceived(ev);
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

    void HandleNewDataBatchLimited(TEvSolomonProvider::TEvNewDataBatch::TPtr& newDataBatch) {
        auto& batch = *newDataBatch->Get();

        if (batch.Response.Status != NSo::EStatus::STATUS_OK) {
            TIssues issues { TIssue(batch.Response.Error) };
            SOURCE_LOG_W("Got " << "error data response[" << newDataBatch->Cookie << "] from solomon: " << issues.ToOneLineString());
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        MetricsData.insert(MetricsData.end(), batch.Response.Result.Timeseries.begin(), batch.Response.Result.Timeseries.end());
        CompletedMetricsCount++;

        NotifyComputeActorWithData();
    }

    i64 GetAsyncInputData(TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished, i64) final {
        YQL_ENSURE(!buffer.IsWide(), "Wide stream is not supported");
        SOURCE_LOG_D("GetAsyncInputData sending " << MetricsData.size() << " metrics, finished = " << LastMetricProcessed());

        TInstant from = TInstant::Seconds(ReadParams.Source.GetFrom());
        TInstant to = TInstant::Seconds(ReadParams.Source.GetTo());

        for (const auto& data : MetricsData) {
            auto& labels = data.Metric.Labels;

            auto dictValueBuilder = HolderFactory.NewDict(DictType, 0);
            for (auto& [key, value] : labels) {
                dictValueBuilder->Add(NKikimr::NMiniKQL::MakeString(key), NKikimr::NMiniKQL::MakeString(value));
            }
            auto dictValue = dictValueBuilder->Build();

            auto& timestamps = data.Timestamps;
            auto& values = data.Values;
            auto& type = data.Metric.Type;

            for (size_t i = 0; i < timestamps.size(); ++i){
                TInstant timestamp = TInstant::MilliSeconds(timestamps[i]);
                if (timestamp < from || timestamp > to) {
                    continue;
                }

                NUdf::TUnboxedValue* items = nullptr;
                auto value = HolderFactory.CreateDirectArrayHolder(ReadParams.Source.GetSystemColumns().size() + ReadParams.Source.GetLabelNames().size(), items);

                if (auto it = Index.find(SOLOMON_SCHEME_VALUE); it != Index.end()) {
                    items[it->second] = isnan(values[i]) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(values[i]).MakeOptional();
                }

                if (auto it = Index.find(SOLOMON_SCHEME_TYPE); it != Index.end()) {
                    items[it->second] = NKikimr::NMiniKQL::MakeString(type);
                }

                if (auto it = Index.find(SOLOMON_SCHEME_TS); it != Index.end()) {
                    // convert ms to sec
                    items[it->second] = NUdf::TUnboxedValuePod((ui64)timestamps[i] / 1000);
                }

                if (auto it = Index.find(SOLOMON_SCHEME_LABELS); it != Index.end()) {
                    items[it->second] = dictValue;
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
        SOURCE_LOG_I("PassAway, processed " << CompletedMetricsCount << " metrics.");
        if (UseMetricsQueue) {
            MetricsQueueEvents.Unsubscribe();
        }
        TActor<TDqSolomonReadActor>::PassAway();
    }

private:
    TSourceState BuildState() { return {}; }

    void NotifyComputeActorWithData() const {
        SOURCE_LOG_D("NotifyComputeActorWithData");
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    bool LastMetricProcessed() const {
        if (UseMetricsQueue) {
            return IsMetricsQueueEmpty && CompletedMetricsCount == ListedMetricsCount;
        }
        return CompletedMetricsCount == 1;
    }

    void TryRequestMetrics() {
        if (ListedMetrics.size() < 1000 && !IsMetricsQueueEmpty && !IsWaitingMetricsQueueResponse) {
            RequestMetrics();
        }
    }

    void RequestMetrics() {
        MetricsQueueEvents.Send(new TEvSolomonProvider::TEvGetNextBatch());
        IsWaitingMetricsQueueResponse = true;
    }

    bool TryRequestPointsCount() {
        TryRequestMetrics();
        if (ListedMetrics.empty()) {
            return false;
        }

        RequestPointsCount();
        return true;
    }

    void RequestPointsCount() {
        NSo::TMetric requestMetric = ListedMetrics.back();
        ListedMetrics.pop_back();

        auto getPointsCountFuture = SolomonClient->GetPointsCount(requestMetric.Labels, TrueRangeFrom, TrueRangeTo);

        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        getPointsCountFuture.Subscribe([actorSystem, metric = std::move(requestMetric), selfId = SelfId()](
            const NThreading::TFuture<NSo::TGetPointsCountResponse>& response) mutable -> void
        {
            actorSystem->Send(selfId, new TEvSolomonProvider::TEvPointsCountBatch(
                std::move(metric),
                response.GetValue())
            );
        });
    }

    void TryRequestData() {
        TryRequestPointsCount();
        while (!MetricsWithTimeRange.empty()) {
            RequestData();
            TryRequestPointsCount();
        }
    }

    void RequestData() {
        NThreading::TFuture<NSo::TGetDataResponse> getDataFuture;
        NSo::TMetric metric;
        TInstant from;
        TInstant to;

        if (UseMetricsQueue) {
            auto request = MetricsWithTimeRange.back();
            MetricsWithTimeRange.pop_back();

            metric = request.Metric;
            from = request.From;
            to = request.To;

            getDataFuture = SolomonClient->GetData(metric.Labels, from, to);
        } else {
            getDataFuture = SolomonClient->GetData(
                ReadParams.Source.GetProgram(),
                TInstant::Seconds(ReadParams.Source.GetFrom()),
                TInstant::Seconds(ReadParams.Source.GetTo())
            );
        }

        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        getDataFuture.Subscribe([actorSystem, metric, from, to, selfId = SelfId()](
            const NThreading::TFuture<NSo::TGetDataResponse>& response) -> void
        {
            actorSystem->Send(selfId, new TEvSolomonProvider::TEvNewDataBatch(
                metric,
                from,
                to,
                response.GetValue())
            );
        });
    }

    void ParsePointsCount(const NSo::TMetric& metric, ui64 pointsCount) {
        auto ranges = SplitTimeIntervalIntoRanges(pointsCount);

        if (ranges.empty()) {
            CompletedMetricsCount++;
            return;
        }

        for (const auto& [fromRange, toRange] : ranges) {
            MetricsWithTimeRange.emplace_back(metric, fromRange, toRange);
        }
    }

    std::vector<std::pair<TInstant, TInstant>> SplitTimeIntervalIntoRanges(ui64 pointsCount) const {
        TInstant from = TrueRangeFrom;
        TInstant to = TrueRangeTo;

        std::vector<std::pair<TInstant, TInstant>> result;
        if (pointsCount == 0) {
            return result;
        }

        result.reserve(pointsCount / MaxPointsPerOneRequest);
        auto rangeDuration = to - from;
        for (ui64 i = 0; i < pointsCount; i += MaxPointsPerOneRequest) {
            double start = i;
            double end = std::min(i + MaxPointsPerOneRequest, pointsCount);
            result.emplace_back(
                from + rangeDuration * start / pointsCount,
                from + rangeDuration * end / pointsCount
            );
        }

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
    const ui64 ComputeActorBatchSize;
    const TInstant TrueRangeFrom;
    const TInstant TrueRangeTo;
    const ui64 MetricsQueueConsumersCountDelta;

    bool UseMetricsQueue;
    TRetryEventsQueue MetricsQueueEvents;
    NActors::TActorId MetricsQueueActor;
    bool IsWaitingMetricsQueueResponse = false;
    bool IsMetricsQueueEmpty = false;
    bool IsConfirmedMetricsQueueFinish = false;

    std::deque<NSo::TMetric> ListedMetrics;
    std::deque<TMetricTimeRange> MetricsWithTimeRange;
    std::deque<NSo::TTimeseries> MetricsData;
    size_t ListedMetricsCount = 0;
    size_t CompletedMetricsCount = 0;
    const ui64 MaxPointsPerOneRequest = 10000;

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
    ui64 metricsQueueConsumersCountDelta,
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

    ui64 computeActorBatchSize = 1;
    if (auto it = settings.find("computeActorBatchSize"); it != settings.end()) {
        computeActorBatchSize = FromString<ui64>(it->second);
    }

    ui32 truePointsFindRange = 301;
    if (auto it = settings.find("truePointsFindRange"); it != settings.end()) {
        truePointsFindRange = FromString<ui32>(it->second);
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
        computeActorBatchSize,
        truePointsFindRange,
        metricsQueueConsumersCountDelta,
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

            ui64 metricsQueueConsumersCountDelta = 0;
            if (args.ReadRanges.size() > 1) {
                metricsQueueConsumersCountDelta = args.ReadRanges.size() - 1;
            }

            return CreateDqSolomonReadActor(
                std::move(settings),
                args.InputIndex,
                metricsQueueConsumersCountDelta,
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
