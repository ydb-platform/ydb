#include "dq_solomon_write_actor.h"
#include "metrics_encoder.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
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
#include <library/cpp/json/easy_parse/json_easy_parser.h>


#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/system/compiler.h>

#include <algorithm>
#include <queue>
#include <variant>

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

const ui64 MaxMetricsPerRequest = 1000; // Max allowed count is 10000
const ui64 MaxRequestsInflight = 3;

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

struct TDqSolomonWriteParams {
    NSo::NProto::TDqSolomonShard Shard;
};

struct TMetricsToSend {
    TString Data;
    ui64 MetricsCount = 0;
};

struct TMetricsInflight {
    TActorId HttpSenderId;
    ui64 MetricsCount = 0;
    ui64 BodySize = 0;
};

} // namespace

TString GetSolomonUrl(const TString& endpoint, bool useSsl, const TString& project, const TString& cluster, const TString& service, const ::NYql::NSo::NProto::ESolomonClusterType& type) {
    TUrlBuilder builder((useSsl ? "https://" : "http://") + endpoint);

    switch (type) {
        case NSo::NProto::ESolomonClusterType::CT_SOLOMON: {
            builder.AddPathComponent("api");
            builder.AddPathComponent("v2");
            builder.AddPathComponent("push");
            builder.AddUrlParam("project", project);
            builder.AddUrlParam("cluster", cluster);
            builder.AddUrlParam("service", service);
            break;
        }
        case NSo::NProto::ESolomonClusterType::CT_MONITORING: {
            builder.AddPathComponent("monitoring/v2/data/write");
            builder.AddUrlParam("folderId", cluster);
            builder.AddUrlParam("service", service);
            break;
        }
        default:
            Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(type));
    }

    return builder.Build();
}

class TDqSolomonWriteActor : public NActors::TActor<TDqSolomonWriteActor>, public IDqComputeActorAsyncOutput {
public:
    static constexpr char ActorName[] = "DQ_SOLOMON_WRITE_ACTOR";

    TDqSolomonWriteActor(
        ui64 outputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        TDqSolomonWriteParams&& writeParams,
        NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* callbacks,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
        i64 freeSpace)
        : TActor<TDqSolomonWriteActor>(&TDqSolomonWriteActor::StateFunc)
        , OutputIndex(outputIndex)
        , TxId(txId)
        , LogPrefix(TStringBuilder() << "TxId: " << TxId << ", Solomon sink. ")
        , WriteParams(std::move(writeParams))
        , Url(GetUrl())
        , Callbacks(callbacks)
        , Metrics(counters)
        , FreeSpace(freeSpace)
        , UserMetricsEncoder(
            WriteParams.Shard.GetScheme(),
            WriteParams.Shard.GetClusterType() == NSo::NProto::ESolomonClusterType::CT_MONITORING)
        , CredentialsProvider(credentialsProvider)
    {
        SINK_LOG_D("Init");
        EgressStats.Level = statsLevel;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvHttpBase::TEvSendResult, Handle);
    )

public:
    void SendData(
        NKikimr::NMiniKQL::TUnboxedValueBatch&& batch,
        i64,
        const TMaybe<NDqProto::TCheckpoint>& checkpoint,
        bool finished) override
    {
        YQL_ENSURE(!batch.IsWide(), "Wide streams are not supported");
        SINK_LOG_T("Got " << batch.RowCount() << " items to send. Checkpoint: " << checkpoint.Defined()
                   << ". Send queue: " << SendingBuffer.size() << ". Inflight: " << InflightBuffer.size()
                   << ". Checkpoint in progress: " << CheckpointInProgress.has_value());

        if (finished) {
            Finished = true;
        }

        ui64 metricsCount = 0;
        batch.ForEachRow([&](const auto& value) {
            if (metricsCount + WriteParams.Shard.GetScheme().GetSensors().size() > MaxMetricsPerRequest) {
                PushMetricsToBuffer(metricsCount);
            }

            metricsCount += UserMetricsEncoder.Append(value);
            EgressStats.Rows++;
        });

        if (metricsCount != 0) {
            PushMetricsToBuffer(metricsCount);
        }

        if (checkpoint) {
            SendingBuffer.emplace(*checkpoint);
        }

        while (TryToSendNextBatch()) {}

        if (FreeSpace <= 0) {
            ShouldNotifyNewFreeSpace = true;
        }

        CheckFinished();
    };

    void LoadState(const TSinkState&) override { }

    void CommitState(const NDqProto::TCheckpoint&) override { }

    i64 GetFreeSpace() const override {
        return FreeSpace;
    };

    ui64 GetOutputIndex() const override {
        return OutputIndex;
    };

    const TDqAsyncStats& GetEgressStats() const override {
        return EgressStats;
    }

private:
    struct TDqSolomonWriteActorMetrics {
        explicit TDqSolomonWriteActorMetrics(const ::NMonitoring::TDynamicCounterPtr& counters) {
            auto subgroup = counters->GetSubgroup("subsystem", "dq_solomon_write_actor");
            SendingBufferSize = subgroup->GetCounter("SendingBufferSize");
            WindowMinSendingBufferSize = subgroup->GetCounter("WindowMinSendingBufferSize");
            InflightRequests = subgroup->GetCounter("InflightRequests");
            WindowMinInflightRequests = subgroup->GetCounter("WindowMinInflightRequests");
            SentMetrics = subgroup->GetCounter("SentMetrics", true);
            СonfirmedMetrics = subgroup->GetCounter("СonfirmedMetrics", true);
        }

        ::NMonitoring::TDynamicCounters::TCounterPtr SendingBufferSize;
        ::NMonitoring::TDynamicCounters::TCounterPtr WindowMinSendingBufferSize;
        ::NMonitoring::TDynamicCounters::TCounterPtr InflightRequests;
        ::NMonitoring::TDynamicCounters::TCounterPtr WindowMinInflightRequests;
        ::NMonitoring::TDynamicCounters::TCounterPtr SentMetrics;
        ::NMonitoring::TDynamicCounters::TCounterPtr СonfirmedMetrics;

    public:
        void ReportSendingBufferSize(size_t size) {
            SendingBufferSizeHistoryMin += size;
            SendingBufferSizeHistory.push(size);

            if (SendingBufferSizeHistory.size() > WindowSize) {
                SendingBufferSizeHistoryMin -= SendingBufferSizeHistory.front();
                SendingBufferSizeHistory.pop();
            }

            *SendingBufferSize = size;
            *WindowMinSendingBufferSize = SendingBufferSizeHistoryMin / SendingBufferSizeHistory.size();
        }

        void ReportInflightRequestsCount(size_t count) {
            InflightRequestsHistoryMin += count;
            InflightRequestsHistory.push(count);

            if (InflightRequestsHistory.size() > WindowSize) {
                InflightRequestsHistoryMin -= InflightRequestsHistory.front();
                InflightRequestsHistory.pop();
            }

            *InflightRequests = count;
            *WindowMinInflightRequests = InflightRequestsHistoryMin / InflightRequestsHistory.size();
        }

    private:
        constexpr static size_t WindowSize = 30;

        std::queue<size_t> SendingBufferSizeHistory;
        size_t SendingBufferSizeHistoryMin = 0;

        std::queue<size_t> InflightRequestsHistory;
        size_t InflightRequestsHistoryMin = 0;
    };

    void Handle(TEvHttpBase::TEvSendResult::TPtr& ev) {
        const auto* res = ev->Get();
        const TString& error = res->HttpIncomingResponse->Get()->GetError();

        if (!error.empty()) {
            TStringBuilder errorBuilder;
            errorBuilder << "Error while sending request to monitoring api: " << error;
            const auto& response = res->HttpIncomingResponse->Get()->Response;
            if (response) {
                errorBuilder << " " << response->GetObfuscatedData();
            }

            TIssues issues { TIssue(errorBuilder) };
            SINK_LOG_W("Got " << (res->IsTerminal ? "terminal " : "") << "error response[" << ev->Cookie << "] from solomon: " << issues.ToOneLineString());

            Callbacks->OnAsyncOutputError(OutputIndex, issues, res->IsTerminal ? NYql::NDqProto::StatusIds::EXTERNAL_ERROR : NYql::NDqProto::StatusIds::UNSPECIFIED);
            return;
        }

        HandleSuccessSolomonResponse(*res->HttpIncomingResponse->Get(), ev->Cookie);

        while (TryToSendNextBatch()) {}
    }

    // IActor & IDqComputeActorAsyncOutput
    void PassAway() override { // Is called from Compute Actor
        for (const auto& [_, metricsInflight] : InflightBuffer) {
            Send(metricsInflight.HttpSenderId, new TEvents::TEvPoison());
        }

        if (HttpProxyId) {
            Send(HttpProxyId, new TEvents::TEvPoison());
        }

        TActor<TDqSolomonWriteActor>::PassAway();
    }

private:
    TSinkState BuildState() { return {}; }

    TString GetUrl() const {
        return GetSolomonUrl(WriteParams.Shard.GetEndpoint(),
                WriteParams.Shard.GetUseSsl(),
                WriteParams.Shard.GetProject(),
                WriteParams.Shard.GetCluster(),
                WriteParams.Shard.GetService(),
                WriteParams.Shard.GetClusterType());
    }

    void PushMetricsToBuffer(ui64& metricsCount) {
        try {
            auto data = UserMetricsEncoder.Encode();
            SINK_LOG_T("Push " << data.size() << " bytes of data to buffer");

            FreeSpace -= data.size();
            SendingBuffer.emplace(TMetricsToSend { std::move(data), metricsCount });
        } catch (const yexception& e) {
            TIssues issues { TIssue(TStringBuilder() << "Error while encoding solomon metrics: " << e.what()) };
            Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
        }

        metricsCount = 0;
    }

    NHttp::THttpOutgoingRequestPtr BuildSolomonRequest(const TString& data) {
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Url);
        FillAuth(httpRequest);
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/json");
        httpRequest->Set<&NHttp::THttpRequest::Body>(data);
        return httpRequest;
    }

    void FillAuth(NHttp::THttpOutgoingRequestPtr& httpRequest) {
        const TString authorizationHeader = "Authorization";
        const TString authToken = CredentialsProvider->GetAuthInfo();

        switch (WriteParams.Shard.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                httpRequest->Set(authorizationHeader, "OAuth " + authToken);
                break;
            case NSo::NProto::ESolomonClusterType::CT_MONITORING:
                httpRequest->Set(authorizationHeader, "Bearer " + authToken);
                break;
            default:
                Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(WriteParams.Shard.GetClusterType()));
        }
    }

    bool TryToSendNextBatch() {
        Metrics.ReportSendingBufferSize(SendingBuffer.size());
        Metrics.ReportInflightRequestsCount(InflightBuffer.size());

        if (CheckpointInProgress || SendingBuffer.empty() || InflightBuffer.size() >= MaxRequestsInflight) {
            TStringBuilder skipReason;
            skipReason << "Skip sending to solomon. Reason: ";
            if (CheckpointInProgress) {
                skipReason << "CheckpointInProgress ";
            }
            if (SendingBuffer.empty()) {
                skipReason << "Empty buffer ";
            }
            if (InflightBuffer.size() >= MaxRequestsInflight) {
                skipReason << "MaxRequestsInflight ";
            }
            SINK_LOG_T(skipReason);
            return false;
        }

        auto variant = SendingBuffer.front();
        SendingBuffer.pop();

        if (std::holds_alternative<TMetricsToSend>(variant)) {
            if (Y_UNLIKELY(!HttpProxyId)) {
                HttpProxyId = Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
            }

            const auto& metricsToSend = std::get<TMetricsToSend>(variant);
            const NHttp::THttpOutgoingRequestPtr httpRequest = BuildSolomonRequest(metricsToSend.Data);

            const size_t bodySize = metricsToSend.Data.size();
            const TActorId httpSenderId = Register(CreateHttpSenderActor(SelfId(), HttpProxyId, RetryPolicy));
            Send(httpSenderId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest), /*flags=*/0, Cookie);
            SINK_LOG_T("Sent " << metricsToSend.MetricsCount << " metrics with size of " << metricsToSend.Data.size() << " bytes to solomon");

            *Metrics.SentMetrics += metricsToSend.MetricsCount;
            InflightBuffer.emplace(Cookie++, TMetricsInflight { httpSenderId, metricsToSend.MetricsCount, bodySize });
            EgressStats.Bytes += bodySize;
            EgressStats.Chunks++;
            return true;
        }

        if (std::holds_alternative<NDqProto::TCheckpoint>(variant)) {
            SINK_LOG_D("Process checkpoint. Inflight before checkpoint: " << InflightBuffer.size());
            CheckpointInProgress = std::get<NDqProto::TCheckpoint>(std::move(variant));
            if (InflightBuffer.empty()) {
                DoCheckpoint();
                return true;
            }
            return false;
        }

        Y_ENSURE(false, "Bad type");
    }

    void HandleSuccessSolomonResponse(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& response, ui64 cookie) {
        SINK_LOG_T("Solomon response[" << cookie << "]: " << response.Response->GetObfuscatedData());
        NJson::TJsonParser parser;
        switch (WriteParams.Shard.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                parser.AddField("sensorsProcessed", true);
                break;
            case NSo::NProto::ESolomonClusterType::CT_MONITORING:
                parser.AddField("writtenMetricsCount", true);
                break;
            default:
                Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(WriteParams.Shard.GetClusterType()));
        }
        parser.AddField("errorMessage", false);

        TVector<TString> res;
        if (!parser.Parse(TString(response.Response->Body), &res)) {
            TIssues issues { TIssue(TStringBuilder() << "Invalid monitoring response: " << response.Response->GetObfuscatedData()) };
            SINK_LOG_E("Failed to parse response[" << cookie << "] from solomon: " << issues.ToOneLineString());
            Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            return;
        }
        Y_ABORT_UNLESS(res.size() == 2);

        auto ptr = InflightBuffer.find(cookie);
        if (ptr == InflightBuffer.end()) {
            SINK_LOG_E("Solomon response[" << cookie << "] was not found in inflight");
            TIssues issues { TIssue(TStringBuilder() << "Internal error in monitoring writer") };
            Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            return;
        }

        const ui64 writtenMetricsCount = std::stoul(res[0]);
        *Metrics.СonfirmedMetrics += writtenMetricsCount;
        if (writtenMetricsCount != ptr->second.MetricsCount) {
            // TODO: YQ-340
            // TIssues issues { TIssue(TStringBuilder() << ToString(ptr->second.MetricsCount - writtenMetricsCount) << " metrics were not written: " << res[1]) };
            // Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            // return;
            SINK_LOG_W("Some metrics were not written. MetricsCount=" << ptr->second.MetricsCount << " writtenMetricsCount=" << writtenMetricsCount << " Solomon response: " << response.Response->GetObfuscatedData());
        }

        FreeSpace += ptr->second.BodySize;
        if (ShouldNotifyNewFreeSpace && FreeSpace > 0) {
            Callbacks->ResumeExecution();
            ShouldNotifyNewFreeSpace = false;
        }
        InflightBuffer.erase(ptr);

        if (CheckpointInProgress && InflightBuffer.empty()) {
            DoCheckpoint();
        }

        CheckFinished();
    }

    void DoCheckpoint() {
        Callbacks->OnAsyncOutputStateSaved(BuildState(), OutputIndex, *CheckpointInProgress);
        CheckpointInProgress = std::nullopt;
    }

    void CheckFinished() {
        if (Finished && InflightBuffer.empty() && SendingBuffer.empty()) {
            Callbacks->OnAsyncOutputFinished(OutputIndex);
        }
    }

private:
    const ui64 OutputIndex;
    TDqAsyncStats EgressStats;
    const TTxId TxId;
    const TString LogPrefix;
    const TDqSolomonWriteParams WriteParams;
    const TString Url;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* const Callbacks;
    TDqSolomonWriteActorMetrics Metrics;
    i64 FreeSpace = 0;
    TActorId HttpProxyId;
    bool Finished = false;

    TString SourceId;
    bool ShouldNotifyNewFreeSpace = false;
    std::optional<NDqProto::TCheckpoint> CheckpointInProgress = std::nullopt;
    std::queue<std::variant<TMetricsToSend, NDqProto::TCheckpoint>> SendingBuffer;
    THashMap<ui64, TMetricsInflight> InflightBuffer;

    TMetricsEncoder UserMetricsEncoder;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    ui64 Cookie = 0;
};

std::pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqSolomonWriteActor(
    NYql::NSo::NProto::TDqSolomonShard&& settings,
    ui64 outputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const THashMap<TString, TString>& secureParams,
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    i64 freeSpace)
{
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());

    TDqSolomonWriteParams params {
        .Shard = std::move(settings),
    };

    auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    auto credentialsProvider = credentialsProviderFactory->CreateProvider();

    TDqSolomonWriteActor* actor = new TDqSolomonWriteActor(
        outputIndex,
        statsLevel,
        txId,
        std::move(params),
        callbacks,
        counters,
        credentialsProvider,
        freeSpace);
    return {actor, actor};
}

void RegisterDQSolomonWriteActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.RegisterSink<NSo::NProto::TDqSolomonShard>("SolomonSink",
        [credentialsFactory](
            NYql::NSo::NProto::TDqSolomonShard&& settings,
            IDqAsyncIoFactory::TSinkArguments&& args)
        {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

            return CreateDqSolomonWriteActor(
                std::move(settings),
                args.OutputIndex,
                args.StatsLevel,
                args.TxId,
                args.SecureParams,
                args.Callback,
                counters,
                credentialsFactory);
        });
}

} // namespace NYql::NDq
