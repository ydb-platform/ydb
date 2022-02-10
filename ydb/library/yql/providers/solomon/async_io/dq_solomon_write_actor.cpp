#include "dq_solomon_write_actor.h"
#include "metrics_encoder.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sinks.h> 
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h> 
#include <ydb/library/yql/dq/proto/dq_checkpoint.pb.h> 

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h> 
#include <ydb/library/yql/minikql/mkql_alloc.h> 
#include <ydb/library/yql/minikql/mkql_string_util.h> 
#include <ydb/library/yql/utils/yql_panic.h> 
#include <ydb/library/yql/utils/actors/http_sender_actor.h> 

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/json/easy_parse/json_easy_parser.h>


#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/system/compiler.h>

#include <algorithm>
#include <queue>
#include <variant>

#define SINK_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)
#define SINK_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)
#define SINK_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)
#define SINK_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)
#define SINK_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)
#define SINK_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)
#define SINK_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)
#define SINK_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, "Solomon sink. " << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

namespace {

const ui64 MaxMetricsPerRequest = 1000; // Max allowed count is 10000
const ui64 MaxRequestsInflight = 3;

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
};

} // namespace

class TDqSolomonWriteActor : public NActors::TActor<TDqSolomonWriteActor>, public IDqSinkActor {
public:
    static constexpr char ActorName[] = "DQ_SOLOMON_WRITE_ACTOR";

    TDqSolomonWriteActor(
        ui64 outputIndex,
        TDqSolomonWriteParams&& writeParams,
        NYql::NDq::IDqSinkActor::ICallbacks* callbacks,
        const NMonitoring::TDynamicCounterPtr& counters,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
        i64 freeSpace)
        : TActor<TDqSolomonWriteActor>(&TDqSolomonWriteActor::StateFunc)
        , OutputIndex(outputIndex)
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
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvHttpBase::TEvSendResult, Handle);
    )

public:
    void SendData(
        NKikimr::NMiniKQL::TUnboxedValueVector&& batch,
        i64,
        const TMaybe<NDqProto::TCheckpoint>& checkpoint,
        bool) override
    {
        SINK_LOG_D("Got " << batch.size() << " items to send");

        ui64 metricsCount = 0;
        for (const auto& item : batch) {
            if (metricsCount + WriteParams.Shard.GetScheme().GetSensors().size() > MaxMetricsPerRequest) {
                PushMetricsToBuffer(metricsCount);
            }

            metricsCount += UserMetricsEncoder.Append(item);
        }

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
    };

    void LoadState(const NDqProto::TSinkState&) override { }

    void CommitState(const NDqProto::TCheckpoint&) override { }

    i64 GetFreeSpace() const override {
        return FreeSpace;
    };

    ui64 GetOutputIndex() const override {
        return OutputIndex;
    };

private:
    struct TDqSolomonWriteActorMetrics {
        explicit TDqSolomonWriteActorMetrics(const NMonitoring::TDynamicCounterPtr& counters) {
            auto subgroup = counters->GetSubgroup("subsystem", "dq_solomon_write_actor");
            SendingBufferSize = subgroup->GetCounter("SendingBufferSize");
            WindowMinSendingBufferSize = subgroup->GetCounter("WindowMinSendingBufferSize");
            InflightRequests = subgroup->GetCounter("InflightRequests");
            WindowMinInflightRequests = subgroup->GetCounter("WindowMinInflightRequests");
            SentMetrics = subgroup->GetCounter("SentMetrics", true);
            СonfirmedMetrics = subgroup->GetCounter("СonfirmedMetrics", true);
        }

        NMonitoring::TDynamicCounters::TCounterPtr SendingBufferSize;
        NMonitoring::TDynamicCounters::TCounterPtr WindowMinSendingBufferSize;
        NMonitoring::TDynamicCounters::TCounterPtr InflightRequests;
        NMonitoring::TDynamicCounters::TCounterPtr WindowMinInflightRequests;
        NMonitoring::TDynamicCounters::TCounterPtr SentMetrics;
        NMonitoring::TDynamicCounters::TCounterPtr СonfirmedMetrics;

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
            SINK_LOG_W("Got error response from solomon " << issues.ToString());
            Callbacks->OnSinkError(OutputIndex, issues, res->IsTerminal);
            return;
        }

        HandleSuccessSolomonResponse(*res->HttpIncomingResponse->Get());

        while (TryToSendNextBatch()) {}
    }

    // IActor & IDqSinkActor
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
    NDqProto::TSinkState BuildState() { return {}; }

    TString GetUrl() const {
        TStringBuilder builder;
        builder << (WriteParams.Shard.GetUseSsl() ? "https://" : "http://");
        builder << WriteParams.Shard.GetEndpoint();

        switch (WriteParams.Shard.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON: {
                builder << "/api/v2/push";
                builder << "?project=" << WriteParams.Shard.GetProject();
                builder << "&cluster=" << WriteParams.Shard.GetCluster();
                builder << "&service=" << WriteParams.Shard.GetService();
                break;
            }
            case NSo::NProto::ESolomonClusterType::CT_MONITORING: {
                builder << "/monitoring/v2/data/write";
                builder << "?folderId=" << WriteParams.Shard.GetCluster();
                builder << "&service=" << WriteParams.Shard.GetService();
                break;
            }
            default:
                Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(WriteParams.Shard.GetClusterType()));
        }

        return builder;
    }

    void PushMetricsToBuffer(ui64& metricsCount) {
        try {
            auto data = UserMetricsEncoder.Encode();
            SINK_LOG_D("Push " << data.size() << " bytes of data to buffer");

            FreeSpace -= data.size();
            SendingBuffer.emplace(TMetricsToSend { std::move(data), metricsCount });
        } catch (const yexception& e) {
            TIssues issues { TIssue(TStringBuilder() << "Error while encoding solomon metrics: " << e.what()) };
            Callbacks->OnSinkError(OutputIndex, issues, true);
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
            SINK_LOG_D(skipReason);
            return false;
        }

        auto variant = SendingBuffer.front();
        SendingBuffer.pop();

        if (std::holds_alternative<TMetricsToSend>(variant)) {
            if (Y_UNLIKELY(!HttpProxyId)) {
                HttpProxyId = Register(NHttp::CreateHttpProxy(*NMonitoring::TMetricRegistry::Instance()));
            }

            const auto metricsToSend = std::get<TMetricsToSend>(variant);
            const NHttp::THttpOutgoingRequestPtr httpRequest = BuildSolomonRequest(metricsToSend.Data);

            const TActorId httpSenderId = Register(CreateHttpSenderActor(SelfId(), HttpProxyId));
            Send(httpSenderId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
            SINK_LOG_D("Sent " << metricsToSend.MetricsCount << " metrics with size of " << metricsToSend.Data.size() << " bytes to solomon");

            *Metrics.SentMetrics += metricsToSend.MetricsCount;
            InflightBuffer.emplace(httpRequest, TMetricsInflight { httpSenderId, metricsToSend.MetricsCount });
            return true;
        }

        if (std::holds_alternative<NDqProto::TCheckpoint>(variant)) {
            CheckpointInProgress = std::get<NDqProto::TCheckpoint>(std::move(variant));
            if (InflightBuffer.empty()) {
                DoCheckpoint();
                return true;
            }
            return false;
        }

        Y_ENSURE(false, "Bad type");
    }

    void HandleSuccessSolomonResponse(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& response) {
        SINK_LOG_D("Solomon response: " << response.Response->GetObfuscatedData());
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
            Callbacks->OnSinkError(OutputIndex, issues, true);
            return;
        }
        Y_VERIFY(res.size() == 2);

        auto ptr = InflightBuffer.find(response.Request);
        Y_VERIFY(ptr != InflightBuffer.end());

        const ui64 writtenMetricsCount = std::stoul(res[0]);
        *Metrics.СonfirmedMetrics += writtenMetricsCount;
        if (writtenMetricsCount != ptr->second.MetricsCount) {
            // TODO: YQ-340
            // TIssues issues { TIssue(TStringBuilder() << ToString(ptr->second.MetricsCount - writtenMetricsCount) << " metrics were not written: " << res[1]) };
            // Callbacks->OnSinkError(OutputIndex, issues, true);
            // return;
            SINK_LOG_W("Some metrics were not written. MetricsCount=" << ptr->second.MetricsCount << " writtenMetricsCount=" << writtenMetricsCount << " Solomon response: " << response.Response->GetObfuscatedData());
        }

        FreeSpace += ptr->first->Body.Size();
        if (ShouldNotifyNewFreeSpace) {
            Callbacks->ResumeExecution();
            ShouldNotifyNewFreeSpace = false;
        }
        InflightBuffer.erase(ptr);

        if (CheckpointInProgress && InflightBuffer.empty()) {
            DoCheckpoint();
        }
    }

    void DoCheckpoint() {
        Callbacks->OnSinkStateSaved(BuildState(), OutputIndex, *CheckpointInProgress);
        CheckpointInProgress = std::nullopt;
    }

private:
    const ui64 OutputIndex;
    const TDqSolomonWriteParams WriteParams;
    const TString Url;
    NYql::NDq::IDqSinkActor::ICallbacks* const Callbacks;
    TDqSolomonWriteActorMetrics Metrics;
    i64 FreeSpace = 0;
    TActorId HttpProxyId;

    TString SourceId;
    bool ShouldNotifyNewFreeSpace = false;
    std::optional<NDqProto::TCheckpoint> CheckpointInProgress = std::nullopt;
    std::queue<std::variant<TMetricsToSend, NDqProto::TCheckpoint>> SendingBuffer;
    THashMap<NHttp::THttpOutgoingRequestPtr, TMetricsInflight> InflightBuffer;

    TMetricsEncoder UserMetricsEncoder;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
};

std::pair<NYql::NDq::IDqSinkActor*, NActors::IActor*> CreateDqSolomonWriteActor(
    NYql::NSo::NProto::TDqSolomonShard&& settings,
    ui64 outputIndex,
    const THashMap<TString, TString>& secureParams,
    NYql::NDq::IDqSinkActor::ICallbacks* callbacks,
    const NMonitoring::TDynamicCounterPtr& counters,
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
        std::move(params),
        callbacks,
        counters,
        credentialsProvider,
        freeSpace);
    return {actor, actor};
}

void RegisterDQSolomonWriteActorFactory(TDqSinkFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.Register<NSo::NProto::TDqSolomonShard>("SolomonSink",
        [credentialsFactory](
            NYql::NSo::NProto::TDqSolomonShard&& settings,
            IDqSinkActorFactory::TArguments&& args)
        {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();

            return CreateDqSolomonWriteActor(
                std::move(settings),
                args.OutputIndex,
                args.SecureParams,
                args.Callback,
                counters,
                credentialsFactory);
        });
}

} // namespace NYql::NDq
