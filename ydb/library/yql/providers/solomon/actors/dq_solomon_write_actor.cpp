#include "dq_solomon_actors_util.h"
#include "dq_solomon_write_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/providers/solomon/common/constants.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/actors/http_sender_actor.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/json/easy_parse/json_easy_parser.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/system/compiler.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/url_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <queue>
#include <variant>

#define SINK_LOG_T(s) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_D(s) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_I(s) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_W(s) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_N(s) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_E(s) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_C(s) LOG_CRIT_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG(prio, s) LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

namespace {

constexpr ui64 MaxMetricsPerRequest = 1000;   // Max allowed count is 10000
const ui64 MaxRequestsInflight = 3;

static_assert(NSo::NConstants::MaxSensorsPerRow <= MaxMetricsPerRequest);

const auto RetryPolicy =
    NYql::NDq::THttpSenderRetryPolicy::GetExponentialBackoffPolicy([](const NHttp::TEvHttpProxy::TEvHttpIncomingResponse* resp) {
        if (!resp || !resp->Response) {
            // Connection wasn't established. Should retry.
            return ERetryErrorClass::ShortRetry;
        }

        if (resp->Response->Status == "401") {
            return ERetryErrorClass::NoRetry;
        }

        return ERetryErrorClass::ShortRetry;
    }, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(10), std::numeric_limits<size_t>::max(),
        TDuration::Seconds(60));

struct TDqSolomonWriteParams {
    NSo::NProto::TDqSolomonShard Shard;
};

struct TPendingBatch {
    TUnboxedValueBatch Rows;
    i64 DataSize = 0;
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

class TDqSolomonWriteActor: public NActors::TActor<TDqSolomonWriteActor>, public IDqComputeActorAsyncOutput {
public:
    static constexpr char ActorName[] = "DQ_SOLOMON_WRITE_ACTOR";

    TDqSolomonWriteActor(ui64 outputIndex, TCollectStatsLevel statsLevel, const TTxId& txId, ui64 taskId, TDqSolomonWriteParams&& writeParams,
        NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* callbacks, const ::NMonitoring::TDynamicCounterPtr& counters,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider, std::shared_ptr<TScopedAlloc> alloc, i64 freeSpace,
        bool enableStreamingQueriesCounters, bool enableCountersPerTask)
        : TActor<TDqSolomonWriteActor>(&TDqSolomonWriteActor::StateFunc)
        , OutputIndex(outputIndex)
        , TxId(txId)
        , LogPrefix(TStringBuilder() << "TxId: " << TxId << ", Solomon sink. ")
        , WriteParams(std::move(writeParams))
        , Url(GetUrl())
        , Callbacks(callbacks)
        , Metrics(counters, TxId, taskId, enableStreamingQueriesCounters, enableCountersPerTask)
        , FreeSpace(freeSpace)
        , Alloc(std::move(alloc))
        ,
        UserMetricsEncoder(WriteParams.Shard.GetScheme(), WriteParams.Shard.GetClusterType() == NSo::NProto::ESolomonClusterType::CT_MONITORING)
        , CredentialsProvider(credentialsProvider)
    {
        YQL_ENSURE(Alloc, "Solomon sink requires a MiniKQL allocator");
        SINK_LOG_D("Init");
        EgressStats.Level = statsLevel;
    }

    ~TDqSolomonWriteActor() override {
        ClearPendingBatches();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvHttpBase::TEvSendResult, Handle);
    )

public:
    void SendData(
        NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 dataSize, const TMaybe<NDqProto::TCheckpoint>& checkpoint, bool finished) override {
        YQL_ENSURE(!batch.IsWide(), "Wide streams are not supported");
        SINK_LOG_T("Got " << batch.RowCount() << " items to send. Checkpoint: " << checkpoint.Defined()
                          << ". Send queue: " << SendingBuffer.size() << ". Inflight: " << InflightBuffer.size()
                          << ". Checkpoint in progress: " << CheckpointInProgress.has_value());

        if (finished) {
            Finished = true;
        }

        if (!batch.empty()) {
            YQL_ENSURE(dataSize >= 0, "Negative input batch size");
            if (dataSize == 0) {
                // Direct callers may omit the upstream size estimate.
                dataSize = batch.RowCount() * (1 + WriteParams.Shard.GetScheme().LabelsSize() + WriteParams.Shard.GetScheme().SensorsSize()) *
                           sizeof(NUdf::TUnboxedValue);
                batch.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                    for (const auto& label : WriteParams.Shard.GetScheme().GetLabels()) {
                        const auto labelValue = value.GetElement(label.GetIndex());
                        dataSize += labelValue.AsStringRef().Size();
                    }
                });
            }

            FreeSpace -= dataSize;
            EgressStats.Rows += batch.RowCount();
            auto* pending = SendingBuffer.empty() ? nullptr : std::get_if<TPendingBatch>(&SendingBuffer.back());
            const auto sensorsPerRow = WriteParams.Shard.GetScheme().SensorsSize();
            if (pending && (pending->Rows.RowCount() + batch.RowCount()) * sensorsPerRow <= MaxMetricsPerRequest) {
                // Compact small inputs instead of retaining a separate MiniKQL page per SendData call.
                batch.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    pending->Rows.emplace_back(std::move(value));
                });
                pending->DataSize += dataSize;
            } else {
                SendingBuffer.emplace(TPendingBatch{ std::move(batch), dataSize });
            }
        }

        if (checkpoint) {
            SendingBuffer.emplace(*checkpoint);
        }

        SendPendingBatches();

        CheckFinished();
    };

    // Checkpointing is not supported for the Solomon sink: writes are not
    // idempotent and there is no way to roll back a partially-sent batch.
    // Silently ignore checkpoint calls so the actor can coexist with
    // checkpoint-enabled pipelines.
    void LoadState(const TSinkState&, const NDqProto::TCheckpoint&) override {
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        Callbacks->OnAsyncOutputStateCommitted(OutputIndex, checkpoint);
    }

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
        explicit TDqSolomonWriteActorMetrics(const ::NMonitoring::TDynamicCounterPtr& counters, const TTxId& txId, ui64 taskId,
            bool enableStreamingQueriesCounters, bool enableCountersPerTask = false)
            : TxId(std::visit(
                  [](auto arg) {
                      return ToString(arg);
                  }, txId))
        {
            auto subgroup = counters->GetSubgroup("sink", "SolomonSink");
            ;

            if (enableStreamingQueriesCounters) {
                CountersRoot = subgroup;
                subgroup = subgroup->GetSubgroup("tx_id", TxId);
                if (enableCountersPerTask) {
                    subgroup = subgroup->GetSubgroup("task_id", ToString(taskId));
                }
            }

            SendingBufferSize = subgroup->GetCounter("SendingBufferSize");
            WindowMinSendingBufferSize = subgroup->GetCounter("WindowMinSendingBufferSize");
            InflightRequests = subgroup->GetCounter("InflightRequests");
            WindowMinInflightRequests = subgroup->GetCounter("WindowMinInflightRequests");
            SentMetrics = subgroup->GetCounter("SentMetrics", true);
            ConfirmedMetrics = subgroup->GetCounter("ConfirmedMetrics", true);
            Errors = subgroup->GetCounter("Errors", true);
        }

        ~TDqSolomonWriteActorMetrics() {
            if (CountersRoot) {
                // XXX Group will be removed by each task of query (repeatedly);
                // Should be mostly harmless (as all of them removed at once at query termination);
                // Though, there are some racing potential on query restart;
                // This is common problem with solomon sink, pq source, pq sink and dq slj actor
                CountersRoot->RemoveSubgroup("tx_id", TxId);
            }
        }

        TString TxId;
        ::NMonitoring::TDynamicCounters::TCounterPtr SendingBufferSize;
        ::NMonitoring::TDynamicCounters::TCounterPtr WindowMinSendingBufferSize;
        ::NMonitoring::TDynamicCounters::TCounterPtr InflightRequests;
        ::NMonitoring::TDynamicCounters::TCounterPtr WindowMinInflightRequests;
        ::NMonitoring::TDynamicCounters::TCounterPtr SentMetrics;
        ::NMonitoring::TDynamicCounters::TCounterPtr ConfirmedMetrics;
        ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
        ::NMonitoring::TDynamicCounterPtr CountersRoot;

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

            TIssues issues{ TIssue(errorBuilder) };
            SINK_LOG_W("Got " << (res->IsTerminal ? "terminal " : "") << "error response[" << ev->Cookie
                              << "] from solomon: " << issues.ToOneLineString());
            Metrics.Errors->Inc();

            // Restore FreeSpace and remove the inflight entry so backpressure
            // accounting stays consistent even when we report an error.
            if (auto ptr = InflightBuffer.find(ev->Cookie); ptr != InflightBuffer.end() && res->IsTerminal) {
                FreeSpace += ptr->second.BodySize;
                InflightBuffer.erase(ptr);
            }

            Callbacks->OnAsyncOutputError(
                OutputIndex, issues, res->IsTerminal ? NYql::NDqProto::StatusIds::EXTERNAL_ERROR : NYql::NDqProto::StatusIds::UNSPECIFIED);
            return;
        }

        HandleSuccessSolomonResponse(*res->HttpIncomingResponse->Get(), ev->Cookie);

        SendPendingBatches();
    }

    // IActor & IDqComputeActorAsyncOutput
    void PassAway() override {   // Is called from Compute Actor
        ClearPendingBatches();
        for (const auto& [_, metricsInflight] : InflightBuffer) {
            Send(metricsInflight.HttpSenderId, new TEvents::TEvPoison());
        }

        if (HttpProxyId) {
            Send(HttpProxyId, new TEvents::TEvPoison());
        }

        TActor<TDqSolomonWriteActor>::PassAway();
    }

private:
    TSinkState BuildState() {
        return {};
    }

    TString GetUrl() const {
        return GetSolomonUrl(WriteParams.Shard.GetEndpoint(), WriteParams.Shard.GetUseSsl(), WriteParams.Shard.GetProject(),
            WriteParams.Shard.GetCluster(), WriteParams.Shard.GetService(), WriteParams.Shard.GetClusterType());
    }

    void ClearPendingBatches() {
        if (SendingBuffer.empty()) {
            return;
        }
        auto guard = Guard(*Alloc);
        while (!SendingBuffer.empty()) {
            if (const auto* pending = std::get_if<TPendingBatch>(&SendingBuffer.front())) {
                FreeSpace += pending->DataSize;
            }
            SendingBuffer.pop();
        }
    }

    TMetricsToSend BuildMetricsBatch() {
        auto guard = Guard(*Alloc);
        TMetricsToSend result;
        const auto sensorsPerRow = WriteParams.Shard.GetScheme().SensorsSize();
        YQL_ENSURE(sensorsPerRow > 0 && sensorsPerRow <= NSo::NConstants::MaxSensorsPerRow, "Invalid Solomon sensor count");
        while (!SendingBuffer.empty() && std::holds_alternative<TPendingBatch>(SendingBuffer.front()) &&
               result.MetricsCount + sensorsPerRow <= MaxMetricsPerRequest) {
            auto& pending = std::get<TPendingBatch>(SendingBuffer.front());
            auto value = std::move(*pending.Rows.Head());
            result.MetricsCount += UserMetricsEncoder.Append(value);
            pending.Rows.Pop();
            if (pending.Rows.empty()) {
                // Keep the whole input reservation until its last row is consumed:
                // dataSize is an upstream estimate, not an exact per-row allocation size.
                FreeSpace += pending.DataSize;
                SendingBuffer.pop();
            }
        }
        result.Data = UserMetricsEncoder.Encode();
        FreeSpace -= result.Data.size();
        return result;
    }

    std::optional<NHttp::THttpOutgoingRequestPtr> BuildSolomonRequest(const TString& data) {
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Url);

        if (!FillAuth(httpRequest)) {
            return {};
        }

        httpRequest->Set("x-client-id", "yandex-query");
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/json");
        httpRequest->Set<&NHttp::THttpRequest::Body>(data);
        return httpRequest;
    }

    bool FillAuth(NHttp::THttpOutgoingRequestPtr& httpRequest) {
        const TString authorizationHeader = "Authorization";

        TString authToken;
        try {
            authToken = CredentialsProvider->GetAuthInfo();
        } catch (const std::exception& ex) {
            TIssues issues{ TIssue(TStringBuilder() << "Failed to get auth token: " << ex.what()) };
            Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            return false;
        }

        switch (WriteParams.Shard.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                httpRequest->Set(authorizationHeader, "OAuth " + authToken);
                break;
            case NSo::NProto::ESolomonClusterType::CT_MONITORING:
            case NSo::NProto::ESolomonClusterType::CT_MONIUM:
                httpRequest->Set(authorizationHeader, "Bearer " + authToken);
                break;
            default:
                TIssues issues{ TIssue(TStringBuilder() << "Invalid cluster type: " << ToString<ui32>(WriteParams.Shard.GetClusterType())) };
                Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                return false;
        }

        return true;
    }

    void SendPendingBatches() {
        while (TryToSendNextBatch()) {
        }

        // Encoding pending rows can consume the space released by an ACK.
        // Notify the producer only after all available slots have been filled.
        if (FreeSpace <= 0) {
            ShouldNotifyNewFreeSpace = true;
        } else if (ShouldNotifyNewFreeSpace) {
            ShouldNotifyNewFreeSpace = false;
            Callbacks->ResumeExecution();
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

        if (std::holds_alternative<TPendingBatch>(SendingBuffer.front())) {
            if (Y_UNLIKELY(!HttpProxyId)) {
                HttpProxyId = Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
            }

            TMetricsToSend metricsToSend;
            try {
                metricsToSend = BuildMetricsBatch();
            } catch (const yexception& e) {
                TIssues issues{ TIssue(TStringBuilder() << "Error while encoding solomon metrics: " << e.what()) };
                Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
                return false;
            }
            const auto httpRequest = BuildSolomonRequest(metricsToSend.Data);

            if (!httpRequest) {
                // BuildSolomonRequest failed (e.g. auth retrieval error).
                // OnAsyncOutputError has already been called inside FillAuth.
                // Restore FreeSpace so backpressure accounting stays consistent.
                FreeSpace += metricsToSend.Data.size();
                return false;
            }

            const size_t bodySize = metricsToSend.Data.size();
            const TActorId httpSenderId = Register(CreateHttpSenderActor(SelfId(), HttpProxyId, RetryPolicy));
            Send(
                httpSenderId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(*httpRequest, /*allowConnectionReuse=*/true), /*flags=*/0, Cookie);
            SINK_LOG_T("Sent " << metricsToSend.MetricsCount << " metrics with size of " << metricsToSend.Data.size() << " bytes to solomon");

            *Metrics.SentMetrics += metricsToSend.MetricsCount;
            Y_VALIDATE(InflightBuffer.emplace(Cookie++, TMetricsInflight{ httpSenderId, metricsToSend.MetricsCount, bodySize }).second,
                "Duplicated inflight event");
            EgressStats.Bytes += bodySize;
            EgressStats.Chunks++;
            return true;
        }

        if (std::holds_alternative<NDqProto::TCheckpoint>(SendingBuffer.front())) {
            SINK_LOG_D("Process checkpoint. Inflight before checkpoint: " << InflightBuffer.size());
            CheckpointInProgress = std::get<NDqProto::TCheckpoint>(std::move(SendingBuffer.front()));
            SendingBuffer.pop();
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

        // Look up the inflight entry first so we can always restore FreeSpace
        // before reporting an error — regardless of which parse step fails.
        auto ptr = InflightBuffer.find(cookie);
        if (ptr == InflightBuffer.end()) {
            SINK_LOG_E("Solomon response[" << cookie << "] was not found in inflight");
            TIssues issues{ TIssue("Internal error in monitoring writer") };
            Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            return;
        }

        // Helper: erase the inflight entry and restore FreeSpace, then report error.
        auto reportError = [&](TIssues issues, NYql::NDqProto::StatusIds::StatusCode code) {
            FreeSpace += ptr->second.BodySize;
            InflightBuffer.erase(ptr);
            Callbacks->OnAsyncOutputError(OutputIndex, issues, code);
        };

        NJson::TJsonParser parser;
        switch (WriteParams.Shard.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
            case NSo::NProto::ESolomonClusterType::CT_MONIUM:
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
            TIssues issues{ TIssue(TStringBuilder() << "Invalid monitoring response: " << response.Response->GetObfuscatedData()) };
            SINK_LOG_E("Failed to parse response[" << cookie << "] from solomon: " << issues.ToOneLineString());
            reportError(issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            return;
        }
        if (res.size() != 2) {
            TIssues issues{ TIssue(TStringBuilder() << "Unexpected monitoring response format (fields=" << res.size()
                                                    << "): " << response.Response->GetObfuscatedData()) };
            SINK_LOG_E("Unexpected response size[" << cookie << "] from solomon: " << issues.ToOneLineString());
            reportError(issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            return;
        }

        ui64 writtenMetricsCount = 0;
        try {
            writtenMetricsCount = std::stoull(res[0]);
        } catch (const std::exception& e) {
            TIssues issues{ TIssue(TStringBuilder() << "Failed to parse writtenMetricsCount from monitoring response: " << e.what()) };
            SINK_LOG_E("Failed to parse writtenMetricsCount[" << cookie << "] from solomon: " << issues.ToOneLineString());
            reportError(issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            return;
        }
        *Metrics.ConfirmedMetrics += writtenMetricsCount;
        if (writtenMetricsCount != ptr->second.MetricsCount) {
            // TODO: YQ-340
            // TIssues issues { TIssue(TStringBuilder() << ToString(ptr->second.MetricsCount - writtenMetricsCount) << " metrics were not written: " << res[1]) };
            // Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            // return;
            SINK_LOG_W("Some metrics were not written. MetricsCount=" << ptr->second.MetricsCount
                                                                      << " writtenMetricsCount=" << writtenMetricsCount
                                                                      << " Solomon response: " << response.Response->GetObfuscatedData());
        }

        FreeSpace += ptr->second.BodySize;
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
    const std::shared_ptr<TScopedAlloc> Alloc;
    TActorId HttpProxyId;
    bool Finished = false;

    TString SourceId;
    bool ShouldNotifyNewFreeSpace = false;
    std::optional<NDqProto::TCheckpoint> CheckpointInProgress = std::nullopt;
    std::queue<std::variant<TPendingBatch, NDqProto::TCheckpoint>> SendingBuffer;
    THashMap<ui64, TMetricsInflight> InflightBuffer;

    TMetricsEncoder UserMetricsEncoder;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    ui64 Cookie = 0;
};

}   // anonymous namespace

std::pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqSolomonWriteActor(NYql::NSo::NProto::TDqSolomonShard&& settings,
    ui64 outputIndex, TCollectStatsLevel statsLevel, const TTxId& txId, ui64 taskId, const THashMap<TString, TString>& secureParams,
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* callbacks, const ::NMonitoring::TDynamicCounterPtr& counters,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory, std::shared_ptr<TScopedAlloc> alloc, i64 freeSpace,
    bool enableStreamingQueriesCounters, bool enableCountersPerTask)
{
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());

    TDqSolomonWriteParams params{
        .Shard = std::move(settings),
    };

    auto credentialsProviderFactory = credentialsFactory->Create(token);
    auto credentialsProvider = credentialsProviderFactory->CreateProvider();

    TDqSolomonWriteActor* actor = new TDqSolomonWriteActor(outputIndex, statsLevel, txId, taskId, std::move(params), callbacks, counters,
        credentialsProvider, std::move(alloc), freeSpace, enableStreamingQueriesCounters, enableCountersPerTask);
    return { actor, actor };
}

void RegisterDQSolomonWriteActorFactory(TDqAsyncIoFactory& factory, IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters, bool enableStreamingQueriesCounters) {
    factory.RegisterSink<NSo::NProto::TDqSolomonShard>("SolomonSink",
        [credentialsFactory, counters, enableStreamingQueriesCounters](
            NYql::NSo::NProto::TDqSolomonShard&& settings, IDqAsyncIoFactory::TSinkArguments&& args)
        {
            auto txId = args.TxId;
            auto taskParamsIt = args.TaskParams.find("query_path");
            if (taskParamsIt != args.TaskParams.end()) {
                txId = taskParamsIt->second;
            }
            bool enableCountersPerTask = args.StatsLevel == TCollectStatsLevel::Profile;

            return CreateDqSolomonWriteActor(std::move(settings), args.OutputIndex, args.StatsLevel, txId, args.TaskId, args.SecureParams,
                args.Callback, counters, credentialsFactory, std::move(args.Alloc), DqSolomonDefaultFreeSpace, enableStreamingQueriesCounters,
                enableCountersPerTask);
        });
}

TString GetSolomonUrl(const TString& endpoint, bool useSsl, const TString& project, const TString& cluster, const TString& service,
    const ::NYql::NSo::NProto::ESolomonClusterType& type) {
    TUrlBuilder builder((useSsl ? "https://" : "http://") + endpoint);

    switch (type) {
        case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
        case NSo::NProto::ESolomonClusterType::CT_MONIUM: {
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

}   // namespace NYql::NDq
