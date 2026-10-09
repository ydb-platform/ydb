#include "dq_pq_read_actor.h"

#include "dq_pq_meta_extractor.h"
#include "dq_pq_rd_read_actor.h"
#include "dq_pq_read_actor_base.h"
#include "probes.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/log_backend/actor_log_backend.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/pq/common/events.h>
#include <ydb/library/yql/providers/pq/common/pq_events_processor.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/gateway/clients/composite/yql_pq_composite_read_session.h>
#include <ydb/library/yql/providers/pq/gateway/clients/message_stream/yql_pq_message_stream_client.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/string/join.h>

#include <queue>
#include <map>
#include <variant>


namespace NYql::NDq {
using namespace NActors;
using namespace NKikimr::NMiniKQL;
namespace {
LWTRACE_USING(DQ_PQ_PROVIDER);

class TPqReadActorState final : public NInternal::TPqReadState, public NFq::NMessageStream::IMessageStreamReadActorState {
public:
    using TClusterInfo = NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo;
    TPqReadActorState(ui64 inputIndex, ui64 taskId, const TTxId& txId,
        NPq::NProto::TDqPqTopicSource source, TVector<NPq::NProto::TDqReadTaskParams> readParams,
        TActorId compute, TActorId controlPlane, std::vector<TClusterInfo> clusters)
        : TPqReadState(inputIndex, taskId, {}, txId, std::move(source), std::move(readParams), compute, controlPlane)
        , Clusters(std::move(clusters)) {}
    NFq::NMessageStream::TMessageStreamReadState& GetReadState() override { return *this; }
    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override {
        TPqReadState::SaveState(checkpoint, state);
    }
    void LoadState(const TSourceState& state) override { TPqReadState::LoadState(state); }
    void InitConsumerOffsets(TActorId self, ui32 index, std::shared_ptr<NFq::IMessageStreamClient> client, ui32 count) override {
        TPqReadState::InitConsumerOffsets(self, Clusters.at(index), std::move(client), count);
    }
    bool ConsumerOffsetsInitialized() const override { return TPqReadState::ConsumerOffsetsInitialized(); }
    void StopConsumerOffsetInitialization() override { TPqReadState::StopConsumerOffsetInitialization(); }
    void HandleConsumerOffsets(TEvents::TEvInvokeResult::TPtr& event) override { TPqReadState::HandleConsumerOffsets(event); }
private:
    void OnConsumerOffsetsInitialized() override {
        TActivationContext::Send(new IEventHandle(ComputeActorId, TActivationContext::AsActorContext().SelfID,
            new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex)));
    }
    std::vector<TClusterInfo> Clusters;
};
}

ui32 ExtractPartitionsFromParams(
    TVector<NPq::NProto::TDqReadTaskParams>& readTaskParamsMsg,
    const THashMap<TString, TString>& taskParams, // partitions are here in dq
    const TVector<TString>& readRanges            // partitions are here in kqp
) {
    ui32 partitionCount = 0;
    if (!readRanges.empty()) {
        for (const auto& readRange : readRanges) {
            NPq::NProto::TDqReadTaskParams params;
            YQL_ENSURE(params.ParseFromString(readRange), "Failed to parse DqPqRead task params");
            if (!partitionCount) {
                partitionCount = params.GetPartitioningParams(0).GetTopicPartitionsCount();
            }
            YQL_ENSURE(partitionCount == params.GetPartitioningParams(0).GetTopicPartitionsCount(),
                "Different partition count " << partitionCount << ", " << params.GetPartitioningParams(0).GetTopicPartitionsCount());
            readTaskParamsMsg.emplace_back(std::move(params));
        }
    } else {
        auto taskParamsIt = taskParams.find("pq");
        YQL_ENSURE(taskParamsIt != taskParams.end(), "Failed to get pq task params");
        NPq::NProto::TDqReadTaskParams params;
        YQL_ENSURE(params.ParseFromString(taskParamsIt->second), "Failed to parse DqPqRead task params");
        partitionCount = params.GetPartitioningParams(0).GetTopicPartitionsCount();
        readTaskParamsMsg.emplace_back(std::move(params));
    }
    return partitionCount;
}

std::pair<IDqComputeActorAsyncInput*, IActor*> CreateDqPqReadActor(
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    TVector<NPq::NProto::TDqReadTaskParams>&& readTaskParamsMsg,
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    const TActorId& computeActorId,
    const THolderFactory& holderFactory,
    const TTypeEnvironment& typeEnv,
    std::shared_ptr<TScopedAlloc> alloc,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    IPqStaticGateway::TPtr pqGateway,
    ui32 topicPartitionsCount,
    bool enableStreamingQueriesCounters,
    i64 bufferSize,
    TActorId infoAggregator,
    TDuration checkPartitionCountPeriod,
    TActorId controlPlaneActorId,
    bool enableStreamingQueryTopicAutopartitioning,
    IDqSchedulableWorkFactoryPtr workFactory
) {
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();
    const auto configuredReadBufferBytes = settings.GetReadSessionBufferBytes();
    const i64 effectiveReadBufferBytes = configuredReadBufferBytes
        ? static_cast<i64>(configuredReadBufferBytes)
        : bufferSize;

    NFq::NMessageStream::TMessageStreamReadActorSettings common;
    common.WorkFactory = workFactory;
    common.InputIndex = inputIndex;
    common.TaskId = taskId;
    common.TxId = txId;
    common.ComputeActorId = computeActorId;
    common.Stream = settings.GetTopicPath();
    common.Consumer = settings.GetConsumerName();
    common.StopAtCurrentEndOffsets = settings.GetStopAtCurrentEndOffsets();
    common.RequireWriteTime = true;
    common.EnableStreamingAutopartitioning = enableStreamingQueryTopicAutopartitioning;
    common.BufferSize = effectiveReadBufferBytes;
    TDuration::TryParse(settings.GetReconnectPeriod(), common.ReconnectPeriod);
    common.CheckPartitionCountPeriod = checkPartitionCountPeriod;
    common.StatsLevel = statsLevel;
    common.Counters = counters;
    common.MetricsSource = "PqRead";
    common.EnableStreamingQueriesCounters = enableStreamingQueriesCounters;
    for (const auto& sensor : settings.GetTaskSensorLabel()) {
        common.SensorLabels.emplace_back(sensor.GetLabel(), sensor.GetValue());
    }
    common.HolderFactory = &holderFactory;
    common.Alloc = std::move(alloc);
    for (const auto& name : settings.GetMetadataFields()) {
        common.MetadataFields.push_back(CreatePqMetaExtractorLambda(name, holderFactory, typeEnv));
    }
    common.TraceRecord = [tx = TString(TStringBuilder() << txId), stream = common.Stream](const NFq::TMessageStreamRecord& record) {
        LWPROBE(PqReadDataReceived, tx, stream, *record.Data);
    };
    const auto& watermarks = settings.GetWatermarks();
    common.WatermarksEnabled = watermarks.GetEnabled();
    common.IdlePartitionsEnabled = watermarks.GetIdlePartitionsEnabled();
    common.WatermarkGranularity = TDuration::MicroSeconds(watermarks.GetGranularityUs());
    common.LateArrivalDelay = TDuration::MicroSeconds(watermarks.GetLateArrivalDelayUs());
    common.IdleTimeout = TDuration::MicroSeconds(watermarks.HasIdleTimeoutUs() ? watermarks.GetIdleTimeoutUs() : watermarks.GetLateArrivalDelayUs());
    YQL_ENSURE(settings.GetOffsetPredicate().ItemSize() <= 1, "Multiple OffsetPredicate is not implemented");
    for (const auto& predicate : settings.GetOffsetPredicate().GetItem()) {
        YQL_ENSURE(!predicate.HasPartitionId(), "Not empty PartitionId is not implemented");
        if (predicate.HasBegin()) { common.BeginOffset = predicate.GetBegin(); }
        if (predicate.HasEnd()) { common.EndOffset = predicate.GetEnd(); }
    }
    YQL_ENSURE(settings.GetWriteTimePredicate().ItemSize() <= 1, "Multiple WriteTimePredicate is not implemented");
    for (const auto& predicate : settings.GetWriteTimePredicate().GetItem()) {
        YQL_ENSURE(!predicate.HasPartitionId(), "Not empty PartitionId is not implemented");
        YQL_ENSURE(settings.GetDisposition().GetDispositionCase() == NPq::NProto::StreamingDisposition::kOldest,
            "WriteTimePredicate is supported only in table mode");
        if (predicate.HasBegin()) { common.BeginWriteTime = TInstant::MicroSeconds(predicate.GetBegin()); }
        if (predicate.HasEnd()) { common.EndWriteTime = TInstant::MicroSeconds(predicate.GetEnd()); }
    }

    using TClusterInfo = TPqReadActorState::TClusterInfo;
    std::vector<TClusterInfo> clusters;
    if (settings.FederatedClustersSize()) {
        for (const auto& cluster : settings.GetFederatedClusters()) {
            clusters.push_back({.Name = cluster.GetName(), .Endpoint = cluster.GetEndpoint(), .Path = cluster.GetDatabase()});
            common.Clusters.push_back({.Name = cluster.GetName(),
                .PartitionsCount = cluster.GetPartitionsCount() ? cluster.GetPartitionsCount() : topicPartitionsCount});
        }
    } else {
        clusters.push_back({.Endpoint = settings.GetEndpoint(), .Path = settings.GetDatabase()});
        common.Clusters.push_back({.PartitionsCount = topicPartitionsCount});
    }
    ui64 totalPartitions = 0;
    for (const auto& cluster : common.Clusters) { totalPartitions += cluster.PartitionsCount; }
    auto credentials = credentialsFactory->Create(token, addBearerToToken);
    for (size_t i = 0; i < clusters.size(); ++i) {
        auto& cluster = common.Clusters[i];
        for (const auto& params : readTaskParamsMsg) {
            for (const auto& partitioning : params.GetPartitioningParams()) {
                YQL_ENSURE(partitioning.GetDqPartitionsCount(), "Zero DQ partition count");
                for (ui64 id = partitioning.GetEachTopicPartitionGroupId(); id < cluster.PartitionsCount; id += partitioning.GetDqPartitionsCount()) {
                    cluster.Partitions.push_back(id);
                }
            }
        }
        auto executor = std::make_shared<TTopicEventProcessor<NFq::NMessageStream::TEvExecuteMessageStreamCallback>>();
        cluster.CreateClient = [driver, pqGateway, settings, info = clusters[i], credentials, executor, useCpuQuota = bool(workFactory)](const TActorContext& ctx) {
            auto options = pqGateway->GetTopicClientSettings();
            if (settings.GetUseActorSystemThreadsInTopicClient() || useCpuQuota) {
                executor->SetupTopicClientSettings(ctx.ActorSystem(), ctx.SelfID, options);
            }
            options.Database(settings.GetDatabase()).DiscoveryEndpoint(settings.GetEndpoint())
                .SslCredentials(NYdb::TSslCredentials(settings.GetUseSsl())).CredentialsProviderFactory(credentials);
            info.AdjustTopicClientSettings(options);
            std::string path = settings.GetTopicPath();
            info.AdjustTopicPath(path);
            return pqGateway->GetTopicClient(TString(path), driver, options);
        };
        auto control = std::make_shared<ICompositeTopicReadSessionControl::TPtr>();
        cluster.CreateSession = [settings, infoAggregator, txId, taskId, inputIndex, totalPartitions, name = cluster.Name,
            counters, enableStreamingQueriesCounters, control](const TActorContext& ctx, NFq::IMessageStreamClient& client,
                const NFq::TMessageStreamReadSessionSettings& read) {
            const auto skew = NProtoInterop::CastFromProto(settings.GetMaxPartitionReadSkew());
            if (!skew || settings.GetStopAtCurrentEndOffsets()) {
                control->reset();
                return NThreading::MakeFuture(client.CreateReadSession(read));
            }
            YQL_ENSURE(infoAggregator, "Missing DQ info aggregator for distributed read session");
            auto taskCounters = counters ? counters->GetSubgroup("source", "PqRead") : MakeIntrusive<NMonitoring::TDynamicCounters>();
            if (enableStreamingQueriesCounters) {
                for (const auto& sensor : settings.GetTaskSensorLabel()) {
                    taskCounters = taskCounters->GetSubgroup(sensor.GetLabel(), sensor.GetValue());
                }
                taskCounters = taskCounters->GetSubgroup("tx_id", std::visit([](auto arg) { return ToString(arg); }, txId));
            }
            if (!name.empty()) { taskCounters = taskCounters->GetSubgroup("federated_pq_cluster", name); }
            auto [session, sessionControl] = CreateCompositeTopicReadSession(ctx, client, {
                .TxId = txId, .TaskId = taskId, .Cluster = name, .AmountPartitionsCount = totalPartitions,
                .InputIndex = inputIndex, .Counters = taskCounters, .BaseSettings = read,
                .IdleTimeout = NProtoInterop::CastFromProto(settings.GetPartitionsBalancingIdleTimeout()),
                .MaxPartitionReadSkew = skew, .AggregatorActor = infoAggregator});
            *control = std::move(sessionControl);
            return NThreading::MakeFuture(std::move(session));
        };
        cluster.AdvancePartitionTime = [control](ui64 id, TInstant time) {
            if (*control) { (*control)->AdvancePartitionTime(id, time); }
        };
    }
    auto state = std::make_unique<TPqReadActorState>(inputIndex, taskId, txId, std::move(settings),
        std::move(readTaskParamsMsg), computeActorId, controlPlaneActorId, std::move(clusters));
    return NFq::NMessageStream::CreateMessageStreamReadActor(std::move(common), std::move(state));

}

void RegisterDqPqReadActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory, const IPqStaticGateway::TPtr& pqGateway, const ::NMonitoring::TDynamicCounterPtr& counters, const TString& reconnectPeriod, bool enableStreamingQueriesCounters, bool enableStreamingQueryTopicAutopartitioning) {
    factory.RegisterSource<NPq::NProto::TDqPqTopicSource>(TString(PqSource),
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), counters, pqGateway, reconnectPeriod, enableStreamingQueriesCounters, enableStreamingQueryTopicAutopartitioning](
            NPq::NProto::TDqPqTopicSource&& settings,
            IDqAsyncIoFactory::TSourceArguments&& args)
    {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));

        if (reconnectPeriod) {
            settings.SetReconnectPeriod(reconnectPeriod);
        }

        TVector<NPq::NProto::TDqReadTaskParams> readTaskParamsMsg;
        ui32 topicPartitionsCount = ExtractPartitionsFromParams(readTaskParamsMsg, args.TaskParams, args.ReadRanges);

        auto txId = args.TxId;
        auto taskParamsIt = args.TaskParams.find("query_path");
        if (taskParamsIt != args.TaskParams.end()) {
            txId = taskParamsIt->second;
        }

        TDuration checkPartitionCountPeriod;
        taskParamsIt = args.TaskParams.find("partition_count_check_enabled");
        if (taskParamsIt != args.TaskParams.end()) {
            if (taskParamsIt->second == "true") {
                checkPartitionCountPeriod = PqDefaultCheckPartitionCountPeriod;
            }
        }

        TActorId infoAggregator;
        if (const auto it = args.TaskParams.find("ControlPlane/PqSourcePartitionBalancerAggregatorId"); it != args.TaskParams.end()) {
            NActorsProto::TActorId actorIdProto;
            YQL_ENSURE(actorIdProto.ParseFromString(it->second), "Failed to parse " << it->first);
            infoAggregator = ActorIdFromProto(actorIdProto);
        }

        TActorId controlPlaneActorId;
        if (const auto it = args.TaskParams.find(PqControlPlaneActorIdParam); it != args.TaskParams.end()) {
            NActorsProto::TActorId actorIdProto;
            YQL_ENSURE(actorIdProto.ParseFromString(it->second), "Failed to parse " << it->first);
            controlPlaneActorId = ActorIdFromProto(actorIdProto);
        }

        if (!settings.GetSharedReading()) {
            return CreateDqPqReadActor(
                std::move(settings),
                args.InputIndex,
                args.StatsLevel,
                txId,
                args.TaskId,
                args.SecureParams,
                std::move(readTaskParamsMsg),
                driver,
                credentialsFactory,
                args.ComputeActorId,
                args.HolderFactory,
                args.TypeEnv,
                std::move(args.Alloc),
                counters ? counters : args.TaskCounters,
                pqGateway,
                topicPartitionsCount,
                enableStreamingQueriesCounters,
                PQReadDefaultFreeSpace,
                infoAggregator,
                checkPartitionCountPeriod,
                controlPlaneActorId,
                enableStreamingQueryTopicAutopartitioning,
                args.SchedulableWorkFactory);
        }

        const TStringBuf format(settings.GetFormat());
        const TStringBuf normalizedFormat = format.empty() ? TStringBuf("raw") : format;
        YQL_ENSURE(normalizedFormat == "json_each_row"sv || normalizedFormat == "raw"sv,
            "Row dispatcher (shared reading) supports only json_each_row and raw formats, got: " << format);

        return CreateDqPqRdReadActor(
            args.TypeEnv,
            std::move(settings),
            args.InputIndex,
            args.StatsLevel,
            txId,
            args.TaskId,
            args.SecureParams,
            std::move(readTaskParamsMsg),
            driver,
            credentialsFactory,
            args.ComputeActorId,
            NFq::RowDispatcherServiceActorId(),
            args.HolderFactory,
            counters ? counters : args.TaskCounters,
            PQReadDefaultFreeSpace,
            pqGateway,
            enableStreamingQueriesCounters,
            checkPartitionCountPeriod,
            controlPlaneActorId);
    });
}

} // namespace NYql::NDq
