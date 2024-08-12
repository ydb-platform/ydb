#include "task_command_executor.h"

#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>
#include <ydb/library/yql/providers/dq/counters/task_counters.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include <ydb/library/yql/providers/dq/api/protos/task_command_executor.pb.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/context.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <util/system/thread.h>
#include <util/system/fs.h>
#include <util/system/env.h>
#include <util/system/rusage.h>

#include <util/folder/path.h>

#include <util/stream/file.h>
#include <util/stream/pipe.h>

#ifdef _unix_
#include <sys/resource.h>
#endif

namespace NYql {
namespace NTaskRunnerProxy {

// static const int CurrentProtocolVersion = 1;
// static const int CurrentProtocolVersion = 2; // GetFreeSpace
// static const int CurrentProtocolVersion = 3; // Calls for ComputeActor
// static const int CurrentProtocolVersion = 4; // Calls for Sources
// static const int CurrentProtocolVersion = 5; // Calls for Sinks
static const int CurrentProtocolVersion = 6; // Respond free space after run

template<typename T>
void ToProto(T& proto, const NDq::TDqAsyncStats& stats)
{
    proto.SetBytes(stats.Bytes);
    proto.SetDecompressedBytes(stats.DecompressedBytes);
    proto.SetRows(stats.Rows);
    proto.SetChunks(stats.Chunks);
    proto.SetSplits(stats.Splits);

    proto.SetFirstMessageMs(stats.FirstMessageTs.MilliSeconds());
    proto.SetPauseMessageMs(stats.PauseMessageTs.MilliSeconds());
    proto.SetResumeMessageMs(stats.ResumeMessageTs.MilliSeconds());
    proto.SetLastMessageMs(stats.LastMessageTs.MilliSeconds());
    proto.SetWaitTimeUs(stats.WaitTime.MicroSeconds());
    proto.SetWaitPeriods(stats.WaitPeriods);
}

template<typename T>
void ToProto(T& proto, const NDq::IDqInputChannel& channel)
{
    proto.SetChannelId(channel.GetPushStats().ChannelId);
    proto.SetSrcStageId(channel.GetPushStats().SrcStageId);
    ToProto(*proto.MutablePush(), channel.GetPushStats());
    ToProto(*proto.MutablePop(), channel.GetPopStats());
    proto.SetMaxMemoryUsage(channel.GetPushStats().MaxMemoryUsage);
    proto.SetDeserializationTimeUs(channel.GetPushStats().DeserializationTime.MicroSeconds());
}

template<typename T>
void ToProto(T& proto, const NDq::IDqAsyncInputBuffer& asyncBuffer)
{
    proto.SetInputIndex(asyncBuffer.GetPushStats().InputIndex);
    proto.SetIngressName(asyncBuffer.GetPushStats().Type);
    ToProto(*proto.MutablePush(), asyncBuffer.GetPushStats());
    ToProto(*proto.MutablePop(), asyncBuffer.GetPopStats());
    proto.SetMaxMemoryUsage(asyncBuffer.GetPushStats().MaxMemoryUsage);
}

template<typename T>
void ToProto(T& proto, const NDq::IDqOutputChannel& channel)
{
    proto.SetChannelId(channel.GetPopStats().ChannelId);
    proto.SetDstStageId(channel.GetPopStats().DstStageId);
    ToProto(*proto.MutablePush(), channel.GetPushStats());
    ToProto(*proto.MutablePop(), channel.GetPopStats());
    proto.SetMaxMemoryUsage(channel.GetPopStats().MaxMemoryUsage);
}

template<typename T>
void ToProto(T& proto, const NDq::IDqAsyncOutputBuffer& asyncBuffer)
{
    proto.SetOutputIndex(asyncBuffer.GetPopStats().OutputIndex);
    proto.SetEgressName(asyncBuffer.GetPopStats().Type);
    ToProto(*proto.MutablePush(), asyncBuffer.GetPushStats());
    ToProto(*proto.MutablePop(), asyncBuffer.GetPopStats());
    proto.SetMaxMemoryUsage(asyncBuffer.GetPopStats().MaxMemoryUsage);
}

class TTaskCommandExecutor {
public:
    TTaskCommandExecutor(NKikimr::NMiniKQL::TComputationNodeFactory compFactory, TTaskTransformFactory taskTransformFactory, NKikimr::NMiniKQL::IStatsRegistry* jobStats, bool terminateOnError)
        : ComputationFactory(std::move(compFactory))
        , TaskTransformFactory(std::move(taskTransformFactory))
        , JobStats(std::move(jobStats))
        , TerminateOnError(terminateOnError)
    {
    }

    void UpdateStats() {
        if (!Runner) {
            return;
        }

        if (JobStats) {
            auto taskId = Runner->GetTaskId();
            std::map<TString, TString> labels = {
                {"Task", ToString(taskId)}
            };

            JobStats->ForEachStat([&](const NKikimr::NMiniKQL::TStatKey& key, i64 value) {
                if (value) {
                    TString name = TString(key.GetName());
                    auto counterName = TCounters::GetCounterName(
                        "TaskRunner",
                        labels,
                        name);
                    if (name.EndsWith("Time")) {
                        QueryStat.SetTimeCounter(counterName, value);
                    } else {
                        QueryStat.SetCounter(counterName, value);
                    }
                }
            });
        }

        QueryStat.AddTaskRunnerStats(
            *Runner->GetStats(),
            Runner->GetTaskId(), StageId);

        auto statsMode = DqConfiguration->TaskRunnerStats.Get().GetOrElse(TDqSettings::TDefault::TaskRunnerStats);
        if (statsMode == TDqSettings::ETaskRunnerStats::Profile) {
            auto stats = Runner->GetStats();
            for (const auto& [stageId, stageChannels] : stats->OutputChannels) {
                for (const auto& [id, channel] : stageChannels) {
                    UpdateOutputChannelStats(channel);
                }
            }
            for (const auto& [stageId, stageChannels] : stats->InputChannels) {
                for (const auto& [id, channel] : stageChannels) {
                    UpdateInputChannelStats(channel);
                }
            }
            for (const auto& [sourceId, source] : stats->Sources) {
                UpdateSourceStats(source);
            }
        }
    }

    void UpdateOutputChannelStats(NDq::IDqOutputChannel::TPtr channel) {
        QueryStat.AddOutputChannelStats(
            channel->GetPushStats(),
            channel->GetPopStats(),
            Runner->GetTaskId(), StageId);
    }

    void UpdateInputChannelStats(NDq::IDqInputChannel::TPtr channel)
    {
        QueryStat.AddInputChannelStats(
            channel->GetPushStats(),
            channel->GetPopStats(),
            Runner->GetTaskId(), StageId);
    }

    void UpdateSourceStats(NDq::IDqAsyncInputBuffer::TPtr source)
    {
        QueryStat.AddSourceStats(
            source->GetPushStats(),
            source->GetPopStats(),
            Runner->GetTaskId(), StageId);
    }

    template<typename T>
    void UpdateStats(T& t) {
        UpdateStats();

        TTaskCounters deltaStat;
        deltaStat.TakeDeltaCounters(QueryStat, PrevStat);
        deltaStat.FlushCounters(t);
        QueryStat.ClearCounters();

        auto currentRusage = TRusage::Get();
        TRusage delta;
        delta.Utime = currentRusage.Utime - Rusage.Utime;
        delta.Stime = currentRusage.Stime - Rusage.Stime;
        delta.MajorPageFaults = currentRusage.MajorPageFaults - Rusage.MajorPageFaults;
        t.MutableRusage()->SetUtime(delta.Utime.MicroSeconds());
        t.MutableRusage()->SetStime(delta.Stime.MicroSeconds());
        t.MutableRusage()->SetMajorPageFaults(delta.MajorPageFaults);
        Rusage = currentRusage;
    }

    void Run()
    {
        try {
            RunUnsafe();
        } catch (const std::exception& ex) {
            try {
                // don't generate core if parent died before child
                Cerr << ex.what() << Endl;
                Cerr << "Command " << LastCommand << Endl;
                Cerr << "Version " << LastVersion << Endl;
                Cerr << "TaskId " << LastTaskId << Endl;
                Cerr << "ChannelId " << LastChannelId << Endl;
                Cerr << "CommandNo " << CommandNo << Endl;
                UpdateStats();

                // sum min max avg count
                for (const auto& [k, v] : QueryStat.Get()) {
                    Cerr << "Counter: "
                        << k << " "
                        << v.Sum << " "
                        << v.Min << " "
                        << v.Max << " "
                        << v.Avg << " "
                        << v.Count << Endl;
                }
            } catch (...) { }
        }
        _exit(127);
    }

    NDqProto::TMeteringStatsResponse GetMeteringStats() {
        NDqProto::TMeteringStatsResponse resp;
        auto* stats = Runner->GetMeteringStats();
        for (auto& input : stats->Inputs) {
            auto* i = resp.AddInputs();
            i->SetRowsConsumed(input->RowsConsumed);
            i->SetBytesConsumed(input->BytesConsumed);
        }
        return resp;
    }

    NDqProto::TGetStatsResponse GetStats(ui64 taskId) {
        const auto stats = Runner->GetStats();

        NDqProto::TGetStatsResponse response;
        auto* s = response.MutableStats();
        s->SetTaskId(taskId);
        //s->SetStartTs(stats->StartTs.MilliSeconds());
        //s->SetFinishTs(stats->FinishTs.MilliSeconds());
        s->SetBuildCpuTimeUs(stats->BuildCpuTime.MicroSeconds());
        s->SetComputeCpuTimeUs(stats->ComputeCpuTime.MicroSeconds());

        // All run statuses metrics
        // s->SetPendingInputTimeUs(stats->RunStatusTimeMetrics[NDq::ERunStatus::PendingInput].MicroSeconds());
        // s->SetPendingOutputTimeUs(stats->RunStatusTimeMetrics[NDq::ERunStatus::PendingOutput].MicroSeconds());
        // s->SetFinishTimeUs(stats->RunStatusTimeMetrics[NDq::ERunStatus::Finished].MicroSeconds());
        // static_assert(NDq::TRunStatusTimeMetrics::StatusesCount == 3); // Add all statuses here

        // s->SetTotalTime(stats->TotalTime.MilliSeconds());
        // s->SetWaitTimeUs(stats->WaitTime.MicroSeconds());
        // s->SetWaitOutputTimeUs(stats->WaitOutputTime.MicroSeconds());

        //s->SetMkqlTotalNodes(stats->MkqlTotalNodes);
        //s->SetMkqlCodegenFunctions(stats->MkqlCodegenFunctions);
        //s->SetCodeGenTotalInstructions(stats->CodeGenTotalInstructions);
        //s->SetCodeGenTotalFunctions(stats->CodeGenTotalFunctions);
        //s->SetCodeGenFullTime(stats->CodeGenFullTime);
        //s->SetCodeGenFinalizeTime(stats->CodeGenFinalizeTime);
        //s->SetCodeGenModulePassTime(stats->CodeGenModulePassTime);

        for (const auto& [stageId, stageChannels] : stats->OutputChannels) {
            for (const auto& [id, channel] : stageChannels) {
                ToProto(*s->AddOutputChannels(), *channel);
            }
        }

        for (const auto& [stageId, stageChannels] : stats->InputChannels) {
            for (const auto& [id, channel] : stageChannels) {
                ToProto(*s->AddInputChannels(), *channel);
            }
        }

        for (const auto& [id, source] : stats->Sources) {
            ToProto(*s->AddSources(), *source);
        }

        return response;
    }

    void RunUnsafe()
    {
        FunctionRegistry = QueryStat.Measure<TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry>>("CreateFunctionRegistry", [&]() {
            auto ret = NKikimr::NMiniKQL::CreateFunctionRegistry(
                &NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {})->Clone();
            NKikimr::NMiniKQL::FillStaticModules(*ret);
            return ret;
        });

        auto deterministicMode = !!GetEnv("YQL_DETERMINISTIC_MODE");

        auto randomProvider = deterministicMode
            ? CreateDeterministicRandomProvider(1)
            : CreateDefaultRandomProvider();
        auto timeProvider = deterministicMode
            ? CreateDeterministicTimeProvider(10000000)
            : CreateDefaultTimeProvider();

        Ctx.FuncRegistry = FunctionRegistry.Get();
        Ctx.ComputationFactory = ComputationFactory;
        Ctx.RandomProvider = randomProvider.Get();
        Ctx.TimeProvider = timeProvider.Get();

        TPipedInput input(0);
        TPipedOutput output(1);

        while (true) {
            NDqProto::TCommandHeader header;
            header.Load(&input);

            ui64 taskId = header.GetTaskId();
            ui64 channelId = header.GetChannelId();

            LastCommand = static_cast<i64>(header.GetCommand());
            LastVersion = header.GetVersion();
            LastTaskId = taskId;
            LastChannelId = channelId;
            CommandNo ++;

            switch (header.GetCommand()) {
            case NDqProto::TCommandHeader::VERSION: {
                NDqProto::TGetVersionResponse response;
                response.SetVersion(CurrentProtocolVersion);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::PUSH: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto channel = Runner->GetInputChannel(channelId);

                NDq::TDqSerializedBatch data;
                data.Proto.Load(&input);
                if (data.IsOOB()) {
                    LoadRopeFromPipe(input, data.Payload);
                }

                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit
                channel->Push(std::move(data));
                break;
            }
            case NDqProto::TCommandHeader::PUSH_SOURCE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto source = Runner->GetSource(channelId);

                NDqProto::TSourcePushRequest request;
                request.Load(&input);

                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit
                NKikimr::NMiniKQL::TUnboxedValueBatch buffer(source->GetInputType());
                NDq::TDqSerializedBatch batch;
                batch.Proto = std::move(*request.MutableData());
                if (batch.IsOOB()) {
                    LoadRopeFromPipe(input, batch.Payload);
                }
                NDq::TDqDataSerializer dataSerializer(Runner->GetTypeEnv(), Runner->GetHolderFactory(),
                    (NDqProto::EDataTransportVersion)batch.Proto.GetTransportVersion());
                dataSerializer.Deserialize(std::move(batch), source->GetInputType(), buffer);

                source->Push(std::move(buffer), request.GetSpace());
                break;
            }
            case NDqProto::TCommandHeader::FINISH: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto channel = Runner->GetInputChannel(channelId);
                channel->Finish();
                break;
            }
            case NDqProto::TCommandHeader::FINISH_OUTPUT: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto channel = Runner->GetOutputChannel(channelId);
                channel->Finish();
                break;
            }
            case NDqProto::TCommandHeader::FINISH_SOURCE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto source = Runner->GetSource(channelId);
                source->Finish();
                break;
            }
            case NDqProto::TCommandHeader::DROP_OUTPUT: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto res = Runner->GetOutputChannel(channelId)->Drop();

                NDqProto::TDropOutputResponse response;
                response.SetResult(res);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::TERMINATE_OUTPUT: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto channel = Runner->GetOutputChannel(channelId);
                channel->Terminate();
                break;
            }
            case NDqProto::TCommandHeader::GET_STORED_BYTES: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto res = Runner->GetInputChannel(channelId)->GetStoredBytes();

                NDqProto::TGetStoredBytesResponse response;
                response.SetResult(res);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::GET_STORED_BYTES_SOURCE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto res = Runner->GetSource(channelId)->GetStoredBytes();

                NDqProto::TGetStoredBytesResponse response;
                response.SetResult(res);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::POP: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit

                Y_ENSURE(taskId == Runner->GetTaskId());

                auto channel = Runner->GetOutputChannel(channelId);

                NDqProto::TPopResponse response;

                NDq::TDqSerializedBatch batch;
                response.SetResult(channel->Pop(batch));
                bool isOOB = batch.IsOOB();
                *response.MutableData() = std::move(batch.Proto);

                TTaskCounters deltaStat;
                deltaStat.TakeDeltaCounters(QueryStat, PrevStat);
                deltaStat.FlushCounters(response);
                QueryStat.ClearCounters();

                response.MutableStats()->PackFrom(GetStats(taskId));
                response.Save(&output);
                if (isOOB) {
                    SaveRopeToPipe(output, batch.Payload);
                }

                break;
            }
            case NDqProto::TCommandHeader::IS_FINISHED: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto channel = Runner->GetOutputChannel(channelId);
                NDqProto::TIsFinishedResponse response;
                response.SetResult(channel->IsFinished());
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::RUN: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                auto guard = Runner->BindAllocator(DqConfiguration->MemoryLimit.Get().GetOrElse(0));

                Y_ENSURE(taskId == Runner->GetTaskId());
                try {
                    NDqProto::TRunResponse response;
                    auto status = Runner->Run();
                    response.SetResult(static_cast<ui32>(status));
                    if (status == NDq::ERunStatus::Finished) {
                        UpdateStats(response);
                    }
                    for (auto id : InputChannels) {
                        auto* space = response.AddChannelFreeSpace();
                        space->SetId(id);
                        space->SetSpace(Runner->GetInputChannel(id)->GetFreeSpace());
                    }

                    for (auto id : Sources) {
                        auto* space = response.AddSourceFreeSpace();
                        space->SetId(id);
                        space->SetSpace(Runner->GetSource(id)->GetFreeSpace());
                    }
                    response.Save(&output);
                } catch (const NKikimr::TMemoryLimitExceededException& ex) {
                    throw yexception() << "DQ computation exceeds the memory limit " << DqConfiguration->MemoryLimit.Get().GetOrElse(0) << ". Try to increase the limit using PRAGMA dq.MemoryLimit";
                }
                break;
            }
            case NDqProto::TCommandHeader::PREPARE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);

                NDqProto::TPrepareRequest request;
                Yql::DqsProto::TTaskMeta taskMeta;

                QueryStat.Measure<void>("LoadTask", [&]() {
                    request.Load(&input);
                });

                NDqProto::TDqTask task;
                request.MutableTask()->Swap(&task);
                task.GetMeta().UnpackTo(&taskMeta);
                NDq::TDqTaskSettings settings(&task);
                try {
                    Prepare(settings, taskMeta, output);
                } catch (const NKikimr::TMemoryLimitExceededException& ex) {
                    throw yexception() << "DQ computation exceeds the memory limit " << DqConfiguration->MemoryLimit.Get().GetOrElse(0) << ". Try to increase the limit using PRAGMA dq.MemoryLimit";
                }

                break;
            }
            case NDqProto::TCommandHeader::CONFIGURE_FAILURE_INJECTOR: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                TFailureInjector::Activate();
                NDqProto::TConfigureFailureInjectorRequest request;
                request.Load(&input);
                TFailureInjector::Set(request.name(), request.skip(), request.fail());
                break;
            }
            case NDqProto::TCommandHeader::GET_FREE_SPACE: {
                Y_ENSURE(header.GetVersion() >= 2);

                Y_ENSURE(taskId == Runner->GetTaskId());
                auto channel = Runner->GetInputChannel(channelId);
                NDqProto::TGetFreeSpaceResponse response;
                response.SetFreeSpace(channel->GetFreeSpace());
                response.Save(&output);

                break;
            }
            case NDqProto::TCommandHeader::GET_FREE_SPACE_SOURCE: {
                Y_ENSURE(header.GetVersion() >= 4);

                Y_ENSURE(taskId == Runner->GetTaskId());
                auto source = Runner->GetSource(channelId);
                NDqProto::TGetFreeSpaceResponse response;
                response.SetFreeSpace(source->GetFreeSpace());
                response.Save(&output);

                break;
            }
            case NDqProto::TCommandHeader::GET_STATS: {
                Y_ENSURE(header.GetVersion() >= 3);
                Y_ENSURE(taskId == Runner->GetTaskId());
                GetStats(taskId).Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::GET_METERING_STATS: {
                Y_ENSURE(header.GetVersion() >= 3);
                Y_ENSURE(taskId == Runner->GetTaskId());
                GetMeteringStats().Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::GET_STATS_INPUT: {
                Y_ENSURE(header.GetVersion() >= 3);
                Y_ENSURE(taskId == Runner->GetTaskId());
                NDqProto::TGetStatsInputResponse response;
                ToProto(*response.MutableStats(), *Runner->GetInputChannel(channelId));
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::GET_STATS_SOURCE: {
                Y_ENSURE(header.GetVersion() >= 4);
                Y_ENSURE(taskId == Runner->GetTaskId());
                NDqProto::TGetStatsSourceResponse response;
                ToProto(*response.MutableStats(), *Runner->GetSource(channelId));
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::GET_STATS_OUTPUT: {
                Y_ENSURE(header.GetVersion() >= 3);
                Y_ENSURE(taskId == Runner->GetTaskId());
                NDqProto::TGetStatsOutputResponse response;
                ToProto(*response.MutableStats(), *Runner->GetOutputChannel(channelId));
                response.Save(&output);
                break;
            }
            // Sinks
            case NDqProto::TCommandHeader::SINK_IS_FINISHED: {
                Y_ENSURE(header.GetVersion() >= 5);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto flag = Runner->GetSink(channelId)->IsFinished();
                NDqProto::TIsFinishedResponse response;
                response.SetResult(flag);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::SINK_POP: {
                Y_ENSURE(header.GetVersion() >= 5);
                Y_ENSURE(taskId == Runner->GetTaskId());

                NDqProto::TSinkPopRequest request;
                request.Load(&input);

                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit
                auto sink = Runner->GetSink(channelId);
                auto* outputType = sink->GetOutputType();
                NKikimr::NMiniKQL::TUnboxedValueBatch batch(outputType);
                YQL_ENSURE(!batch.IsWide());
                auto bytes = sink->Pop(batch, request.GetBytes());

                NDqProto::TSinkPopResponse response;
                NDq::TDqDataSerializer dataSerializer(
                    Runner->GetTypeEnv(),
                    Runner->GetHolderFactory(),
                    DataTransportVersion);
                NDq::TDqSerializedBatch serialized = dataSerializer.Serialize(batch, outputType);
                bool isOOB = serialized.IsOOB();
                *response.MutableData() = std::move(serialized.Proto);
                response.SetBytes(bytes);
                response.Save(&output);
                if (isOOB) {
                    SaveRopeToPipe(output, serialized.Payload);
                }
                break;
            }
            case NDqProto::TCommandHeader::SINK_STATS: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());

                NDqProto::TSinkStatsResponse response;
                ToProto(*response.MutableStats(), *Runner->GetSink(channelId));
                response.Save(&output);
                break;
            }
            //
            case NDqProto::TCommandHeader::STOP: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                _exit(0);
            }
            default:
                Y_ABORT_UNLESS(false);
            }
        }
    }

    void DontCollectDumps() {
#ifdef _unix_
        rlimit limit = {0, 0};
        if (setrlimit(RLIMIT_CORE, &limit) != 0) {
            Cerr << "Failed to set rlimit " << Endl;
        }
#endif
    }

    template<typename T>
    void Prepare(const NDq::TDqTaskSettings& task, const T& taskMeta, TPipedOutput& output) {
        this->StageId = task.GetStageId();
        NYql::NDqProto::TPrepareResponse result;
        result.SetResult(true); // COMPAT(aozeritsky) YQL-14268

        DqConfiguration->Dispatch(taskMeta.GetSettings());
        DqConfiguration->FreezeDefaults();
        if (!DqConfiguration->CollectCoreDumps.Get().GetOrElse(false)) {
            DontCollectDumps();
        }
        DataTransportVersion = DqConfiguration->GetDataTransportVersion();
        // TODO: Maybe use taskParams from task.GetTask().GetParameters()
        THashMap<TString, TString> taskParams;
        for (const auto& x: taskMeta.GetTaskParams()) {
            taskParams[x.first] = x.second;
        }

        TString workingDirectory = taskParams[NTaskRunnerProxy::WorkingDirectoryParamName];
        Y_ABORT_UNLESS(workingDirectory);
        NFs::SetCurrentWorkingDirectory(workingDirectory);

        QueryStat.Measure<void>("LoadPgExtensions", [&]()
        {
            if (TFsPath(NCommon::PgCatalogFileName).Exists()) {
                TFileInput file(TString{NCommon::PgCatalogFileName});
                NPg::ImportExtensions(file.ReadAll(), false,
                    NKikimr::NMiniKQL::CreateExtensionLoader().get());
            }
        });

        THashMap<TString, TString> modulesMapping;

        QueryStat.Measure<void>("LoadUdfs", [&]()
        {
            THashSet<TString> initializedUdfs;
            for (const auto& file : taskMeta.GetFiles()) {
                auto name = file.GetName();
                auto path = file.GetLocalPath();

                switch (file.GetObjectType()) {
                    case Yql::DqsProto::TFile::EEXE_FILE:
                        modulesMapping.emplace("EXE", file.GetObjectId());
                        break;
                    case Yql::DqsProto::TFile::EUSER_FILE:
                        break;
                    case Yql::DqsProto::TFile::EUDF_FILE:
                        if (initializedUdfs.insert(file.GetObjectId()).second) {
                            FunctionRegistry->LoadUdfs(path, EmptyRemappings, 0, file.GetCustomUdfPrefix());
                            modulesMapping.emplace(TFsPath(path).RealPath(), file.GetObjectId());
                        }
                        break;
                    default:
                        Y_ABORT_UNLESS(false);
                }
            }
        });

        NBacktrace::SetModulesMapping(modulesMapping);

        QueryStat.Measure<void>("MakeDqTaskRunner", [&]() {
            NDq::TDqTaskRunnerSettings settings;
            auto statsMode = DqConfiguration->TaskRunnerStats.Get().GetOrElse(TDqSettings::TDefault::TaskRunnerStats);
            settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_NONE;
            switch (statsMode) {
            case TDqSettings::ETaskRunnerStats::Basic:
                settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_BASIC;
                break;
            case TDqSettings::ETaskRunnerStats::Full:
                settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_FULL;
                break;
            case TDqSettings::ETaskRunnerStats::Profile:
                settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_PROFILE;
                break;
            default: break;
            }
            settings.TerminateOnError = TerminateOnError;
            for (const auto& x: taskMeta.GetSecureParams()) {
                settings.SecureParams[x.first] = x.second;
                YQL_CLOG(DEBUG, ProviderDq) << "SecureParam " << x.first << ":XXX";
            }
            settings.OptLLVM = DqConfiguration->OptLLVM.Get().GetOrElse("");

            Ctx.FuncProvider = TaskTransformFactory(taskParams, Ctx.FuncRegistry);

            Y_ABORT_UNLESS(!Alloc);
            Y_ABORT_UNLESS(FunctionRegistry);
            Alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(
                __LOCATION__,
                NKikimr::TAlignedPagePoolCounters(),
                FunctionRegistry->SupportsSizedAllocators(),
                false
            );

            Runner = MakeDqTaskRunner(Alloc, Ctx, settings, nullptr);
        });

        auto guard = Runner->BindAllocator(DqConfiguration->MemoryLimit.Get().GetOrElse(0));

        QueryStat.Measure<void>("Prepare", [&]() {
            NDq::TDqTaskRunnerMemoryLimits limits;
            limits.ChannelBufferSize = DqConfiguration->ChannelBufferSize.Get().GetOrElse(TDqSettings::TDefault::ChannelBufferSize);
            limits.OutputChunkMaxSize = DqConfiguration->OutputChunkMaxSize.Get().GetOrElse(TDqSettings::TDefault::OutputChunkMaxSize);
            limits.ChunkSizeLimit = DqConfiguration->ChunkSizeLimit.Get().GetOrElse(TDqSettings::TDefault::ChunkSizeLimit);

            NDq::TDqTaskRunnerExecutionContextDefault execCtx;
            Runner->Prepare(task, limits, execCtx);

            for (ui32 i = 0; i < task.InputsSize(); ++i) {
                auto& inputDesc = task.GetInputs(i);
                if (inputDesc.HasSource()) {
                    Sources.emplace(i);
                } else {
                    for (auto& inputChannelDesc : inputDesc.GetChannels()) {
                        ui64 channelId = inputChannelDesc.GetId();
                        InputChannels.emplace(channelId);
                    }
                }
            }
        });

        result.Save(&output);
    }
private:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    TTaskTransformFactory TaskTransformFactory;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats;
    bool TerminateOnError;
    TIntrusivePtr<NDq::IDqTaskRunner> Runner;
    THashSet<ui64> Sources;
    THashSet<ui64> InputChannels;
    ui32 StageId = 0;
    TTaskCounters QueryStat;
    TTaskCounters PrevStat;
    TDqConfiguration::TPtr DqConfiguration = MakeIntrusive<TDqConfiguration>();
    NDqProto::EDataTransportVersion DataTransportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    NDq::TDqTaskRunnerContext Ctx;
    const NKikimr::NMiniKQL::TUdfModuleRemappings EmptyRemappings;

    TRusage Rusage;

    i64 LastCommand = -1;
    i64 LastVersion = -1;
    ui64 LastTaskId = -1;
    ui64 LastChannelId = -1;
    i64 CommandNo = 0;
};

int CreateTaskCommandExecutor(NKikimr::NMiniKQL::TComputationNodeFactory compFactory, TTaskTransformFactory taskTransformFactory, NKikimr::NMiniKQL::IStatsRegistry* jobStats, bool terminateOnError)
{
    TTaskCommandExecutor(compFactory, taskTransformFactory, jobStats, terminateOnError).Run();
    return 0;
}

} // namespace NTaskRunnerProxy
} // namespace NYql
