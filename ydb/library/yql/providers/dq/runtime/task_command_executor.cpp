#include "task_command_executor.h"

#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include <ydb/library/yql/providers/dq/api/protos/task_command_executor.pb.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

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
static const int CurrentProtocolVersion = 5; // Calls for Sinks

template<typename T>
void ToProto(T* s1, const NDq::TDqInputChannelStats* ss)
{
    s1->SetChannelId(ss->ChannelId);
    s1->SetChunks(ss->Chunks);
    s1->SetBytes(ss->Bytes);
    s1->SetRowsIn(ss->RowsIn);
    s1->SetRowsOut(ss->RowsOut);
    s1->SetMaxMemoryUsage(ss->MaxMemoryUsage);
    s1->SetDeserializationTimeUs(ss->DeserializationTime.MicroSeconds());
}

template<typename T>
void ToProto(T* s1, const NDq::TDqAsyncInputBufferStats* ss)
{
    s1->SetChunks(ss->Chunks);
    s1->SetBytes(ss->Bytes);
    s1->SetRowsIn(ss->RowsIn);
    s1->SetRowsOut(ss->RowsOut);
    s1->SetMaxMemoryUsage(ss->MaxMemoryUsage);
    s1->SetInputIndex(ss->InputIndex);
}

template<typename T>
void ToProto(T* s1, const NDq::TDqOutputChannelStats* ss)
{
    s1->SetChannelId(ss->ChannelId);
    s1->SetChunks(ss->Chunks);
    s1->SetBytes(ss->Bytes);
    s1->SetRowsIn(ss->RowsIn);
    s1->SetRowsOut(ss->RowsOut);
    s1->SetMaxMemoryUsage(ss->MaxMemoryUsage);
}

template<typename T>
void ToProto(T* s1, const NDq::TDqAsyncOutputBufferStats* ss)
{
    s1->SetChunks(ss->Chunks);
    s1->SetBytes(ss->Bytes);
    s1->SetRowsIn(ss->RowsIn);
    s1->SetRowsOut(ss->RowsOut);
    s1->SetMaxMemoryUsage(ss->MaxMemoryUsage);
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
                    auto& old = CurrentJobStats[counterName];
                    if (name.EndsWith("Time")) {
                        QueryStat.AddTimeCounter(counterName, value - old);
                    } else {
                        QueryStat.AddCounter(counterName, value - old);
                    }
                    old = value;
                }
            });
        }

        QueryStat.AddTaskRunnerStats(
            *Runner->GetStats(),
            CurrentStats,
            Runner->GetTaskId());
        for (const auto& [inputId, _]: CurrentInputChannelsStats) {
            UpdateInputChannelStats(inputId);
        }
        for (const auto& [outputId, _]: CurrentOutputChannelsStats) {
            UpdateOutputChannelStats(outputId);
        }
    }

    void UpdateOutputChannelStats(ui64 channelId)
    {
        if (!Runner) {
            return;
        }
        auto s = Runner->GetStats();
        auto maybeOutputChannelStats = s->OutputChannels.find(channelId);
        if (maybeOutputChannelStats == s->OutputChannels.end() || !maybeOutputChannelStats->second) {
            return;
        }
        auto maybeOutputChannelOldStats = CurrentOutputChannelsStats.find(channelId);
        if (maybeOutputChannelOldStats == CurrentOutputChannelsStats.end()) {
            maybeOutputChannelOldStats = CurrentOutputChannelsStats.emplace_hint(
                maybeOutputChannelOldStats, channelId, NDq::TDqOutputChannelStats(channelId));
        }
        QueryStat.AddOutputChannelStats(
            *maybeOutputChannelStats->second,
            maybeOutputChannelOldStats->second,
            Runner->GetTaskId(), channelId);
    }

    void UpdateInputChannelStats(ui64 channelId)
    {
        if (!Runner) {
            return;
        }
        auto s = Runner->GetStats();
        auto maybeInputChannelStats = s->InputChannels.find(channelId);
        if (maybeInputChannelStats == s->InputChannels.end() || !maybeInputChannelStats->second) {
            return;
        }
        auto maybeInputChannelOldStats = CurrentInputChannelsStats.find(channelId);
        if (maybeInputChannelOldStats == CurrentInputChannelsStats.end()) {
            maybeInputChannelOldStats = CurrentInputChannelsStats.emplace_hint(
                maybeInputChannelOldStats, channelId, NDq::TDqInputChannelStats(channelId));
        }
        QueryStat.AddInputChannelStats(
            *maybeInputChannelStats->second,
            maybeInputChannelOldStats->second,
            Runner->GetTaskId(), channelId);
    }

    void UpdateSourceStats(ui64 inputIndex)
    {
        if (!Runner) {
            return;
        }
        auto s = Runner->GetStats();
        auto maybeSourceStats = s->Sources.find(inputIndex);
        if (maybeSourceStats == s->Sources.end() || !maybeSourceStats->second) {
            return;
        }
        auto maybeSourceOldStats = CurrentSourcesStats.find(inputIndex);
        if (maybeSourceOldStats == CurrentSourcesStats.end()) {
            maybeSourceOldStats = CurrentSourcesStats.emplace_hint(
                maybeSourceOldStats, inputIndex, NDq::TDqAsyncInputBufferStats(inputIndex));
        }
        QueryStat.AddSourceStats(
            *maybeSourceStats->second,
            maybeSourceOldStats->second,
            Runner->GetTaskId(), inputIndex);
    }

    template<typename T>
    void UpdateStats(T& t) {
        UpdateStats();
        QueryStat.FlushCounters(t);

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
        s->SetPendingInputTimeUs(stats->RunStatusTimeMetrics[NDq::ERunStatus::PendingInput].MicroSeconds());
        s->SetPendingOutputTimeUs(stats->RunStatusTimeMetrics[NDq::ERunStatus::PendingOutput].MicroSeconds());
        s->SetFinishTimeUs(stats->RunStatusTimeMetrics[NDq::ERunStatus::Finished].MicroSeconds());
        static_assert(NDq::TRunStatusTimeMetrics::StatusesCount == 3); // Add all statuses here

        //s->SetTotalTime(stats->TotalTime.MilliSeconds());
        s->SetWaitTimeUs(stats->WaitTime.MicroSeconds());
        s->SetWaitOutputTimeUs(stats->WaitOutputTime.MicroSeconds());

        //s->SetMkqlTotalNodes(stats->MkqlTotalNodes);
        //s->SetMkqlCodegenFunctions(stats->MkqlCodegenFunctions);
        //s->SetCodeGenTotalInstructions(stats->CodeGenTotalInstructions);
        //s->SetCodeGenTotalFunctions(stats->CodeGenTotalFunctions);
        //s->SetCodeGenFullTime(stats->CodeGenFullTime);
        //s->SetCodeGenFinalizeTime(stats->CodeGenFinalizeTime);
        //s->SetCodeGenModulePassTime(stats->CodeGenModulePassTime);

        for (const auto& [id, ss] : stats->OutputChannels) {
            ToProto(s->AddOutputChannels(), ss);
        }

        for (const auto& [id, ss] : stats->InputChannels) {
            ToProto(s->AddInputChannels(), ss);
        }

        for (const auto& [id, ss] : stats->Sources) {
            ToProto(s->AddSources(), ss);
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

                NDqProto::TData data;
                data.Load(&input);

                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit
                channel->Push(std::move(data));
                UpdateInputChannelStats(channelId);
                break;
            }
            case NDqProto::TCommandHeader::PUSH_SOURCE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto source = Runner->GetSource(channelId);

                NDqProto::TSourcePushRequest request;
                request.Load(&input);

                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit
                NDq::TDqDataSerializer dataSerializer(Runner->GetTypeEnv(), Runner->GetHolderFactory(),
                    NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED);
                NKikimr::NMiniKQL::TUnboxedValueVector buffer;
                buffer.reserve(request.GetData().GetRows());
                if (request.GetString().empty() && request.GetChunks() == 0) {
                    dataSerializer.Deserialize(request.GetData(), source->GetInputType(), buffer);
                } else if (!request.GetString().empty()) {
                    buffer.reserve(request.GetString().size());
                    for (auto& row : request.GetString()) {
                        buffer.emplace_back(NKikimr::NMiniKQL::MakeString(row));
                    }
                } else {
                    i64 chunks = request.GetChunks();
                    buffer.reserve(chunks);
                    for (i64 i = 0; i < chunks; i++) {
                        NDqProto::TSourcePushChunk chunk;
                        chunk.Load(&input);
                        i64 parts = chunk.GetParts();

                        if (parts == 1) {
                            buffer.emplace_back(NKikimr::NMiniKQL::MakeString(chunk.GetString()));
                        } else {
                            TString str;
                            for (i64 j = 0; j < parts; j++) {
                                NDqProto::TSourcePushPart part;
                                part.Load(&input);
                                str += part.GetString();
                            }
                            buffer.emplace_back(NKikimr::NMiniKQL::MakeString(str));
                        }
                    }
                }

                source->Push(std::move(buffer), request.GetSpace());
                UpdateSourceStats(channelId);
                break;
            }
            case NDqProto::TCommandHeader::FINISH: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                Runner->GetInputChannel(channelId)->Finish();
                UpdateInputChannelStats(channelId);
                break;
            }
            case NDqProto::TCommandHeader::FINISH_OUTPUT: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                Runner->GetOutputChannel(channelId)->Finish();
                UpdateOutputChannelStats(channelId);
                break;
            }
            case NDqProto::TCommandHeader::FINISH_SOURCE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());
                Runner->GetSource(channelId)->Finish();
                UpdateSourceStats(channelId);
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
                Runner->GetOutputChannel(channelId)->Terminate();
                UpdateOutputChannelStats(channelId);
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

                response.SetResult(channel->Pop(*response.MutableData()));
                UpdateOutputChannelStats(channelId);
                QueryStat.FlushCounters(response);
                response.MutableStats()->PackFrom(GetStats(taskId));
                response.Save(&output);

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
                    UpdateStats(response);
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
                NDq::TDqTaskSettings settings(std::move(task));
                try {
                    Prepare(settings, taskMeta, output);
                } catch (const NKikimr::TMemoryLimitExceededException& ex) {
                    throw yexception() << "DQ computation exceeds the memory limit " << DqConfiguration->MemoryLimit.Get().GetOrElse(0) << ". Try to increase the limit using PRAGMA dq.MemoryLimit";
                }

                break;
            }
            case NDqProto::TCommandHeader::GET_INPUT_TYPE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit

                Y_ENSURE(taskId == Runner->GetTaskId());
                auto channel = Runner->GetInputChannel(channelId);
                auto inputType = channel->GetInputType();

                NDqProto::TGetTypeResponse response;
                response.SetResult(NKikimr::NMiniKQL::SerializeNode(inputType, Runner->GetTypeEnv()));
                response.Save(&output);

                break;
            }
            case NDqProto::TCommandHeader::GET_SOURCE_TYPE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit

                Y_ENSURE(taskId == Runner->GetTaskId());
                auto source = Runner->GetSource(channelId);
                auto inputType = source->GetInputType();

                NDqProto::TGetTypeResponse response;
                response.SetResult(NKikimr::NMiniKQL::SerializeNode(inputType, Runner->GetTypeEnv()));
                response.Save(&output);

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
                auto ss = Runner->GetInputChannel(channelId)->GetStats();
                NDqProto::TGetStatsInputResponse response;
                ToProto(response.MutableStats(), ss);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::GET_STATS_SOURCE: {
                Y_ENSURE(header.GetVersion() >= 4);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto ss = Runner->GetSource(channelId)->GetStats();
                NDqProto::TGetStatsSourceResponse response;
                ToProto(response.MutableStats(), ss);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::GET_STATS_OUTPUT: {
                Y_ENSURE(header.GetVersion() >= 3);
                Y_ENSURE(taskId == Runner->GetTaskId());
                auto ss = Runner->GetOutputChannel(channelId)->GetStats();
                NDqProto::TGetStatsOutputResponse response;
                ToProto(response.MutableStats(), ss);
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
                NKikimr::NMiniKQL::TUnboxedValueVector batch;
                auto sink = Runner->GetSink(channelId);
                auto* outputType = sink->GetOutputType();
                auto bytes = sink->Pop(batch, request.GetBytes());

                NDqProto::TSinkPopResponse response;
                if (request.GetRaw()) {
                    for (auto& raw : batch) {
                        *response.AddString() = raw.AsStringRef();
                    }
                } else {
                    NDq::TDqDataSerializer dataSerializer(
                        Runner->GetTypeEnv(),
                        Runner->GetHolderFactory(),
                        NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);

                    *response.MutableData() = dataSerializer.Serialize(
                        batch.begin(), batch.end(),
                        static_cast<NKikimr::NMiniKQL::TType*>(outputType));
                }
                response.SetBytes(bytes);
                response.Save(&output);
                break;
            }
            case NDqProto::TCommandHeader::SINK_OUTPUT_TYPE: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                auto guard = Runner->BindAllocator(0); // Explicitly reset memory limit

                Y_ENSURE(taskId == Runner->GetTaskId());
                auto outputType = Runner->GetSink(channelId)->GetOutputType();

                NDqProto::TGetTypeResponse response;
                response.SetResult(NKikimr::NMiniKQL::SerializeNode(outputType, Runner->GetTypeEnv()));
                response.Save(&output);

                break;
            }
            case NDqProto::TCommandHeader::SINK_STATS: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                Y_ENSURE(taskId == Runner->GetTaskId());

                auto* stats = Runner->GetSink(channelId)->GetStats();
                NDqProto::TSinkStatsResponse response;
                ToProto(response.MutableStats(), stats);
                response.Save(&output);
                break;
            }
            //
            case NDqProto::TCommandHeader::STOP: {
                Y_ENSURE(header.GetVersion() <= CurrentProtocolVersion);
                _exit(0);
            }
            default:
                Y_VERIFY(false);
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
        NYql::NDqProto::TPrepareResponse result;
        result.SetResult(true); // COMPAT(aozeritsky) YQL-14268

        DqConfiguration->Dispatch(taskMeta.GetSettings());
        DqConfiguration->FreezeDefaults();
        if (!DqConfiguration->CollectCoreDumps.Get().GetOrElse(false)) {
            DontCollectDumps();
        }
        // TODO: Maybe use taskParams from task.GetTask().GetParameters()
        THashMap<TString, TString> taskParams;
        for (const auto& x: taskMeta.GetTaskParams()) {
            taskParams[x.first] = x.second;
        }

        TString workingDirectory = taskParams[NTaskRunnerProxy::WorkingDirectoryParamName];
        Y_VERIFY(workingDirectory);
        NFs::SetCurrentWorkingDirectory(workingDirectory);

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
                        Y_VERIFY(false);
                }
            }
        });

        NBacktrace::SetModulesMapping(modulesMapping);

        QueryStat.Measure<void>("MakeDqTaskRunner", [&]() {
            NDq::TDqTaskRunnerSettings settings;
            settings.CollectBasicStats = true;
            settings.CollectProfileStats = true;
            settings.TerminateOnError = TerminateOnError;
            for (const auto& x: taskMeta.GetSecureParams()) {
                settings.SecureParams[x.first] = x.second;
                YQL_CLOG(DEBUG, ProviderDq) << "SecureParam " << x.first << ":XXX";
            }
            settings.OptLLVM = DqConfiguration->OptLLVM.Get().GetOrElse("");

            Ctx.FuncProvider = TaskTransformFactory(taskParams, Ctx.FuncRegistry);

            Runner = MakeDqTaskRunner(Ctx, settings, nullptr);
        });

        auto guard = Runner->BindAllocator(DqConfiguration->MemoryLimit.Get().GetOrElse(0));

        QueryStat.Measure<void>("Prepare", [&]() {
            NDq::TDqTaskRunnerMemoryLimits limits;
            limits.ChannelBufferSize = DqConfiguration->ChannelBufferSize.Get().GetOrElse(TDqSettings::TDefault::ChannelBufferSize);
            limits.OutputChunkMaxSize = DqConfiguration->OutputChunkMaxSize.Get().GetOrElse(TDqSettings::TDefault::OutputChunkMaxSize);
            limits.ChunkSizeLimit = DqConfiguration->ChunkSizeLimit.Get().GetOrElse(TDqSettings::TDefault::ChunkSizeLimit);
            Runner->Prepare(task, limits);
        });

        result.Save(&output);
    }

    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    TTaskTransformFactory TaskTransformFactory;
    THashMap<TString, i64> CurrentJobStats;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats;
    bool TerminateOnError;
    TIntrusivePtr<NDq::IDqTaskRunner> Runner;
    TCounters QueryStat;
    TDqConfiguration::TPtr DqConfiguration = MakeIntrusive<TDqConfiguration>();
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    NDq::TDqTaskRunnerContext Ctx;
    const NKikimr::NMiniKQL::TUdfModuleRemappings EmptyRemappings;

    TRusage Rusage;

    NDq::TDqTaskRunnerStats CurrentStats;
    std::unordered_map<ui64, NDq::TDqInputChannelStats> CurrentInputChannelsStats;
    std::unordered_map<ui64, NDq::TDqAsyncInputBufferStats> CurrentSourcesStats;
    std::unordered_map<ui64, NDq::TDqOutputChannelStats> CurrentOutputChannelsStats;

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
