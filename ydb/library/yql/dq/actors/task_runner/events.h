#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/event_pb.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h> 
#include <ydb/library/yql/minikql/mkql_node.h> 

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h> 
#include <ydb/library/yql/dq/common/dq_common.h> 
#include <ydb/library/yql/dq/proto/dq_transport.pb.h> 
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h> 


namespace NYql::NDq {

namespace NTaskRunnerActor {

struct TTaskRunnerEvents {
    enum {
        ES_CREATE = EventSpaceBegin(NActors::TEvents::EEventSpace::ES_USERSPACE) + 20000,
        ES_CREATE_FINISHED,
        // channel->Pop -> TEvChannelPopFinished
        ES_POP,
        ES_POP_FINISHED,
        // channel->Push -> TEvContinueRun
        ES_PUSH,
        ES_CONTINUE_RUN,
        // ES_CONTINUE_RUN -> TaskRunner->Run() -> TEvTaskRunFinished
        ES_RUN_FINISHED,

        ES_SOURCE_PUSH,
        ES_SOURCE_PUSH_FINISHED,

        ES_SINK_POP,
        ES_SINK_POP_FINISHED,

        ES_ERROR
    };
};

struct TTaskRunnerActorSensorEntry {
    TString Name;
    i64 Sum = 0;
    i64 Max = 0;
    i64 Min = 0;
    i64 Avg = 0;
    i64 Count = 0;
};

struct TTaskRunnerActorRusage {
    i64 Utime = 0;
    i64 Stime = 0;
    i64 MajorPageFaults = 0;
};

using TTaskRunnerActorSensors = TVector<TTaskRunnerActorSensorEntry>;

struct TEvError
    : NActors::TEventLocal<TEvError, TTaskRunnerEvents::ES_ERROR>
{
    struct TStatus {
        int ExitCode;
        TString Stderr;
    };

    TEvError() = default;

    TEvError(const TString& message, bool retriable = false, bool fallback = false)
        : Message(message)
        , Retriable(retriable)
        , Fallback(fallback)
    { }

    TEvError(const TString& message, TStatus status, bool retriable = false, bool fallback = false)
        : Message(message)
        , Retriable(retriable)
        , Fallback(fallback)
        , Status(status)
    { }

    TString Message;
    bool Retriable;
    bool Fallback;
    TMaybe<TStatus> Status = Nothing();
};

struct TEvPop
    : NActors::TEventLocal<TEvPop, TTaskRunnerEvents::ES_POP>
{
    TEvPop() = default;
    TEvPop(ui32 channelId, bool wasFinished, i64 toPop)
        : ChannelId(channelId)
        , WasFinished(wasFinished)
        , Size(toPop)
    { }
    TEvPop(ui32 channelId)
        : ChannelId(channelId)
        , WasFinished(false)
        , Size(0)
    { }

    const ui32 ChannelId;
    const bool WasFinished;
    const i64 Size;
};

struct TEvPush
    : NActors::TEventLocal<TEvPush, TTaskRunnerEvents::ES_PUSH>
{
    TEvPush() = default;
    TEvPush(ui32 channelId, bool finish = true, bool askFreeSpace = false)
        : ChannelId(channelId)
        , HasData(false)
        , Finish(finish)
        , AskFreeSpace(askFreeSpace)
    { }
    TEvPush(ui32 channelId, NDqProto::TData&& data, bool finish = false, bool askFreeSpace = false)
        : ChannelId(channelId)
        , HasData(true)
        , Finish(finish)
        , AskFreeSpace(askFreeSpace)
        , Data(std::move(data))
    { }

    const ui32 ChannelId;
    const bool HasData = false;
    const bool Finish = false;
    const bool AskFreeSpace = false;
    NDqProto::TData Data;
};

struct TEvTaskRunnerCreate
    : NActors::TEventLocal<TEvTaskRunnerCreate, TTaskRunnerEvents::ES_CREATE>
{
    TEvTaskRunnerCreate() = default;
    TEvTaskRunnerCreate(
        const NDqProto::TDqTask& task,
        const TDqTaskRunnerMemoryLimits& memoryLimits,
        const std::shared_ptr<IDqTaskRunnerExecutionContext>& execCtx = std::shared_ptr<IDqTaskRunnerExecutionContext>(new TDqTaskRunnerExecutionContext()),
        const TDqTaskRunnerParameterProvider& parameterProvider = {})
        : Task(task)
        , MemoryLimits(memoryLimits)
        , ExecCtx(execCtx)
        , ParameterProvider(parameterProvider)
    { }

    NDqProto::TDqTask Task;
    TDqTaskRunnerMemoryLimits MemoryLimits;
    std::shared_ptr<IDqTaskRunnerExecutionContext> ExecCtx;
    TDqTaskRunnerParameterProvider ParameterProvider;
};

struct TEvTaskRunnerCreateFinished
    : NActors::TEventLocal<TEvTaskRunnerCreateFinished, TTaskRunnerEvents::ES_CREATE_FINISHED>
{

    TEvTaskRunnerCreateFinished() = default;
    TEvTaskRunnerCreateFinished(
        const THashMap<TString, TString>& secureParams,
        const THashMap<TString, TString>& taskParams,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const TTaskRunnerActorSensors& sensors = {})
        : Sensors(sensors)
        , SecureParams(secureParams)
        , TaskParams(taskParams)
        , TypeEnv(typeEnv)
        , HolderFactory(holderFactory)
    { }

    TTaskRunnerActorSensors Sensors;

    // for sources/sinks
    const THashMap<TString, TString>& SecureParams;
    const THashMap<TString, TString>& TaskParams;
    const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
};

struct TEvTaskRunFinished
    : NActors::TEventLocal<TEvTaskRunFinished, TTaskRunnerEvents::ES_RUN_FINISHED>
{
    TEvTaskRunFinished() = default;
    TEvTaskRunFinished(
        NDq::ERunStatus runStatus,
        THashMap<ui32, ui64>&& inputMap,
        THashMap<ui32,  ui64>&& sourcesMap,
        const TTaskRunnerActorSensors& sensors = {},
        const TTaskRunnerActorRusage& rusage = {})
        : RunStatus(runStatus)
        , Sensors(sensors)
        , Rusage(rusage)
        , InputChannelFreeSpace(std::move(inputMap))
        , SourcesFreeSpace(std::move(sourcesMap))
    { }

    NDq::ERunStatus RunStatus;
    TTaskRunnerActorSensors Sensors;
    TTaskRunnerActorRusage Rusage;

    THashMap<ui32, ui64> InputChannelFreeSpace;
    THashMap<ui32, ui64> SourcesFreeSpace;
};

struct TEvChannelPopFinished
    : NActors::TEventLocal<TEvChannelPopFinished, TTaskRunnerEvents::ES_POP_FINISHED> {
    TEvChannelPopFinished() = default;
    TEvChannelPopFinished(ui32 channelId)
        : ChannelId(channelId)
    { }
    TEvChannelPopFinished(ui32 channelId, TVector<NDqProto::TData>&& data, bool finished, bool changed, const TTaskRunnerActorSensors& sensors = {})
        : Sensors(sensors)
        , ChannelId(channelId)
        , Data(std::move(data))
        , Finished(finished)
        , Changed(changed)
    { }

    TTaskRunnerActorSensors Sensors;

    const ui32 ChannelId;
    TVector<NDqProto::TData> Data;
    bool Finished;
    bool Changed;
};

struct TEvContinueRun
    : NActors::TEventLocal<TEvContinueRun, TTaskRunnerEvents::ES_CONTINUE_RUN> {

    TEvContinueRun() = default;
    TEvContinueRun(ui32 channelId, ui64 freeSpace)
        : ChannelId(channelId)
        , AskFreeSpace(false)
        , MemLimit(0)
        , FreeSpace(freeSpace)
    { }
    TEvContinueRun(THashSet<ui32>&& inputChannels, ui64 memLimit)
        : ChannelId(0)
        , AskFreeSpace(false)
        , InputChannels(std::move(inputChannels))
        , MemLimit(memLimit)
    { }

    ui32 ChannelId;
    bool AskFreeSpace = true;
    const THashSet<ui32> InputChannels;
    ui64 MemLimit;
    ui64 FreeSpace;
};

struct TEvSourcePushFinished
    : NActors::TEventLocal<TEvSourcePushFinished, TTaskRunnerEvents::ES_SOURCE_PUSH_FINISHED>
{
    TEvSourcePushFinished() = default;
    TEvSourcePushFinished(ui64 index)
        : Index(index)
    { }

    ui64 Index;
};

struct TEvSinkPop
    : NActors::TEventLocal<TEvSinkPop, TTaskRunnerEvents::ES_SINK_POP>
{
    TEvSinkPop() = default;
    TEvSinkPop(ui64 index, i64 size)
        : Index(index)
        , Size(size)
    { }

    ui64 Index;
    i64 Size;
};

struct TEvSinkPopFinished
    : NActors::TEventLocal<TEvSinkPopFinished, TTaskRunnerEvents::ES_SINK_POP_FINISHED>
{
    TEvSinkPopFinished() = default;
    TEvSinkPopFinished(
        ui64 index,
        TMaybe<NDqProto::TCheckpoint>&& checkpoint,
        i64 size,
        i64 checkpointSize,
        bool finished,
        bool changed)
        : Index(index)
        , Checkpoint(std::move(checkpoint))
        , Size(size)
        , CheckpointSize(checkpointSize)
        , Finished(finished)
        , Changed(changed)
    { }

    const ui64 Index;
    TVector<TString> Strings;
    TMaybe<NDqProto::TCheckpoint> Checkpoint;
    i64 Size;
    i64 CheckpointSize;
    bool Finished;
    bool Changed;
};

} // namespace NTaskRunnerActor

} // namespace NYql::NDq
