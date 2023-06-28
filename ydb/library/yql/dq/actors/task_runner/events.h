#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/event_pb.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_checkpoint.pb.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/vector.h>

namespace NYql::NDq {

namespace NTaskRunnerActor {

struct TTaskRunnerEvents {
    enum {
        EvCreate = EventSpaceBegin(NActors::TEvents::EEventSpace::ES_USERSPACE) + 20000,
        EvCreateFinished,
        // channel->Pop -> TEvChannelPopFinished
        EvPop,
        EvPopFinished,
        // channel->Push -> TEvPushFinished
        EvPush,
        EvPushFinished,
        EvContinueRun,
        // EvContinueRun -> TaskRunner->Run() -> TEvTaskRunFinished
        EvRunFinished,

        EvAsyncInputPush,
        EvAsyncInputPushFinished,

        EvSinkPop,
        EvSinkPopFinished,

        EvLoadTaskRunnerFromState,
        EvLoadTaskRunnerFromStateDone,

        EvStatistics,

        EvError
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

using TTaskRunnerActorSensors = TVector<TTaskRunnerActorSensorEntry>;

struct TEvError
    : NActors::TEventLocal<TEvError, TTaskRunnerEvents::EvError>
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
    : NActors::TEventLocal<TEvPop, TTaskRunnerEvents::EvPop>
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
    : NActors::TEventLocal<TEvPush, TTaskRunnerEvents::EvPush>
{
    TEvPush() = default;
    TEvPush(ui32 channelId, bool finish = true, bool pauseAfterPush = false, bool isOut = false)
        : ChannelId(channelId)
        , HasData(false)
        , Finish(finish)
        , PauseAfterPush(pauseAfterPush)
        , IsOut(isOut)
    { }
    TEvPush(ui32 channelId, TDqSerializedBatch&& data, bool finish = false, bool pauseAfterPush = false)
        : ChannelId(channelId)
        , HasData(true)
        , Finish(finish)
        , Data(std::move(data))
        , PauseAfterPush(pauseAfterPush)
    { }

    const ui32 ChannelId;
    const bool HasData = false;
    const bool Finish = false;
    TDqSerializedBatch Data;
    bool PauseAfterPush = false;
    const bool IsOut = false;
};

struct TEvPushFinished
    : NActors::TEventLocal<TEvPushFinished, TTaskRunnerEvents::EvPushFinished> {

    TEvPushFinished() = default;

    TEvPushFinished(ui32 channelId, ui64 freeSpace)
        : ChannelId(channelId)
        , FreeSpace(freeSpace)
    { }

    ui32 ChannelId;
    ui64 FreeSpace;
};

struct TEvTaskRunnerCreate
    : NActors::TEventLocal<TEvTaskRunnerCreate, TTaskRunnerEvents::EvCreate>
{
    TEvTaskRunnerCreate() = default;
    TEvTaskRunnerCreate(
        const NDqProto::TDqTask& task,
        const TDqTaskRunnerMemoryLimits& memoryLimits,
        const std::shared_ptr<IDqTaskRunnerExecutionContext>& execCtx = std::shared_ptr<IDqTaskRunnerExecutionContext>(new TDqTaskRunnerExecutionContext()))
        : Task(task)
        , MemoryLimits(memoryLimits)
        , ExecCtx(execCtx)
    { }

    NDqProto::TDqTask Task;
    TDqTaskRunnerMemoryLimits MemoryLimits;
    std::shared_ptr<IDqTaskRunnerExecutionContext> ExecCtx;
};

struct TEvTaskRunnerCreateFinished
    : NActors::TEventLocal<TEvTaskRunnerCreateFinished, TTaskRunnerEvents::EvCreateFinished>
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
    : NActors::TEventLocal<TEvTaskRunFinished, TTaskRunnerEvents::EvRunFinished>
{
    TEvTaskRunFinished() = default;
    TEvTaskRunFinished(
        NDq::ERunStatus runStatus,
        THashMap<ui32, i64>&& inputChannelsMap,
        THashMap<ui32, i64>&& sourcesMap,
        const TTaskRunnerActorSensors& sensors = {},
        const TDqMemoryQuota::TProfileStats& profileStats = {},
        ui64 mkqlMemoryLimit = 0,
        THolder<NDqProto::TMiniKqlProgramState>&& programState = nullptr,
        bool watermarkInjectedToOutputs = false,
        bool checkpointRequestedFromTaskRunner = false,
        TDuration computeTime = TDuration::Zero())
        : RunStatus(runStatus)
        , Sensors(sensors)
        , InputChannelFreeSpace(std::move(inputChannelsMap))
        , SourcesFreeSpace(std::move(sourcesMap))
        , ProfileStats(profileStats)
        , MkqlMemoryLimit(mkqlMemoryLimit)
        , ProgramState(std::move(programState))
        , WatermarkInjectedToOutputs(watermarkInjectedToOutputs)
        , CheckpointRequestedFromTaskRunner(checkpointRequestedFromTaskRunner)
        , ComputeTime(computeTime)
    { }

    NDq::ERunStatus RunStatus;
    TTaskRunnerActorSensors Sensors;

    THashMap<ui32, i64> InputChannelFreeSpace;
    THashMap<ui32, i64> SourcesFreeSpace;
    TDqMemoryQuota::TProfileStats ProfileStats;
    ui64 MkqlMemoryLimit = 0;
    THolder<NDqProto::TMiniKqlProgramState> ProgramState;
    bool WatermarkInjectedToOutputs = false;
    bool CheckpointRequestedFromTaskRunner = false;
    TDuration ComputeTime;
};

struct TEvChannelPopFinished
    : NActors::TEventLocal<TEvChannelPopFinished, TTaskRunnerEvents::EvPopFinished>
{
    TEvChannelPopFinished() = default;

    TEvChannelPopFinished(ui32 channelId)
        : Stats()
        , ChannelId(channelId)
        , Data()
        , Finished(false)
        , Changed(false)
    { }

    TEvChannelPopFinished(
            ui32 channelId,
            TVector<TDqSerializedBatch>&& data,
            TMaybe<NDqProto::TWatermark>&& watermark,
            TMaybe<NDqProto::TCheckpoint>&& checkpoint,
            bool finished,
            bool changed,
            const TTaskRunnerActorSensors& sensors = {},
            TDqTaskRunnerStatsView&& stats = {})
        : Sensors(sensors)
        , Stats(std::move(stats))
        , ChannelId(channelId)
        , Data(std::move(data))
        , Watermark(std::move(watermark))
        , Checkpoint(std::move(checkpoint))
        , Finished(finished)
        , Changed(changed)
    { }

    TTaskRunnerActorSensors Sensors;
    NDq::TDqTaskRunnerStatsView Stats;

    const ui32 ChannelId;
    // The order is Data -> Watermark -> Checkpoint
    TVector<TDqSerializedBatch> Data;
    TMaybe<NDqProto::TWatermark> Watermark;
    TMaybe<NDqProto::TCheckpoint> Checkpoint;
    bool Finished;
    bool Changed;
};

struct TWatermarkRequest {
    TWatermarkRequest() = default;

    TWatermarkRequest(TVector<ui32>&& channelIds, TInstant watermark)
        : ChannelIds(std::move(channelIds))
        , Watermark(watermark) {
    }

    TVector<ui32> ChannelIds;
    TInstant Watermark;
};

// Holds info required to inject barriers to outputs
struct TCheckpointRequest {
    TCheckpointRequest(TVector<ui32>&& channelIds, TVector<ui32>&& sinkIds, const NDqProto::TCheckpoint& checkpoint)
        : ChannelIds(std::move(channelIds))
        , SinkIds(std::move(sinkIds))
        , Checkpoint(checkpoint) {
    }

    TVector<ui32> ChannelIds;
    TVector<ui32> SinkIds;
    NDqProto::TCheckpoint Checkpoint;
};

struct TEvContinueRun
    : NActors::TEventLocal<TEvContinueRun, TTaskRunnerEvents::EvContinueRun> {

    TEvContinueRun() = default;

    explicit TEvContinueRun(
        TMaybe<TWatermarkRequest>&& watermarkRequest,
        TMaybe<TCheckpointRequest>&& checkpointRequest,
        bool checkpointOnly
    )
        : MemLimit(0)
        , WatermarkRequest(std::move(watermarkRequest))
        , CheckpointRequest(std::move(checkpointRequest))
        , CheckpointOnly(checkpointOnly)
    { }

    TEvContinueRun(THashSet<ui32>&& inputChannels, ui64 memLimit)
        : AskFreeSpace(false)
        , InputChannels(std::move(inputChannels))
        , MemLimit(memLimit)
    { }

    bool AskFreeSpace = true;
    const THashSet<ui32> InputChannels;
    ui64 MemLimit;
    TMaybe<TWatermarkRequest> WatermarkRequest = Nothing();
    TMaybe<TCheckpointRequest> CheckpointRequest = Nothing();
    bool CheckpointOnly = false;
    TVector<ui32> SinkIds;
    TVector<ui32> InputTransformIds;
};

struct TEvAsyncInputPushFinished
    : NActors::TEventLocal<TEvAsyncInputPushFinished, TTaskRunnerEvents::EvAsyncInputPushFinished>
{
    TEvAsyncInputPushFinished() = default;
    TEvAsyncInputPushFinished(ui64 index, i64 freeSpaceLeft)
        : Index(index)
        , FreeSpaceLeft(freeSpaceLeft)
    { }

    ui64 Index;
    i64 FreeSpaceLeft;
};

struct TEvSinkPop
    : NActors::TEventLocal<TEvSinkPop, TTaskRunnerEvents::EvSinkPop>
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
    : NActors::TEventLocal<TEvSinkPopFinished, TTaskRunnerEvents::EvSinkPopFinished>
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

struct TEvLoadTaskRunnerFromState : NActors::TEventLocal<TEvLoadTaskRunnerFromState, TTaskRunnerEvents::EvLoadTaskRunnerFromState>{
    TEvLoadTaskRunnerFromState(TString&& blob) : Blob(std::move(blob)) {}

    TMaybe<TString> Blob;
};

struct TEvLoadTaskRunnerFromStateDone : NActors::TEventLocal<TEvLoadTaskRunnerFromStateDone, TTaskRunnerEvents::EvLoadTaskRunnerFromStateDone>{
    TEvLoadTaskRunnerFromStateDone(TMaybe<TString> error) : Error(error) {}

    TMaybe<TString> Error;
};

struct TEvStatistics : NActors::TEventLocal<TEvStatistics, TTaskRunnerEvents::EvStatistics>
{
    explicit TEvStatistics(TVector<ui32>&& sinkIds, TVector<ui32>&& inputTransformIds)
        : SinkIds(std::move(sinkIds))
        , InputTransformIds(std::move(inputTransformIds))
        , Stats() {
    }

    TVector<ui32> SinkIds;
    TVector<ui32> InputTransformIds;
    NDq::TDqTaskRunnerStatsView Stats;
};

} // namespace NTaskRunnerActor

} // namespace NYql::NDq
