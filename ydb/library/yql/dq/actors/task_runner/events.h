#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/vector.h>

namespace NYql::NDq {

namespace NTaskRunnerActor {

struct TTaskRunnerEvents {
    enum {
        EvCreate = EventSpaceBegin(NActors::TEvents::EEventSpace::ES_USERSPACE) + 20000,
        EvCreateFinished,

        EvOutputChannelDataRequest,
        EvOutputChannelData,

        EvInputChannelData,
        EvInputChannelDataAck,

        // EvContinueRun -> TaskRunner->Run() -> TEvTaskRunFinished
        EvContinueRun,
        EvRunFinished,

        EvSourceDataAck,

        EvSinkDataRequest,
        EvSinkData,

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

//Sent by ComputActor to TaskRunnerActor to request data from output channel.
//Upon receiving this event, TaskRunnerActor reads data from corresponding TaskRunner's output channel,
//collects data by chunks and then sends data chunks to the sender of this message with TEvOutputChaanelData.
//See also notes to TEvOutpuChannelData.
struct TEvOutputChannelDataRequest
    : NActors::TEventLocal<TEvOutputChannelDataRequest, TTaskRunnerEvents::EvOutputChannelDataRequest>
{
    TEvOutputChannelDataRequest(ui32 channelId, bool wasFinished, i64 requestedSize)
        : ChannelId(channelId)
        , WasFinished(wasFinished)
        , Size(requestedSize)
    { }
    TEvOutputChannelDataRequest(ui32 channelId)
        : ChannelId(channelId)
        , WasFinished(false)
        , Size(0)
    { }

    const ui32 ChannelId;
    const bool WasFinished;
    const i64 Size;
};

//Sent by ComputeActor to TaskRunnerActor and contains a portion on input channel data
//Upon receiving this event, TaskRunnerActor moves data to its input channel buffer and send TEvInputChannelDataAck back to the sender
//See also notes to TEvInputChannelDataAck
struct TEvInputChannelData
    : NActors::TEventLocal<TEvInputChannelData, TTaskRunnerEvents::EvInputChannelData>
{
    TEvInputChannelData(ui32 channelId, std::optional<TDqSerializedBatch>&& data, bool finish, bool pauseAfterPush)
        : ChannelId(channelId)
        , Data(std::move(data))
        , Finish(finish)
        , PauseAfterPush(pauseAfterPush)
    { }

    const ui32 ChannelId;
    std::optional<TDqSerializedBatch> Data; //not const, because we want to efficiently move data out of this event on a reciever side
    const bool Finish;
    const bool PauseAfterPush;
};

//Sent by TaskRunnerActor to ComputeActor to ackonowledge input data received in TEvInputChannelData
//See also note to TEvInputChannelData
struct TEvInputChannelDataAck
    : NActors::TEventLocal<TEvInputChannelDataAck, TTaskRunnerEvents::EvInputChannelDataAck> {

    TEvInputChannelDataAck(ui32 channelId, ui64 freeSpace)
        : ChannelId(channelId)
        , FreeSpace(freeSpace)
    { }

    const ui32 ChannelId;
    const ui64 FreeSpace;
};

struct TEvTaskRunnerCreate
    : NActors::TEventLocal<TEvTaskRunnerCreate, TTaskRunnerEvents::EvCreate>
{
    TEvTaskRunnerCreate() = default;
    TEvTaskRunnerCreate(
        const NDqProto::TDqTask& task,
        const TDqTaskRunnerMemoryLimits& memoryLimits,
        NDqProto::EDqStatsMode statsMode,
        const std::shared_ptr<IDqTaskRunnerExecutionContext>& execCtx)
        : Task(task)
        , MemoryLimits(memoryLimits)
        , StatsMode(statsMode)
        , ExecCtx(execCtx)
    { }

    NDqProto::TDqTask Task;
    TDqTaskRunnerMemoryLimits MemoryLimits;
    NDqProto::EDqStatsMode StatsMode;
    std::shared_ptr<IDqTaskRunnerExecutionContext> ExecCtx;
};

struct TEvTaskRunnerCreateFinished
    : NActors::TEventLocal<TEvTaskRunnerCreateFinished, TTaskRunnerEvents::EvCreateFinished>
{

    TEvTaskRunnerCreateFinished(
        const THashMap<TString, TString>& secureParams,
        const THashMap<TString, TString>& taskParams,
        const TVector<TString>& readRanges,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        THashMap<ui64, std::pair<NUdf::TUnboxedValue, IDqAsyncInputBuffer::TPtr>>&& inputTransforms,
        const TTaskRunnerActorSensors& sensors = {}
    )
        : Sensors(sensors)
        , SecureParams(secureParams)
        , TaskParams(taskParams)
        , ReadRanges(readRanges)
        , TypeEnv(typeEnv)
        , HolderFactory(holderFactory)
        , Alloc(alloc)
        , InputTransforms(std::move(inputTransforms))
    { 
        Y_ABORT_UNLESS(inputTransforms.empty() || Alloc);
    }

    ~TEvTaskRunnerCreateFinished() {
        if (!InputTransforms.empty()) {
            auto guard = Guard(*Alloc);
            InputTransforms.clear();
        }
    }

    TTaskRunnerActorSensors Sensors;

    // for sources/sinks
    const THashMap<TString, TString>& SecureParams;
    const THashMap<TString, TString>& TaskParams;
    const TVector<TString>& ReadRanges;
    const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    THashMap<ui64, std::pair<NUdf::TUnboxedValue, IDqAsyncInputBuffer::TPtr>> InputTransforms; //can'not be const, because we need to explicitly clear it in destructor
};

struct TEvTaskRunFinished
    : NActors::TEventLocal<TEvTaskRunFinished, TTaskRunnerEvents::EvRunFinished>
{
    TEvTaskRunFinished(
        NDq::ERunStatus runStatus,
        THashMap<ui32, i64>&& inputChannelsMap,
        THashMap<ui32, i64>&& sourcesMap,
        const TTaskRunnerActorSensors& sensors = {},
        const TDqMemoryQuota::TProfileStats& profileStats = {},
        ui64 mkqlMemoryLimit = 0,
        THolder<TMiniKqlProgramState>&& programState = nullptr,
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
    THolder<TMiniKqlProgramState> ProgramState;
    bool WatermarkInjectedToOutputs = false;
    bool CheckpointRequestedFromTaskRunner = false;
    TDuration ComputeTime;
};

//Sent by TaskRunnerActor to ComputeActor in response to TEvOutputChannelDataRequest.
//Contains data read from output channel accompanied with auxiliary info.
//See also notes to TEvOutputChannelDataRequest
struct TEvOutputChannelData
    : NActors::TEventLocal<TEvOutputChannelData, TTaskRunnerEvents::EvOutputChannelData>
{
    TEvOutputChannelData(ui32 channelId)
        : Stats()
        , ChannelId(channelId)
        , Data()
        , Finished(false)
        , Changed(false)
    { }

    TEvOutputChannelData(
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

//Sent by TaskRunnerActor to ComputeActor as an acknowledgement in AsyncInputPush method call
//after data is pushed to corresponding TaskRunner's source
struct TEvSourceDataAck
    : NActors::TEventLocal<TEvSourceDataAck, TTaskRunnerEvents::EvSourceDataAck>
{
    TEvSourceDataAck(ui64 index, i64 freeSpaceLeft)
        : Index(index)
        , FreeSpaceLeft(freeSpaceLeft)
    { }

    const ui64 Index;
    const i64 FreeSpaceLeft;
};

//Sent by ComputeActor to TaskRunnerActor to request output data from a sink.
//Upon receiving this event, TaskRunnerActor reads data from corresponding TaskRunner's sink buffer,
//and sends data to the sender of this message with TEvSinkData.
//See also notes to TEvSinkData.
struct TEvSinkDataRequest
    : NActors::TEventLocal<TEvSinkDataRequest, TTaskRunnerEvents::EvSinkDataRequest>
{
    TEvSinkDataRequest(ui64 index, i64 size)
        : Index(index)
        , Size(size)
    { }

    const ui64 Index;
    const i64 Size;
};

//Sent by TaskRunnerActor to ComputeActor in response to TEvSinkDataRequest.
//Contains data from TaskRunner's sink buffer
//See also notes to TEvSinkDataRequest.
struct TEvSinkData
    : NActors::TEventLocal<TEvSinkData, TTaskRunnerEvents::EvSinkData>
{
    TEvSinkData(
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
    NDq::TDqSerializedBatch Batch;
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
