#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/runtime/dq_async_stats.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NYql {
namespace NDq {

struct TEvDqCompute {
    struct TEvState : public NActors::TEventPB<TEvState, NDqProto::TEvComputeActorState, TDqComputeEvents::EvState> {};
    struct TEvStateRequest : public NActors::TEventPB<TEvStateRequest, NDqProto::TEvComputeStateRequest, TDqComputeEvents::EvStateRequest> {};

    struct TEvResumeExecution : public NActors::TEventLocal<TEvResumeExecution, TDqComputeEvents::EvResumeExecution> {
        TEvResumeExecution(EResumeSource source)
            : Source(source)
        { }

        TEvResumeExecution() = default;

        EResumeSource Source = EResumeSource::Default;
    };

    struct TEvChannelsInfo : public NActors::TEventPB<TEvChannelsInfo, NDqProto::TEvChannelsInfo,
        TDqComputeEvents::EvChannelsInfo> {};

    struct TEvChannelData : public NActors::TEventPB<TEvChannelData, NDqProto::TEvComputeChannelData,
        TDqComputeEvents::EvChannelData> {};

    struct TEvChannelDataAck : public NActors::TEventPB<TEvChannelDataAck, NDqProto::TEvComputeChannelDataAck,
        TDqComputeEvents::EvChannelDataAck> {};

    // todo: make it private
    struct TEvRetryChannelData : public NActors::TEventLocal<TEvRetryChannelData, TDqComputeEvents::EvRetryChannelData> {
        TEvRetryChannelData(ui64 channelId, ui64 fromSeqNo, ui64 toSeqNo)
            : ChannelId(channelId)
            , FromSeqNo(fromSeqNo)
            , ToSeqNo(toSeqNo) {}

        const ui64 ChannelId;
        const ui64 FromSeqNo;
        const ui64 ToSeqNo;
    };

    // todo: make it private
    struct TEvRetryChannelDataAck : public NActors::TEventLocal<TEvRetryChannelDataAck, TDqComputeEvents::EvRetryChannelDataAck> {
        TEvRetryChannelDataAck(ui64 channelId, ui64 fromSeqNo, ui64 toSeqNo)
            : ChannelId(channelId)
            , FromSeqNo(fromSeqNo)
            , ToSeqNo(toSeqNo) {}

        const ui64 ChannelId;
        const ui64 FromSeqNo;
        const ui64 ToSeqNo;
    };

    struct TEvRun : public NActors::TEventPB<TEvRun, NDqProto::TEvRun, TDqComputeEvents::EvRun> {};

    struct TEvNewCheckpointCoordinator : public NActors::TEventPB<TEvNewCheckpointCoordinator,
        NDqProto::TEvNewCheckpointCoordinator, TDqComputeEvents::EvNewCheckpointCoordinator> {

        TEvNewCheckpointCoordinator() = default;

        TEvNewCheckpointCoordinator(ui64 generation, TString graphId) {
            Record.SetGeneration(generation);
            Record.SetGraphId(std::move(graphId));
        }
    };

    struct TEvNewCheckpointCoordinatorAck : public NActors::TEventPB<TEvNewCheckpointCoordinatorAck,
        NDqProto::TEvNewCheckpointCoordinatorAck, TDqComputeEvents::EvNewCheckpointCoordinatorAck> {

        TEvNewCheckpointCoordinatorAck() = default;
    };

    struct TEvInjectCheckpoint : public NActors::TEventPB<TEvInjectCheckpoint,
        NDqProto::TEvInjectCheckpoint, TDqComputeEvents::EvInjectCheckpoint> {

        TEvInjectCheckpoint() = default;

        TEvInjectCheckpoint(ui64 id, ui64 generation, NDqProto::ECheckpointType type) {
            Record.MutableCheckpoint()->SetId(id);
            Record.MutableCheckpoint()->SetGeneration(generation);
            Record.MutableCheckpoint()->SetType(type);
            Record.SetGeneration(generation);
        }
    };

    struct TEvSaveTaskState : public NActors::TEventLocal<TEvSaveTaskState, TDqComputeEvents::EvSaveTaskState> {
        TEvSaveTaskState(TString graphId, ui64 taskId, NDqProto::TCheckpoint checkpoint)
            : GraphId(std::move(graphId))
            , TaskId(taskId)
            , Checkpoint(std::move(checkpoint))
        {}

        const TString GraphId;
        const ui64 TaskId;
        const NDqProto::TCheckpoint Checkpoint;
        TComputeActorState State;
    };

    struct TEvSaveTaskStateResult : public NActors::TEventPB<TEvSaveTaskStateResult,
        NDqProto::TEvSaveTaskStateResult, TDqComputeEvents::EvSaveTaskStateResult> {};

    struct TEvCommitState : public NActors::TEventPB<TEvCommitState,
        NDqProto::TEvCommitState, TDqComputeEvents::EvCommitState> {

        TEvCommitState() = default;

        TEvCommitState(ui64 checkpointId, ui64 checkpointGeneration, ui64 coordinatorGeneration) {
            Record.MutableCheckpoint()->SetId(checkpointId);
            Record.MutableCheckpoint()->SetGeneration(checkpointGeneration);
            Record.SetGeneration(coordinatorGeneration);
        }
    };

    struct TEvStateCommitted : public NActors::TEventPB<TEvStateCommitted,
        NDqProto::TEvStateCommitted, TDqComputeEvents::EvStateCommitted> {

        TEvStateCommitted() = default;

        TEvStateCommitted(ui64 id, ui64 generation, ui64 taskId) {
            Record.MutableCheckpoint()->SetId(id);
            Record.MutableCheckpoint()->SetGeneration(generation);
            Record.SetTaskId(taskId);
        }
    };

    struct TEvRestoreFromCheckpoint : public NActors::TEventPB<TEvRestoreFromCheckpoint,
        NDqProto::TEvRestoreFromCheckpoint, TDqComputeEvents::EvRestoreFromCheckpoint> {

        TEvRestoreFromCheckpoint() = default;

        TEvRestoreFromCheckpoint(ui64 checkpointId, ui64 checkpointGeneration, ui64 coordinatorGeneration) {
            Init(checkpointId, checkpointGeneration, coordinatorGeneration);
            Record.MutableStateLoadPlan()->SetStateType(NDqProto::NDqStateLoadPlan::STATE_TYPE_OWN); // default
        }

        TEvRestoreFromCheckpoint(ui64 checkpointId, ui64 checkpointGeneration, ui64 coordinatorGeneration, const NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan) {
            Init(checkpointId, checkpointGeneration, coordinatorGeneration);
            *Record.MutableStateLoadPlan() = taskPlan;
        }

    private:
        void Init(ui64 checkpointId, ui64 checkpointGeneration, ui64 coordinatorGeneration) {
            Record.MutableCheckpoint()->SetId(checkpointId);
            Record.MutableCheckpoint()->SetGeneration(checkpointGeneration);
            Record.SetGeneration(coordinatorGeneration);
        }
    };

    struct TEvRestoreFromCheckpointResult : public NActors::TEventPB<TEvRestoreFromCheckpointResult,
        NDqProto::TEvRestoreFromCheckpointResult, TDqComputeEvents::EvRestoreFromCheckpointResult> {
        using TBaseEventPB = NActors::TEventPB<TEvRestoreFromCheckpointResult, NDqProto::TEvRestoreFromCheckpointResult, TDqComputeEvents::EvRestoreFromCheckpointResult>;

        using TBaseEventPB::TBaseEventPB;

        TEvRestoreFromCheckpointResult(
            const NDqProto::TCheckpoint& checkpoint,
            ui64 taskId,
            NDqProto::TEvRestoreFromCheckpointResult::ERestoreStatus status,
            const NYql::TIssues& issues) {
            Record.MutableCheckpoint()->CopyFrom(checkpoint);
            Record.SetTaskId(taskId);
            Record.SetStatus(status);
            NYql::IssuesToMessage(issues, Record.MutableIssues());
        }
    };

    struct TEvGetTaskState : public NActors::TEventLocal<TEvGetTaskState, TDqComputeEvents::EvGetTaskState> {
        TEvGetTaskState(TString graphId, const std::vector<ui64>& taskIds, NDqProto::TCheckpoint checkpoint, ui64 generation)
            : GraphId(std::move(graphId))
            , TaskIds(taskIds)
            , Checkpoint(std::move(checkpoint))
            , Generation(generation) {}

        const TString GraphId;
        const std::vector<ui64> TaskIds;
        const NDqProto::TCheckpoint Checkpoint;
        const ui64 Generation;
    };

    struct TEvGetTaskStateResult : public NActors::TEventLocal<TEvGetTaskStateResult, TDqComputeEvents::EvGetTaskStateResult> {
        TEvGetTaskStateResult(NDqProto::TCheckpoint checkpoint, TIssues issues, ui64 generation)
            : Checkpoint(std::move(checkpoint))
            , Issues(std::move(issues))
            , Generation(generation) {}

        const NDqProto::TCheckpoint Checkpoint;
        std::vector<TComputeActorState> States;
        const TIssues Issues;
        const ui64 Generation;
    };
};

struct TDqExecutionSettings {
    struct TFlowControl {
        ui64 MaxOutputChunkSize = 2_MB;
        float InFlightBytesOvercommit = 1.5f;

        TDuration OutputChannelDeliveryInterval = TDuration::Seconds(30);
        TDuration OutputChannelRetryInterval = TDuration::MilliSeconds(500);
    };

    TFlowControl FlowControl;

    void Reset() {
        FlowControl = TFlowControl();
    }
};

const TDqExecutionSettings& GetDqExecutionSettings();
TDqExecutionSettings& GetDqExecutionSettingsForTests();

struct TReportStatsSettings {
    // Min interval between stats messages.
    TDuration MinInterval;
    // Max interval to sent stats in case of no activity.
    TDuration MaxInterval;
};

inline TCollectStatsLevel StatsModeToCollectStatsLevel(NDqProto::EDqStatsMode statsMode) {
         if (statsMode >= NDqProto::DQ_STATS_MODE_PROFILE) return TCollectStatsLevel::Profile;
    else if (statsMode >= NDqProto::DQ_STATS_MODE_FULL)    return TCollectStatsLevel::Full;
    else if (statsMode >= NDqProto::DQ_STATS_MODE_BASIC)   return TCollectStatsLevel::Basic;
    else                                                   return TCollectStatsLevel::None;
}

struct TComputeRuntimeSettings {
    TMaybe<TDuration> Timeout;
    NDqProto::EDqStatsMode StatsMode = NDqProto::DQ_STATS_MODE_NONE;
    TMaybe<TReportStatsSettings> ReportStatsSettings;

    // see kqp_rm.h
    // 0 - disable extra memory allocation
    // 1 - allocate via memory pool ScanQuery
    // 2 - allocate via memory pool DataQuery
    ui32 ExtraMemoryAllocationPool = 0;

    bool FailOnUndelivery = true;
    bool UseSpilling = false;

    std::function<void(bool success, const TIssues& issues)> TerminateHandler;
    TMaybe<NDqProto::TRlPath> RlPath;

    i64 AsyncInputPushLimit = std::numeric_limits<i64>::max();

    inline bool CollectNone() const {
        return StatsMode <= NDqProto::DQ_STATS_MODE_NONE;
    }

    inline bool CollectBasic() const {
        return StatsMode >= NDqProto::DQ_STATS_MODE_BASIC;
    }

    inline bool CollectFull() const {
        return StatsMode >= NDqProto::DQ_STATS_MODE_FULL;
    }

    inline bool CollectProfile() const {
        return StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE;
    }

    inline TCollectStatsLevel GetCollectStatsLevel() const {
        return StatsModeToCollectStatsLevel(StatsMode);
    }
};

struct TGuaranteeQuotaManager : public IMemoryQuotaManager {

    TGuaranteeQuotaManager(ui64 limit, ui64 guarantee, ui64 step = 1_MB, ui64 quota = 0)
        : Limit(limit), Guarantee(guarantee), Step(step), Quota(quota) {
        Y_ABORT_UNLESS(Limit >= Guarantee);
        Y_ABORT_UNLESS(Limit >= Quota);
        Y_ABORT_UNLESS((Step ^ ~Step) + 1 == 0);
        MaxMemorySize = Limit;
    }

    bool AllocateQuota(ui64 memorySize) override {
        if (Quota + memorySize > Limit) {
            ui64 delta = Quota + memorySize - Limit;
            ui64 alignMask = Step - 1;
            delta = (delta + alignMask) & ~alignMask;

            if (!AllocateExtraQuota(delta)) {
                return false;
            }

            Limit += delta;
            if (MaxMemorySize < Limit) {
                MaxMemorySize = Limit;
            }
        }

        Quota += memorySize;
        return true;
    }

    void FreeQuota(ui64 memorySize) override {
        Y_ABORT_UNLESS(Quota >= memorySize);
        Quota -= memorySize;
        ui64 delta = Limit - std::max(Quota, Guarantee);
        if (delta >= Step) {
            ui64 alignMask = Step - 1;
            delta &= ~alignMask;
            FreeExtraQuota(delta);
            Limit -= delta;
        }
    }

    ui64 GetCurrentQuota() const override {
        return Quota;
    }

    ui64 GetMaxMemorySize() const override {
        return MaxMemorySize;
    };

    virtual bool AllocateExtraQuota(ui64) {
        return false;
    }

    virtual void FreeExtraQuota(ui64) {
    }

    ui64 Limit;     // current consumption (Quota + leftover from allocation chunk)
    ui64 Guarantee; // do not free memory below this value even if Quota == 0
    ui64 Step;      // allocation chunk size
    ui64 Quota;     // current value
    ui64 MaxMemorySize; // usage peak for statistics
};

struct TChainedQuotaManager : public TGuaranteeQuotaManager {

    TChainedQuotaManager(IMemoryQuotaManager::TPtr extraQuotaManager, ui64 limit, ui64 guarantee, ui64 step = 1_MB, ui64 quota = 0)
    : TGuaranteeQuotaManager(limit, guarantee, step, quota)
    , ExtraQuotaManager(extraQuotaManager) {
    }

    bool AllocateExtraQuota(ui64 memorySize) override {
        return ExtraQuotaManager->AllocateQuota(memorySize);
    }

    void FreeExtraQuota(ui64 memorySize) override {
        ExtraQuotaManager->FreeQuota(memorySize);
    }

    IMemoryQuotaManager::TPtr ExtraQuotaManager;
};

struct TComputeMemoryLimits {
    ui64 ChannelBufferSize = 0;
    ui64 MkqlLightProgramMemoryLimit = 0; // Limit for light program.
    ui64 MkqlHeavyProgramMemoryLimit = 0; // Limit for heavy program.
    ui64 MkqlProgramHardMemoryLimit = 0; // Limit that stops program execution if reached.

    ui64 MinMemAllocSize = 30_MB;
    ui64 MinMemFreeSize = 30_MB;
    ui64 OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

    IMemoryQuotaManager::TPtr MemoryQuotaManager;
};

using TTaskRunnerFactory = std::function<
    TIntrusivePtr<IDqTaskRunner>(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const TLogFunc& logFunc)
>;

void FillAsyncStats(NDqProto::TDqAsyncBufferStats& proto, TDqAsyncStats stats);

void FillTaskRunnerStats(ui64 taskId, ui32 stageId, const TTaskRunnerStatsBase& taskStats,
    NDqProto::TDqTaskStats* protoTask, TCollectStatsLevel level);

NActors::IActor* CreateDqComputeActor(const NActors::TActorId& executerId, const TTxId& txId, NDqProto::TDqTask* task,
    IDqAsyncIoFactory::TPtr asyncIoFactory,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    const TTaskRunnerFactory& taskRunnerFactory,
    ::NMonitoring::TDynamicCounterPtr taskCounters = nullptr);

} // namespace NDq
} // namespace NYql
