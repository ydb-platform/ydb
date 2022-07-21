#include "task_runner_actor.h"

#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

#include <ydb/library/yql/dq/actors/dq.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h>

#include <ydb/library/yql/dq/actors/task_runner/task_runner_actor.h>

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/queue.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "SelfId: " << SelfId() << ", TxId: " << TxId << ", task: " << TaskId << ". " << stream);
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "SelfId: " << SelfId() << ", TxId: " << TxId << ", task: " << TaskId << ". " << stream);
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "SelfId: " << SelfId() << ", TxId: " << TxId << ", task: " << TaskId << ". " << stream);

using namespace NActors;

namespace NYql::NDq {

namespace NTaskRunnerActor {

class TLocalTaskRunnerActor
    : public TActor<TLocalTaskRunnerActor>
    , public ITaskRunnerActor
{
public:
    static constexpr char ActorName[] = "YQL_DQ_TASK_RUNNER";

    TLocalTaskRunnerActor(ITaskRunnerActor::ICallbacks* parent, const TTaskRunnerFactory& factory, const TTxId& txId, ui64 taskId, THashSet<ui32>&& inputChannelsWithDisabledCheckpoints, THolder<NYql::NDq::TDqMemoryQuota>&& memoryQuota)
        : TActor<TLocalTaskRunnerActor>(&TLocalTaskRunnerActor::Handler)
        , Parent(parent)
        , Factory(factory)
        , TxId(txId)
        , TaskId(taskId)
        , InputChannelsWithDisabledCheckpoints(std::move(inputChannelsWithDisabledCheckpoints))
        , MemoryQuota(std::move(memoryQuota))
    { }

    ~TLocalTaskRunnerActor()
    { }

    STFUNC(Handler) {
        Y_UNUSED(ctx);
        try {
            switch (ev->GetTypeRewrite()) {
                cFunc(NActors::TEvents::TEvPoison::EventType, TLocalTaskRunnerActor::PassAway);
                hFunc(TEvTaskRunnerCreate, OnDqTask);
                hFunc(TEvContinueRun, OnContinueRun);
                hFunc(TEvPop, OnChannelPop);
                hFunc(TEvPush, OnChannelPush);
                hFunc(TEvSinkPop, OnSinkPop);
                hFunc(TEvLoadTaskRunnerFromState, OnLoadTaskRunnerFromState);
                hFunc(TEvStatistics, OnStatisticsRequest);
                default: {
                    Y_VERIFY_DEBUG(false, "%s: unexpected message type 0x%08" PRIx32, __func__, ev->GetTypeRewrite());
                }
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            Send(
                ev->Sender,
                GetError(e).Release(),
                0,
                ev->Cookie);
        } catch (...) {
            Send(
                ev->Sender,
                GetError(CurrentExceptionMessage()).Release(),
                /*flags=*/0,
                ev->Cookie);
        }
    }

private:
    void OnStatisticsRequest(TEvStatistics::TPtr& ev) {
        TaskRunner->UpdateStats();
        THashMap<ui32, const TDqAsyncOutputBufferStats*> sinkStats;
        for (const auto sinkId : ev->Get()->SinkIds) {
            sinkStats[sinkId] = TaskRunner->GetSink(sinkId)->GetStats();
        }
        ev->Get()->Stats = TDqTaskRunnerStatsView(TaskRunner->GetStats(), std::move(sinkStats));
        Send(
            ev->Sender,
            ev->Release().Release(),
            /*flags=*/0,
            ev->Cookie);
    }

    void OnLoadTaskRunnerFromState(TEvLoadTaskRunnerFromState::TPtr& ev) {
        TMaybe<TString> error = Nothing();
        try {
            auto guard = TaskRunner->BindAllocator();
            TaskRunner->Load(std::move(*ev->Get()->Blob));
        } catch (const std::exception& e) {
            error = e.what();
        }
        Send(
            ev->Sender,
            new TEvLoadTaskRunnerFromStateDone(std::move(error)),
            /*flags=*/0,
            ev->Cookie);
    }

    void PassAway() override {
        if (MemoryQuota) {
            MemoryQuota->TryReleaseQuota();
        }
        TActor<TLocalTaskRunnerActor>::PassAway();
    }

    bool ReadyToCheckpoint() {
        for (const auto inputChannelId: Inputs) {
            if (InputChannelsWithDisabledCheckpoints.contains(inputChannelId)) {
                continue;
            }

            const auto input = TaskRunner->GetInputChannel(inputChannelId);
            if (!input->IsPaused()) {
                return false;
            }
            if (!input->Empty()) {
                return false;
            }
        }
        return true;
    }

    void DoContinueRun(TEvContinueRun::TPtr& ev) {
        auto guard = TaskRunner->BindAllocator(MemoryQuota ? MemoryQuota->GetMkqlMemoryLimit() : ev->Get()->MemLimit);
        auto inputMap = ev->Get()->AskFreeSpace
            ? Inputs
            : ev->Get()->InputChannels;

        NYql::NDq::ERunStatus res = ERunStatus::Finished;
        THashMap<ui32, ui64> inputChannelFreeSpace;
        THashMap<ui32, ui64> sourcesFreeSpace;
        if (!ev->Get()->CheckpointOnly) {
            res = TaskRunner->Run();
            LOG_D("Resume execution, run status: " << res);
        }
        if (res == ERunStatus::PendingInput) {
            for (auto& channelId : inputMap) {
                inputChannelFreeSpace[channelId] = TaskRunner->GetInputChannel(channelId)->GetFreeSpace();
            }

            for (auto& index : Sources) {
                sourcesFreeSpace[index] = TaskRunner->GetSource(index)->GetFreeSpace();
            }
        }

        THolder<NDqProto::TMiniKqlProgramState> mkqlProgramState;
        if ((res == ERunStatus::PendingInput || res == ERunStatus::Finished) && ev->Get()->CheckpointRequest.Defined() && ReadyToCheckpoint()) {
            mkqlProgramState = MakeHolder<NDqProto::TMiniKqlProgramState>();
            try {
                mkqlProgramState->SetRuntimeVersion(NDqProto::RUNTIME_VERSION_YQL_1_0);
                NDqProto::TStateData::TData& data = *mkqlProgramState->MutableData()->MutableStateData();
                data.SetVersion(TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion);
                data.SetBlob(TaskRunner->Save());
                // inject barriers
                // todo:(whcrc) barriers are injected even if source state save failed
                for (const auto& channelId : ev->Get()->CheckpointRequest->ChannelIds) {
                    TaskRunner->GetOutputChannel(channelId)->Push(NDqProto::TCheckpoint(ev->Get()->CheckpointRequest->Checkpoint));
                }
                for (const auto& sinkId : ev->Get()->CheckpointRequest->SinkIds) {
                    TaskRunner->GetSink(sinkId)->Push(NDqProto::TCheckpoint(ev->Get()->CheckpointRequest->Checkpoint));
                }
            } catch (const std::exception& e) {
                LOG_E("Failed to save state: " << e.what());
                mkqlProgramState = nullptr;
            }
        }

        if (MemoryQuota) {
            MemoryQuota->TryShrinkMemory(guard.GetMutex());
        }

        Send(
            ev->Sender,
            new TEvTaskRunFinished(
                res,
                std::move(inputChannelFreeSpace),
                std::move(sourcesFreeSpace),
                {},
                MemoryQuota ? *MemoryQuota->GetProfileStats() : TDqMemoryQuota::TProfileStats(),
                MemoryQuota ? MemoryQuota->GetMkqlMemoryLimit() : 0,
                std::move(mkqlProgramState),
                ev->Get()->CheckpointRequest.Defined()),
            /*flags=*/0,
            ev->Cookie);
    }

    void OnContinueRun(TEvContinueRun::TPtr& ev) {
        DoContinueRun(ev);
    }

    void OnChannelPush(TEvPush::TPtr& ev) {
        auto guard = TaskRunner->BindAllocator();
        auto hasData = ev->Get()->HasData;
        auto finish = ev->Get()->Finish;
        auto askFreeSpace = ev->Get()->AskFreeSpace;
        auto channelId = ev->Get()->ChannelId;
        auto data = ev->Get()->Data;
        if (ev->Get()->IsOut) {
            Y_VERIFY(ev->Get()->Finish, "dont know what to do with the output channel");
            TaskRunner->GetOutputChannel(channelId)->Finish();
            return;
        }
        ui64 freeSpace = 0;
        if (hasData) {
            TaskRunner->GetInputChannel(channelId)->Push(std::move(data));
            if (askFreeSpace) {
                freeSpace = TaskRunner->GetInputChannel(channelId)->GetFreeSpace();
            }
        }
        if (finish) {
            TaskRunner->GetInputChannel(channelId)->Finish();
        }
        if (ev->Get()->PauseAfterPush) {
            TaskRunner->GetInputChannel(channelId)->Pause();
        }

        // run
        Send(
            ev->Sender,
            new TEvContinueRun(channelId, freeSpace),
            /*flags=*/0,
            ev->Cookie);
    }

    void AsyncInputPush(
        ui64 cookie,
        ui64 index,
        NKikimr::NMiniKQL::TUnboxedValueVector&& batch,
        i64 space,
        bool finish) override
    {
        auto source = TaskRunner->GetSource(index);
        source->Push(std::move(batch), space);
        if (finish) {
            source->Finish();
        }
        Send(
            ParentId,
            new TEvAsyncInputPushFinished(index),
            /*flags=*/0,
            cookie);
    }

    void OnChannelPop(TEvPop::TPtr& ev) {
        auto guard = TaskRunner->BindAllocator();

        auto channelId = ev->Get()->ChannelId;
        auto channel = TaskRunner->GetOutputChannel(channelId);
        int maxChunks = std::numeric_limits<int>::max();
        auto wasFinished = ev->Get()->WasFinished;
        bool changed = false;
        bool isFinished = false;
        i64 remain = ev->Get()->Size;
        ui32 dataSize = 0;
        bool hasData = true;

        if (remain == 0) {
            // special case to WorkerActor
            remain = 5<<20;
            maxChunks = 1;
        }

        TVector<NDqProto::TData> chunks;
        TMaybe<NDqProto::TCheckpoint> checkpoint = Nothing();
        for (;maxChunks && remain > 0 && !isFinished && hasData; maxChunks--, remain -= dataSize) {
            NDqProto::TData data;
            hasData = channel->Pop(data, remain);
            NDqProto::TCheckpoint poppedCheckpoint;
            bool hasCheckpoint = channel->Pop(poppedCheckpoint);
            dataSize = data.GetRaw().size();
            isFinished = !hasData && channel->IsFinished();

            changed = changed || hasData || hasCheckpoint || (isFinished != wasFinished);

            if (hasData) {
                chunks.emplace_back(std::move(data));
            }
            if (hasCheckpoint) {
                checkpoint = std::move(poppedCheckpoint);
                ResumeInputs();
                break;
            }
        }

        Send(
            ev->Sender,
            new TEvChannelPopFinished(
                channelId,
                std::move(chunks),
                std::move(checkpoint),
                isFinished,
                changed,
                {},
                TDqTaskRunnerStatsView(TaskRunner->GetStats())),
            /*flags=*/0,
            ev->Cookie);
    }

    void ResumeInputs() {
        for (const auto& inputId : Inputs) {
            TaskRunner->GetInputChannel(inputId)->Resume();
        }
    }

    void OnSinkPop(TEvSinkPop::TPtr& ev) {
        auto guard = TaskRunner->BindAllocator();
        auto sink = TaskRunner->GetSink(ev->Get()->Index);

        NKikimr::NMiniKQL::TUnboxedValueVector batch;
        NDqProto::TCheckpoint checkpoint;
        TMaybe<NDqProto::TCheckpoint> maybeCheckpoint;
        i64 size = 0;
        i64 checkpointSize = 0;

        if (ev->Get()->Size > 0) {
            size = sink->Pop(batch, ev->Get()->Size);
        }
        const bool hasCheckpoint = sink->Pop(checkpoint);
        if (hasCheckpoint) {
            checkpointSize = checkpoint.ByteSize();
            maybeCheckpoint.ConstructInPlace(std::move(checkpoint));
            ResumeInputs();
        }
        const bool finished = sink->IsFinished();
        const bool changed = finished || size > 0 || hasCheckpoint;

        Parent->SinkSend(ev->Get()->Index, std::move(batch), std::move(maybeCheckpoint), checkpointSize, size, finished, changed);
    }

    void OnDqTask(TEvTaskRunnerCreate::TPtr& ev) {
        ParentId = ev->Sender;
        TaskRunner = Factory(ev->Get()->Task, [this](const TString& message) {
            LOG_D(message);
        });

        auto& inputs = ev->Get()->Task.GetInputs();
        for (auto inputId = 0; inputId < inputs.size(); inputId++) {
            auto& input = inputs[inputId];
            if (input.HasSource()) {
                Sources.emplace(inputId);
            } else {
                for (auto& channel : input.GetChannels()) {
                    Inputs.emplace(channel.GetId());
                }
            }
        }

        auto guard = TaskRunner->BindAllocator(MemoryQuota ? TMaybe<ui64>(MemoryQuota->GetMkqlMemoryLimit()) : Nothing());
        if (MemoryQuota) {
            MemoryQuota->TrySetIncreaseMemoryLimitCallback(guard.GetMutex());
        }

        TaskRunner->Prepare(ev->Get()->Task, ev->Get()->MemoryLimits, *ev->Get()->ExecCtx, ev->Get()->ParameterProvider);

        auto event = MakeHolder<TEvTaskRunnerCreateFinished>(
            TaskRunner->GetSecureParams(),
            TaskRunner->GetTaskParams(),
            TaskRunner->GetTypeEnv(),
            TaskRunner->GetHolderFactory());

        Send(
            ev->Sender,
            event.Release(),
            /*flags=*/0,
            ev->Cookie);
    }

    THolder<TEvDq::TEvAbortExecution> GetError(const NKikimr::TMemoryLimitExceededException&) {
        const TString err = TStringBuilder() << "Mkql memory limit exceeded"
            << ", limit: " << (MemoryQuota ? MemoryQuota->GetMkqlMemoryLimit() : -1)
            << ", canAllocateExtraMemory: " << (MemoryQuota ? MemoryQuota->GetCanAllocateExtraMemory() : 0);
        LOG_E("TMemoryLimitExceededException: " << err);
        TIssue issue(err);
        SetIssueCode(TIssuesIds::KIKIMR_PRECONDITION_FAILED, issue);
        return MakeHolder<TEvDq::TEvAbortExecution>(NYql::NDqProto::StatusIds::OVERLOADED, TVector<TIssue>{issue});
    }

    THolder<TEvDq::TEvAbortExecution> GetError(const TString& message) {
        return MakeHolder<TEvDq::TEvAbortExecution>(NYql::NDqProto::StatusIds::BAD_REQUEST, TVector<TIssue>{TIssue(message).SetCode(TIssuesIds::DQ_GATEWAY_ERROR, TSeverityIds::S_ERROR)});
    }

    NActors::TActorId ParentId;
    ITaskRunnerActor::ICallbacks* Parent;
    TTaskRunnerFactory Factory;
    const TTxId TxId;
    const ui64 TaskId;
    THashSet<ui32> Inputs;
    THashSet<ui32> Sources;
    TIntrusivePtr<NDq::IDqTaskRunner> TaskRunner;
    THashSet<ui32> InputChannelsWithDisabledCheckpoints;
    THolder<TDqMemoryQuota> MemoryQuota;
};

struct TLocalTaskRunnerActorFactory: public ITaskRunnerActorFactory {
    TLocalTaskRunnerActorFactory(const TTaskRunnerFactory& factory)
        : Factory(factory)
    { }

    std::tuple<ITaskRunnerActor*, NActors::IActor*> Create(
        ITaskRunnerActor::ICallbacks* parent,
        const TTxId& txId,
        ui64 taskId,
        THashSet<ui32>&& inputChannelsWithDisabledCheckpoints,
        THolder<NYql::NDq::TDqMemoryQuota>&& memoryQuota) override
    {
        auto* actor = new TLocalTaskRunnerActor(parent, Factory, txId, taskId, std::move(inputChannelsWithDisabledCheckpoints), std::move(memoryQuota));
        return std::make_tuple(
            static_cast<ITaskRunnerActor*>(actor),
            static_cast<NActors::IActor*>(actor)
            );
    }

    TTaskRunnerFactory Factory;
};

ITaskRunnerActorFactory::TPtr CreateLocalTaskRunnerActorFactory(const TTaskRunnerFactory& factory)
{
    return ITaskRunnerActorFactory::TPtr(new TLocalTaskRunnerActorFactory(factory));
}

} // namespace NTaskRunnerActor

} // namespace NYql::NDq
