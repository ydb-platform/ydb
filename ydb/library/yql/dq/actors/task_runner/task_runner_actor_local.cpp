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

using namespace NActors;

namespace NYql::NDq {

namespace NTaskRunnerActor {

class TLocalTaskRunnerActor
    : public TActor<TLocalTaskRunnerActor>
    , public ITaskRunnerActor
{
public:
    static constexpr char ActorName[] = "YQL_DQ_TASK_RUNNER";

    TLocalTaskRunnerActor(ITaskRunnerActor::ICallbacks* parent, const TTaskRunnerFactory& factory, const TString& traceId, THashSet<ui32>&& inputChannelsWithDisabledCheckpoints, THolder<NYql::NDq::TDqMemoryQuota>&& memoryQuota)
        : TActor<TLocalTaskRunnerActor>(&TLocalTaskRunnerActor::Handler)
        , Parent(parent)
        , Factory(factory)
        , TraceId(traceId)
        , InputChannelsWithDisabledCheckpoints(std::move(inputChannelsWithDisabledCheckpoints))
        , MemoryQuota(std::move(memoryQuota))
    { }

    ~TLocalTaskRunnerActor()
    { }

    STFUNC(Handler) {
        try {
            switch (ev->GetTypeRewrite()) {
                cFunc(NActors::TEvents::TEvPoison::EventType, TLocalTaskRunnerActor::PassAway);
                HFunc(TEvTaskRunnerCreate, OnDqTask);
                HFunc(TEvContinueRun, OnContinueRun);
                HFunc(TEvPop, OnChannelPop);
                HFunc(TEvPush, OnChannelPush);
                HFunc(TEvSinkPop, OnSinkPop);
                HFunc(TEvLoadTaskRunnerFromState, OnLoadTaskRunnerFromState);
                HFunc(TEvStatistics, OnStatisticsRequest);
                default: {
                    Y_VERIFY_DEBUG(false, "%s: unexpected message type 0x%08" PRIx32, __func__, ev->GetTypeRewrite());
                }
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            ctx.Send(
                new IEventHandle(
                    ev->Sender,
                    SelfId(),
                    GetError(e).Release(),
                    0,
                    ev->Cookie));
        } catch (...) {
            ctx.Send(
                new IEventHandle(
                    ev->Sender,
                    SelfId(),
                    GetError(CurrentExceptionMessage()).Release(),
                    /*flags=*/0,
                    ev->Cookie));
        }
    }

private:
    void OnStatisticsRequest(TEvStatistics::TPtr& ev, const TActorContext& ctx) {
        TaskRunner->UpdateStats();
        THashMap<ui32, const TDqAsyncOutputBufferStats*> sinkStats;
        for (const auto sinkId : ev->Get()->SinkIds) {
            sinkStats[sinkId] = TaskRunner->GetSink(sinkId)->GetStats();
        }
        ev->Get()->Stats = TDqTaskRunnerStatsView(TaskRunner->GetStats(), std::move(sinkStats));
        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
                ev->Release().Release(),
                /*flags=*/0,
                ev->Cookie));
    }

    void OnLoadTaskRunnerFromState(TEvLoadTaskRunnerFromState::TPtr& ev, const TActorContext& ctx) {
        TMaybe<TString> error = Nothing();
        try {
            auto guard = TaskRunner->BindAllocator();
            TaskRunner->Load(std::move(*ev->Get()->Blob));
        } catch (const std::exception& e) {
            error = e.what();
        }
        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
                new TEvLoadTaskRunnerFromStateDone(std::move(error)),
                /*flags=*/0,
                ev->Cookie));
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

    void DoContinueRun(TEvContinueRun::TPtr& ev, const TActorContext& ctx) {
        auto guard = TaskRunner->BindAllocator(MemoryQuota ? MemoryQuota->GetMkqlMemoryLimit() : ev->Get()->MemLimit);
        auto inputMap = ev->Get()->AskFreeSpace
            ? Inputs
            : ev->Get()->InputChannels;

        auto& sourcesMap = Sources;

        NYql::NDq::ERunStatus res = ERunStatus::Finished;
        THashMap<ui32, ui64> inputChannelFreeSpace;
        THashMap<ui32, ui64> sourcesFreeSpace;
        if (!ev->Get()->CheckpointOnly) {
            res = TaskRunner->Run();
        }
        if (res == ERunStatus::PendingInput) {
            for (auto& channelId : inputMap) {
                inputChannelFreeSpace[channelId] = TaskRunner->GetInputChannel(channelId)->GetFreeSpace();
            }

            for (auto& index : sourcesMap) {
                sourcesFreeSpace[index] = TaskRunner->GetSource(index)->GetFreeSpace();
            }
        }

        THolder<NDqProto::TMiniKqlProgramState> mkqlProgramState;
        if ((res == ERunStatus::PendingInput || res == ERunStatus::Finished) && ev->Get()->CheckpointRequest.Defined() && ReadyToCheckpoint()) {
            mkqlProgramState = MakeHolder<NDqProto::TMiniKqlProgramState>();
            try {
                auto guard = TaskRunner->BindAllocator();
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
                LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, Sprintf("Failed to save state: %s", e.what()));
                mkqlProgramState = nullptr;
            }
        }

        if (MemoryQuota) {
            MemoryQuota->TryShrinkMemory(guard.GetMutex());
        }

        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
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
                ev->Cookie));
    }

    void OnContinueRun(TEvContinueRun::TPtr& ev, const TActorContext& ctx) {
        DoContinueRun(ev, ctx);
    }

    void OnChannelPush(TEvPush::TPtr& ev, const NActors::TActorContext& ctx) {
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
        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
                new TEvContinueRun(channelId, freeSpace),
                /*flags=*/0,
                ev->Cookie));
    }

    void AsyncInputPush(
        ui64 cookie,
        ui64 index,
        NKikimr::NMiniKQL::TUnboxedValueVector&& batch,
        i64 space,
        bool finish) override
    {
        auto& ctx = NActors::TlsActivationContext;
        auto source = TaskRunner->GetSource(index);
        source->Push(std::move(batch), space);
        if (finish) {
            source->Finish();
        }
        ctx->Send(
            new IEventHandle(
                ParentId,
                SelfId(),
                new TEvAsyncInputPushFinished(index),
                /*flags=*/0,
                cookie));
    }

    void OnChannelPop(TEvPop::TPtr& ev, const NActors::TActorContext& ctx) {
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

        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
                new TEvChannelPopFinished(
                    channelId,
                    std::move(chunks),
                    std::move(checkpoint),
                    isFinished,
                    changed,
                    {},
                    TDqTaskRunnerStatsView(TaskRunner->GetStats())),
                /*flags=*/0,
                ev->Cookie));
    }

    void ResumeInputs() {
        for (const auto& inputId : Inputs) {
            TaskRunner->GetInputChannel(inputId)->Resume();
        }
    }

    void OnSinkPop(TEvSinkPop::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
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
        bool hasCheckpoint = sink->Pop(checkpoint);
        if (hasCheckpoint) {
            checkpointSize = checkpoint.ByteSize();
            maybeCheckpoint.ConstructInPlace(std::move(checkpoint));
            ResumeInputs();
        }
        auto finished = sink->IsFinished();
        bool changed = finished || ev->Get()->Size > 0 || hasCheckpoint;

        Parent->SinkSend(ev->Get()->Index, std::move(batch), std::move(maybeCheckpoint), checkpointSize, size, finished, changed);
    }

    void OnDqTask(TEvTaskRunnerCreate::TPtr& ev, const NActors::TActorContext& ctx) {
        ParentId = ev->Sender;
        TaskRunner = Factory(ev->Get()->Task, [](const TString& message) {
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, message);
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

        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
                event.Release(),
                /*flags=*/0,
                ev->Cookie));
    }

    THolder<TEvDq::TEvAbortExecution> GetError(const NKikimr::TMemoryLimitExceededException&) {
        const auto err = TStringBuilder() << "Mkql memory limit exceeded"
            << ", limit: " << (MemoryQuota ? MemoryQuota->GetMkqlMemoryLimit() : -1)
            << ", canAllocateExtraMemory: " << (MemoryQuota ? MemoryQuota->GetCanAllocateExtraMemory() : 0);
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, Sprintf("TMemoryLimitExceededException: %s", err.c_str()));
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
    TString TraceId;
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
        const TString& traceId,
        THashSet<ui32>&& inputChannelsWithDisabledCheckpoints,
        THolder<NYql::NDq::TDqMemoryQuota>&& memoryQuota) override
    {
        auto* actor = new TLocalTaskRunnerActor(parent, Factory, traceId, std::move(inputChannelsWithDisabledCheckpoints), std::move(memoryQuota));
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
