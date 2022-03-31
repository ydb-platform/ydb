#include "task_runner_actor.h"

#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

#include <ydb/library/yql/dq/actors/dq.h>

#include <ydb/library/yql/dq/actors/task_runner/task_runner_actor.h>

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <library/cpp/actors/core/hfunc.h>

using namespace NActors;

namespace NYql::NDq {

namespace NTaskRunnerActor {

class TLocalTaskRunnerActor
    : public TActor<TLocalTaskRunnerActor>
    , public ITaskRunnerActor
{
public:
    static constexpr char ActorName[] = "YQL_DQ_TASK_RUNNER";

    TLocalTaskRunnerActor(ITaskRunnerActor::ICallbacks* parent, const TTaskRunnerFactory& factory, const TString& traceId)
        : TActor<TLocalTaskRunnerActor>(&TLocalTaskRunnerActor::Handler)
        , Parent(parent)
        , Factory(factory)
        , TraceId(traceId)
    { }

    ~TLocalTaskRunnerActor()
    { }

    STRICT_STFUNC(Handler, {
        cFunc(NActors::TEvents::TEvPoison::EventType, TLocalTaskRunnerActor::PassAway);
        HFunc(TEvTaskRunnerCreate, OnDqTask);
        HFunc(TEvContinueRun, OnContinueRun);
        HFunc(TEvPop, OnChannelPop);
        HFunc(TEvPush, OnChannelPush);
        HFunc(TEvSinkPop, OnSinkPop);
    });

private:
    void PassAway() override {
        TActor<TLocalTaskRunnerActor>::PassAway();
    }

    void OnContinueRun(TEvContinueRun::TPtr& ev, const TActorContext& ctx) {
        auto guard = TaskRunner->BindAllocator();
        auto inputMap = ev->Get()->AskFreeSpace
            ? Inputs
            : ev->Get()->InputChannels;

        auto& sourcesMap = Sources;

        try {
            guard.GetMutex()->SetLimit(ev->Get()->MemLimit);
            auto res = TaskRunner->Run();

            THashMap<ui32, ui64> inputChannelFreeSpace;
            THashMap<ui32, ui64> sourcesFreeSpace;
            if (res == ERunStatus::PendingInput) {
                for (auto& channelId : inputMap) {
                    inputChannelFreeSpace[channelId] = TaskRunner->GetInputChannel(channelId)->GetFreeSpace();
                }

                for (auto& index : sourcesMap) {
                    sourcesFreeSpace[index] = TaskRunner->GetSource(index)->GetFreeSpace();
                }
            }

            ctx.Send(
                new IEventHandle(
                    ev->Sender,
                    SelfId(),
                    new TEvTaskRunFinished(
                        res,
                        std::move(inputChannelFreeSpace),
                        std::move(sourcesFreeSpace)),
                    /*flags=*/0,
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

    void OnChannelPush(TEvPush::TPtr& ev, const NActors::TActorContext& ctx) {
        auto guard = TaskRunner->BindAllocator();
        auto hasData = ev->Get()->HasData;
        auto finish = ev->Get()->Finish;
        auto askFreeSpace = ev->Get()->AskFreeSpace;
        auto channelId = ev->Get()->ChannelId;
        auto data = ev->Get()->Data;

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

        // run
        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
                new TEvContinueRun(channelId, freeSpace),
                /*flags=*/0,
                ev->Cookie));
    }

    void SourcePush(
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
                new TEvSourcePushFinished(index),
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
        for (;maxChunks && remain > 0 && !isFinished && hasData; maxChunks--, remain -= dataSize) {
            NDqProto::TData data;
            hasData = channel->Pop(data, remain);
            dataSize = data.GetRaw().size();
            isFinished = !hasData && channel->IsFinished();

            changed = changed || hasData || (isFinished != wasFinished);

            if (hasData) {
                chunks.emplace_back(std::move(data));
            }
        }

        ctx.Send(
            new IEventHandle(
                ev->Sender,
                SelfId(),
                new TEvChannelPopFinished(
                    channelId,
                    std::move(chunks),
                    isFinished,
                    changed,
                    {},
                    TDqTaskRunnerStatsView(TaskRunner->GetStats())),
                /*flags=*/0,
                ev->Cookie));
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

        auto guard = TaskRunner->BindAllocator();
        try {
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
        } catch (...) {
            ctx.Send(
                new IEventHandle(
                    ev->Sender,
                    SelfId(),
                    GetError(CurrentExceptionMessage()).Release(),
                    0,
                    ev->Cookie));
        }
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
};

struct TLocalTaskRunnerActorFactory: public ITaskRunnerActorFactory {
    TLocalTaskRunnerActorFactory(const TTaskRunnerFactory& factory)
        : Factory(factory)
    { }

    std::tuple<ITaskRunnerActor*, NActors::IActor*> Create(
        ITaskRunnerActor::ICallbacks* parent,
        const TString& traceId) override
    {
        auto* actor = new TLocalTaskRunnerActor(parent, Factory, traceId);
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
