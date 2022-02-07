#include "task_runner_actor.h"

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>

#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <library/cpp/actors/core/hfunc.h>

using namespace NYql::NDqs;
using namespace NActors;

namespace NYql::NDq {

namespace NTaskRunnerActor {

namespace {
template<typename T>
TTaskRunnerActorSensors GetSensors(const T& t) {
    TTaskRunnerActorSensors result;
    for (const auto& m : t.GetMetric()) {
        result.push_back(
            {
                m.GetName(), m.GetSum(), m.GetMax(), m.GetMin(), m.GetAvg(), m.GetCount()
            });
    }
    return result;
}

template<typename T>
TTaskRunnerActorRusage GetRusage(const T& t) {
    TTaskRunnerActorRusage rusage = {
        t.GetRusage().GetUtime(),
        t.GetRusage().GetStime(),
        t.GetRusage().GetMajorPageFaults()
    };
    return rusage;
}

} // namespace

class TTaskRunnerActor
    : public TActor<TTaskRunnerActor>
    , public ITaskRunnerActor
{
public:
    static constexpr char ActorName[] = "YQL_DQ_TASK_RUNNER";

    TTaskRunnerActor(
        ITaskRunnerActor::ICallbacks* parent,
        const NTaskRunnerProxy::IProxyFactory::TPtr& factory,
        const ITaskRunnerInvoker::TPtr& invoker,
        const TString& traceId)
        : TActor<TTaskRunnerActor>(&TTaskRunnerActor::Handler)
        , Parent(parent)
        , TraceId(traceId)
        , Factory(factory)
        , Invoker(invoker)
        , Local(Invoker->IsLocal())
    { }

    ~TTaskRunnerActor() { }

    STRICT_STFUNC(Handler, {
        cFunc(NActors::TEvents::TEvPoison::EventType, TTaskRunnerActor::PassAway);
        HFunc(TEvTaskRunnerCreate, OnDqTask);
        HFunc(TEvContinueRun, OnContinueRun);
        HFunc(TEvPop, OnChannelPop);
        HFunc(TEvPush, OnChannelPush);
        HFunc(TEvSinkPop, OnSinkPop);
        HFunc(TEvSinkPopFinished, OnSinkPopFinished);
    })

private:
    void PassAway() override {
        if (TaskRunner) {
            Invoker->Invoke([taskRunner=std::move(TaskRunner)] () {
                taskRunner->Kill();
            });
            TaskRunner.Reset();
        }
        TActor<TTaskRunnerActor>::PassAway();
    }

    void OnContinueRun(TEvContinueRun::TPtr& ev, const TActorContext& ctx) {
        Run(ev, ctx);
    }

    void OnChannelPush(TEvPush::TPtr& ev, const NActors::TActorContext& ctx) {
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto hasData = ev->Get()->HasData;
        auto finish = ev->Get()->Finish;
        auto askFreeSpace = ev->Get()->AskFreeSpace;
        auto channelId = ev->Get()->ChannelId;
        auto cookie = ev->Cookie;
        auto data = ev->Get()->Data;
        Invoker->Invoke([hasData, selfId, cookie, askFreeSpace, finish, channelId, taskRunner=TaskRunner, data, actorSystem, replyTo] () mutable {
            try {
                ui64 freeSpace = 0;
                if (hasData) {
                    // auto guard = taskRunner->BindAllocator(); // only for local mode
                    taskRunner->GetInputChannel(channelId)->Push(std::move(data));
                    if (askFreeSpace) {
                        freeSpace = taskRunner->GetInputChannel(channelId)->GetFreeSpace();
                    }
                }
                if (finish) {
                    taskRunner->GetInputChannel(channelId)->Finish();
                }

                // run
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvContinueRun(channelId, freeSpace),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvError(CurrentExceptionMessage(), {status.ExitCode, status.Stderr}),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void SourcePush(
        ui64 cookie,
        ui64 index,
        NKikimr::NMiniKQL::TUnboxedValueVector&& batch,
        i64 space,
        bool finish) override
    {
        auto* actorSystem = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        auto selfId = SelfId();

        TVector<TString> strings;
        for (auto& row : batch) {
            strings.emplace_back(row.AsStringRef());
        }

        Invoker->Invoke([strings=std::move(strings),taskRunner=TaskRunner, actorSystem, selfId, cookie, parentId=ParentId, space, finish, index]() mutable {
            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                auto source = taskRunner->GetSource(index);
                (static_cast<NTaskRunnerProxy::IStringSource*>(source.Get()))->PushString(std::move(strings), space);
                if (finish) {
                    source->Finish();
                }
                actorSystem->Send(
                    new IEventHandle(
                        parentId,
                        selfId,
                        new TEvSourcePushFinished(index),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        parentId,
                        selfId,
                        new TEvError(CurrentExceptionMessage(), {status.ExitCode, status.Stderr}),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void OnChannelPop(TEvPop::TPtr& ev, const NActors::TActorContext& ctx) {
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto cookie = ev->Cookie;
        auto wasFinished = ev->Get()->WasFinished;
        auto toPop = ev->Get()->Size;
        Invoker->Invoke([cookie,selfId,channelId=ev->Get()->ChannelId, actorSystem, replyTo, wasFinished, toPop, taskRunner=TaskRunner]() {
            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                auto channel = taskRunner->GetOutputChannel(channelId);
                int maxChunks = std::numeric_limits<int>::max();
                bool changed = false;
                bool isFinished = false;
                i64 remain = toPop;
                ui32 dataSize = 0;
                bool hasData = true;

                if (remain == 0) {
                    // special case to WorkerActor
                    remain = 5<<20;
                    maxChunks = 1;
                }

                TVector<NDqProto::TData> chunks;
                NDqProto::TPopResponse response;
                for (;maxChunks && remain > 0 && !isFinished && hasData; maxChunks--, remain -= dataSize) {
                    NDqProto::TData data;
                    NDqProto::TPopResponse pop = channel->Pop(data, remain);

                    for (auto& metric : pop.GetMetric()) {
                        *response.AddMetric() = metric;
                    }

                    hasData = pop.GetResult();
                    dataSize = data.GetRaw().size();
                    isFinished = !hasData && channel->IsFinished();
                    response.SetResult(response.GetResult() || hasData);
                    changed = changed || hasData || (isFinished != wasFinished);

                    if (hasData) {
                        chunks.emplace_back(std::move(data));
                    }
                }

                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvChannelPopFinished(
                            channelId,
                            std::move(chunks),
                            isFinished,
                            changed,
                            GetSensors(response)),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvError(CurrentExceptionMessage(), {status.ExitCode, status.Stderr}),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void OnSinkPopFinished(TEvSinkPopFinished::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto guard = TaskRunner->BindAllocator();
        NKikimr::NMiniKQL::TUnboxedValueVector batch;
        for (auto& row: ev->Get()->Strings) {
            batch.emplace_back(NKikimr::NMiniKQL::MakeString(row));
        }
        Parent->SinkSend(
            ev->Get()->Index,
            std::move(batch),
            std::move(ev->Get()->Checkpoint),
            ev->Get()->CheckpointSize,
            ev->Get()->Size,
            ev->Get()->Finished,
            ev->Get()->Changed);
    }

    void OnSinkPop(TEvSinkPop::TPtr& ev, const NActors::TActorContext& ctx) {
        auto selfId = SelfId();
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;

        Invoker->Invoke([taskRunner=TaskRunner, selfId, actorSystem, ev=std::move(ev)] {
            auto cookie = ev->Cookie;
            auto replyTo = ev->Sender;

            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                auto sink = taskRunner->GetSink(ev->Get()->Index);
                TVector<TString> batch;
                NDqProto::TCheckpoint checkpoint;
                TMaybe<NDqProto::TCheckpoint> maybeCheckpoint;
                i64 size = 0;
                i64 checkpointSize = 0;
                if (ev->Get()->Size > 0) {
                    size = (static_cast<NTaskRunnerProxy::IStringSink*>(sink.Get()))->PopString(batch, ev->Get()->Size);
                }
                bool hasCheckpoint = sink->Pop(checkpoint);
                if (hasCheckpoint) {
                    checkpointSize = checkpoint.ByteSize();
                    maybeCheckpoint.ConstructInPlace(std::move(checkpoint));
                }
                auto finished = sink->IsFinished();
                bool changed = finished || ev->Get()->Size > 0 || hasCheckpoint;
                auto event = MakeHolder<TEvSinkPopFinished>(
                    ev->Get()->Index,
                    std::move(maybeCheckpoint), size, checkpointSize, finished, changed);
                event->Strings = std::move(batch);
                // repack data and forward
                actorSystem->Send(
                    new IEventHandle(
                        selfId,
                        replyTo,
                        event.Release(),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvError(CurrentExceptionMessage(), {status.ExitCode, status.Stderr}),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void OnDqTask(TEvTaskRunnerCreate::TPtr& ev, const NActors::TActorContext& ctx) {
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto cookie = ev->Cookie;
        auto taskId = ev->Get()->Task.GetId();
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
        ParentId = ev->Sender;

        try {
            TaskRunner = Factory->GetOld(ev->Get()->Task, TraceId);
        } catch (...) {
            TString message = "Could not create TaskRunner for " + ToString(taskId) + " on node " + ToString(replyTo.NodeId()) + ", error: " + CurrentExceptionMessage();
            Send(replyTo, new TEvError(message, /*retriable = */ true, /*fallback=*/ true), 0, cookie);
            return;
        }

        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        Invoker->Invoke([taskRunner=TaskRunner, replyTo, selfId, cookie, actorSystem](){
            try {
                //auto guard = taskRunner->BindAllocator(); // only for local mode
                auto result = taskRunner->Prepare();

                auto event = MakeHolder<TEvTaskRunnerCreateFinished>(
                    taskRunner->GetSecureParams(),
                    taskRunner->GetTaskParams(),
                    taskRunner->GetTypeEnv(),
                    taskRunner->GetHolderFactory(),
                    GetSensors(result));

                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        event.Release(),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(replyTo, selfId, new TEvError(CurrentExceptionMessage(), {status.ExitCode, status.Stderr}), 0, cookie));
            }
        });
    }

    void Run(TEvContinueRun::TPtr& ev, const TActorContext& ctx) {
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto cookie = ev->Cookie;
        auto inputMap = ev->Get()->AskFreeSpace
            ? Inputs
            : ev->Get()->InputChannels;

        auto sourcesMap = Sources;

        Invoker->Invoke([selfId, cookie, actorSystem, replyTo, taskRunner=TaskRunner, inputMap, sourcesMap, memLimit=ev->Get()->MemLimit]() mutable {
            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                // guard.GetMutex()->SetLimit(memLimit);
                auto response = taskRunner->Run();
                auto res = static_cast<NDq::ERunStatus>(response.GetResult());

                THashMap<ui32, ui64> inputChannelFreeSpace;
                THashMap<ui32, ui64> sourcesFreeSpace;
                if (res == ERunStatus::PendingInput) {
                    for (auto& channelId : inputMap) {
                        inputChannelFreeSpace[channelId] = taskRunner->GetInputChannel(channelId)->GetFreeSpace();
                    }

                    for (auto& index : sourcesMap) {
                        sourcesFreeSpace[index] = taskRunner->GetSource(index)->GetFreeSpace();
                    }
                }

                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvTaskRunFinished(
                            res,
                            std::move(inputChannelFreeSpace),
                            std::move(sourcesFreeSpace),
                            GetSensors(response),
                            GetRusage(response)),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvError(CurrentExceptionMessage(), {status.ExitCode, status.Stderr}),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    NActors::TActorId ParentId;
    ITaskRunnerActor::ICallbacks* Parent;
    TString TraceId;
    NTaskRunnerProxy::IProxyFactory::TPtr Factory;
    NTaskRunnerProxy::ITaskRunner::TPtr TaskRunner;
    ITaskRunnerInvoker::TPtr Invoker;
    bool Local;
    THashSet<ui32> Inputs;
    THashSet<ui32> Sources;
};

class TTaskRunnerActorFactory: public ITaskRunnerActorFactory {
public:
    TTaskRunnerActorFactory(
        const NTaskRunnerProxy::IProxyFactory::TPtr& proxyFactory,
        const NDqs::ITaskRunnerInvokerFactory::TPtr& invokerFactory)
        : ProxyFactory(proxyFactory)
        , InvokerFactory(invokerFactory)
    { }

    std::tuple<ITaskRunnerActor*, NActors::IActor*> Create(
        ITaskRunnerActor::ICallbacks* parent,
        const TString& traceId) override
    {
        auto* actor = new TTaskRunnerActor(parent, ProxyFactory, InvokerFactory->Create(), traceId);
        return std::make_tuple(
            static_cast<ITaskRunnerActor*>(actor),
            static_cast<NActors::IActor*>(actor)
            );
    }

private:
    NTaskRunnerProxy::IProxyFactory::TPtr ProxyFactory;
    NDqs::ITaskRunnerInvokerFactory::TPtr InvokerFactory;
};

ITaskRunnerActorFactory::TPtr CreateTaskRunnerActorFactory(
    const NTaskRunnerProxy::IProxyFactory::TPtr& proxyFactory,
    const NDqs::ITaskRunnerInvokerFactory::TPtr& invokerFactory)
{
    return ITaskRunnerActorFactory::TPtr(new TTaskRunnerActorFactory(proxyFactory, invokerFactory));
}

} // namespace NTaskRunnerActor

} // namespace NYql::NDq
