#include "task_runner_actor.h"

#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/runtime/runtime_data.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>

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
        const TTxId& txId,
        ui64 taskId,
        TWorkerRuntimeData* runtimeData)
        : TActor<TTaskRunnerActor>(&TTaskRunnerActor::Handler)
        , Parent(parent)
        , TraceId(TStringBuilder() << txId)
        , TaskId(taskId)
        , Factory(factory)
        , Invoker(invoker)
        , Local(Invoker->IsLocal())
        , Settings(MakeIntrusive<TDqConfiguration>())
        , StageId(0)
        , RuntimeData(runtimeData)
        , ClusterName(RuntimeData ? RuntimeData->ClusterName : "local")
    {
        Y_UNUSED(TaskId);
        if (RuntimeData) {
            RuntimeData->OnWorkerStart(TraceId);
        }
    }

    ~TTaskRunnerActor()
    {
        if (RuntimeData) {
            RuntimeData->OnWorkerStop(TraceId);
        }
    }

    STRICT_STFUNC(Handler, {
        cFunc(NActors::TEvents::TEvPoison::EventType, TTaskRunnerActor::PassAway);
        hFunc(TEvTaskRunnerCreate, OnDqTask);
        hFunc(TEvContinueRun, OnContinueRun);
        hFunc(TEvPop, OnChannelPop);
        hFunc(TEvPush, OnChannelPush);
        hFunc(TEvSinkPop, OnSinkPop);
        hFunc(TEvSinkPopFinished, OnSinkPopFinished);
    })

private:
    static std::pair<NYql::NDqProto::StatusIds::StatusCode, TString> ParseStderr(const TString& input, TIntrusivePtr<TDqConfiguration> settings) {
        TString filteredStderr;
        THashSet<TString> fallbackOn;
        if (settings->_FallbackOnRuntimeErrors.Get()) {
            TString parts = settings->_FallbackOnRuntimeErrors.Get().GetOrElse("");
            for (const auto& it : StringSplitter(parts).Split(',')) {
                fallbackOn.insert(TString(it.Token()));
            }
        }

        bool fallback = false;
        bool retry = false;
        for (TStringBuf line: StringSplitter(input).SplitByString("\n")) {
            if (line.Contains("mlockall failed")) {
                // skip
            } else {
                if (!fallback) {
                    if (line.Contains("FindColumnInfo(): requirement memberType->GetKind() == TType::EKind::Data")) {
                    // YQL-14757: temporary workaround for part6/produce-reduce_lambda_list_table-default.txt
                        fallback = true;
                    } else if (line.Contains("embedded:Len")) {
                        // YQL-14763
                        fallback = true;
                    } else if (line.Contains("No such transaction")) {
                        // YQL-15542
                        fallback = true;
                    } else if (line.Contains("(NYT::TErrorException) Request timed out")) {
                        // RPC reader fallback to YT
                        fallback = true;
                    } else if (line.Contains("Transaction") && line.Contains("aborted")) {
                        // YQL-15542
                        fallback = true;
                    } else if (line.Contains("Container killed by OOM")) {
                        // temporary workaround for YQL-12066
                        fallback = true;
                    } else if (line.Contains("Expected data or optional of data, actual:")) {
                        // temporary workaround for YQL-12835
                        fallback = true;
                    } else if (line.Contains("Pattern nodes can not get computation node by index:")) {
                        // temporary workaround for YQL-12987
                        fallback = true;
                    } else if (line.Contains("contrib/libs/protobuf/src/google/protobuf/messagext.cc") && line.Contains("Message size") && line.Contains("exceeds")) {
                        // temporary workaround for YQL-12988
                        fallback = true;
                    } else if (line.Contains("Cannot start container")) {
                        // temporary workaround for YQL-14221
                        retry = true;
                        fallback = true;
                    } else if (line.Contains("Cannot execl")) {
                        // YQL-14099
                        retry = true;
                        fallback = true;
                    } else {
                        for (const auto& part : fallbackOn) {
                            if (line.Contains(part)) {
                                fallback = true;
                            }
                        }
                    }
                }

                filteredStderr += line;
                filteredStderr += "\n";
            }
        }
        auto status = NYql::NDqProto::StatusIds::BAD_REQUEST;  // no retries, no fallback, error though
        if (retry) {
            status = NYql::NDqProto::StatusIds::UNAVAILABLE;  // retries, fallback on retries limit
        } else if (fallback) {
            status = NYql::NDqProto::StatusIds::UNSUPPORTED;  // no retries, fallback immediately
        }
        return {status, filteredStderr};
    }

    static THolder<TEvDq::TEvAbortExecution> MakeError(
        const TEvError::TStatus& status,
        TIntrusivePtr<TDqConfiguration> settings,
        ui64 stageId,
        TString message = CurrentExceptionMessage()) {
        // stderr always affects retriable/fallback flags
        auto [queryStatus, filteredStderr] = ParseStderr(status.Stderr, settings);
        auto stderrStr = TStringBuilder{}
            << "ExitCode: " << status.ExitCode << "\n"
            << "StageId: " << stageId << "\n"
            << filteredStderr;
        auto issueCode = NCommon::NeedFallback(queryStatus)
            ? TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR
            : TIssuesIds::DQ_GATEWAY_ERROR;
        TIssue issue;
        if (status.ExitCode == 0) {
            // if exit code is 0, then problem lies in our code => passing message produced by it
            issue = TIssue(message).SetCode(issueCode, TSeverityIds::S_ERROR);
        } else {
            // if exit code is not 0, then problem is in their code => passing stderr
            issue = TIssue(stderrStr).SetCode(issueCode, TSeverityIds::S_ERROR);
            TStringBuf terminationMessage = stderrStr;
            auto parsedPos = TryParseTerminationMessage(terminationMessage);
            if (terminationMessage.size() < stderrStr.size()) {
                issue.AddSubIssue(MakeIntrusive<TIssue>(YqlIssue(parsedPos.GetOrElse(TPosition()), TIssuesIds::DQ_GATEWAY_ERROR, TString{terminationMessage})));
            }
        }
        Y_VERIFY(queryStatus != NYql::NDqProto::StatusIds::SUCCESS);
        return MakeHolder<NDq::TEvDq::TEvAbortExecution>(queryStatus, TVector<TIssue>{issue});
    }

    void PassAway() override {
        if (TaskRunner) {
            Invoker->Invoke([taskRunner=std::move(TaskRunner)] () {
                taskRunner->Kill();
            });
            TaskRunner.Reset();
        }
        TActor<TTaskRunnerActor>::PassAway();
    }

    void OnContinueRun(TEvContinueRun::TPtr& ev) {
        Run(ev);
    }

    void OnChannelPush(TEvPush::TPtr& ev) {
        auto* actorSystem = TActivationContext::ActorSystem();
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto hasData = ev->Get()->HasData;
        auto finish = ev->Get()->Finish;
        auto channelId = ev->Get()->ChannelId;
        auto cookie = ev->Cookie;
        auto data = ev->Get()->Data;
        Invoker->Invoke([hasData, selfId, cookie, finish, channelId, taskRunner=TaskRunner, data, actorSystem, replyTo, settings=Settings, stageId=StageId] () mutable {
            try {
                // todo:(whcrc) finish output channel?
                ui64 freeSpace = 0;
                if (hasData) {
                    // auto guard = taskRunner->BindAllocator(); // only for local mode
                    taskRunner->GetInputChannel(channelId)->Push(std::move(data));
                    freeSpace = taskRunner->GetInputChannel(channelId)->GetFreeSpace();
                }
                if (finish) {
                    taskRunner->GetInputChannel(channelId)->Finish();
                }

                // run
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvPushFinished(channelId, freeSpace),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        MakeError({status.ExitCode, status.Stderr}, settings, stageId).Release(),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void AsyncInputPush(
        ui64 cookie,
        ui64 index,
        NKikimr::NMiniKQL::TUnboxedValueBatch&& batch,
        i64 space,
        bool finish) override
    {
        auto* actorSystem = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        auto selfId = SelfId();

        TVector<TString> strings;
        YQL_ENSURE(!batch.IsWide());
        batch.ForEachRow([&strings](const auto& value) {
            strings.emplace_back(value.AsStringRef());
        });

        Invoker->Invoke([strings=std::move(strings),taskRunner=TaskRunner, actorSystem, selfId, cookie, parentId=ParentId, space, finish, index, settings=Settings, stageId=StageId]() mutable {
            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                auto source = taskRunner->GetSource(index);
                if (!strings.empty()) {
                    (static_cast<NTaskRunnerProxy::IStringSource*>(source.Get()))->PushString(std::move(strings), space);
                }
                if (finish) {
                    source->Finish();
                }
                actorSystem->Send(
                    new IEventHandle(
                        parentId,
                        selfId,
                        new TEvAsyncInputPushFinished(index, source->GetFreeSpace()),
                        /*flags=*/0,
                        cookie));
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        parentId,
                        selfId,
                        MakeError({status.ExitCode, status.Stderr}, settings, stageId).Release(),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void OnChannelPop(TEvPop::TPtr& ev) {
        auto* actorSystem = TActivationContext::ActorSystem();
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto cookie = ev->Cookie;
        auto wasFinished = ev->Get()->WasFinished;
        auto toPop = ev->Get()->Size;
        Invoker->Invoke([cookie,selfId,channelId=ev->Get()->ChannelId, actorSystem, replyTo, wasFinished, toPop, taskRunner=TaskRunner, settings=Settings, stageId=StageId]() {
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

                TVector<TDqSerializedBatch> chunks;
                NDqProto::TPopResponse response;
                for (;maxChunks && remain > 0 && !isFinished && hasData; maxChunks--, remain -= dataSize) {
                    TDqSerializedBatch data;
                    const auto lastPop = std::move(channel->Pop(data));

                    for (auto& metric : lastPop.GetMetric()) {
                        *response.AddMetric() = metric;
                    }

                    hasData = lastPop.GetResult();
                    dataSize = data.Size();
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
                            Nothing(),
                            Nothing(),
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
                        MakeError({status.ExitCode, status.Stderr}, settings, stageId).Release(),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void OnSinkPopFinished(TEvSinkPopFinished::TPtr& ev) {
        auto guard = TaskRunner->BindAllocator();
        NKikimr::NMiniKQL::TUnboxedValueBatch batch;
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

    void OnSinkPop(TEvSinkPop::TPtr& ev) {
        auto selfId = SelfId();
        auto* actorSystem = TActivationContext::ActorSystem();

        Invoker->Invoke([taskRunner=TaskRunner, selfId, actorSystem, ev=std::move(ev), settings=Settings, stageId=StageId] {
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
                        MakeError({status.ExitCode, status.Stderr}, settings, stageId).Release(),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    void OnDqTask(TEvTaskRunnerCreate::TPtr& ev) {
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto cookie = ev->Cookie;
        auto taskId = ev->Get()->Task.GetId();
        auto& inputs = ev->Get()->Task.GetInputs();
        auto startTime = TInstant::Now();

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
            NDq::TDqTaskSettings settings(&ev->Get()->Task);
            TaskRunner = Factory->GetOld(settings, TraceId);
        } catch (...) {
            TString message = "Could not create TaskRunner for " + ToString(taskId) + " on node " + ToString(replyTo.NodeId()) + ", error: " + CurrentExceptionMessage();
            Send(replyTo, MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::INTERNAL_ERROR, message), 0, cookie);
            return;
        }

        auto* actorSystem = TActivationContext::ActorSystem();
        {
            Yql::DqsProto::TTaskMeta taskMeta;
            ev->Get()->Task.GetMeta().UnpackTo(&taskMeta);
            Settings->Dispatch(taskMeta.GetSettings());
            Settings->FreezeDefaults();
            StageId = taskMeta.GetStageId();
        }
        Invoker->Invoke([taskRunner=TaskRunner, replyTo, selfId, cookie, actorSystem, settings=Settings, stageId=StageId, startTime, clusterName = ClusterName](){
            try {
                //auto guard = taskRunner->BindAllocator(); // only for local mode
                auto result = taskRunner->Prepare();
                auto sensors = GetSensors(result);
                auto sensorName = TCounters::GetCounterName(
                    "Actor",
                    {{"ClusterName", clusterName}},
                    "ProcessInit");
                i64 val = (TInstant::Now()-startTime).MilliSeconds();
                sensors.push_back({sensorName, val, val, val, val, 1});

                auto event = MakeHolder<TEvTaskRunnerCreateFinished>(
                    taskRunner->GetSecureParams(),
                    taskRunner->GetTaskParams(),
                    taskRunner->GetReadRanges(),
                    taskRunner->GetTypeEnv(),
                    taskRunner->GetHolderFactory(),
                    sensors);

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
                    new IEventHandle(replyTo, selfId, MakeError({status.ExitCode, status.Stderr}, settings, stageId).Release(), 0, cookie));
            }
        });
    }

    void Run(TEvContinueRun::TPtr& ev) {
        auto* actorSystem = TActivationContext::ActorSystem();
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto cookie = ev->Cookie;
        auto inputMap = ev->Get()->AskFreeSpace
            ? Inputs
            : ev->Get()->InputChannels;

        auto sourcesMap = Sources;

        Invoker->Invoke([selfId, cookie, actorSystem, replyTo, taskRunner=TaskRunner, inputMap, sourcesMap, memLimit=ev->Get()->MemLimit, settings=Settings, stageId=StageId, runtimeData=RuntimeData]() mutable {
            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                // guard.GetMutex()->SetLimit(memLimit);
                auto response = taskRunner->Run();
                auto res = static_cast<NDq::ERunStatus>(response.GetResult());

                THashMap<ui32, i64> inputChannelFreeSpace;
                THashMap<ui32, i64> sourcesFreeSpace;
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
                            GetSensors(response)),
                        /*flags=*/0,
                        cookie));

                if (runtimeData) {
                    ::TRusage delta;
                    delta.Stime = TDuration::MicroSeconds(response.GetRusage().GetStime());
                    delta.Utime = TDuration::MicroSeconds(response.GetRusage().GetUtime());
                    delta.MajorPageFaults = response.GetRusage().GetMajorPageFaults();
                    runtimeData->AddRusageDelta(delta);
                }
            } catch (...) {
                auto status = taskRunner->GetStatus();
                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        MakeError({status.ExitCode, status.Stderr}, settings, stageId).Release(),
                        /*flags=*/0,
                        cookie));
            }
        });
    }

    NActors::TActorId ParentId;
    ITaskRunnerActor::ICallbacks* Parent;
    const TString TraceId;
    const ui64 TaskId;
    NTaskRunnerProxy::IProxyFactory::TPtr Factory;
    NTaskRunnerProxy::ITaskRunner::TPtr TaskRunner;
    ITaskRunnerInvoker::TPtr Invoker;
    bool Local;
    THashSet<ui32> Inputs;
    THashSet<ui32> Sources;
    TIntrusivePtr<TDqConfiguration> Settings;
    ui64 StageId;
    TWorkerRuntimeData* RuntimeData;
    TString ClusterName;
};

class TTaskRunnerActorFactory: public ITaskRunnerActorFactory {
public:
    TTaskRunnerActorFactory(
        const NTaskRunnerProxy::IProxyFactory::TPtr& proxyFactory,
        const NDqs::ITaskRunnerInvokerFactory::TPtr& invokerFactory,
        TWorkerRuntimeData* runtimeData)
        : ProxyFactory(proxyFactory)
        , InvokerFactory(invokerFactory)
        , RuntimeData(runtimeData)
    { }

    std::tuple<ITaskRunnerActor*, NActors::IActor*> Create(
        ITaskRunnerActor::ICallbacks* parent,
        const TTxId& txId,
        ui64 taskId,
        THashSet<ui32>&&,
        THolder<NYql::NDq::TDqMemoryQuota>&&) override
    {
        auto* actor = new TTaskRunnerActor(parent, ProxyFactory, InvokerFactory->Create(), txId, taskId, RuntimeData);
        return std::make_tuple(
            static_cast<ITaskRunnerActor*>(actor),
            static_cast<NActors::IActor*>(actor)
            );
    }

private:
    NTaskRunnerProxy::IProxyFactory::TPtr ProxyFactory;
    NDqs::ITaskRunnerInvokerFactory::TPtr InvokerFactory;
    TWorkerRuntimeData* RuntimeData;
};

ITaskRunnerActorFactory::TPtr CreateTaskRunnerActorFactory(
    const NTaskRunnerProxy::IProxyFactory::TPtr& proxyFactory,
    const NDqs::ITaskRunnerInvokerFactory::TPtr& invokerFactory,
    TWorkerRuntimeData* runtimeData)
{
    return ITaskRunnerActorFactory::TPtr(new TTaskRunnerActorFactory(proxyFactory, invokerFactory, runtimeData));
}

} // namespace NTaskRunnerActor

} // namespace NYql::NDq
