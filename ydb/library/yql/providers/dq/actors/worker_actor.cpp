#include "worker_actor.h"

#include <ydb/library/yql/providers/dq/task_runner_actor/task_runner_actor.h>
#include <ydb/library/yql/providers/dq/runtime/runtime_data.h>

#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/utils/actor_log/log.h> 
#include <ydb/library/yql/utils/log/log.h> 

#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/stream/output.h>

using namespace NYql::NDq;
using namespace NYql::NDq::NTaskRunnerActor;
using namespace NYql::NDqProto;
using namespace NActors;

namespace NYql::NDqs {

struct TInputChannel {
    bool Finished{false};
    bool Requested{false};
    ui32 InputId = 0;
    ui32 ChannelId = 0;
    NActors::TActorId ActorID;
    int Retries = 0;
    TInstant RequestTime;
    TInstant ResponseTime;

    bool PingRequested{false};
    TInstant PingStartTime;
};

struct TOutputChannel {
    ui32 OutputId = 0;
    ui32 ChannelId = 0;
    NActors::TActorId ActorID;
    bool Finished = false;
    TInstant RequestTime = TInstant();
};

struct TSourceInfo {
    IDqSourceActor* SourceActor = nullptr;
    NActors::IActor* Actor = nullptr;
    i64 FreeSpace = 1;
    bool HasData = false;
    bool PushStarted = false;
    bool Finished = false;
    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
};

struct TSinkInfo {
    IDqSinkActor* SinkActor = nullptr;
    NActors::IActor* Actor = nullptr;
    bool Finished = false;
    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
};

class TDqWorker: public TRichActor<TDqWorker>
               , IDqSourceActor::ICallbacks
               , IDqSinkActor::ICallbacks
               , ITaskRunnerActor::ICallbacks
{
    static constexpr ui32 INPUT_SIZE = 100000;

public:
    static constexpr char ActorName[] = "YQL_DQ_WORKER";

    explicit TDqWorker(
        const ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
        const IDqSourceActorFactory::TPtr& sourceActorFactory,
        const IDqSinkActorFactory::TPtr& sinkActorFactory,
        TWorkerRuntimeData* runtimeData,
        const TString& traceId)
        : TRichActor<TDqWorker>(&TDqWorker::Handler)
        , SourceActorFactory(sourceActorFactory)
        , SinkActorFactory(sinkActorFactory)
        , TaskRunnerActorFactory(taskRunnerActorFactory)
        , RuntimeData(runtimeData)
        , TraceId(traceId)
    {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << "TDqWorker created ";

        if (RuntimeData) {
            RuntimeData->OnWorkerStart(TraceId);
        }
    }

    ~TDqWorker()
    {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << "TDqWorker destroyed ";

        if (RuntimeData) {
            RuntimeData->OnWorkerStop(TraceId);
        }
    }

    void DoPassAway() override {
        YQL_LOG_CTX_SCOPE(TraceId);
        for (const auto& inputs : InputMap) {
            Send(inputs.first, new NActors::TEvents::TEvPoison());
        }

        YQL_LOG(DEBUG) << "TDqWorker passed away ";
        if (Actor) {
            Actor->PassAway();
        }
        for (const auto& [_, v] : SourcesMap) {
            v.SourceActor->PassAway();
        }
        for (const auto& [_, v] : SinksMap) {
            v.SinkActor->PassAway();
        }
        Dump();
    }

private:
    STRICT_STFUNC(Handler, {
        HFunc(TEvDqTask, OnDqTask);
        HFunc(TEvPullDataRequest, OnPullRequest);
        HFunc(TEvPullDataResponse, OnPullResponse);
        HFunc(TEvPingRequest, OnPingRequest);
        HFunc(TEvPingResponse, OnPingResponse);
        HFunc(TEvents::TEvUndelivered, OnUndelivered);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvTaskRunnerCreateFinished, OnTaskRunnerCreated);
        HFunc(TEvChannelPopFinished, OnChannelPopFinished);
        HFunc(TEvTaskRunFinished, OnRunFinished);
        HFunc(TEvSourcePushFinished, OnSourcePushFinished);

        HFunc(TEvError, OnErrorFromPipe);
        HFunc(TEvContinueRun, OnContinueRun);
        cFunc(TEvents::TEvWakeup::EventType, OnWakeup);
    })

    TString ParseStatus(const TString& input, bool* retriableFlag, bool* fallbackFlag) {
        TString result;
        for (TStringBuf line: StringSplitter(input).SplitByString("\n").SkipEmpty()) {
            if (line.StartsWith("Counter1:")) {
                TVector<TString> parts;
                Split(TString(line), " ", parts);
                if (parts.size() >= 3) {
                    auto name = parts[1];
                    i64 value;
                    if (TryFromString<i64>(parts[2], value)) {
                        Stat.AddCounter(name, TDuration::MilliSeconds(value));
                    }
                }
            } else if (line.StartsWith("Counter:")) {
                TVector<TString> parts;
                Split(TString(line), " ", parts);
                // name sum min max avg count
                if (parts.size() >= 7) {
                    auto name = parts[1];
                    TCounters::TEntry entry;
                    if (
                        TryFromString<i64>(parts[2], entry.Sum) &&
                        TryFromString<i64>(parts[3], entry.Min) &&
                        TryFromString<i64>(parts[4], entry.Max) &&
                        TryFromString<i64>(parts[5], entry.Avg) &&
                        TryFromString<i64>(parts[6], entry.Count))
                    {
                        Stat.AddCounter(name, entry);
                    }
                }
            } else if (line.Contains("mlockall failed")) {
                // skip
            } else {
                if (fallbackFlag && !*fallbackFlag) {
                    if (line.Contains("FindColumnInfo(): requirement memberType->GetKind() == TType::EKind::Data")) {
                    // temporary workaround for part6/produce-reduce_lambda_list_table-default.txt
                        *fallbackFlag = true;
                    } else if (line.Contains("Unsupported builtin function:")) {
                        // temporary workaround for YQL-11791
                        *fallbackFlag = true;
                    } else if (line.Contains("embedded:Len")) {
                        *fallbackFlag = true;
                    } else if (line.Contains("Container killed by OOM")) {
                        // temporary workaround for YQL-12066
                        *fallbackFlag = true;
                    } else if (line.Contains("Expected data or optional of data, actual:")) {
                        // temporary workaround for YQL-12835
                        *fallbackFlag = true;
                    } else if (line.Contains("Cannot create Skiff writer for ")) {
                        // temporary workaround for YQL-12986
                        *fallbackFlag = true;
                    } else if (line.Contains("Skiff format expected")) {
                        // temporary workaround for YQL-12986
                        *fallbackFlag = true;
                    } else if (line.Contains("Pattern nodes can not get computation node by index:")) {
                        // temporary workaround for YQL-12987
                        *fallbackFlag = true;
                    } else if (line.Contains("contrib/libs/protobuf/src/google/protobuf/messagext.cc") && line.Contains("Message size") && line.Contains("exceeds")) {
                        // temporary workaround for YQL-12988
                        *fallbackFlag = true;
                    } else if (line.Contains("Cannot start container")) {
                        // temporary workaround for YQL-14221
                        *retriableFlag = true;
                        *fallbackFlag = true;
                    } else if (line.Contains("Cannot execl")) {
                        // YQL-14099
                        *retriableFlag = true;
                        *fallbackFlag = true;
                    } else {
                        for (const auto& part : FallbackOn) {
                            if (line.Contains(part)) {
                                *fallbackFlag = true;
                            }
                        }
                    }
                }

                result += line;
                result += "\n";
            }
        }
        return result;
    }

    void OnError(TString message = "", bool retriable = false, bool fallback = false, TMaybe<TEvError::TStatus> status = Nothing()) {
        if (!Executer) {
            // Posible Error on Undelivered before OnDqTask
            YQL_LOG(ERROR) << "Error " << message;
            return;
        }

        int exitCode = -1;
        TString stderrStr;

        if (status) {
            exitCode = status->ExitCode;
            stderrStr = TStringBuilder{}
                << "ExitCode: " << exitCode << "\n"
                << "StageId: " << StageId << "\n"
                << ParseStatus(status->Stderr, &retriable, &fallback);
        }

        if (message.Empty() && (status.Empty() || exitCode == 0)) {
            message = CurrentExceptionMessage();
        }

        auto issueCode = fallback
            ? TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR
            : TIssuesIds::DQ_GATEWAY_ERROR;

        TIssue issue = TIssue(message).SetCode(issueCode, TSeverityIds::S_ERROR);
        if (status && exitCode != 0) {
            issue = TIssue(stderrStr).SetCode(issueCode, TSeverityIds::S_ERROR);
            TStringBuf message = stderrStr;
            auto parsedPos = TryParseTerminationMessage(message);
            if (message.size() < stderrStr.size()) {
                issue.AddSubIssue(
                    MakeIntrusive<TIssue>(
                        YqlIssue(parsedPos.GetOrElse(TPosition()),
                                 TIssuesIds::DQ_GATEWAY_ERROR,
                                 TString{message})));
            }
        }

        auto req = MakeHolder<TEvDqFailure>(issue, retriable, fallback);
        Stat.FlushCounters(req->Record);
        Send(Executer, req.Release());
    }

    void OnErrorFromPipe(TEvError::TPtr& ev, const TActorContext& )
    {
        OnError(ev->Get()->Message, ev->Get()->Retriable, ev->Get()->Fallback, ev->Get()->Status);
    }

    void OnContinueRun(TEvContinueRun::TPtr&, const TActorContext& ctx) {
        Run(ctx);
    }

    void OnDqTask(TEvDqTask::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << "TDqWorker::OnDqTask";

        TFailureInjector::Reach("dq_task_failure", [] {::_exit(1); });

        Y_VERIFY(!TaskRunnerActor);

        Stat.StartCounter(Stat.GetCounterName("Actor", {{"ClusterName", RuntimeData ? RuntimeData->ClusterName : "local"}}, "ProcessInit"));
        Y_VERIFY(!Executer);
        Executer = ev->Sender;
        Task = ev->Get()->Record.GetTask();

        Yql::DqsProto::TTaskMeta taskMeta;
        Task.GetMeta().UnpackTo(&taskMeta);
        for (const auto& w : taskMeta.GetWorkerInfo()) {
            AllWorkers.push_back(w);
        }
        StageId = taskMeta.GetStageId();
        Settings->Dispatch(taskMeta.GetSettings());
        Settings->FreezeDefaults();
        PullRequestTimeout = TDuration::MilliSeconds(Settings->PullRequestTimeoutMs.Get().GetOrElse(0));
        PingTimeout = TDuration::MilliSeconds(Settings->PingTimeoutMs.Get().GetOrElse(0));
        if (PingTimeout) {
            PingPeriod = Max(PingTimeout/4, TDuration::MilliSeconds(1000));
        }
        if (Settings->_FallbackOnRuntimeErrors.Get()) {
            TString parts = Settings->_FallbackOnRuntimeErrors.Get().GetOrElse("");
            for (const auto& it : StringSplitter(parts).Split(',')) {
                FallbackOn.insert(TString(it.Token()));
            }
        }

        NActors::IActor* actor;
        std::tie(Actor, actor) = TaskRunnerActorFactory->Create(this, TraceId);
        TaskRunnerActor = RegisterLocalChild(actor);
        TDqTaskRunnerMemoryLimits limits; // used for local mode only
        limits.ChannelBufferSize = 20_MB;
        limits.OutputChunkMaxSize = 2_MB;
        Send(TaskRunnerActor, new TEvTaskRunnerCreate(std::move(ev->Get()->Record.GetTask()), limits));
    }

    void OnTaskRunnerCreated(TEvTaskRunnerCreateFinished::TPtr& ev, const TActorContext& ) {
        Stat.FlushCounter(Stat.GetCounterName("Actor", {{"ClusterName", RuntimeData ? RuntimeData->ClusterName : "local"}}, "ProcessInit"));
        TaskRunnerPrepared = true;

        try {
            Stat.AddCounters2(ev->Get()->Sensors);

            const auto& secureParams = ev->Get()->SecureParams;
            const auto& taskParams = ev->Get()->TaskParams;
            const auto& typeEnv = ev->Get()->TypeEnv;
            const auto& holderFactory = ev->Get()->HolderFactory;

            Stat.Measure<void>("PrepareChannels", [&](){
                auto& inputs = Task.GetInputs();
                for (auto inputId = 0; inputId < inputs.size(); inputId++) {
                    auto& input = inputs[inputId];
                    if (input.HasSource()) {
                        auto& source = SourcesMap[inputId];
                        source.TypeEnv = const_cast<NKikimr::NMiniKQL::TTypeEnvironment*>(&typeEnv);
                        std::tie(source.SourceActor, source.Actor) =
                            SourceActorFactory->CreateDqSourceActor(
                            IDqSourceActorFactory::TArguments{
                                .InputDesc = input,
                                .InputIndex = static_cast<ui64>(inputId),
                                .TxId = TraceId,
                                .SecureParams = secureParams,
                                .TaskParams = taskParams,
                                .Callback = this,
                                .TypeEnv = typeEnv,
                                .HolderFactory = holderFactory
                            });
                        RegisterLocalChild(source.Actor);
                    } else {
                        for (auto& channel : input.GetChannels()) {
                            TInputChannel inChannel;
                            inChannel.InputId = inputId;
                            inChannel.ChannelId = channel.GetId();
                            inChannel.ActorID = ResolveEndpoint(channel.GetSrcEndpoint());
                            InputMap.emplace(inChannel.ActorID, inChannel);
                            InputChannelCount++;
                        }
                    }
                }

                auto& outputs = Task.GetOutputs();
                for (auto outputId = 0; outputId < outputs.size(); outputId++) {
                    auto& output = outputs[outputId];
                    if (output.HasSink()) {
                        auto& sink = SinksMap[outputId];
                        sink.TypeEnv = const_cast<NKikimr::NMiniKQL::TTypeEnvironment*>(&typeEnv);
                        std::tie(sink.SinkActor, sink.Actor) = SinkActorFactory->CreateDqSinkActor(
                            IDqSinkActorFactory::TArguments {
                                .OutputDesc = output,
                                .OutputIndex = static_cast<ui64>(outputId),
                                .TxId = TraceId,
                                .SecureParams = secureParams,
                                .Callback = this,
                                .TypeEnv = typeEnv,
                                .HolderFactory = holderFactory
                            });
                        RegisterLocalChild(sink.Actor);
                    } else {
                        for (auto& channel : output.GetChannels()) {
                            TOutputChannel outChannel;
                            outChannel.OutputId = outputId;
                            outChannel.ChannelId = channel.GetId();
                            outChannel.ActorID = ResolveEndpoint(channel.GetDstEndpoint());
                            OutputMap.emplace(outChannel.ActorID, outChannel);
                            OutChannelId2ActorId.emplace(outChannel.ChannelId, outChannel.ActorID);
                        }
                    }
                }
            });

            if (PingTimeout) {
                Schedule(PingPeriod, new TEvents::TEvWakeup);
            }
        } catch (...) {
            OnError();
        }
    }

    void OnPullRequest(TEvPullDataRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(TRACE) << "TDqWorker::OnPullRequest " << ev->Sender;

        if (!TaskRunnerActor || !TaskRunnerPrepared) {
            // waiting for initialization
            TPullResponse response;
            response.SetResponseType(EPullResponseType::YIELD);
            Send(ev->Sender, MakeHolder<TEvPullDataResponse>(response));
            return;
        }

        auto now = TInstant::Now();
        auto& outChannel = OutputMap[ev->Sender];
        outChannel.RequestTime = now;

        Send(TaskRunnerActor, new TEvPop(outChannel.ChannelId));
    }

    void OnChannelPopFinished(TEvChannelPopFinished::TPtr& ev, const NActors::TActorContext& ctx) {
        try {
            auto outputActorId = OutChannelId2ActorId[ev->Get()->ChannelId];
            auto& outChannel = OutputMap[outputActorId];
            TPullResponse response;
            auto hasData = !ev->Get()->Data.empty();
            Stat.AddCounters2(ev->Get()->Sensors);

            if (hasData) {
                response.SetResponseType(EPullResponseType::CONTINUE);
            } else if (ev->Get()->Finished) {
                outChannel.Finished = true;
                response.SetResponseType(EPullResponseType::FINISH);
            } else {
                response.SetResponseType(EPullResponseType::YIELD);
            }

            Y_VERIFY(ev->Get()->Data.size() <= 1);

            if (ev->Get()->Data.size() == 1) {
                response.MutableData()->Swap(&ev->Get()->Data.front());
            }

            Stat.FlushCounters(response);

            Send(outputActorId, MakeHolder<TEvPullDataResponse>(response));

            Run(ctx);
        } catch (...) {
            OnError();
        }
    }

    void OnPullResponse(TEvPullDataResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(TRACE) << "TDqWorker::OnPullResponse";

        Stat.AddCounters(ev->Get()->Record);

        auto& channel = InputMap[ev->Sender];
        channel.Requested = false;
        channel.Retries = 0;
        channel.ResponseTime = TInstant::Now();
        auto responseType = ev->Get()->Record.GetResponseType();
        if (responseType == FINISH) {
            channel.Finished = true;
            FinishedChannels++;
        }
        if (responseType == YIELD) {
            return;
        }
        if (responseType == ERROR) {
            Send(SelfId(), new TEvError(ev->Get()->Record.GetErrorMessage(), {}));
            return;
        }
        Y_VERIFY (responseType == FINISH || responseType == CONTINUE);
        if (responseType == FINISH) {
            Send(TaskRunnerActor, new TEvPush(channel.ChannelId));
        } else {
            Send(TaskRunnerActor, new TEvPush(
                     channel.ChannelId,
                     std::move(*ev->Get()->Record.MutableData())));
        }
    }

    void OnPingRequest(TEvPingRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        Send(ev->Sender, MakeHolder<TEvPingResponse>(), IEventHandle::FlagTrackDelivery);
    }

    void OnPingResponse(TEvPingResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto& channel = InputMap[ev->Sender];
        channel.PingRequested = false;
    }

    void OnWakeup() {
        auto now = TInstant::Now();
        for (auto& [sender, channel] : InputMap) {
            if (channel.Finished) {
                continue;
            }
            if (!channel.PingRequested) {
                Send(channel.ActorID, MakeHolder<TEvPingRequest>(), IEventHandle::FlagTrackDelivery);
                channel.PingRequested = true;
                channel.PingStartTime = now;
            } else if ((now - channel.PingStartTime) > PingTimeout) {
                Stat.AddCounter("PingTimeout", static_cast<ui64>(1));
                OnError("PingTimeout " + TimeoutInfo(channel.ActorID, now, channel.PingStartTime), true, true);
            }
        }

        Schedule(PingPeriod, new TEvents::TEvWakeup);
    }

    NActors::TActorId ResolveEndpoint(const TEndpoint& ep) {
        switch (ep.GetEndpointTypeCase()) {
            case TEndpoint::kActorId:
                return NActors::ActorIdFromProto(ep.GetActorId());
            case TEndpoint::kUri:
                Y_ENSURE(false, "kUri is not supported");
            case TEndpoint::kTabletId:
                Y_ENSURE(false, "Tablets not supported by dqs");
            case TEndpoint::ENDPOINTTYPE_NOT_SET: {
                Y_ENSURE(false, "Endpoint must be set");
            } break;
        }
    }

    void OnUndelivered(TEvents::TEvUndelivered::TPtr& ev, const NActors::TActorContext&) {
        Stat.AddCounter("Undelivered", TDuration::MilliSeconds(1));

        bool sendError = false;
        auto maybeChannel = InputMap.find(ev->Sender);
        if (ev->Get()->Reason != TEvents::TEvUndelivered::Disconnected) {
            sendError = true;
        } else if (ev->Sender.NodeId() == SelfId().NodeId()) {
            sendError = true;
        } else if (maybeChannel == InputMap.end()) {
            sendError = true;
        } else if (maybeChannel->second.Retries > Settings->MaxNetworkRetries.Get().GetOrElse(5)) {
            sendError = true;
        }

        if (sendError) {
            TString message = "Undelivered from " + ToString(ev->Sender) + " to " + ToString(SelfId())
                + " reason: " + ToString(ev->Get()->Reason) + " retries: " + ToString(
                    maybeChannel == InputMap.end()
                        ? 0
                        : maybeChannel->second.Retries
                ) + " " + JobDebugInfo(ev->Sender);
            OnError(message, /*retriable = */ true, /*fallback =*/ true);
        } else if (ev->Get()->SourceType == TEvPullDataRequest::EventType) {
            TActivationContext::Schedule(TDuration::MilliSeconds(100),
                new IEventHandle(maybeChannel->second.ActorID, SelfId(), new TEvPullDataRequest(INPUT_SIZE), IEventHandle::FlagTrackDelivery)
            );
            maybeChannel->second.Retries ++;
            maybeChannel->second.Requested = true;
            maybeChannel->second.RequestTime = TInstant::Now();
        } else {
            // Ping
            TActivationContext::Schedule(TDuration::MilliSeconds(100),
                new IEventHandle(maybeChannel->second.ActorID, SelfId(), new TEvPingRequest(), IEventHandle::FlagTrackDelivery)
            );
            maybeChannel->second.PingRequested = true;
        }
    }

    void Run(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        if (TaskFinished) {
            return;
        }

        THashSet<ui32> inputChannels;
        for (auto& input : InputMap) {
            auto& channel = input.second;
            if (!channel.Requested && !channel.Finished) {
                inputChannels.insert(channel.ChannelId);
            }
        }

        Send(TaskRunnerActor, new TEvContinueRun(std::move(inputChannels), Settings->MemoryLimit.Get().GetOrElse(0)));
    }

    void OnRunFinished(TEvTaskRunFinished::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto res = ev->Get()->RunStatus;
        if (RuntimeData) {
            ::TRusage delta;
            delta.Stime = TDuration::MicroSeconds(ev->Get()->Rusage.Stime);
            delta.Utime = TDuration::MicroSeconds(ev->Get()->Rusage.Utime);
            delta.MajorPageFaults = ev->Get()->Rusage.MajorPageFaults;
            RuntimeData->AddRusageDelta(delta);
        }

        Stat.AddCounters2(ev->Get()->Sensors);

        switch (res) {
            case ERunStatus::Finished: {
                TaskFinished = true;
                break;
            }
            case ERunStatus::PendingInput: {
                auto now = TInstant::Now();
                for (auto& [sender, channel] : InputMap) {
                    if (!channel.Requested && !channel.Finished) {
                        auto freeSpace = ev->Get()->InputChannelFreeSpace.find(channel.ChannelId);
                        auto hasFreeSpace = freeSpace == ev->Get()->InputChannelFreeSpace.end()
                            || freeSpace->second > 0;
                        if (hasFreeSpace) {
                            YQL_LOG(TRACE) << "Send TEvPullDataRequest to " <<
                                channel.ActorID << " from " <<
                                SelfId();
                            Send(channel.ActorID, MakeHolder<TEvPullDataRequest>(INPUT_SIZE), IEventHandle::FlagTrackDelivery);
                            channel.Requested = true;
                            channel.RequestTime = now;
                        }
                    } else if (channel.Requested && !channel.Finished) {
                        if (PullRequestTimeout && (now - channel.RequestTime) > PullRequestTimeout) {
                            Stat.AddCounter("ReadTimeout", static_cast<ui64>(1));
                            OnError("PullTimeout " + TimeoutInfo(channel.ActorID, now, channel.RequestTime), false, true);
                        }
                    }
                }

                for (auto& [inputIndex, source] : SourcesMap) {
                    auto& freeSpace = source.FreeSpace;
                    auto it = ev->Get()->SourcesFreeSpace.find(inputIndex);
                    if (it != ev->Get()->SourcesFreeSpace.end()) {
                        freeSpace = it->second;
                    }
                    if (freeSpace < 0 || source.PushStarted || source.Finished) {
                        continue;
                    }
                    auto guard = source.TypeEnv->BindAllocator();
                    NKikimr::NMiniKQL::TUnboxedValueVector batch;
                    bool finished = false;
                    const i64 space = source.SourceActor->GetSourceData(batch, finished, freeSpace);
                    const ui64 index = inputIndex;
                    if (space <= 0) {
                        continue;
                    }
                    source.PushStarted = true;
                    source.Finished = finished;

                    Actor->SourcePush(0, index, std::move(batch), space, finished);
                }
                break;
            }
            case ERunStatus::PendingOutput: {
                for (auto& [index, sink] : SinksMap) {
                    const i64 sinkActorFreeSpaceBeforeSend = sink.SinkActor->GetFreeSpace();
                    if (sinkActorFreeSpaceBeforeSend > 0 && !sink.Finished) {
                        Send(TaskRunnerActor, new TEvSinkPop(index, sinkActorFreeSpaceBeforeSend));
                    }
                }
                break;
            }
        }
    }

    TString TimeoutInfo(TActorId actorID, TInstant now, TInstant startTime) {
        TString message = ToString(actorID)
            + " to " + ToString(SelfId())
            + " duration " + ToString(now - startTime)
            + " stageId " + ToString(StageId) + " "
            + JobDebugInfo(actorID);

        return message;
    }

    TString JobDebugInfo(TActorId actorID) {
        TString message;
        TString from, to;
        for (const auto& w : AllWorkers) {
            if (w.GetNodeId() == actorID.NodeId() && w.GetJobId() && w.GetOperationId()) {
                from = "J/O=" + w.GetJobId() + "/" + w.GetOperationId();
            }
            if (w.GetNodeId() == SelfId().NodeId() && w.GetJobId() && w.GetOperationId()) {
                to = "J/O=" + w.GetJobId() + "/" + w.GetOperationId();
            }
        }

        if (from && to) {
            message += "(" + from + "->" + to + " )";
        }
        return message;
    }

    void Dump() {
        auto now = TInstant::Now();
        for (const auto& [actorId, channel] : InputMap) {
            if (!channel.Finished) {
                if (channel.Requested) {
                    YQL_LOG(DEBUG) << "Input " << JobDebugInfo(actorId) << (now - channel.RequestTime) << " Requested? " << channel.Requested;
                    if (RuntimeData) {
                        RuntimeData->UpdateChannelInputDelay(now - channel.RequestTime);
                    }
                } else {
                    YQL_LOG(DEBUG) << "Input " << JobDebugInfo(actorId) << (now - channel.ResponseTime)  << " Requested? " << channel.Requested;
                    if (RuntimeData) {
                        RuntimeData->UpdateChannelInputDelay(now - channel.ResponseTime);
                    }
                }
            } else {
                YQL_LOG(DEBUG) << "Input " << JobDebugInfo(actorId) << " Finished";
                if (RuntimeData) {
                    RuntimeData->UpdateChannelInputDelay(TDuration::Seconds(0));
                }
            }
        }

        for (const auto& [actorId, channel] : OutputMap) {
            if (!channel.Finished) {
                YQL_LOG(DEBUG) << "Output " << JobDebugInfo(actorId) << (now - channel.RequestTime);
                if (RuntimeData) {
                    RuntimeData->UpdateChannelOutputDelay(now - channel.RequestTime);
                }
            } else {
                YQL_LOG(DEBUG) << "Output " << JobDebugInfo(actorId) << " Finished";
                if (RuntimeData) {
                    RuntimeData->UpdateChannelOutputDelay(TDuration::Seconds(0));
                }
            }
        }
    }

    /*____________________ SourceActorEvents __________________*/
    void OnNewSourceDataArrived(ui64 inputIndex) override {
        try {
            if (!TaskRunnerPrepared) {
                return;
            }
            auto& source = SourcesMap[inputIndex];
            source.HasData = true;
            Send(SelfId(), new TEvContinueRun());
        } catch (...) {
            OnError();
        }
    }
    void OnSourceError(ui64 inputIndex, const TIssues& issues, bool isFatal) override {
        Y_UNUSED(inputIndex);
        OnError(issues.ToString(), !isFatal, !isFatal);
    }
    void OnSourcePushFinished(TEvSourcePushFinished::TPtr& ev, const TActorContext& ctx) {
        auto index = ev->Get()->Index;
        auto& source = SourcesMap[index];
        source.PushStarted = false;
        Run(ctx);
    }
    /*_________________________________________________________*/
    /*______________________ SinkActorEvents __________________*/
    void ResumeExecution() override {
        Send(SelfId(), new TEvContinueRun());
    }

    void OnSinkError(ui64 outputIndex, const TIssues& issues, bool isFatal) override {
        Y_UNUSED(outputIndex);
        OnError(issues.ToString(), !isFatal, !isFatal);
    }

    void OnSinkStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override {
        Y_UNUSED(state);
        Y_UNUSED(outputIndex);
        Y_UNUSED(checkpoint);
        OnError("Unimplemented");
    }

    void SinkSend(
        ui64 index,
        NKikimr::NMiniKQL::TUnboxedValueVector&& batch,
        TMaybe<NDqProto::TCheckpoint>&& checkpoint,
        i64 size,
        i64 checkpointSize,
        bool finished,
        bool changed) override
    {
        Y_UNUSED(checkpointSize); Y_UNUSED(checkpoint); Y_UNUSED(changed);
        auto& sink = SinksMap[index];
        sink.Finished = finished;
        sink.SinkActor->SendData(std::move(batch), size, {}, finished);
    }

    /*_________________________________________________________*/

    IDqSourceActorFactory::TPtr SourceActorFactory;
    IDqSinkActorFactory::TPtr SinkActorFactory;
    ITaskRunnerActorFactory::TPtr TaskRunnerActorFactory;
    NTaskRunnerActor::ITaskRunnerActor* Actor = nullptr;
    TActorId TaskRunnerActor;

    NDqProto::TDqTask Task;
    ui64 StageId = 0;
    bool TaskRunnerPrepared = false;

    THashMap<NActors::TActorId, TInputChannel> InputMap;
    THashMap<NActors::TActorId, TOutputChannel> OutputMap;
    THashMap<ui64, NActors::TActorId> OutChannelId2ActorId;
    THashMap<ui64, TSourceInfo> SourcesMap;
    THashMap<ui64, TSinkInfo> SinksMap;

    ui32 InputChannelCount = 0;
    ui32 FinishedChannels = 0;

    NActors::TActorId Executer;

    TWorkerRuntimeData* RuntimeData;
    bool TaskFinished = false;

    const TString TraceId;

    TDqConfiguration::TPtr Settings = MakeIntrusive<TDqConfiguration>();
    TDuration PullRequestTimeout;
    TDuration PingTimeout;
    TDuration PingPeriod;
    NYql::TCounters Stat;

    TVector<Yql::DqsProto::TWorkerInfo> AllWorkers;
    THashSet<TString> FallbackOn;
};

NActors::IActor* CreateWorkerActor(
    TWorkerRuntimeData* runtimeData,
    const TString& traceId,
    const ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
    const IDqSourceActorFactory::TPtr& sourceActorFactory,
    const IDqSinkActorFactory::TPtr& sinkActorFactory)
{
    Y_VERIFY(taskRunnerActorFactory);
    return new TLogWrapReceive(
        new TDqWorker(
            taskRunnerActorFactory,
            sourceActorFactory,
            sinkActorFactory,
            runtimeData,
            traceId), traceId);
}

} // namespace NYql::NDqs
