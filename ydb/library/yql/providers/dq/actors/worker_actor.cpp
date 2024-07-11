#include "worker_actor.h"

#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/dq/actors/compute/dq_task_runner_exec_ctx.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/task_runner_actor/task_runner_actor.h>
#include <ydb/library/yql/providers/dq/runtime/runtime_data.h>

#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>

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
    IDqComputeActorAsyncInput* Source = nullptr;
    NActors::IActor* Actor = nullptr;
    i64 FreeSpace = 1;
    bool HasData = false;
    bool PushStarted = false;
    bool Finished = false;
    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
    std::optional<NKikimr::NMiniKQL::TProgramBuilder> ProgramBuilder;
};

struct TSinkInfo {
    IDqComputeActorAsyncOutput* Sink = nullptr;
    NActors::IActor* Actor = nullptr;
    bool Finished = false;
    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
};

class TDummyMemoryQuotaManager: public IMemoryQuotaManager {
    bool AllocateQuota(ui64) override {
        return true;
    }

    void FreeQuota(ui64) override { }

    ui64 GetCurrentQuota() const override {
        return std::numeric_limits<ui64>::max();
    }

    ui64 GetMaxMemorySize() const override {
        return std::numeric_limits<ui64>::max();
    }

    TString MemoryConsumptionDetails() const override {
        return TString();
    }

    bool IsReasonableToUseSpilling() const override {
        return false;
    }
};

class TDqWorker: public TRichActor<TDqWorker>
               , IDqComputeActorAsyncOutput::ICallbacks
               , ITaskRunnerActor::ICallbacks
{
    static constexpr ui32 INPUT_SIZE = 100000;

public:
    static constexpr char ActorName[] = "YQL_DQ_WORKER";

    explicit TDqWorker(
        const ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
        const IDqAsyncIoFactory::TPtr& asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TWorkerRuntimeData* runtimeData,
        const TString& traceId)
        : TRichActor<TDqWorker>(&TDqWorker::Handler)
        , AsyncIoFactory(asyncIoFactory)
        , FunctionRegistry(functionRegistry)
        , TaskRunnerActorFactory(taskRunnerActorFactory)
        , RuntimeData(runtimeData)
        , TraceId(traceId)
        , MemoryQuotaManager(new TDummyMemoryQuotaManager)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << "TDqWorker created ";
    }

    ~TDqWorker()
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << "TDqWorker destroyed ";
    }

    void DoPassAway() override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        for (const auto& inputs : InputMap) {
            Send(inputs.first, new NActors::TEvents::TEvPoison());
        }

        YQL_CLOG(DEBUG, ProviderDq) << "TDqWorker passed away ";
        if (Actor) {
            Actor->PassAway();
        }
        for (const auto& [_, v] : SourcesMap) {
            v.Source->PassAway();
        }
        for (const auto& [_, v] : SinksMap) {
            v.Sink->PassAway();
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
        HFunc(TEvOutputChannelData, OnOutputChannelData);
        HFunc(TEvTaskRunFinished, OnRunFinished);
        HFunc(TEvSourceDataAck, OnSourceDataAck);

        // weird to have two events for error handling, but we need to use TEvDqFailure
        // between worker_actor <-> executer_actor, cause it transmits statistics in 'Metric' field
        HFunc(NDq::TEvDq::TEvAbortExecution, OnErrorFromPipe);  // received from task_runner_actor
        HFunc(TEvDqFailure, OnError); // received from this actor itself
        HFunc(TEvInputChannelDataAck, OnInputChannelDataAck);
        cFunc(TEvents::TEvWakeup::EventType, OnWakeup);

        hFunc(IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived, OnNewAsyncInputDataArrived);
        hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, OnAsyncInputError);
        IgnoreFunc(TEvStatistics);
    })

    void ExtractStats(::Ydb::Issue::IssueMessage* issue) {
        TString filteredMessage;
        for (auto line : StringSplitter(TString(issue->message().data(), issue->message().size())).SplitByString("\n").SkipEmpty()) {
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
            } else {
                filteredMessage += line;
                filteredMessage += "\n";
            }
        }
        issue->set_message(filteredMessage);
    }

    void OnErrorFromPipe(NDq::TEvDq::TEvAbortExecution::TPtr& ev, const TActorContext&) {
        for (size_t i = 0; i < ev->Get()->Record.IssuesSize(); i++) {
            ExtractStats(ev->Get()->Record.MutableIssues(i));
        }
        // hacky conversion to TEvDqFailure
        auto convertedError = MakeHolder<TEvDqFailure>();
        convertedError->Record.SetStatusCode(ev->Get()->Record.GetStatusCode());
        convertedError->Record.MutableIssues()->Swap(ev->Get()->Record.MutableIssues());
        Y_ABORT_UNLESS(convertedError->Record.GetStatusCode() != NYql::NDqProto::StatusIds::SUCCESS);
        SendFailure(std::move(convertedError));  // enreached with stats inside
    }

    void OnError(TEvDqFailure::TPtr& ev, const TActorContext&) {
        SendFailure(ev->Release());
    }

    void SendFailure(THolder<TEvDqFailure> ev) {
        if (!Executer) {
            // Posible Error on Undelivered before OnDqTask
            YQL_CLOG(ERROR, ProviderDq) << "Error " << ev->Record.ShortUtf8DebugString();
            return;
        }
        Stat.FlushCounters(ev->Record);
        Send(Executer, std::move(ev));
    }

    void OnInputChannelDataAck(TEvInputChannelDataAck::TPtr&, const TActorContext& ctx) {
        Run(ctx);
    }

    void OnDqTask(TEvDqTask::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << "TDqWorker::OnDqTask";

        TFailureInjector::Reach("dq_task_failure", [] {::_exit(1); });

        Y_ABORT_UNLESS(!TaskRunnerActor);
        Y_ABORT_UNLESS(!Executer);
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

        NActors::IActor* actor;
        std::tie(Actor, actor) = TaskRunnerActorFactory->Create(
            this,
            std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(
                __LOCATION__,
                NKikimr::TAlignedPagePoolCounters(),
                true,
                false
            ),
            TraceId,
            Task.GetId());
        TaskRunnerActor = RegisterLocalChild(actor);
        TDqTaskRunnerMemoryLimits limits; // used for local mode only
        limits.ChannelBufferSize = 20_MB;
        limits.OutputChunkMaxSize = 2_MB;

        auto wakeup = [this]{ ResumeExecution(EResumeSource::Default); };
        std::shared_ptr<IDqTaskRunnerExecutionContext> execCtx = std::make_shared<TDqTaskRunnerExecutionContext>(TraceId, std::move(wakeup));

        Send(TaskRunnerActor, new TEvTaskRunnerCreate(std::move(ev->Get()->Record.GetTask()), limits, NDqProto::DQ_STATS_MODE_BASIC, execCtx));
    }

    void OnTaskRunnerCreated(TEvTaskRunnerCreateFinished::TPtr& ev, const TActorContext& ) {
        TaskRunnerPrepared = true;

        try {
            TFailureInjector::Reach("dq_fail_on_task_runner_created", [] { throw yexception() << "dq_fail_on_task_runner_created"; });
            Stat.AddCounters2(ev->Get()->Sensors);

            const auto& secureParams = ev->Get()->SecureParams;
            const auto& taskParams = ev->Get()->TaskParams;
            const auto& readRanges = ev->Get()->ReadRanges;
            const auto& typeEnv = ev->Get()->TypeEnv;
            const auto& holderFactory = ev->Get()->HolderFactory;

            Stat.Measure<void>("PrepareChannels", [&](){
                auto& inputs = Task.GetInputs();
                for (auto inputId = 0; inputId < inputs.size(); inputId++) {
                    auto& input = inputs[inputId];
                    if (input.HasSource()) {
                        auto& source = SourcesMap[inputId];
                        source.TypeEnv = const_cast<NKikimr::NMiniKQL::TTypeEnvironment*>(&typeEnv);
                        source.ProgramBuilder.emplace(*source.TypeEnv, *FunctionRegistry);
                        std::tie(source.Source, source.Actor) =
                            AsyncIoFactory->CreateDqSource(
                            IDqAsyncIoFactory::TSourceArguments {
                                .InputDesc = input,
                                .InputIndex = static_cast<ui64>(inputId),
                                .TxId = TraceId,
                                .TaskId = Task.GetId(),
                                .SecureParams = secureParams,
                                .TaskParams = taskParams,
                                .ReadRanges = readRanges,
                                .ComputeActorId = SelfId(),
                                .TypeEnv = typeEnv,
                                .HolderFactory = holderFactory,
                                .ProgramBuilder = *source.ProgramBuilder,
                                .MemoryQuotaManager = MemoryQuotaManager
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
                        std::tie(sink.Sink, sink.Actor) = AsyncIoFactory->CreateDqSink(
                            IDqAsyncIoFactory::TSinkArguments {
                                .OutputDesc = output,
                                .OutputIndex = static_cast<ui64>(outputId),
                                .TxId = TraceId,
                                .TaskId = Task.GetId(),
                                .Callback = this,
                                .SecureParams = secureParams,
                                .TaskParams = taskParams,
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
            SendFailure(MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::INTERNAL_ERROR, CurrentExceptionMessage()));
        }
    }

    void OnPullRequest(TEvPullDataRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(TRACE, ProviderDq) << "TDqWorker::OnPullRequest " << ev->Sender;

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

        Send(TaskRunnerActor, new TEvOutputChannelDataRequest(outChannel.ChannelId, false, 0));
    }

    void OnOutputChannelData(TEvOutputChannelData::TPtr& ev, const NActors::TActorContext& ctx) {
        try {
            TFailureInjector::Reach("dq_fail_on_channel_pop_finished", [] { throw yexception() << "dq_fail_on_channel_pop_finished"; });
            auto outputActorId = OutChannelId2ActorId[ev->Get()->ChannelId];
            auto& outChannel = OutputMap[outputActorId];

            auto responseMsg = MakeHolder<TEvPullDataResponse>();
            TPullResponse& response = responseMsg->Record;

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

            Y_ABORT_UNLESS(ev->Get()->Data.size() <= 1);

            if (ev->Get()->Data.size() == 1) {
                TDqSerializedBatch& batch = ev->Get()->Data.front();
                response.MutableData()->Swap(&batch.Proto);
                response.MutableData()->ClearPayloadId();
                if (!batch.Payload.IsEmpty()) {
                    response.MutableData()->SetPayloadId(responseMsg->AddPayload(std::move(batch.Payload)));
                }
            }

            Stat.FlushCounters(response);

            Send(outputActorId, std::move(responseMsg));

            Run(ctx);
        } catch (...) {
            SendFailure(MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::INTERNAL_ERROR, CurrentExceptionMessage()));
        }
    }

    void OnPullResponse(TEvPullDataResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(TRACE, ProviderDq) << "TDqWorker::OnPullResponse";

        TPullResponse& response = ev->Get()->Record;

        Stat.AddCounters(ev->Get()->Record);

        auto& channel = InputMap[ev->Sender];
        channel.Requested = false;
        channel.Retries = 0;
        channel.ResponseTime = TInstant::Now();
        auto responseType = response.GetResponseType();
        if (responseType == FINISH) {
            channel.Finished = true;
            FinishedChannels++;
        }
        if (responseType == YIELD) {
            return;
        }
        if (responseType == ERROR) {
            Send(SelfId(), MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::UNSPECIFIED, response.GetErrorMessage()));
            return;
        }
        Y_ABORT_UNLESS(responseType == FINISH || responseType == CONTINUE);
        if (responseType == FINISH) {
            Send(TaskRunnerActor, new TEvInputChannelData(channel.ChannelId, {}, true, false));
        } else {
            TDqSerializedBatch data;
            if (response.GetData().HasPayloadId()) {
                data.Payload = ev->Get()->GetPayload(response.GetData().GetPayloadId());
            }
            data.Proto = std::move(*response.MutableData());
            data.Proto.ClearPayloadId();
            Send(TaskRunnerActor, new TEvInputChannelData(channel.ChannelId, {std::move(data)}, false, false));
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
                SendFailure(MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::TIMEOUT, "PingTimeout " + TimeoutInfo(channel.ActorID, now, channel.PingStartTime)));
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
            SendFailure(MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::UNAVAILABLE, message));
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
                            YQL_CLOG(TRACE, ProviderDq) << "Send TEvPullDataRequest to " <<
                                channel.ActorID << " from " <<
                                SelfId();
                            Send(channel.ActorID, MakeHolder<TEvPullDataRequest>(INPUT_SIZE), IEventHandle::FlagTrackDelivery);
                            channel.Requested = true;
                            channel.RequestTime = now;
                        }
                    } else if (channel.Requested && !channel.Finished) {
                        if (PullRequestTimeout && (now - channel.RequestTime) > PullRequestTimeout) {
                            Stat.AddCounter("ReadTimeout", static_cast<ui64>(1));
                            SendFailure(MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::TIMEOUT, "PullTimeout " + TimeoutInfo(channel.ActorID, now, channel.RequestTime)));
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
                    TMaybe<TInstant> watermark;
                    NKikimr::NMiniKQL::TUnboxedValueBatch batch;
                    bool finished = false;
                    const i64 space = source.Source->GetAsyncInputData(batch, watermark, finished, freeSpace);
                    const ui64 index = inputIndex;
                    if (space <= 0) {
                        continue;
                    }
                    source.PushStarted = true;
                    source.Finished = finished;

                    Actor->AsyncInputPush(0, index, std::move(batch), space, finished);
                }
                break;
            }
            case ERunStatus::PendingOutput: {
                for (auto& [index, sink] : SinksMap) {
                    const i64 sinkFreeSpaceBeforeSend = sink.Sink->GetFreeSpace();
                    if (sinkFreeSpaceBeforeSend > 0 && !sink.Finished) {
                        Send(TaskRunnerActor, new TEvSinkDataRequest(index, sinkFreeSpaceBeforeSend));
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
                    YQL_CLOG(DEBUG, ProviderDq) << "Input " << JobDebugInfo(actorId) << (now - channel.RequestTime) << " Requested? " << channel.Requested;
                    if (RuntimeData) {
                        RuntimeData->UpdateChannelInputDelay(now - channel.RequestTime);
                    }
                } else {
                    YQL_CLOG(DEBUG, ProviderDq) << "Input " << JobDebugInfo(actorId) << (now - channel.ResponseTime)  << " Requested? " << channel.Requested;
                    if (RuntimeData) {
                        RuntimeData->UpdateChannelInputDelay(now - channel.ResponseTime);
                    }
                }
            } else {
                YQL_CLOG(DEBUG, ProviderDq) << "Input " << JobDebugInfo(actorId) << " Finished";
                if (RuntimeData) {
                    RuntimeData->UpdateChannelInputDelay(TDuration::Seconds(0));
                }
            }
        }

        for (const auto& [actorId, channel] : OutputMap) {
            if (!channel.Finished) {
                YQL_CLOG(DEBUG, ProviderDq) << "Output " << JobDebugInfo(actorId) << (now - channel.RequestTime);
                if (RuntimeData) {
                    RuntimeData->UpdateChannelOutputDelay(now - channel.RequestTime);
                }
            } else {
                YQL_CLOG(DEBUG, ProviderDq) << "Output " << JobDebugInfo(actorId) << " Finished";
                if (RuntimeData) {
                    RuntimeData->UpdateChannelOutputDelay(TDuration::Seconds(0));
                }
            }
        }
    }

    /*____________________ SourceEvents __________________*/
    void OnNewAsyncInputDataArrived(const IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::TPtr& ev) {
        try {
            if (!TaskRunnerPrepared) {
                return;
            }
            auto& source = SourcesMap[ev->Get()->InputIndex];
            source.HasData = true;
            Send(SelfId(), new TEvContinueRun());
        } catch (...) {
            SendFailure(MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::UNSPECIFIED, CurrentExceptionMessage()));
        }
    }
    void OnAsyncInputError(const IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        Y_UNUSED(ev->Get()->InputIndex);
        auto fatalCode = ev->Get()->FatalCode;
        if (fatalCode != NYql::NDqProto::StatusIds::UNSPECIFIED) {
            fatalCode = NYql::NDqProto::StatusIds::INTERNAL_ERROR;
        }
        SendFailure(MakeHolder<TEvDqFailure>(fatalCode, ev->Get()->Issues.ToString()));
    }
    void OnSourceDataAck(TEvSourceDataAck::TPtr& ev, const TActorContext& ctx) {
        auto index = ev->Get()->Index;
        auto& source = SourcesMap[index];
        source.PushStarted = false;
        Run(ctx);
    }
    /*_________________________________________________________*/
    /*______________________ Sink Events ----__________________*/
    void ResumeExecution(EResumeSource) override {
        Send(SelfId(), new TEvContinueRun());
    }

    void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override {
        Y_UNUSED(outputIndex);
        if (fatalCode != NYql::NDqProto::StatusIds::UNSPECIFIED) {
            fatalCode = NYql::NDqProto::StatusIds::INTERNAL_ERROR;
        }
        SendFailure(MakeHolder<TEvDqFailure>(fatalCode, issues.ToString()));
    }

    void OnAsyncOutputFinished(ui64 outputIndex) override {
        Y_UNUSED(outputIndex);
    }

    void OnAsyncOutputStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override {
        Y_UNUSED(state);
        Y_UNUSED(outputIndex);
        Y_UNUSED(checkpoint);
        SendFailure(MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::BAD_REQUEST, "Unimplemented"));
    }

    void SinkSend(
        ui64 index,
        NKikimr::NMiniKQL::TUnboxedValueBatch&& batch,
        TMaybe<NDqProto::TCheckpoint>&& checkpoint,
        i64 size,
        i64 checkpointSize,
        bool finished,
        bool changed) override
    {
        Y_UNUSED(checkpointSize); Y_UNUSED(checkpoint); Y_UNUSED(changed);
        auto& sink = SinksMap[index];
        sink.Finished = finished;
        sink.Sink->SendData(std::move(batch), size, {}, finished);
    }

    /*_________________________________________________________*/

    IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
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

    IMemoryQuotaManager::TPtr MemoryQuotaManager;
};

NActors::IActor* CreateWorkerActor(
    TWorkerRuntimeData* runtimeData,
    const TString& traceId,
    const ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
    const IDqAsyncIoFactory::TPtr& asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry)
{
    Y_ABORT_UNLESS(taskRunnerActorFactory);
    return new TLogWrapReceive(
        new TDqWorker(
            taskRunnerActorFactory,
            asyncIoFactory,
            functionRegistry,
            runtimeData,
            traceId), traceId);
}

} // namespace NYql::NDqs
