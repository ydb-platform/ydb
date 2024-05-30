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

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/size_literals.h>

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

class TSpillingStorageInfo : public TSimpleRefCount<TSpillingStorageInfo> {
public:
    using TPtr = std::shared_ptr<TSpillingStorageInfo>;

    TSpillingStorageInfo(const IDqChannelStorage::TPtr spillingStorage, ui64 channelId)
        : SpillingStorage(spillingStorage)
        , ChannelId(channelId)
        , FirstStoredId(0)
        , NextStoredId(0)
    {}

    const IDqChannelStorage::TPtr SpillingStorage = nullptr;
    ui64 ChannelId = 0;
    ui64 FirstStoredId = 0;
    ui64 NextStoredId = 0;
};

struct TOutputChannelReadResult {
    bool IsChanged = false;
    bool IsFinished = false;
    bool HasData = false;
    std::list<::NYql::NDqProto::TMetric> Metrics;
    TVector<TDqSerializedBatch> DataChunks;
};

class TOutputChannelReader {
public:
    TOutputChannelReader(NTaskRunnerProxy::IOutputChannel::TPtr channel, i64 toPopSize,
        bool wasFinished, TSpillingStorageInfo::TPtr spillingStorageInfo, ui64 cookie
    )
        : Channel(channel)
        , SpillingStorageInfo(spillingStorageInfo)
        , ToPopSize(toPopSize)
        , WasFinished(wasFinished)
        , Cookie(cookie)
    {}

    TOutputChannelReadResult Read() {
        if (SpillingStorageInfo) {
            return ReadWithSpilling();
        }
        return ReadDirectly();
    }

private:

    TOutputChannelReadResult ReadDirectly() {
        int maxChunks = std::numeric_limits<int>::max();
        bool changed = false;
        bool isFinished = false;
        i64 remain = ToPopSize;
        ui32 dataSize = 0;
        bool hasData = true;
        TOutputChannelReadResult result;

        if (remain == 0) {
            // special case to WorkerActor
            remain = 5_MB;
            maxChunks = 1;
        }

        TVector<TDqSerializedBatch> chunks;
        for (;maxChunks && remain > 0 && !isFinished && hasData; maxChunks--, remain -= dataSize) {
            TDqSerializedBatch data;
            const auto lastPop = std::move(Channel->Pop(data));

            for (auto& metric : lastPop.GetMetric()) {
                result.Metrics.push_back(metric);
            }

            hasData = lastPop.GetResult();
            result.HasData = result.HasData || hasData;
            dataSize = data.Size();
            isFinished = !hasData && Channel->IsFinished();
            changed = changed || hasData || (isFinished != WasFinished);

            if (hasData) {
                result.DataChunks.emplace_back(std::move(data));
            }
        }
        result.IsFinished = isFinished;
        result.IsChanged = changed;
        return result;
    }

    TOutputChannelReadResult ReadWithSpilling() {
        bool changed = false;
        bool isChanFinished = false;
        i64 remain = ToPopSize;
        bool hasData = true;
        TOutputChannelReadResult result;

        if (remain == 0) {
            // special case to WorkerActor
            remain = 5_MB;
        }

        auto spillingStorage = SpillingStorageInfo->SpillingStorage;
        // Read all available data from the pipe and spill it
        while (spillingStorage && !isChanFinished && hasData) {
            TDqSerializedBatch data;
            const auto lastPop = std::move(Channel->Pop(data));

            for (auto& metric : lastPop.GetMetric()) {
                result.Metrics.push_back(metric);
            }

            hasData = lastPop.GetResult();
            isChanFinished = !hasData && Channel->IsFinished();
            changed = changed || hasData || (isChanFinished != WasFinished);
            if (hasData) {
                spillingStorage->Put(SpillingStorageInfo->NextStoredId++, SaveForSpilling(std::move(data)), Cookie);
            }
        }

        changed = false;
        result.DataChunks.reserve(SpillingStorageInfo->NextStoredId - SpillingStorageInfo->FirstStoredId);
        while (SpillingStorageInfo->FirstStoredId < SpillingStorageInfo->NextStoredId && remain > 0) {
            TDqSerializedBatch data;
            YQL_ENSURE(spillingStorage);
            TBuffer blob;
            if (!spillingStorage->Get(SpillingStorageInfo->FirstStoredId, blob, Cookie)) {
                break;
            }
            ++SpillingStorageInfo->FirstStoredId;
            data = LoadSpilled(std::move(blob));
            remain -= data.Size();
            result.DataChunks.emplace_back(std::move(data));
            changed = true;
            hasData = true;
        }

        result.IsFinished = isChanFinished && SpillingStorageInfo->FirstStoredId == SpillingStorageInfo->NextStoredId;
        result.IsChanged = changed;
        result.HasData = hasData;
        return result;
    }

    NTaskRunnerProxy::IOutputChannel::TPtr Channel;
    TSpillingStorageInfo::TPtr SpillingStorageInfo;
    i64 ToPopSize;
    bool WasFinished;
    ui64 Cookie;
};

} // namespace

class TTaskRunnerActor
    : public TActor<TTaskRunnerActor>
    , public ITaskRunnerActor
{
public:
    static constexpr char ActorName[] = "YQL_DQ_TASK_RUNNER";

    TTaskRunnerActor(
        ITaskRunnerActor::ICallbacks* parent,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NTaskRunnerProxy::IProxyFactory::TPtr& factory,
        const ITaskRunnerInvoker::TPtr& invoker,
        const TTxId& txId,
        ui64 taskId,
        TWorkerRuntimeData* runtimeData)
        : TActor<TTaskRunnerActor>(&TTaskRunnerActor::Handler)
        , Parent(parent)
        , Alloc(alloc)
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

    STFUNC(Handler) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, TTaskRunnerActor::PassAway);
            hFunc(TEvTaskRunnerCreate, OnDqTask);
            hFunc(TEvContinueRun, OnContinueRun);
            hFunc(TEvOutputChannelDataRequest, OnOutputhannelDataRequest);
            hFunc(TEvInputChannelData, OnInputChannelData);
            hFunc(TEvSinkDataRequest, OnSinkDataRequest);
            hFunc(TEvSinkData, OnSinkData);
            IgnoreFunc(TEvStatistics);
            default: {
                auto message = TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite() << " (" << ev->GetTypeName() << ")" << " stageId: " << StageId;
                auto issue = TIssue(message).SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_ERROR);
                auto reply = MakeHolder<NDq::TEvDq::TEvAbortExecution>(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TVector<TIssue>{issue});
                Send(ParentId, reply.Release());
            }
        }
    }

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
        bool rpcReaderFalledBack = false;
        for (TStringBuf line: StringSplitter(input).SplitByString("\n")) {
            if (line.Contains("mlockall failed")) {
                // skip
            } else {
                if (!fallback || rpcReaderFalledBack) {
                    if (line.Contains("FindColumnInfo(): requirement memberType->GetKind() == TType::EKind::Data")) {
                    // YQL-14757: temporary workaround for part6/produce-reduce_lambda_list_table-default.txt
                        fallback = true;
                    } else if (line.Contains("embedded:Len")) {
                        // YQL-14763
                        fallback = true;
                    } else if (line.Contains("No such transaction")) {
                        // YQL-15542
                        fallback = true;
                    } else if (line.Contains("YT RPC Reader exception:")) {
                        // RPC reader fallback to YT
                        fallback = true;
                        rpcReaderFalledBack = true;
                    } else if (line.Contains("Attachments stream write timed out") || line.Contains("No alive peers found")) {
                        // RPC reader DQ retry
                        retry = true;
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
        Y_ABORT_UNLESS(queryStatus != NYql::NDqProto::StatusIds::SUCCESS);
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

    void OnInputChannelData(TEvInputChannelData::TPtr& ev) {
        auto* actorSystem = TActivationContext::ActorSystem();
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto finish = ev->Get()->Finish;
        auto channelId = ev->Get()->ChannelId;
        auto cookie = ev->Cookie;
        auto data = ev->Get()->Data;
        Invoker->Invoke([selfId, cookie, finish, channelId, taskRunner=TaskRunner, data, actorSystem, replyTo, settings=Settings, stageId=StageId] () mutable {
            try {
                // todo:(whcrc) finish output channel?
                ui64 freeSpace = 0;
                if (data) {
                    // auto guard = taskRunner->BindAllocator(); // only for local mode
                    taskRunner->GetInputChannel(channelId)->Push(std::move(*data));
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
                        new TEvInputChannelDataAck(channelId, freeSpace),
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

        YQL_ENSURE(!batch.IsWide());

        auto* source = TaskRunner->GetSource(index);
        TDqDataSerializer dataSerializer(TaskRunner->GetTypeEnv(), TaskRunner->GetHolderFactory(), DataTransportVersion);
        TDqSerializedBatch serialized = dataSerializer.Serialize(batch, source->GetInputType());

        Invoker->Invoke([serialized=std::move(serialized), taskRunner=TaskRunner, actorSystem, selfId, cookie, parentId=ParentId, space, finish, index, settings=Settings, stageId=StageId]() mutable {
            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                auto* source = taskRunner->GetSource(index);
                source->Push(std::move(serialized), space);
                if (finish) {
                    source->Finish();
                }
                actorSystem->Send(
                    new IEventHandle(
                        parentId,
                        selfId,
                        new TEvSourceDataAck(index, source->GetFreeSpace()),
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

    void OnOutputhannelDataRequest(TEvOutputChannelDataRequest::TPtr& ev) {
        auto* actorSystem = TActivationContext::ActorSystem();
        auto replyTo = ev->Sender;
        auto selfId = SelfId();
        auto cookie = ev->Cookie;
        auto wasFinished = ev->Get()->WasFinished;
        auto toPop = ev->Get()->Size;
        ui64 channelId = ev->Get()->ChannelId;

        TSpillingStorageInfo::TPtr spillingStorageInfo = GetSpillingStorage(channelId);

        Invoker->Invoke([spillingStorageInfo, cookie, selfId, channelId=ev->Get()->ChannelId, actorSystem, replyTo, wasFinished, toPop, taskRunner=TaskRunner, settings=Settings, stageId=StageId]() {
            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                auto channel = taskRunner->GetOutputChannel(channelId);
                TOutputChannelReader reader(channel, toPop, wasFinished, spillingStorageInfo, cookie);
                TOutputChannelReadResult result = reader.Read();

                NDqProto::TPopResponse response;
                for (auto& metric : result.Metrics) {
                    *response.AddMetric() = metric;
                }
                response.SetResult(response.GetResult() || result.HasData);

                actorSystem->Send(
                    new IEventHandle(
                        replyTo,
                        selfId,
                        new TEvOutputChannelData(
                            channelId,
                            std::move(result.DataChunks),
                            Nothing(),
                            Nothing(),
                            result.IsFinished,
                            result.IsChanged,
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

    void OnSinkData(TEvSinkData::TPtr& ev) {
        auto guard = TaskRunner->BindAllocator();
        NKikimr::NMiniKQL::TUnboxedValueBatch batch;
        auto sink = TaskRunner->GetSink(ev->Get()->Index);
        TDqDataSerializer dataSerializer(TaskRunner->GetTypeEnv(), TaskRunner->GetHolderFactory(), (NDqProto::EDataTransportVersion)ev->Get()->Batch.Proto.GetTransportVersion());
        dataSerializer.Deserialize(std::move(ev->Get()->Batch), sink->GetOutputType(), batch);

        Parent->SinkSend(
            ev->Get()->Index,
            std::move(batch),
            std::move(ev->Get()->Checkpoint),
            ev->Get()->CheckpointSize,
            ev->Get()->Size,
            ev->Get()->Finished,
            ev->Get()->Changed);
    }

    void OnSinkDataRequest(TEvSinkDataRequest::TPtr& ev) {
        auto selfId = SelfId();
        auto* actorSystem = TActivationContext::ActorSystem();

        Invoker->Invoke([taskRunner=TaskRunner, selfId, actorSystem, ev=std::move(ev), settings=Settings, stageId=StageId] {
            auto cookie = ev->Cookie;
            auto replyTo = ev->Sender;

            try {
                // auto guard = taskRunner->BindAllocator(); // only for local mode
                auto sink = taskRunner->GetSink(ev->Get()->Index);
                NDq::TDqSerializedBatch batch;
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
                auto event = MakeHolder<TEvSinkData>(
                    ev->Get()->Index,
                    std::move(maybeCheckpoint), size, checkpointSize, finished, changed);
                event->Batch = std::move(batch);
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
        auto& outputs = ev->Get()->Task.GetOutputs();
        auto startTime = TInstant::Now();
        ExecCtx = ev->Get()->ExecCtx;
        auto* actorSystem = TActivationContext::ActorSystem();

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

        for (auto outputId = 0; outputId < outputs.size(); outputId++) {
            auto& channels = outputs[outputId].GetChannels();
            for (auto& channel : channels) {
                CreateSpillingStorage(channel.GetId(), actorSystem, channel.GetEnableSpilling());
            }
        }

        ParentId = ev->Sender;

        try {
            Yql::DqsProto::TTaskMeta taskMeta;
            ev->Get()->Task.GetMeta().UnpackTo(&taskMeta);
            Settings->Dispatch(taskMeta.GetSettings());
            Settings->FreezeDefaults();
            DataTransportVersion = Settings->GetDataTransportVersion();
            StageId = taskMeta.GetStageId();

            NDq::TDqTaskSettings settings(&ev->Get()->Task);
            TaskRunner = Factory->GetOld(Alloc, settings, TraceId);
        } catch (...) {
            TString message = "Could not create TaskRunner for " + ToString(taskId) + " on node " + ToString(replyTo.NodeId()) + ", error: " + CurrentExceptionMessage();
            Send(replyTo, TEvDq::TEvAbortExecution::Unavailable(message), 0, cookie); // retries, fallback on retries limit
            return;
        }

        Invoker->Invoke([taskRunner=TaskRunner, replyTo, selfId, cookie, actorSystem, settings=Settings, stageId=StageId, startTime, clusterName = ClusterName](){
            try {
                //auto guard = taskRunner->BindAllocator(); // only for local mode
                NDq::TDqTaskRunnerMemoryLimits limits;
                limits.ChannelBufferSize = settings->ChannelBufferSize.Get().GetOrElse(TDqSettings::TDefault::ChannelBufferSize);
                limits.OutputChunkMaxSize = settings->OutputChunkMaxSize.Get().GetOrElse(TDqSettings::TDefault::OutputChunkMaxSize);
                limits.ChunkSizeLimit = settings->ChunkSizeLimit.Get().GetOrElse(TDqSettings::TDefault::ChunkSizeLimit);
                auto result = taskRunner->Prepare(limits);
                auto sensors = GetSensors(result);
                auto sensorName = TCounters::GetCounterName(
                    "Actor",
                    {{"ClusterName", clusterName}},
                    "ProcessInitUs");
                i64 val = (TInstant::Now()-startTime).MicroSeconds();
                sensors.push_back({sensorName, val, val, val, val, 1});

                auto event = MakeHolder<TEvTaskRunnerCreateFinished>(
                    taskRunner->GetSecureParams(),
                    taskRunner->GetTaskParams(),
                    taskRunner->GetReadRanges(),
                    taskRunner->GetTypeEnv(),
                    taskRunner->GetHolderFactory(),
                    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>{},
                    THashMap<ui64, std::pair<NUdf::TUnboxedValue, IDqAsyncInputBuffer::TPtr>>{},
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

    TSpillingStorageInfo::TPtr GetSpillingStorage(ui64 channelId) {
        TSpillingStorageInfo::TPtr spillingStorage = nullptr;
        auto spillingIt = SpillingStoragesInfos.find(channelId);
        if (spillingIt != SpillingStoragesInfos.end()) {
            spillingStorage = spillingIt->second;
        }
        return spillingStorage;
    }

    void CreateSpillingStorage(ui64 channelId, TActorSystem* actorSystem, bool enableSpilling) {
        TSpillingStorageInfo::TPtr spillingStorageInfo = nullptr;
        auto channelStorage = ExecCtx->CreateChannelStorage(channelId, enableSpilling, actorSystem);

        if (channelStorage) {
            auto spillingIt = SpillingStoragesInfos.find(channelId);
            YQL_ENSURE(spillingIt == SpillingStoragesInfos.end());

            TSpillingStorageInfo* info = new TSpillingStorageInfo(channelStorage, channelId);
            spillingIt = SpillingStoragesInfos.emplace(channelId, info).first;
        }
    }

    NActors::TActorId ParentId;
    ITaskRunnerActor::ICallbacks* Parent;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const TString TraceId;
    const ui64 TaskId;
    NTaskRunnerProxy::IProxyFactory::TPtr Factory;
    NTaskRunnerProxy::ITaskRunner::TPtr TaskRunner;
    ITaskRunnerInvoker::TPtr Invoker;
    bool Local;
    THashSet<ui32> Inputs;
    THashSet<ui32> Sources;
    TIntrusivePtr<TDqConfiguration> Settings;
    NDqProto::EDataTransportVersion DataTransportVersion;
    ui64 StageId;
    TWorkerRuntimeData* RuntimeData;
    TString ClusterName;

    std::shared_ptr<IDqTaskRunnerExecutionContext> ExecCtx;
    std::unordered_map<ui64, TSpillingStorageInfo::TPtr> SpillingStoragesInfos;
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
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const TTxId& txId,
        ui64 taskId,
        THashSet<ui32>&&,
        THolder<NYql::NDq::TDqMemoryQuota>&&) override
    {
        auto* actor = new TTaskRunnerActor(parent, alloc, ProxyFactory, InvokerFactory->Create(), txId, taskId, RuntimeData);
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
