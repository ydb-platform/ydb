#include "kqp_executer.h"
#include "kqp_executer_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/rm/kqp_rm.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/prepare/kqp_query_plan.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <ydb/core/base/wilson.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

namespace {

TDqTaskRunnerContext CreateTaskRunnerContext(NMiniKQL::TKqpComputeContextBase* computeCtx, NMiniKQL::TScopedAlloc* alloc,
    NMiniKQL::TTypeEnvironment* typeEnv)
{
    TDqTaskRunnerContext context;
    context.FuncRegistry = AppData()->FunctionRegistry;
    context.RandomProvider = TAppData::RandomProvider.Get();
    context.TimeProvider = TAppData::TimeProvider.Get();
    context.ComputeCtx = computeCtx;
    context.ComputationFactory = NMiniKQL::GetKqpBaseComputeFactory(computeCtx);
    context.Alloc = alloc;
    context.TypeEnv = typeEnv;
    context.ApplyCtx = nullptr;
    return context;
}

TDqTaskRunnerSettings CreateTaskRunnerSettings(Ydb::Table::QueryStatsCollection::Mode statsMode) {
    TDqTaskRunnerSettings settings;
    // Always collect basic stats for system views / request unit computation.
    settings.CollectBasicStats = true;
    settings.CollectProfileStats = CollectProfileStats(statsMode);
    settings.OptLLVM = "OFF";
    settings.TerminateOnError = false;
    settings.AllowGeneratorsInUnboxedValues = false;
    return settings;
}

TDqTaskRunnerMemoryLimits CreateTaskRunnerMemoryLimits() {
    TDqTaskRunnerMemoryLimits memoryLimits;
    memoryLimits.ChannelBufferSize = std::numeric_limits<ui32>::max();
    memoryLimits.OutputChunkMaxSize = std::numeric_limits<ui32>::max();
    return memoryLimits;
}

TDqTaskRunnerExecutionContext CreateTaskRunnerExecutionContext() {
    return {};
}

class TKqpLiteralExecuter : public TActorBootstrapped<TKqpLiteralExecuter> {
    using TBase = TActorBootstrapped<TKqpLiteralExecuter>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_LITERAL_EXECUTER_ACTOR;
    }

public:
    TKqpLiteralExecuter(IKqpGateway::TExecPhysicalRequest&& request, TKqpRequestCounters::TPtr counters)
        : Request(std::move(request))
        , Counters(counters)
        , LiteralExecuterSpan(TWilsonKqp::LiteralExecuter, std::move(Request.TraceId), "LiteralExecuter")
    {
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>();
        ResponseEv->Orbit = std::move(Request.Orbit);
        Stats = std::make_unique<TQueryExecutionStats>(Request.StatsMode, &TasksGraph,
            ResponseEv->Record.MutableResponse()->MutableResult()->MutableStats());
    }

    void Bootstrap() {
        StartTime = TAppData::TimeProvider->Now();
        if (Request.Timeout) {
            Deadline = StartTime + Request.Timeout;
        }
        if (Request.CancelAfter) {
            CancelAt = StartTime + *Request.CancelAfter;
        }

        LOG_D("Begin literal execution. Operation timeout: " << Request.Timeout << ", cancelAfter: " << Request.CancelAfter);

        Become(&TKqpLiteralExecuter::WorkState);
    }

private:
    STATEFN(WorkState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxRequest, Handle);
                default: {
                    LOG_C("TKqpLiteralExecuter, unexpected event: " << ev->GetTypeRewrite() << ", selfID: " << SelfId());
                    InternalError("Unexpected event");
                }
            }
        } catch (const TMemoryLimitExceededException&) {
            LOG_W("TKqpLiteralExecuter, memory limit exceeded.");
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, "Memory limit exceeded"));
        } catch (...) {
            auto msg = CurrentExceptionMessage();
            LOG_C("TKqpLiteralExecuter, unexpected exception caught: " << msg);
            InternalError(TStringBuilder() << "Unexpected exception: " << msg);
        }
    }

    void Handle(TEvKqpExecuter::TEvTxRequest::TPtr& ev) {
        NWilson::TSpan prepareTasksSpan(TWilsonKqp::LiteralExecuterPrepareTasks, LiteralExecuterSpan.GetTraceId(), "PrepareTasks", NWilson::EFlags::AUTO_END);
        if (Stats) {
            Stats->StartTs = TInstant::Now();
        }

        TxId = ev->Get()->Record.GetRequest().GetTxId();
        Target = ActorIdFromProto(ev->Get()->Record.GetTarget());

        {
            LOG_D("Report self actorId " << SelfId() << " to " << Target);
            auto progressEv = MakeHolder<TEvKqpExecuter::TEvExecuterProgress>();
            ActorIdToProto(SelfId(), progressEv->Record.MutableExecuterActorId());
            Send(Target, progressEv.Release());
        }

        LOG_D("Begin literal execution, txs: " << Request.Transactions.size());

        FillKqpTasksGraphStages(TasksGraph, Request.Transactions);

        for (ui32 txIdx = 0; txIdx < Request.Transactions.size(); ++txIdx) {
            auto& tx = Request.Transactions[txIdx];

            for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
                auto& stage = tx.Body->GetStages(stageIdx);
                auto& stageInfo = TasksGraph.GetStageInfo(TStageId(txIdx, stageIdx));
                LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

                YQL_ENSURE(stageInfo.Meta.ShardOperations.empty());
                YQL_ENSURE(stageInfo.InputsCount == 0);

                TasksGraph.AddTask(stageInfo);
            }

            BuildKqpExecuterResults(*tx.Body, Results);
            BuildKqpTaskGraphResultChannels(TasksGraph, *tx.Body, txIdx);
        }

        if (TerminateIfTimeout()) {
            return;
        }

        auto funcRegistry = AppData()->FunctionRegistry;
        NMiniKQL::TScopedAlloc alloc(TAlignedPagePoolCounters(), funcRegistry->SupportsSizedAllocators());
        NMiniKQL::TTypeEnvironment typeEnv(alloc);
        NMiniKQL::TMemoryUsageInfo memInfo("KqpLocalExecuter");
        NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, funcRegistry);

        ui64 mkqlMemoryLimit = Request.MkqlMemoryLimit > 0
            ? Request.MkqlMemoryLimit
            : 1_GB;

        auto rmConfig = GetKqpResourceManager()->GetConfig();
        ui64 mkqlInitialLimit = std::min(mkqlMemoryLimit, rmConfig.GetMkqlLightProgramMemoryLimit());
        alloc.SetLimit(mkqlInitialLimit);

        // TODO: KIKIMR-15350
        alloc.Ref().SetIncreaseMemoryLimitCallback([this, &alloc, mkqlMemoryLimit](ui64 currentLimit, ui64 required) {
            if (required < mkqlMemoryLimit) {
                LOG_D("Increase memory limit from " << currentLimit << " to " << required);
                alloc.SetLimit(required);
            }
        });

        if (prepareTasksSpan) {
            prepareTasksSpan.EndOk();
        }

        NWilson::TSpan runTasksSpan(TWilsonKqp::LiteralExecuterRunTasks, LiteralExecuterSpan.GetTraceId(), "RunTasks", NWilson::EFlags::AUTO_END);

        // task runner settings
        NMiniKQL::TKqpComputeContextBase computeCtx;
        TDqTaskRunnerContext context = CreateTaskRunnerContext(&computeCtx, &alloc, &typeEnv);
        TDqTaskRunnerSettings settings = CreateTaskRunnerSettings(Request.StatsMode);

        Y_DEFER {
            // clear allocator state
            Results.crop(0);
            TaskRunners.crop(0);
        };

        for (auto& task : TasksGraph.GetTasks()) {
            RunTask(task, context, settings);

            if (TerminateIfTimeout()) {
                return;
            }
        }

        if (runTasksSpan) {
            runTasksSpan.End();
        }

        Finalize(context, holderFactory);
        PassAway();
    }

    void RunTask(TTask& task, const TDqTaskRunnerContext& context, const TDqTaskRunnerSettings& settings) {
        auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
        auto& stage = GetStage(stageInfo);

        NDqProto::TDqTask protoTask;
        protoTask.SetId(task.Id);
        protoTask.SetStageId(task.StageId.StageId);
        protoTask.MutableProgram()->CopyFrom(stage.GetProgram()); // it's not good...

        TaskId2StageId[task.Id] = task.StageId.StageId;

        for (auto& output : task.Outputs) {
            YQL_ENSURE(output.Type == TTaskOutputType::Map, "" << output.Type);
            YQL_ENSURE(output.Channels.size() == 1);

            auto* protoOutput = protoTask.AddOutputs();
            protoOutput->MutableMap();

            auto& resultChannel = TasksGraph.GetChannel(output.Channels[0]);
            auto* protoResultChannel = protoOutput->AddChannels();

            protoResultChannel->SetId(resultChannel.Id);
            protoResultChannel->SetSrcTaskId(resultChannel.SrcTask);
            protoResultChannel->SetDstTaskId(resultChannel.DstTask);
            protoResultChannel->SetInMemory(true);

            YQL_ENSURE(resultChannel.SrcTask != 0);
            YQL_ENSURE(resultChannel.DstTask == 0);
        }

        auto parameterProvider = [&task, &stageInfo](std::string_view name, NMiniKQL::TType* type,
            const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
            NUdf::TUnboxedValue& value)
        {
            if (auto* data = task.Meta.Params.FindPtr(name)) {
                TDqDataSerializer::DeserializeParam(*data, type, holderFactory, value);
                return true;
            }

            if (auto* param = stageInfo.Meta.Tx.Params.Values.FindPtr(name)) {
                NMiniKQL::TType* typeFromProto;
                std::tie(typeFromProto, value) = ImportValueFromProto(param->GetType(), param->GetValue(), typeEnv, holderFactory);
#ifndef NDEBUG
                YQL_ENSURE(ToString(*type) == ToString(*typeFromProto), "" << *type << " != " << *typeFromProto);
#else
                Y_UNUSED(typeFromProto);
#endif
                return true;
            }

            return false;
        };

        auto log = [as = TlsActivationContext->ActorSystem(), txId = TxId, taskId = task.Id](const TString& message) {
            LOG_DEBUG_S(*as, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId << ", task: " << taskId << ". "
                << message);
        };

        auto taskRunner = CreateKqpTaskRunner(context, settings, log);
        TaskRunners.emplace_back(taskRunner);

        std::shared_ptr<NMiniKQL::TPatternWithEnv> patternEnv;
        bool useCache = AppData()->FeatureFlags.GetEnableKqpPatternCacheLiteral();
        bool foundInCache = false;
        if (useCache) {
            auto *cache = Singleton<NMiniKQL::TComputationPatternLRUCache>();

            patternEnv = cache->Find(protoTask.GetProgram().GetRaw());
            if (patternEnv) {
                foundInCache = true;
            } else {
                patternEnv = cache->CreateEnv();
            }
        }

        taskRunner->Prepare(protoTask, CreateTaskRunnerMemoryLimits(), CreateTaskRunnerExecutionContext(),
            parameterProvider, patternEnv);

        if (useCache && !foundInCache) {
            auto *cache = Singleton<NMiniKQL::TComputationPatternLRUCache>();
            cache->EmplacePattern(protoTask.GetProgram().GetRaw(), std::move(patternEnv));
        }

        auto status = taskRunner->Run();
        YQL_ENSURE(status == ERunStatus::Finished);

        for (auto& taskOutput : task.Outputs) {
            for (ui64 outputChannelId : taskOutput.Channels) {
                auto outputChannel = taskRunner->GetOutputChannel(outputChannelId);
                auto& channelDesc = TasksGraph.GetChannel(outputChannelId);

                outputChannel->PopAll(Results[channelDesc.DstInputIndex].Rows);
                YQL_ENSURE(outputChannel->IsFinished());
            }
        }
    }

    void Finalize(const TDqTaskRunnerContext& context, NMiniKQL::THolderFactory& holderFactory) {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(Ydb::StatusIds::SUCCESS);
        Counters->TxProxyMon->ReportStatusOK->Inc();

        ui64 rows = 0;
        ui64 bytes = 0;

        TKqpProtoBuilder protoBuilder(context.Alloc, context.TypeEnv, &holderFactory);
        for (auto& result : Results) {
            rows += result.Rows.size();
            auto* protoResult = response.MutableResult()->AddResults();
            if (result.IsStream) {
                protoBuilder.BuildStream(result.Rows, result.ItemType, result.ResultItemType.Get(), protoResult);
            } else {
                protoBuilder.BuildValue(result.Rows, result.ItemType, protoResult);
            }
            bytes += protoResult->ByteSizeLong();
        }

        if (Stats) {
            ui64 elapsedMicros = TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
            TDuration executerCpuTime = TDuration::MicroSeconds(elapsedMicros);

            NYql::NDqProto::TDqComputeActorStats fakeComputeActorStats;

            for (auto& taskRunner : TaskRunners) {
                auto* stats = taskRunner->GetStats();
                auto taskCpuTime = stats->BuildCpuTime + stats->ComputeCpuTime;
                executerCpuTime -= taskCpuTime;
                NYql::NDq::FillTaskRunnerStats(taskRunner->GetTaskId(), TaskId2StageId[taskRunner->GetTaskId()],
                    *stats, fakeComputeActorStats.AddTasks(), CollectProfileStats(Request.StatsMode));
                fakeComputeActorStats.SetCpuTimeUs(fakeComputeActorStats.GetCpuTimeUs() + taskCpuTime.MicroSeconds());
            }

            fakeComputeActorStats.SetDurationUs(elapsedMicros);

            Stats->AddComputeActorStats(SelfId().NodeId(), std::move(fakeComputeActorStats));

            Stats->ExecuterCpuTime = executerCpuTime;
            Stats->FinishTs = Stats->StartTs + TDuration::MicroSeconds(elapsedMicros);
            Stats->ResultRows = rows;
            Stats->ResultBytes = bytes;

            Stats->Finish();

            if (Y_UNLIKELY(CollectFullStats(Request.StatsMode))) {
                for (ui32 txId = 0; txId < Request.Transactions.size(); ++txId) {
                    const auto& tx = Request.Transactions[txId].Body;
                    auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), response.GetResult().GetStats());
                    response.MutableResult()->MutableStats()->AddTxPlansWithStats(planWithStats);
                }
            }
        }

        LWTRACK(KqpLiteralExecuterFinalize, ResponseEv->Orbit, TxId);

        if (LiteralExecuterSpan) {
            LiteralExecuterSpan.EndOk();
        }

        LOG_D("Sending response to: " << Target << ", results: " << Results.size());
        Send(Target, ResponseEv.release());
    }

private:
    bool TerminateIfTimeout() {
        auto now = AppData()->TimeProvider->Now();

        if (Deadline && *Deadline <= now) {
            LOG_I("Timeout exceeded. Send timeout event to the rpc actor " << Target);

            ReplyErrorAndDie(Ydb::StatusIds::TIMEOUT,
                YqlIssue({}, TIssuesIds::KIKIMR_TIMEOUT, "Request timeout exceeded."));
            return true;
        }

        if (CancelAt && *CancelAt <= now) {
            LOG_I("CancelAt exceeded. Send cancel event to the rpc actor " << Target);

            ReplyErrorAndDie(Ydb::StatusIds::CANCELLED,
                YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_CANCELLED, "Request timeout exceeded."));
            return true;
        }

        return false;
    }

private:
    void InternalError(const TString& message) {
        LOG_E(message);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, "Internal error while executing transaction.");
        issue.AddSubIssue(MakeIntrusive<TIssue>(message));
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const TIssue& issue) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        IssueToMessage(issue, issues.Add());
        ReplyErrorAndDie(status, &issues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        if (status != Ydb::StatusIds::SUCCESS) {
            Counters->TxProxyMon->ReportStatusNotOK->Inc();
        } else {
            Counters->TxProxyMon->ReportStatusOK->Inc();
        }

        if (Stats) {
            ui64 elapsedMicros = TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
            Stats->ExecuterCpuTime += TDuration::MicroSeconds(elapsedMicros);
        }

        // TODO: fill stats

        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        LWTRACK(KqpLiteralExecuterReplyErrorAndDie, ResponseEv->Orbit, TxId);

        if (LiteralExecuterSpan) {
            LiteralExecuterSpan.EndError(response.DebugString());
        }

        Send(Target, ResponseEv.release());
        PassAway();
    }

    void PassAway() override {
        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->LiteralTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());

        TBase::PassAway();
    }

private:
    IKqpGateway::TExecPhysicalRequest Request;
    TKqpRequestCounters::TPtr Counters;
    TInstant StartTime;
    std::unique_ptr<TQueryExecutionStats> Stats;
    TMaybe<TInstant> Deadline;
    TMaybe<TInstant> CancelAt;
    TActorId Target;
    ui64 TxId = 0;
    TKqpTasksGraph TasksGraph;
    TVector<TKqpExecuterTxResult> Results;
    TVector<TIntrusivePtr<IDqTaskRunner>> TaskRunners;
    std::unordered_map<ui64, ui32> TaskId2StageId;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    NWilson::TSpan LiteralExecuterSpan;
};

} // anonymous namespace

IActor* CreateKqpLiteralExecuter(IKqpGateway::TExecPhysicalRequest&& request, TKqpRequestCounters::TPtr counters) {
    return new TKqpLiteralExecuter(std::move(request), counters);
}

} // namespace NKqp
} // namespace NKikimr

