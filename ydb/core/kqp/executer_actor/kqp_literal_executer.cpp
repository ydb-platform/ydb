#include "kqp_executer.h"
#include "kqp_executer_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

namespace {

std::unique_ptr<TDqTaskRunnerContext> CreateTaskRunnerContext(NMiniKQL::TKqpComputeContextBase* computeCtx,
    NMiniKQL::TTypeEnvironment* typeEnv)
{
    std::unique_ptr<TDqTaskRunnerContext> context = std::make_unique<TDqTaskRunnerContext>();
    context->FuncRegistry = AppData()->FunctionRegistry;
    context->RandomProvider = TAppData::RandomProvider.Get();
    context->TimeProvider = TAppData::TimeProvider.Get();
    context->ComputeCtx = computeCtx;

    auto computeFactory = NMiniKQL::GetKqpBaseComputeFactory(computeCtx);
    context->ComputationFactory =
        [computeFactory](NMiniKQL::TCallable& callable, const NMiniKQL::TComputationNodeFactoryContext& ctx)
        -> NMiniKQL::IComputationNode*
    {
        if (auto compute = computeFactory(callable, ctx)) {
            return compute;
        }
        auto name = callable.GetType()->GetName();
        // only for _pure_ compute actors!
        if (name == "KqpEnsure"sv) {
            return WrapKqpEnsure(callable, ctx);
        }
        return nullptr;
    };

    context->TypeEnv = typeEnv;
    context->ApplyCtx = nullptr;
    return context;
}

TDqTaskRunnerSettings CreateTaskRunnerSettings(Ydb::Table::QueryStatsCollection::Mode statsMode) {
    TDqTaskRunnerSettings settings;
    // Always collect basic stats for system views / request unit computation.
    settings.StatsMode = GetDqStatsMode(statsMode);
    settings.OptLLVM = "OFF";
    settings.TerminateOnError = false;
    return settings;
}

TDqTaskRunnerMemoryLimits CreateTaskRunnerMemoryLimits() {
    TDqTaskRunnerMemoryLimits memoryLimits;
    memoryLimits.ChannelBufferSize = std::numeric_limits<ui32>::max();
    memoryLimits.OutputChunkMaxSize = std::numeric_limits<ui32>::max();
    return memoryLimits;
}

TDqTaskRunnerExecutionContextDefault CreateTaskRunnerExecutionContext() {
    return {};
}

class TKqpLiteralExecuter {
public:
    TKqpLiteralExecuter(IKqpGateway::TExecPhysicalRequest&& request, TKqpRequestCounters::TPtr counters, TActorId owner,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext)
        : Request(std::move(request))
        , Counters(counters)
        , OwnerActor(owner)
        , LiteralExecuterSpan(TWilsonKqp::LiteralExecuter, std::move(Request.TraceId), "LiteralExecuter")
        , UserRequestContext(userRequestContext)
    {
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(
            Request.TxAlloc, TEvKqpExecuter::TEvTxResponse::EExecutionType::Literal);

        ResponseEv->Orbit = std::move(Request.Orbit);
        Stats = std::make_unique<TQueryExecutionStats>(Request.StatsMode, &TasksGraph,
            ResponseEv->Record.MutableResponse()->MutableResult()->MutableStats());
        StartTime = TAppData::TimeProvider->Now();
        if (Request.Timeout) {
            Deadline = StartTime + Request.Timeout;
        }
        if (Request.CancelAfter) {
            CancelAt = StartTime + *Request.CancelAfter;
        }

        LOG_D("Begin literal execution. Operation timeout: " << Request.Timeout << ", cancelAfter: " << Request.CancelAfter);
    }

    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ExecuteLiteral() {
        try {
            ExecuteLiteralImpl();
        } catch (const TMemoryLimitExceededException&) {
            LOG_W("TKqpLiteralExecuter, memory limit exceeded.");
            CreateErrorResponse(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, "Memory limit exceeded"));
        } catch (const NMiniKQL::TKqpEnsureFail& e) {
            LOG_E("TKqpLiteralExecuter, TKqpEnsure failed.");
            CreateErrorResponse(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, EYqlIssueCode(e.GetCode()), e.GetMessage()));
        } catch (...) {
            auto msg = CurrentExceptionMessage();
            LOG_C("TKqpLiteralExecuter, unexpected exception caught: " << msg);
            CreateErrorResponse(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, msg));
        }

        return std::move(ResponseEv);
    }

    void ExecuteLiteralImpl() {
        if (Stats) {
            Stats->StartTs = TInstant::Now();
        }

        LOG_D("Begin literal execution, txs: " << Request.Transactions.size());
        auto& transactions = Request.Transactions;
        FillKqpTasksGraphStages(TasksGraph, transactions);

        for (ui32 txIdx = 0; txIdx < transactions.size(); ++txIdx) {
            auto& tx = transactions[txIdx];

            for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
                auto& stage = tx.Body->GetStages(stageIdx);
                auto& stageInfo = TasksGraph.GetStageInfo(TStageId(txIdx, stageIdx));
                LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

                YQL_ENSURE(stageInfo.Meta.ShardOperations.empty());
                YQL_ENSURE(stageInfo.InputsCount == 0);

                TasksGraph.AddTask(stageInfo);
            }

            ResponseEv->InitTxResult(tx.Body);
            BuildKqpTaskGraphResultChannels(TasksGraph, tx.Body, txIdx);
        }

        if (TerminateIfTimeout()) {
            return;
        }

        // task runner settings
        ComputeCtx = std::make_unique<NMiniKQL::TKqpComputeContextBase>();
        RunnerContext = CreateTaskRunnerContext(ComputeCtx.get(), &Request.TxAlloc->TypeEnv);
        RunnerContext->PatternCache = GetKqpResourceManager()->GetPatternCache();
        TDqTaskRunnerSettings settings = CreateTaskRunnerSettings(Request.StatsMode);

        for (auto& task : TasksGraph.GetTasks()) {
            RunTask(Request.TxAlloc->Alloc, task, *RunnerContext, settings);

            if (TerminateIfTimeout()) {
                return;
            }
        }

        Finalize();
        UpdateCounters();
    }

    void RunTask(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, TTask& task, const TDqTaskRunnerContext& context, const TDqTaskRunnerSettings& settings) {
        auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        NDqProto::TDqTask protoTask;
        protoTask.SetId(task.Id);
        protoTask.SetStageId(task.StageId.StageId);
        protoTask.SetEnableSpilling(false); // TODO: enable spilling
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

        auto log = [as = TlsActivationContext->ActorSystem(), txId = TxId, taskId = task.Id](const TString& message) {
            LOG_DEBUG_S(*as, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId << ", task: " << taskId << ". "
                << message);
        };

        auto taskRunner = MakeDqTaskRunner(alloc, context, settings, log);
        TaskRunners.emplace_back(taskRunner);

        auto taskSettings = NDq::TDqTaskSettings(&protoTask);
        taskSettings.SetParamsProvider(std::move(TQueryData::GetParameterProvider(stageInfo.Meta.Tx.Params)));
        taskRunner->Prepare(taskSettings, CreateTaskRunnerMemoryLimits(), CreateTaskRunnerExecutionContext());

        auto status = taskRunner->Run();
        YQL_ENSURE(status == ERunStatus::Finished);

        with_lock (*alloc) { // allocator is used only by outputChannel->PopAll()
            for (auto& taskOutput : task.Outputs) {
                for (ui64 outputChannelId : taskOutput.Channels) {
                    auto outputChannel = taskRunner->GetOutputChannel(outputChannelId);
                    auto& channelDesc = TasksGraph.GetChannel(outputChannelId);
                    NYql::NDq::TDqSerializedBatch outputData;
                    while (outputChannel->Pop(outputData)) {
                        ResponseEv->TakeResult(channelDesc.DstInputIndex, std::move(outputData));
                        outputData = {};
                    }
                    YQL_ENSURE(outputChannel->IsFinished());
                }
            }
        }
    }

    void Finalize() {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(Ydb::StatusIds::SUCCESS);
        Counters->TxProxyMon->ReportStatusOK->Inc();

        if (Stats) {
            ui64 elapsedMicros = TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
            TDuration executerCpuTime = TDuration::MicroSeconds(elapsedMicros);

            NYql::NDqProto::TDqComputeActorStats fakeComputeActorStats;

            for (auto& taskRunner : TaskRunners) {
                auto* stats = taskRunner->GetStats();
                auto taskCpuTime = stats->BuildCpuTime + stats->ComputeCpuTime;
                executerCpuTime -= taskCpuTime;
                NYql::NDq::FillTaskRunnerStats(taskRunner->GetTaskId(), TaskId2StageId[taskRunner->GetTaskId()],
                    *stats, fakeComputeActorStats.AddTasks(), StatsModeToCollectStatsLevel(GetDqStatsMode(Request.StatsMode)));
                fakeComputeActorStats.SetCpuTimeUs(fakeComputeActorStats.GetCpuTimeUs() + taskCpuTime.MicroSeconds());
            }

            fakeComputeActorStats.SetDurationUs(elapsedMicros);

            Stats->AddComputeActorStats(OwnerActor.NodeId(), std::move(fakeComputeActorStats));

            Stats->ExecuterCpuTime = executerCpuTime;
            Stats->FinishTs = Stats->StartTs + TDuration::MicroSeconds(elapsedMicros);
            Stats->ResultRows = ResponseEv->GetResultRowsCount();
            Stats->ResultBytes = ResponseEv->GetByteSize();

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
        LiteralExecuterSpan.EndOk();
        CleanupCtx();
        LOG_D("Execution is complete, results: " << ResponseEv->ResultsSize());
    }

private:
    void CleanupCtx() {
        with_lock(*Request.TxAlloc->Alloc) {
            TaskRunners.erase(TaskRunners.begin(), TaskRunners.end());
            Request.Transactions.erase(Request.Transactions.begin(), Request.Transactions.end());
            ComputeCtx.reset();
            RunnerContext.reset();
        }
    }

    bool TerminateIfTimeout() {
        auto now = AppData()->TimeProvider->Now();

        if (Deadline && *Deadline <= now) {
            LOG_I("Timeout exceeded.");

            CreateErrorResponse(Ydb::StatusIds::TIMEOUT,
                YqlIssue({}, TIssuesIds::KIKIMR_TIMEOUT, "Request timeout exceeded."));
            return true;
        }

        if (CancelAt && *CancelAt <= now) {
            LOG_I("CancelAt exceeded.");

            CreateErrorResponse(Ydb::StatusIds::CANCELLED,
                YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_CANCELLED, "Request timeout exceeded."));
            return true;
        }

        return false;
    }

private:
    TActorId SelfId() const {
        return {};
    }

    void InternalError(const TString& message) {
        LOG_E(message);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, "Internal error while executing transaction.");
        issue.AddSubIssue(MakeIntrusive<TIssue>(message));
        CreateErrorResponse(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void CreateErrorResponse(Ydb::StatusIds::StatusCode status, const TIssue& issue) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        IssueToMessage(issue, issues.Add());
        CreateErrorResponse(status, &issues);
    }

    void CreateErrorResponse(Ydb::StatusIds::StatusCode status,
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

        LWTRACK(KqpLiteralExecuterCreateErrorResponse, ResponseEv->Orbit, TxId);

        LiteralExecuterSpan.EndError(response.DebugString());

        CleanupCtx();
        UpdateCounters();
    }

    void UpdateCounters() {
        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->LiteralTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());
    }

    const TIntrusivePtr<TUserRequestContext>& GetUserRequestContext() {
        return UserRequestContext;
    }

private:
    IKqpGateway::TExecPhysicalRequest Request;
    TKqpRequestCounters::TPtr Counters;
    TInstant StartTime;
    std::unique_ptr<TQueryExecutionStats> Stats;
    TMaybe<TInstant> Deadline;
    TMaybe<TInstant> CancelAt;
    TActorId OwnerActor;
    ui64 TxId = 0;
    TKqpTasksGraph TasksGraph;
    std::unordered_map<ui64, ui32> TaskId2StageId;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;

    TVector<TIntrusivePtr<NYql::NDq::IDqTaskRunner>> TaskRunners;
    std::unique_ptr<NKikimr::NMiniKQL::TKqpComputeContextBase> ComputeCtx;
    std::unique_ptr<NYql::NDq::TDqTaskRunnerContext> RunnerContext;
    NWilson::TSpan LiteralExecuterSpan;

    TIntrusivePtr<TUserRequestContext> UserRequestContext;
};

} // anonymous namespace

std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ExecuteLiteral(
    IKqpGateway::TExecPhysicalRequest&& request, TKqpRequestCounters::TPtr counters, TActorId owner, const TIntrusivePtr<TUserRequestContext>& userRequestContext)
{
    std::unique_ptr<TKqpLiteralExecuter> executer = std::make_unique<TKqpLiteralExecuter>(
        std::move(request), counters, owner, userRequestContext);

    return executer->ExecuteLiteral();
}

} // namespace NKqp
} // namespace NKikimr
