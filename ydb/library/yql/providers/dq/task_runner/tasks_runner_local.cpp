#include "tasks_runner_local.h"
#include "file_cache.h"

#include <ydb/library/yql/providers/dq/counters/task_counters.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/system/env.h>
#include <util/stream/file.h>
#include <util/generic/size_literals.h>


namespace NYql::NTaskRunnerProxy {

using namespace NKikimr;
using namespace NDq;

class TLocalInputChannel: public IInputChannel {
public:
    TLocalInputChannel(const IDqInputChannel::TPtr& channel, ui64 taskId, ui32 stageId, TTaskCounters* queryStat)
        : TaskId(taskId)
        , StageId(stageId)
        , Channel(channel)
        , QueryStat(*queryStat)
    {}

    void Push(TDqSerializedBatch&& data) override {
        Channel->Push(std::move(data));
    }

    i64 GetFreeSpace() override {
        return Channel->GetFreeSpace();
    }

    void Finish() override {
        Channel->Finish();
        UpdateInputChannelStats();
    }

private:
    void UpdateInputChannelStats()
    {
        QueryStat.AddInputChannelStats(Channel->GetPushStats(), Channel->GetPopStats(), TaskId, StageId);
    }

    ui64 TaskId;
    ui32 StageId;
    IDqInputChannel::TPtr Channel;
    TTaskCounters& QueryStat;
};

class TLocalOutputChannel : public IOutputChannel {
public:
    TLocalOutputChannel(const IDqOutputChannel::TPtr channel, ui64 taskId, ui32 stageId, TTaskCounters* queryStat)
        : TaskId(taskId)
        , StageId(stageId)
        , Channel(channel)
        , QueryStat(*queryStat)
    {}

    [[nodiscard]]
    NDqProto::TPopResponse Pop(TDqSerializedBatch& data) override {
        NDqProto::TPopResponse response;
        response.SetResult(Channel->Pop(data));
        if (Channel->IsFinished()) {
            UpdateOutputChannelStats();
            QueryStat.FlushCounters(response);
        }
        return response;
    }

    bool IsFinished() const override {
        return Channel->IsFinished();
    }

private:
    void UpdateOutputChannelStats()
    {
        QueryStat.AddOutputChannelStats(Channel->GetPushStats(), Channel->GetPopStats(), TaskId, StageId);
    }

    ui64 TaskId;
    ui32 StageId;
    IDqOutputChannel::TPtr Channel;
    TTaskCounters& QueryStat;
};

class TLocalTaskRunner: public ITaskRunner {
public:
    TLocalTaskRunner(const NDq::TDqTaskSettings& task, TIntrusivePtr<IDqTaskRunner> runner)
        : Task(task)
        , Runner(runner)
    { }

    ~TLocalTaskRunner()
    { }

    i32 GetProtocolVersion() override {
        return 1;
    }

    ui64 GetTaskId() const override {
        return Task.GetId();
    }

    NYql::NDqProto::TPrepareResponse Prepare(const NDq::TDqTaskRunnerMemoryLimits& limits) override {
        NYql::NDqProto::TPrepareResponse ret;
        TDqTaskRunnerExecutionContextDefault ctx;
        Runner->Prepare(Task, limits, ctx);
        return ret;
    }

    NYql::NDqProto::TRunResponse Run() override {
        auto status = Runner->Run();
        NYql::NDqProto::TRunResponse response;
        response.SetResult(static_cast<ui32>(status));

        if (status == NDq::ERunStatus::Finished) {
            UpdateStats();
            QueryStat.FlushCounters(response);
        }
        return response;
    }

    IInputChannel::TPtr GetInputChannel(ui64 channelId) override {
        return new TLocalInputChannel(Runner->GetInputChannel(channelId), Task.GetId(), Task.GetStageId(), &QueryStat);
    }

    IOutputChannel::TPtr GetOutputChannel(ui64 channelId) override {
        return new TLocalOutputChannel(Runner->GetOutputChannel(channelId), Task.GetId(), Task.GetStageId(), &QueryStat);
    }

    IDqAsyncInputBuffer* GetSource(ui64 index) override {
        return Runner->GetSource(index).Get();
    }

    IDqAsyncOutputBuffer::TPtr GetSink(ui64 index) override {
        return Runner->GetSink(index);
    }

    const THashMap<TString,TString>& GetTaskParams() const override {
        return Runner->GetTaskParams();
    }

    const TVector<TString>& GetReadRanges() const override {
        return Runner->GetReadRanges();
    }

    const THashMap<TString,TString>& GetSecureParams() const override {
        return Runner->GetSecureParams();
    }

    const NMiniKQL::TTypeEnvironment& GetTypeEnv() const override {
        return Runner->GetTypeEnv();
    }

    const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() const override {
        return Runner->GetHolderFactory();
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator(TMaybe<ui64> memoryLimit) override {
        return Runner->BindAllocator(memoryLimit);
    }

    bool IsAllocatorAttached() override {
        return Runner->IsAllocatorAttached();
    }

    TStatus GetStatus() override {
        return {0, ""};
    }

private:
    void UpdateStats() {
        QueryStat.AddTaskRunnerStats(*Runner->GetStats(), Task.GetId(), Task.GetStageId());
    }

    NDq::TDqTaskSettings Task;
    TIntrusivePtr<IDqTaskRunner> Runner;
    TTaskCounters QueryStat;
};

/*______________________________________________________________________________________________*/

class TLocalFactory: public IProxyFactory {
public:
    TLocalFactory(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        TTaskTransformFactory taskTransformFactory,
        std::shared_ptr<NKikimr::NMiniKQL::TComputationPatternLRUCache> patternCache,
        bool terminateOnError)
        : DeterministicMode(!!GetEnv("YQL_DETERMINISTIC_MODE"))
        , RandomProvider(
            DeterministicMode
                ? CreateDeterministicRandomProvider(1)
                : CreateDefaultRandomProvider())
        , TimeProvider(
            DeterministicMode
                ? CreateDeterministicTimeProvider(10000000)
                : CreateDefaultTimeProvider())
        , FunctionRegistry(functionRegistry)
        , TaskTransformFactory(std::move(taskTransformFactory))
        , TerminateOnError(terminateOnError)
    {
        ExecutionContext.FuncRegistry = FunctionRegistry;
        ExecutionContext.ComputationFactory = compFactory;
        ExecutionContext.RandomProvider = RandomProvider.Get();
        ExecutionContext.TimeProvider = TimeProvider.Get();
        ExecutionContext.PatternCache = patternCache;
    }

    ITaskRunner::TPtr GetOld(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const TDqTaskSettings& task, const TString& traceId) override {
        return new TLocalTaskRunner(task, Get(alloc, task, NDqProto::DQ_STATS_MODE_BASIC, traceId));
    }

    TIntrusivePtr<NDq::IDqTaskRunner> Get(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const TString& traceId) override {
        Y_UNUSED(traceId);
        NDq::TDqTaskRunnerSettings settings;
        settings.TerminateOnError = TerminateOnError;
        settings.StatsMode = statsMode;

        Yql::DqsProto::TTaskMeta taskMeta;
        task.GetMeta().UnpackTo(&taskMeta);

        for (const auto& s : taskMeta.GetSettings()) {
            if ("OptLLVM" == s.GetName()) {
                settings.OptLLVM = s.GetValue();
            }
            if ("TaskRunnerStats" == s.GetName()) {
                if ("Disable" == s.GetValue()) {
                    settings.StatsMode = NDqProto::DQ_STATS_MODE_NONE;
                } else if ("Basic" == s.GetValue()) {
                    settings.StatsMode = NDqProto::DQ_STATS_MODE_BASIC;
                } else if ("Full" == s.GetValue()) {
                    settings.StatsMode = NDqProto::DQ_STATS_MODE_FULL;
                } else if ("Profile" == s.GetValue()) {
                    settings.StatsMode = NDqProto::DQ_STATS_MODE_PROFILE;
                }
            }
        }
        for (const auto& x : taskMeta.GetSecureParams()) {
            settings.SecureParams[x.first] = x.second;
        }

        for (const auto& x : taskMeta.GetTaskParams()) {
            settings.TaskParams[x.first] = x.second;
        }

        for (const auto& readRange : taskMeta.GetReadRanges()) {
            settings.ReadRanges.push_back(readRange);
        }
        auto ctx = ExecutionContext;
        ctx.FuncProvider = TaskTransformFactory(settings.TaskParams, ctx.FuncRegistry);
        return MakeDqTaskRunner(alloc, ctx, settings, [taskId = task.GetId(), traceId = traceId](const TString& message) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(traceId, ToString(taskId));
            YQL_CLOG(DEBUG, ProviderDq) << message;
        });
    }

private:
    bool DeterministicMode;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TTaskTransformFactory TaskTransformFactory;
    NDq::TDqTaskRunnerContext ExecutionContext;
    const bool TerminateOnError;
};

IProxyFactory::TPtr CreateFactory(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, std::shared_ptr<NKikimr::NMiniKQL::TComputationPatternLRUCache> patternCache, bool terminateOnError)
{
    return new TLocalFactory(functionRegistry, compFactory, taskTransformFactory, patternCache, terminateOnError);
}

} // namespace NYql::NTaskRunnerProxy
