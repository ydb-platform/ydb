#include "tasks_runner_local.h"
#include "file_cache.h"

#include <ydb/library/yql/providers/dq/counters/counters.h>
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
#include <library/cpp/svnversion/svnversion.h>

#include <util/system/env.h>
#include <util/stream/file.h>
#include <util/generic/size_literals.h>


namespace NYql::NTaskRunnerProxy {

using namespace NKikimr;
using namespace NDq;

#define ADD_COUNTER(name) \
    if (stats->name) { \
        QueryStat.AddCounter(QueryStat.GetCounterName("TaskRunner", labels, #name), stats->name); \
    }

#define ADD_TIME_COUNTER(name) \
    if (stats->name) { \
        QueryStat.AddTimeCounter(QueryStat.GetCounterName("TaskRunner", labels, #name), stats->name); \
    }

class TLocalInputChannel: public IInputChannel {
public:
    TLocalInputChannel(const IDqInputChannel::TPtr& channel, ui64 taskId, ui64 channelId, TCounters* queryStat)
        : TaskId(taskId)
        , ChannelId(channelId)
        , Channel(channel)
        , QueryStat(*queryStat)
        , Stats(channelId)
    { }

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
        QueryStat.AddInputChannelStats(*Channel->GetStats(), Stats, TaskId, ChannelId);
    }

    ui64 TaskId;
    ui64 ChannelId;
    IDqInputChannel::TPtr Channel;
    TCounters& QueryStat;
    TDqInputChannelStats Stats;
};

class TLocalOutputChannel : public IOutputChannel {
public:
    TLocalOutputChannel(const IDqOutputChannel::TPtr channel, ui64 taskId, ui64 channelId, TCounters* queryStat)
        : TaskId(taskId)
        , ChannelId(channelId)
        , Channel(channel)
        , QueryStat(*queryStat)
        , Stats(channelId)
    { }

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
        QueryStat.AddOutputChannelStats(*Channel->GetStats(), Stats, TaskId, ChannelId);
    }

    ui64 TaskId;
    ui64 ChannelId;
    IDqOutputChannel::TPtr Channel;
    TCounters& QueryStat;
    TDqOutputChannelStats Stats;
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

    NYql::NDqProto::TPrepareResponse Prepare() override {
        NYql::NDqProto::TPrepareResponse ret;
        Runner->Prepare(Task, DefaultMemoryLimits());
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
        return new TLocalInputChannel(Runner->GetInputChannel(channelId), Task.GetId(), channelId, &QueryStat);
    }

    IOutputChannel::TPtr GetOutputChannel(ui64 channelId) override {
        return new TLocalOutputChannel(Runner->GetOutputChannel(channelId), Task.GetId(), channelId, &QueryStat);
    }

    IDqAsyncInputBuffer::TPtr GetSource(ui64 index) override {
        return Runner->GetSource(index);
    }

    IDqAsyncOutputBuffer::TPtr GetSink(ui64 index) override {
        return Runner->GetSink(index);
    }

    const THashMap<TString,TString>& GetTaskParams() const override {
        return Runner->GetTaskParams();
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
        QueryStat.AddTaskRunnerStats(*Runner->GetStats(), Stats, Task.GetId());
    }

    NDq::TDqTaskSettings Task;
    TIntrusivePtr<IDqTaskRunner> Runner;
    TCounters QueryStat;
    TDqTaskRunnerStats Stats;
};

/*______________________________________________________________________________________________*/

class TAbstractFactory: public IProxyFactory {
public:
    TAbstractFactory(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        TTaskTransformFactory taskTransformFactory,
        std::shared_ptr<NKikimr::NMiniKQL::TComputationPatternLRUCache> patternCache)
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
    {
        ExecutionContext.FuncRegistry = FunctionRegistry;
        ExecutionContext.ComputationFactory = compFactory;
        ExecutionContext.RandomProvider = RandomProvider.Get();
        ExecutionContext.TimeProvider = TimeProvider.Get();
        ExecutionContext.PatternCache = patternCache;
    }

protected:
    bool DeterministicMode;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TTaskTransformFactory TaskTransformFactory;

    NDq::TDqTaskRunnerContext ExecutionContext;
};

/*______________________________________________________________________________________________*/

class TLocalFactory: public TAbstractFactory {
public:
    TLocalFactory(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        TTaskTransformFactory taskTransformFactory,
        std::shared_ptr<NKikimr::NMiniKQL::TComputationPatternLRUCache> patternCache,
        bool terminateOnError)
        : TAbstractFactory(functionRegistry, compFactory, taskTransformFactory, patternCache)
        , TerminateOnError(terminateOnError)
    { }

    ITaskRunner::TPtr GetOld(const TDqTaskSettings& task, const TString& traceId) override {
        return new TLocalTaskRunner(task, Get(task, traceId));
    }

    TIntrusivePtr<NDq::IDqTaskRunner> Get(const TDqTaskSettings& task, const TString& traceId) override {
        Y_UNUSED(traceId);
        NDq::TDqTaskRunnerSettings settings;
        settings.TerminateOnError = TerminateOnError;
        settings.CollectBasicStats = true;
        settings.CollectProfileStats = true;

        Yql::DqsProto::TTaskMeta taskMeta;
        task.GetMeta().UnpackTo(&taskMeta);

        for (const auto& s : taskMeta.GetSettings()) {
            if ("OptLLVM" == s.GetName())
                settings.OptLLVM = s.GetValue();
        }
        for (const auto& x : taskMeta.GetSecureParams()) {
            settings.SecureParams[x.first] = x.second;
        }

        for (const auto& x : taskMeta.GetTaskParams()) {
            settings.TaskParams[x.first] = x.second;
        }
        auto ctx = ExecutionContext;
        ctx.FuncProvider = TaskTransformFactory(settings.TaskParams, ctx.FuncRegistry);
        return MakeDqTaskRunner(ctx, settings, { });
    }

private:
    const bool TerminateOnError;
};

IProxyFactory::TPtr CreateFactory(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, std::shared_ptr<NKikimr::NMiniKQL::TComputationPatternLRUCache> patternCache, bool terminateOnError)
{
    return new TLocalFactory(functionRegistry, compFactory, taskTransformFactory, patternCache, terminateOnError);
}

} // namespace NYql::NTaskRunnerProxy
