#pragma once

#include <ydb/library/yql/providers/dq/api/protos/task_command_executor.pb.h>
#include <ydb/library/yql/dq/common/dq_serialized_batch.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NTaskRunnerProxy {

extern const TString WorkingDirectoryParamName;
extern const TString WorkingDirectoryDontInitParamName; // COMPAT(aozeritsky)
extern const TString UseMetaParamName; // COMPAT(aozeritsky)

i64 SaveRopeToPipe(IOutputStream& output, const TRope& rope);
void LoadRopeFromPipe(IInputStream& input, TRope& rope);
NDq::TDqTaskRunnerMemoryLimits DefaultMemoryLimits();

class IInputChannel : public TThrRefBase, private TNonCopyable {
public:
    using TPtr = TIntrusivePtr<IInputChannel>;

    virtual ~IInputChannel() = default;

    virtual void Push(NDq::TDqSerializedBatch&& data) = 0;

    virtual i64 GetFreeSpace() = 0;

    virtual void Finish() = 0;
};

class IOutputChannel : public TThrRefBase, private TNonCopyable {
public:
    using TPtr = TIntrusivePtr<IOutputChannel>;

    virtual ~IOutputChannel() = default;

    [[nodiscard]]
    virtual NDqProto::TPopResponse Pop(NDq::TDqSerializedBatch& data) = 0;

    virtual bool IsFinished() const = 0;
};

class ITaskRunner: public TThrRefBase, private TNonCopyable {
public:
    using TPtr = TIntrusivePtr<ITaskRunner>;

    virtual ~ITaskRunner() = default;

    virtual ui64 GetTaskId() const = 0;

    virtual NYql::NDqProto::TPrepareResponse Prepare(const NDq::TDqTaskRunnerMemoryLimits& limits = DefaultMemoryLimits()) = 0;
    virtual NYql::NDqProto::TRunResponse Run() = 0;

    virtual IInputChannel::TPtr GetInputChannel(ui64 channelId) = 0;
    virtual IOutputChannel::TPtr GetOutputChannel(ui64 channelId) = 0;
    virtual NDq::IDqAsyncInputBuffer* GetSource(ui64 index) = 0;
    virtual NDq::IDqAsyncOutputBuffer::TPtr GetSink(ui64 index) = 0;

    virtual const THashMap<TString,TString>& GetTaskParams() const = 0;
    virtual const TVector<TString>& GetReadRanges() const = 0;
    virtual const THashMap<TString,TString>& GetSecureParams() const = 0;
    virtual const NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnv() const = 0;
    virtual const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() const = 0;

    // if memoryLimit = Nothing()  then don't set memory limit, use existing one (if any)
    // if memoryLimit = 0          then set unlimited
    // otherwise use particular memory limit
    virtual TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator(TMaybe<ui64> memoryLimit = Nothing()) = 0;
    virtual bool IsAllocatorAttached() = 0;

    struct TStatus {
        int ExitCode;
        TString Stderr;
    };

    virtual i32 GetProtocolVersion() = 0;
    virtual TStatus GetStatus() = 0;
    virtual void Kill() { }
};

class IProxyFactory: public TThrRefBase, private TNonCopyable {
public:
    using TPtr = TIntrusivePtr<IProxyFactory>;

    virtual ITaskRunner::TPtr GetOld(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& task, const TString& traceId = "") = 0;

    virtual TIntrusivePtr<NDq::IDqTaskRunner> Get(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const TString& traceId = "TODO") = 0;
};

} // namespace NYql::NTaskRunnerProxy
