#pragma once

#include <yql/essentials/minikql/async/async.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <util/generic/hash.h>

namespace NKikimr::NMiniKQL::NAsync {

// Compiler metadata, independent of runtime provider instances and the scalar UDF ABI.
// All async occurrences are effectful: no CSE, constant evaluation or duplication.
struct TCallableDescriptor {
    TString Name;
    TType* InputType = nullptr;
    TType* ResultType = nullptr;
    TString Provider;
    TString Operation;
    ui64 MaxResponseBytes = 1024 * 1024;
};

class TCallableRegistry {
public:
    void Register(TCallableDescriptor descriptor);
    const TCallableDescriptor& Resolve(TStringBuf name) const;
    TRuntimeNode Call(TProgramBuilder& builder, TStringBuf name, TRuntimeNode argument) const;

private:
    THashMap<TString, TCallableDescriptor> Descriptors_;
};

// Typed internal IR, not a SQL syntax or a scalar UDF wrapper. Only explicitly
// supported row expressions are lowered; unknown/lazy/higher-order contexts fail
// preparation. CASE lowers to nested If. Flow is adapted through Stream/Yield.
TRuntimeNode BuildAsyncMap(TProgramBuilder& builder, TRuntimeNode input,
                           TRuntimeNode rowArgument, TRuntimeNode body,
                           const TCallableRegistry& registry, ui32 maxFrames = 16);
IComputationNode* WrapAsyncMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

// Per graph execution, never stored in a cached computation pattern. All methods
// except notification callbacks run on the task owner with its allocator bound.
// The owner MUST call BeginTurn once at the START of each compute event, before
// fetching any stream, including on initial execution and source/timer wakeups.
class TExecutionServices {
public:
    explicit TExecutionServices(TWakeup enqueue, size_t stepBudget = 1024, TLimits limits = {});
    ~TExecutionServices();
    void RegisterProvider(TString name, std::shared_ptr<IProvider> provider);
    bool HasProvider(TStringBuf name) const;
    void BeginTurn();
    bool TryStep();
    void RequestRerun();
    ui64 GetTurn() const;
    TSubmission Submit(TStringBuf provider, TRequest request);
    bool IsReady(TOperationId id) const;
    std::optional<TResult> TakeResult(TOperationId id);
    void CancelAndForget(TOperationId id);
    void Shutdown();
    TStats GetStats() const;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NKikimr::NMiniKQL::NAsync
