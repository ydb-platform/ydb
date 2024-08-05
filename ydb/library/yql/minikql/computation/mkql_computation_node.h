#pragma once

#include "mkql_computation_node_list.h"
#include "mkql_spiller_factory.h"

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_validate.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <map>
#include <unordered_set>
#include <unordered_map>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {

inline const TDefaultListRepresentation* GetDefaultListRepresentation(const NUdf::TUnboxedValuePod& value) {
    return reinterpret_cast<const TDefaultListRepresentation*>(NUdf::TBoxedValueAccessor::GetListRepresentation(*value.AsBoxed()));
}

enum class EGraphPerProcess {
    Multi,
    Single
};

struct TComputationOpts {
    TComputationOpts(IStatsRegistry* stats)
        : Stats(stats)
    {}

    IStatsRegistry *const Stats;
};

struct TComputationOptsFull: public TComputationOpts {
    TComputationOptsFull(IStatsRegistry* stats, TAllocState& allocState, const TTypeEnvironment* typeEnv, IRandomProvider& randomProvider,
            ITimeProvider& timeProvider, NUdf::EValidatePolicy validatePolicy, const NUdf::ISecureParamsProvider* secureParamsProvider, NUdf::ICountersProvider* countersProvider)
        : TComputationOpts(stats)
        , AllocState(allocState)
        , TypeEnv(typeEnv)
        , RandomProvider(randomProvider)
        , TimeProvider(timeProvider)
        , ValidatePolicy(validatePolicy)
        , SecureParamsProvider(secureParamsProvider)
        , CountersProvider(countersProvider)
    {}

    TComputationOptsFull(IStatsRegistry* stats, const TTypeEnvironment* typeEnv, IRandomProvider& randomProvider,
            ITimeProvider& timeProvider, NUdf::EValidatePolicy validatePolicy, const NUdf::ISecureParamsProvider* secureParamsProvider, NUdf::ICountersProvider* countersProvider)
        : TComputationOpts(stats)
        , AllocState(typeEnv->GetAllocator().Ref())
        , TypeEnv(typeEnv)
        , RandomProvider(randomProvider)
        , TimeProvider(timeProvider)
        , ValidatePolicy(validatePolicy)
        , SecureParamsProvider(secureParamsProvider)
        , CountersProvider(countersProvider)
    {}

    TAllocState& AllocState;
    const TTypeEnvironment *const TypeEnv;
    IRandomProvider& RandomProvider;
    ITimeProvider& TimeProvider;
    NUdf::EValidatePolicy ValidatePolicy;
    const NUdf::ISecureParamsProvider *const SecureParamsProvider;
    NUdf::ICountersProvider *const CountersProvider;
};

struct TWideFieldsInitInfo {
    ui32 MutablesIndex = 0;
    ui32 WideFieldsIndex = 0;
    std::set<ui32> Used;
};

struct TComputationMutables {
    ui32 CurValueIndex = 0U;
    std::vector<ui32> SerializableValues; // Indices of values that need to be saved in IComputationGraph::SaveGraphState() and restored in IComputationGraph::LoadGraphState().
    ui32 CurWideFieldsIndex = 0U;
    std::vector<TWideFieldsInitInfo> WideFieldInitialize;

    void DeferWideFieldsInit(ui32 count, std::set<ui32> used) {
        Y_DEBUG_ABORT_UNLESS(AllOf(used, [count](ui32 i) { return i < count; }));
        WideFieldInitialize.push_back({CurValueIndex, CurWideFieldsIndex, std::move(used)});
        CurValueIndex += count;
        CurWideFieldsIndex += count;
    }

    ui32 IncrementWideFieldsIndex(ui32 addend) {
        auto cur = CurWideFieldsIndex;
        CurWideFieldsIndex += addend;
        return cur;
    }
};

class THolderFactory;

// Do not reorder: used in LLVM!
struct TComputationContextLLVM {
    const THolderFactory& HolderFactory;
    IStatsRegistry *const Stats;
    const std::unique_ptr<NUdf::TUnboxedValue[]> MutableValues;
    const NUdf::IValueBuilder *const Builder;
    float UsageAdjustor = 1.f;
    ui32 RssCounter = 0U;
    const NUdf::TSourcePosition* CalleePosition = nullptr;
};

struct TComputationContext : public TComputationContextLLVM {
    IRandomProvider& RandomProvider;
    ITimeProvider& TimeProvider;
    bool ExecuteLLVM = false;
    arrow::MemoryPool& ArrowMemoryPool;
    std::vector<NUdf::TUnboxedValue*> WideFields;
    const TTypeEnvironment* TypeEnv = nullptr;
    const TComputationMutables Mutables;
    std::shared_ptr<ISpillerFactory> SpillerFactory;
    const NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    NUdf::ICountersProvider *const CountersProvider;
    const NUdf::ISecureParamsProvider *const SecureParamsProvider;

    TComputationContext(const THolderFactory& holderFactory,
        const NUdf::IValueBuilder* builder,
        const TComputationOptsFull& opts,
        const TComputationMutables& mutables,
        arrow::MemoryPool& arrowMemoryPool);

    ~TComputationContext();

    // Returns true if current usage delta exceeds the memory limit
    // The function automatically adjusts memory limit taking into account RSS delta between calls
    template<bool TrackRss>
    inline bool CheckAdjustedMemLimit(ui64 memLimit, ui64 initMemUsage);

    void UpdateUsageAdjustor(ui64 memLimit);
private:
    ui64 InitRss = 0ULL;
    ui64 LastRss = 0ULL;
#ifndef NDEBUG
    TInstant LastPrintUsage;
#endif
};

class IArrowKernelComputationNode;

class IComputationNode {
public:
    typedef TIntrusivePtr<IComputationNode> TPtr;
    typedef std::map<ui32, EValueRepresentation> TIndexesMap;

    virtual ~IComputationNode() {}

    virtual void InitNode(TComputationContext&) const = 0;

    virtual NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const = 0;

    virtual IComputationNode* AddDependence(const IComputationNode* node) = 0;

    virtual const IComputationNode* GetSource() const = 0;

    virtual void RegisterDependencies() const = 0;

    virtual ui32 GetIndex() const = 0;
    virtual void CollectDependentIndexes(const IComputationNode* owner, TIndexesMap& dependencies) const = 0;
    virtual ui32 GetDependencyWeight() const = 0;
    virtual ui32 GetDependencesCount() const = 0;

    virtual bool IsTemporaryValue() const = 0;

    virtual EValueRepresentation GetRepresentation() const = 0;

    virtual void PrepareStageOne() = 0;
    virtual void PrepareStageTwo() = 0;

    virtual TString DebugString() const = 0;

    virtual void Ref() = 0;
    virtual void UnRef() = 0;
    virtual ui32 RefCount() const = 0;

    virtual std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const;
};

class IComputationExternalNode : public IComputationNode {
public:
    virtual NUdf::TUnboxedValue& RefValue(TComputationContext& compCtx) const = 0;
    virtual void SetValue(TComputationContext& compCtx, NUdf::TUnboxedValue&& newValue) const = 0;
    virtual void SetOwner(const IComputationNode* node) = 0;

    using TGetter = std::function<NUdf::TUnboxedValue(TComputationContext&)>;
    virtual void SetGetter(TGetter&& getter) = 0;
    virtual void InvalidateValue(TComputationContext& compCtx) const = 0;
};

enum class EFetchResult : i32 {
    Finish = -1,
    Yield = 0,
    One = 1
};

class IComputationWideFlowNode : public IComputationNode {
public:
    virtual EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue*const* values) const = 0;
};

class IComputationWideFlowProxyNode : public IComputationWideFlowNode {
public:
    using TFetcher = std::function<EFetchResult(TComputationContext&, NUdf::TUnboxedValue*const*)>;
    virtual void SetFetcher(TFetcher&& fetcher) = 0;
    virtual void SetOwner(const IComputationNode* node) = 0;
    virtual void InvalidateValue(TComputationContext& compCtx) const = 0;
};

using TDatumProvider = std::function<arrow::Datum()>;

TDatumProvider MakeDatumProvider(const arrow::Datum& datum);
TDatumProvider MakeDatumProvider(const IComputationNode* node, TComputationContext& ctx);

class IArrowKernelComputationNode {
public:
    virtual ~IArrowKernelComputationNode() = default;

    virtual TStringBuf GetKernelName() const = 0;
    virtual const arrow::compute::ScalarKernel& GetArrowKernel() const = 0;
    virtual const std::vector<arrow::ValueDescr>& GetArgsDesc() const = 0;
    virtual const IComputationNode* GetArgument(ui32 index) const = 0;
};

struct TArrowKernelsTopologyItem {
    std::vector<ui32> Inputs;
    std::unique_ptr<IArrowKernelComputationNode> Node;
};

struct TArrowKernelsTopology {
    ui32 InputArgsCount = 0;
    std::vector<TArrowKernelsTopologyItem> Items;
};

using TComputationNodePtrVector = std::vector<IComputationNode*, TMKQLAllocator<IComputationNode*>>;
using TComputationWideFlowNodePtrVector = std::vector<IComputationWideFlowNode*, TMKQLAllocator<IComputationWideFlowNode*>>;
using TComputationExternalNodePtrVector = std::vector<IComputationExternalNode*, TMKQLAllocator<IComputationExternalNode*>>;
using TConstComputationNodePtrVector = std::vector<const IComputationNode*, TMKQLAllocator<const IComputationNode*>>;
using TComputationNodePtrDeque = std::deque<IComputationNode::TPtr, TMKQLAllocator<IComputationNode::TPtr>>;
using TComputationNodeOnNodeMap = std::unordered_map<const IComputationNode*, IComputationNode*, std::hash<const IComputationNode*>, std::equal_to<const IComputationNode*>, TMKQLAllocator<std::pair<const IComputationNode *const, IComputationNode*>>>;

class IComputationGraph {
public:
    virtual ~IComputationGraph() {}
    virtual void Prepare() = 0;
    virtual NUdf::TUnboxedValue GetValue() = 0;
    virtual TComputationContext& GetContext() = 0;
    virtual IComputationExternalNode* GetEntryPoint(size_t index, bool require) = 0;
    virtual const TArrowKernelsTopology* GetKernelsTopology() = 0;
    virtual const TComputationNodePtrDeque& GetNodes() const = 0;
    virtual void Invalidate() = 0;
    virtual TMemoryUsageInfo& GetMemInfo() const = 0;
    virtual const THolderFactory& GetHolderFactory() const = 0;
    virtual ITerminator* GetTerminator() const = 0;
    virtual bool SetExecuteLLVM(bool value) = 0;
    virtual TString SaveGraphState() = 0;
    virtual void LoadGraphState(TStringBuf state) = 0;
};

class TNodeFactory;
typedef std::function<IComputationNode* (TNode* node, bool pop)> TNodeLocator;
typedef std::function<void (IComputationNode*)> TNodePushBack;

struct TComputationNodeFactoryContext {
    TNodeLocator NodeLocator;
    const IFunctionRegistry& FunctionRegistry;
    const TTypeEnvironment& Env;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    NUdf::ICountersProvider* CountersProvider;
    const NUdf::ISecureParamsProvider* SecureParamsProvider;
    const TNodeFactory& NodeFactory;
    const THolderFactory& HolderFactory;
    const NUdf::IValueBuilder *const Builder;
    NUdf::EValidateMode ValidateMode;
    NUdf::EValidatePolicy ValidatePolicy;
    EGraphPerProcess GraphPerProcess;
    TComputationMutables& Mutables;
    TComputationNodeOnNodeMap& ElementsCache;
    const TNodePushBack NodePushBack;

    TComputationNodeFactoryContext(
            const TNodeLocator& nodeLocator,
            const IFunctionRegistry& functionRegistry,
            const TTypeEnvironment& env,
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
            NUdf::ICountersProvider* countersProvider,
            const NUdf::ISecureParamsProvider* secureParamsProvider,
            const TNodeFactory& nodeFactory,
            const THolderFactory& holderFactory,
            const NUdf::IValueBuilder* builder,
            NUdf::EValidateMode validateMode,
            NUdf::EValidatePolicy validatePolicy,
            EGraphPerProcess graphPerProcess,
            TComputationMutables& mutables,
            TComputationNodeOnNodeMap& elementsCache,
            TNodePushBack&& nodePushBack
            )
        : NodeLocator(nodeLocator)
        , FunctionRegistry(functionRegistry)
        , Env(env)
        , TypeInfoHelper(typeInfoHelper)
        , CountersProvider(countersProvider)
        , SecureParamsProvider(secureParamsProvider)
        , NodeFactory(nodeFactory)
        , HolderFactory(holderFactory)
        , Builder(builder)
        , ValidateMode(validateMode)
        , ValidatePolicy(validatePolicy)
        , GraphPerProcess(graphPerProcess)
        , Mutables(mutables)
        , ElementsCache(elementsCache)
        , NodePushBack(std::move(nodePushBack))
    {}
};

using TComputationNodeFactory = std::function<IComputationNode* (TCallable&, const TComputationNodeFactoryContext&)>;
using TStreamEmitter = std::function<void(NUdf::TUnboxedValue&&)>;

struct TPatternCacheEntry;

struct TComputationPatternOpts {
    TComputationPatternOpts(TAllocState& allocState, const TTypeEnvironment& env)
        : AllocState(allocState)
        , Env(env)
    {}

    TComputationPatternOpts(
        TAllocState& allocState,
        const TTypeEnvironment& env,
        TComputationNodeFactory factory,
        const IFunctionRegistry* functionRegistry,
        NUdf::EValidateMode validateMode,
        NUdf::EValidatePolicy validatePolicy,
        const TString& optLLVM,
        EGraphPerProcess graphPerProcess,
        IStatsRegistry* stats = nullptr,
        NUdf::ICountersProvider* countersProvider = nullptr,
        const NUdf::ISecureParamsProvider* secureParamsProvider = nullptr)
        : AllocState(allocState)
        , Env(env)
        , Factory(factory)
        , FunctionRegistry(functionRegistry)
        , ValidateMode(validateMode)
        , ValidatePolicy(validatePolicy)
        , OptLLVM(optLLVM)
        , GraphPerProcess(graphPerProcess)
        , Stats(stats)
        , CountersProvider(countersProvider)
        , SecureParamsProvider(secureParamsProvider)
    {}

    void SetOptions(TComputationNodeFactory factory, const IFunctionRegistry* functionRegistry,
        NUdf::EValidateMode validateMode, NUdf::EValidatePolicy validatePolicy,
        const TString& optLLVM, EGraphPerProcess graphPerProcess, IStatsRegistry* stats = nullptr,
        NUdf::ICountersProvider* counters = nullptr,
        const NUdf::ISecureParamsProvider* secureParamsProvider = nullptr) {
        Factory = factory;
        FunctionRegistry = functionRegistry;
        ValidateMode = validateMode;
        ValidatePolicy = validatePolicy;
        OptLLVM = optLLVM;
        GraphPerProcess = graphPerProcess;
        Stats = stats;
        CountersProvider = counters;
        SecureParamsProvider = secureParamsProvider;
    }

    void SetPatternEnv(std::shared_ptr<TPatternCacheEntry> cacheEnv) {
        PatternEnv = std::move(cacheEnv);
    }

    mutable std::shared_ptr<TPatternCacheEntry> PatternEnv;
    TAllocState& AllocState;
    const TTypeEnvironment& Env;

    TComputationNodeFactory Factory;
    const IFunctionRegistry* FunctionRegistry = nullptr;
    NUdf::EValidateMode ValidateMode = NUdf::EValidateMode::None;
    NUdf::EValidatePolicy ValidatePolicy = NUdf::EValidatePolicy::Fail;
    TString OptLLVM;
    EGraphPerProcess GraphPerProcess = EGraphPerProcess::Multi;
    IStatsRegistry* Stats = nullptr;
    NUdf::ICountersProvider* CountersProvider = nullptr;
    const NUdf::ISecureParamsProvider* SecureParamsProvider = nullptr;

    /// \todo split and exclude
    TComputationOptsFull ToComputationOptions(IRandomProvider& randomProvider, ITimeProvider& timeProvider, const TTypeEnvironment* typeEnv, TAllocState* allocStatePtr = nullptr) const {
        return TComputationOptsFull(Stats, allocStatePtr ? *allocStatePtr : AllocState, typeEnv, randomProvider, timeProvider, ValidatePolicy, SecureParamsProvider, CountersProvider);
    }

    TComputationOptsFull ToComputationOptions(IRandomProvider& randomProvider, ITimeProvider& timeProvider, TTypeEnvironment* typeEnv) const {
        return TComputationOptsFull(Stats, typeEnv, randomProvider, timeProvider, ValidatePolicy, SecureParamsProvider, CountersProvider);
    }
};

class IComputationPattern: public TAtomicRefCount<IComputationPattern> {
public:
    typedef TIntrusivePtr<IComputationPattern> TPtr;

    virtual ~IComputationPattern() = default;
    virtual void Compile(TString optLLVM, IStatsRegistry* stats) = 0;
    virtual bool IsCompiled() const = 0;
    virtual size_t CompiledCodeSize() const = 0;
    virtual void RemoveCompiledCode() = 0;
    virtual THolder<IComputationGraph> Clone(const TComputationOptsFull& compOpts) = 0;
    virtual bool GetSuitableForCache() const = 0;
};

// node cookie's will be clean up when graph will be destroyed, explorer must not be changed/destroyed until that time
IComputationPattern::TPtr MakeComputationPattern(
        TExploringNodeVisitor& explorer,
        const TRuntimeNode& root,
        const std::vector<TNode*>& entryPoints,
        const TComputationPatternOpts& opts);

std::unique_ptr<NUdf::ISecureParamsProvider> MakeSimpleSecureParamsProvider(const THashMap<TString, TString>& secureParams);

using TCallableComputationNodeBuilder = std::function<IComputationNode* (TCallable&, const TComputationNodeFactoryContext& ctx)>;

template<typename... Ts>
TCallableComputationNodeBuilder WrapComputationBuilder(IComputationNode* (*f)(const TComputationNodeFactoryContext&, Ts...)){
    return [f](TCallable& callable, const TComputationNodeFactoryContext& ctx) {
        MKQL_ENSURE(callable.GetInputsCount() == sizeof...(Ts), "Incorrect number of inputs");
        return CallComputationBuilderWithArgs(f, callable, ctx, std::make_index_sequence<sizeof...(Ts)>());
    };
}
template<typename F, size_t... Is>
auto CallComputationBuilderWithArgs(F* f, TCallable& callable, const TComputationNodeFactoryContext& ctx,
                                           const std::integer_sequence<size_t, Is...> &)  {
    return f(ctx, callable.GetInput(Is)...);
}

} // namespace NMiniKQL
} // namespace NKikimr
