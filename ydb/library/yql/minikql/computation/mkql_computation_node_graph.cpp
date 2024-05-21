#include "mkql_computation_node_holders.h"
#include "mkql_computation_node_holders_codegen.h"
#include "mkql_value_builder.h"
#include "mkql_computation_node_codegen.h" // Y_IGNORE
#include <ydb/library/yql/minikql/arrow/mkql_memory_pool.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_pattern_cache.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <util/system/env.h>
#include <util/system/mutex.h>
#include <util/digest/city.h>

#ifndef MKQL_DISABLE_CODEGEN
#include <llvm/Support/raw_ostream.h>  // Y_IGNORE
#endif

namespace NKikimr {
namespace NMiniKQL {

using namespace NDetail;

namespace {

#ifndef MKQL_DISABLE_CODEGEN
constexpr ui64 TotalFunctionsLimit = 1000;
constexpr ui64 TotalInstructionsLimit = 100000;
constexpr ui64 MaxFunctionInstructionsLimit = 50000;
#endif

const ui64 IS_NODE_REACHABLE = 1;

const static TStatKey PagePool_PeakAllocated("PagePool_PeakAllocated", false);
const static TStatKey PagePool_PeakUsed("PagePool_PeakUsed", false);
const static TStatKey PagePool_AllocCount("PagePool_AllocCount", true);
const static TStatKey PagePool_PageAllocCount("PagePool_PageAllocCount", true);
const static TStatKey PagePool_PageHitCount("PagePool_PageHitCount", true);
const static TStatKey PagePool_PageMissCount("PagePool_PageMissCount", true);
const static TStatKey PagePool_OffloadedAllocCount("PagePool_OffloadedAllocCount", true);
const static TStatKey PagePool_OffloadedBytes("PagePool_OffloadedBytes", true);

const static TStatKey CodeGen_FullTime("CodeGen_FullTime", true);
const static TStatKey CodeGen_GenerateTime("CodeGen_GenerateTime", true);
const static TStatKey CodeGen_CompileTime("CodeGen_CompileTime", true);
const static TStatKey CodeGen_TotalFunctions("CodeGen_TotalFunctions", true);
const static TStatKey CodeGen_TotalInstructions("CodeGen_TotalInstructions", true);
const static TStatKey CodeGen_MaxFunctionInstructions("CodeGen_MaxFunctionInstructions", false);
const static TStatKey CodeGen_FunctionPassTime("CodeGen_FunctionPassTime", true);
const static TStatKey CodeGen_ModulePassTime("CodeGen_ModulePassTime", true);
const static TStatKey CodeGen_FinalizeTime("CodeGen_FinalizeTime", true);

const static TStatKey Mkql_TotalNodes("Mkql_TotalNodes", true);
const static TStatKey Mkql_CodegenFunctions("Mkql_CodegenFunctions", true);

class TDependencyScanVisitor : public TEmptyNodeVisitor {
public:
    void Walk(TNode* root, const TTypeEnvironment& env) {
        Stack = &env.GetNodeStack();
        Stack->clear();
        Stack->push_back(root);
        while (!Stack->empty()) {
            auto top = Stack->back();
            Stack->pop_back();
            if (top->GetCookie() != IS_NODE_REACHABLE) {
                top->SetCookie(IS_NODE_REACHABLE);
                top->Accept(*this);
            }
        }

        Stack = nullptr;
    }

private:
    using TEmptyNodeVisitor::Visit;

    void Visit(TStructLiteral& node) override {
        for (ui32 i = 0; i < node.GetValuesCount(); ++i) {
            AddNode(node.GetValue(i).GetNode());
        }
    }

    void Visit(TListLiteral& node) override {
        for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
            AddNode(node.GetItems()[i].GetNode());
        }
    }

    void Visit(TOptionalLiteral& node) override {
        if (node.HasItem()) {
            AddNode(node.GetItem().GetNode());
        }
    }

    void Visit(TDictLiteral& node) override {
        for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
            AddNode(node.GetItem(i).first.GetNode());
            AddNode(node.GetItem(i).second.GetNode());
        }
    }

    void Visit(TCallable& node) override {
        if (node.HasResult()) {
            AddNode(node.GetResult().GetNode());
        } else {
            for (ui32 i = 0; i < node.GetInputsCount(); ++i) {
                AddNode(node.GetInput(i).GetNode());
            }
        }
    }

    void Visit(TAny& node) override {
        if (node.HasItem()) {
            AddNode(node.GetItem().GetNode());
        }
    }

    void Visit(TTupleLiteral& node) override {
        for (ui32 i = 0; i < node.GetValuesCount(); ++i) {
            AddNode(node.GetValue(i).GetNode());
        }
    }

    void Visit(TVariantLiteral& node) override {
        AddNode(node.GetItem().GetNode());
    }


    void AddNode(TNode* node) {
        if (node->GetCookie() != IS_NODE_REACHABLE) {
            Stack->push_back(node);
        }
    }

    std::vector<TNode*>* Stack = nullptr;
};

class TPatternNodes: public TAtomicRefCount<TPatternNodes> {
public:
    typedef TIntrusivePtr<TPatternNodes> TPtr;

    TPatternNodes(TAllocState& allocState)
        : AllocState(allocState)
        , MemInfo(MakeIntrusive<TMemoryUsageInfo>("ComputationPatternNodes"))
    {
#ifndef NDEBUG
        AllocState.ActiveMemInfo.emplace(MemInfo.Get(), MemInfo);
#else
        Y_UNUSED(AllocState);
#endif
    }

    ~TPatternNodes()
    {
        for (auto it = ComputationNodesList.rbegin(); it != ComputationNodesList.rend(); ++it) {
            *it = nullptr;
        }

        ComputationNodesList.clear();
        if (!UncaughtException()) {
#ifndef NDEBUG
            AllocState.ActiveMemInfo.erase(MemInfo.Get());
#endif
        }
    }

    const TComputationMutables& GetMutables() const {
        return Mutables;
    }

    const TComputationNodePtrDeque& GetNodes() const {
        return ComputationNodesList;
    }

    IComputationNode* GetComputationNode(TNode* node, bool pop = false, bool require = true) {
        const auto cookie = node->GetCookie();
        const auto result = reinterpret_cast<IComputationNode*>(cookie);

        if (cookie <= IS_NODE_REACHABLE) {
            MKQL_ENSURE(!require, "Computation graph builder, node not found, type:"
                << node->GetType()->GetKindAsStr());
            return result;
        }

        if (pop) {
            node->SetCookie(0);
        }

        return result;
    }

    IComputationExternalNode* GetEntryPoint(size_t index, bool require) {
        MKQL_ENSURE(index < Runtime2ComputationEntryPoints.size() && (!require || Runtime2ComputationEntryPoints[index]),
            "Pattern nodes can not get computation node by index: " << index << ", require: " << require
                << ", Runtime2ComputationEntryPoints size: " << Runtime2ComputationEntryPoints.size());
        return Runtime2ComputationEntryPoints[index];
    }

    IComputationNode* GetRoot() {
        return RootNode;
    }

    bool GetSuitableForCache() const {
        return SuitableForCache;
    }

    size_t GetEntryPointsCount() const {
        return Runtime2ComputationEntryPoints.size();
    }

private:
    friend class TComputationGraphBuildingVisitor;
    friend class TComputationGraph;

    TAllocState& AllocState;
    TIntrusivePtr<TMemoryUsageInfo> MemInfo;
    THolder<THolderFactory> HolderFactory;
    THolder<TDefaultValueBuilder> ValueBuilder;
    TComputationMutables Mutables;
    TComputationNodePtrDeque ComputationNodesList;
    IComputationNode* RootNode = nullptr;
    TComputationExternalNodePtrVector Runtime2ComputationEntryPoints;
    TComputationNodeOnNodeMap ElementsCache;
    bool SuitableForCache = true;
};

class TComputationGraphBuildingVisitor:
        public INodeVisitor,
        private TNonCopyable
{
public:
    TComputationGraphBuildingVisitor(const TComputationPatternOpts& opts)
        : Env(opts.Env)
        , TypeInfoHelper(new TTypeInfoHelper())
        , CountersProvider(opts.CountersProvider)
        , SecureParamsProvider(opts.SecureParamsProvider)
        , Factory(opts.Factory)
        , FunctionRegistry(*opts.FunctionRegistry)
        , ValidateMode(opts.ValidateMode)
        , ValidatePolicy(opts.ValidatePolicy)
        , GraphPerProcess(opts.GraphPerProcess)
        , PatternNodes(MakeIntrusive<TPatternNodes>(opts.AllocState))
        , ExternalAlloc(opts.PatternEnv)
    {
        PatternNodes->HolderFactory = MakeHolder<THolderFactory>(opts.AllocState, *PatternNodes->MemInfo, &FunctionRegistry);
        PatternNodes->ValueBuilder = MakeHolder<TDefaultValueBuilder>(*PatternNodes->HolderFactory, ValidatePolicy);
        PatternNodes->ValueBuilder->SetSecureParamsProvider(opts.SecureParamsProvider);
        NodeFactory = MakeHolder<TNodeFactory>(*PatternNodes->MemInfo, PatternNodes->Mutables);
    }

    ~TComputationGraphBuildingVisitor() {
        auto g = Env.BindAllocator();
        NodeFactory.Reset();
        PatternNodes.Reset();
    }

    const TTypeEnvironment& GetTypeEnvironment() const {
        return Env;
    }

    const IFunctionRegistry& GetFunctionRegistry() const {
        return FunctionRegistry;
    }

private:
    template <typename T>
    void VisitType(T& node) {
        AddNode(node, NodeFactory->CreateTypeNode(&node));
    }

    void Visit(TTypeType& node) override {
        VisitType<TTypeType>(node);
    }

    void Visit(TVoidType& node) override {
        VisitType<TVoidType>(node);
    }

    void Visit(TNullType& node) override {
        VisitType<TNullType>(node);
    }

    void Visit(TEmptyListType& node) override {
        VisitType<TEmptyListType>(node);
    }

    void Visit(TEmptyDictType& node) override {
        VisitType<TEmptyDictType>(node);
    }

    void Visit(TDataType& node) override {
        VisitType<TDataType>(node);
    }

    void Visit(TPgType& node) override {
        VisitType<TPgType>(node);
    }

    void Visit(TStructType& node) override {
        VisitType<TStructType>(node);
    }

    void Visit(TListType& node) override {
        VisitType<TListType>(node);
    }

    void Visit(TStreamType& node) override {
        VisitType<TStreamType>(node);
    }

    void Visit(TFlowType& node) override {
        VisitType<TFlowType>(node);
    }

    void Visit(TBlockType& node) override {
        VisitType<TBlockType>(node);
    }

    void Visit(TMultiType& node) override {
        VisitType<TMultiType>(node);
    }

    void Visit(TTaggedType& node) override {
        VisitType<TTaggedType>(node);
    }

    void Visit(TOptionalType& node) override {
        VisitType<TOptionalType>(node);
    }

    void Visit(TDictType& node) override {
        VisitType<TDictType>(node);
    }

    void Visit(TCallableType& node) override {
        VisitType<TCallableType>(node);
    }

    void Visit(TAnyType& node) override {
        VisitType<TAnyType>(node);
    }

    void Visit(TTupleType& node) override {
        VisitType<TTupleType>(node);
    }

    void Visit(TResourceType& node) override {
        VisitType<TResourceType>(node);
    }

    void Visit(TVariantType& node) override {
        VisitType<TVariantType>(node);
    }

    void Visit(TVoid& node) override {
        AddNode(node, NodeFactory->CreateImmutableNode(NUdf::TUnboxedValue::Void()));
    }

    void Visit(TNull& node) override {
        AddNode(node, NodeFactory->CreateImmutableNode(NUdf::TUnboxedValue()));
    }

    void Visit(TEmptyList& node) override {
        AddNode(node, NodeFactory->CreateImmutableNode(PatternNodes->HolderFactory->GetEmptyContainerLazy()));
    }

    void Visit(TEmptyDict& node) override {
        AddNode(node, NodeFactory->CreateImmutableNode(PatternNodes->HolderFactory->GetEmptyContainerLazy()));
    }

    void Visit(TDataLiteral& node) override {
        auto value = node.AsValue();
        NUdf::TDataTypeId typeId = node.GetType()->GetSchemeType();
        if (typeId != 0x101) { // TODO remove
            const auto slot = NUdf::GetDataSlot(typeId);
            MKQL_ENSURE(IsValidValue(slot, value),
                "Bad data literal for type: " << NUdf::GetDataTypeInfo(slot).Name << ", " << value);
        }

        NUdf::TUnboxedValue externalValue;
        if (ExternalAlloc) {
            if (value.IsString()) {
                externalValue = MakeString(value.AsStringRef());
            }
        }
        if (!externalValue) {
            externalValue = std::move(value);
        }

        AddNode(node, NodeFactory->CreateImmutableNode(std::move(externalValue)));
    }

    void Visit(TStructLiteral& node) override {
        TComputationNodePtrVector values;
        values.reserve(node.GetValuesCount());
        for (ui32 i = 0, e = node.GetValuesCount(); i < e; ++i) {
            values.push_back(GetComputationNode(node.GetValue(i).GetNode()));
        }

        AddNode(node, NodeFactory->CreateArrayNode(std::move(values)));
    }

    void Visit(TListLiteral& node) override {
        TComputationNodePtrVector items;
        items.reserve(node.GetItemsCount());
        for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
            items.push_back(GetComputationNode(node.GetItems()[i].GetNode()));
        }

        AddNode(node, NodeFactory->CreateArrayNode(std::move(items)));
    }

    void Visit(TOptionalLiteral& node) override {
        auto item = node.HasItem() ? GetComputationNode(node.GetItem().GetNode()) : nullptr;
        AddNode(node, NodeFactory->CreateOptionalNode(item));
    }

    void Visit(TDictLiteral& node) override {
        auto keyType = node.GetType()->GetKeyType();
        TKeyTypes types;
        bool isTuple;
        bool encoded;
        bool useIHash;
        GetDictionaryKeyTypes(keyType, types, isTuple, encoded, useIHash);

        std::vector<std::pair<IComputationNode*, IComputationNode*>> items;
        items.reserve(node.GetItemsCount());
        for (ui32 i = 0, e = node.GetItemsCount(); i < e; ++i) {
            auto item = node.GetItem(i);
            items.push_back(std::make_pair(GetComputationNode(item.first.GetNode()), GetComputationNode(item.second.GetNode())));
        }

        bool isSorted = !CanHash(keyType);
        AddNode(node, NodeFactory->CreateDictNode(std::move(items), types, isTuple, encoded ? keyType : nullptr,
            useIHash && !isSorted ? MakeHashImpl(keyType) : nullptr,
            useIHash ? MakeEquateImpl(keyType) : nullptr,
            useIHash && isSorted ? MakeCompareImpl(keyType) : nullptr, isSorted));
    }

    void Visit(TCallable& node) override {
        if (node.HasResult()) {
            node.GetResult().GetNode()->Accept(*this);
            auto computationNode = PatternNodes->ComputationNodesList.back().Get();
            node.SetCookie((ui64)computationNode);
            return;
        }

        if (node.GetType()->GetName() == "Steal") {
            return;
        }

        TNodeLocator nodeLocator = [this](TNode* dependentNode, bool pop) {
            return GetComputationNode(dependentNode, pop);
        };
        TComputationNodeFactoryContext ctx(
                nodeLocator,
                FunctionRegistry,
                Env,
                TypeInfoHelper,
                CountersProvider,
                SecureParamsProvider,
                *NodeFactory,
                *PatternNodes->HolderFactory,
                PatternNodes->ValueBuilder.Get(),
                ValidateMode,
                ValidatePolicy,
                GraphPerProcess,
                PatternNodes->Mutables,
                PatternNodes->ElementsCache,
                std::bind(&TComputationGraphBuildingVisitor::PushBackNode, this, std::placeholders::_1));
        const auto computationNode = Factory(node, ctx);
        const auto& name = node.GetType()->GetName();
        if (name == "KqpWideReadTable" ||
            name == "KqpWideReadTableRanges" ||
            name == "KqpBlockReadTableRanges" ||
            name == "KqpLookupTable" ||
            name == "KqpReadTable"
        ) {
            PatternNodes->SuitableForCache = false;
        }

        if (!computationNode) {
            THROW yexception()
                << "Computation graph builder, unsupported function: " << name << " type: " << Factory.target_type().name() ;
        }

        AddNode(node, computationNode);
    }

    void Visit(TAny& node) override {
        if (!node.HasItem()) {
            AddNode(node, NodeFactory->CreateImmutableNode(NUdf::TUnboxedValue::Void()));
        } else {
            AddNode(node, GetComputationNode(node.GetItem().GetNode()));
        }
    }

    void Visit(TTupleLiteral& node) override {
        TComputationNodePtrVector values;
        values.reserve(node.GetValuesCount());
        for (ui32 i = 0, e = node.GetValuesCount(); i < e; ++i) {
            values.push_back(GetComputationNode(node.GetValue(i).GetNode()));
        }

        AddNode(node, NodeFactory->CreateArrayNode(std::move(values)));
    }

    void Visit(TVariantLiteral& node) override {
        auto item = GetComputationNode(node.GetItem().GetNode());
        AddNode(node, NodeFactory->CreateVariantNode(item, node.GetIndex()));
    }

public:
    IComputationNode* GetComputationNode(TNode* node, bool pop = false, bool require = true) {
        return PatternNodes->GetComputationNode(node, pop, require);
    }

    TMemoryUsageInfo& GetMemInfo() {
        return *PatternNodes->MemInfo;
    }

    const THolderFactory& GetHolderFactory() const {
        return *PatternNodes->HolderFactory;
    }

    TPatternNodes::TPtr GetPatternNodes() {
        return PatternNodes;
    }

    const TComputationNodePtrDeque& GetNodes() const {
        return PatternNodes->GetNodes();
    }

    void PreserveRoot(IComputationNode* rootNode) {
        PatternNodes->RootNode = rootNode;
    }

    void PreserveEntryPoints(TComputationExternalNodePtrVector&& runtime2ComputationEntryPoints) {
        PatternNodes->Runtime2ComputationEntryPoints = std::move(runtime2ComputationEntryPoints);
    }

private:
    void PushBackNode(const IComputationNode::TPtr& computationNode) {
        computationNode->RegisterDependencies();
        PatternNodes->ComputationNodesList.push_back(computationNode);
    }

    void AddNode(TNode& node, const IComputationNode::TPtr& computationNode) {
        PushBackNode(computationNode);
        node.SetCookie((ui64)computationNode.Get());
    }

private:
    const TTypeEnvironment& Env;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    NUdf::ICountersProvider* CountersProvider;
    const NUdf::ISecureParamsProvider* SecureParamsProvider;
    const TComputationNodeFactory Factory;
    const IFunctionRegistry& FunctionRegistry;
    TIntrusivePtr<TMemoryUsageInfo> MemInfo;
    THolder<TNodeFactory> NodeFactory;
    NUdf::EValidateMode ValidateMode;
    NUdf::EValidatePolicy ValidatePolicy;
    EGraphPerProcess GraphPerProcess;
    TPatternNodes::TPtr PatternNodes;
    const bool ExternalAlloc; // obsolete, will be removed after YQL-13977
};

class TComputationGraph final : public IComputationGraph {
public:
    TComputationGraph(TPatternNodes::TPtr& patternNodes, const TComputationOptsFull& compOpts, NYql::NCodegen::ICodegen::TSharedPtr codegen)
        : PatternNodes(patternNodes)
        , MemInfo(MakeIntrusive<TMemoryUsageInfo>("ComputationGraph"))
        , CompOpts(compOpts)
        , Codegen(std::move(codegen))
    {
#ifndef NDEBUG
        CompOpts.AllocState.ActiveMemInfo.emplace(MemInfo.Get(), MemInfo);
#endif
        HolderFactory = MakeHolder<THolderFactory>(CompOpts.AllocState, *MemInfo, patternNodes->HolderFactory->GetFunctionRegistry());
        ValueBuilder = MakeHolder<TDefaultValueBuilder>(*HolderFactory.Get(), compOpts.ValidatePolicy);
        ValueBuilder->SetSecureParamsProvider(CompOpts.SecureParamsProvider);
        ArrowMemoryPool = MakeArrowMemoryPool(CompOpts.AllocState);
    }

    ~TComputationGraph() {
        auto stats = CompOpts.Stats;
        auto& pagePool = HolderFactory->GetPagePool();
        MKQL_SET_MAX_STAT(stats, PagePool_PeakAllocated, pagePool.GetPeakAllocated());
        MKQL_SET_MAX_STAT(stats, PagePool_PeakUsed, pagePool.GetPeakUsed());
        MKQL_ADD_STAT(stats, PagePool_AllocCount, pagePool.GetAllocCount());
        MKQL_ADD_STAT(stats, PagePool_PageAllocCount, pagePool.GetPageAllocCount());
        MKQL_ADD_STAT(stats, PagePool_PageHitCount, pagePool.GetPageHitCount());
        MKQL_ADD_STAT(stats, PagePool_PageMissCount, pagePool.GetPageMissCount());
        MKQL_ADD_STAT(stats, PagePool_OffloadedAllocCount, pagePool.GetOffloadedAllocCount());
        MKQL_ADD_STAT(stats, PagePool_OffloadedBytes, pagePool.GetOffloadedBytes());
    }

    void Prepare() override {
        if (!IsPrepared) {
            Ctx.Reset(new TComputationContext(*HolderFactory,
                ValueBuilder.Get(),
                CompOpts,
                PatternNodes->GetMutables(),
                //*ArrowMemoryPool
                *arrow::default_memory_pool()));
            Ctx->ExecuteLLVM = Codegen.get() != nullptr;
            ValueBuilder->SetCalleePositionHolder(Ctx->CalleePosition);
            for (auto& node : PatternNodes->GetNodes()) {
                node->InitNode(*Ctx);
            }
            IsPrepared = true;
        }
    }

    TComputationContext& GetContext() override {
        Prepare();
        return *Ctx;
    }

    NUdf::TUnboxedValue GetValue() override {
        Prepare();
        return PatternNodes->GetRoot()->GetValue(*Ctx);
    }

    IComputationExternalNode* GetEntryPoint(size_t index, bool require) override {
        Prepare();
        return PatternNodes->GetEntryPoint(index, require);
    }

    const TArrowKernelsTopology* GetKernelsTopology() override {
        Prepare();
        if (!KernelsTopology.has_value()) {
            CalculateKernelTopology(*Ctx);
        }

        return &KernelsTopology.value();
    }

    void CalculateKernelTopology(TComputationContext& ctx) {
        KernelsTopology.emplace();
        KernelsTopology->InputArgsCount = PatternNodes->GetEntryPointsCount();

        std::stack<const IComputationNode*> stack;
        struct TNodeState {
            bool Visited;
            ui32 Index;
        };

        std::unordered_map<const IComputationNode*, TNodeState> deps;
        for (ui32 i = 0; i < KernelsTopology->InputArgsCount; ++i) {
            auto entryPoint = PatternNodes->GetEntryPoint(i, false);
            if (!entryPoint) {
                continue;
            }

            deps.emplace(entryPoint, TNodeState{ true, i});
        }

        stack.push(PatternNodes->GetRoot());
        while (!stack.empty()) {
            auto node = stack.top();
            auto [iter, inserted]  = deps.emplace(node, TNodeState{ false, 0 });
            auto extNode = dynamic_cast<const IComputationExternalNode*>(node);
            if (extNode) {
                MKQL_ENSURE(!inserted, "Unexpected external node");
                stack.pop();
                continue;
            }

            auto kernelNode = node->PrepareArrowKernelComputationNode(ctx);
            MKQL_ENSURE(kernelNode, "No kernel for node: " << node->DebugString());
            auto argsCount = kernelNode->GetArgsDesc().size();

            if (!iter->second.Visited) {
                for (ui32 j = 0; j < argsCount; ++j) {
                    stack.push(kernelNode->GetArgument(j));
                }
                iter->second.Visited = true;
            } else {
                iter->second.Index = KernelsTopology->InputArgsCount + KernelsTopology->Items.size();
                KernelsTopology->Items.emplace_back();
                auto& i = KernelsTopology->Items.back();
                i.Inputs.reserve(argsCount);
                for (ui32 j = 0; j < argsCount; ++j) {
                    auto it = deps.find(kernelNode->GetArgument(j));
                    MKQL_ENSURE(it != deps.end(), "Missing argument");
                    i.Inputs.emplace_back(it->second.Index);
                }

                i.Node = std::move(kernelNode);
                stack.pop();
            }
        }
    }

    void Invalidate() override {
        std::fill_n(Ctx->MutableValues.get(), PatternNodes->GetMutables().CurValueIndex, NUdf::TUnboxedValue(NUdf::TUnboxedValuePod::Invalid()));
    }

    const TComputationNodePtrDeque& GetNodes() const override {
        return PatternNodes->GetNodes();
    }

    TMemoryUsageInfo& GetMemInfo() const override {
        return *MemInfo;
    }

    const THolderFactory& GetHolderFactory() const override {
        return *HolderFactory;
    }

    ITerminator* GetTerminator() const override {
        return ValueBuilder.Get();
    }

    bool SetExecuteLLVM(bool value) override {
        const bool old = Ctx->ExecuteLLVM;
        Ctx->ExecuteLLVM = value;
        return old;
    }

    TString SaveGraphState() override {
        Prepare();

        TString result;
        for (ui32 i : PatternNodes->GetMutables().SerializableValues) {
            const NUdf::TUnboxedValuePod& mutableValue = Ctx->MutableValues[i];
            if (mutableValue.IsInvalid()) {
                WriteUi64(result, std::numeric_limits<ui64>::max()); // -1.
            } else if (mutableValue.IsBoxed()) {
                TList<TString> taskState;
                size_t taskStateSize = 0;

                auto saveList = [&](auto& list) {
                    auto listIt = list.GetListIterator();
                    NUdf::TUnboxedValue str;
                    while (listIt.Next(str)) {
                        const TStringBuf strRef = str.AsStringRef();
                        taskStateSize += strRef.Size();
                        taskState.push_back({});
                        taskState.back().AppendNoAlias(strRef.Data(), strRef.Size());
                    }
                };
                bool isList = mutableValue.HasListItems();
                NUdf::TUnboxedValue list;
                if (isList) {   // No load was done during previous runs.
                    saveList(mutableValue);
                } else {
                    NUdf::TUnboxedValue saved = mutableValue.Save();
                    if (saved.IsString() || saved.IsEmbedded()) {  // Old version.
                        const TStringBuf savedBuf = saved.AsStringRef();
                        taskState.push_back({});
                        taskState.back().AppendNoAlias(savedBuf.Data(), savedBuf.Size());
                        taskStateSize = savedBuf.Size();
                    } else {
                        saveList(saved);
                    }
                }
                WriteUi64(result, taskStateSize);
                for (auto it = taskState.begin(); it != taskState.end();) {
                    result.AppendNoAlias(it->Data(), it->Size());
                    it = taskState.erase(it);
                }
            } else { // No load was done during previous runs (if any).
                MKQL_ENSURE(mutableValue.HasValue() && (mutableValue.IsString() || mutableValue.IsEmbedded()), "State is expected to have data or invalid value");
                const NUdf::TStringRef savedRef = mutableValue.AsStringRef();
                WriteUi64(result, savedRef.Size());
                result.AppendNoAlias(savedRef.Data(), savedRef.Size());
            }
        }
        return result;
    }

    void LoadGraphState(TStringBuf state) override {
        Prepare();

        for (ui32 i : PatternNodes->GetMutables().SerializableValues) {
            if (const ui64 size = ReadUi64(state); size != std::numeric_limits<ui64>::max()) {
                MKQL_ENSURE(state.Size() >= size, "Serialized state is corrupted - buffer is too short (" << state.Size() << ") for specified size: " << size);
                TStringBuf savedRef(state.Data(), size);
                Ctx->MutableValues[i] = NKikimr::NMiniKQL::TOutputSerializer::MakeArray(*Ctx, savedRef);
                state.Skip(size);
            } // else leave it Invalid()
        }

        MKQL_ENSURE(state.Empty(), "Serialized state is corrupted - extra bytes left: " << state.Size());
    }

private:
    const TPatternNodes::TPtr PatternNodes;
    const TIntrusivePtr<TMemoryUsageInfo> MemInfo;
    THolder<THolderFactory> HolderFactory;
    THolder<TDefaultValueBuilder> ValueBuilder;
    std::unique_ptr<arrow::MemoryPool> ArrowMemoryPool;
    THolder<TComputationContext> Ctx;
    TComputationOptsFull CompOpts;
    NYql::NCodegen::ICodegen::TSharedPtr Codegen;
    bool IsPrepared = false;
    std::optional<TArrowKernelsTopology> KernelsTopology;
};

class TComputationPatternImpl final : public IComputationPattern {
public:
    TComputationPatternImpl(THolder<TComputationGraphBuildingVisitor>&& builder, const TComputationPatternOpts& opts)
#if defined(MKQL_DISABLE_CODEGEN)
        : Codegen()
#elif defined(MKQL_FORCE_USE_CODEGEN)
        : Codegen(NYql::NCodegen::ICodegen::MakeShared(NYql::NCodegen::ETarget::Native))
#else
        : Codegen((NYql::NCodegen::ICodegen::IsCodegenAvailable() && opts.OptLLVM != "OFF") || GetEnv(TString("MKQL_FORCE_USE_LLVM")) ? NYql::NCodegen::ICodegen::MakeShared(NYql::NCodegen::ETarget::Native) : NYql::NCodegen::ICodegen::TPtr())
#endif
    {
    /// TODO: Enable JIT for AARCH64
#if defined(__aarch64__)
        Codegen = {};
#endif

        const auto& nodes = builder->GetNodes();
        for (const auto& node : nodes)
            node->PrepareStageOne();
        for (const auto& node : nodes)
            node->PrepareStageTwo();

        MKQL_ADD_STAT(opts.Stats, Mkql_TotalNodes, nodes.size());
        PatternNodes = builder->GetPatternNodes();

        if (Codegen) {
            Compile(opts.OptLLVM, opts.Stats);
        }
    }

    ~TComputationPatternImpl() {
        if (TypeEnv) {
            auto guard = TypeEnv->BindAllocator();
            PatternNodes.Reset();
        }
    }

    void Compile(TString optLLVM, IStatsRegistry* stats) {
        if (IsPatternCompiled.load())
            return;

#ifndef MKQL_DISABLE_CODEGEN
        if (!Codegen)
            Codegen = NYql::NCodegen::ICodegen::Make(NYql::NCodegen::ETarget::Native);

        const auto& nodes = PatternNodes->GetNodes();

        TStatTimer timerFull(CodeGen_FullTime);
        timerFull.Acquire();
        {
            TStatTimer timerGen(CodeGen_GenerateTime);
            timerGen.Acquire();
            for (auto it = nodes.crbegin(); nodes.crend() != it; ++it) {
                if (const auto codegen = dynamic_cast<ICodegeneratorRootNode*>(it->Get())) {
                    codegen->GenerateFunctions(*Codegen);
                }
            }
            timerGen.Release();
            timerGen.Report(stats);
        }

        if (optLLVM.Contains("--dump-generated")) {
            Cerr << "############### Begin generated module ###############" << Endl;
            Codegen->GetModule().print(llvm::errs(), nullptr);
            Cerr << "################ End generated module ################" << Endl;
        }

        TStatTimer timerComp(CodeGen_CompileTime);
        timerComp.Acquire();

        NYql::NCodegen::TCodegenStats codegenStats;
        Codegen->GetStats(codegenStats);
        MKQL_ADD_STAT(stats, CodeGen_TotalFunctions, codegenStats.TotalFunctions);
        MKQL_ADD_STAT(stats, CodeGen_TotalInstructions, codegenStats.TotalInstructions);
        MKQL_SET_MAX_STAT(stats, CodeGen_MaxFunctionInstructions, codegenStats.MaxFunctionInstructions);
        if (optLLVM.Contains("--dump-stats")) {
            Cerr << "TotalFunctions: " << codegenStats.TotalFunctions << Endl;
            Cerr << "TotalInstructions: " << codegenStats.TotalInstructions << Endl;
            Cerr << "MaxFunctionInstructions: " << codegenStats.MaxFunctionInstructions << Endl;
        }

        if (optLLVM.Contains("--dump-perf-map")) {
            Codegen->TogglePerfJITEventListener();
        }

        if (codegenStats.TotalFunctions >= TotalFunctionsLimit ||
            codegenStats.TotalInstructions >= TotalInstructionsLimit ||
            codegenStats.MaxFunctionInstructions >= MaxFunctionInstructionsLimit) {
            Codegen.reset();
        } else {
            Codegen->Verify();
            Codegen->Compile(GetCompileOptions(optLLVM), &CompileStats);

            MKQL_ADD_STAT(stats, CodeGen_FunctionPassTime, CompileStats.FunctionPassTime);
            MKQL_ADD_STAT(stats, CodeGen_ModulePassTime, CompileStats.ModulePassTime);
            MKQL_ADD_STAT(stats, CodeGen_FinalizeTime, CompileStats.FinalizeTime);
        }

        timerComp.Release();
        timerComp.Report(stats);

        if (Codegen) {
            if (optLLVM.Contains("--dump-compiled")) {
                Cerr << "############### Begin compiled module ###############" << Endl;
                Codegen->GetModule().print(llvm::errs(), nullptr);
                Cerr << "################ End compiled module ################" << Endl;
            }

            if (optLLVM.Contains("--asm-compiled")) {
                Cerr << "############### Begin compiled asm ###############" << Endl;
                Codegen->ShowGeneratedFunctions(&Cerr);
                Cerr << "################ End compiled asm ################" << Endl;
            }

            ui64 count = 0U;
            for (const auto& node : nodes) {
                if (const auto codegen = dynamic_cast<ICodegeneratorRootNode*>(node.Get())) {
                    codegen->FinalizeFunctions(*Codegen);
                    ++count;
                }
            }

            if (count) {
                MKQL_ADD_STAT(stats, Mkql_CodegenFunctions, count);
            }
        }

        timerFull.Release();
        timerFull.Report(stats);
#else
        Y_UNUSED(optLLVM);
        Y_UNUSED(stats);
#endif

        IsPatternCompiled.store(true);
    }

    bool IsCompiled() const {
        return IsPatternCompiled.load();
    }

    size_t CompiledCodeSize() const {
        return CompileStats.TotalObjectSize;
    }

    void RemoveCompiledCode() {
        IsPatternCompiled.store(false);
        CompileStats = {};
        Codegen.reset();
    }

    THolder<IComputationGraph> Clone(const TComputationOptsFull& compOpts) {
        if (IsPatternCompiled.load()) {
            return MakeHolder<TComputationGraph>(PatternNodes, compOpts, Codegen);
        }

        return MakeHolder<TComputationGraph>(PatternNodes, compOpts, nullptr);
    }

    bool GetSuitableForCache() const {
        return PatternNodes->GetSuitableForCache();
    }

private:
    TStringBuf GetCompileOptions(const TString& s) {
        const TString flag = "--compile-options";
        auto lpos = s.rfind(flag);
        if (lpos == TString::npos)
            return TStringBuf();
        lpos += flag.Size();
        auto rpos = s.find(" --", lpos);
        if (rpos == TString::npos)
            return TStringBuf(s, lpos);
        else
            return TStringBuf(s, lpos, rpos - lpos);
    };

    TTypeEnvironment* TypeEnv = nullptr;
    TPatternNodes::TPtr PatternNodes;
    NYql::NCodegen::ICodegen::TSharedPtr Codegen;
    std::atomic<bool> IsPatternCompiled = false;
    NYql::NCodegen::TCompileStats CompileStats;
};


TIntrusivePtr<TComputationPatternImpl> MakeComputationPatternImpl(TExploringNodeVisitor& explorer, const TRuntimeNode& root,
        const std::vector<TNode*>& entryPoints, const TComputationPatternOpts& opts) {
    TDependencyScanVisitor depScanner;
    depScanner.Walk(root.GetNode(), opts.Env);

    auto builder = MakeHolder<TComputationGraphBuildingVisitor>(opts);
    for (const auto& node : explorer.GetNodes()) {
        Y_ABORT_UNLESS(node->GetCookie() <= IS_NODE_REACHABLE, "TNode graph should not be reused");
        if (node->GetCookie() == IS_NODE_REACHABLE) {
            node->Accept(*builder);
        }
    }

    const auto rootNode = builder->GetComputationNode(root.GetNode());

    TComputationExternalNodePtrVector runtime2ComputationEntryPoints;
    runtime2ComputationEntryPoints.resize(entryPoints.size(), nullptr);
    std::unordered_map<TNode*, std::vector<ui32>> entryPointIndex;
    for (ui32 i = 0; i < entryPoints.size(); ++i) {
        entryPointIndex[entryPoints[i]].emplace_back(i);
    }

    for (const auto& node : explorer.GetNodes()) {
        auto it = entryPointIndex.find(node);
        if (it == entryPointIndex.cend()) {
            continue;
        }

        auto compNode = dynamic_cast<IComputationExternalNode*>(builder->GetComputationNode(node));
        for (auto index : it->second) {
            runtime2ComputationEntryPoints[index] = compNode;
        }
    }

    for (const auto& node : explorer.GetNodes()) {
        node->SetCookie(0);
    }

    builder->PreserveRoot(rootNode);
    builder->PreserveEntryPoints(std::move(runtime2ComputationEntryPoints));

    return MakeIntrusive<TComputationPatternImpl>(std::move(builder), opts);
}

} // namespace

IComputationPattern::TPtr MakeComputationPattern(TExploringNodeVisitor& explorer, const TRuntimeNode& root,
        const std::vector<TNode*>& entryPoints, const TComputationPatternOpts& opts) {
    return MakeComputationPatternImpl(explorer, root, entryPoints, opts);
}

} // namespace NMiniKQL
} // namespace NKikimr
