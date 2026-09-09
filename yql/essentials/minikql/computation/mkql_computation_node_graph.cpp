#include "mkql_computation_node_holders.h"
#include "mkql_computation_node_holders_codegen.h"
#include "mkql_value_builder.h"
#include "mkql_computation_node_codegen.h" // Y_IGNORE
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/minikql/computation/mkql_computation_pattern_cache.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <util/system/env.h>
#include <util/system/mutex.h>
#include <util/system/type_name.h>
#include <util/digest/city.h>
#include <util/generic/adaptor.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#ifndef MKQL_DISABLE_CODEGEN
    #include <llvm/Support/raw_ostream.h> // Y_IGNORE
#endif

namespace NKikimr::NMiniKQL {

using namespace NDetail;

namespace {

#ifndef MKQL_DISABLE_CODEGEN
constexpr ui64 TotalFunctionsLimit = 1000;
constexpr ui64 TotalInstructionsLimit = 100000;
constexpr ui64 MaxFunctionInstructionsLimit = 50000;
#endif

const ui64 IS_NODE_REACHABLE = 1;

const TStatKey PagePool_PeakAllocated("PagePool_PeakAllocated", /*deriv=*/false);
const TStatKey PagePool_PeakUsed("PagePool_PeakUsed", /*deriv=*/false);
const TStatKey PagePool_AllocCount("PagePool_AllocCount", /*deriv=*/true);
const TStatKey PagePool_PageAllocCount("PagePool_PageAllocCount", /*deriv=*/true);
const TStatKey PagePool_PageHitCount("PagePool_PageHitCount", /*deriv=*/true);
const TStatKey PagePool_PageMissCount("PagePool_PageMissCount", /*deriv=*/true);
const TStatKey PagePool_OffloadedAllocCount("PagePool_OffloadedAllocCount", /*deriv=*/true);
const TStatKey PagePool_OffloadedBytes("PagePool_OffloadedBytes", /*deriv=*/true);

const TStatKey CodeGen_FullTime("CodeGen_FullTime", /*deriv=*/true);
const TStatKey CodeGen_GenerateTime("CodeGen_GenerateTime", /*deriv=*/true);
const TStatKey CodeGen_CompileTime("CodeGen_CompileTime", /*deriv=*/true);
const TStatKey CodeGen_TotalFunctions("CodeGen_TotalFunctions", /*deriv=*/true);
const TStatKey CodeGen_TotalInstructions("CodeGen_TotalInstructions", /*deriv=*/true);
const TStatKey CodeGen_MaxFunctionInstructions("CodeGen_MaxFunctionInstructions", /*deriv=*/false);
const TStatKey CodeGen_FunctionPassTime("CodeGen_FunctionPassTime", /*deriv=*/true);
const TStatKey CodeGen_ModulePassTime("CodeGen_ModulePassTime", /*deriv=*/true);
const TStatKey CodeGen_FinalizeTime("CodeGen_FinalizeTime", /*deriv=*/true);

const TStatKey Mkql_TotalNodes("Mkql_TotalNodes", /*deriv=*/true);
const TStatKey Mkql_CodegenFunctions("Mkql_CodegenFunctions", /*deriv=*/true);

class TDependencyScanVisitor: public TEmptyNodeVisitor {
public:
    void Walk(TNode* root, std::vector<TNode*>& nodeStack) {
        Stack_ = &nodeStack;
        Stack_->clear();
        Stack_->push_back(root);
        while (!Stack_->empty()) {
            auto top = Stack_->back();
            Stack_->pop_back();
            if (top->GetCookie() != IS_NODE_REACHABLE) {
                top->SetCookie(IS_NODE_REACHABLE);
                top->Accept(*this);
            }
        }

        Stack_ = nullptr;
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
            Stack_->push_back(node);
        }
    }

    std::vector<TNode*>* Stack_ = nullptr;
};

class TPatternNodes: public TAtomicRefCount<TPatternNodes> {
public:
    using TPtr = TIntrusivePtr<TPatternNodes>;

    explicit TPatternNodes(TAllocState& allocState)
        : AllocState_(allocState)
        , MemInfo_(MakeIntrusive<TMemoryUsageInfo>("ComputationPatternNodes"))
    {
#ifndef NDEBUG
        AllocState_.ActiveMemInfo.emplace(MemInfo_.Get(), MemInfo_);
#else
        Y_UNUSED(AllocState_);
#endif
    }

    ~TPatternNodes()
    {
        for (auto& computationNode : Reversed(ComputationNodesList_)) {
            computationNode = nullptr;
        }

        ComputationNodesList_.clear();
        if (!UncaughtException()) {
#ifndef NDEBUG
            AllocState_.ActiveMemInfo.erase(MemInfo_.Get());
#endif
        }
    }

    ITerminator& GetTerminator() {
        return *ValueBuilder_;
    }

    const TComputationMutables& GetMutables() const {
        return Mutables_;
    }

    const TComputationNodePtrDeque& GetNodes() const {
        return ComputationNodesList_;
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
        MKQL_ENSURE(index < Runtime2ComputationEntryPoints_.size() && (!require || Runtime2ComputationEntryPoints_[index]),
                    "Pattern nodes can not get computation node by index: "
                        << index << ", require: " << require
                        << ", Runtime2ComputationEntryPoints size: " << Runtime2ComputationEntryPoints_.size());
        return Runtime2ComputationEntryPoints_[index];
    }

    IComputationNode* GetRoot() {
        return RootNode_;
    }

    bool GetSuitableForCache() const {
        return SuitableForCache_;
    }

    size_t GetEntryPointsCount() const {
        return Runtime2ComputationEntryPoints_.size();
    }

private:
    friend class TComputationGraphBuildingVisitor;
    friend class TComputationGraph;

    TAllocState& AllocState_;
    TIntrusivePtr<TMemoryUsageInfo> MemInfo_;
    THolder<THolderFactory> HolderFactory_;
    THolder<TDefaultValueBuilder> ValueBuilder_;
    TComputationMutables Mutables_;
    TComputationNodePtrDeque ComputationNodesList_;
    IComputationNode* RootNode_ = nullptr;
    TComputationExternalNodePtrVector Runtime2ComputationEntryPoints_;
    TComputationNodeOnNodeMap ElementsCache_;
    bool SuitableForCache_ = true;
};

class TComputationGraphBuildingVisitor: public INodeVisitor,
                                        private TNonCopyable {
public:
    explicit TComputationGraphBuildingVisitor(const TComputationPatternOpts& opts)
        : Env_(opts.Env)
        , TypeInfoHelper_(new TTypeInfoHelper())
        , CountersProvider_(opts.CountersProvider)
        , SecureParamsProvider_(opts.SecureParamsProvider)
        , LogProvider_(opts.LogProvider)
        , LangVer_(opts.LangVer)
        , Factory_(opts.Factory)
        , FunctionRegistry_(*opts.FunctionRegistry)
        , ValidateMode_(opts.ValidateMode)
        , ValidatePolicy_(opts.ValidatePolicy)
        , BridgeMode_(opts.BridgeMode)
        , BridgeBinaryPath_(opts.BridgeBinaryPath)
        , GraphPerProcess_(opts.GraphPerProcess)
        , PatternNodes_(MakeIntrusive<TPatternNodes>(opts.AllocState))
        , ExternalAlloc_(opts.PatternEnv)
        , RuntimeSettings_(opts.RuntimeSettings)
    {
        PatternNodes_->HolderFactory_ = MakeHolder<THolderFactory>(opts.AllocState, *PatternNodes_->MemInfo_, &FunctionRegistry_);
        PatternNodes_->ValueBuilder_ = MakeHolder<TDefaultValueBuilder>(*PatternNodes_->HolderFactory_, ValidatePolicy_);
        PatternNodes_->ValueBuilder_->SetSecureParamsProvider(opts.SecureParamsProvider);
        NodeFactory_ = MakeHolder<TNodeFactory>(*PatternNodes_->MemInfo_, PatternNodes_->Mutables_);
    }

    ~TComputationGraphBuildingVisitor() override {
        auto g = Env_.BindAllocator();
        NodeFactory_.Reset();
        PatternNodes_.Reset();
    }

    const TTypeEnvironment& GetTypeEnvironment() const {
        return Env_;
    }

    const IFunctionRegistry& GetFunctionRegistry() const {
        return FunctionRegistry_;
    }

private:
    template <typename T>
    void VisitType(T& node) {
        AddNode(node, NodeFactory_->CreateTypeNode(&node));
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

    void Visit(TLinearType& node) override {
        VisitType<TLinearType>(node);
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
        AddNode(node, NodeFactory_->CreateImmutableNode(NUdf::TUnboxedValue::Void()));
    }

    void Visit(TNull& node) override {
        AddNode(node, NodeFactory_->CreateImmutableNode(NUdf::TUnboxedValue()));
    }

    void Visit(TEmptyList& node) override {
        AddNode(node, NodeFactory_->CreateImmutableNode(PatternNodes_->HolderFactory_->GetEmptyContainerLazy()));
    }

    void Visit(TEmptyDict& node) override {
        AddNode(node, NodeFactory_->CreateImmutableNode(PatternNodes_->HolderFactory_->GetEmptyContainerLazy()));
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
        if (ExternalAlloc_) {
            if (value.IsString()) {
                externalValue = MakeString(value.AsStringRef());
            }
        }
        if (!externalValue) {
            externalValue = std::move(value);
        }

        AddNode(node, NodeFactory_->CreateImmutableNode(std::move(externalValue)));
    }

    void Visit(TStructLiteral& node) override {
        TComputationNodePtrVector values;
        values.reserve(node.GetValuesCount());
        for (ui32 i = 0, e = node.GetValuesCount(); i < e; ++i) {
            values.push_back(GetComputationNode(node.GetValue(i).GetNode()));
        }

        AddNode(node, NodeFactory_->CreateArrayNode(std::move(values)));
    }

    void Visit(TListLiteral& node) override {
        TComputationNodePtrVector items;
        items.reserve(node.GetItemsCount());
        for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
            items.push_back(GetComputationNode(node.GetItems()[i].GetNode()));
        }

        AddNode(node, NodeFactory_->CreateArrayNode(std::move(items)));
    }

    void Visit(TOptionalLiteral& node) override {
        auto item = node.HasItem() ? GetComputationNode(node.GetItem().GetNode()) : nullptr;
        AddNode(node, NodeFactory_->CreateOptionalNode(item));
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
            items.emplace_back(
                GetComputationNode(item.first.GetNode()),
                GetComputationNode(item.second.GetNode()));
        }

        bool isSorted = !CanHash(keyType);
        AddNode(node, NodeFactory_->CreateDictNode(std::move(items), types, isTuple, encoded ? keyType : nullptr,
                                                   useIHash && !isSorted ? MakeHashImpl(keyType) : nullptr,
                                                   useIHash ? MakeEquateImpl(keyType) : nullptr,
                                                   useIHash && isSorted ? MakeCompareImpl(keyType) : nullptr, isSorted));
    }

    void Visit(TCallable& node) override {
        if (node.HasResult()) {
            node.GetResult().GetNode()->Accept(*this);
            auto computationNode = PatternNodes_->ComputationNodesList_.back().Get();
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
            FunctionRegistry_,
            Env_,
            TypeInfoHelper_,
            CountersProvider_,
            SecureParamsProvider_,
            LogProvider_,
            LangVer_,
            *NodeFactory_,
            *PatternNodes_->HolderFactory_,
            PatternNodes_->ValueBuilder_.Get(),
            ValidateMode_,
            ValidatePolicy_,
            BridgeMode_,
            BridgeBinaryPath_,
            GraphPerProcess_,
            PatternNodes_->Mutables_,
            PatternNodes_->ElementsCache_,
            std::bind(&TComputationGraphBuildingVisitor::PushBackNode, this, std::placeholders::_1),
            RuntimeSettings_);
        const auto computationNode = Factory_(node, ctx);
        const auto& name = node.GetType()->GetName();
        if (name == "KqpWideReadTable" ||
            name == "KqpWideReadTableRanges" ||
            name == "KqpBlockReadTableRanges" ||
            name == "KqpLookupTable" ||
            name == "KqpReadTable" ||
            name == "MultiHoppingCore" ||
            name == "DqWatermarkGenerator") {
            PatternNodes_->SuitableForCache_ = false;
        }

        if (!computationNode) {
            THROW yexception()
                << "Computation graph builder, unsupported function: " << name << " type: " << TypeName(Factory_.target_type());
        }

        AddNode(node, computationNode);
    }

    void Visit(TAny& node) override {
        if (!node.HasItem()) {
            AddNode(node, NodeFactory_->CreateImmutableNode(NUdf::TUnboxedValue::Void()));
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

        AddNode(node, NodeFactory_->CreateArrayNode(std::move(values)));
    }

    void Visit(TVariantLiteral& node) override {
        auto item = GetComputationNode(node.GetItem().GetNode());
        AddNode(node, NodeFactory_->CreateVariantNode(item, node.GetIndex()));
    }

public:
    IComputationNode* GetComputationNode(TNode* node, bool pop = false, bool require = true) {
        return PatternNodes_->GetComputationNode(node, pop, require);
    }

    TMemoryUsageInfo& GetMemInfo() {
        return *PatternNodes_->MemInfo_;
    }

    const THolderFactory& GetHolderFactory() const {
        return *PatternNodes_->HolderFactory_;
    }

    TPatternNodes::TPtr GetPatternNodes() {
        return PatternNodes_;
    }

    const TComputationNodePtrDeque& GetNodes() const {
        return PatternNodes_->GetNodes();
    }

    void PreserveRoot(IComputationNode* rootNode) {
        PatternNodes_->RootNode_ = rootNode;
    }

    void PreserveEntryPoints(TComputationExternalNodePtrVector&& runtime2ComputationEntryPoints) {
        PatternNodes_->Runtime2ComputationEntryPoints_ = std::move(runtime2ComputationEntryPoints);
    }

private:
    void PushBackNode(const IComputationNode::TPtr& computationNode) {
        computationNode->RegisterDependencies();
        PatternNodes_->ComputationNodesList_.push_back(computationNode);
    }

    void AddNode(TNode& node, const IComputationNode::TPtr& computationNode) {
        PushBackNode(computationNode);
        node.SetCookie((ui64)computationNode.Get());
    }

    const TTypeEnvironment& Env_;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
    NUdf::ICountersProvider* CountersProvider_;
    const NUdf::ISecureParamsProvider* SecureParamsProvider_;
    const NUdf::ILogProvider* LogProvider_;
    const NYql::TLangVersion LangVer_;
    const TComputationNodeFactory Factory_;
    const IFunctionRegistry& FunctionRegistry_;
    TIntrusivePtr<TMemoryUsageInfo> MemInfo_;
    THolder<TNodeFactory> NodeFactory_;
    NUdf::EValidateMode ValidateMode_;
    NUdf::EValidatePolicy ValidatePolicy_;
    NUdf::EBridgeMode BridgeMode_;
    const TString BridgeBinaryPath_;
    EGraphPerProcess GraphPerProcess_;
    TPatternNodes::TPtr PatternNodes_;
    const bool ExternalAlloc_; // obsolete, will be removed after YQL-13977
    NYql::TRuntimeSettings::TConstPtr RuntimeSettings_;
};

class TComputationGraph final: public IComputationGraph {
public:
    TComputationGraph(TPatternNodes::TPtr& patternNodes, const TComputationOptsFull& compOpts,
                      NYql::NCodegen::ICodegen::TSharedPtr codegen)
        : PatternNodes_(patternNodes)
        , MemInfo_(MakeIntrusive<TMemoryUsageInfo>("ComputationGraph"))
        , CompOpts_(compOpts)
        , Codegen_(std::move(codegen))
    {
#ifndef NDEBUG
        CompOpts_.AllocState.ActiveMemInfo.emplace(MemInfo_.Get(), MemInfo_);
#endif
        HolderFactory_ = MakeHolder<THolderFactory>(CompOpts_.AllocState, *MemInfo_, patternNodes->HolderFactory_->GetFunctionRegistry());
        ValueBuilder_ = MakeHolder<TDefaultValueBuilder>(*HolderFactory_.Get(), compOpts.ValidatePolicy);
        ValueBuilder_->SetSecureParamsProvider(CompOpts_.SecureParamsProvider);
    }

    ~TComputationGraph() override {
        auto stats = CompOpts_.Stats;
        auto& pagePool = HolderFactory_->GetPagePool();
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
        if (!IsPrepared_) {
            Ctx_.Reset(new TComputationContext(*HolderFactory_,
                                               ValueBuilder_.Get(),
                                               CompOpts_,
                                               PatternNodes_->GetMutables(),
                                               *NYql::NUdf::GetYqlMemoryPool(),
                                               NotConsumedLinear_,
                                               CompOpts_.RuntimeSettings));
            Ctx_->ExecuteLLVM = Codegen_.get() != nullptr;
            ValueBuilder_->SetCalleePositionHolder(Ctx_->CalleePosition);
            for (auto& node : PatternNodes_->GetNodes()) {
                node->InitNode(*Ctx_);
            }
            IsPrepared_ = true;
        }
    }

    TComputationContext& GetContext() override {
        Prepare();
        return *Ctx_;
    }

    NUdf::TUnboxedValue GetValue() override {
        Prepare();
        return PatternNodes_->GetRoot()->GetValue(*Ctx_);
    }

    IComputationExternalNode* GetEntryPoint(size_t index, bool require) override {
        Prepare();
        return PatternNodes_->GetEntryPoint(index, require);
    }

    const TArrowKernelsTopology* GetKernelsTopology() override {
        Prepare();
        if (!KernelsTopology_.has_value()) {
            CalculateKernelTopology(*Ctx_);
        }

        return &KernelsTopology_.value();
    }

    void CalculateKernelTopology(TComputationContext& ctx) {
        KernelsTopology_.emplace();
        KernelsTopology_->InputArgsCount = PatternNodes_->GetEntryPointsCount();

        std::stack<const IComputationNode*> stack;
        struct TNodeState {
            bool Visited;
            ui32 Index;
        };

        std::unordered_map<const IComputationNode*, TNodeState> deps;
        for (ui32 i = 0; i < KernelsTopology_->InputArgsCount; ++i) {
            auto entryPoint = PatternNodes_->GetEntryPoint(i, /*require=*/false);
            if (!entryPoint) {
                continue;
            }

            deps.emplace(entryPoint, TNodeState{.Visited = true, .Index = i});
        }

        stack.push(PatternNodes_->GetRoot());
        while (!stack.empty()) {
            auto node = stack.top();
            auto [iter, inserted] = deps.emplace(node, TNodeState{.Visited = false, .Index = 0});
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
                iter->second.Index = KernelsTopology_->InputArgsCount + KernelsTopology_->Items.size();
                KernelsTopology_->Items.emplace_back();
                auto& i = KernelsTopology_->Items.back();
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
        std::fill_n(
            Ctx_->MutableValues.get(),
            PatternNodes_->GetMutables().CurValueIndex,
            NUdf::TUnboxedValue(NUdf::TUnboxedValuePod::Invalid()));
    }

    void InvalidateCaches() override {
        for (const auto cachedIndex : Ctx_->Mutables.CachedValues) {
            Ctx_->MutableValues[cachedIndex] = NUdf::TUnboxedValuePod::Invalid();
        }
    }

    const TComputationNodePtrDeque& GetNodes() const override {
        return PatternNodes_->GetNodes();
    }

    TMemoryUsageInfo& GetMemInfo() const override {
        return *MemInfo_;
    }

    const THolderFactory& GetHolderFactory() const override {
        return *HolderFactory_;
    }

    ITerminator* GetTerminator() const override {
        return ValueBuilder_.Get();
    }

    bool SetExecuteLLVM(bool value) override {
        const bool old = Ctx_->ExecuteLLVM;
        Ctx_->ExecuteLLVM = value;
        return old;
    }

    TString SaveGraphState() override {
        Prepare();

        TString result;
        for (ui32 i : PatternNodes_->GetMutables().SerializableValues) {
            const NUdf::TUnboxedValuePod& mutableValue = Ctx_->MutableValues[i];
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
                if (isList) { // No load was done during previous runs.
                    saveList(mutableValue);
                } else {
                    NUdf::TUnboxedValue saved = mutableValue.Save();
                    if (saved.IsString() || saved.IsEmbedded()) { // Old version.
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
                    result.AppendNoAlias(it->data(), it->size());
                    it = taskState.erase(it);
                }
            } else { // No load was done during previous runs (if any).
                MKQL_ENSURE(mutableValue.HasValue() && (mutableValue.IsString() || mutableValue.IsEmbedded()),
                            "State is expected to have data or invalid value");
                const NUdf::TStringRef savedRef = mutableValue.AsStringRef();
                WriteUi64(result, savedRef.Size());
                result.AppendNoAlias(savedRef.Data(), savedRef.Size());
            }
        }
        return result;
    }

    void LoadGraphState(TStringBuf state) override {
        Prepare();

        for (ui32 i : PatternNodes_->GetMutables().SerializableValues) {
            if (const ui64 size = ReadUi64(state); size != std::numeric_limits<ui64>::max()) {
                MKQL_ENSURE(state.Size() >= size,
                            "Serialized state is corrupted - buffer is too short ("
                                << state.Size() << ") for specified size: " << size);
                TStringBuf savedRef(state.Data(), size);
                Ctx_->MutableValues[i] = NKikimr::NMiniKQL::TOutputSerializer::MakeArray(*Ctx_, savedRef);
                state.Skip(size);
            } // else leave it Invalid()
        }

        MKQL_ENSURE(state.Empty(), "Serialized state is corrupted - extra bytes left: " << state.Size());
    }

    TMaybe<NUdf::TSourcePosition> GetNotConsumedLinear() override {
        return NotConsumedLinear_;
    }

    bool GetFlushingMode() const override {
        return Ctx_->FlushingMode;
    }

    void SetFlushingMode(bool value) override {
        Ctx_->FlushingMode = value;
    }

private:
    const TPatternNodes::TPtr PatternNodes_;
    const TIntrusivePtr<TMemoryUsageInfo> MemInfo_;
    THolder<THolderFactory> HolderFactory_;
    THolder<TDefaultValueBuilder> ValueBuilder_;
    THolder<TComputationContext> Ctx_;
    TComputationOptsFull CompOpts_;
    NYql::NCodegen::ICodegen::TSharedPtr Codegen_;
    bool IsPrepared_ = false;
    std::optional<TArrowKernelsTopology> KernelsTopology_;
    TMaybe<NUdf::TSourcePosition> NotConsumedLinear_;
};

class TComputationPatternImpl final: public IComputationPattern {
public:
    TComputationPatternImpl(THolder<TComputationGraphBuildingVisitor>&& builder, const TComputationPatternOpts& opts)
#if defined(MKQL_DISABLE_CODEGEN)
        : Codegen_()
#elif defined(MKQL_FORCE_USE_CODEGEN)
        : Codegen_(NYql::NCodegen::ICodegen::MakeShared(NYql::NCodegen::ETarget::Native))
#else
        : Codegen_((NYql::NCodegen::ICodegen::IsCodegenAvailable() && opts.OptLLVM != "OFF") ||
                           GetEnv(TString("MKQL_FORCE_USE_LLVM"))
                       ? NYql::NCodegen::ICodegen::MakeShared(NYql::NCodegen::ETarget::Native)
                       : NYql::NCodegen::ICodegen::TPtr())
#endif
    {
        /// TODO: Enable JIT for AARCH64/Win/Darwin (YDBREQUESTS-7823)
#if defined(__aarch64__) || defined(_win_) || defined(_darwin_)
        Codegen_ = {};
#endif

        const auto& nodes = builder->GetNodes();
        for (const auto& node : nodes) {
            node->PrepareStageOne();
        }
        for (const auto& node : nodes) {
            node->PrepareStageTwo();
        }

        MKQL_ADD_STAT(opts.Stats, Mkql_TotalNodes, nodes.size());
        PatternNodes_ = builder->GetPatternNodes();

        if (Codegen_) {
            Compile(opts.OptLLVM, opts.Stats);
        }
    }

    ~TComputationPatternImpl() override {
        if (TypeEnv_) {
            auto guard = TypeEnv_->BindAllocator();
            PatternNodes_.Reset();
        }
    }

    void Compile(TString optLLVM, IStatsRegistry* stats) override {
        TGuard<TMutex> lock(CompileMutex_);

        if (IsPatternCompiled_) {
            return;
        }

#ifndef MKQL_DISABLE_CODEGEN
        if (!Codegen_) {
            Codegen_ = NYql::NCodegen::ICodegen::Make(NYql::NCodegen::ETarget::Native);
        }

        const auto& nodes = PatternNodes_->GetNodes();

        TStatTimer timerFull(CodeGen_FullTime);
        timerFull.Acquire();
        {
            TStatTimer timerGen(CodeGen_GenerateTime);
            timerGen.Acquire();
            for (const auto& node : Reversed(nodes)) {
                if (const auto codegen = dynamic_cast<ICodegeneratorRootNode*>(node.Get())) {
                    codegen->GenerateFunctions(*Codegen_);
                }
            }
            timerGen.Release();
            timerGen.Report(stats);
        }

        if (optLLVM.Contains("--dump-generated")) {
            Cerr << "############### Begin generated module ###############" << Endl;
            Codegen_->GetModule().print(llvm::errs(), /*AAW=*/nullptr);
            Cerr << "################ End generated module ################" << Endl;
        }

        TStatTimer timerComp(CodeGen_CompileTime);
        timerComp.Acquire();

        NYql::NCodegen::TCodegenStats codegenStats;
        Codegen_->GetStats(codegenStats);
        MKQL_ADD_STAT(stats, CodeGen_TotalFunctions, codegenStats.TotalFunctions);
        MKQL_ADD_STAT(stats, CodeGen_TotalInstructions, codegenStats.TotalInstructions);
        MKQL_SET_MAX_STAT(stats, CodeGen_MaxFunctionInstructions, codegenStats.MaxFunctionInstructions);
        if (optLLVM.Contains("--dump-stats")) {
            Cerr << "TotalFunctions: " << codegenStats.TotalFunctions << Endl;
            Cerr << "TotalInstructions: " << codegenStats.TotalInstructions << Endl;
            Cerr << "MaxFunctionInstructions: " << codegenStats.MaxFunctionInstructions << Endl;
        }

        if (optLLVM.Contains("--dump-perf-map")) {
            Codegen_->TogglePerfJITEventListener();
        }

        if (codegenStats.TotalFunctions >= TotalFunctionsLimit ||
            codegenStats.TotalInstructions >= TotalInstructionsLimit ||
            codegenStats.MaxFunctionInstructions >= MaxFunctionInstructionsLimit) {
            Codegen_.reset();
        } else {
            Codegen_->Verify();
            Codegen_->Compile(GetCompileOptions(optLLVM), &CompileStats_);

            MKQL_ADD_STAT(stats, CodeGen_FunctionPassTime, CompileStats_.FunctionPassTime);
            MKQL_ADD_STAT(stats, CodeGen_ModulePassTime, CompileStats_.ModulePassTime);
            MKQL_ADD_STAT(stats, CodeGen_FinalizeTime, CompileStats_.FinalizeTime);
        }

        timerComp.Release();
        timerComp.Report(stats);

        if (Codegen_) {
            if (optLLVM.Contains("--dump-compiled")) {
                Cerr << "############### Begin compiled module ###############" << Endl;
                Codegen_->GetModule().print(llvm::errs(), /*AAW=*/nullptr);
                Cerr << "################ End compiled module ################" << Endl;
            }

            if (optLLVM.Contains("--asm-compiled")) {
                Cerr << "############### Begin compiled asm ###############" << Endl;
                Codegen_->ShowGeneratedFunctions(&Cerr);
                Cerr << "################ End compiled asm ################" << Endl;
            }

            ui64 count = 0U;
            for (const auto& node : nodes) {
                if (const auto codegen = dynamic_cast<ICodegeneratorRootNode*>(node.Get())) {
                    codegen->FinalizeFunctions(*Codegen_);
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

        IsPatternCompiled_ = true;
    }

    bool IsCompiled() const override {
        TGuard<TMutex> lock(CompileMutex_);
        return IsPatternCompiled_;
    }

    size_t CompiledCodeSize() const override {
        TGuard<TMutex> lock(CompileMutex_);
        return CompileStats_.TotalObjectSize;
    }

    void RemoveCompiledCode() override {
        TGuard<TMutex> lock(CompileMutex_);

        IsPatternCompiled_ = false;
        CompileStats_ = {};
        Codegen_.reset();
    }

    THolder<IComputationGraph> Clone(const TComputationOptsFull& compOpts) override {
        TGuard<TMutex> lock(CompileMutex_);
        return MakeHolder<TComputationGraph>(PatternNodes_, compOpts, IsPatternCompiled_ ? Codegen_ : nullptr);
    }

    bool GetSuitableForCache() const override {
        return PatternNodes_->GetSuitableForCache();
    }

private:
    TStringBuf GetCompileOptions(const TString& s) {
        const TString flag = "--compile-options";
        auto lpos = s.rfind(flag);
        if (lpos == TString::npos) {
            return TStringBuf();
        }
        lpos += flag.size();
        auto rpos = s.find(" --", lpos);
        if (rpos == TString::npos) {
            return TStringBuf(s, lpos);
        } else {
            return TStringBuf(s, lpos, rpos - lpos);
        }
    };

    TTypeEnvironment* TypeEnv_ = nullptr;
    TPatternNodes::TPtr PatternNodes_;

    TMutex CompileMutex_;
    NYql::NCodegen::ICodegen::TSharedPtr Codegen_; // protected by CompileMutex_
    bool IsPatternCompiled_ = false;               // protected by CompileMutex_
    NYql::NCodegen::TCompileStats CompileStats_;   // protected by CompileMutex_
    NYql::TRuntimeSettings::TConstPtr RuntimeSettings_;
};

TIntrusivePtr<TComputationPatternImpl> MakeComputationPatternImpl(
    TExploringNodeVisitor& explorer,
    const TRuntimeNode& root,
    const std::vector<TNode*>& entryPoints,
    const TComputationPatternOpts& opts) {
    TDependencyScanVisitor depScanner;
    depScanner.Walk(root.GetNode(), opts.Env.GetNodeStack());

    auto builder = MakeHolder<TComputationGraphBuildingVisitor>(opts);
    const TBindTerminator bind(&builder->GetPatternNodes()->GetTerminator());
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

} // namespace NKikimr::NMiniKQL
