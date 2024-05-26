#pragma once

#include "mkql_computation_node.h"

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/system/type_name.h>

namespace NKikimr {
namespace NMiniKQL {

enum class EDictType {
    Sorted,
    Hashed
};

enum class EContainerOptMode {
    NoOpt,
    OptNoAdd,
    OptAdd
};

template <class IComputationNodeInterface>
class TRefCountedComputationNode : public IComputationNodeInterface {
private:
    void Ref() final;

    void UnRef() final;

    ui32 RefCount() const final;

private:
    ui32 Refs_ = 0;
};

class TUnboxedImmutableComputationNode: public TRefCountedComputationNode<IComputationNode>
{
public:
    TUnboxedImmutableComputationNode(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value);

    ~TUnboxedImmutableComputationNode();

private:
    void InitNode(TComputationContext&) const override {}

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final;

    const IComputationNode* GetSource() const final;

    IComputationNode* AddDependence(const IComputationNode*) final;

    void RegisterDependencies() const final;

    ui32 GetIndex() const final;

    void CollectDependentIndexes(const IComputationNode* owner, TIndexesMap&) const final;

    ui32 GetDependencyWeight() const final;

    ui32 GetDependencesCount() const final;

    bool IsTemporaryValue() const final;

    void PrepareStageOne() final;
    void PrepareStageTwo() final;

    TString DebugString() const final;

    EValueRepresentation GetRepresentation() const final;

    TMemoryUsageInfo *const MemInfo;
protected:
    const NUdf::TUnboxedValue UnboxedValue;
    const EValueRepresentation RepresentationKind;
};

template <class IComputationNodeInterface, bool SerializableState = false>
class TStatefulComputationNode: public TRefCountedComputationNode<IComputationNodeInterface>
{
protected:
    TStatefulComputationNode(TComputationMutables& mutables, EValueRepresentation kind);

protected:
    void InitNode(TComputationContext&) const override {}

    IComputationNode* AddDependence(const IComputationNode* node) final;

    EValueRepresentation GetRepresentation() const override;

    NUdf::TUnboxedValue& ValueRef(TComputationContext& compCtx) const {
        return compCtx.MutableValues[ValueIndex];
    }

    ui32 GetIndex() const final;

    TConstComputationNodePtrVector Dependencies;

    const ui32 ValueIndex;
    const EValueRepresentation RepresentationKind;
private:
    ui32 GetDependencesCount() const final;
};

class TExternalComputationNode: public TStatefulComputationNode<IComputationExternalNode>
{
public:
    TExternalComputationNode(TComputationMutables& mutables, EValueRepresentation kind = EValueRepresentation::Any);

protected:

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override;

    NUdf::TUnboxedValue& RefValue(TComputationContext& compCtx) const override;
    void SetValue(TComputationContext& compCtx, NUdf::TUnboxedValue&& value) const override;

    TString DebugString() const final;
private:
    ui32 GetDependencyWeight() const final;

    void RegisterDependencies() const final;

    void SetOwner(const IComputationNode* owner) final;

    void PrepareStageOne() final;
    void PrepareStageTwo() final;

    void CollectDependentIndexes(const IComputationNode* owner, TIndexesMap& dependencies) const final;

    bool IsTemporaryValue() const final;

    const IComputationNode* Owner = nullptr;

    void SetGetter(TGetter&& getter) final;

    void InvalidateValue(TComputationContext& compCtx) const final;

    const IComputationNode* GetSource() const final;
protected:
    std::vector<std::pair<ui32, EValueRepresentation>> InvalidationSet;
    TGetter Getter;
};

template <typename TDerived, bool SerializableState = false>
class TStatefulSourceComputationNode: public TStatefulComputationNode<IComputationNode, SerializableState>
{
    using TStatefulComputationNode = TStatefulComputationNode<IComputationNode, SerializableState>;
private:
    bool IsTemporaryValue() const final {
        return *Stateless;
    }

    ui32 GetDependencyWeight() const final {
        return Sources.size();
    }

    void PrepareStageOne() final {
        if (!Stateless) {
            Stateless = std::accumulate(this->Dependencies.cbegin(), this->Dependencies.cend(), 0,
                std::bind(std::plus<i32>(), std::placeholders::_1, std::bind(&IComputationNode::GetDependencyWeight, std::placeholders::_2))) <= 1;
        }
    }

    void PrepareStageTwo() final {}

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        if (const auto ins = dependencies.emplace(this->ValueIndex, this->RepresentationKind); ins.second) {
            std::for_each(this->Dependencies.cbegin(), this->Dependencies.cend(), std::bind(&IComputationNode::CollectDependentIndexes, std::placeholders::_1, owner, std::ref(dependencies)));

            if (*Stateless) {
                dependencies.erase(ins.first);
            }
        }
    }

    const IComputationNode* GetSource() const final { return this; }
protected:
    TStatefulSourceComputationNode(TComputationMutables& mutables, EValueRepresentation kind = EValueRepresentation::Any)
        : TStatefulComputationNode(mutables, kind)
    {}

    void DependsOn(IComputationNode* node) const {
        if (node) {
            if (const auto source = node->AddDependence(this)) {
                Sources.emplace(source);
            }
        }
    }

    void Own(IComputationExternalNode* node) const {
        if (node) {
            node->SetOwner(this);
        }
    }

    TString DebugString() const override {
        return TypeName<TDerived>();
    }

    mutable std::unordered_set<const IComputationNode*> Sources; // TODO: remove const and mutable.
    std::optional<bool> Stateless;
};

template <typename TDerived>
class TMutableComputationNode: public TStatefulSourceComputationNode<TDerived> {
protected:
    using TStatefulSourceComputationNode<TDerived>::TStatefulSourceComputationNode;

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override {
        if (*this->Stateless)
            return static_cast<const TDerived*>(this)->DoCalculate(compCtx);
        NUdf::TUnboxedValue& valueRef = this->ValueRef(compCtx);
        if (valueRef.IsInvalid()) {
            valueRef = static_cast<const TDerived*>(this)->DoCalculate(compCtx);
        }

        return valueRef;
    }
};

template <typename TDerived, typename IFlowInterface>
class TFlowSourceBaseComputationNode: public TStatefulComputationNode<IFlowInterface>
{
    using TBase = TStatefulComputationNode<IFlowInterface>;
protected:
    TFlowSourceBaseComputationNode(TComputationMutables& mutables, EValueRepresentation stateKind)
        : TBase(mutables, stateKind)
    {}

    TString DebugString() const override {
        return TypeName<TDerived>();
    }

    void DependsOn(IComputationNode* node) const {
        if (node) {
            if (const auto source = node->AddDependence(this)) {
                Sources.emplace(source);
            }
        }
    }

    void Own(IComputationExternalNode* node) const {
        if (node) {
            node->SetOwner(this);
        }
    }
private:
    bool IsTemporaryValue() const final {
        return true;
    }

    ui32 GetDependencyWeight() const final {
        return this->Dependencies.size() + Sources.size();
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationExternalNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        if (dependencies.emplace(TBase::ValueIndex, TBase::RepresentationKind).second) {
            std::for_each(this->Dependencies.cbegin(), this->Dependencies.cend(), std::bind(&IComputationNode::CollectDependentIndexes, std::placeholders::_1, owner, std::ref(dependencies)));
        }
    }

    void PrepareStageOne() final {}
    void PrepareStageTwo() final {}

    const IComputationNode* GetSource() const final { return this; }

    mutable std::unordered_set<const IComputationNode*> Sources; // TODO: remove const and mutable.
};

template <typename TDerived>
class TFlowSourceComputationNode: public TFlowSourceBaseComputationNode<TDerived, IComputationNode>
{
    using TBase = TFlowSourceBaseComputationNode<TDerived, IComputationNode>;
protected:
    TFlowSourceComputationNode(TComputationMutables& mutables, EValueRepresentation kind, EValueRepresentation stateKind)
        : TBase(mutables, stateKind), RepresentationKind(kind)
    {}

private:
    EValueRepresentation GetRepresentation() const final {
        return RepresentationKind;
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(this->ValueRef(compCtx), compCtx);
    }
private:
    const EValueRepresentation RepresentationKind;
};

template <typename TDerived>
class TWideFlowSourceComputationNode: public TFlowSourceBaseComputationNode<TDerived, IComputationWideFlowNode>
{
    using TBase = TFlowSourceBaseComputationNode<TDerived, IComputationWideFlowNode>;
protected:
    TWideFlowSourceComputationNode(TComputationMutables& mutables, EValueRepresentation stateKind)
        : TBase(mutables, stateKind)
    {}
private:
    EValueRepresentation GetRepresentation() const final {
        THROW yexception() << "Failed to get representation kind.";
    }

    NUdf::TUnboxedValue GetValue(TComputationContext&) const final {
        THROW yexception() << "Failed to get value from wide flow node.";
    }

    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue*const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(this->ValueRef(compCtx), compCtx, values);
    }
};

template <typename TDerived, typename IFlowInterface>
class TFlowBaseComputationNode: public TRefCountedComputationNode<IFlowInterface>
{
protected:
    TFlowBaseComputationNode(const IComputationNode* source) : Source(source) {}

    void InitNode(TComputationContext&) const override {}

    TString DebugString() const override {
        return TypeName<TDerived>();
    }

    IComputationNode* FlowDependsOn(IComputationNode* node) const {
        if (node) {
            if (const auto source = node->AddDependence(this); dynamic_cast<IComputationExternalNode*>(source) || dynamic_cast<IComputationWideFlowProxyNode*>(source))
                return const_cast<IComputationNode*>(static_cast<const IComputationNode*>(this)); // TODO: remove const in RegisterDependencies.
            else
                return source;
        }
        return nullptr;
    }

    IComputationNode* FlowDependsOnBoth(IComputationNode* one, IComputationNode* two) const {
        const auto flowOne = FlowDependsOn(one);
        const auto flowTwo = FlowDependsOn(two);
        if (flowOne && flowTwo) {
            if (flowOne == flowTwo)
                return flowOne;
            const auto flow = const_cast<IComputationNode*>(static_cast<const IComputationNode*>(this));
            DependsOn(flow, flowOne);
            DependsOn(flow, flowTwo);
            return flow;
        } else if (flowOne) {
            return flowOne;
        } else if (flowTwo) {
            return flowTwo;
        }

        return nullptr;
    }

    IComputationNode* FlowDependsOnAll(const std::vector<IFlowInterface*, TMKQLAllocator<IFlowInterface*>>& sources) const {
        std::unordered_set<IComputationNode*> flows(sources.size());
        for (const auto& source : sources)
            if (const auto flow = FlowDependsOn(source))
                flows.emplace(flow);

        if (flows.size() > 1U) {
            const auto flow = const_cast<IComputationNode*>(static_cast<const IComputationNode*>(this));
            std::for_each(flows.cbegin(), flows.cend(), std::bind(&TFlowBaseComputationNode::DependsOn, flow, std::placeholders::_1));
            return flow;
        }

        return flows.empty() ? nullptr : *flows.cbegin();
    }

    static void DependsOn(IComputationNode* source, IComputationNode* node) {
        if (node && source && node != source) {
            node->AddDependence(source);
        }
    }

    static void Own(IComputationNode* source, IComputationExternalNode* node) {
        if (node && source && node != source) {
            node->SetOwner(source);
        }
    }

    static void OwnProxy(IComputationNode* source, IComputationWideFlowProxyNode* node) {
        if (node && source && node != source) {
            node->SetOwner(source);
        }
    }
private:
    ui32 GetDependencyWeight() const final { return 42U; }

    ui32 GetDependencesCount() const final {
        return Dependence ? 1U : 0U;
    }

    IComputationNode* AddDependence(const IComputationNode* node) final {
        if (!Dependence) {
            Dependence = node;
        }
        return this;
    }

    bool IsTemporaryValue() const final {
        return true;
    }

    void PrepareStageOne() final {}
    void PrepareStageTwo() final {}

    const IComputationNode* GetSource() const final {
        if (Source && Source != this)
            if (const auto s = Source->GetSource())
                return s;
        return this;
    }
protected:
    const IComputationNode *const Source;
    const IComputationNode *Dependence = nullptr;
};

template <typename TDerived>
class TBaseFlowBaseComputationNode: public TFlowBaseComputationNode<TDerived, IComputationNode>
{
protected:
    TBaseFlowBaseComputationNode(const IComputationNode* source, EValueRepresentation kind)
        : TFlowBaseComputationNode<TDerived, IComputationNode>(source), RepresentationKind(kind)
    {}
private:
    EValueRepresentation GetRepresentation() const final {
        return RepresentationKind;
    }

    const EValueRepresentation RepresentationKind;
};

template <typename TDerived>
class TStatelessFlowComputationNode: public TBaseFlowBaseComputationNode<TDerived>
{
protected:
    TStatelessFlowComputationNode(const IComputationNode* source, EValueRepresentation kind)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind)
    {}

    TStatelessFlowComputationNode(TComputationMutables&, const IComputationNode* source, EValueRepresentation kind)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind)
    {}

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx);
    }
private:
    ui32 GetIndex() const final {
        THROW yexception() << "Failed to get stateless node index.";
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        if (this->Dependence) {
            this->Dependence->CollectDependentIndexes(owner, dependencies);
        }
    }
};

template <typename TDerived, bool SerializableState = false>
class TStatefulFlowComputationNode: public TBaseFlowBaseComputationNode<TDerived>
{
protected:
    TStatefulFlowComputationNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation kind, EValueRepresentation stateKind = EValueRepresentation::Any)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind), StateIndex(mutables.CurValueIndex++), StateKind(stateKind)
    {
        if constexpr (SerializableState) {
            mutables.SerializableValues.push_back(StateIndex);
        }
    }

    NUdf::TUnboxedValue& RefState(TComputationContext& compCtx) const {
        return compCtx.MutableValues[GetIndex()];
    }

private:
    ui32 GetIndex() const final {
        return StateIndex;
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx.MutableValues[StateIndex], compCtx);
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        const auto ins = dependencies.emplace(StateIndex, StateKind);
        if (ins.second && this->Dependence) {
            this->Dependence->CollectDependentIndexes(owner, dependencies);
        }
    }

    const ui32 StateIndex;
    const EValueRepresentation StateKind;
};

const IComputationNode* GetCommonSource(const IComputationNode* first, const IComputationNode* second, const IComputationNode* common);

template <typename TDerived>
class TPairStateFlowComputationNode: public TBaseFlowBaseComputationNode<TDerived>
{
protected:
    TPairStateFlowComputationNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation kind, EValueRepresentation firstKind = EValueRepresentation::Any, EValueRepresentation secondKind = EValueRepresentation::Any)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind), StateIndex(mutables.CurValueIndex++), FirstKind(firstKind), SecondKind(secondKind)
    {
        ++mutables.CurValueIndex;
    }

private:
    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx.MutableValues[StateIndex], compCtx.MutableValues[StateIndex + 1U], compCtx);
    }

    ui32 GetIndex() const final {
        return StateIndex;
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        const auto ins1 = dependencies.emplace(StateIndex, FirstKind);
        const auto ins2 = dependencies.emplace(StateIndex + 1U, SecondKind);
        if (ins1.second && ins2.second && this->Dependence) {
            this->Dependence->CollectDependentIndexes(owner, dependencies);
        }
    }

    const ui32 StateIndex;
    const EValueRepresentation FirstKind, SecondKind;
};

class TWideFlowProxyComputationNode: public TRefCountedComputationNode<IComputationWideFlowProxyNode>
{
public:
    TWideFlowProxyComputationNode() = default;
protected:
    TString DebugString() const final;
private:
    void InitNode(TComputationContext&) const override {}

    EValueRepresentation GetRepresentation() const final;

    NUdf::TUnboxedValue GetValue(TComputationContext&) const final;

    ui32 GetIndex() const final;

    ui32 GetDependencyWeight() const final;

    ui32 GetDependencesCount() const final;

    const IComputationNode* GetSource() const final;

    IComputationNode* AddDependence(const IComputationNode* node) final;

    bool IsTemporaryValue() const final;

    void RegisterDependencies() const final;

    void PrepareStageOne() final;

    void PrepareStageTwo() final;

    void SetOwner(const IComputationNode* owner) final;

    void CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const final;

    void InvalidateValue(TComputationContext& ctx) const final;

    void SetFetcher(TFetcher&& fetcher) final;

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* values) const final;

protected:
    const IComputationNode* Dependence = nullptr;
    const IComputationNode* Owner = nullptr;
    std::vector<std::pair<ui32, EValueRepresentation>> InvalidationSet;
    TFetcher Fetcher;
};

template <typename TDerived>
class TWideFlowBaseComputationNode: public TFlowBaseComputationNode<TDerived, IComputationWideFlowNode>
{
protected:
    TWideFlowBaseComputationNode(const IComputationNode* source)
        : TFlowBaseComputationNode<TDerived, IComputationWideFlowNode>(source)
    {}
private:
    EValueRepresentation GetRepresentation() const final {
        THROW yexception() << "Failed to get representation kind.";
    }

    NUdf::TUnboxedValue GetValue(TComputationContext&) const {
        THROW yexception() << "Failed to get value from wide flow node.";
    }
};

template <typename TDerived>
class TStatelessWideFlowComputationNode: public TWideFlowBaseComputationNode<TDerived>
{
protected:
    TStatelessWideFlowComputationNode(const IComputationNode* source)
        :TWideFlowBaseComputationNode<TDerived>(source)
    {}

private:
    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue*const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx, values);
    }

    ui32 GetIndex() const final {
        THROW yexception() << "Failed to get stateless node index.";
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        if (this->Dependence) {
            this->Dependence->CollectDependentIndexes(owner, dependencies);
        }
    }
};

template <typename TDerived, bool SerializableState = false>
class TStatefulWideFlowComputationNode: public TWideFlowBaseComputationNode<TDerived>
{
protected:
    TStatefulWideFlowComputationNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation stateKind)
        : TWideFlowBaseComputationNode<TDerived>(source), StateIndex(mutables.CurValueIndex++), StateKind(stateKind)
    {
        if constexpr (SerializableState) {
            mutables.SerializableValues.push_back(StateIndex);
        }
    }

    NUdf::TUnboxedValue& RefState(TComputationContext& compCtx) const {
        return compCtx.MutableValues[GetIndex()];
    }
private:
    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue*const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx.MutableValues[StateIndex], compCtx, values);
    }

    ui32 GetIndex() const final {
        return StateIndex;
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        const auto ins = dependencies.emplace(StateIndex, StateKind);
        if (ins.second && this->Dependence) {
            this->Dependence->CollectDependentIndexes(owner, dependencies);
        }
    }

    const ui32 StateIndex;
    const EValueRepresentation StateKind;
};

template <typename TDerived>
class TPairStateWideFlowComputationNode: public TWideFlowBaseComputationNode<TDerived>
{
protected:
    TPairStateWideFlowComputationNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation firstKind, EValueRepresentation secondKind)
        : TWideFlowBaseComputationNode<TDerived>(source), StateIndex(mutables.CurValueIndex++), FirstKind(firstKind), SecondKind(secondKind)
    {
        ++mutables.CurValueIndex;
    }

private:
    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue*const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx.MutableValues[StateIndex], compCtx.MutableValues[StateIndex + 1U], compCtx, values);
    }

    ui32 GetIndex() const final {
        return StateIndex;
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        const auto ins1 = dependencies.emplace(StateIndex, FirstKind);
        const auto ins2 = dependencies.emplace(StateIndex + 1U, SecondKind);
        if (ins1.second && ins2.second && this->Dependence) {
            this->Dependence->CollectDependentIndexes(owner, dependencies);
        }
    }

    const ui32 StateIndex;
    const EValueRepresentation FirstKind, SecondKind;
};

template <typename TDerived>
class TDecoratorComputationNode: public TRefCountedComputationNode<IComputationNode>
{
private:
    void InitNode(TComputationContext&) const final {}

    const IComputationNode* GetSource() const final { return Node; }

    IComputationNode* AddDependence(const IComputationNode* node) final {
        return Node->AddDependence(node);
    }

    EValueRepresentation GetRepresentation() const final { return Kind; }
    bool IsTemporaryValue() const final { return true; }

    void PrepareStageOne() final {}
    void PrepareStageTwo() final {}

    void RegisterDependencies() const final { Node->AddDependence(this); }

    void CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const final {}

    ui32 GetDependencyWeight() const final { return 0U; }

    ui32 GetDependencesCount() const final {
        return Node->GetDependencesCount();
    }

    ui32 GetIndex() const final {
        THROW yexception() << "Can't get index from decorator node.";
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx, Node->GetValue(compCtx));
    }

protected:
    TDecoratorComputationNode(IComputationNode* node, EValueRepresentation kind)
        : Node(node), Kind(kind)
    {}

    TDecoratorComputationNode(IComputationNode* node)
        : Node(node), Kind(node->GetRepresentation())
    {}


    TString DebugString() const override {
        return TypeName<TDerived>() += "(" + Node->DebugString() + ")";
    }

    IComputationNode *const Node;
    const EValueRepresentation Kind;
};

template <typename TDerived>
class TBinaryComputationNode: public TRefCountedComputationNode<IComputationNode>
{
private:
    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(ctx);
    }

    const IComputationNode* GetSource() const final {
        return GetCommonSource(Left, Right, this);
    }

    void InitNode(TComputationContext&) const final {}
protected:
    TString DebugString() const override {
        return TypeName<TDerived>() += "(" + Left->DebugString() + "," + Right->DebugString() + ")";
    }

    IComputationNode* AddDependence(const IComputationNode* node) final {
        const auto l = Left->AddDependence(node);
        const auto r = Right->AddDependence(node);

        if (!l) return r;
        if (!r) return l;

        return this;
    }

    EValueRepresentation GetRepresentation() const final {
        return Kind;
    }

    bool IsTemporaryValue() const final { return true; }

    void PrepareStageOne() final {}
    void PrepareStageTwo() final {}

    void RegisterDependencies() const final {
        Left->AddDependence(this);
        Right->AddDependence(this);
    }

    void CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const final {}

    ui32 GetDependencyWeight() const final { return 0U; }

    ui32 GetDependencesCount() const final {
        return Left->GetDependencesCount() + Right->GetDependencesCount();
    }

    ui32 GetIndex() const final {
        THROW yexception() << "Can't get index from decorator node.";
    }

    TBinaryComputationNode(IComputationNode* left, IComputationNode* right, const EValueRepresentation kind)
        : Left(left), Right(right), Kind(kind)
    {
    }

    IComputationNode *const Left;
    IComputationNode *const Right;
    const EValueRepresentation Kind;
};

template <typename TDerived, typename TBaseExt = NYql::NUdf::IBoxedValue>
class TComputationValueBase: public TBaseExt
{
private:
    using TBase = TBaseExt;
public:
    template <typename... Args>
    TComputationValueBase(Args&&... args)
        : TBase(std::forward<Args>(args)...)
    {
    }

    ~TComputationValueBase() {
    }

private:
    bool HasFastListLength() const override {
        ThrowNotSupported(__func__);
        return false;
    }

    ui64 GetListLength() const override {
        ThrowNotSupported(__func__);
        return 0;
    }

    ui64 GetEstimatedListLength() const override {
        ThrowNotSupported(__func__);
        return 0;
    }

    bool HasListItems() const override {
        ThrowNotSupported(__func__);
        return false;
    }

    const NUdf::TOpaqueListRepresentation* GetListRepresentation() const override {
        return nullptr;
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return nullptr;
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        Y_UNUSED(count);
        return nullptr;
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        Y_UNUSED(count);
        return nullptr;
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return nullptr;
    }

    ui64 GetDictLength() const override {
        ThrowNotSupported(__func__);
        return 0;
    }

    bool HasDictItems() const override {
        ThrowNotSupported(__func__);
        return false;
    }

    NUdf::TStringRef GetResourceTag() const override {
        ThrowNotSupported(__func__);
        return NUdf::TStringRef();
    }

    void* GetResource() override {
        ThrowNotSupported(__func__);
        return nullptr;
    }

    void Apply(NUdf::IApplyContext& applyCtx) const override {
        Y_UNUSED(applyCtx);
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        ThrowNotSupported(__func__);
        return {};
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        ThrowNotSupported(__func__);
        return {};
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        ThrowNotSupported(__func__);
        return {};
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        ThrowNotSupported(__func__);
        return {};
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        Y_UNUSED(key);
        ThrowNotSupported(__func__);
        return false;
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        Y_UNUSED(key);
        ThrowNotSupported(__func__);
        return NUdf::TUnboxedValuePod();
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        Y_UNUSED(index);
        ThrowNotSupported(__func__);
        return {};
    }

    const NUdf::TUnboxedValue* GetElements() const override {
        return nullptr;
    }

    NUdf::TUnboxedValue Run(
            const NUdf::IValueBuilder* valueBuilder,
            const NUdf::TUnboxedValuePod* args) const override
    {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        ThrowNotSupported(__func__);
        return {};
    }

    bool Skip() override {
        NUdf::TUnboxedValue stub;
        return Next(stub);
    }

    bool Next(NUdf::TUnboxedValue&) override {
        ThrowNotSupported(__func__);
        return false;
    }

    bool NextPair(NUdf::TUnboxedValue&, NUdf::TUnboxedValue&) override {
        ThrowNotSupported(__func__);
        return false;
    }

    ui32 GetVariantIndex() const override {
        ThrowNotSupported(__func__);
        return 0;
    }

    NUdf::TUnboxedValue GetVariantItem() const override {
        ThrowNotSupported(__func__);
        return {};
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        Y_UNUSED(result);
        ThrowNotSupported(__func__);
        return NUdf::EFetchStatus::Finish;
    }

    ui32 GetTraverseCount() const override {
        ThrowNotSupported(__func__);
        return 0;
    }

    NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
        Y_UNUSED(index);
        ThrowNotSupported(__func__);
        return {};
    }

    NUdf::TUnboxedValue Save() const override {
        ThrowNotSupported(__func__);
        return NUdf::TUnboxedValue::Zero();
    }

    void Load(const NUdf::TStringRef& state) override {
        Y_UNUSED(state);
        ThrowNotSupported(__func__);
    }

    bool Load2(const NUdf::TUnboxedValue& state) override {
        Y_UNUSED(state);
        ThrowNotSupported(__func__);
        return false;
    }

    void Push(const NUdf::TUnboxedValuePod& value) override {
        Y_UNUSED(value);
        ThrowNotSupported(__func__);
    }

    bool IsSortedDict() const override {
        ThrowNotSupported(__func__);
        return false;
    }

    void Unused1() override {
        ThrowNotSupported(__func__);
    }

    void Unused2() override {
        ThrowNotSupported(__func__);
    }

    void Unused3() override {
        ThrowNotSupported(__func__);
    }

    void Unused4() override {
        ThrowNotSupported(__func__);
    }

    void Unused5() override {
        ThrowNotSupported(__func__);
    }

    void Unused6() override {
        ThrowNotSupported(__func__);
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) override {
        Y_UNUSED(result);
        Y_UNUSED(width);
        ThrowNotSupported(__func__);
        return NUdf::EFetchStatus::Finish;
    }

public:
    TString DebugString() const {
        return TypeName<TDerived>();
    }

protected:
    [[noreturn]] void ThrowNotSupported(const char* func) const {
        THROW yexception() << "Unsupported access to '" << func << "' method of: " << TypeName(*this);
    }
};

template <typename TDerived, typename TBaseExt, EMemorySubPool MemoryPool>
class TComputationValueImpl: public TComputationValueBase<TDerived, TBaseExt>,
    public TWithMiniKQLAlloc<MemoryPool> {
private:
    using TBase = TComputationValueBase<TDerived, TBaseExt>;
protected:
    inline TMemoryUsageInfo* GetMemInfo() const {
#ifndef NDEBUG
        return static_cast<TMemoryUsageInfo*>(M_.MemInfo);
#else
        return nullptr;
#endif
    }
    using TWithMiniKQLAlloc<MemoryPool>::AllocWithSize;
    using TWithMiniKQLAlloc<MemoryPool>::FreeWithSize;
public:
    template <typename... Args>
    TComputationValueImpl(TMemoryUsageInfo* memInfo, Args&&... args)
        : TBase(std::forward<Args>(args)...) {
#ifndef NDEBUG
        M_.MemInfo = memInfo;
        MKQL_MEM_TAKE(memInfo, this, sizeof(TDerived), __MKQL_LOCATION__);
#else
        Y_UNUSED(memInfo);
#endif
    }

    ~TComputationValueImpl() {
#ifndef NDEBUG
        MKQL_MEM_RETURN(GetMemInfo(), this, sizeof(TDerived));
#endif
    }
private:
#ifndef NDEBUG
    struct {
        void* MemInfo; // used for tracking memory usage during execution
    } M_;
#endif
};

template <typename TDerived, typename TBaseExt = NUdf::IBoxedValue>
class TTemporaryComputationValue: public TComputationValueImpl<TDerived, TBaseExt, EMemorySubPool::Temporary> {
private:
    using TBase = TComputationValueImpl<TDerived, TBaseExt, EMemorySubPool::Temporary>;
public:
    using TBase::TBase;
};

template <typename TDerived, typename TBaseExt = NUdf::IBoxedValue>
class TComputationValue: public TComputationValueImpl<TDerived, TBaseExt, EMemorySubPool::Default> {
private:
    using TBase = TComputationValueImpl<TDerived, TBaseExt, EMemorySubPool::Default>;
public:
    using TBase::TBase;
};

template<bool IsStream>
struct TThresher;

template<>
struct TThresher<true> {
    template<class Handler>
    static void DoForEachItem(const NUdf::TUnboxedValuePod& stream, const Handler& handler) {
        for (NUdf::TUnboxedValue item;; handler(std::move(item))) {
            const auto status = stream.Fetch(item);
            MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Unexpected stream status!");
            if (status == NUdf::EFetchStatus::Finish)
                break;
        }
    }
};

template<>
struct TThresher<false> {
    template<class Handler>
    static void DoForEachItem(const NUdf::TUnboxedValuePod& list, const Handler& handler) {
        if (auto ptr = list.GetElements()) {
            if (auto size = list.GetListLength()) do {
                handler(NUdf::TUnboxedValue(*ptr++));
            } while (--size);
        } else if (const auto& iter = list.GetListIterator()) {
            for (NUdf::TUnboxedValue item; iter.Next(item); handler(std::move(item)))
                continue;
        }
    }
};

IComputationNode* LocateNode(const TNodeLocator& nodeLocator, TCallable& callable, ui32 index, bool pop = false);
IComputationNode* LocateNode(const TNodeLocator& nodeLocator, TNode& node, bool pop = false);
IComputationExternalNode* LocateExternalNode(const TNodeLocator& nodeLocator, TCallable& callable, ui32 index, bool pop = true);

using TPasstroughtMap = std::vector<std::optional<size_t>>;

template<class TContainerOne, class TContainerTwo>
TPasstroughtMap GetPasstroughtMap(const TContainerOne& from, const TContainerTwo& to);

template<class TContainerOne, class TContainerTwo>
TPasstroughtMap GetPasstroughtMapOneToOne(const TContainerOne& from, const TContainerTwo& to);

std::optional<size_t> IsPasstrought(const IComputationNode* root, const TComputationExternalNodePtrVector& args);

TPasstroughtMap MergePasstroughtMaps(const TPasstroughtMap& lhs, const TPasstroughtMap& rhs);

void ApplyChanges(const NUdf::TUnboxedValue& value, NUdf::IApplyContext& applyCtx);
void CleanupCurrentContext();

}
}
