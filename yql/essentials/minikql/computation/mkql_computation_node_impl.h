#pragma once

#include "mkql_computation_node.h"

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/system/type_name.h>

namespace NKikimr::NMiniKQL {

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
class TRefCountedComputationNode: public IComputationNodeInterface {
private:
    void Ref() final;

    void UnRef() final;

    ui32 RefCount() const final;

private:
    ui32 Refs_ = 0;
};

class TUnboxedImmutableComputationNode: public TRefCountedComputationNode<IComputationNode> {
public:
    TUnboxedImmutableComputationNode(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value);

    ~TUnboxedImmutableComputationNode() override;

private:
    void InitNode(TComputationContext&) const override {
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final;

    const IComputationNode* GetSource() const final;

    IComputationNode* AddDependent(const IComputationNode*) final;
    void AddDependency(const IComputationNode*) const final;
    void AddOwned(IComputationExternalNode*) const final;

    void RegisterDependencies() const final;

    ui32 GetIndex() const final;

    void CollectDependentIndexes(const IComputationNode* owner, TIndexesMap&) const final;
    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final;

    ui32 GetDependentWeight() const final;

    ui32 GetDependentsCount() const final;

    TComputationExternalNodePtrSet GetUpvalues() const final;

    bool IsTemporaryValue() const final;

    void PrepareStageOne() final;
    void PrepareStageTwo() final;

    TString DebugString() const final;

    EValueRepresentation GetRepresentation() const final;

    TMemoryUsageInfo* const MemInfo_;

protected:
    const NUdf::TUnboxedValue UnboxedValue_;
    const EValueRepresentation RepresentationKind_;
};

class TStatefulComputationNodeBase {
protected:
    TStatefulComputationNodeBase(ui32 valueIndex, EValueRepresentation kind);
    ~TStatefulComputationNodeBase();
    void AddDependentImpl(const IComputationNode* node);
    void AddDependencyImpl(const IComputationNode* node) const;
    void AddOwnedImpl(IComputationExternalNode* node) const;
    void CollectDependentIndexesImpl(const IComputationNode* self, const IComputationNode* owner,
                                     IComputationNode::TIndexesMap& dependents, bool stateless) const;
    void CollectUpvaluesImpl(TComputationExternalNodePtrSet& upvalues) const;

    TConstComputationNodePtrVector Dependents_;
    mutable TConstComputationNodePtrVector Dependencies_;
    mutable TComputationExternalNodePtrSet Owned_;
    TComputationExternalNodePtrSet Upvalues_;
    mutable bool UpvaluesCollected_;

    const ui32 ValueIndex_;
    const EValueRepresentation RepresentationKind_;
};

template <class IComputationNodeInterface, bool SerializableState = false>
class TStatefulComputationNode: public TRefCountedComputationNode<IComputationNodeInterface>, protected TStatefulComputationNodeBase {
protected:
    TStatefulComputationNode(TComputationMutables& mutables, EValueRepresentation kind);

protected:
    void InitNode(TComputationContext&) const override;

    ui32 GetIndex() const final;

    IComputationNode* AddDependent(const IComputationNode* node) final;
    void AddDependency(const IComputationNode* node) const final;
    void AddOwned(IComputationExternalNode* node) const final;

    EValueRepresentation GetRepresentation() const override;

    NUdf::TUnboxedValue& ValueRef(TComputationContext& compCtx) const {
        return compCtx.MutableValues[ValueIndex_];
    }

private:
    ui32 GetDependentsCount() const final;
};

class TExternalComputationNode: public TStatefulComputationNode<IComputationExternalNode> {
public:
    explicit TExternalComputationNode(TComputationMutables& mutables, EValueRepresentation kind = EValueRepresentation::Any);

protected:
    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override;

    NUdf::TUnboxedValue& RefValue(TComputationContext& compCtx) const override;
    void SetValue(TComputationContext& compCtx, NUdf::TUnboxedValue&& value) const override;

    TString DebugString() const final;

private:
    ui32 GetDependentWeight() const final;

    TComputationExternalNodePtrSet GetUpvalues() const final;

    void RegisterDependencies() const final;

    void SetOwner(const IComputationNode* owner) final;

    void PrepareStageOne() final;
    void PrepareStageTwo() final;

    void CollectDependentIndexes(const IComputationNode* owner, TIndexesMap& dependents) const final;
    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final;

    bool IsTemporaryValue() const final;

    const IComputationNode* Owner_ = nullptr;

    void SetGetter(TGetter&& getter) final;

    void InvalidateValue(TComputationContext& compCtx) const final;

    const IComputationNode* GetSource() const final;

protected:
    std::vector<std::pair<ui32, EValueRepresentation>> InvalidationSet_;
    TGetter Getter_;
};

class TStatefulSourceComputationNodeBase {
protected:
    TStatefulSourceComputationNodeBase();
    ~TStatefulSourceComputationNodeBase();
    void PrepareStageOneImpl(const TConstComputationNodePtrVector& dependents);
    void AddSource(IComputationNode* source) const;

    mutable std::unordered_set<const IComputationNode*> Sources_; // TODO: remove const and mutable.
    std::optional<bool> Stateless_;
};

template <typename TDerived, bool SerializableState = false>
class TStatefulSourceComputationNode: public TStatefulComputationNode<IComputationNode, SerializableState>,
                                      protected TStatefulSourceComputationNodeBase {
    using TStatefulComputationNode = TStatefulComputationNode<IComputationNode, SerializableState>;

private:
    bool IsTemporaryValue() const final {
        return *Stateless_;
    }

    ui32 GetDependentWeight() const final {
        return Sources_.size();
    }

    TComputationExternalNodePtrSet GetUpvalues() const final {
        MKQL_ENSURE(this->UpvaluesCollected_, "Upvalues have not been collected yet");
        return this->Upvalues_;
    }

    void PrepareStageOne() final {
        PrepareStageOneImpl(this->Dependents_);
    }

    void PrepareStageTwo() final {
        CollectUpvalues(this->Upvalues_);
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependents) const final {
        this->CollectDependentIndexesImpl(this, owner, dependents, *Stateless_);
    }

    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final {
        this->CollectUpvaluesImpl(upvalues);
    }

    const IComputationNode* GetSource() const final {
        return this;
    }

protected:
    explicit TStatefulSourceComputationNode(TComputationMutables& mutables, EValueRepresentation kind = EValueRepresentation::Any)
        : TStatefulComputationNode(mutables, kind)
    {
    }

    void DependsOn(IComputationNode* node) const {
        if (node) {
            this->AddDependency(node);
            if (const auto source = node->AddDependent(this)) {
                AddSource(source);
            }
        }
    }

    void Own(IComputationExternalNode* node) const {
        if (node) {
            this->AddOwned(node);
            node->SetOwner(this);
        }
    }

    TString DebugString() const override {
        return TypeName<TDerived>();
    }
};

template <typename TDerived>
class TMutableComputationNode: public TStatefulSourceComputationNode<TDerived> {
protected:
    using TStatefulSourceComputationNode<TDerived>::TStatefulSourceComputationNode;

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override {
        if (*this->Stateless_) {
            return static_cast<const TDerived*>(this)->DoCalculate(compCtx);
        }
        NUdf::TUnboxedValue& valueRef = this->ValueRef(compCtx);
        if (valueRef.IsInvalid()) {
            valueRef = static_cast<const TDerived*>(this)->DoCalculate(compCtx);
        }

        return valueRef;
    }
};

template <typename TDerived, typename IFlowInterface>
class TFlowSourceBaseComputationNode: public TStatefulComputationNode<IFlowInterface> {
    using TBase = TStatefulComputationNode<IFlowInterface>;

protected:
    TFlowSourceBaseComputationNode(TComputationMutables& mutables, EValueRepresentation stateKind)
        : TBase(mutables, stateKind)
    {
    }

    TString DebugString() const override {
        return TypeName<TDerived>();
    }

    void DependsOn(IComputationNode* node) const {
        if (node) {
            this->AddDependency(node);
            if (const auto source = node->AddDependent(this)) {
                Sources_.emplace(source);
            }
        }
    }

    void Own(IComputationExternalNode* node) const {
        if (node) {
            this->AddOwned(node);
            node->SetOwner(this);
        }
    }

private:
    bool IsTemporaryValue() const final {
        return true;
    }

    ui32 GetDependentWeight() const final {
        return this->Dependents_.size() + Sources_.size();
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationExternalNode::TIndexesMap& dependents) const final {
        this->CollectDependentIndexesImpl(this, owner, dependents, false);
    }

    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final {
        this->CollectUpvaluesImpl(upvalues);
    }

    TComputationExternalNodePtrSet GetUpvalues() const final {
        MKQL_ENSURE(this->UpvaluesCollected_, "Upvalues have not been collected yet");
        return this->Upvalues_;
    }

    void PrepareStageOne() final {
    }
    void PrepareStageTwo() final {
        CollectUpvalues(this->Upvalues_);
    }

    const IComputationNode* GetSource() const final {
        return this;
    }

    mutable std::unordered_set<const IComputationNode*> Sources_; // TODO: remove const and mutable.
};

template <typename TDerived>
class TFlowSourceComputationNode: public TFlowSourceBaseComputationNode<TDerived, IComputationNode> {
    using TBase = TFlowSourceBaseComputationNode<TDerived, IComputationNode>;

protected:
    TFlowSourceComputationNode(TComputationMutables& mutables, EValueRepresentation kind, EValueRepresentation stateKind)
        : TBase(mutables, stateKind)
        , RepresentationKind_(kind)
    {
    }

private:
    EValueRepresentation GetRepresentation() const final {
        return RepresentationKind_;
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(this->ValueRef(compCtx), compCtx);
    }

private:
    const EValueRepresentation RepresentationKind_;
};

template <typename TDerived>
class TWideFlowSourceComputationNode: public TFlowSourceBaseComputationNode<TDerived, IComputationWideFlowNode> {
    using TBase = TFlowSourceBaseComputationNode<TDerived, IComputationWideFlowNode>;

protected:
    TWideFlowSourceComputationNode(TComputationMutables& mutables, EValueRepresentation stateKind)
        : TBase(mutables, stateKind)
    {
    }

private:
    EValueRepresentation GetRepresentation() const final {
        THROW yexception() << "Failed to get representation kind.";
    }

    NUdf::TUnboxedValue GetValue(TComputationContext&) const final {
        THROW yexception() << "Failed to get value from wide flow node.";
    }

    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue* const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(this->ValueRef(compCtx), compCtx, values);
    }
};

template <typename TDerived, typename IFlowInterface>
class TFlowBaseComputationNode: public TRefCountedComputationNode<IFlowInterface> {
protected:
    explicit TFlowBaseComputationNode(const IComputationNode* source)
        : Source_(source)
        , UpvaluesCollected_(false)
    {
    }

    void InitNode(TComputationContext&) const override {
    }

    TString DebugString() const override {
        return TypeName<TDerived>();
    }

    IComputationNode* FlowDependsOn(IComputationNode* node) const {
        if (node) {
            this->AddDependency(node);
            if (const auto source = node->AddDependent(this);
                dynamic_cast<IComputationExternalNode*>(source) ||
                dynamic_cast<IComputationWideFlowProxyNode*>(source)) {
                return const_cast<IComputationNode*>(static_cast<const IComputationNode*>(this)); // TODO: remove const in RegisterDependencies.
            } else {
                return source;
            }
        }
        return nullptr;
    }

    IComputationNode* FlowDependsOnBoth(IComputationNode* one, IComputationNode* two) const {
        const auto flowOne = FlowDependsOn(one);
        const auto flowTwo = FlowDependsOn(two);
        if (flowOne && flowTwo) {
            if (flowOne == flowTwo) {
                return flowOne;
            }
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
        for (const auto& source : sources) {
            if (const auto flow = FlowDependsOn(source)) {
                flows.emplace(flow);
            }
        }

        if (flows.size() > 1U) {
            const auto flow = const_cast<IComputationNode*>(static_cast<const IComputationNode*>(this));
            std::for_each(flows.cbegin(), flows.cend(),
                          std::bind(&TFlowBaseComputationNode::DependsOn, flow, std::placeholders::_1));
            return flow;
        }

        return flows.empty() ? nullptr : *flows.cbegin();
    }

    static void DependsOn(IComputationNode* source, IComputationNode* node) {
        if (node && source && node != source) {
            source->AddDependency(node);
            node->AddDependent(source);
        }
    }

    static void Own(IComputationNode* source, IComputationExternalNode* node) {
        if (node && source && node != source) {
            source->AddOwned(node);
            node->SetOwner(source);
        }
    }

    static void OwnProxy(IComputationNode* source, IComputationWideFlowProxyNode* node) {
        if (node && source && node != source) {
            node->SetOwner(source);
        }
    }

private:
    ui32 GetDependentWeight() const final {
        return 42U;
    }

    ui32 GetDependentsCount() const final {
        return Dependents_.size();
    }

    TComputationExternalNodePtrSet GetUpvalues() const final {
        MKQL_ENSURE(this->UpvaluesCollected_, "Upvalues have not been collected yet");
        return this->Upvalues_;
    }

    IComputationNode* AddDependent(const IComputationNode* node) final {
        Dependents_.push_back(node);
        return this;
    }

    void AddDependency(const IComputationNode* node) const final {
        Dependencies_.push_back(node);
    }

    void AddOwned(IComputationExternalNode* node) const final {
        Owned_.emplace(node);
    }

    bool IsTemporaryValue() const final {
        return true;
    }

    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final {
        // XXX: If upvalues for the node are already collected,
        // just enrich the set, given by the caller;..
        if (this->UpvaluesCollected_) {
            upvalues.insert(this->Upvalues_.cbegin(), this->Upvalues_.cend());
            return;
        }
        // ... otherwise, recursively collect the upvalue candidates...
        std::for_each(Dependencies_.cbegin(), Dependencies_.cend(), std::bind(&IComputationNode::CollectUpvalues, std::placeholders::_1, std::ref(upvalues)));
        // ... and filter out the owned nodes.
        std::erase_if(upvalues, [this](IComputationExternalNode* uv) { return this->Owned_.find(uv) != this->Owned_.cend(); });
        this->UpvaluesCollected_ = true;
    }

    void PrepareStageOne() final {
    }
    void PrepareStageTwo() final {
        CollectUpvalues(this->Upvalues_);
    }

    const IComputationNode* GetSource() const final {
        if (Source_ && Source_ != this) {
            if (const auto s = Source_->GetSource()) {
                return s;
            }
        }
        return this;
    }

protected:
    const IComputationNode* const Source_;
    TConstComputationNodePtrVector Dependents_;
    mutable TConstComputationNodePtrVector Dependencies_;
    mutable TComputationExternalNodePtrSet Owned_;
    TComputationExternalNodePtrSet Upvalues_;
    mutable bool UpvaluesCollected_;
};

template <typename TDerived>
class TBaseFlowBaseComputationNode: public TFlowBaseComputationNode<TDerived, IComputationNode> {
protected:
    TBaseFlowBaseComputationNode(const IComputationNode* source, EValueRepresentation kind)
        : TFlowBaseComputationNode<TDerived, IComputationNode>(source)
        , RepresentationKind_(kind)
    {
    }

private:
    EValueRepresentation GetRepresentation() const final {
        return RepresentationKind_;
    }

    const EValueRepresentation RepresentationKind_;
};

class TStatelessFlowComputationNodeBase {
protected:
    ui32 GetIndexImpl() const;
    void CollectDependentIndexesImpl(const IComputationNode* self,
                                     const IComputationNode* owner, IComputationNode::TIndexesMap& dependents,
                                     const TConstComputationNodePtrVector& dependences) const;
};

template <typename TDerived>
class TStatelessFlowComputationNode: public TBaseFlowBaseComputationNode<TDerived>, protected TStatelessFlowComputationNodeBase {
protected:
    TStatelessFlowComputationNode(const IComputationNode* source, EValueRepresentation kind)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind)
    {
    }

    TStatelessFlowComputationNode(TComputationMutables&, const IComputationNode* source, EValueRepresentation kind)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind)
    {
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx);
    }

private:
    ui32 GetIndex() const final {
        return GetIndexImpl();
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependents) const final {
        CollectDependentIndexesImpl(this, owner, dependents, this->Dependents_);
    }
};

class TStatefulFlowComputationNodeBase {
protected:
    TStatefulFlowComputationNodeBase(ui32 stateIndex, EValueRepresentation stateKind);
    void CollectDependentIndexesImpl(const IComputationNode* self,
                                     const IComputationNode* owner,
                                     IComputationNode::TIndexesMap& dependents,
                                     const TConstComputationNodePtrVector& dependences) const;

    const ui32 StateIndex_;
    const EValueRepresentation StateKind_;
};

template <typename TDerived, bool SerializableState = false>
class TStatefulFlowComputationNode: public TBaseFlowBaseComputationNode<TDerived>, protected TStatefulFlowComputationNodeBase {
protected:
    TStatefulFlowComputationNode(TComputationMutables& mutables,
                                 const IComputationNode* source,
                                 EValueRepresentation kind,
                                 EValueRepresentation stateKind = EValueRepresentation::Any)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind)
        , TStatefulFlowComputationNodeBase(mutables.CurValueIndex++, stateKind)
    {
        if constexpr (SerializableState) {
            mutables.SerializableValues.push_back(StateIndex_);
        }
    }

    NUdf::TUnboxedValue& RefState(TComputationContext& compCtx) const {
        return compCtx.MutableValues[GetIndex()];
    }

private:
    ui32 GetIndex() const final {
        return StateIndex_;
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx.MutableValues[StateIndex_], compCtx);
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependents) const final {
        CollectDependentIndexesImpl(this, owner, dependents, this->Dependents_);
    }
};

const IComputationNode* GetCommonSource(const IComputationNode* first, const IComputationNode* second, const IComputationNode* common);

class TPairStateFlowComputationNodeBase {
protected:
    TPairStateFlowComputationNodeBase(ui32 stateIndex, EValueRepresentation firstKind, EValueRepresentation secondKind);
    void CollectDependentIndexesImpl(const IComputationNode* self,
                                     const IComputationNode* owner,
                                     IComputationNode::TIndexesMap& dependents,
                                     const TConstComputationNodePtrVector& dependences) const;

    const ui32 StateIndex_;
    const EValueRepresentation FirstKind_, SecondKind_;
};

template <typename TDerived>
class TPairStateFlowComputationNode: public TBaseFlowBaseComputationNode<TDerived>, protected TPairStateFlowComputationNodeBase {
protected:
    TPairStateFlowComputationNode(TComputationMutables& mutables,
                                  const IComputationNode* source,
                                  EValueRepresentation kind,
                                  EValueRepresentation firstKind = EValueRepresentation::Any,
                                  EValueRepresentation secondKind = EValueRepresentation::Any)
        : TBaseFlowBaseComputationNode<TDerived>(source, kind)
        , TPairStateFlowComputationNodeBase(mutables.CurValueIndex++, firstKind, secondKind)
    {
        ++mutables.CurValueIndex;
    }

private:
    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(
            compCtx.MutableValues[StateIndex_], compCtx.MutableValues[StateIndex_ + 1U], compCtx);
    }

    ui32 GetIndex() const final {
        return StateIndex_;
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependents) const final {
        CollectDependentIndexesImpl(this, owner, dependents, this->Dependents_);
    }
};

class TWideFlowProxyComputationNode: public TRefCountedComputationNode<IComputationWideFlowProxyNode> {
public:
    TWideFlowProxyComputationNode() = default;

protected:
    TString DebugString() const final;

private:
    void InitNode(TComputationContext&) const override {
    }

    EValueRepresentation GetRepresentation() const final;

    NUdf::TUnboxedValue GetValue(TComputationContext&) const final;

    ui32 GetIndex() const final;

    ui32 GetDependentWeight() const final;

    ui32 GetDependentsCount() const final;

    TComputationExternalNodePtrSet GetUpvalues() const final;

    const IComputationNode* GetSource() const final;

    IComputationNode* AddDependent(const IComputationNode* node) final;
    void AddDependency(const IComputationNode* node) const final;
    void AddOwned(IComputationExternalNode* node) const final;

    bool IsTemporaryValue() const final;

    void RegisterDependencies() const final;

    void PrepareStageOne() final;

    void PrepareStageTwo() final;

    void SetOwner(const IComputationNode* owner) final;

    void CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const final;

    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final;

    void InvalidateValue(TComputationContext& ctx) const final;

    void SetFetcher(TFetcher&& fetcher) final;

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* values) const final;

protected:
    TConstComputationNodePtrVector Dependents_;
    mutable TConstComputationNodePtrVector Dependencies_;
    mutable TComputationExternalNodePtrSet Owned_;
    const IComputationNode* Owner_ = nullptr;
    std::vector<std::pair<ui32, EValueRepresentation>> InvalidationSet_;
    TFetcher Fetcher_;
};

class TWideFlowBaseComputationNodeBase {
protected:
    EValueRepresentation GetRepresentationImpl() const;
    NUdf::TUnboxedValue GetValueImpl(TComputationContext&) const;
};

template <typename TDerived>
class TWideFlowBaseComputationNode: public TFlowBaseComputationNode<TDerived, IComputationWideFlowNode>,
                                    protected TWideFlowBaseComputationNodeBase {
protected:
    explicit TWideFlowBaseComputationNode(const IComputationNode* source)
        : TFlowBaseComputationNode<TDerived, IComputationWideFlowNode>(source)
    {
    }

private:
    EValueRepresentation GetRepresentation() const final {
        return GetRepresentationImpl();
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const {
        return GetValueImpl(ctx);
    }
};

class TStatelessWideFlowComputationNodeBase {
protected:
    ui32 GetIndexImpl() const;
    void CollectDependentIndexesImpl(const IComputationNode* self,
                                     const IComputationNode* owner,
                                     IComputationNode::TIndexesMap& dependents,
                                     const TConstComputationNodePtrVector& dependences) const;
};

template <typename TDerived>
class TStatelessWideFlowComputationNode: public TWideFlowBaseComputationNode<TDerived>,
                                         protected TStatelessWideFlowComputationNodeBase {
protected:
    explicit TStatelessWideFlowComputationNode(const IComputationNode* source)
        : TWideFlowBaseComputationNode<TDerived>(source)
    {
    }

private:
    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue* const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx, values);
    }

    ui32 GetIndex() const final {
        return GetIndexImpl();
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependents) const final {
        CollectDependentIndexesImpl(this, owner, dependents, this->Dependents_);
    }
};

class TStatefulWideFlowComputationNodeBase {
protected:
    TStatefulWideFlowComputationNodeBase(ui32 stateIndex, EValueRepresentation stateKind);
    void CollectDependentIndexesImpl(const IComputationNode* self,
                                     const IComputationNode* owner,
                                     IComputationNode::TIndexesMap& dependents,
                                     const TConstComputationNodePtrVector& dependences) const;

    const ui32 StateIndex_;
    const EValueRepresentation StateKind_;
};

template <typename TDerived, bool SerializableState = false>
class TStatefulWideFlowComputationNode: public TWideFlowBaseComputationNode<TDerived>,
                                        protected TStatefulWideFlowComputationNodeBase {
protected:
    TStatefulWideFlowComputationNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation stateKind)
        : TWideFlowBaseComputationNode<TDerived>(source)
        , TStatefulWideFlowComputationNodeBase(mutables.CurValueIndex++, stateKind)
    {
        if constexpr (SerializableState) {
            mutables.SerializableValues.push_back(StateIndex_);
        }
    }

    NUdf::TUnboxedValue& RefState(TComputationContext& compCtx) const {
        return compCtx.MutableValues[GetIndex()];
    }

private:
    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue* const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx.MutableValues[StateIndex_], compCtx, values);
    }

    ui32 GetIndex() const final {
        return StateIndex_;
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependents) const final {
        CollectDependentIndexesImpl(this, owner, dependents, this->Dependents_);
    }
};

class TPairStateWideFlowComputationNodeBase {
protected:
    TPairStateWideFlowComputationNodeBase(ui32 stateIndex, EValueRepresentation firstKind, EValueRepresentation secondKind);
    void CollectDependentIndexesImpl(const IComputationNode* self,
                                     const IComputationNode* owner,
                                     IComputationNode::TIndexesMap& dependents,
                                     const TConstComputationNodePtrVector& dependences) const;

    const ui32 StateIndex_;
    const EValueRepresentation FirstKind_, SecondKind_;
};

template <typename TDerived>
class TPairStateWideFlowComputationNode: public TWideFlowBaseComputationNode<TDerived>,
                                         protected TPairStateWideFlowComputationNodeBase {
protected:
    TPairStateWideFlowComputationNode(TComputationMutables& mutables,
                                      const IComputationNode* source,
                                      EValueRepresentation firstKind,
                                      EValueRepresentation secondKind)
        : TWideFlowBaseComputationNode<TDerived>(source)
        , TPairStateWideFlowComputationNodeBase(mutables.CurValueIndex++, firstKind, secondKind)
    {
        ++mutables.CurValueIndex;
    }

private:
    EFetchResult FetchValues(TComputationContext& compCtx, NUdf::TUnboxedValue* const* values) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(
            compCtx.MutableValues[StateIndex_], compCtx.MutableValues[StateIndex_ + 1U], compCtx, values);
    }

    ui32 GetIndex() const final {
        return StateIndex_;
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependents) const final {
        CollectDependentIndexesImpl(this, owner, dependents, this->Dependents_);
    }
};

class TDecoratorComputationNodeBase {
protected:
    TDecoratorComputationNodeBase(IComputationNode* node, EValueRepresentation kind);
    ui32 GetIndexImpl() const;
    TString DebugStringImpl(const TString& typeName) const;

    IComputationNode* const Node_;
    const EValueRepresentation Kind_;
    TComputationExternalNodePtrSet Upvalues_;
    mutable bool UpvaluesCollected_;
};

template <typename TDerived>
class TDecoratorComputationNode: public TRefCountedComputationNode<IComputationNode>,
                                 protected TDecoratorComputationNodeBase {
private:
    void InitNode(TComputationContext&) const final {
    }

    const IComputationNode* GetSource() const final {
        return Node_;
    }

    IComputationNode* AddDependent(const IComputationNode* node) final {
        return Node_->AddDependent(node);
    }

    void AddDependency(const IComputationNode*) const final {
    }

    void AddOwned(IComputationExternalNode*) const final {
    }

    EValueRepresentation GetRepresentation() const final {
        return Kind_;
    }
    bool IsTemporaryValue() const final {
        return true;
    }

    void PrepareStageOne() final {
    }
    void PrepareStageTwo() final {
        CollectUpvalues(this->Upvalues_);
    }

    void RegisterDependencies() const final {
        Node_->AddDependent(this);
    }

    void CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const final {
    }

    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final {
        // XXX: If upvalues for the node are already collected,
        // just enrich the set, given by the caller;..
        if (this->UpvaluesCollected_) {
            upvalues.insert(this->Upvalues_.cbegin(), this->Upvalues_.cend());
            return;
        }
        // ... otherwise, recursively collect the upvalues.
        Node_->CollectUpvalues(upvalues);
        this->UpvaluesCollected_ = true;
    }

    ui32 GetDependentWeight() const final {
        return 0U;
    }

    ui32 GetDependentsCount() const final {
        return Node_->GetDependentsCount();
    }

    TComputationExternalNodePtrSet GetUpvalues() const final {
        MKQL_ENSURE(this->UpvaluesCollected_, "Upvalues have not been collected yet");
        return this->Upvalues_;
    }

    ui32 GetIndex() const final {
        return GetIndexImpl();
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(compCtx, Node_->GetValue(compCtx));
    }

protected:
    TDecoratorComputationNode(IComputationNode* node, EValueRepresentation kind)
        : TDecoratorComputationNodeBase(node, kind)
    {
    }

    explicit TDecoratorComputationNode(IComputationNode* node)
        : TDecoratorComputationNodeBase(node, node->GetRepresentation())
    {
    }

    TString DebugString() const override {
        return DebugStringImpl(TypeName<TDerived>());
    }
};

class TBinaryComputationNodeBase {
protected:
    TBinaryComputationNodeBase(IComputationNode* left, IComputationNode* right, EValueRepresentation kind);
    ui32 GetIndexImpl() const;
    TString DebugStringImpl(const TString& typeName) const;

    IComputationNode* const Left_;
    IComputationNode* const Right_;
    const EValueRepresentation Kind_;
    TComputationExternalNodePtrSet Upvalues_;
    mutable bool UpvaluesCollected_;
};

template <typename TDerived>
class TBinaryComputationNode: public TRefCountedComputationNode<IComputationNode>,
                              protected TBinaryComputationNodeBase {
private:
    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const final {
        return static_cast<const TDerived*>(this)->DoCalculate(ctx);
    }

    const IComputationNode* GetSource() const final {
        return GetCommonSource(Left_, Right_, this);
    }

    void InitNode(TComputationContext&) const final {
    }

protected:
    TString DebugString() const override {
        return DebugStringImpl(TypeName<TDerived>());
    }

    IComputationNode* AddDependent(const IComputationNode* node) final {
        const auto l = Left_->AddDependent(node);
        const auto r = Right_->AddDependent(node);

        if (!l) {
            return r;
        }
        if (!r) {
            return l;
        }

        return this;
    }

    void AddDependency(const IComputationNode*) const final {
    }

    void AddOwned(IComputationExternalNode*) const final {
    }

    EValueRepresentation GetRepresentation() const final {
        return Kind_;
    }

    bool IsTemporaryValue() const final {
        return true;
    }

    void PrepareStageOne() final {
    }
    void PrepareStageTwo() final {
        CollectUpvalues(this->Upvalues_);
    }

    void RegisterDependencies() const final {
        Left_->AddDependent(this);
        Right_->AddDependent(this);
    }

    void CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const final {
    }

    void CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const final {
        // XXX: If upvalues for the node are already collected,
        // just enrich the set, given by the caller;..
        if (this->UpvaluesCollected_) {
            upvalues.insert(this->Upvalues_.cbegin(), this->Upvalues_.cend());
            return;
        }
        // ... otherwise, recursively collect the upvalues.
        Left_->CollectUpvalues(upvalues);
        Right_->CollectUpvalues(upvalues);
        this->UpvaluesCollected_ = true;
    }

    ui32 GetDependentWeight() const final {
        return 0U;
    }

    ui32 GetDependentsCount() const final {
        return Left_->GetDependentsCount() + Right_->GetDependentsCount();
    }

    TComputationExternalNodePtrSet GetUpvalues() const final {
        MKQL_ENSURE(this->UpvaluesCollected_, "Upvalues have not been collected yet");
        return this->Upvalues_;
    }

    ui32 GetIndex() const final {
        return GetIndexImpl();
    }

    TBinaryComputationNode(IComputationNode* left, IComputationNode* right, const EValueRepresentation kind)
        : TBinaryComputationNodeBase(left, right, kind)
    {
    }
};

[[noreturn]]
void ThrowNotSupportedImplForClass(const TString& className, const char* func);

class TComputationValueBaseNotSupportedStub: public NYql::NUdf::IBoxedValue {
private:
    using TBase = NYql::NUdf::IBoxedValue;

public:
    template <typename... Args>
    explicit TComputationValueBaseNotSupportedStub(Args&&... args)
        : TBase(std::forward<Args>(args)...)
    {
    }

    ~TComputationValueBaseNotSupportedStub() override {
    }

private:
    bool HasFastListLength() const override;
    ui64 GetListLength() const override;
    ui64 GetEstimatedListLength() const override;
    bool HasListItems() const override;
    const NUdf::TOpaqueListRepresentation* GetListRepresentation() const override;
    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override;
    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override;
    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override;
    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override;
    ui64 GetDictLength() const override;
    bool HasDictItems() const override;
    NUdf::TStringRef GetResourceTag() const override;
    void* GetResource() override;
    void Apply(NUdf::IApplyContext& applyCtx) const override;
    NUdf::TUnboxedValue GetListIterator() const override;
    NUdf::TUnboxedValue GetDictIterator() const override;
    NUdf::TUnboxedValue GetKeysIterator() const override;
    NUdf::TUnboxedValue GetPayloadsIterator() const override;
    bool Contains(const NUdf::TUnboxedValuePod& key) const override;
    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override;
    NUdf::TUnboxedValue GetElement(ui32 index) const override;
    const NUdf::TUnboxedValue* GetElements() const override;
    NUdf::TUnboxedValue Run(
        const NUdf::IValueBuilder* valueBuilder,
        const NUdf::TUnboxedValuePod* args) const override;
    bool Skip() override;
    bool Next(NUdf::TUnboxedValue&) override;
    bool NextPair(NUdf::TUnboxedValue&, NUdf::TUnboxedValue&) override;
    ui32 GetVariantIndex() const override;
    NUdf::TUnboxedValue GetVariantItem() const override;
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override;
    ui32 GetTraverseCount() const override;
    NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override;
    NUdf::TUnboxedValue Save() const override;
    void Load(const NUdf::TStringRef& state) override;
    bool Load2(const NUdf::TUnboxedValue& state) override;
    void Push(const NUdf::TUnboxedValuePod& value) override;
    bool IsSortedDict() const override;
    void Unused1() override;
    void Unused2() override;
    void Unused3() override;
    void Unused4() override;
    void Unused5() override;
    void Unused6() override;
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) override;

protected:
    [[noreturn]] virtual void ThrowNotSupported(const char* func) const = 0;
};

template <typename TDerived>
class TComputationValueBase: public TComputationValueBaseNotSupportedStub {
private:
    using TBase = TComputationValueBaseNotSupportedStub;

public:
    template <typename... Args>
    explicit TComputationValueBase(Args&&... args)
        : TBase(std::forward<Args>(args)...)
    {
    }

    ~TComputationValueBase() override {
    }

    TString DebugString() const {
        return TypeName<TDerived>();
    }

protected:
    [[noreturn]] void ThrowNotSupported(const char* func) const final {
        ThrowNotSupportedImplForClass(TypeName(*this), func);
    }
};

template <typename TDerived, EMemorySubPool MemoryPool>
class TComputationValueImpl: public TComputationValueBase<TDerived>,
                             public TWithMiniKQLAlloc<MemoryPool> {
private:
    using TBase = TComputationValueBase<TDerived>;

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
    explicit TComputationValueImpl(TMemoryUsageInfo* memInfo, Args&&... args)
        : TBase(std::forward<Args>(args)...)
    {
#ifndef NDEBUG
        M_.MemInfo = memInfo;
        MKQL_MEM_TAKE(memInfo, this, sizeof(TDerived), __MKQL_LOCATION__);
#else
        Y_UNUSED(memInfo);
#endif
    }

    ~TComputationValueImpl() override {
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

template <typename TDerived>
class TTemporaryComputationValue: public TComputationValueImpl<TDerived, EMemorySubPool::Temporary> {
private:
    using TBase = TComputationValueImpl<TDerived, EMemorySubPool::Temporary>;

public:
    using TBase::TBase;
};

template <typename TDerived>
class TComputationValue: public TComputationValueImpl<TDerived, EMemorySubPool::Default> {
private:
    using TBase = TComputationValueImpl<TDerived, EMemorySubPool::Default>;

public:
    using TBase::TBase;
};

template <bool IsStream>
struct TThresher;

template <>
struct TThresher<true> {
    template <class Handler>
    static void DoForEachItem(const NUdf::TUnboxedValuePod& stream, const Handler& handler) {
        for (NUdf::TUnboxedValue item;; handler(std::move(item))) {
            const auto status = stream.Fetch(item);
            MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Unexpected stream status!");
            if (status == NUdf::EFetchStatus::Finish) {
                break;
            }
        }
    }
};

template <>
struct TThresher<false> {
    template <class Handler>
    static void DoForEachItem(const NUdf::TUnboxedValuePod& list, const Handler& handler) {
        if (auto ptr = list.GetElements()) {
            if (auto size = list.GetListLength()) {
                do {
                    handler(NUdf::TUnboxedValue(*ptr++));
                } while (--size);
            }
        } else if (const auto& iter = list.GetListIterator()) {
            for (NUdf::TUnboxedValue item; iter.Next(item); handler(std::move(item))) {
                continue;
            }
        }
    }
};

IComputationNode* LocateNode(const TNodeLocator& nodeLocator, TCallable& callable, ui32 index, bool pop = false);
IComputationNode* LocateNode(const TNodeLocator& nodeLocator, TNode& node, bool pop = false);
IComputationExternalNode* LocateExternalNode(const TNodeLocator& nodeLocator, TCallable& callable, ui32 index, bool pop = true);

using TPasstroughtMap = std::vector<std::optional<size_t>>;
using TPassthroughSpan = std::vector<std::optional<size_t>>;

template <class TContainerOne, class TContainerTwo>
TPasstroughtMap GetPasstroughtMap(const TContainerOne& from, const TContainerTwo& to);

template <class TContainerOne, class TContainerTwo>
TPasstroughtMap GetPasstroughtMapOneToOne(const TContainerOne& from, const TContainerTwo& to);

std::optional<size_t> IsPasstrought(const IComputationNode* root, const TComputationExternalNodePtrVector& args);

TPasstroughtMap MergePasstroughtMaps(const TPasstroughtMap& lhs, const TPasstroughtMap& rhs);

void ApplyChanges(const NUdf::TUnboxedValue& value, NUdf::IApplyContext& applyCtx);
void CleanupCurrentContext();

template <typename T>
class TBoxedData: public NUdf::TBoxedValue {
public:
    template <typename... Args>
    explicit TBoxedData(Args&&... args)
        : Data_(std::forward<Args>(args)...)
    {
    }

    T& Get() {
        return Data_;
    }

    const T& Get() const {
        return Data_;
    }

private:
    T Data_;
};

template <typename T>
class TMutableDataOnContext: private TNonCopyable {
public:
    explicit TMutableDataOnContext(TComputationMutables& mutables)
        : Index_(mutables.CurValueIndex++)
    {
    }

    bool Empty(TComputationContext& ctx) const {
        return ctx.MutableValues[Index_].IsInvalid();
    }

    T& Get(TComputationContext& ctx) const {
        MKQL_ENSURE(!Empty(ctx), "Value not created");
        auto& val = ctx.MutableValues[Index_];
        return static_cast<TBoxedData<T>*>(val.AsBoxed().Get())->Get();
    }

    template <typename... Args>
    T& GetOrCreate(TComputationContext& ctx, Args&&... args) const {
        auto& val = ctx.MutableValues[Index_];
        if (val.IsInvalid()) {
            val = NUdf::TUnboxedValuePod(new TBoxedData<T>(std::forward<Args>(args)...));
        }
        return static_cast<TBoxedData<T>*>(val.AsBoxed().Get())->Get();
    }

private:
    const ui32 Index_;
};

} // namespace NKikimr::NMiniKQL
