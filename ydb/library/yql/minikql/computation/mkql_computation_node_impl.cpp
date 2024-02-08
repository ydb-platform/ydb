#include "mkql_computation_node_impl.h"

#include "ydb/library/yql/minikql/mkql_string_util.h"

namespace NKikimr {
namespace NMiniKQL {

template <class IComputationNodeInterface>
void TRefCountedComputationNode<IComputationNodeInterface>::Ref() {
    ++Refs_;
}

template <class IComputationNodeInterface>
void TRefCountedComputationNode<IComputationNodeInterface>::UnRef() {
    Y_ABORT_UNLESS(Refs_ > 0);
    if (--Refs_ == 0) {
        delete this;
    }
}

template <class IComputationNodeInterface>
ui32 TRefCountedComputationNode<IComputationNodeInterface>::RefCount() const {
    return Refs_;
}

template class TRefCountedComputationNode<IComputationWideFlowNode>;
template class TRefCountedComputationNode<IComputationWideFlowProxyNode>;

TUnboxedImmutableComputationNode::TUnboxedImmutableComputationNode(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value)
    : MemInfo(memInfo)
    , UnboxedValue(std::move(value))
    , RepresentationKind(UnboxedValue.HasValue() ? (UnboxedValue.IsBoxed() ? EValueRepresentation::Boxed : (UnboxedValue.IsString() ? EValueRepresentation::String : EValueRepresentation::Embedded)) : EValueRepresentation::Embedded)
{
    MKQL_MEM_TAKE(MemInfo, this, sizeof(*this), __MKQL_LOCATION__);
    TlsAllocState->LockObject(UnboxedValue);
}

TUnboxedImmutableComputationNode::~TUnboxedImmutableComputationNode() {
    MKQL_MEM_RETURN(MemInfo, this, sizeof(*this));
    TlsAllocState->UnlockObject(UnboxedValue);
}

NUdf::TUnboxedValue TUnboxedImmutableComputationNode::GetValue(TComputationContext& compCtx) const {
    Y_UNUSED(compCtx);
    if (!TlsAllocState->UseRefLocking && RepresentationKind == EValueRepresentation::String) {
        /// TODO: YQL-4461
        return MakeString(UnboxedValue.AsStringRef());
    }
    return UnboxedValue;
}

const IComputationNode* TUnboxedImmutableComputationNode::GetSource() const { return nullptr; }

IComputationNode* TUnboxedImmutableComputationNode::AddDependence(const IComputationNode*) { return nullptr; }

void TUnboxedImmutableComputationNode::RegisterDependencies() const {}

ui32 TUnboxedImmutableComputationNode::GetIndex() const {
    THROW yexception() << "Failed to get index.";
}

void TUnboxedImmutableComputationNode::CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const {
    THROW yexception() << "Failed to collect dependent indexes.";
}

ui32 TUnboxedImmutableComputationNode::GetDependencyWeight() const {
    THROW yexception() << "Can't get dependency weight from const node.";
}

ui32 TUnboxedImmutableComputationNode::GetDependencesCount() const {
    THROW yexception() << "Can't get dependences count from const node.";
}

bool TUnboxedImmutableComputationNode::IsTemporaryValue() const { return false; }

void TUnboxedImmutableComputationNode::PrepareStageOne() {}
void TUnboxedImmutableComputationNode::PrepareStageTwo() {}

TString TUnboxedImmutableComputationNode::DebugString() const {
    return UnboxedValue ? (UnboxedValue.IsBoxed() ? "Boxed" : "Literal") : "Empty";
}

EValueRepresentation TUnboxedImmutableComputationNode::GetRepresentation() const {
    return RepresentationKind;
}

template <class IComputationNodeInterface, bool SerializableState>
TStatefulComputationNode<IComputationNodeInterface, SerializableState>::TStatefulComputationNode(TComputationMutables& mutables, EValueRepresentation kind)
    : ValueIndex(mutables.CurValueIndex++), RepresentationKind(kind)
{
    if constexpr (SerializableState) {
        mutables.SerializableValues.push_back(ValueIndex);
    }
}

template <class IComputationNodeInterface, bool SerializableState>
IComputationNode* TStatefulComputationNode<IComputationNodeInterface, SerializableState>::AddDependence(const IComputationNode* node) {
    Dependencies.emplace_back(node);
    return this;
}

template <class IComputationNodeInterface, bool SerializableState>
EValueRepresentation TStatefulComputationNode<IComputationNodeInterface, SerializableState>::GetRepresentation() const {
    return RepresentationKind;
}

template <class IComputationNodeInterface, bool SerializableState>
ui32 TStatefulComputationNode<IComputationNodeInterface, SerializableState>::GetIndex() const { return ValueIndex; }

template <class IComputationNodeInterface, bool SerializableState>
ui32 TStatefulComputationNode<IComputationNodeInterface, SerializableState>::GetDependencesCount() const { return Dependencies.size(); }

template class TStatefulComputationNode<IComputationNode, false>;
template class TStatefulComputationNode<IComputationWideFlowNode, false>;
template class TStatefulComputationNode<IComputationExternalNode, false>;
template class TStatefulComputationNode<IComputationNode, true>;
template class TStatefulComputationNode<IComputationWideFlowNode, true>;
template class TStatefulComputationNode<IComputationExternalNode, true>;

void TExternalComputationNode::CollectDependentIndexes(const IComputationNode*, TIndexesMap& map) const {
    map.emplace(ValueIndex, RepresentationKind);
}

TExternalComputationNode::TExternalComputationNode(TComputationMutables& mutables, EValueRepresentation kind)
    : TStatefulComputationNode(mutables, kind)
{}

NUdf::TUnboxedValue TExternalComputationNode::GetValue(TComputationContext& ctx) const {
    return Getter ? Getter(ctx) : ValueRef(ctx);
}

NUdf::TUnboxedValue& TExternalComputationNode::RefValue(TComputationContext& ctx) const {
    InvalidateValue(ctx);
    return ValueRef(ctx);
}

void TExternalComputationNode::SetValue(TComputationContext& ctx, NUdf::TUnboxedValue&& value) const {
    ValueRef(ctx) = std::move(value);
    InvalidateValue(ctx);
}

TString TExternalComputationNode::DebugString() const {
    return "External";
}

void TExternalComputationNode::RegisterDependencies() const {}

void TExternalComputationNode::SetOwner(const IComputationNode* owner) {
    Y_DEBUG_ABORT_UNLESS(!Owner);
    Owner = owner;
}

void TExternalComputationNode::PrepareStageOne() {
    std::sort(Dependencies.begin(), Dependencies.end());
    Dependencies.erase(std::unique(Dependencies.begin(), Dependencies.end()), Dependencies.cend());
    if (const auto it = std::find(Dependencies.cbegin(), Dependencies.cend(), Owner); Dependencies.cend() != it)
        Dependencies.erase(it);
}

void TExternalComputationNode::PrepareStageTwo() {
    TIndexesMap dependencies;
    std::for_each(Dependencies.cbegin(), Dependencies.cend(),
        std::bind(&IComputationNode::CollectDependentIndexes, std::placeholders::_1, Owner, std::ref(dependencies)));
    InvalidationSet.assign(dependencies.cbegin(), dependencies.cend());
}

const IComputationNode* TExternalComputationNode::GetSource() const { return nullptr; }

ui32 TExternalComputationNode::GetDependencyWeight() const { return 0U; }

bool TExternalComputationNode::IsTemporaryValue() const {
    return bool(Getter);
}

void TExternalComputationNode::SetGetter(TGetter&& getter) {
    Getter = std::move(getter);
}

void TExternalComputationNode::InvalidateValue(TComputationContext& ctx) const {
    for (const auto& index : InvalidationSet) {
        ctx.MutableValues[index.first] = NUdf::TUnboxedValuePod::Invalid();
    }
}

TString TWideFlowProxyComputationNode::DebugString() const { return "WideFlowArg"; }

EValueRepresentation TWideFlowProxyComputationNode::GetRepresentation() const {
    THROW yexception() << "Failed to get representation kind.";
}

NUdf::TUnboxedValue TWideFlowProxyComputationNode::GetValue(TComputationContext&) const {
    THROW yexception() << "Failed to get value from wide flow node.";
}

ui32 TWideFlowProxyComputationNode::GetIndex() const {
    THROW yexception() << "Failed to get proxy node index.";
}

ui32 TWideFlowProxyComputationNode::GetDependencyWeight() const {
    THROW yexception() << "Failed to get dependency weight.";
}

ui32 TWideFlowProxyComputationNode::GetDependencesCount() const {
    return Dependence ? 1U : 0U;
}

IComputationNode* TWideFlowProxyComputationNode::AddDependence(const IComputationNode* node) {
    Y_DEBUG_ABORT_UNLESS(!Dependence);
    Dependence = node;
    return this;
}

const IComputationNode* TWideFlowProxyComputationNode::GetSource() const { return nullptr; }

bool TWideFlowProxyComputationNode::IsTemporaryValue() const { return true; }

void TWideFlowProxyComputationNode::RegisterDependencies() const {}

void TWideFlowProxyComputationNode::PrepareStageOne() {}

void TWideFlowProxyComputationNode::PrepareStageTwo() {
    if (Dependence) {
        TIndexesMap dependencies;
        Dependence->CollectDependentIndexes(Owner, dependencies);
        InvalidationSet.assign(dependencies.cbegin(), dependencies.cend());
    }
}

void TWideFlowProxyComputationNode::SetOwner(const IComputationNode* owner) {
    Y_DEBUG_ABORT_UNLESS(!Owner);
    Owner = owner;
}

void TWideFlowProxyComputationNode::CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const {
    THROW yexception() << "Failed to collect dependent indexes.";
}

void TWideFlowProxyComputationNode::InvalidateValue(TComputationContext& ctx) const {
    for (const auto& index : InvalidationSet) {
        ctx.MutableValues[index.first] = NUdf::TUnboxedValuePod::Invalid();
    }
}

void TWideFlowProxyComputationNode::SetFetcher(TFetcher&& fetcher) {
    Fetcher = std::move(fetcher);
}

EFetchResult TWideFlowProxyComputationNode::FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* values) const {
    return Fetcher(ctx, values);
}

IComputationNode* LocateNode(const TNodeLocator& nodeLocator, TCallable& callable, ui32 index, bool pop) {
    return nodeLocator(callable.GetInput(index).GetNode(), pop);
}

IComputationNode* LocateNode(const TNodeLocator& nodeLocator, TNode& node, bool pop) {
    return nodeLocator(&node, pop);
}

IComputationExternalNode* LocateExternalNode(const TNodeLocator& nodeLocator, TCallable& callable, ui32 index, bool pop) {
    return dynamic_cast<IComputationExternalNode*>(LocateNode(nodeLocator, callable, index, pop));
}

template<class TContainerOne, class TContainerTwo>
TPasstroughtMap GetPasstroughtMap(const TContainerOne& from, const TContainerTwo& to) {
    TPasstroughtMap map(from.size());
    for (size_t i = 0U; i < map.size(); ++i) {
        for (size_t j = 0U; j < to.size(); ++j) {
            if (from[i] == to[j]) {
                map[i].emplace(j);
                break;
            }
        }
    }
    return map;
}

template<class TContainerOne, class TContainerTwo>
TPasstroughtMap GetPasstroughtMapOneToOne(const TContainerOne& from, const TContainerTwo& to) {
    TPasstroughtMap map(from.size());
    std::unordered_map<typename TContainerOne::value_type, size_t> unique(map.size());
    for (size_t i = 0U; i < map.size(); ++i) {
        if (const auto ins = unique.emplace(from[i], i); ins.second) {
            for (size_t j = 0U; j < to.size(); ++j) {
                if (from[i] == to[j]) {
                    if (auto& item = map[i]) {
                        item.reset();
                        break;
                    } else
                        item.emplace(j);

                }
            }
        } else
            map[ins.first->second].reset();
    }
    return map;
}

template TPasstroughtMap GetPasstroughtMap(const TComputationExternalNodePtrVector& from, const TComputationNodePtrVector& to);
template TPasstroughtMap GetPasstroughtMap(const TComputationNodePtrVector& from, const TComputationExternalNodePtrVector& to);
template TPasstroughtMap GetPasstroughtMapOneToOne(const TComputationExternalNodePtrVector& from, const TComputationNodePtrVector& to);
template TPasstroughtMap GetPasstroughtMapOneToOne(const TComputationNodePtrVector& from, const TComputationExternalNodePtrVector& to);

std::optional<size_t> IsPasstrought(const IComputationNode* root, const TComputationExternalNodePtrVector& args) {
    for (size_t i = 0U; i < args.size(); ++i)
        if (root == args[i])
            return {i};
    return std::nullopt;
}

TPasstroughtMap MergePasstroughtMaps(const TPasstroughtMap& lhs, const TPasstroughtMap& rhs) {
    TPasstroughtMap map(std::min(lhs.size(), rhs.size()));
    auto i = 0U;
    for (auto& item : map) {
        if (const auto l = lhs[i], r = rhs[i]; l && r && *l == *r) {
            item.emplace(*l);
        }
        ++i;
    }
    return map;
}

void ApplyChanges(const NUdf::TUnboxedValue& list, NUdf::IApplyContext& applyCtx) {
    TThresher<false>::DoForEachItem(list,
        [&applyCtx] (const NUdf::TUnboxedValue& item) {
            if (item.IsBoxed())
                item.Apply(applyCtx);
        }
    );
}

const IComputationNode* GetCommonSource(const IComputationNode* first, const IComputationNode* second, const IComputationNode* common) {
    const auto f = first->GetSource();
    const auto s = second->GetSource();
    if (f && s) {
        return f == s ? f : common;
    } else if (f) {
        return f;
    } else if (s) {
        return s;
    }
    return common;
}

void CleanupCurrentContext() {
    auto& allocState = *TlsAllocState;
    if (allocState.CurrentContext) {
        TAllocState::CleanupPAllocList(allocState.CurrentPAllocList);
    }
}

}
}
