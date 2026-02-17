#include "mkql_computation_node_impl.h"

#include "yql/essentials/minikql/mkql_string_util.h"

namespace NKikimr::NMiniKQL {

void ThrowNotSupportedImplForClass(const TString& className, const char* func) {
    THROW yexception() << "Unsupported access to '" << func << "' method of: " << className;
}

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
    : MemInfo_(memInfo)
    , UnboxedValue_(std::move(value))
    , RepresentationKind_(UnboxedValue_.HasValue()
                              ? (UnboxedValue_.IsBoxed()
                                     ? EValueRepresentation::Boxed
                                     : (UnboxedValue_.IsString()
                                            ? EValueRepresentation::String
                                            : EValueRepresentation::Embedded))
                              : EValueRepresentation::Embedded)
{
    MKQL_MEM_TAKE(MemInfo_, this, sizeof(*this), __MKQL_LOCATION__);
    TlsAllocState->LockObject(UnboxedValue_);
}

TUnboxedImmutableComputationNode::~TUnboxedImmutableComputationNode() {
    TlsAllocState->UnlockObject(UnboxedValue_);
    MKQL_MEM_RETURN(MemInfo_, this, sizeof(*this));
}

NUdf::TUnboxedValue TUnboxedImmutableComputationNode::GetValue(TComputationContext& compCtx) const {
    Y_UNUSED(compCtx);
    if (!TlsAllocState->UseRefLocking && RepresentationKind_ == EValueRepresentation::String) {
        /// TODO: YQL-4461
        return MakeString(UnboxedValue_.AsStringRef());
    }
    return UnboxedValue_;
}

const IComputationNode* TUnboxedImmutableComputationNode::GetSource() const {
    return nullptr;
}

IComputationNode* TUnboxedImmutableComputationNode::AddDependent(const IComputationNode*) {
    return nullptr;
}

void TUnboxedImmutableComputationNode::AddDependency(const IComputationNode*) const {
}

void TUnboxedImmutableComputationNode::AddOwned(IComputationExternalNode*) const {
}

void TUnboxedImmutableComputationNode::RegisterDependencies() const {
}

ui32 TUnboxedImmutableComputationNode::GetIndex() const {
    THROW yexception() << "Failed to get index.";
}

void TUnboxedImmutableComputationNode::CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const {
    THROW yexception() << "Failed to collect dependent indexes.";
}

ui32 TUnboxedImmutableComputationNode::GetDependentWeight() const {
    THROW yexception() << "Can't get dependent weight from const node.";
}

void TUnboxedImmutableComputationNode::CollectUpvalues(TComputationExternalNodePtrSet&) const {
}

ui32 TUnboxedImmutableComputationNode::GetDependentsCount() const {
    THROW yexception() << "Can't get dependents count from const node.";
}

TComputationExternalNodePtrSet TUnboxedImmutableComputationNode::GetUpvalues() const {
    return {};
}
bool TUnboxedImmutableComputationNode::IsTemporaryValue() const {
    return false;
}

void TUnboxedImmutableComputationNode::PrepareStageOne() {
}
void TUnboxedImmutableComputationNode::PrepareStageTwo() {
}

TString TUnboxedImmutableComputationNode::DebugString() const {
    return UnboxedValue_ ? (UnboxedValue_.IsBoxed() ? "Boxed" : "Literal") : "Empty";
}

EValueRepresentation TUnboxedImmutableComputationNode::GetRepresentation() const {
    return RepresentationKind_;
}

Y_NO_INLINE TStatefulComputationNodeBase::TStatefulComputationNodeBase(ui32 valueIndex, EValueRepresentation kind)
    : UpvaluesCollected_(false)
    , ValueIndex_(valueIndex)
    , RepresentationKind_(kind)
{
}

Y_NO_INLINE TStatefulComputationNodeBase::~TStatefulComputationNodeBase()
{
}

Y_NO_INLINE void TStatefulComputationNodeBase::AddDependentImpl(const IComputationNode* node) {
    Dependents_.emplace_back(node);
}

Y_NO_INLINE void TStatefulComputationNodeBase::AddDependencyImpl(const IComputationNode* node) const {
    Dependencies_.emplace_back(node);
}

Y_NO_INLINE void TStatefulComputationNodeBase::AddOwnedImpl(IComputationExternalNode* node) const {
    Owned_.emplace(node);
}

Y_NO_INLINE void TStatefulComputationNodeBase::CollectDependentIndexesImpl(
    const IComputationNode* self, const IComputationNode* owner,
    IComputationNode::TIndexesMap& dependents, bool stateless) const {
    if (self == owner) {
        return;
    }

    if (const auto ins = dependents.emplace(ValueIndex_, RepresentationKind_); ins.second) {
        std::for_each(Dependents_.cbegin(), Dependents_.cend(),
                      std::bind(&IComputationNode::CollectDependentIndexes, std::placeholders::_1, owner, std::ref(dependents)));

        if (stateless) {
            dependents.erase(ins.first);
        }
    }
}

Y_NO_INLINE void TStatefulComputationNodeBase::CollectUpvaluesImpl(TComputationExternalNodePtrSet& upvalues) const {
    // XXX: If upvalues for the node are already collected, just
    // enrich the set, given by the caller;..
    if (this->UpvaluesCollected_) {
        upvalues.insert(this->Upvalues_.cbegin(), this->Upvalues_.cend());
        return;
    }
    // ... otherwise, recursively collect the upvalue candidates...
    std::for_each(Dependencies_.cbegin(), Dependencies_.cend(), std::bind(&IComputationNode::CollectUpvalues, std::placeholders::_1, std::ref(upvalues)));
    // ... and filter out the owned nodes.
    std::erase_if(upvalues, [this](IComputationExternalNode* uv) { return Owned_.find(uv) != Owned_.cend(); });
    this->UpvaluesCollected_ = true;
}

Y_NO_INLINE TStatefulSourceComputationNodeBase::TStatefulSourceComputationNodeBase()
{
}

Y_NO_INLINE TStatefulSourceComputationNodeBase::~TStatefulSourceComputationNodeBase()
{
}

Y_NO_INLINE void TStatefulSourceComputationNodeBase::PrepareStageOneImpl(const TConstComputationNodePtrVector& dependents) {
    if (!Stateless_) {
        Stateless_ = std::accumulate(
                         dependents.cbegin(), dependents.cend(), 0,
                         std::bind(std::plus<i32>(), std::placeholders::_1,
                                   std::bind(&IComputationNode::GetDependentWeight, std::placeholders::_2))) <= 1;
    }
}

Y_NO_INLINE void TStatefulSourceComputationNodeBase::AddSource(IComputationNode* source) const {
    Sources_.emplace(source);
}

template <class IComputationNodeInterface, bool SerializableState>
TStatefulComputationNode<IComputationNodeInterface, SerializableState>::TStatefulComputationNode(
    TComputationMutables& mutables, EValueRepresentation kind)
    : TStatefulComputationNodeBase(mutables.CurValueIndex++, kind)
{
    if constexpr (SerializableState) {
        mutables.SerializableValues.push_back(ValueIndex_);
    }
}

template <class IComputationNodeInterface, bool SerializableState>
IComputationNode* TStatefulComputationNode<IComputationNodeInterface, SerializableState>::AddDependent(
    const IComputationNode* node) {
    AddDependentImpl(node);
    return this;
}

template <class IComputationNodeInterface, bool SerializableState>
void TStatefulComputationNode<IComputationNodeInterface, SerializableState>::AddDependency(const IComputationNode* node) const {
    AddDependencyImpl(node);
}

template <class IComputationNodeInterface, bool SerializableState>
void TStatefulComputationNode<IComputationNodeInterface, SerializableState>::AddOwned(IComputationExternalNode* node) const {
    AddOwnedImpl(node);
}

template <class IComputationNodeInterface, bool SerializableState>
EValueRepresentation TStatefulComputationNode<IComputationNodeInterface, SerializableState>::GetRepresentation() const {
    return RepresentationKind_;
}

template <class IComputationNodeInterface, bool SerializableState>
void TStatefulComputationNode<IComputationNodeInterface, SerializableState>::InitNode(TComputationContext&) const {
}

template <class IComputationNodeInterface, bool SerializableState>
ui32 TStatefulComputationNode<IComputationNodeInterface, SerializableState>::GetIndex() const {
    return ValueIndex_;
}

template <class IComputationNodeInterface, bool SerializableState>
ui32 TStatefulComputationNode<IComputationNodeInterface, SerializableState>::GetDependentsCount() const {
    return Dependents_.size();
}

template class TStatefulComputationNode<IComputationNode, false>;
template class TStatefulComputationNode<IComputationWideFlowNode, false>;
template class TStatefulComputationNode<IComputationExternalNode, false>;
template class TStatefulComputationNode<IComputationNode, true>;
template class TStatefulComputationNode<IComputationWideFlowNode, true>;
template class TStatefulComputationNode<IComputationExternalNode, true>;

Y_NO_INLINE ui32 TStatelessFlowComputationNodeBase::GetIndexImpl() const {
    THROW yexception() << "Failed to get stateless node index.";
}

Y_NO_INLINE void TStatelessFlowComputationNodeBase::CollectDependentIndexesImpl(
    const IComputationNode* self,
    const IComputationNode* owner,
    IComputationNode::TIndexesMap& dependents,
    const TConstComputationNodePtrVector& dependences) const {
    if (self == owner) {
        return;
    }

    for (auto& dependence : dependences) {
        dependence->CollectDependentIndexes(owner, dependents);
    }
}

Y_NO_INLINE TStatefulFlowComputationNodeBase::TStatefulFlowComputationNodeBase(ui32 stateIndex, EValueRepresentation stateKind)
    : StateIndex_(stateIndex)
    , StateKind_(stateKind)
{
}

Y_NO_INLINE void TStatefulFlowComputationNodeBase::CollectDependentIndexesImpl(
    const IComputationNode* self,
    const IComputationNode* owner,
    IComputationNode::TIndexesMap& dependents,
    const TConstComputationNodePtrVector& dependences) const {
    if (self == owner) {
        return;
    }

    const auto ins = dependents.emplace(StateIndex_, StateKind_);
    if (ins.second) {
        for (auto& dependence : dependences) {
            dependence->CollectDependentIndexes(owner, dependents);
        }
    }
}

Y_NO_INLINE TPairStateFlowComputationNodeBase::TPairStateFlowComputationNodeBase(
    ui32 stateIndex, EValueRepresentation firstKind, EValueRepresentation secondKind)
    : StateIndex_(stateIndex)
    , FirstKind_(firstKind)
    , SecondKind_(secondKind)
{
}

Y_NO_INLINE void TPairStateFlowComputationNodeBase::CollectDependentIndexesImpl(
    const IComputationNode* self,
    const IComputationNode* owner,
    IComputationNode::TIndexesMap& dependents,
    const TConstComputationNodePtrVector& dependences) const {
    if (self == owner) {
        return;
    }

    const auto ins1 = dependents.emplace(StateIndex_, FirstKind_);
    const auto ins2 = dependents.emplace(StateIndex_ + 1U, SecondKind_);
    if (ins1.second && ins2.second) {
        for (auto& dependence : dependences) {
            dependence->CollectDependentIndexes(owner, dependents);
        }
    }
}

Y_NO_INLINE ui32 TStatelessWideFlowComputationNodeBase::GetIndexImpl() const {
    THROW yexception() << "Failed to get stateless node index.";
}

Y_NO_INLINE void TStatelessWideFlowComputationNodeBase::CollectDependentIndexesImpl(
    const IComputationNode* self,
    const IComputationNode* owner,
    IComputationNode::TIndexesMap& dependents,
    const TConstComputationNodePtrVector& dependences) const {
    if (self == owner) {
        return;
    }

    for (auto& dependence : dependences) {
        dependence->CollectDependentIndexes(owner, dependents);
    }
}

Y_NO_INLINE EValueRepresentation TWideFlowBaseComputationNodeBase::GetRepresentationImpl() const {
    THROW yexception() << "Failed to get representation kind.";
}

Y_NO_INLINE NUdf::TUnboxedValue TWideFlowBaseComputationNodeBase::GetValueImpl(TComputationContext&) const {
    THROW yexception() << "Failed to get value from wide flow node.";
}

Y_NO_INLINE TStatefulWideFlowComputationNodeBase::TStatefulWideFlowComputationNodeBase(
    ui32 stateIndex, EValueRepresentation stateKind)
    : StateIndex_(stateIndex)
    , StateKind_(stateKind)
{
}

Y_NO_INLINE void TStatefulWideFlowComputationNodeBase::CollectDependentIndexesImpl(
    const IComputationNode* self,
    const IComputationNode* owner,
    IComputationNode::TIndexesMap& dependents,
    const TConstComputationNodePtrVector& dependences) const {
    if (self == owner) {
        return;
    }

    const auto ins = dependents.emplace(StateIndex_, StateKind_);
    if (ins.second) {
        for (auto& dependence : dependences) {
            dependence->CollectDependentIndexes(owner, dependents);
        }
    }
}

Y_NO_INLINE TPairStateWideFlowComputationNodeBase::TPairStateWideFlowComputationNodeBase(
    ui32 stateIndex, EValueRepresentation firstKind, EValueRepresentation secondKind)
    : StateIndex_(stateIndex)
    , FirstKind_(firstKind)
    , SecondKind_(secondKind)
{
}

Y_NO_INLINE void TPairStateWideFlowComputationNodeBase::CollectDependentIndexesImpl(
    const IComputationNode* self, const IComputationNode* owner,
    IComputationNode::TIndexesMap& dependents, const TConstComputationNodePtrVector& dependences) const {
    if (self == owner) {
        return;
    }

    const auto ins1 = dependents.emplace(StateIndex_, FirstKind_);
    const auto ins2 = dependents.emplace(StateIndex_ + 1U, SecondKind_);
    if (ins1.second && ins2.second) {
        for (auto& dependence : dependences) {
            dependence->CollectDependentIndexes(owner, dependents);
        }
    }
}

Y_NO_INLINE TDecoratorComputationNodeBase::TDecoratorComputationNodeBase(IComputationNode* node, EValueRepresentation kind)
    : Node_(node)
    , Kind_(kind)
    , UpvaluesCollected_(false)
{
}

Y_NO_INLINE ui32 TDecoratorComputationNodeBase::GetIndexImpl() const {
    THROW yexception() << "Can't get index from decorator node.";
}

Y_NO_INLINE TString TDecoratorComputationNodeBase::DebugStringImpl(const TString& typeName) const {
    return typeName + "(" + Node_->DebugString() + ")";
}

Y_NO_INLINE TBinaryComputationNodeBase::TBinaryComputationNodeBase(
    IComputationNode* left, IComputationNode* right, EValueRepresentation kind)
    : Left_(left)
    , Right_(right)
    , Kind_(kind)
    , UpvaluesCollected_(false)
{
}

Y_NO_INLINE ui32 TBinaryComputationNodeBase::GetIndexImpl() const {
    THROW yexception() << "Can't get index from decorator node.";
}

Y_NO_INLINE TString TBinaryComputationNodeBase::DebugStringImpl(const TString& typeName) const {
    return typeName + "(" + Left_->DebugString() + "," + Right_->DebugString() + ")";
}

void TExternalComputationNode::CollectDependentIndexes(const IComputationNode*, TIndexesMap& map) const {
    map.emplace(ValueIndex_, RepresentationKind_);
}

void TExternalComputationNode::CollectUpvalues(TComputationExternalNodePtrSet& upvalues) const {
    upvalues.emplace(const_cast<TExternalComputationNode*>(this));
}

TExternalComputationNode::TExternalComputationNode(TComputationMutables& mutables, EValueRepresentation kind)
    : TStatefulComputationNode(mutables, kind)
{
    mutables.CachedValues.push_back(ValueIndex_);
}

NUdf::TUnboxedValue TExternalComputationNode::GetValue(TComputationContext& ctx) const {
    return Getter_ ? Getter_(ctx) : ValueRef(ctx);
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

void TExternalComputationNode::RegisterDependencies() const {
}

void TExternalComputationNode::SetOwner(const IComputationNode* owner) {
    Y_DEBUG_ABORT_UNLESS(!Owner_);
    Owner_ = owner;
}

void TExternalComputationNode::PrepareStageOne() {
    std::sort(Dependents_.begin(), Dependents_.end());
    Dependents_.erase(std::unique(Dependents_.begin(), Dependents_.end()), Dependents_.cend());
    if (const auto it = std::find(Dependents_.cbegin(), Dependents_.cend(), Owner_); Dependents_.cend() != it) {
        Dependents_.erase(it);
    }
}

void TExternalComputationNode::PrepareStageTwo() {
    TIndexesMap dependents;
    std::for_each(Dependents_.cbegin(), Dependents_.cend(),
                  std::bind(&IComputationNode::CollectDependentIndexes, std::placeholders::_1, Owner_, std::ref(dependents)));
    InvalidationSet_.assign(dependents.cbegin(), dependents.cend());
}

const IComputationNode* TExternalComputationNode::GetSource() const {
    return nullptr;
}

ui32 TExternalComputationNode::GetDependentWeight() const {
    return 0U;
}

TComputationExternalNodePtrSet TExternalComputationNode::GetUpvalues() const {
    return {};
}

bool TExternalComputationNode::IsTemporaryValue() const {
    return bool(Getter_);
}

void TExternalComputationNode::SetGetter(TGetter&& getter) {
    Getter_ = std::move(getter);
}

void TExternalComputationNode::InvalidateValue(TComputationContext& ctx) const {
    for (const auto& index : InvalidationSet_) {
        ctx.MutableValues[index.first] = NUdf::TUnboxedValuePod::Invalid();
    }
}

bool TComputationValueBaseNotSupportedStub::HasFastListLength() const {
    ThrowNotSupported(__func__);
    return false;
}

ui64 TComputationValueBaseNotSupportedStub::GetListLength() const {
    ThrowNotSupported(__func__);
    return 0;
}

ui64 TComputationValueBaseNotSupportedStub::GetEstimatedListLength() const {
    ThrowNotSupported(__func__);
    return 0;
}

bool TComputationValueBaseNotSupportedStub::HasListItems() const {
    ThrowNotSupported(__func__);
    return false;
}

const NUdf::TOpaqueListRepresentation* TComputationValueBaseNotSupportedStub::GetListRepresentation() const {
    return nullptr;
}

NUdf::IBoxedValuePtr TComputationValueBaseNotSupportedStub::ReverseListImpl(const NUdf::IValueBuilder& builder) const {
    Y_UNUSED(builder);
    return nullptr;
}

NUdf::IBoxedValuePtr TComputationValueBaseNotSupportedStub::SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const {
    Y_UNUSED(builder);
    Y_UNUSED(count);
    return nullptr;
}

NUdf::IBoxedValuePtr TComputationValueBaseNotSupportedStub::TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const {
    Y_UNUSED(builder);
    Y_UNUSED(count);
    return nullptr;
}

NUdf::IBoxedValuePtr TComputationValueBaseNotSupportedStub::ToIndexDictImpl(const NUdf::IValueBuilder& builder) const {
    Y_UNUSED(builder);
    return nullptr;
}

ui64 TComputationValueBaseNotSupportedStub::GetDictLength() const {
    ThrowNotSupported(__func__);
    return 0;
}

bool TComputationValueBaseNotSupportedStub::HasDictItems() const {
    ThrowNotSupported(__func__);
    return false;
}

NUdf::TStringRef TComputationValueBaseNotSupportedStub::GetResourceTag() const {
    ThrowNotSupported(__func__);
    return NUdf::TStringRef();
}

void* TComputationValueBaseNotSupportedStub::GetResource() {
    ThrowNotSupported(__func__);
    return nullptr;
}

void TComputationValueBaseNotSupportedStub::Apply(NUdf::IApplyContext& applyCtx) const {
    Y_UNUSED(applyCtx);
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::GetListIterator() const {
    ThrowNotSupported(__func__);
    return {};
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::GetDictIterator() const {
    ThrowNotSupported(__func__);
    return {};
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::GetKeysIterator() const {
    ThrowNotSupported(__func__);
    return {};
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::GetPayloadsIterator() const {
    ThrowNotSupported(__func__);
    return {};
}

bool TComputationValueBaseNotSupportedStub::Contains(const NUdf::TUnboxedValuePod& key) const {
    Y_UNUSED(key);
    ThrowNotSupported(__func__);
    return false;
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::Lookup(const NUdf::TUnboxedValuePod& key) const {
    Y_UNUSED(key);
    ThrowNotSupported(__func__);
    return NUdf::TUnboxedValuePod();
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::GetElement(ui32 index) const {
    Y_UNUSED(index);
    ThrowNotSupported(__func__);
    return {};
}

const NUdf::TUnboxedValue* TComputationValueBaseNotSupportedStub::GetElements() const {
    return nullptr;
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::Run(
    const NUdf::IValueBuilder* valueBuilder,
    const NUdf::TUnboxedValuePod* args) const {
    Y_UNUSED(valueBuilder);
    Y_UNUSED(args);
    ThrowNotSupported(__func__);
    return {};
}

bool TComputationValueBaseNotSupportedStub::Skip() {
    NUdf::TUnboxedValue stub;
    return Next(stub);
}

bool TComputationValueBaseNotSupportedStub::Next(NUdf::TUnboxedValue&) {
    ThrowNotSupported(__func__);
    return false;
}

bool TComputationValueBaseNotSupportedStub::NextPair(NUdf::TUnboxedValue&, NUdf::TUnboxedValue&) {
    ThrowNotSupported(__func__);
    return false;
}

ui32 TComputationValueBaseNotSupportedStub::GetVariantIndex() const {
    ThrowNotSupported(__func__);
    return 0;
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::GetVariantItem() const {
    ThrowNotSupported(__func__);
    return {};
}

NUdf::EFetchStatus TComputationValueBaseNotSupportedStub::Fetch(NUdf::TUnboxedValue& result) {
    Y_UNUSED(result);
    ThrowNotSupported(__func__);
    return NUdf::EFetchStatus::Finish;
}

ui32 TComputationValueBaseNotSupportedStub::GetTraverseCount() const {
    ThrowNotSupported(__func__);
    return 0;
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::GetTraverseItem(ui32 index) const {
    Y_UNUSED(index);
    ThrowNotSupported(__func__);
    return {};
}

NUdf::TUnboxedValue TComputationValueBaseNotSupportedStub::Save() const {
    ThrowNotSupported(__func__);
    return NUdf::TUnboxedValue::Zero();
}

void TComputationValueBaseNotSupportedStub::Load(const NUdf::TStringRef& state) {
    Y_UNUSED(state);
    ThrowNotSupported(__func__);
}

bool TComputationValueBaseNotSupportedStub::Load2(const NUdf::TUnboxedValue& state) {
    Y_UNUSED(state);
    ThrowNotSupported(__func__);
    return false;
}

void TComputationValueBaseNotSupportedStub::Push(const NUdf::TUnboxedValuePod& value) {
    Y_UNUSED(value);
    ThrowNotSupported(__func__);
}

bool TComputationValueBaseNotSupportedStub::IsSortedDict() const {
    ThrowNotSupported(__func__);
    return false;
}

void TComputationValueBaseNotSupportedStub::Unused1() {
    ThrowNotSupported(__func__);
}

void TComputationValueBaseNotSupportedStub::Unused2() {
    ThrowNotSupported(__func__);
}

void TComputationValueBaseNotSupportedStub::Unused3() {
    ThrowNotSupported(__func__);
}

void TComputationValueBaseNotSupportedStub::Unused4() {
    ThrowNotSupported(__func__);
}

void TComputationValueBaseNotSupportedStub::Unused5() {
    ThrowNotSupported(__func__);
}

void TComputationValueBaseNotSupportedStub::Unused6() {
    ThrowNotSupported(__func__);
}

NUdf::EFetchStatus TComputationValueBaseNotSupportedStub::WideFetch(NUdf::TUnboxedValue* result, ui32 width) {
    Y_UNUSED(result);
    Y_UNUSED(width);
    ThrowNotSupported(__func__);
    return NUdf::EFetchStatus::Finish;
}

TString TWideFlowProxyComputationNode::DebugString() const {
    return "WideFlowArg";
}

EValueRepresentation TWideFlowProxyComputationNode::GetRepresentation() const {
    THROW yexception() << "Failed to get representation kind.";
}

NUdf::TUnboxedValue TWideFlowProxyComputationNode::GetValue(TComputationContext&) const {
    THROW yexception() << "Failed to get value from wide flow node.";
}

ui32 TWideFlowProxyComputationNode::GetIndex() const {
    THROW yexception() << "Failed to get proxy node index.";
}

ui32 TWideFlowProxyComputationNode::GetDependentWeight() const {
    THROW yexception() << "Failed to get dependent weight.";
}

ui32 TWideFlowProxyComputationNode::GetDependentsCount() const {
    return Dependents_.size();
}

TComputationExternalNodePtrSet TWideFlowProxyComputationNode::GetUpvalues() const {
    THROW yexception() << "Failed to get proxy node upvalues.";
}

IComputationNode* TWideFlowProxyComputationNode::AddDependent(const IComputationNode* node) {
    Dependents_.push_back(node);
    return this;
}

void TWideFlowProxyComputationNode::AddDependency(const IComputationNode* node) const {
    Dependencies_.push_back(node);
}

void TWideFlowProxyComputationNode::AddOwned(IComputationExternalNode*) const {
}

const IComputationNode* TWideFlowProxyComputationNode::GetSource() const {
    return nullptr;
}

bool TWideFlowProxyComputationNode::IsTemporaryValue() const {
    return true;
}

void TWideFlowProxyComputationNode::RegisterDependencies() const {
}

void TWideFlowProxyComputationNode::PrepareStageOne() {
    std::sort(Dependents_.begin(), Dependents_.end());
    Dependents_.erase(std::unique(Dependents_.begin(), Dependents_.end()), Dependents_.cend());
    if (const auto it = std::find(Dependents_.cbegin(), Dependents_.cend(), Owner_); Dependents_.cend() != it) {
        Dependents_.erase(it);
    }
}

void TWideFlowProxyComputationNode::PrepareStageTwo() {
    TIndexesMap dependents;
    std::for_each(Dependents_.cbegin(), Dependents_.cend(),
                  std::bind(&IComputationNode::CollectDependentIndexes, std::placeholders::_1, Owner_, std::ref(dependents)));
    InvalidationSet_.assign(dependents.cbegin(), dependents.cend());
}

void TWideFlowProxyComputationNode::SetOwner(const IComputationNode* owner) {
    Y_DEBUG_ABORT_UNLESS(!Owner_);
    Owner_ = owner;
}

void TWideFlowProxyComputationNode::CollectDependentIndexes(const IComputationNode*, TIndexesMap&) const {
    THROW yexception() << "Failed to collect dependent indexes.";
}

void TWideFlowProxyComputationNode::CollectUpvalues(TComputationExternalNodePtrSet&) const {
}

void TWideFlowProxyComputationNode::InvalidateValue(TComputationContext& ctx) const {
    for (const auto& index : InvalidationSet_) {
        ctx.MutableValues[index.first] = NUdf::TUnboxedValuePod::Invalid();
    }
}

void TWideFlowProxyComputationNode::SetFetcher(TFetcher&& fetcher) {
    Fetcher_ = std::move(fetcher);
}

EFetchResult TWideFlowProxyComputationNode::FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* values) const {
    return Fetcher_(ctx, values);
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

template <class TContainerOne, class TContainerTwo>
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

template <class TContainerOne, class TContainerTwo>
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
                    } else {
                        item.emplace(j);
                    }
                }
            }
        } else {
            map[ins.first->second].reset();
        }
    }
    return map;
}

template TPasstroughtMap GetPasstroughtMap(const TComputationExternalNodePtrVector& from, const TComputationNodePtrVector& to);
template TPasstroughtMap GetPasstroughtMap(const TComputationNodePtrVector& from, const TComputationExternalNodePtrVector& to);
template TPasstroughtMap GetPasstroughtMapOneToOne(const TComputationExternalNodePtrVector& from, const TComputationNodePtrVector& to);
template TPasstroughtMap GetPasstroughtMapOneToOne(const TComputationNodePtrVector& from, const TComputationExternalNodePtrVector& to);

std::optional<size_t> IsPasstrought(const IComputationNode* root, const TComputationExternalNodePtrVector& args) {
    for (size_t i = 0U; i < args.size(); ++i) {
        if (root == args[i]) {
            return {i};
        }
    }
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
                                    [&applyCtx](const NUdf::TUnboxedValue& item) {
                                        if (item.IsBoxed()) {
                                            item.Apply(applyCtx);
                                        }
                                    });
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

} // namespace NKikimr::NMiniKQL
