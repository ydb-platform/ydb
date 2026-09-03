#include "bridge_node_table.h"

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;
using namespace NYdb::NWasm;

const void* BridgeIdentityKey(const TUnboxedValuePod& value) {
    if (!value) {
        return nullptr;
    }
    if (value.IsBoxed()) {
        return value.AsBoxed().Get();
    }
    if (value.IsString()) {
        // Large MiniKQL strings are EMarkers::String (TStringValue), not Boxed.
        return value.AsRawStringValue();
    }
    return nullptr;
}

namespace {

TBridgeKinds KindsFromDataSlot(EDataSlot slot) {
    switch (slot) {
        case EDataSlot::Bool:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Boolean};
        case EDataSlot::Int8:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Int8};
        case EDataSlot::Uint8:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Uint8};
        case EDataSlot::Int16:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Int16};
        case EDataSlot::Uint16:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Uint16};
        case EDataSlot::Int32:
        case EDataSlot::Date32:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Int32};
        case EDataSlot::Uint32:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Uint32};
        case EDataSlot::Int64:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Int64};
        case EDataSlot::Uint64:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Uint64};
        case EDataSlot::Float:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Float};
        case EDataSlot::Double:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Double};
        case EDataSlot::Date:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Date};
        case EDataSlot::Datetime:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Datetime};
        case EDataSlot::Timestamp:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Timestamp};
        case EDataSlot::Interval:
        case EDataSlot::Interval64:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Interval};
        case EDataSlot::Decimal:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Decimal};
        case EDataSlot::Utf8:
            return {EBridgeNodeKind::String, EBridgeValueKind::Utf8};
        case EDataSlot::Yson:
            return {EBridgeNodeKind::String, EBridgeValueKind::Yson};
        case EDataSlot::Json:
            return {EBridgeNodeKind::String, EBridgeValueKind::Json};
        default:
            // String, Uuid, DyNumber, JsonDocument and the Tz* family are all
            // byte buffers as far as the guest is concerned.
            return {EBridgeNodeKind::String, EBridgeValueKind::String};
    }
}

} // namespace

TBridgeKinds BridgeKindsFromValue(const TUnboxedValuePod& value) {
    if (!value) {
        return {EBridgeNodeKind::Scalar, EBridgeValueKind::Null};
    }
    if (value.IsString()) {
        return {EBridgeNodeKind::String, EBridgeValueKind::String};
    }
    if (value.IsBoxed()) {
        // Could be a list, a dict, a resource or a callable: without a type
        // there is nothing better than the most permissive of them.
        return {EBridgeNodeKind::Callable, EBridgeValueKind::Callable};
    }
    return {EBridgeNodeKind::Scalar, EBridgeValueKind::Int64};
}

TBridgeKinds BridgeKindsFromType(const TType* type, const ITypeInfoHelper* helper) {
    if (!type || !helper) {
        return {};
    }
    switch (helper->GetTypeKind(type)) {
        case ETypeKind::Data: {
            const TDataTypeInspector data(*helper, type);
            if (!data) {
                return {EBridgeNodeKind::Scalar, EBridgeValueKind::Null};
            }
            return KindsFromDataSlot(GetDataSlot(data.GetTypeId()));
        }
        case ETypeKind::Optional: {
            // MiniKQL stores Optional<Data> as the data itself; report the
            // payload kind so scalar getters keep working through a level of
            // optionality, as the historical leaf arguments did.
            const TOptionalTypeInspector optional(*helper, type);
            const auto inner = BridgeKindsFromType(optional.GetItemType(), helper);
            if (inner.Node == EBridgeNodeKind::Scalar || inner.Node == EBridgeNodeKind::String) {
                return inner;
            }
            return {EBridgeNodeKind::Optional, EBridgeValueKind::Optional};
        }
        case ETypeKind::Tagged: {
            const TTaggedTypeInspector tagged(*helper, type);
            return BridgeKindsFromType(tagged.GetBaseType(), helper);
        }
        case ETypeKind::List:
        case ETypeKind::EmptyList:
        case ETypeKind::Stream:
            return {EBridgeNodeKind::List, EBridgeValueKind::List};
        case ETypeKind::Dict:
        case ETypeKind::EmptyDict:
            return {EBridgeNodeKind::Dict, EBridgeValueKind::Dict};
        case ETypeKind::Tuple:
            return {EBridgeNodeKind::Tuple, EBridgeValueKind::Tuple};
        case ETypeKind::Struct:
            return {EBridgeNodeKind::Struct, EBridgeValueKind::Struct};
        case ETypeKind::Variant:
            return {EBridgeNodeKind::Variant, EBridgeValueKind::Variant};
        case ETypeKind::Resource:
            return {EBridgeNodeKind::Resource, EBridgeValueKind::Resource};
        case ETypeKind::Callable:
            return {EBridgeNodeKind::Callable, EBridgeValueKind::Callable};
        case ETypeKind::Null:
        case ETypeKind::Void:
            return {EBridgeNodeKind::Scalar, EBridgeValueKind::Null};
        default:
            return {};
    }
}

TWasmBridgeNodeTable::TWasmBridgeNodeTable(ui64 generation)
    : Generation_(generation)
{
    if (Generation_ == 0) {
        ythrow yexception() << "Bridge: node table requires a non-zero generation";
    }
}

ui64 TWasmBridgeNodeTable::AllocateIndex() {
    if (NextIndex_ > MaxBridgeNodeIndex) {
        ythrow yexception() << "Bridge: node table index overflow";
    }
    return NextIndex_++;
}

void TWasmBridgeNodeTable::EnsureHandle(ui64 handle) const {
    if (handle == NullBridgeHandle) {
        ythrow yexception() << "Bridge: null handle";
    }
    if (BridgeHandleGeneration(handle) != Generation_) {
        ythrow yexception()
            << "Bridge: stale handle generation=" << BridgeHandleGeneration(handle)
            << " (current=" << Generation_ << ")";
    }
}

TWasmBridgeNodeTable::TNode& TWasmBridgeNodeTable::ResolveIndex(ui64 index) {
    auto* node = Nodes_.FindPtr(index);
    if (!node) {
        ythrow yexception() << "Bridge: unknown node index " << index;
    }
    return *node;
}

const TWasmBridgeNodeTable::TNode& TWasmBridgeNodeTable::ResolveIndex(ui64 index) const {
    const auto* node = Nodes_.FindPtr(index);
    if (!node) {
        ythrow yexception() << "Bridge: unknown node index " << index;
    }
    return *node;
}

ui64 TWasmBridgeNodeTable::RegisterUntracked(
    EBridgeNodeKind kind,
    EBridgeValueKind valueKind,
    const TType* type,
    TUnboxedValue&& value,
    const TType* auxType)
{
    const ui64 index = AllocateIndex();
    TNode node;
    node.Kind = kind;
    node.ValueKind = valueKind;
    node.Type = type;
    node.AuxType = auxType;
    node.Value = std::move(value);
    node.Refs = 1;

    if (const void* key = BridgeIdentityKey(node.Value)) {
        // Aliasing values (Optional over the same pod) must not steal the
        // entry: only the first registration owns and erases it.
        node.OwnsIdentity = Identity_.emplace(key, index).second;
    }

    Nodes_.emplace(index, std::move(node));
    return PackBridgeHandle(Generation_, index);
}

ui64 TWasmBridgeNodeTable::Register(
    EBridgeNodeKind kind,
    EBridgeValueKind valueKind,
    const TType* type,
    TUnboxedValue&& value,
    const TType* auxType)
{
    return TrackInRunScope(
        RegisterUntracked(kind, valueKind, type, std::move(value), auxType));
}

ui64 TWasmBridgeNodeTable::RegisterOrReuse(
    EBridgeNodeKind kind,
    EBridgeValueKind valueKind,
    const TType* type,
    const TUnboxedValuePod& value,
    const TType* auxType)
{
    if (const ui64 existing = TryReuse(value); existing != NullBridgeHandle) {
        Ref(existing);
        return TrackInRunScope(existing);
    }
    return TrackInRunScope(
        RegisterUntracked(kind, valueKind, type, TUnboxedValue(value), auxType));
}

ui64 TWasmBridgeNodeTable::TryReuse(const TUnboxedValuePod& value) const {
    const void* key = BridgeIdentityKey(value);
    if (!key) {
        return NullBridgeHandle;
    }
    const auto* index = Identity_.FindPtr(key);
    if (!index) {
        return NullBridgeHandle;
    }
    return PackBridgeHandle(Generation_, *index);
}

TWasmBridgeNodeTable::TNode& TWasmBridgeNodeTable::Resolve(ui64 handle) {
    EnsureHandle(handle);
    return ResolveIndex(BridgeHandleIndex(handle));
}

const TWasmBridgeNodeTable::TNode& TWasmBridgeNodeTable::Resolve(ui64 handle) const {
    EnsureHandle(handle);
    return ResolveIndex(BridgeHandleIndex(handle));
}

void TWasmBridgeNodeTable::Ref(ui64 handle) {
    auto& node = Resolve(handle);
    // A guest looping on BridgeRef would otherwise wrap the counter and turn
    // the next Unref into a use-after-free of the TUnboxedValue.
    if (node.Refs == Max<ui32>()) {
        ythrow yexception()
            << "Bridge: refcount overflow on node index " << BridgeHandleIndex(handle);
    }
    ++node.Refs;
}

void TWasmBridgeNodeTable::Unref(ui64 handle) {
    EnsureHandle(handle);
    const ui64 index = BridgeHandleIndex(handle);
    auto it = Nodes_.find(index);
    if (it == Nodes_.end()) {
        ythrow yexception() << "Bridge: unknown node index " << index;
    }
    if (it->second.Refs == 0) {
        ythrow yexception() << "Bridge: Unref on node with zero refs, index " << index;
    }
    if (--it->second.Refs > 0) {
        return;
    }
    if (it->second.OwnsIdentity) {
        if (const void* key = BridgeIdentityKey(it->second.Value)) {
            Identity_.erase(key);
        }
    }
    Nodes_.erase(it);
}

void TWasmBridgeNodeTable::BeginRunScope() {
    RunScopes_.emplace_back();
}

void TWasmBridgeNodeTable::EndRunScope() noexcept {
    if (RunScopes_.empty()) {
        return;
    }
    const auto tracked = std::move(RunScopes_.back());
    RunScopes_.pop_back();
    // Youngest first, so a child node dies before the parent it borrows from.
    for (auto it = tracked.rbegin(); it != tracked.rend(); ++it) {
        ReleaseTracked(*it);
    }
}

ui64 TWasmBridgeNodeTable::TrackInRunScope(ui64 handle) {
    if (!RunScopes_.empty()) {
        RunScopes_.back().push_back(handle);
    }
    return handle;
}

void TWasmBridgeNodeTable::ReleaseTracked(ui64 handle) noexcept {
    try {
        // A guest that released its own temporary already erased the node.
        // Indices are handed out monotonically, so a missing index can never
        // have been recycled into an unrelated node.
        if (Nodes_.contains(BridgeHandleIndex(handle))) {
            Unref(handle);
        }
    } catch (...) {
        // Nothing sensible to do while tearing a Run down.
    }
}

ui64 EnsureBridgeStringResident(
    const TWasmBridgeNodeTable::TNode& node,
    TCompartmentResidentCache& cache)
{
    if (!IsBridgeStringKind(node.ValueKind)) {
        ythrow yexception()
            << "Bridge: EnsureBridgeStringResident on non-string node, kind "
            << static_cast<int>(node.ValueKind);
    }
    const TStringRef bytes = node.Value.AsStringRef();
    if (const void* key = BridgeIdentityKey(node.Value)) {
        return cache.Pin(key, node.Value, bytes);
    }
    // Embedded strings live inside the pod: no identity to key a pin on, and
    // nothing to reuse across rows either.
    return cache.PinScratch(bytes);
}

} // namespace NKikimr::NUdfStore::NWasm
