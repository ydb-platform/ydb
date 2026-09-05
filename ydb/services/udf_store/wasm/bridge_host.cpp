#include "bridge_node_table.h"
#include "bridge_resident.h"
#include "bridge_types.h"

#include <yql/essentials/public/udf/udf_type_inspection.h>
#include "compartment_manager.h"
#include "host.h"
#include "host_intrinsic.h"
#include "invocation_context.h"
#include "registry_helpers.h"

#include <ydb/services/udf_store/wasm/abi/bridge_abi.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <util/generic/utility.h>

#include <bit>
#include <cstring>
#include <limits>
#include <optional>

namespace NKikimr::NUdfStore::NWasm {
namespace {

using namespace NYdb::NWasm;
using namespace NYql::NUdf;

constexpr ui32 MaxBridgeRunDepth = 64;

TQueryCompartmentHandle& CurrentQueryOrThrow() {
    auto* query = GetCurrentQueryCompartment();
    if (!query) {
        ythrow yexception() << "Bridge: no active query compartment";
    }
    return *query;
}

TWasmBridgeNodeTable& CurrentBridgeTable() {
    auto& query = CurrentQueryOrThrow();
    if (!query.BridgeNodes) {
        ythrow yexception() << "Bridge: no active node table";
    }
    return *query.BridgeNodes;
}

IWebAssemblyCompartment* CurrentCompartmentOrThrow() {
    auto* compartment = GetCurrentCompartment();
    if (!compartment) {
        ythrow yexception() << "Bridge: no active WASM compartment";
    }
    return compartment;
}

TCompartmentResidentCache& CurrentResidentCache() {
    auto& query = CurrentQueryOrThrow();
    if (!query.Resident) {
        query.Resident = std::make_unique<TCompartmentResidentCache>(CurrentCompartmentOrThrow());
    }
    return *query.Resident;
}

const IValueBuilder* CurrentValueBuilderOrThrow() {
    auto* builder = CurrentBridgeTable().GetValueBuilder();
    if (!builder) {
        ythrow yexception() << "Bridge: value builder is not installed";
    }
    return builder;
}

ui64 RegisterOwned(
    EBridgeNodeKind kind,
    EBridgeValueKind valueKind,
    TUnboxedValue&& value,
    const TType* type = nullptr,
    const TType* auxType = nullptr)
{
    return CurrentBridgeTable().Register(
        kind,
        valueKind,
        type,
        std::move(value),
        auxType);
}

const ITypeInfoHelper* CurrentTypeHelper() {
    return CurrentBridgeTable().GetTypeInfoHelper();
}

//! Register a value reached by traversal (dict payload, list item, tuple
//! element, callable result). Kinds come from the declared type, so nested
//! containers and non-Int64 scalars keep working; only untyped nodes fall
//! back to guessing from the representation.
ui64 RegisterChild(const TUnboxedValuePod& value, const TType* type, const TType* auxType = nullptr) {
    auto& table = CurrentBridgeTable();
    auto kinds = BridgeKindsFromType(type, table.GetTypeInfoHelper());
    if (kinds.Node == EBridgeNodeKind::Unknown) {
        kinds = BridgeKindsFromValue(value);
    }
    if (!value) {
        return table.Register(kinds.Node, EBridgeValueKind::Null, type, {}, auxType);
    }
    return table.RegisterOrReuse(kinds.Node, kinds.Value, type, value, auxType);
}

const TType* OptionalItemTypeOf(const TType* type) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper) {
        return nullptr;
    }
    const TOptionalTypeInspector optional(*helper, type);
    return optional ? optional.GetItemType() : nullptr;
}

const TType* ListItemTypeOf(const TType* type) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper) {
        return nullptr;
    }
    const TListTypeInspector list(*helper, type);
    return list ? list.GetItemType() : nullptr;
}

const TType* DictKeyTypeOf(const TType* type) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper) {
        return nullptr;
    }
    const TDictTypeInspector dict(*helper, type);
    return dict ? dict.GetKeyType() : nullptr;
}

const TType* DictPayloadTypeOf(const TType* type) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper) {
        return nullptr;
    }
    const TDictTypeInspector dict(*helper, type);
    return dict ? dict.GetValueType() : nullptr;
}

const TType* ElementTypeOf(const TType* type, ui32 index) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper) {
        return nullptr;
    }
    if (const TTupleTypeInspector tuple(*helper, type); tuple) {
        return index < tuple.GetElementsCount() ? tuple.GetElementType(index) : nullptr;
    }
    if (const TStructTypeInspector members(*helper, type); members) {
        return index < members.GetMembersCount() ? members.GetMemberType(index) : nullptr;
    }
    return nullptr;
}

const TType* CallableResultTypeOf(const TType* type) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper) {
        return nullptr;
    }
    const TCallableTypeInspector callable(*helper, type);
    return callable ? callable.GetReturnType() : nullptr;
}

//! Arity of a Tuple / Struct type; nothing when the type is neither or when
//! the node carries no type at all.
std::optional<ui32> MemberCountOfType(const TType* type) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper) {
        return std::nullopt;
    }
    if (const TStructTypeInspector members(*helper, type); members) {
        return members.GetMembersCount();
    }
    if (const TTupleTypeInspector elements(*helper, type); elements) {
        return elements.GetElementsCount();
    }
    return std::nullopt;
}

//! The Variant a UDF with this result type is allowed to build, looked up
//! through the Optional / List / Dict layers a result type may wrap it in.
const TType* FindVariantTypeIn(const TType* type, ui32 depth = 0) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper || depth > 8) {
        return nullptr;
    }
    if (const TVariantTypeInspector variant(*helper, type); variant) {
        return type;
    }
    if (const TType* item = OptionalItemTypeOf(type)) {
        return FindVariantTypeIn(item, depth + 1);
    }
    if (const TType* item = ListItemTypeOf(type)) {
        return FindVariantTypeIn(item, depth + 1);
    }
    if (const TType* payload = DictPayloadTypeOf(type)) {
        return FindVariantTypeIn(payload, depth + 1);
    }
    return nullptr;
}

//! Same lookup for the Struct / Tuple a UDF with this result type is allowed
//! to build. BridgeMakeStruct is defined in terms of the declared result type
//! ("members follow its member order"), so that type is the only candidate.
const TType* FindMemberedTypeIn(const TType* type, bool wantStruct, ui32 depth = 0) {
    const auto* helper = CurrentTypeHelper();
    if (!type || !helper || depth > 8) {
        return nullptr;
    }
    if (wantStruct) {
        if (const TStructTypeInspector members(*helper, type); members) {
            return type;
        }
    } else {
        if (const TTupleTypeInspector elements(*helper, type); elements) {
            return type;
        }
    }
    if (const TType* item = OptionalItemTypeOf(type)) {
        return FindMemberedTypeIn(item, wantStruct, depth + 1);
    }
    if (const TType* item = ListItemTypeOf(type)) {
        return FindMemberedTypeIn(item, wantStruct, depth + 1);
    }
    if (const TType* payload = DictPayloadTypeOf(type)) {
        return FindMemberedTypeIn(payload, wantStruct, depth + 1);
    }
    return nullptr;
}

void EnsureKind(const TWasmBridgeNodeTable::TNode& node, EBridgeValueKind expected, const char* what) {
    if (node.ValueKind != expected) {
        ythrow yexception()
            << "Bridge: " << what << " expected kind "
            << static_cast<int>(expected)
            << ", got " << static_cast<int>(node.ValueKind);
    }
}

void EnsureStringKind(const TWasmBridgeNodeTable::TNode& node, const char* what) {
    if (!IsBridgeStringKind(node.ValueKind)) {
        ythrow yexception()
            << "Bridge: " << what << " expected a string-like kind, got "
            << static_cast<int>(node.ValueKind);
    }
}

//! Integral getters widen: the guest asks for i64/ui64 whatever the declared
//! width is, and BridgeGetKind tells it what the value really is.
i64 ReadSigned(const TWasmBridgeNodeTable::TNode& node, const char* what) {
    switch (node.ValueKind) {
        case EBridgeValueKind::Int8:
            return node.Value.Get<i8>();
        case EBridgeValueKind::Int16:
            return node.Value.Get<i16>();
        case EBridgeValueKind::Int32:
            return node.Value.Get<i32>();
        case EBridgeValueKind::Int64:
        case EBridgeValueKind::Interval:
            return node.Value.Get<i64>();
        default:
            ythrow yexception()
                << "Bridge: " << what << " expected a signed integer kind, got "
                << static_cast<int>(node.ValueKind);
    }
}

ui64 ReadUnsigned(const TWasmBridgeNodeTable::TNode& node, const char* what) {
    switch (node.ValueKind) {
        case EBridgeValueKind::Uint8:
            return node.Value.Get<ui8>();
        case EBridgeValueKind::Uint16:
        case EBridgeValueKind::Date:
            return node.Value.Get<ui16>();
        case EBridgeValueKind::Uint32:
        case EBridgeValueKind::Datetime:
            return node.Value.Get<ui32>();
        case EBridgeValueKind::Uint64:
        case EBridgeValueKind::Timestamp:
            return node.Value.Get<ui64>();
        default:
            ythrow yexception()
                << "Bridge: " << what << " expected an unsigned integer kind, got "
                << static_cast<int>(node.ValueKind);
    }
}

double ReadFloating(const TWasmBridgeNodeTable::TNode& node, const char* what) {
    switch (node.ValueKind) {
        case EBridgeValueKind::Float:
            return node.Value.Get<float>();
        case EBridgeValueKind::Double:
            return node.Value.Get<double>();
        default:
            ythrow yexception()
                << "Bridge: " << what << " expected a floating point kind, got "
                << static_cast<int>(node.ValueKind);
    }
}

// --- Host implementations (signatures must match InferType-capable types) ---

i32 BridgeGetKindHost(ui64 handle) {
    if (handle == NullBridgeHandle) {
        return static_cast<i32>(EBridgeValueKind::Null);
    }
    return static_cast<i32>(CurrentBridgeTable().Resolve(handle).ValueKind);
}

i32 BridgeIsNullHost(ui64 handle) {
    if (handle == NullBridgeHandle) {
        return 1;
    }
    const auto& node = CurrentBridgeTable().Resolve(handle);
    if (node.ValueKind == EBridgeValueKind::Null) {
        return 1;
    }
    if (node.ValueKind == EBridgeValueKind::Optional && !node.Value) {
        return 1;
    }
    return 0;
}

i64 BridgeGetInt64Host(ui64 handle) {
    return ReadSigned(CurrentBridgeTable().Resolve(handle), "BridgeGetInt64");
}

ui64 BridgeGetUint64Host(ui64 handle) {
    return ReadUnsigned(CurrentBridgeTable().Resolve(handle), "BridgeGetUint64");
}

i32 BridgeGetInt32Host(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Int32, "BridgeGetInt32");
    return node.Value.Get<i32>();
}

ui32 BridgeGetUint32Host(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    if (node.ValueKind != EBridgeValueKind::Uint32 && node.ValueKind != EBridgeValueKind::Datetime) {
        ythrow yexception()
            << "Bridge: BridgeGetUint32 expected Uint32/Datetime, got "
            << static_cast<int>(node.ValueKind);
    }
    return node.Value.Get<ui32>();
}

float BridgeGetFloatHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Float, "BridgeGetFloat");
    return node.Value.Get<float>();
}

double BridgeGetDoubleHost(ui64 handle) {
    return ReadFloating(CurrentBridgeTable().Resolve(handle), "BridgeGetDouble");
}

//! Decimal has no scalar intrinsic: the guest reads the raw 16-byte value.
void BridgeCopyDecimalHost(ui64 handle, ui64 dstOff) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Decimal, "BridgeCopyDecimal");
    const auto value = node.Value.GetInt128();
    auto* compartment = CurrentCompartmentOrThrow();
    char* dst = PtrFromVM(
        compartment,
        std::bit_cast<char*>(static_cast<uintptr_t>(dstOff)),
        sizeof(value));
    std::memcpy(dst, &value, sizeof(value));
}

i32 BridgeGetBoolHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Boolean, "BridgeGetBool");
    return node.Value.Get<bool>() ? 1 : 0;
}

i64 BridgeGetStringLenHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureStringKind(node, "BridgeGetStringLen");
    return static_cast<i64>(node.Value.AsStringRef().Size());
}

i64 BridgeCopyStringHost(ui64 handle, ui64 dstOff, i64 cap) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureStringKind(node, "BridgeCopyString");
    const TStringRef ref = node.Value.AsStringRef();
    if (cap < 0 || static_cast<ui64>(cap) < ref.Size()) {
        ythrow yexception()
            << "Bridge: BridgeCopyString capacity " << cap
            << " is less than string length " << ref.Size();
    }
    auto* compartment = CurrentCompartmentOrThrow();
    if (ref.Size() == 0) {
        return 0;
    }
    char* dst = PtrFromVM(compartment, std::bit_cast<char*>(static_cast<uintptr_t>(dstOff)), ref.Size());
    std::memcpy(dst, ref.Data(), ref.Size());
    return static_cast<i64>(ref.Size());
}

ui64 BridgeEnsureStringHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureStringKind(node, "BridgeEnsureString");
    return EnsureBridgeStringResident(node, CurrentResidentCache());
}

ui64 BridgeGetOptionalHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Optional, "BridgeGetOptional");
    if (!node.Value) {
        return NullBridgeHandle;
    }
    auto inner = node.Value.GetOptionalValue();
    if (!inner) {
        return NullBridgeHandle;
    }
    const TType* itemType = OptionalItemTypeOf(node.Type);
    return RegisterChild(inner, itemType ? itemType : node.AuxType);
}

ui64 BridgeGetElementHost(ui64 handle, i32 index) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    if (node.ValueKind != EBridgeValueKind::Tuple
        && node.ValueKind != EBridgeValueKind::Struct)
    {
        ythrow yexception()
            << "Bridge: BridgeGetElement expected Tuple/Struct, got "
            << static_cast<int>(node.ValueKind);
    }
    // The element array is host memory: an unchecked index would read past it.
    const auto arity = MemberCountOfType(node.Type);
    if (!arity) {
        ythrow yexception() << "Bridge: BridgeGetElement needs a typed Struct/Tuple node";
    }
    if (index < 0 || static_cast<ui32>(index) >= *arity) {
        ythrow yexception()
            << "Bridge: BridgeGetElement index " << index
            << " is out of range [0, " << *arity << ")";
    }
    auto elem = node.Value.GetElement(static_cast<ui32>(index));
    return RegisterChild(elem, ElementTypeOf(node.Type, static_cast<ui32>(index)));
}

i32 BridgeGetMemberCountHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    const auto arity = MemberCountOfType(node.Type);
    if (!arity) {
        ythrow yexception() << "Bridge: BridgeGetMemberCount needs a typed Struct/Tuple node";
    }
    return static_cast<i32>(*arity);
}

i32 BridgeGetMemberIndexHost(ui64 handle, ui64 nameOff, i64 nameLen) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    const auto* helper = CurrentTypeHelper();
    if (!node.Type || !helper || nameLen < 0) {
        ythrow yexception() << "Bridge: BridgeGetMemberIndex needs a typed Struct node";
    }
    const TStructTypeInspector members(*helper, node.Type);
    if (!members) {
        ythrow yexception() << "Bridge: BridgeGetMemberIndex expected Struct";
    }
    auto* compartment = CurrentCompartmentOrThrow();
    const char* name = nameLen == 0
        ? nullptr
        : PtrFromVM(
            compartment,
            std::bit_cast<char*>(static_cast<uintptr_t>(nameOff)),
            static_cast<size_t>(nameLen));
    const ui32 index = members.GetMemberIndex(
        TStringRef(name, CheckedAbiLength(static_cast<size_t>(nameLen), "BridgeGetMemberIndex")));
    return index == Max<ui32>() ? -1 : static_cast<i32>(index);
}

i32 BridgeGetVariantIndexHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Variant, "BridgeGetVariantIndex");
    return static_cast<i32>(node.Value.GetVariantIndex());
}

ui64 BridgeGetVariantItemHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Variant, "BridgeGetVariantItem");
    auto item = node.Value.GetVariantItem();
    const auto* helper = CurrentTypeHelper();
    const TType* itemType = nullptr;
    if (node.Type && helper) {
        const TVariantTypeInspector variant(*helper, node.Type);
        if (variant) {
            itemType = ElementTypeOf(variant.GetUnderlyingType(), node.Value.GetVariantIndex());
        }
    }
    return RegisterChild(item, itemType);
}

i64 BridgeListLengthHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::List, "BridgeListLength");
    return static_cast<i64>(node.Value.GetListLength());
}

i32 BridgeListHasItemsHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::List, "BridgeListHasItems");
    return node.Value.HasListItems() ? 1 : 0;
}

ui64 BridgeListMakeIteratorHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::List, "BridgeListMakeIterator");
    auto iter = node.Value.GetListIterator();
    // Iterator nodes keep the type of what they yield: AuxType is the item
    // type (payload type for a dict pair iterator, see below).
    return RegisterOwned(
        EBridgeNodeKind::ListIterator,
        EBridgeValueKind::List,
        std::move(iter),
        /*type*/ nullptr,
        ListItemTypeOf(node.Type));
}

i32 BridgeListIterNextHost(ui64 iterHandle, ui64* outItem) {
    auto& node = CurrentBridgeTable().Resolve(iterHandle);
    if (node.Kind != EBridgeNodeKind::ListIterator) {
        ythrow yexception() << "Bridge: BridgeListIterNext expected list iterator";
    }
    auto* compartment = CurrentCompartmentOrThrow();
    const TType* itemType = node.AuxType;
    TUnboxedValue item;
    const bool has = node.Value.Next(item);
    ui64 itemHandle = NullBridgeHandle;
    if (has) {
        itemHandle = RegisterChild(item, itemType);
    }
    if (outItem) {
        *PtrFromVM(compartment, outItem) = itemHandle;
    }
    return has ? 1 : 0;
}

i64 BridgeDictLengthHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Dict, "BridgeDictLength");
    return static_cast<i64>(node.Value.GetDictLength());
}

i32 BridgeDictHasItemsHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Dict, "BridgeDictHasItems");
    return node.Value.HasDictItems() ? 1 : 0;
}

i32 BridgeDictContainsHost(ui64 dictHandle, ui64 keyHandle) {
    const auto& dict = CurrentBridgeTable().Resolve(dictHandle);
    EnsureKind(dict, EBridgeValueKind::Dict, "BridgeDictContains");
    const auto& key = CurrentBridgeTable().Resolve(keyHandle);
    return dict.Value.Contains(key.Value) ? 1 : 0;
}

ui64 BridgeDictLookupHost(ui64 dictHandle, ui64 keyHandle) {
    const auto& dict = CurrentBridgeTable().Resolve(dictHandle);
    EnsureKind(dict, EBridgeValueKind::Dict, "BridgeDictLookup");
    const auto& key = CurrentBridgeTable().Resolve(keyHandle);
    auto payload = dict.Value.Lookup(key.Value);
    if (!payload) {
        return NullBridgeHandle;
    }
    // Lookup wraps the payload in an Optional to say "found". Unwrapping it
    // may well give an empty value again -- that is a Dict<K, V?> holding a
    // null -- and RegisterChild turns it into a Null node. Answering with a
    // handle keeps that case apart from the missing key above.
    auto inner = payload.GetOptionalValue();
    return RegisterChild(inner, DictPayloadTypeOf(dict.Type));
}

ui64 BridgeDictMakeIteratorHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Dict, "BridgeDictMakeIterator");
    auto iter = node.Value.GetDictIterator();
    // Pair iterator: Type is the key type, AuxType the payload type.
    return RegisterOwned(
        EBridgeNodeKind::DictIterator,
        EBridgeValueKind::Dict,
        std::move(iter),
        DictKeyTypeOf(node.Type),
        DictPayloadTypeOf(node.Type));
}

ui64 BridgeDictMakeKeysIteratorHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Dict, "BridgeDictMakeKeysIterator");
    auto iter = node.Value.GetKeysIterator();
    return RegisterOwned(
        EBridgeNodeKind::ListIterator,
        EBridgeValueKind::List,
        std::move(iter),
        /*type*/ nullptr,
        DictKeyTypeOf(node.Type));
}

ui64 BridgeDictMakePayloadsIteratorHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Dict, "BridgeDictMakePayloadsIterator");
    auto iter = node.Value.GetPayloadsIterator();
    return RegisterOwned(
        EBridgeNodeKind::ListIterator,
        EBridgeValueKind::List,
        std::move(iter),
        /*type*/ nullptr,
        DictPayloadTypeOf(node.Type));
}

i32 BridgeDictIterNextHost(ui64 iterHandle, ui64* outKey, ui64* outPayload) {
    auto& node = CurrentBridgeTable().Resolve(iterHandle);
    if (node.Kind != EBridgeNodeKind::DictIterator) {
        ythrow yexception() << "Bridge: BridgeDictIterNext expected dict iterator";
    }
    auto* compartment = CurrentCompartmentOrThrow();
    const TType* keyType = node.Type;
    const TType* payloadType = node.AuxType;
    TUnboxedValue key;
    TUnboxedValue payload;
    const bool has = node.Value.NextPair(key, payload);
    ui64 keyHandle = NullBridgeHandle;
    ui64 payloadHandle = NullBridgeHandle;
    if (has) {
        keyHandle = RegisterChild(key, keyType);
        payloadHandle = RegisterChild(payload, payloadType);
    }
    if (outKey) {
        *PtrFromVM(compartment, outKey) = keyHandle;
    }
    if (outPayload) {
        *PtrFromVM(compartment, outPayload) = payloadHandle;
    }
    return has ? 1 : 0;
}

ui64 BridgeMakeNullHost() {
    return RegisterOwned(EBridgeNodeKind::Scalar, EBridgeValueKind::Null, {});
}

ui64 BridgeMakeInt64Host(i64 value) {
    return RegisterOwned(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        TUnboxedValuePod(value));
}

ui64 BridgeMakeUint64Host(ui64 value) {
    return RegisterOwned(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Uint64,
        TUnboxedValuePod(value));
}

ui64 BridgeMakeInt32Host(i32 value) {
    return RegisterOwned(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int32,
        TUnboxedValuePod(value));
}

ui64 BridgeMakeUint32Host(ui32 value) {
    return RegisterOwned(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Uint32,
        TUnboxedValuePod(value));
}

ui64 BridgeMakeFloatHost(float value) {
    return RegisterOwned(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Float,
        TUnboxedValuePod(value));
}

ui64 BridgeMakeDoubleHost(double value) {
    return RegisterOwned(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Double,
        TUnboxedValuePod(value));
}

ui64 BridgeMakeBoolHost(i32 value) {
    return RegisterOwned(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Boolean,
        TUnboxedValuePod(value != 0));
}

ui64 BridgeMakeStringHost(ui64 srcOff, i64 len) {
    if (len < 0) {
        ythrow yexception() << "Bridge: BridgeMakeString negative length";
    }
    auto* builder = CurrentValueBuilderOrThrow();
    auto* compartment = CurrentCompartmentOrThrow();
    const char* src = len == 0
        ? nullptr
        : PtrFromVM(compartment, std::bit_cast<char*>(static_cast<uintptr_t>(srcOff)), static_cast<size_t>(len));
    auto value = builder->NewString(
        TStringRef(src, CheckedAbiLength(static_cast<size_t>(len), "BridgeMakeString")));
    return RegisterOwned(EBridgeNodeKind::String, EBridgeValueKind::String, std::move(value));
}

ui64 BridgeMakeOptionalHost(ui64 innerHandle) {
    if (innerHandle == NullBridgeHandle) {
        return RegisterOwned(EBridgeNodeKind::Optional, EBridgeValueKind::Optional, {});
    }
    auto& table = CurrentBridgeTable();
    const auto& inner = table.Resolve(innerHandle);
    const TType* innerType = inner.Type;
    const TUnboxedValuePod optional = inner.Value.MakeOptional();
    // MiniKQL represents Optional over a boxed value or a refcounted string as
    // the payload itself, so MakeOptional gives back the identity it was
    // handed. RegisterOrReuse then returns innerHandle with an extra ref
    // instead of a second node, keeping one node per identity and the resident
    // cache keyed once. The reused node keeps its original kind: the guest may
    // still be reading it as the list or dict it was registered as.
    return table.RegisterOrReuse(
        EBridgeNodeKind::Optional,
        EBridgeValueKind::Optional,
        /*type*/ nullptr,
        optional,
        innerType);
}

//! Read `n` handles from linear memory into their values.
TVector<TUnboxedValue> ResolveHandleArray(ui64 handlesOff, i32 n, const char* what) {
    if (n < 0) {
        ythrow yexception() << "Bridge: " << what << " negative count";
    }
    auto* compartment = CurrentCompartmentOrThrow();
    const ui64* handles = n == 0
        ? nullptr
        : PtrFromVM(
            compartment,
            std::bit_cast<ui64*>(static_cast<uintptr_t>(handlesOff)),
            sizeof(ui64) * static_cast<size_t>(n));
    TVector<TUnboxedValue> values(static_cast<size_t>(n));
    for (i32 i = 0; i < n; ++i) {
        if (handles[i] != NullBridgeHandle) {
            values[i] = CurrentBridgeTable().Resolve(handles[i]).Value;
        }
    }
    return values;
}

ui64 MakeArrayLike(ui64 elemsOff, i32 n, EBridgeNodeKind kind, EBridgeValueKind valueKind, const char* what) {
    auto values = ResolveHandleArray(elemsOff, n, what);
    auto* builder = CurrentValueBuilderOrThrow();
    TUnboxedValue* items = nullptr;
    auto result = builder->NewArray(static_cast<ui32>(n), items);
    for (i32 i = 0; i < n; ++i) {
        items[i] = std::move(values[i]);
    }
    // Untyped the guest cannot read back what it just built: GetMemberCount,
    // GetElement and GetMemberIndex all need a type. Take the one from the
    // declared result type, but only when the arity agrees -- a nested
    // container of a different width is a different type.
    auto* invocation = GetCurrentInvocationContext();
    const TType* type = invocation
        ? FindMemberedTypeIn(invocation->ResultType, valueKind == EBridgeValueKind::Struct)
        : nullptr;
    if (const auto arity = MemberCountOfType(type); !arity || *arity != static_cast<ui32>(n)) {
        type = nullptr;
    }
    return RegisterOwned(kind, valueKind, std::move(result), type);
}

ui64 BridgeMakeArrayHost(ui64 elemsOff, i32 n) {
    return MakeArrayLike(elemsOff, n, EBridgeNodeKind::Tuple, EBridgeValueKind::Tuple, "BridgeMakeArray");
}

//! A MiniKQL struct is laid out like a tuple; members follow the declared
//! member order of the result type.
ui64 BridgeMakeStructHost(ui64 membersOff, i32 n) {
    return MakeArrayLike(membersOff, n, EBridgeNodeKind::Struct, EBridgeValueKind::Struct, "BridgeMakeStruct");
}

ui64 BridgeMakeListHost(ui64 itemsOff, i32 n) {
    auto values = ResolveHandleArray(itemsOff, n, "BridgeMakeList");
    auto* builder = CurrentValueBuilderOrThrow();
    auto result = builder->NewList(values.data(), static_cast<ui64>(n));
    return RegisterOwned(EBridgeNodeKind::List, EBridgeValueKind::List, std::move(result));
}

ui64 BridgeMakeVariantHost(i32 index, ui64 itemHandle) {
    if (index < 0) {
        ythrow yexception() << "Bridge: BridgeMakeVariant negative index";
    }
    // Nothing validates the alternative index later on: MiniKQL stores it as
    // given and indexes the underlying type with it when reading the value.
    auto* invocation = GetCurrentInvocationContext();
    const TType* variantType = invocation
        ? FindVariantTypeIn(invocation->ResultType)
        : nullptr;
    if (!variantType) {
        ythrow yexception()
            << "Bridge: BridgeMakeVariant needs a Variant in the declared result type";
    }
    const auto* helper = CurrentTypeHelper();
    const TVariantTypeInspector variant(*helper, variantType);
    const auto arity = MemberCountOfType(variant.GetUnderlyingType());
    if (!arity || static_cast<ui32>(index) >= *arity) {
        ythrow yexception()
            << "Bridge: BridgeMakeVariant index " << index
            << " is out of range for the declared Variant with "
            << arity.value_or(0) << " alternatives";
    }
    auto& table = CurrentBridgeTable();
    auto* builder = CurrentValueBuilderOrThrow();
    TUnboxedValue item;
    if (itemHandle != NullBridgeHandle) {
        // Not consumed: the copy takes its own MiniKQL ref and the guest keeps
        // the handle it passed in, same as every other Make*.
        item = table.Resolve(itemHandle).Value;
    }
    auto result = builder->NewVariant(static_cast<ui32>(index), std::move(item));
    return RegisterOwned(
        EBridgeNodeKind::Variant,
        EBridgeValueKind::Variant,
        std::move(result));
}

//! Type of the value the running UDF has to return, as a value-less node.
//! Needed by BridgeMakeDict: only the host knows the MiniKQL dict type.
ui64 BridgeGetResultTypeHost() {
    auto* invocation = GetCurrentInvocationContext();
    if (!invocation || !invocation->ResultType) {
        ythrow yexception() << "Bridge: BridgeGetResultType outside a typed bridge call";
    }
    return CurrentBridgeTable().Register(
        EBridgeNodeKind::TypeRef,
        EBridgeValueKind::Null,
        invocation->ResultType,
        {});
}

//! `pairsOff` points at 2*n handles: key, payload, key, payload, ...
//! `typeHandle` names the dict type (BridgeGetResultType or an input dict).
ui64 BridgeMakeDictHost(ui64 typeHandle, ui64 pairsOff, i32 n) {
    auto& table = CurrentBridgeTable();
    const TType* dictType = table.Resolve(typeHandle).Type;
    const auto* helper = CurrentTypeHelper();
    if (!dictType || !helper) {
        ythrow yexception() << "Bridge: BridgeMakeDict needs a typed dict node";
    }
    // The type may be Optional<Dict<..>> when it comes from a result type.
    if (const TType* item = OptionalItemTypeOf(dictType)) {
        dictType = item;
    }
    if (!TDictTypeInspector(*helper, dictType)) {
        ythrow yexception() << "Bridge: BridgeMakeDict expected a Dict type";
    }
    if (n < 0) {
        ythrow yexception() << "Bridge: BridgeMakeDict negative count";
    }

    auto values = ResolveHandleArray(pairsOff, 2 * n, "BridgeMakeDict");
    auto* builder = CurrentValueBuilderOrThrow();
    auto dictBuilder = builder->NewDict(dictType, /*flags*/ 0);
    for (i32 i = 0; i < n; ++i) {
        dictBuilder->Add(std::move(values[2 * i]), std::move(values[2 * i + 1]));
    }
    return RegisterOwned(EBridgeNodeKind::Dict, EBridgeValueKind::Dict, dictBuilder->Build());
}

ui64 BridgeRunHost(ui64 callableHandle, ui64 argsOff, i32 n) {
    if (n < 0) {
        ythrow yexception() << "Bridge: BridgeRun negative argc";
    }
    auto& query = CurrentQueryOrThrow();
    if (query.BridgeRunDepth >= MaxBridgeRunDepth) {
        ythrow yexception()
            << "Bridge: BridgeRun recursion limit exceeded (" << MaxBridgeRunDepth << ")";
    }

    const auto& callableNode = CurrentBridgeTable().Resolve(callableHandle);
    EnsureKind(callableNode, EBridgeValueKind::Callable, "BridgeRun");
    // Own copies: running the callable may register nodes and rehash the table.
    const TUnboxedValue callable = callableNode.Value;
    const TType* callableType = callableNode.Type;
    const TType* resultType = CallableResultTypeOf(callableType);
    auto* builder = CurrentValueBuilderOrThrow();
    auto* compartment = CurrentCompartmentOrThrow();

    // The callee reads exactly GetArgsCount() slots from the array we pass,
    // so a short argv would make it read past our vector.
    const auto* helper = CurrentTypeHelper();
    if (!callableType || !helper) {
        ythrow yexception() << "Bridge: BridgeRun needs a typed callable node";
    }
    const TCallableTypeInspector inspector(*helper, callableType);
    if (!inspector) {
        ythrow yexception() << "Bridge: BridgeRun expected a Callable type";
    }
    const ui32 declared = inspector.GetArgsCount();
    const ui32 required = declared - Min(declared, inspector.GetOptionalArgsCount());
    if (static_cast<ui32>(n) < required || static_cast<ui32>(n) > declared) {
        ythrow yexception()
            << "Bridge: BridgeRun got " << n << " arguments, callable takes "
            << required << ".." << declared;
    }

    // Sized by the declaration, not by the guest: omitted optional arguments
    // stay empty in the slots the callee still reads.
    TVector<TUnboxedValue> argsStorage(declared);
    TVector<TUnboxedValuePod> argsPod(declared);
    const ui64* handles = n == 0
        ? nullptr
        : PtrFromVM(compartment, std::bit_cast<ui64*>(static_cast<uintptr_t>(argsOff)), sizeof(ui64) * static_cast<size_t>(n));
    for (i32 i = 0; i < n; ++i) {
        const ui64 h = handles[i];
        if (h != NullBridgeHandle) {
            argsStorage[i] = CurrentBridgeTable().Resolve(h).Value;
        }
        argsPod[i] = argsStorage[i];
    }

    ++query.BridgeRunDepth;
    Y_DEFER {
        --query.BridgeRunDepth;
    };
    auto result = callable.Run(builder, argsPod.data());

    return RegisterChild(result, resultType);
}

i64 BridgeGetResourceTagLenHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Resource, "BridgeGetResourceTagLen");
    return static_cast<i64>(node.Value.GetResourceTag().Size());
}

i64 BridgeCopyResourceTagHost(ui64 handle, ui64 dstOff, i64 cap) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    EnsureKind(node, EBridgeValueKind::Resource, "BridgeCopyResourceTag");
    const TStringRef tag = node.Value.GetResourceTag();
    if (cap < 0 || static_cast<ui64>(cap) < tag.Size()) {
        ythrow yexception()
            << "Bridge: BridgeCopyResourceTag capacity " << cap
            << " is less than tag length " << tag.Size();
    }
    auto* compartment = CurrentCompartmentOrThrow();
    if (tag.Size() == 0) {
        return 0;
    }
    char* dst = PtrFromVM(compartment, std::bit_cast<char*>(static_cast<uintptr_t>(dstOff)), tag.Size());
    std::memcpy(dst, tag.Data(), tag.Size());
    return static_cast<i64>(tag.Size());
}

ui64 BridgeGetUserDataHost(ui64 handle) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    const void* key = BridgeIdentityKey(node.Value);
    if (!key) {
        return 0;
    }
    return CurrentResidentCache().GetUserData(key);
}

void BridgeSetUserDataHost(ui64 handle, ui64 value) {
    const auto& node = CurrentBridgeTable().Resolve(handle);
    const void* key = BridgeIdentityKey(node.Value);
    if (!key) {
        ythrow yexception()
            << "Bridge: BridgeSetUserData needs a value with identity"
               " (string or boxed), got kind " << static_cast<int>(node.ValueKind);
    }
    CurrentResidentCache().SetUserData(key, node.Value, value);
}

ui64 BridgeAllocResidentHost(ui64 length) {
    return CurrentResidentCache().AllocGuest(length);
}

void BridgeFreeResidentHost(ui64 offset) {
    CurrentResidentCache().FreeGuest(offset);
}

i32 BridgeTakeReleasedUserDataHost(ui64 dstOff, i32 cap) {
    if (cap <= 0) {
        return 0;
    }
    auto& cache = CurrentResidentCache();
    auto* compartment = CurrentCompartmentOrThrow();
    ui64* out = PtrFromVM(
        compartment,
        std::bit_cast<ui64*>(static_cast<uintptr_t>(dstOff)),
        sizeof(ui64) * static_cast<size_t>(cap));
    i32 count = 0;
    ui64 value = 0;
    while (count < cap && cache.PopReleasedUserData(value)) {
        out[count++] = value;
    }
    return count;
}

void BridgeRefHost(ui64 handle) {
    CurrentBridgeTable().Ref(handle);
}

void BridgeUnrefHost(ui64 handle) {
    CurrentBridgeTable().Unref(handle);
}

//! Every bridge intrinsic, named once. WASM_INTRINSIC turns each line into the
//! registration static WAVM picks up; BridgeIntrinsicAnchors turns the same
//! line into a reference that keeps that static in the binary. Adding an
//! intrinsic means adding one line here.
#define BRIDGE_INTRINSICS(X) \
    X(BridgeGetKind, BridgeGetKindHost, i32(ui64)) \
    X(BridgeIsNull, BridgeIsNullHost, i32(ui64)) \
    X(BridgeGetInt64, BridgeGetInt64Host, i64(ui64)) \
    X(BridgeGetUint64, BridgeGetUint64Host, ui64(ui64)) \
    X(BridgeGetInt32, BridgeGetInt32Host, i32(ui64)) \
    X(BridgeGetUint32, BridgeGetUint32Host, ui32(ui64)) \
    X(BridgeGetFloat, BridgeGetFloatHost, float(ui64)) \
    X(BridgeGetDouble, BridgeGetDoubleHost, double(ui64)) \
    X(BridgeCopyDecimal, BridgeCopyDecimalHost, void(ui64, ui64)) \
    X(BridgeGetBool, BridgeGetBoolHost, i32(ui64)) \
    X(BridgeGetStringLen, BridgeGetStringLenHost, i64(ui64)) \
    X(BridgeCopyString, BridgeCopyStringHost, i64(ui64, ui64, i64)) \
    X(BridgeEnsureString, BridgeEnsureStringHost, ui64(ui64)) \
    X(BridgeGetOptional, BridgeGetOptionalHost, ui64(ui64)) \
    X(BridgeGetElement, BridgeGetElementHost, ui64(ui64, i32)) \
    X(BridgeGetMemberCount, BridgeGetMemberCountHost, i32(ui64)) \
    X(BridgeGetMemberIndex, BridgeGetMemberIndexHost, i32(ui64, ui64, i64)) \
    X(BridgeGetVariantIndex, BridgeGetVariantIndexHost, i32(ui64)) \
    X(BridgeGetVariantItem, BridgeGetVariantItemHost, ui64(ui64)) \
    X(BridgeListLength, BridgeListLengthHost, i64(ui64)) \
    X(BridgeListHasItems, BridgeListHasItemsHost, i32(ui64)) \
    X(BridgeListMakeIterator, BridgeListMakeIteratorHost, ui64(ui64)) \
    X(BridgeListIterNext, BridgeListIterNextHost, i32(ui64, ui64*)) \
    X(BridgeDictLength, BridgeDictLengthHost, i64(ui64)) \
    X(BridgeDictHasItems, BridgeDictHasItemsHost, i32(ui64)) \
    X(BridgeDictContains, BridgeDictContainsHost, i32(ui64, ui64)) \
    X(BridgeDictLookup, BridgeDictLookupHost, ui64(ui64, ui64)) \
    X(BridgeDictMakeIterator, BridgeDictMakeIteratorHost, ui64(ui64)) \
    X(BridgeDictMakeKeysIterator, BridgeDictMakeKeysIteratorHost, ui64(ui64)) \
    X(BridgeDictMakePayloadsIterator, BridgeDictMakePayloadsIteratorHost, ui64(ui64)) \
    X(BridgeDictIterNext, BridgeDictIterNextHost, i32(ui64, ui64*, ui64*)) \
    X(BridgeMakeNull, BridgeMakeNullHost, ui64()) \
    X(BridgeMakeInt64, BridgeMakeInt64Host, ui64(i64)) \
    X(BridgeMakeUint64, BridgeMakeUint64Host, ui64(ui64)) \
    X(BridgeMakeInt32, BridgeMakeInt32Host, ui64(i32)) \
    X(BridgeMakeUint32, BridgeMakeUint32Host, ui64(ui32)) \
    X(BridgeMakeFloat, BridgeMakeFloatHost, ui64(float)) \
    X(BridgeMakeDouble, BridgeMakeDoubleHost, ui64(double)) \
    X(BridgeMakeBool, BridgeMakeBoolHost, ui64(i32)) \
    X(BridgeMakeString, BridgeMakeStringHost, ui64(ui64, i64)) \
    X(BridgeMakeOptional, BridgeMakeOptionalHost, ui64(ui64)) \
    X(BridgeMakeArray, BridgeMakeArrayHost, ui64(ui64, i32)) \
    X(BridgeMakeStruct, BridgeMakeStructHost, ui64(ui64, i32)) \
    X(BridgeMakeList, BridgeMakeListHost, ui64(ui64, i32)) \
    X(BridgeMakeVariant, BridgeMakeVariantHost, ui64(i32, ui64)) \
    X(BridgeGetResultType, BridgeGetResultTypeHost, ui64()) \
    X(BridgeMakeDict, BridgeMakeDictHost, ui64(ui64, ui64, i32)) \
    X(BridgeRun, BridgeRunHost, ui64(ui64, ui64, i32)) \
    X(BridgeGetResourceTagLen, BridgeGetResourceTagLenHost, i64(ui64)) \
    X(BridgeCopyResourceTag, BridgeCopyResourceTagHost, i64(ui64, ui64, i64)) \
    X(BridgeGetUserData, BridgeGetUserDataHost, ui64(ui64)) \
    X(BridgeSetUserData, BridgeSetUserDataHost, void(ui64, ui64)) \
    X(BridgeAllocResident, BridgeAllocResidentHost, ui64(ui64)) \
    X(BridgeFreeResident, BridgeFreeResidentHost, void(ui64)) \
    X(BridgeTakeReleasedUserData, BridgeTakeReleasedUserDataHost, i32(ui64, i32)) \
    X(BridgeRef, BridgeRefHost, void(ui64)) \
    X(BridgeUnref, BridgeUnrefHost, void(ui64))

BRIDGE_INTRINSICS(WASM_INTRINSIC)

#define BRIDGE_INTRINSIC_ADDRESS(ExportName, Fn, Signature) &IntrinsicFunction##ExportName,
WAVM::Intrinsics::Function* const BridgeIntrinsicAnchors[] = {
    BRIDGE_INTRINSICS(BRIDGE_INTRINSIC_ADDRESS)
};
#undef BRIDGE_INTRINSIC_ADDRESS

#undef BRIDGE_INTRINSICS

//! Registration happens in the static initializers above, which only run if
//! this object file makes it into the binary at all. [[gnu::used]] keeps the
//! compiler from stripping the symbols but says nothing about what the linker
//! pulls out of the archive, so the cross-object call into
//! KeepBridgeHostIntrinsicsLinked is what actually anchors them. The volatile
//! read keeps the array itself from being optimized into nothing.
void KeepBridgeIntrinsicsLinked() {
    [[maybe_unused]] WAVM::Intrinsics::Function* volatile anchor = BridgeIntrinsicAnchors[0];
}

} // namespace

void KeepBridgeHostIntrinsicsLinked() {
    KeepBridgeIntrinsicsLinked();
}

} // namespace NKikimr::NUdfStore::NWasm
