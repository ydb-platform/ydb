#pragma once

#include "bridge_resident.h"
#include "bridge_types.h"

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

#include <util/generic/hash.h>
#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr::NUdfStore::NWasm {

//! Stable identity of a value for cross-row reuse: boxed object pointer or
//! refcounted string buffer (TStringValue::TData*). Returns nullptr for values
//! without identity (embedded strings, plain scalars, empty values).
const void* BridgeIdentityKey(const NYql::NUdf::TUnboxedValuePod& value);

struct TBridgeKinds {
    EBridgeNodeKind Node = EBridgeNodeKind::Unknown;
    EBridgeValueKind Value = EBridgeValueKind::Null;
};

//! What the guest should see for a value of this MiniKQL type. Optional and
//! Tagged wrappers around data are transparent: MiniKQL represents them like
//! the payload, and so does the bridge.
TBridgeKinds BridgeKindsFromType(
    const NYql::NUdf::TType* type,
    const NYql::NUdf::ITypeInfoHelper* helper);

//! Last-resort kinds for values registered without a type: all we can tell is
//! the runtime representation.
TBridgeKinds BridgeKindsFromValue(const NYql::NUdf::TUnboxedValuePod& value);

//! Per-query-compartment table of host TUnboxedValue nodes exposed to WASM
//! as ui64 bridge handles. Lifetime equals the query compartment generation.
class TWasmBridgeNodeTable: public TNonCopyable {
public:
    struct TNode {
        EBridgeNodeKind Kind = EBridgeNodeKind::Unknown;
        EBridgeValueKind ValueKind = EBridgeValueKind::Null;
        const NYql::NUdf::TType* Type = nullptr;
        NYql::NUdf::TUnboxedValue Value;
        ui32 Refs = 1;
        //! For iterators: parent list/dict type (item / key+payload).
        const NYql::NUdf::TType* AuxType = nullptr;
        //! This node owns the Identity_ entry for its value and must erase it
        //! when destroyed. Aliasing nodes (e.g. Optional over the same pod)
        //! leave the entry to its owner.
        bool OwnsIdentity = false;
    };

    explicit TWasmBridgeNodeTable(ui64 generation);

    ui64 Generation() const {
        return Generation_;
    }

    //! Register a fresh node. Returns packed handle. The first node registered
    //! for a value with a BridgeIdentityKey owns its Identity_ entry, so a
    //! later TryReuse returns that handle. Inside a Run scope the returned
    //! handle is tracked and released when the scope closes.
    ui64 Register(
        EBridgeNodeKind kind,
        EBridgeValueKind valueKind,
        const NYql::NUdf::TType* type,
        NYql::NUdf::TUnboxedValue&& value,
        const NYql::NUdf::TType* auxType = nullptr);

    //! Reuse the node already registered for this value's identity (bumping
    //! its ref count) or register a fresh one. Preferred entry point: two
    //! nodes for one identity mean the resident cache is keyed twice.
    ui64 RegisterOrReuse(
        EBridgeNodeKind kind,
        EBridgeValueKind valueKind,
        const NYql::NUdf::TType* type,
        const NYql::NUdf::TUnboxedValuePod& value,
        const NYql::NUdf::TType* auxType = nullptr);

    //! If `value` is a boxed object or refcounted string already registered
    //! in this table, return its existing handle; otherwise NullBridgeHandle.
    ui64 TryReuse(const NYql::NUdf::TUnboxedValuePod& value) const;

    //! Resolve handle → node. Throws on null / stale generation / unknown index.
    TNode& Resolve(ui64 handle);
    const TNode& Resolve(ui64 handle) const;

    void Ref(ui64 handle);
    void Unref(ui64 handle);

    //! Open a scope for one Run. Every handle Register / RegisterOrReuse
    //! hands out while the scope is open carries a host-owned ref that
    //! EndRunScope drops, so a guest that never calls BridgeUnref cannot leak
    //! nodes past the row. A guest that wants a handle to outlive the row
    //! takes its own ref with BridgeRef. Scopes nest: a BridgeRun callback
    //! opens its own, and closing it leaves the outer scope's handles alone.
    void BeginRunScope();
    void EndRunScope() noexcept;

    size_t DebugSize() const {
        return Nodes_.size();
    }

    size_t DebugRunScopeDepth() const {
        return RunScopes_.size();
    }

    void SetValueBuilder(const NYql::NUdf::IValueBuilder* builder) {
        ValueBuilder_ = builder;
    }

    const NYql::NUdf::IValueBuilder* GetValueBuilder() const {
        return ValueBuilder_;
    }

    void SetTypeInfoHelper(NYql::NUdf::ITypeInfoHelper::TPtr helper) {
        TypeInfoHelper_ = std::move(helper);
    }

    const NYql::NUdf::ITypeInfoHelper* GetTypeInfoHelper() const {
        return TypeInfoHelper_.Get();
    }

private:
    ui64 AllocateIndex();
    TNode& ResolveIndex(ui64 index);
    const TNode& ResolveIndex(ui64 index) const;
    void EnsureHandle(ui64 handle) const;

    ui64 RegisterUntracked(
        EBridgeNodeKind kind,
        EBridgeValueKind valueKind,
        const NYql::NUdf::TType* type,
        NYql::NUdf::TUnboxedValue&& value,
        const NYql::NUdf::TType* auxType);

    //! Remember that the innermost Run scope owes an Unref on this handle.
    ui64 TrackInRunScope(ui64 handle);
    //! Unref a tracked handle, tolerating a node the guest already released:
    //! indices are never reused, so a missing one cannot be a fresh node.
    void ReleaseTracked(ui64 handle) noexcept;

    const ui64 Generation_;
    ui64 NextIndex_ = 1; // 0 reserved (would collide with null when generation packs)
    THashMap<ui64, TNode> Nodes_;
    //! Identity for IsBoxed() objects and IsString() buffers (TStringValue::TData*).
    THashMap<const void*, ui64> Identity_;
    //! Handles owed an Unref, innermost Run scope last. Holding a handle per
    //! node created in a row costs one ui64 per node, but bounds the sweep to
    //! the nodes this row actually made instead of every live node.
    TVector<TVector<ui64>> RunScopes_;
    const NYql::NUdf::IValueBuilder* ValueBuilder_ = nullptr;
    NYql::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
};

//! Offset of the node's String bytes in compartment linear memory, copying
//! them there on first use. Values with identity (refcounted strings) are
//! pinned and reused across rows; the rest go to per-Run scratch.
//! The offset is valid until the end of the current Run.
ui64 EnsureBridgeStringResident(
    const TWasmBridgeNodeTable::TNode& node,
    TCompartmentResidentCache& cache);

//! RAII: install ValueBuilder on the table for the duration of a bridge Run.
class TBridgeValueBuilderGuard: public TNonCopyable {
public:
    TBridgeValueBuilderGuard(TWasmBridgeNodeTable& table, const NYql::NUdf::IValueBuilder* builder)
        : Table_(table)
        , Previous_(table.GetValueBuilder())
    {
        Table_.SetValueBuilder(builder);
    }

    ~TBridgeValueBuilderGuard() {
        Table_.SetValueBuilder(Previous_);
    }

private:
    TWasmBridgeNodeTable& Table_;
    const NYql::NUdf::IValueBuilder* Previous_;
};

//! RAII: open a Run scope on the table, releasing everything the guest
//! registered through it when the Run leaves (normally or by exception).
class TBridgeRunScopeGuard: public TNonCopyable {
public:
    explicit TBridgeRunScopeGuard(TWasmBridgeNodeTable& table)
        : Table_(table)
    {
        Table_.BeginRunScope();
    }

    ~TBridgeRunScopeGuard() {
        Table_.EndRunScope();
    }

private:
    TWasmBridgeNodeTable& Table_;
};

} // namespace NKikimr::NUdfStore::NWasm
