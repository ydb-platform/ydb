#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NKikimr::NUdfStore::NWasm {

enum class EUdfValueType {
    Null,
    Int64,
    Uint64,
    Double,
    Boolean,
    String,
    //! Bridge only: the unversioned_value convention cannot carry these.
    Int32,
    Uint32,
    Float,
    Utf8,
    Date,
    Datetime,
    Timestamp,
    Decimal,
};

//! Types the legacy unversioned_value convention can pass in a TUnversionedValue.
inline bool IsUnversionedValueType(EUdfValueType type) {
    switch (type) {
        case EUdfValueType::Null:
        case EUdfValueType::Int64:
        case EUdfValueType::Uint64:
        case EUdfValueType::Double:
        case EUdfValueType::Boolean:
        case EUdfValueType::String:
            return true;
        default:
            return false;
    }
}

enum class EWasmUdfBinding {
    Plain,
    TypeConfigCallable,
};

enum class EWasmCallingConvention {
    UnversionedValue,
    Bridge,
};

//! Recursive type descriptor for bridge (and future) calling conventions.
//! Leaf kinds reuse EUdfValueType; the rest nest children.
struct TWasmTypeNode {
    enum class EKind {
        Leaf,
        Optional,
        List,
        Dict,
        Tuple,
        Struct,
        Variant,
        Resource,
        Callable,
    };

    struct TMember {
        TString Name;
        std::shared_ptr<TWasmTypeNode> Type;
    };

    EKind Kind = EKind::Leaf;
    EUdfValueType Leaf = EUdfValueType::Null;
    std::shared_ptr<TWasmTypeNode> Item;      // Optional / List
    std::shared_ptr<TWasmTypeNode> Key;       // Dict
    std::shared_ptr<TWasmTypeNode> Payload;   // Dict
    //! Tuple elements (unnamed) or Struct / Variant-over-Struct members.
    TVector<TMember> Members;
    //! Resource tag.
    TString Tag;
    //! Callable return type (`returns` in manifest).
    std::shared_ptr<TWasmTypeNode> CallableReturns;
};

using TWasmTypeNodePtr = std::shared_ptr<TWasmTypeNode>;

inline TWasmTypeNodePtr MakeLeafTypeNode(EUdfValueType leaf) {
    auto node = std::make_shared<TWasmTypeNode>();
    node->Kind = TWasmTypeNode::EKind::Leaf;
    node->Leaf = leaf;
    return node;
}

struct TWasmUdfDescriptor {
    TString Name;
    TVector<EUdfValueType> Args;
    EUdfValueType Result = EUdfValueType::Null;
    //! Structured types for bridge CC (parallel to Args/Result when set).
    TVector<TWasmTypeNodePtr> ArgTypes;
    TWasmTypeNodePtr ResultType;
    EWasmUdfBinding Binding = EWasmUdfBinding::Plain;
    EWasmCallingConvention CallingConvention = EWasmCallingConvention::UnversionedValue;
    // For TypeConfigCallable: create/call/destroy exports (destroy optional).
    TString CreateExport;
    TString CallExport;
    TString DestroyExport;
    // For Plain: wasm export if different from Name (also set for plain object methods).
    TString ExportName;
};

//! Wasm export invoked for a plain binding (ExportName / CallExport / Name).
inline TStringBuf PlainWasmExport(const TWasmUdfDescriptor& descriptor) {
    if (!descriptor.ExportName.empty()) {
        return descriptor.ExportName;
    }
    if (!descriptor.CallExport.empty()) {
        return descriptor.CallExport;
    }
    return descriptor.Name;
}

struct TWasmObjectMethodDescriptor {
    TString Name;
    TString Export;
    EWasmUdfBinding Binding = EWasmUdfBinding::TypeConfigCallable;
    TVector<EUdfValueType> Args;
    EUdfValueType Result = EUdfValueType::Null;
    TVector<TWasmTypeNodePtr> ArgTypes;
    TWasmTypeNodePtr ResultType;
};

struct TWasmObjectDescriptor {
    TString Name;
    TString CreateExport;
    TString DestroyExport;
    TVector<TWasmObjectMethodDescriptor> Methods;
};

struct TWasmManifest {
    TString ModuleName;
    TString ModuleExtension;
    TString CallingConvention;
    EWasmCallingConvention CallingConventionEnum = EWasmCallingConvention::UnversionedValue;
    TVector<TString> RequiredLibraries;
    TVector<TWasmUdfDescriptor> Functions;
    TVector<TWasmObjectDescriptor> Objects;
};

inline TStringBuf CallingConventionAsStr(EWasmCallingConvention cc) {
    switch (cc) {
        case EWasmCallingConvention::UnversionedValue:
            return "unversioned_value";
        case EWasmCallingConvention::Bridge:
            return "bridge";
    }
    return "unknown";
}

} // namespace NKikimr::NUdfStore::NWasm
