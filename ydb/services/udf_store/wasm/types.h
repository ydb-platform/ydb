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
    Int32,
    Uint32,
    Float,
    Utf8,
    Date,
    Datetime,
    Timestamp,
    Decimal,
};

enum class EWasmUdfBinding {
    Plain,
    TypeConfigCallable,
};

//! Owned recursive descriptor of an exact YQL type.
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
    ui8 Precision = 0;
    ui8 Scale = 0;
    bool NamedVariant = false;
    std::shared_ptr<TWasmTypeNode> Item;      // Optional / List
    std::shared_ptr<TWasmTypeNode> Key;       // Dict
    std::shared_ptr<TWasmTypeNode> Payload;   // Dict
    //! Tuple elements (unnamed) or Struct / Variant-over-Struct members.
    TVector<TMember> Members;
    //! Resource tag.
    TString Tag;
    //! Callable return type.
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
    TVector<TWasmTypeNodePtr> ArgTypes;
    TWasmTypeNodePtr ResultType;
    EWasmUdfBinding Binding = EWasmUdfBinding::Plain;
    bool IsObjectConstructor = false;
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
    TVector<TString> RequiredLibraries;
    TVector<TWasmUdfDescriptor> Functions;
    TVector<TWasmObjectDescriptor> Objects;
};

} // namespace NKikimr::NUdfStore::NWasm
