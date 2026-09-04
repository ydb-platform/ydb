#include "manifest.h"
#include "registry_helpers.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <memory>

namespace NKikimr::NUdfStore::NWasm {

namespace {

void ValidateConcreteTag(const NJson::TJsonValue& block, TStringBuf where) {
    if (block.Has("tag")) {
        const auto tag = block["tag"].GetString();
        if (tag != "concrete_type") {
            ythrow yexception()
                << "Only tag=concrete_type is supported in wasm manifest, got tag="
                << tag << " in " << where;
        }
    }
}

EUdfValueType LeafFromTypeNode(const TWasmTypeNode& node) {
    if (node.Kind != TWasmTypeNode::EKind::Leaf) {
        return EUdfValueType::Null;
    }
    return node.Leaf;
}

//! Caps nesting of optional/list/dict/... so a hostile manifest cannot blow
//! the stack in the parser, in BuildTypeFromWasmTypeNode, or while destroying
//! the shared_ptr chain. Sibling FindVariantTypeIn walks wrappers at depth 8;
//! this is the parse-time bound for the whole tree.
constexpr ui32 MaxManifestTypeDepth = 32;

TWasmTypeNodePtr ParseTypeNode(
    const NJson::TJsonValue& valueNode,
    TStringBuf where,
    ui32 depth = 0);

//! Elements of a tuple / members of a struct or variant. Named forms carry
//! {"name": ..., "type": {...}}; unnamed ones are plain type nodes.
TVector<TWasmTypeNode::TMember> ParseTypeMembers(
    const NJson::TJsonValue& valueNode,
    TStringBuf field,
    bool named,
    TStringBuf where,
    ui32 depth)
{
    if (!valueNode.Has(field) || !valueNode[field].IsArray()) {
        ythrow yexception() << where << " type requires an array of " << field;
    }
    TVector<TWasmTypeNode::TMember> members;
    for (const auto& entry : valueNode[field].GetArray()) {
        TWasmTypeNode::TMember member;
        if (named) {
            if (!entry.IsMap() || !entry.Has("name") || !entry.Has("type")) {
                ythrow yexception() << where << " member requires name and type";
            }
            member.Name = entry["name"].GetString();
            member.Type = ParseTypeNode(entry["type"], where, depth);
        } else {
            member.Type = ParseTypeNode(entry, where, depth);
        }
        members.push_back(std::move(member));
    }
    return members;
}

TWasmTypeNodePtr ParseTypeNode(
    const NJson::TJsonValue& valueNode,
    TStringBuf where,
    ui32 depth)
{
    if (depth > MaxManifestTypeDepth) {
        ythrow yexception()
            << "Type nesting exceeds " << MaxManifestTypeDepth
            << " levels in " << where;
    }
    if (!valueNode.IsMap()) {
        ythrow yexception() << "Expected object for typed value in " << where;
    }
    ValidateConcreteTag(valueNode, where);
    if (!valueNode.Has("value")) {
        ythrow yexception() << "Missing value field in " << where;
    }

    const auto valueStr = valueNode["value"].GetString();
    if (valueStr == "optional") {
        if (!valueNode.Has("item")) {
            ythrow yexception() << "optional type requires item in " << where;
        }
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::Optional;
        node->Item = ParseTypeNode(valueNode["item"], "optional.item", depth + 1);
        return node;
    }
    if (valueStr == "list") {
        if (!valueNode.Has("item")) {
            ythrow yexception() << "list type requires item in " << where;
        }
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::List;
        node->Item = ParseTypeNode(valueNode["item"], "list.item", depth + 1);
        return node;
    }
    if (valueStr == "dict") {
        if (!valueNode.Has("key") || !valueNode.Has("payload")) {
            ythrow yexception() << "dict type requires key and payload in " << where;
        }
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::Dict;
        node->Key = ParseTypeNode(valueNode["key"], "dict.key", depth + 1);
        node->Payload = ParseTypeNode(valueNode["payload"], "dict.payload", depth + 1);
        return node;
    }
    if (valueStr == "tuple") {
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::Tuple;
        node->Members = ParseTypeMembers(
            valueNode, "elements", /*named*/ false, "tuple", depth + 1);
        return node;
    }
    if (valueStr == "struct") {
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::Struct;
        node->Members = ParseTypeMembers(
            valueNode, "members", /*named*/ true, "struct", depth + 1);
        return node;
    }
    if (valueStr == "variant") {
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::Variant;
        if (valueNode.Has("members")) {
            node->Members = ParseTypeMembers(
                valueNode, "members", /*named*/ true, "variant", depth + 1);
        } else {
            node->Members = ParseTypeMembers(
                valueNode, "elements", /*named*/ false, "variant", depth + 1);
        }
        return node;
    }
    if (valueStr == "resource") {
        // "tag" is taken by the concrete_type marker, hence "resource_tag".
        if (!valueNode.Has("resource_tag")) {
            ythrow yexception() << "resource type requires resource_tag in " << where;
        }
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::Resource;
        node->Tag = valueNode["resource_tag"].GetString();
        return node;
    }
    if (valueStr == "callable") {
        if (!valueNode.Has("arguments") || !valueNode.Has("returns")) {
            ythrow yexception() << "callable type requires arguments and returns in " << where;
        }
        auto node = std::make_shared<TWasmTypeNode>();
        node->Kind = TWasmTypeNode::EKind::Callable;
        node->Members = ParseTypeMembers(
            valueNode, "arguments", /*named*/ false, "callable", depth + 1);
        node->CallableReturns = ParseTypeNode(
            valueNode["returns"], "callable.returns", depth + 1);
        return node;
    }

    return MakeLeafTypeNode(ParseValueType(valueStr));
}

TVector<TWasmTypeNodePtr> ParseArgumentTypeNodes(const NJson::TJsonValue& node) {
    TVector<TWasmTypeNodePtr> result;
    if (!node.Has("argument_types")) {
        return result;
    }
    const auto& args = node["argument_types"];
    if (!args.IsArray()) {
        ythrow yexception() << "argument_types must be an array in wasm manifest";
    }
    for (const auto& arg : args.GetArray()) {
        result.push_back(ParseTypeNode(arg, "argument_types"));
    }
    return result;
}

EWasmUdfBinding ParseBinding(const NJson::TJsonValue& node) {
    if (!node.Has("yql_binding")) {
        return EWasmUdfBinding::Plain;
    }
    const auto binding = node["yql_binding"].GetString();
    if (binding == "plain") {
        return EWasmUdfBinding::Plain;
    }
    if (binding == "type_config_callable") {
        return EWasmUdfBinding::TypeConfigCallable;
    }
    ythrow yexception() << "Unsupported yql_binding in wasm manifest: " << binding;
}

//! Manifest spelling of a parsed type node, for error messages.
TString DescribeTypeNode(const TWasmTypeNode& node) {
    switch (node.Kind) {
        case TWasmTypeNode::EKind::Leaf:
            return ValueTypeToString(node.Leaf);
        case TWasmTypeNode::EKind::Optional:
            return "optional";
        case TWasmTypeNode::EKind::List:
            return "list";
        case TWasmTypeNode::EKind::Dict:
            return "dict";
        case TWasmTypeNode::EKind::Tuple:
            return "tuple";
        case TWasmTypeNode::EKind::Struct:
            return "struct";
        case TWasmTypeNode::EKind::Variant:
            return "variant";
        case TWasmTypeNode::EKind::Resource:
            return "resource";
        case TWasmTypeNode::EKind::Callable:
            return "callable";
    }
    return "unknown";
}

//! The unversioned_value convention passes each value in a TUnversionedValue,
//! which has slots for exactly six scalar types and no way to name a
//! container. Rejecting the rest here turns what used to be a mid-query
//! failure into a manifest error at registration time.
void ValidateCallingConventionTypes(
    const TWasmTypeNode& node,
    EWasmCallingConvention cc,
    TStringBuf functionName,
    TStringBuf where)
{
    if (cc != EWasmCallingConvention::UnversionedValue) {
        return;
    }
    if (node.Kind != TWasmTypeNode::EKind::Leaf || !IsUnversionedValueType(node.Leaf)) {
        ythrow yexception()
            << "calling_convention=" << CallingConventionAsStr(cc)
            << " cannot pass " << DescribeTypeNode(node) << " in " << where
            << " of function '" << functionName
            << "'; use calling_convention="
            << CallingConventionAsStr(EWasmCallingConvention::Bridge);
    }
}

void ValidateCallingConventionSignature(
    const TVector<TWasmTypeNodePtr>& argTypes,
    const TWasmTypeNodePtr& resultType,
    EWasmCallingConvention cc,
    TStringBuf functionName)
{
    for (size_t i = 0; i < argTypes.size(); ++i) {
        ValidateCallingConventionTypes(
            *argTypes[i],
            cc,
            functionName,
            TStringBuilder() << "argument_types[" << i << "]");
    }
    if (resultType) {
        ValidateCallingConventionTypes(*resultType, cc, functionName, "result_type");
    }
}

EWasmCallingConvention ParseCallingConvention(TStringBuf value) {
    if (value == "unversioned_value") {
        return EWasmCallingConvention::UnversionedValue;
    }
    if (value == "bridge") {
        return EWasmCallingConvention::Bridge;
    }
    ythrow yexception()
        << "Unsupported calling_convention=" << value
        << " (supported: unversioned_value, bridge)";
}

TWasmUdfDescriptor ParseFunctionDescriptor(
    const NJson::TJsonValue& functionNode,
    EWasmCallingConvention moduleCc)
{
    if (!functionNode.IsMap()) {
        ythrow yexception() << "Each function entry in wasm manifest must be an object";
    }
    if (!functionNode.Has("name")) {
        ythrow yexception() << "Missing function name in wasm manifest";
    }
    if (!functionNode.Has("result_type")) {
        ythrow yexception() << "Missing result_type in wasm manifest";
    }

    TWasmUdfDescriptor descriptor;
    descriptor.Name = functionNode["name"].GetString();
    descriptor.ArgTypes = ParseArgumentTypeNodes(functionNode);
    descriptor.Args.reserve(descriptor.ArgTypes.size());
    for (const auto& argType : descriptor.ArgTypes) {
        descriptor.Args.push_back(LeafFromTypeNode(*argType));
    }
    descriptor.ResultType = ParseTypeNode(functionNode["result_type"], "result_type");
    descriptor.Result = LeafFromTypeNode(*descriptor.ResultType);
    descriptor.Binding = ParseBinding(functionNode);
    descriptor.CallingConvention = moduleCc;
    if (functionNode.Has("calling_convention")) {
        descriptor.CallingConvention = ParseCallingConvention(
            functionNode["calling_convention"].GetString());
    }
    if (functionNode.Has("export")) {
        descriptor.ExportName = functionNode["export"].GetString();
    }
    if (descriptor.Binding == EWasmUdfBinding::TypeConfigCallable) {
        ythrow yexception()
            << "type_config_callable is only supported under objects[].methods, not functions[]";
    }
    if (descriptor.CallingConvention == EWasmCallingConvention::Bridge
        && descriptor.Binding == EWasmUdfBinding::TypeConfigCallable)
    {
        ythrow yexception()
            << "calling_convention=bridge is incompatible with type_config_callable"
            << " (function '" << descriptor.Name << "')";
    }
    ValidateCallingConventionSignature(
        descriptor.ArgTypes,
        descriptor.ResultType,
        descriptor.CallingConvention,
        descriptor.Name);
    return descriptor;
}

TWasmObjectMethodDescriptor ParseObjectMethod(const NJson::TJsonValue& methodNode) {
    if (!methodNode.IsMap()) {
        ythrow yexception() << "Each objects[].methods entry must be an object";
    }
    if (!methodNode.Has("name")) {
        ythrow yexception() << "Missing method name in objects[].methods";
    }
    if (!methodNode.Has("export")) {
        ythrow yexception() << "Missing export in objects[].methods";
    }
    if (!methodNode.Has("result_type")) {
        ythrow yexception() << "Missing result_type in objects[].methods";
    }

    TWasmObjectMethodDescriptor method;
    method.Name = methodNode["name"].GetString();
    method.Export = methodNode["export"].GetString();
    method.ArgTypes = ParseArgumentTypeNodes(methodNode);
    method.Args.reserve(method.ArgTypes.size());
    for (const auto& argType : method.ArgTypes) {
        method.Args.push_back(LeafFromTypeNode(*argType));
    }
    method.ResultType = ParseTypeNode(methodNode["result_type"], "objects[].methods.result_type");
    method.Result = LeafFromTypeNode(*method.ResultType);
    method.Binding = methodNode.Has("yql_binding")
        ? ParseBinding(methodNode)
        : EWasmUdfBinding::TypeConfigCallable;
    return method;
}

TWasmObjectDescriptor ParseObjectDescriptor(const NJson::TJsonValue& objectNode) {
    if (!objectNode.IsMap()) {
        ythrow yexception() << "Each objects[] entry must be an object";
    }
    if (!objectNode.Has("name")) {
        ythrow yexception() << "Missing objects[].name";
    }
    if (!objectNode.Has("create_export")) {
        ythrow yexception() << "Missing objects[].create_export";
    }
    if (!objectNode.Has("methods") || !objectNode["methods"].IsArray()
        || objectNode["methods"].GetArray().empty())
    {
        ythrow yexception() << "objects[].methods must be a non-empty array";
    }

    TWasmObjectDescriptor object;
    object.Name = objectNode["name"].GetString();
    object.CreateExport = objectNode["create_export"].GetString();
    if (objectNode.Has("destroy_export")) {
        object.DestroyExport = objectNode["destroy_export"].GetString();
    }
    for (const auto& methodNode : objectNode["methods"].GetArray()) {
        object.Methods.push_back(ParseObjectMethod(methodNode));
    }
    return object;
}

void ExpandObjectsIntoFunctions(
    TWasmManifest& manifest,
    EWasmCallingConvention moduleCc)
{
    THashSet<TString> knownNames;
    for (const auto& function : manifest.Functions) {
        knownNames.insert(function.Name);
    }

    for (const auto& object : manifest.Objects) {
        if (!object.CreateExport.empty()) {
            // Prefer "New" for the first free slot (backward compatible).
            // Additional objects get New{ObjectName} so each stays YQL-visible.
            TString ctorName = "New";
            if (!knownNames.insert(ctorName).second) {
                ctorName = TString("New") + object.Name;
                if (!knownNames.insert(ctorName).second) {
                    ythrow yexception()
                        << "Cannot synthesize constructor for object '" << object.Name
                        << "': YQL names 'New' and '" << ctorName << "' are already taken";
                }
            }
            TWasmUdfDescriptor createFn;
            createFn.Name = std::move(ctorName);
            createFn.ExportName = object.CreateExport;
            createFn.Binding = EWasmUdfBinding::Plain;
            createFn.CallingConvention = EWasmCallingConvention::UnversionedValue;
            createFn.Result = EUdfValueType::Uint64;
            createFn.ResultType = MakeLeafTypeNode(EUdfValueType::Uint64);
            createFn.Args = {};
            manifest.Functions.push_back(std::move(createFn));
        }

        for (const auto& method : object.Methods) {
            if (!knownNames.insert(method.Name).second) {
                ythrow yexception()
                    << "Duplicate YQL function name '" << method.Name
                    << "' from objects[].methods (names must be unique across functions/objects)";
            }
            if (moduleCc == EWasmCallingConvention::Bridge
                && method.Binding == EWasmUdfBinding::TypeConfigCallable)
            {
                ythrow yexception()
                    << "calling_convention=bridge is incompatible with type_config_callable"
                    << " (method '" << method.Name << "')";
            }
            TWasmUdfDescriptor descriptor;
            descriptor.Name = method.Name;
            descriptor.Args = method.Args;
            descriptor.Result = method.Result;
            descriptor.ArgTypes = method.ArgTypes;
            descriptor.ResultType = method.ResultType
                ? method.ResultType
                : MakeLeafTypeNode(method.Result);
            descriptor.Binding = method.Binding;
            // Object methods always ride unversioned_value, even when the
            // module's calling_convention is bridge: the create/call/destroy
            // object framework talks TUnversionedValue, not bridge handles.
            // type_config_callable is rejected above for the same reason;
            // plain methods are accepted and stay on this convention, so a
            // wide type in objects[].methods is still a hard error below.
            descriptor.CallingConvention = EWasmCallingConvention::UnversionedValue;
            descriptor.CreateExport = object.CreateExport;
            descriptor.CallExport = method.Export;
            descriptor.DestroyExport = object.DestroyExport;
            if (descriptor.Binding == EWasmUdfBinding::Plain) {
                descriptor.ExportName = method.Export;
            }
            if (descriptor.Binding == EWasmUdfBinding::TypeConfigCallable
                && descriptor.CreateExport.empty())
            {
                ythrow yexception()
                    << "type_config_callable method '" << method.Name
                    << "' requires objects[].create_export";
            }
            ValidateCallingConventionSignature(
                descriptor.ArgTypes,
                descriptor.ResultType,
                descriptor.CallingConvention,
                descriptor.Name);
            manifest.Functions.push_back(std::move(descriptor));
        }
    }
}

} // namespace

TWasmManifest ParseManifest(TStringBuf manifestJson) {
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(manifestJson, &root, true)) {
        ythrow yexception() << "Failed to parse wasm manifest JSON";
    }
    if (!root.IsMap()) {
        ythrow yexception() << "Wasm manifest must be a JSON object";
    }
    if (!root.Has("module_name")) {
        ythrow yexception() << "Wasm manifest is missing module_name";
    }

    TWasmManifest manifest;
    manifest.ModuleName = root["module_name"].GetString();
    manifest.ModuleExtension = root.Has("module_extension")
        ? root["module_extension"].GetString()
        : TString("wasm");
    manifest.CallingConvention = root.Has("calling_convention")
        ? root["calling_convention"].GetString()
        : TString("unversioned_value");
    manifest.CallingConventionEnum = ParseCallingConvention(manifest.CallingConvention);

    if (root.Has("required_libraries")) {
        const auto& libraries = root["required_libraries"];
        if (!libraries.IsArray()) {
            ythrow yexception() << "required_libraries must be an array in wasm manifest";
        }
        for (const auto& libraryNode : libraries.GetArray()) {
            if (!libraryNode.IsString()) {
                ythrow yexception() << "Each required_libraries entry must be a string";
            }
            manifest.RequiredLibraries.push_back(libraryNode.GetString());
        }
    }

    if (root.Has("functions")) {
        const auto& functions = root["functions"];
        if (!functions.IsArray()) {
            ythrow yexception() << "Wasm manifest functions must be an array";
        }
        for (const auto& functionNode : functions.GetArray()) {
            manifest.Functions.push_back(
                ParseFunctionDescriptor(functionNode, manifest.CallingConventionEnum));
        }
    }

    if (root.Has("objects")) {
        const auto& objects = root["objects"];
        if (!objects.IsArray()) {
            ythrow yexception() << "Wasm manifest objects must be an array";
        }
        for (const auto& objectNode : objects.GetArray()) {
            manifest.Objects.push_back(ParseObjectDescriptor(objectNode));
        }
        ExpandObjectsIntoFunctions(manifest, manifest.CallingConventionEnum);
    }

    if (manifest.Functions.empty()) {
        ythrow yexception()
            << "Wasm manifest must declare non-empty functions[] and/or objects[]";
    }
    return manifest;
}

} // namespace NKikimr::NUdfStore::NWasm
