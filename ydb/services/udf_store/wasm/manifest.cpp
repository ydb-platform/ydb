#include "manifest.h"
#include "registry_helpers.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>

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

EUdfValueType ParseTypedValue(const NJson::TJsonValue& valueNode, TStringBuf where) {
    if (!valueNode.IsMap()) {
        ythrow yexception() << "Expected object for typed value in " << where;
    }
    ValidateConcreteTag(valueNode, where);
    if (!valueNode.Has("value")) {
        ythrow yexception() << "Missing value field in " << where;
    }
    return ParseValueType(valueNode["value"].GetString());
}

TVector<EUdfValueType> ParseArgumentTypes(const NJson::TJsonValue& node) {
    TVector<EUdfValueType> result;
    if (!node.Has("argument_types")) {
        return result;
    }
    const auto& args = node["argument_types"];
    if (!args.IsArray()) {
        ythrow yexception() << "argument_types must be an array in wasm manifest";
    }
    for (const auto& arg : args.GetArray()) {
        result.push_back(ParseTypedValue(arg, "argument_types"));
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

TWasmUdfDescriptor ParseFunctionDescriptor(const NJson::TJsonValue& functionNode) {
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
    descriptor.Args = ParseArgumentTypes(functionNode);
    descriptor.Result = ParseTypedValue(functionNode["result_type"], "result_type");
    descriptor.Binding = ParseBinding(functionNode);
    if (functionNode.Has("export")) {
        descriptor.ExportName = functionNode["export"].GetString();
    }
    if (descriptor.Binding == EWasmUdfBinding::TypeConfigCallable) {
        ythrow yexception()
            << "type_config_callable is only supported under objects[].methods, not functions[]";
    }
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
    method.Args = ParseArgumentTypes(methodNode);
    method.Result = ParseTypedValue(methodNode["result_type"], "objects[].methods.result_type");
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

void ExpandObjectsIntoFunctions(TWasmManifest& manifest) {
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
            createFn.Result = EUdfValueType::Uint64;
            createFn.Args = {};
            manifest.Functions.push_back(std::move(createFn));
        }

        for (const auto& method : object.Methods) {
            if (!knownNames.insert(method.Name).second) {
                ythrow yexception()
                    << "Duplicate YQL function name '" << method.Name
                    << "' from objects[].methods (names must be unique across functions/objects)";
            }
            TWasmUdfDescriptor descriptor;
            descriptor.Name = method.Name;
            descriptor.Args = method.Args;
            descriptor.Result = method.Result;
            descriptor.Binding = method.Binding;
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
    if (manifest.CallingConvention != "unversioned_value") {
        ythrow yexception()
            << "Only calling_convention=unversioned_value is supported, got "
            << manifest.CallingConvention;
    }
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
            manifest.Functions.push_back(ParseFunctionDescriptor(functionNode));
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
        ExpandObjectsIntoFunctions(manifest);
    }

    if (manifest.Functions.empty()) {
        ythrow yexception()
            << "Wasm manifest must declare non-empty functions[] and/or objects[]";
    }
    return manifest;
}

} // namespace NKikimr::NUdfStore::NWasm
