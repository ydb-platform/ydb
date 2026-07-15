#include "manifest.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/yexception.h>

namespace NKikimr::NUdfStore::NWasm {

namespace {

EUdfValueType ParseValueType(TStringBuf type) {
    if (type == "int64") {
        return EUdfValueType::Int64;
    }
    if (type == "uint64") {
        return EUdfValueType::Uint64;
    }
    if (type == "double") {
        return EUdfValueType::Double;
    }
    if (type == "boolean" || type == "bool") {
        return EUdfValueType::Boolean;
    }
    if (type == "string") {
        return EUdfValueType::String;
    }
    if (type == "null") {
        return EUdfValueType::Null;
    }
    ythrow yexception() << "Unsupported wasm UDF manifest type: " << type;
}

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

TVector<EUdfValueType> ParseArgumentTypes(const NJson::TJsonValue& functionNode) {
    TVector<EUdfValueType> result;
    if (!functionNode.Has("argument_types")) {
        return result;
    }
    const auto& args = functionNode["argument_types"];
    if (!args.IsArray()) {
        ythrow yexception() << "argument_types must be an array in wasm manifest";
    }
    for (const auto& arg : args.GetArray()) {
        result.push_back(ParseTypedValue(arg, "argument_types"));
    }
    return result;
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
    return descriptor;
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
    if (!root.Has("functions")) {
        ythrow yexception() << "Wasm manifest is missing functions";
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

    const auto& functions = root["functions"];
    if (!functions.IsArray() || functions.GetArray().empty()) {
        ythrow yexception() << "Wasm manifest functions must be a non-empty array";
    }
    for (const auto& functionNode : functions.GetArray()) {
        manifest.Functions.push_back(ParseFunctionDescriptor(functionNode));
    }
    return manifest;
}

} // namespace NKikimr::NUdfStore::NWasm
