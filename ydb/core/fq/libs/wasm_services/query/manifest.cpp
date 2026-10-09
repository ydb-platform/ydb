#include "manifest.h"

#include <ydb/public/lib/udf/manifest/manifest.h>
#include <library/cpp/json/json_reader.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>

namespace NFq::NWasmServices {
namespace {

using namespace NYdb::NWasm::NServices;

bool ValidName(TStringBuf name) {
    if (name.empty() || name.size() > 128)
        return false;
    for (const char c : name)
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'))
            return false;
    return true;
}

ui32 Number(const NJson::TJsonValue& value, ui32 maximum) {
    const auto number = value.GetUIntegerSafe();
    Y_ENSURE(number <= maximum, "Invalid WASM service manifest numeric limit");
    return number;
}

TVector<TServiceField> Fields(const NJson::TJsonValue& json, ui32& minBytes, ui32& maxBytes) {
    const auto& array = json.GetArraySafe();
    Y_ENSURE(!array.empty() && array.size() <= 32, "Invalid WASM service field count");
    TVector<TServiceField> fields;
    THashSet<TString> names;
    minBytes = maxBytes = 0;
    for (const auto& item : array) {
        TServiceField field;
        field.Name = item["name"].GetStringSafe();
        Y_ENSURE(ValidName(field.Name) && names.insert(field.Name).second, "Invalid or duplicate WASM service field");
        const auto type = item["type"].GetStringSafe();
        ui32 width;
        if (type == "Uint64") {
            field.Type = EValueType::Uint64;
            width = 8;
        } else if (type == "Uint32") {
            field.Type = EValueType::Uint32;
            width = 4;
        } else if (type == "Int64") {
            field.Type = EValueType::Int64;
            width = 8;
        } else if (type == "Bool") {
            field.Type = EValueType::Bool;
            width = 1;
        } else {
            Y_ENSURE(type == "String" || type == "Utf8", "Unsupported WASM service field type");
            field.Type = type == "String" ? EValueType::String : EValueType::Utf8;
            field.MaxBytes = Number(item["max_bytes"], MaxServiceBatchBytes);
            Y_ENSURE(field.MaxBytes, "WASM service string limit must be positive");
            width = 4;
        }
        minBytes += width;
        maxBytes += width + field.MaxBytes;
        Y_ENSURE(maxBytes <= MaxServiceBatchBytes - sizeof(TServiceRequest), "WASM service row exceeds byte limit");
        fields.push_back(std::move(field));
    }
    return fields;
}

} // namespace

TServiceManifest ParseServiceManifest(TStringBuf json) {
    const auto common = NYdb::NUdfManifest::Parse(json);
    Y_ENSURE(common.Type == NYdb::NUdfManifest::EModuleType::Module && common.Kind == NYdb::NUdfManifest::EModuleKind::Wasm,
             "Expected a WASM service module manifest");
    Y_ENSURE(ValidName(common.Name), "Invalid WASM service module name");
    NJson::TJsonValue root;
    Y_ENSURE(NJson::ReadJsonTree(json, &root, true) && root.IsMap(), "Invalid WASM service manifest");
    Y_ENSURE(Number(root["service_abi_version"], ServiceVersion) == ServiceVersion, "Unsupported WASM service ABI version");
    TServiceManifest manifest;
    manifest.Name = common.Name;
    THashSet<ui32> ids;
    const auto& methods = root["service_methods"].GetArraySafe();
    Y_ENSURE(!methods.empty() && methods.size() <= 64, "Invalid WASM service method count");
    for (const auto& item : methods) {
        TServiceMethod method;
        method.Name = item["name"].GetStringSafe();
        Y_ENSURE(ValidName(method.Name), "Invalid WASM service method name");
        Y_ENSURE(!item.Has("id"), "WASM service method IDs must not appear in manifests");
        method.Id = ServiceMethodId({method.Name.data(), method.Name.size()});
        Y_ENSURE(ids.insert(method.Id).second, "WASM service method ID collision");
        method.Batch = item["batch"].GetBooleanSafe();
        method.MaxBatchRows = Number(item["max_batch_rows"], MaxServiceBatchRows);
        Y_ENSURE(method.MaxBatchRows && (method.Batch || method.MaxBatchRows == 1), "Invalid WASM service batch capability");
        method.Input = Fields(item["input"], method.MinInputRowBytes, method.MaxInputRowBytes);
        ui32 minOutput, maxOutput;
        method.Output = Fields(item["output"], minOutput, maxOutput);
        method.MaxOutputRowBytes = Number(item["max_output_row_bytes"], MaxServiceBatchBytes - sizeof(TServiceResult));
        Y_ENSURE(method.MaxOutputRowBytes >= maxOutput, "WASM service output row reservation is too small");
        const auto name = method.Name;
        Y_ENSURE(manifest.Methods.emplace(name, std::move(method)).second, "Duplicate WASM service method name");
    }
    return manifest;
}

} // namespace NFq::NWasmServices
