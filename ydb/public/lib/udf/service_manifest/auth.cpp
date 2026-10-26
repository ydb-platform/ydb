#include "auth.h"

#include <ydb/public/lib/udf/manifest/manifest.h>

#include <library/cpp/json/json_reader.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace NYdb::NWasm::NServices {
namespace {
bool Identifier(TStringBuf value) {
    if (value.empty() || value.size() > 128) {
        return false;
    }
    for (char c : value) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_')) {
            return false;
        }
    }
    return true;
}

bool Header(TStringBuf value) {
    if (value.empty() || value.size() > 64 || value.EndsWith("-bin")) {
        return false;
    }
    for (char c : value) {
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
            return false;
        }
    }
    // Auth results may only target credential headers, never routing/framing.
    return value == "authorization" || value.StartsWith("x-auth-") || value == "x-ya-service-ticket";
}
}   // namespace

TAuthManifest ParseAuthManifest(TStringBuf json) {
    const auto common = NYdb::NUdfManifest::Parse(json);
    Y_ENSURE(common.Type == NYdb::NUdfManifest::EModuleType::Module && common.Kind == NYdb::NUdfManifest::EModuleKind::Wasm &&
                 Identifier(common.Name), "Expected a WASM auth module");
    NJson::TJsonValue root;
    Y_ENSURE(NJson::ReadJsonTree(json, &root, true), "Invalid auth manifest");
    const auto& auth = root["auth_provider"];
    Y_ENSURE(auth.IsMap() && auth["abi_version"].GetUIntegerSafe() == AuthAbiVersion, "Unsupported WASM auth ABI");
    TAuthManifest result{ .Module = common.Name, .Provider = auth["name"].GetStringSafe(), .Header = auth["header"].GetStringSafe() };
    Y_ENSURE(Identifier(result.Provider) && result.Provider != "NONE" && result.Provider != "TOKEN" && result.Provider != "CURRENT_IAM",
        "Invalid or reserved WASM auth provider name");
    Y_ENSURE(Header(result.Header), "Invalid WASM auth credential header");
    const auto& params = auth["parameters"].GetArraySafe();
    Y_ENSURE(params.size() <= 16, "Too many WASM auth parameters");
    THashSet<TString> names;
    for (const auto& item : params) {
        const auto name = item["name"].GetStringSafe();
        Y_ENSURE(Identifier(name) && names.insert(name).second, "Invalid or duplicate WASM auth parameter");
        const auto type = item["type"].GetStringSafe();
        Y_ENSURE(type == "String" || type == "Uint32" || type == "Secret", "Invalid WASM auth parameter type");
        result.Parameters.push_back({ name, type == "Secret"   ? EAuthParameterType::Secret
                                            : type == "Uint32" ? EAuthParameterType::Uint32
                                                               : EAuthParameterType::String });
    }
    return result;
}

void ValidateAuthParameters(const TAuthManifest& manifest, const THashMap<TString, TString>& parameters) {
    Y_ENSURE(parameters.size() == manifest.Parameters.size(), "Unexpected WASM auth parameters");
    for (const auto& param : manifest.Parameters) {
        const auto it = parameters.find(param.Name);
        Y_ENSURE(it != parameters.end() && !it->second.empty() && it->second.size() <= 4096, "Missing or invalid WASM auth parameter");
        if (param.Type == EAuthParameterType::Uint32) {
            ui32 value;
            Y_ENSURE(TryFromString(it->second, value) && value, "WASM auth ID must be a nonzero uint32");
        }
    }
}

TAuthRegistry::TAuthRegistry(THashSet<TString> allowedModules)
    : AllowedModules_(std::move(allowedModules))
{
}

void TAuthRegistry::Register(TAuthManifest manifest) {
    Y_ENSURE(AllowedModules_.contains(manifest.Module), "WASM auth module is not operator-approved");
    const auto provider = manifest.Provider;
    Y_ENSURE(Providers_.emplace(provider, std::move(manifest)).second, "Duplicate WASM auth provider");
}

const TAuthManifest& TAuthRegistry::Find(const TString& provider) const {
    const auto it = Providers_.find(provider);
    Y_ENSURE(it != Providers_.end(), "WASM auth provider is not registered");
    return it->second;
}
}   // namespace NYdb::NWasm::NServices
