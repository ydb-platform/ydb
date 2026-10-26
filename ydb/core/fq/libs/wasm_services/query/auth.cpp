#include "auth.h"

#include <ydb/public/lib/udf/service_manifest/auth.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <util/generic/hash_set.h>

namespace NFq::NWasmServices {
void ValidateAuthProviders(const NConfig::TWasmServicesConfig& config, IServiceModuleSource& source) {
    THashSet<TString> names, modules;
    for (const auto& approved : config.GetAuthProviders()) {
        Y_ENSURE(!approved.GetProvider().empty() && !approved.GetModule().empty() && names.insert(approved.GetProvider()).second &&
                     modules.insert(approved.GetModule()).second, "Duplicate or invalid approved auth provider");
        // Store artifacts can arrive after node startup. Local modules compile now.
        if (config.GetModuleSource() == NConfig::TWasmServicesConfig::LOCAL_FILES) {
            const auto module = source.Load(approved.GetModule());
            Y_ENSURE(module->Manifest.Auth && module->Manifest.Auth->Provider == approved.GetProvider(), "Auth provider manifest mismatch");
        }
    }
}

TAuthModuleConfig PrepareAuthModule(
    const NConfig::TWasmServicesConfig& config, IServiceModuleSource& source, const TAuthConnection& connection) {
    const NConfig::TWasmServicesConfig::TAuthProvider* approved = nullptr;
    for (const auto& provider : config.GetAuthProviders()) {
        if (provider.GetProvider() == connection.Provider) {
            approved = &provider;
            break;
        }
    }
    Y_ENSURE(approved, "Unknown or unapproved auth provider");
    TAuthModuleConfig result;
    result.Module = source.Load(approved->GetModule());
    Y_ENSURE(result.Module->Manifest.Auth && result.Module->Manifest.Auth->Provider == connection.Provider, "Auth provider manifest mismatch");
    const auto& manifest = *result.Module->Manifest.Auth;
    Y_ENSURE(connection.Parameters.size() <= 65536, "Auth parameters exceed limit");
    NJson::TJsonValue json;
    Y_ENSURE(NJson::ReadJsonTree(connection.Parameters, &json, true) && json.IsMap(), "Invalid auth parameters");
    THashMap<TString, TString> values;
    for (const auto& [name, value] : json.GetMap()) {
        Y_ENSURE(value.IsString(), "Auth parameters must be strings; secrets must be references");
        values.emplace(name, value.GetString());
    }
    NYdb::NWasm::NServices::ValidateAuthParameters(manifest, values);
    NJson::TJsonValue publicParams(NJson::JSON_MAP);
    for (const auto& field : manifest.Parameters) {
        if (field.Type == NYdb::NWasm::NServices::EAuthParameterType::Secret) {
            result.SecretReferences.push_back(values.at(field.Name));
        } else {
            publicParams[field.Name] = values.at(field.Name);
        }
    }
    result.ParametersJson = NJson::WriteJson(publicParams, false);
    result.ConnectionIdentity = connection.Identity;
    result.ResolveSecret = connection.ResolveSecret;
    result.Bootstrap.Endpoint = approved->GetEndpoint();
    result.Bootstrap.CaCertificate = approved->GetCaCertificate();
    Y_ENSURE(!result.Bootstrap.Endpoint.empty(), "Auth provider bootstrap endpoint is required");
    return result;
}
}   // namespace NFq::NWasmServices
