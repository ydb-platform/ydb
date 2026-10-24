#pragma once
#include "module_source.h"

#include <ydb/core/fq/libs/wasm_services/auth_module.h>

#include <ydb/library/aclib/aclib.h>

namespace NFq::NWasmServices {
struct TAuthConnection {
    TString Provider;
    TString Parameters;
    TString Identity;
    TAuthSecretResolver ResolveSecret;
};

inline constexpr TStringBuf AuthIdentityKey = "wasm.auth.identity";
inline constexpr TStringBuf AuthDatabaseKey = "wasm.auth.database";
// Resolver identity must come from the engine's private execution context.
TAuthSecretResolver CreateAuthSecretResolver(const NConfig::TWasmServicesConfig& config, const THashMap<TString, TString>& secureParams);
TAuthModuleConfig PrepareAuthModule(const NConfig::TWasmServicesConfig& config, IServiceModuleSource& source, const TAuthConnection& connection);
void ValidateAuthProviders(const NConfig::TWasmServicesConfig& config, IServiceModuleSource& source);
}   // namespace NFq::NWasmServices
