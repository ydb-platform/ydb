#pragma once

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NWasm::NServices {

inline constexpr ui32 AuthAbiVersion = 1;

enum class EAuthParameterType {
    String,
    Uint32,
    Secret
};

struct TAuthParameter {
    TString Name;
    EAuthParameterType Type;
};

// Auth providers expose the async start/poll/cancel/drop ABI in a private realm.
// They do not register SQL functions or execute DDL during module installation.
struct TAuthManifest {
    TString Module;
    TString Provider;
    TString Header;
    TVector<TAuthParameter> Parameters;
};

TAuthManifest ParseAuthManifest(TStringBuf json);
void ValidateAuthParameters(const TAuthManifest& manifest, const THashMap<TString, TString>& parameters);

// Explicit operator policy, never inferred from the ability to upload a UDF.
// An immutable registry snapshot can be retained by a running compartment.
class TAuthRegistry {
public:
    explicit TAuthRegistry(THashSet<TString> allowedModules);
    void Register(TAuthManifest manifest);
    const TAuthManifest& Find(const TString& provider) const;

private:
    THashSet<TString> AllowedModules_;
    THashMap<TString, TAuthManifest> Providers_;
};

}   // namespace NYdb::NWasm::NServices
