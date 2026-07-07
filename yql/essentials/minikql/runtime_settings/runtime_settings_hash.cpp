#include "runtime_settings_hash.h"
#include "runtime_settings_configuration.h"

#include <openssl/sha.h>

namespace NYql {

namespace {

void HashString(SHA256_CTX& sha, TStringBuf str) {
    const ui64 length = str.size();
    SHA256_Update(&sha, &length, sizeof(length));
    SHA256_Update(&sha, str.data(), length);
}

void HashCount(SHA256_CTX& sha, ui64 count) {
    SHA256_Update(&sha, &count, sizeof(count));
}

void HashHostSettings(SHA256_CTX& sha, const TRuntimeSettings& config) {
    TRuntimeSettingsConfiguration configuration(config);
    HashCount(sha, configuration.CountSerializableStaticSettings());
    configuration.SerializeStaticSettings([&](const TString& name, const TString& value) {
        HashString(sha, name);
        HashString(sha, value);
    });
}

void HashUdfSettings(SHA256_CTX& sha, const TRuntimeSettings& config) {
    const auto& udfSettings = config.GetUdfSettings();
    HashCount(sha, udfSettings.size());
    for (const auto& [module, settings] : udfSettings) {
        HashString(sha, module);
        HashCount(sha, settings.size());
        for (const auto& [name, value] : settings) {
            HashString(sha, name);
            HashString(sha, value);
        }
    }
}

} // namespace

TRuntimeSettingsStableHash StableHashRuntimeSettings(const TRuntimeSettings& config) {
    SHA256_CTX sha;
    SHA256_Init(&sha);

    HashHostSettings(sha, config);
    HashUdfSettings(sha, config);

    std::array<ui8, SHA256_DIGEST_LENGTH> hash_array;
    SHA256_Final(hash_array.data(), &sha);
    return TRuntimeSettingsStableHash(reinterpret_cast<const char*>(hash_array.data()), sizeof(hash_array));
}

} // namespace NYql
