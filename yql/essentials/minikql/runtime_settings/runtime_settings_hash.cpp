#include "runtime_settings_hash.h"
#include "runtime_settings_reflection.h"

#include <util/string/hex.h>

#include <openssl/sha.h>

#include <array>
#include <concepts>

namespace NYql {
namespace {

template <std::integral TValue>
void HashValue(SHA256_CTX& sha, TValue value) {
    SHA256_Update(&sha, &value, sizeof(value));
}

void HashValue(SHA256_CTX& sha, TStringBuf value) {
    HashValue(sha, value.size());
    SHA256_Update(&sha, value.data(), value.size());
}

void HashValue(SHA256_CTX& sha, TDuration value) {
    HashValue(sha, value.MicroSeconds());
}

template <typename TEnum>
    requires std::is_enum_v<TEnum>
void HashValue(SHA256_CTX& sha, TEnum value) {
    HashValue(sha, static_cast<ui64>(value));
}

void HashValue(SHA256_CTX& sha, const TUdfModuleSet& values) {
    HashValue(sha, values.size());
    for (const auto& value : values) {
        HashValue(sha, value);
    }
}

template <typename TKey, typename TValue, typename TCompare, typename TAllocator>
void HashValue(SHA256_CTX& sha, const TMap<TKey, TValue, TCompare, TAllocator>& values) {
    HashValue(sha, values.size());
    for (const auto& [key, value] : values) {
        HashValue(sha, key);
        HashValue(sha, value);
    }
}

void HashHostSettings(SHA256_CTX& sha, const NPrivate::TRuntimeHostSettings& settings) {
    constexpr auto reflection = NReflection::TReflection<NPrivate::TRuntimeHostSettings>::SelfType();
    std::array<ui8, FieldsCount<NPrivate::TRuntimeHostSettings>()> presence;
    reflection.ForEachFieldValue(settings, [&]<size_t Index, auto>(const auto& setting) {
        presence[Index] = setting.IsSet();
    });
    SHA256_Update(&sha, presence.data(), presence.size());
    reflection.ForEachFieldValue(settings, [&]<size_t, auto>(const auto& setting) {
        if (setting.IsSet()) {
            HashValue(sha, setting.Get());
        }
    });
}

} // namespace

TRuntimeSettingsStableHash StableHashRuntimeSettings(const TRuntimeSettings& config) {
    SHA256_CTX sha;
    SHA256_Init(&sha);
    HashHostSettings(sha, config);
    HashValue(sha, config.GetUdfSettings());

    TRuntimeSettingsStableHash digest;
    static_assert(std::tuple_size_v<TRuntimeSettingsStableHash::TUnderlying> == SHA256_DIGEST_LENGTH);
    SHA256_Final(digest->data(), &sha);
    return digest;
}

IOutputStream& operator<<(IOutputStream& out, const TRuntimeSettingsStableHash& hash) {
    return out << HexEncode(hash->data(), hash->size());
}

} // namespace NYql
