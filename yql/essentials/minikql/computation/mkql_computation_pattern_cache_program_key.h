#pragma once

#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_hash.h>
#include <util/digest/sequence.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {

class TProgramKey {
public:
    TProgramKey(NYql::TLangVersion languageVersion,
                NYql::TRuntimeSettingsStableHash stableHash,
                TString serializedProgram)
        : LanguageVersion_(languageVersion)
        , StableHash_(std::move(stableHash))
        , SerializedProgram_(std::move(serializedProgram))
    {
    }

    NYql::TLangVersion GetLanguageVersion() const noexcept {
        return LanguageVersion_;
    }

    const NYql::TRuntimeSettingsStableHash& GetStableHash() const noexcept {
        return StableHash_;
    }

    const TString& GetSerializedProgram() const noexcept {
        return SerializedProgram_;
    }

    bool operator<=>(const TProgramKey& other) const = default;

private:
    NYql::TLangVersion LanguageVersion_;
    NYql::TRuntimeSettingsStableHash StableHash_ = {};
    TString SerializedProgram_;
};

} // namespace NKikimr::NMiniKQL

template <>
struct THash<NKikimr::NMiniKQL::TProgramKey> {
    size_t operator()(const NKikimr::NMiniKQL::TProgramKey& key) const noexcept {
        size_t hash = THash<NYql::TLangVersion>{}(key.GetLanguageVersion());
        hash = CombineHashes(hash, THash<NYql::TRuntimeSettingsStableHash>{}(key.GetStableHash()));
        return CombineHashes(hash, THash<TString>{}(key.GetSerializedProgram()));
    }
};
