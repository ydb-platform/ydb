#include "udf_meta.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NYql {

namespace {

class TUdfMeta: public IUdfMeta {
public:
    explicit TUdfMeta(const NJson::TJsonValue& json)
    {
        for (auto& [module, v] : json.GetMapSafe()) {
            auto& names = Modules_[to_lower(module)];
            for (auto& item : v.GetArraySafe()) {
                auto& map = item.GetMapSafe();
                TMeta meta;
                if (auto ptr = map.FindPtr("isTypeAwareness")) {
                    meta.IsTypeAwareness = ptr->GetBooleanSafe();
                }

                if (!meta.IsTypeAwareness) {
                    meta.CallableType = map.at("callableType").GetStringSafe();
                    if (auto ptr = map.FindPtr("runConfigType")) {
                        meta.RunConfigType = ptr->GetStringSafe();
                    }

                    meta.ArgCount = map.at("argCount").GetIntegerSafe();
                    if (auto ptr = map.FindPtr("optionalArgCount")) {
                        meta.OptionalArgCount = ptr->GetIntegerSafe();
                    }

                    if (auto ptr = map.FindPtr("isStrict")) {
                        meta.IsStrict = ptr->GetBooleanSafe();
                    }

                    if (auto ptr = map.FindPtr("supportsBlocks")) {
                        meta.SupportsBlocks = ptr->GetBooleanSafe();
                    }

                    if (auto ptr = map.FindPtr("minLangVer")) {
                        Y_ENSURE(ParseLangVersion(ptr->GetStringSafe(), meta.MinLangVer));
                    }

                    if (auto ptr = map.FindPtr("maxLangVer")) {
                        Y_ENSURE(ParseLangVersion(ptr->GetStringSafe(), meta.MaxLangVer));
                    }
                }

                names.emplace(to_lower(map.at("name").GetStringSafe()), meta);
            }
        }
    }

    bool HasModule(TStringBuf module) const final {
        return Modules_.contains(module);
    }

    bool HasFunction(TStringBuf module, TStringBuf function) const final {
        auto ptr = Modules_.FindPtr(module);
        if (!ptr) {
            return false;
        }

        return ptr->contains(function);
    }

    const TMeta* GetMetadata(TStringBuf module, TStringBuf function) const final {
        auto ptr = Modules_.FindPtr(module);
        if (!ptr) {
            return nullptr;
        }

        return ptr->FindPtr(function);
    }

private:
    THashMap<TString, THashMap<TString, TMeta>> Modules_;
};

} // namespace

std::unique_ptr<IUdfMeta> ParseUdfMeta(const NJson::TJsonValue& json) {
    return std::make_unique<TUdfMeta>(json);
}

} // namespace NYql
