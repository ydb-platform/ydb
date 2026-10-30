#include "udf_meta.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NYql {

namespace {

class TStorage {
public:
    struct TMeta {
        bool IsTypeAwareness = false;
        TString CallableType;
        TString RunConfigType;
        ui32 ArgCount = 0;
        ui32 OptionalArgCount = 0;
        bool IsStrict = false;
        bool SupportsBlocks = false;
        TLangVersion MinLangVer = UnknownLangVersion;
        TLangVersion MaxLangVer = UnknownLangVersion;
        TString PolyArgs;
        TString ResolvedCallableTypes;
    };

    explicit TStorage(const NJson::TJsonValue& json)
    {
        for (auto& [module, v] : json.GetMapSafe()) {
            auto& names = Modules_[to_lower(module)];
            for (auto& item : v.GetArraySafe()) {
                auto& map = item.GetMapSafe();
                TMeta meta;
                if (auto ptr = map.FindPtr("isTypeAwareness")) {
                    meta.IsTypeAwareness = ptr->GetBooleanSafe();
                    if (auto ptr = map.FindPtr("polyArgs")) {
                        meta.PolyArgs = ptr->GetStringSafe();
                    }

                    if (auto ptr = map.FindPtr("resolvedCallableTypes")) {
                        meta.ResolvedCallableTypes = ptr->GetStringSafe();
                    }
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

    bool HasModule(TStringBuf module) const {
        return Modules_.contains(module);
    }

    bool HasFunction(TStringBuf module, TStringBuf function) const {
        auto ptr = Modules_.FindPtr(module);
        if (!ptr) {
            return false;
        }

        return ptr->contains(function);
    }

    const TMeta* GetMetadata(TStringBuf module, TStringBuf function) const {
        auto ptr = Modules_.FindPtr(module);
        if (!ptr) {
            return nullptr;
        }

        return ptr->FindPtr(function);
    }

private:
    THashMap<TString, THashMap<TString, TMeta>> Modules_;
};

TYqlUdfMetaString View(const TString& value) {
    return {value.data(), value.size()};
}

TYqlUdfMetaFunction Describe(const TStorage::TMeta& value) {
    return {
        .IsTypeAwareness = value.IsTypeAwareness,
        .CallableType = View(value.CallableType),
        .RunConfigType = View(value.RunConfigType),
        .ArgCount = value.ArgCount,
        .OptionalArgCount = value.OptionalArgCount,
        .IsStrict = value.IsStrict,
        .SupportsBlocks = value.SupportsBlocks,
        .MinLangVer = value.MinLangVer,
        .MaxLangVer = value.MaxLangVer,
        .PolyArgs = View(value.PolyArgs),
        .ResolvedCallableTypes = View(value.ResolvedCallableTypes),
    };
}

void Free(void* context) noexcept {
    delete static_cast<TStorage*>(context);
}

int HasModule(const void* context, TYqlUdfMetaString module) noexcept {
    try {
        return static_cast<const TStorage*>(context)->HasModule({module.Data, module.Size});
    } catch (...) {
        return -1;
    }
}

int HasFunction(const void* context, TYqlUdfMetaString module, TYqlUdfMetaString function) noexcept {
    try {
        return static_cast<const TStorage*>(context)->HasFunction({module.Data, module.Size}, {function.Data, function.Size});
    } catch (...) {
        return -1;
    }
}

int GetMetadata(const void* context, TYqlUdfMetaString module, TYqlUdfMetaString function, TYqlUdfMetaFunction* result) noexcept {
    try {
        const auto* value = static_cast<const TStorage*>(context)->GetMetadata({module.Data, module.Size}, {function.Data, function.Size});
        if (!value) {
            return 0;
        }
        *result = Describe(*value);
        return 1;
    } catch (...) {
        return -1;
    }
}

} // namespace

std::unique_ptr<TUdfMeta> ParseUdfMeta(const NJson::TJsonValue& json) {
    auto storage = std::make_unique<TStorage>(json);
    auto result = std::make_unique<TUdfMeta>(TYqlUdfMeta{storage.get(), Free, HasModule, HasFunction, GetMetadata});
    storage.release(); // NOLINT(bugprone-unused-return-value): ownership has been transferred to result.
    return result;
}

} // namespace NYql
