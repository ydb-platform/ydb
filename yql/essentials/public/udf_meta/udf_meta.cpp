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
                names.insert(to_lower(item.GetMapSafe().at("name").GetStringSafe()));
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

private:
    THashMap<TString, THashSet<TString>> Modules_;
};

} // namespace

std::unique_ptr<IUdfMeta> ParseUdfMeta(const NJson::TJsonValue& json) {
    return std::make_unique<TUdfMeta>(json);
}

} // namespace NYql
