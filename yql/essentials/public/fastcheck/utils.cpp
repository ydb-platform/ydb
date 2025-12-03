#include "utils.h"

namespace NYql {
namespace NFastCheck {

TUdfFilter ParseUdfFilter(const NJson::TJsonValue& json) {
    TUdfFilter res;
    for (auto& [module, v] : json.GetMapSafe()) {
        auto& names = res.Modules[to_lower(module)];
        for (auto& item : v.GetArraySafe()) {
            names.insert(to_lower(item.GetMapSafe().at("name").GetStringSafe()));
        }
    }

    return res;
}

} // namespace NFastCheck
} // namespace NYql
