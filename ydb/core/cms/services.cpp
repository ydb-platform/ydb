#include "services.h"

#include <util/generic/hash.h>

namespace NKikimr::NCms {

bool TryFromWhiteBoardRole(const TString& role, EService& value) {
    static const THashMap<TString, EService> roleToService = {
        {"Storage", EService::Storage},
        {"Tenant", EService::DynamicNode},
    };

    auto it = roleToService.find(role);
    if (it == roleToService.end()) {
        return false;
    }

    value = it->second;
    return true;
}

} // namespace NKikimr::NCms
