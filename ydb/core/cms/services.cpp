#include "services.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/nameservice.h>
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

void TryFromNodeId(ui32 nodeId, TServices& value) {
    if (AppData()->DynamicNameserviceConfig) {
        if (nodeId <= AppData()->DynamicNameserviceConfig->MaxStaticNodeId) {
            value |= EService::Static;
            value |= EService::Storage; // for compatibility with old scripts
        } else {
            value |= EService::DynamicNode;
        }
    }
}

} // namespace NKikimr::NCms
