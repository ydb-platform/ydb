#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <vector>

namespace NYdb::inline Dev {

template<typename TFrom>
inline void PermissionToSchemeEntry(const TFrom& from, std::vector<NScheme::TPermissions>* to) {
    for (const auto& effPerm : from) {
        to->push_back(NScheme::TPermissions{effPerm.subject(), std::vector<std::string>()});
        for (const auto& permName : effPerm.permission_names()) {
            to->back().PermissionNames.push_back(permName);
        }
    }
}

} // namespace NYdb
