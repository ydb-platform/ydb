#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

namespace NYdb {

template<typename TFrom>
inline void PermissionToSchemeEntry(const TFrom& from, TVector<NScheme::TPermissions>* to) {
    for (const auto& effPerm : from) {
        to->push_back(NScheme::TPermissions{effPerm.subject(), TVector<TString>()});
        for (const auto& permName : effPerm.permission_names()) {
            to->back().PermissionNames.push_back(permName);
        }
    }
}

} // namespace NYdb
