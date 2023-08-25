#pragma once

#include <ydb/core/fq/libs/compute/common/utils.h>

namespace NFq {

template<typename T>
std::shared_ptr<NYdb::NTable::TTableClient> CreateNewTableClient(const T& ev,
                                                                 const NFq::TComputeConfig& computeConfig,
                                                                 const TYqSharedResources::TPtr& yqSharedResources,
                                                                 const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    const auto& scope = ev->Get()->Scope;
    return CreateNewTableClient(scope, computeConfig, ev->Get()->ComputeDatabase->connection(), yqSharedResources, credentialsProviderFactory);
}

inline static const TMap<TString, TPermissions::TPermission> PermissionsItems = {
    {"yq.resources.viewPublic@as", TPermissions::VIEW_PUBLIC},
    {"yq.resources.viewPrivate@as", TPermissions::VIEW_PRIVATE},
    {"yq.queries.viewAst@as", TPermissions::VIEW_AST},
    {"yq.resources.managePublic@as", TPermissions::MANAGE_PUBLIC},
    {"yq.resources.managePrivate@as", TPermissions::MANAGE_PRIVATE},
    {"yq.queries.invoke@as", TPermissions::QUERY_INVOKE},
    {"yq.queries.viewQueryText@as", TPermissions::VIEW_QUERY_TEXT},
};

template<typename T>
TPermissions ExtractPermissions(T& ev, const TPermissions& availablePermissions) {
    TPermissions permissions;
    for (const auto& permission : ev->Get()->Permissions) {
        if (auto it = PermissionsItems.find(permission); it != PermissionsItems.end()) {
            // cut off permissions that should not be used in other services
            if (availablePermissions.Check(it->second)) {
                permissions.Set(it->second);
            }
        }
    }
    return permissions;
}

} // namespace NFq
