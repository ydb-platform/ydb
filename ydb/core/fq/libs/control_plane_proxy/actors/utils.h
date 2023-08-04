#pragma once

#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq {

template<typename T>
std::shared_ptr<NYdb::NTable::TTableClient> CreateNewTableClient(
    const T& ev,
    const ::NFq::TComputeConfig& computeConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    auto scope = ev->Get()->Scope;
    ::NFq::NConfig::TYdbStorageConfig computeConnection = computeConfig.GetConnection(scope);

    computeConnection.set_endpoint(ev->Get()->ComputeDatabase->connection().endpoint());
    computeConnection.set_database(ev->Get()->ComputeDatabase->connection().database());
    computeConnection.set_usessl(ev->Get()->ComputeDatabase->connection().usessl());

    auto tableSettings =
        GetClientSettings<NYdb::NTable::TClientSettings>(computeConnection,
                                                         credentialsProviderFactory);
    return std::make_shared<NYdb::NTable::TTableClient>(yqSharedResources->UserSpaceYdbDriver,
                                                        tableSettings);
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
